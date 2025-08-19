use futures_util::TryStreamExt;
use op_reth::node::OpNode;
use op_reth::primitives::OpPrimitives;
use reth::{
    api::{ConfigureEvm, FullNodeComponents, NodePrimitives},
    builder::NodeTypes,
    core::primitives::AlloyBlockHeader,
    primitives::RecoveredBlock,
    providers::{BlockReader, StateProviderFactory, StateReader, TransactionVariant},
    revm::{
        database::StateProviderDatabase,
        primitives::{
            keccak256,
            map::{B256Map, B256Set},
        },
    },
};
use reth_evm::execute::Executor;
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_tracing::tracing::{info, warn};
use reth_trie::TrieInput;
use reth_trie::{HashedPostState, KeccakKeyHasher};
use std::sync::Arc;

mod config;
mod dynamodb;
mod storage;

use config::{ProofHelperConfig, StorageBackend};
use dynamodb::DynamoDbPreimageStore;
use storage::{PreimageBatch, PreimageEntry, PreimageStore};

/// Proof Helper ExEx - processes blocks and tracks state changes
pub struct ProofHelper<Node>
where
    Node: FullNodeComponents,
    Node::Provider: StateReader,
{
    ctx: ExExContext<Node>,
    storage: Arc<dyn PreimageStore>,
}

impl<Node> ProofHelper<Node>
where
    Node: FullNodeComponents<Types: NodeTypes<Primitives = OpPrimitives>>,
    Node::Provider: StateReader + StateProviderFactory + BlockReader,
{
    /// Create a new ProofHelper instance
    pub fn new(ctx: ExExContext<Node>, storage: Arc<dyn PreimageStore>) -> Self {
        Self { ctx, storage }
    }

    fn process_block(
        &self,
        block: &RecoveredBlock<<<Node::Types as NodeTypes>::Primitives as NodePrimitives>::Block>,
    ) -> eyre::Result<()> {
        let block_number = block.header().number();
        let parent_hash = block.header().parent_hash();
        let state_provider = self.ctx.provider().history_by_block_hash(parent_hash)?;

        let db = StateProviderDatabase::new(&state_provider);
        let block_executor = self.ctx.evm_config().batch_executor(db);

        let execution_result = block_executor
            .execute(&(*block).clone())
            .map_err(|err| eyre::eyre!(err))?;

        let hashed_state =
            HashedPostState::from_bundle_state::<KeccakKeyHasher>(execution_result.state.state());

        let mut targets = B256Map::<B256Set>::with_capacity_and_hasher(
            hashed_state.storages.len(),
            Default::default(),
        );

        for (address, storage) in hashed_state.storages.iter() {
            let set = targets.entry(address.clone()).or_default();
            for (slot, _) in storage.storage.iter() {
                set.insert(slot.clone());
            }
        }

        // for addresses, ensure that the key exists, otherwise insert an empty set
        for (address, _) in hashed_state.accounts.iter() {
            targets.entry(address.clone()).or_default();
        }

        // prove all the changed keys
        let multiproof = state_provider.multiproof(
            TrieInput::from_state(hashed_state),
            targets.into_iter().collect(),
        )?;

        let mut preimages = Vec::new();

        for (nibbles, data) in multiproof.account_subtree.iter() {
            let hash = keccak256(data);

            preimages.push(PreimageEntry {
                hash: hash,
                preimage: data.to_vec(),
                hashed_address: None,
                path: nibbles.clone(),
                block_number: block_number,
            });
        }

        for (account_hash, storage_multiproof) in multiproof.storages.iter() {
            for (nibbles, data) in storage_multiproof.subtree.iter() {
                let hash = keccak256(data);

                preimages.push(PreimageEntry {
                    hash: hash,
                    preimage: data.to_vec(),
                    hashed_address: Some(account_hash.clone()),
                    path: nibbles.clone(),
                    block_number: block_number,
                });
            }
        }

        // Store the batch
        let storage_clone = Arc::clone(&self.storage);
        tokio::spawn(async move {
            if let Err(e) = storage_clone
                .store_preimages_batch(PreimageBatch {
                    block_number: block_number,
                    items: preimages.clone(),
                })
                .await
            {
                warn!(
                    "Failed to store preimages for block {}: {}",
                    block_number, e
                );
            } else {
                info!(
                    "Successfully stored {} preimages for block {}",
                    preimages.len(),
                    block_number
                );
            }
        });

        Ok(())
    }

    /// Main execution loop for the ExEx
    pub async fn run(mut self, config: ProofHelperConfig) -> eyre::Result<()> {
        let max_block_diff = config.processing.max_block_diff;

        while let Some(notification) = self.ctx.notifications.try_next().await? {
            match &notification {
                ExExNotification::ChainCommitted { new } => {
                    let head_block_number = new.tip().num_hash().number;

                    for (block_number, block) in new.blocks() {
                        if head_block_number.saturating_sub(*block_number) > max_block_diff {
                            warn!(
                                "Block {} is too far behind the head block {}, skipping",
                                block_number, head_block_number
                            );
                            continue;
                        }

                        let Some(parent_block) = self.ctx.provider().recovered_block(
                            block.header().parent_hash().into(),
                            TransactionVariant::NoHash,
                        )?
                        else {
                            warn!("Parent block not found for block {}", block_number);
                            continue;
                        };

                        if let Err(err) = self.process_block(&parent_block) {
                            warn!("Error processing block {}: {}", block_number, err);
                        }
                    }
                }
                _ => {}
            };

            // Send finish event for committed chain
            if let Some(committed_chain) = notification.committed_chain() {
                self.ctx
                    .events
                    .send(ExExEvent::FinishedHeight(committed_chain.tip().num_hash()))?;
            }
        }

        Ok(())
    }
}

/// Create storage backend based on configuration
async fn create_storage(config: &ProofHelperConfig) -> eyre::Result<Arc<dyn PreimageStore>> {
    match config.storage.backend {
        StorageBackend::DynamoDB => {
            info!("Using DynamoDB storage backend");
            let dynamodb_config = config.storage.dynamodb.as_ref().ok_or_else(|| {
                eyre::eyre!("DynamoDB configuration is required when using DynamoDB backend")
            })?;

            let store = DynamoDbPreimageStore::new(dynamodb_config)
                .await
                .map_err(|e| eyre::eyre!("Failed to create DynamoDB store: {}", e))?;

            // Run health check to ensure connection is working
            store
                .health_check()
                .await
                .map_err(|e| eyre::eyre!("DynamoDB health check failed: {}", e))?;

            info!("DynamoDB store initialized successfully");
            Ok(Arc::new(store))
        }
    }
}

fn main() -> eyre::Result<()> {
    // Load configuration
    let config = ProofHelperConfig::load_from_env()
        .map_err(|e| eyre::eyre!("Failed to load configuration: {}", e))?;

    op_reth::cli::Cli::parse_args().run(async move |builder, _| {
        let handle = builder
            .node(OpNode::default())
            .install_exex("proof-helper", async move |ctx| {
                // Create storage backend based on configuration
                let storage = create_storage(&config)
                    .await
                    .map_err(|e| eyre::eyre!("Failed to create storage backend: {}", e))?;

                let proof_helper = ProofHelper::new(ctx, storage);
                Ok(proof_helper.run(config))
            })
            .launch()
            .await?;

        handle.wait_for_node_exit().await
    })
}
