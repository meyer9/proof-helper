use alloy_rlp::{Decodable, Encodable};
use futures_util::TryStreamExt;
use op_reth::node::OpNode;
use op_reth::primitives::OpPrimitives;
use reth::{
    api::{Block, ConfigureEvm, FullNodeComponents, NodePrimitives},
    builder::NodeTypes,
    core::primitives::AlloyBlockHeader,
    primitives::RecoveredBlock,
    providers::{BlockReader, HeaderProvider, StateProviderFactory, StateReader},
    revm::{
        database::StateProviderDatabase, primitives::{keccak256, map::{B256Map, B256Set}, Address, B256, KECCAK_EMPTY}, state::AccountInfo, witness::ExecutionWitnessRecord, State
    }, rpc::types::BlockNumberOrTag,
};
use reth_db_api::table::Encode;
use reth_evm::execute::Executor;
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_stateless::ExecutionWitness;
use reth_tracing::tracing::{info, trace, warn, debug};
use reth_trie::{
    BranchNodeCompact, HashedStorage, LeafNode, MultiProofTargets, Nibbles, StoredNibbles, StoredNibblesSubKey, TrieAccount, TrieInput, TrieNode
};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use reth_trie::{HashedPostState, KeccakKeyHasher};
use reth::rpc::types::TransactionTrait;

mod config;
mod dynamodb;
mod storage;

use config::{ProofHelperConfig, StorageBackend};
use dynamodb::DynamoDbPreimageStore;
use storage::{MockPreimageStore, PreimageEntry, PreimageStore, PreimageBatch};

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
    Node: FullNodeComponents<
        Types: NodeTypes<Primitives = OpPrimitives>,
    >,
    Node::Provider: StateReader + StateProviderFactory + BlockReader,
{
    /// Create a new ProofHelper instance
    pub fn new(ctx: ExExContext<Node>, storage: Arc<dyn PreimageStore>) -> Self {
        Self { ctx, storage }
    }

    fn process_block(
        &self,
        block: &RecoveredBlock<<<Node::Types as NodeTypes>::Primitives as NodePrimitives>::Block>,
        progress_interval: u64,
    ) -> eyre::Result<()> {
        let block_number = block.header().number();
        let parent_hash = block.header().parent_hash();
        let parent_number = block.header().number().saturating_sub(1);

        debug!(
            "processing block number={}, parent_number={}, parent_hash={:?}",
            block.header().number(),
            parent_number,
            parent_hash
        );

        // Use proper historical state access - get state at parent hash
        // This ensures we get the exact state before the current block was executed
        // (same pattern as debug RPC uses)
        let state_provider = self.ctx.provider().history_by_block_hash(parent_hash)?;

        debug!("executing block {}, num txs: {}", block_number, block.body().transactions().collect::<Vec<_>>().len());

        // this is correct
        // debug initial state root intentionally removed to avoid trait ambiguity
        // debug!("initial state root: {:?}", state_provider.state_root(Default::default()));

        // Target address instrumentation to diagnose nonce increment
        let target_addr: Address = "0xDeaDDEaDDeAdDeAdDEAdDEaddeAddEAdDEAd0001".parse().unwrap();
        let target_hashed = keccak256(target_addr);

        // Get the correct parent nonce using historical state provider
        let parent_nonce = state_provider
            .basic_account(&target_addr)?
            .map(|a| a.nonce)
            .unwrap_or(0);
        debug!("parent nonce using history_by_block_hash: {}", parent_nonce);

        // Also check nonce at earlier blocks to detect systematic offset
        if parent_number > 0 {
            let earlier_provider = self
                .ctx
                .provider()
                .state_by_block_number_or_tag(BlockNumberOrTag::Number(parent_number.saturating_sub(1)))?;
            let earlier_nonce = earlier_provider
                .basic_account(&target_addr)?
                .map(|a| a.nonce)
                .unwrap_or(0);
            debug!(
                "nonce at block {}: {}",
                parent_number.saturating_sub(1),
                earlier_nonce
            );
        }

        // Test reading from explicit block numbers to verify which block we're actually getting
        for test_block_num in [parent_number.saturating_sub(2), parent_number.saturating_sub(1), parent_number, parent_number + 1] {
            if let Ok(test_provider) = self
                .ctx
                .provider()
                .state_by_block_number_or_tag(BlockNumberOrTag::Number(test_block_num))
            {
                let test_nonce = test_provider
                    .basic_account(&target_addr)?
                    .map(|a| a.nonce)
                    .unwrap_or(0);
                debug!("nonce at explicit block {}: {}", test_block_num, test_nonce);
            }
        }

        // Try using history_by_block_number which should give pre-execution state
        if let Ok(historical_provider) = self.ctx.provider().history_by_block_number(parent_number) {
            let historical_nonce = historical_provider
                .basic_account(&target_addr)?
                .map(|a| a.nonce)
                .unwrap_or(0);
            debug!("nonce via history_by_block_number({}): {}", parent_number, historical_nonce);
        }

        // Also try getting the parent block header and using its parent for state
        let grandparent_number = parent_number.saturating_sub(1);
        if let Ok(grandparent_provider) = self.ctx.provider().history_by_block_number(grandparent_number) {
            let grandparent_nonce = grandparent_provider
                .basic_account(&target_addr)?
                .map(|a| a.nonce)
                .unwrap_or(0);
            debug!("nonce at grandparent block {}: {}", grandparent_number, grandparent_nonce);
            debug!("expected parent nonce should be: {} + 1 = {}", grandparent_nonce, grandparent_nonce + 1);
        }

        // Parent nonce already logged above using history_by_block_hash

        // Try to find where the correct parent nonce (28899403) actually is
        debug!("searching for correct parent nonce 28899403:");
        for search_block in (parent_number.saturating_sub(5))..=(parent_number + 2) {
            if let Ok(search_provider) = self.ctx.provider().history_by_block_number(search_block) {
                let search_nonce = search_provider
                    .basic_account(&target_addr)?
                    .map(|a| a.nonce)
                    .unwrap_or(0);
                if search_nonce == 28899403 {
                    debug!("FOUND correct parent nonce 28899403 at block {}", search_block);
                }
            }
        }

        // Try to determine what block the state_provider thinks it's at
        // by checking if it can give us the parent hash
        let provider_block_hash = self.ctx.provider().sealed_header(parent_number)?.expect("parent block not found").hash();
        debug!("block hash at parent_number {}: {:?}", parent_number, provider_block_hash);
        debug!("expected parent_hash: {:?}", parent_hash);
        debug!("hashes match: {}", provider_block_hash == parent_hash);
    

        // Log the first few transaction nonces in this block for reference
        debug!("first few tx nonces in block:");
        for (i, tx) in block.body().transactions().enumerate().take(3) {
            debug!("  tx[{}]: nonce={}, from={:?}", i, tx.nonce(), block.senders().get(i));
        }

        let db = StateProviderDatabase::new(&state_provider);
        let block_executor = self.ctx.evm_config().batch_executor(db);
    
        let mut witness_record = ExecutionWitnessRecord::default();

        let execution_result = block_executor
            .execute_with_state_closure(&(*block).clone(), |state: &State<_>| {
                witness_record.record_executed_state(state);
            })
            .map_err(|err| eyre::eyre!(err))?;

        // Derive hashed post-state from the executor's bundle state (authoritative post-state)
        let hashed_state = HashedPostState::from_bundle_state::<KeccakKeyHasher>(
            execution_result.state.state(),
        );

        // Log post nonce for the target account and delta
        let post_nonce = hashed_state
            .accounts
            .get(&target_hashed)
            .and_then(|a| a.as_ref().map(|i| i.nonce))
            .unwrap_or(parent_nonce);
        debug!(
            "post nonce for {:?}: {}, delta: {}",
            target_addr,
            post_nonce,
            post_nonce.saturating_sub(parent_nonce)
        );

        // Count how many transactions in the block are from the target address
        let tx_senders = block.senders();
        let mut from_count = 0usize;
        for sender in tx_senders.iter() {
            if *sender == target_addr { from_count += 1; }
        }
        debug!(
            "block senders from target {:?}: {}",
            target_addr,
            from_count
        );

        let initial_state_provider = self.ctx.provider().history_by_block_hash(parent_hash)?;

        let (state_root, _) = initial_state_provider.state_root_with_updates(hashed_state.clone())?;
        debug!("state root: {:?}", state_root);

        let mut targets = B256Map::<B256Set>::with_capacity_and_hasher(hashed_state.storages.len(), Default::default());

        for (address, storage) in hashed_state.storages.iter() {
            let mut set = targets.entry(address.clone()).or_default();
            for (slot, _) in storage.storage.iter() {
                debug!("slot {:?} of address {:?} changed", slot, address);
                set.insert(slot.clone());
            }
        }

        // for addresses, ensure that the key exists, otherwise insert an empty set
        for (address, new_account) in hashed_state.accounts.iter() {
            // nonce here is too high, indicating double execution
            debug!("address {:?} changed to {:?}", address, new_account);
            targets.entry(address.clone()).or_default();
        }

        let multiproof = initial_state_provider.multiproof(TrieInput::from_state(hashed_state), targets.into_iter().collect())?;

        // let prefix_sets = post_state.construct_prefix_sets().freeze();
        // let state_sorted = post_state.into_sorted();
        

        // let trie_cursor_factory = DatabaseTrieCursorFactory::new(tx);
        // let hashed_cursor_factory = HashedPostStateCursorFactory::new(DatabaseHashedCursorFactory::new(tx), &post_state);
            // StateRoot::new(
            //     DatabaseTrieCursorFactory::new(tx),
            //     HashedPostStateCursorFactory::new(DatabaseHashedCursorFactory::new(tx), &state_sorted),
            // )
            // .with_prefix_sets(prefix_sets)
            // .root_with_updates()

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
            if let Err(e) = storage_clone.store_preimages_batch(PreimageBatch {
                block_number: block_number,
                items: preimages.clone(),
            }).await {
                warn!("Failed to store preimages for block {}: {}", block_number, e);
            } else {
                info!("Successfully stored {} preimages for block {}", preimages.len(), block_number);
            }
        });

        Ok(())
    }

    /// Main execution loop for the ExEx
    pub async fn run(mut self, config: ProofHelperConfig) -> eyre::Result<()> {
        let max_block_diff = config.processing.max_block_diff;
        let progress_interval = config.logging.progress_interval;

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

                        let parent_block = self.ctx.provider().block(block.header().parent_hash().into())?.expect("parent block not found");

                        if let Err(err) = self.process_block(&parent_block.try_into_recovered()?, progress_interval) {
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
        StorageBackend::Mock => {
            info!("Using mock storage backend");
            Ok(Arc::new(MockPreimageStore::new()))
        }
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
    // Parse command line arguments to check for config file
    let args: Vec<String> = std::env::args().collect();
    let mut config_file_path: Option<String> = None;

    // // Simple argument parsing for --config flag
    // for i in 0..args.len() {
    //     if args[i] == "--config" && i + 1 < args.len() {
    //         config_file_path = Some(args[i + 1].clone());
    //         break;
    //     }
    // }

    // Load configuration
    let config = if let Some(path) = config_file_path {
        info!("Loading configuration from file: {}", path);
        ProofHelperConfig::load_from_file(&path)
            .map_err(|e| eyre::eyre!("Failed to load configuration: {}", e))?
    } else {
        info!("Loading configuration from environment variables");
        ProofHelperConfig::load_from_env()
            .map_err(|e| eyre::eyre!("Failed to load configuration: {}", e))?
    };

    info!("Configuration loaded successfully");
    info!("Storage backend: {:?}", config.storage.backend);
    info!("Log level: {}", config.logging.level);
    info!("Progress interval: {}", config.logging.progress_interval);
    info!("Max block diff: {}", config.processing.max_block_diff);

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
