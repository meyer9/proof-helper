#![warn(clippy::future_not_send)]
use futures_util::TryStreamExt;
use op_reth::node::OpNode;
use reth::{
    api::{FullNodeComponents, NodePrimitives},
    builder::NodeTypes,
    core::primitives::AlloyBlockHeader,
    primitives::{RecoveredBlock, StorageEntry},
    providers::{DatabaseProviderFactory, StateProviderFactory, StateReader},
    revm::primitives::{FixedBytes, HashMap, map::FbBuildHasher},
};

use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_trie::{
    HashedPostState,
    updates::{StorageTrieUpdates, TrieUpdates},
};
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver};
use tracing::{debug, error, info};

mod backfill;
mod config;
mod proof;
mod provider;
mod rpc;
mod sqlite;
mod storage;

use config::{ProofHelperConfig, StorageBackend};
use sqlite::SqlitePreimageStore;
use storage::{BranchNodeEntry, ExternalStateStore, TrieBranchesBatch};

use crate::{
    backfill::{BackfillJob, BackfillOperation},
    rpc::{EthApiExt, EthApiOverrideServer},
};

/// Proof Helper ExEx - processes blocks and tracks state changes
pub struct ProofHelper<Node, PreimageStore>
where
    Node: FullNodeComponents,
    Node::Provider: StateReader,
{
    ctx: ExExContext<Node>,
    storage: PreimageStore,
}

async fn run_backfill<
    P: StateProviderFactory + DatabaseProviderFactory + Send,
    S: ExternalStateStore + Send,
>(
    storage: S,
    provider: P,
    receiver: &mut Receiver<BackfillOperation>,
) {
    let mut job = BackfillJob::new(storage, provider);
    while let Some(operation) = receiver.recv().await {
        if let Err(e) = job.process_operation(operation).await {
            error!("Error processing operation: {}", e);
            return;
        }
        if let Err(e) = job.step().await {
            error!("Error stepping job: {}", e);
            return;
        }
    }
}

impl<Node, Primitives, P> ProofHelper<Node, P>
where
    Node: FullNodeComponents<Types: NodeTypes<Primitives = Primitives>>,
    Primitives: NodePrimitives,
    P: ExternalStateStore + Clone + 'static,
{
    /// Create a new ProofHelper instance
    pub fn new(ctx: ExExContext<Node>, storage: P) -> Self {
        Self { ctx, storage }
    }

    async fn write_storage_trie_updates(
        &self,
        account_storage_updates: &HashMap<FixedBytes<32>, StorageTrieUpdates, FbBuildHasher<32>>,
        block_number: u64,
    ) -> eyre::Result<u64> {
        let mut num_entries = 0;
        let mut preimage_batch = TrieBranchesBatch::new(block_number);
        for (hashed_address, updates) in account_storage_updates {
            // The storage trie for this account has to be deleted.
            if updates.is_deleted() {
                // TODO: delete all storage trie entries for this account
            }

            // Merge updated and removed nodes. Updated nodes must take precedence.
            let mut storage_updates = updates
                .removed_nodes_ref()
                .iter()
                .filter_map(|n| (!updates.storage_nodes_ref().contains_key(n)).then_some((n, None)))
                .collect::<Vec<_>>();
            storage_updates.extend(
                updates
                    .storage_nodes_ref()
                    .iter()
                    .map(|(nibbles, node)| (nibbles, Some(node))),
            );
            for (nibbles, maybe_updated) in
                storage_updates.into_iter().filter(|(n, _)| !n.is_empty())
            {
                num_entries += 1;
                preimage_batch.items.push(BranchNodeEntry {
                    path: *nibbles,
                    hashed_address: Some(*hashed_address),
                    branch: maybe_updated.map(|node| node.clone()),
                });
            }
        }

        debug!("Storing {} storage trie updates", num_entries);

        self.storage.store_trie_branches(preimage_batch).await?;

        Ok(num_entries)
    }

    async fn write_trie_updates(
        &self,
        trie_updates: &TrieUpdates,
        block: &RecoveredBlock<Primitives::Block>,
    ) -> eyre::Result<u64> {
        // Track the number of inserted entries.
        let mut num_entries = 0;

        // Merge updated and removed nodes. Updated nodes must take precedence.
        let mut account_updates = trie_updates
            .removed_nodes_ref()
            .iter()
            .filter_map(|n| {
                (!trie_updates.account_nodes_ref().contains_key(n)).then_some((n, None))
            })
            .collect::<Vec<_>>();
        account_updates.extend(
            trie_updates
                .account_nodes_ref()
                .iter()
                .map(|(nibbles, node)| (nibbles, Some(node))),
        );
        // Sort trie node updates.
        account_updates.sort_unstable_by(|a, b| a.0.cmp(b.0));

        let mut batch_to_store = TrieBranchesBatch::new(block.number());

        for (key, updated_node) in account_updates {
            let nibbles = *key;
            match updated_node {
                Some(node) => {
                    if !nibbles.is_empty() {
                        num_entries += 1;
                        batch_to_store.items.push(BranchNodeEntry {
                            path: nibbles,
                            hashed_address: None,
                            branch: Some(node.clone()),
                        });
                    }
                }
                None => {
                    num_entries += 1;
                    batch_to_store.items.push(BranchNodeEntry {
                        path: nibbles,
                        hashed_address: None,
                        branch: None,
                    });
                }
            }
        }

        debug!("Storing {} account tries", num_entries);
        self.storage.store_trie_branches(batch_to_store).await?;
        num_entries += self
            .write_storage_trie_updates(trie_updates.storage_tries_ref(), block.number())
            .await?;

        Ok(num_entries)
    }

    async fn write_leaf_updates(
        &self,
        post_state: HashedPostState,
        block_number: u64,
    ) -> eyre::Result<u64> {
        let accounts = post_state
            .accounts
            .iter()
            .map(|(address, account)| (*address, account.clone()))
            .collect::<Vec<_>>();
        let mut num_entries = accounts.len() as u64;
        debug!("Storing {} account leaves", accounts.len());
        self.storage
            .store_hashed_accounts(accounts, block_number)
            .await?;
        for (address, storage) in post_state.storages.iter() {
            let storages = storage
                .storage
                .iter()
                .map(|(key, value)| (*key, *value))
                .collect::<Vec<_>>();
            num_entries += storages.len() as u64;
            debug!(
                "Storing {} storage leaves for address {}",
                storages.len(),
                address
            );
            self.storage
                .store_hashed_storages(
                    storages
                        .into_iter()
                        .map(|storage| {
                            (
                                *address,
                                StorageEntry {
                                    key: storage.0,
                                    value: storage.1,
                                },
                            )
                        })
                        .collect(),
                    block_number,
                )
                .await?;
        }
        Ok(num_entries)
    }

    /// Main execution loop for the ExEx
    pub async fn run(mut self, _config: ProofHelperConfig) -> eyre::Result<()> {
        let provider = self.ctx.provider().clone();

        // let (tx_channel, mut tx_channel_rx) = oneshot::channel();
        let storage = self.storage.clone();
        let (operations_tx, mut operations_rx) = mpsc::channel(1);

        self.ctx
            .components
            .task_executor()
            .spawn_critical("proof-helper", async move {
                run_backfill(storage, provider, &mut operations_rx).await;
            });

        // let operations_tx = tx_channel_rx.try_recv()?;

        while let Some(notification) = self.ctx.notifications.try_next().await? {
            match &notification {
                ExExNotification::ChainCommitted { new } => {
                    operations_tx
                        .send(BackfillOperation::SetTargetBlockNumber {
                            target_block_number: new.tip().num_hash().number,
                        })
                        .await?;
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
async fn create_storage(config: &ProofHelperConfig) -> eyre::Result<Arc<SqlitePreimageStore>> {
    match config.storage.backend {
        StorageBackend::SQLite => {
            info!("Using SQLite storage backend");
            let sqlite_cfg = config.storage.sqlite.as_ref().ok_or_else(|| {
                eyre::eyre!("SQLite configuration is required when using SQLite backend")
            })?;

            let store = SqlitePreimageStore::new(&sqlite_cfg.db_path)
                .await
                .map_err(|e| eyre::eyre!("Failed to create SQLite store: {}", e))?;
            store
                .health_check()
                .await
                .map_err(|e| eyre::eyre!("SQLite health check failed: {}", e))?;
            info!("SQLite store initialized successfully");
            Ok(Arc::new(store))
        }
    }
}

fn main() -> eyre::Result<()> {
    // Load configuration
    let config = ProofHelperConfig::load_from_env()
        .map_err(|e| eyre::eyre!("Failed to load configuration: {}", e))?;

    // run get_storage and wait for it to complete
    let storage = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            create_storage(&config)
                .await
                .map_err(|e| eyre::eyre!("Failed to create storage backend: {}", e))
        })?;

    let storage_2 = storage.clone();

    op_reth::cli::Cli::parse_args().run(async move |builder, _| {
        let handle = builder
            .node(OpNode::default())
            .install_exex("proof-helper", async move |ctx| {
                // let builder = ctx.components.payload_builder_handle();

                let proof_helper = ProofHelper::new(ctx, storage);
                Ok(proof_helper.run(config))
            })
            .extend_rpc_modules(move |ctx| {
                let api_ext = EthApiExt::new(
                    ctx.registry.eth_api().clone(),
                    storage_2,
                    ctx.provider().clone(),
                );
                ctx.modules.replace_configured(api_ext.into_rpc())?;
                Ok(())
            })
            .launch()
            .await?;

        handle.wait_for_node_exit().await
    })
}
