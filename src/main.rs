use futures_util::TryStreamExt;
use op_reth::node::OpNode;
use op_reth::primitives::OpPrimitives;
use reth::{
    api::{FullNodeComponents},
    builder::NodeTypes,
    providers::{DBProvider, DatabaseProviderFactory, StateReader},
};
use reth_db_api::{cursor::{DbCursorRO, DbDupCursorRO}, tables, transaction::DbTx};
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_tracing::tracing::{info, warn};
use reth_trie::{StoredNibbles};
use std::sync::Arc;

mod config;
mod sqlite;
mod storage;

use config::{ProofHelperConfig, StorageBackend};
use sqlite::SqlitePreimageStore;
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
{
    /// Create a new ProofHelper instance
    pub fn new(ctx: ExExContext<Node>, storage: Arc<dyn PreimageStore>) -> Self {
        Self { ctx, storage }
    }

    async fn backfill_accounts_trie(&self) -> eyre::Result<()> {
        let db_provider = self.ctx.provider().database_provider_ro()?;
        let db = db_provider.tx_ref();

        // count entries in AccountsTrie and StorageTrie
        let mut accounts_trie_cursor = db.cursor_read::<tables::AccountsTrie>()?;

        let mut entry = accounts_trie_cursor.first()?;
        let mut batch: PreimageBatch = PreimageBatch::new(0);

        let mut count = 0;
        loop {
            if let Some(entry) = entry {
                // let key = entry.0;
                // let value = entry.1;
                batch.items.push(PreimageEntry {
                    block_number: 0,
                    path: entry.0,
                    hashed_address: None,
                    branch: entry.1,
                });
                count += 1;
            } else {
                break;
            }

            if count % 10000 == 0 {
                info!("AccountsTrie has {} entries", count);
                self.storage.store_preimages_batch(batch).await?;
                batch = PreimageBatch::new(0);
            }

            entry = accounts_trie_cursor.next()?;
        }

        if batch.items.len() > 0 {
            self.storage.store_preimages_batch(batch).await?;
        }

        info!("AccountsTrie has {} entries", count);

        Ok(())
    }

    async fn backfill_storages_trie(&self) -> eyre::Result<()> {
        let db_provider = self.ctx.provider().database_provider_ro()?;
        let db = db_provider.tx_ref();

        // count entries in AccountsTrie and StorageTrie
        let mut hashed_storages_cursor = db.cursor_dup_read::<tables::HashedStorages>()?;
        let mut storages_trie_cursor = db.cursor_dup_read::<tables::StoragesTrie>()?;

        let mut storage_entry = hashed_storages_cursor.first()?;
        let mut batch: PreimageBatch = PreimageBatch::new(0);
        let mut count = 0;
        loop {
            if let Some(storage_entry) = storage_entry {
                let address = storage_entry.0;

                let mut address_count = 0;

                // save all the entries for this address
                let account_entry = storages_trie_cursor.walk_dup(Some(address), None)?;
                for res in account_entry {
                    let Ok((account, branch)) = res else {
                        warn!("Error walking account entry: {}", res.err().unwrap());
                        break;
                    };

                    if account != address {
                        warn!("Account address mismatch: {} != {}", account, address);
                        break;
                    }

                    batch.items.push(PreimageEntry {
                        block_number: 0,
                        path: StoredNibbles(branch.nibbles.0),
                        hashed_address: Some(account),
                        branch: branch.node,
                    });

                    if (count + address_count) % 10000 == 0 {
                        info!("StoragesTrie has {} entries", count + address_count);
                        self.storage.store_preimages_batch(batch).await?;
                        batch = PreimageBatch::new(0);
                    }

                    address_count += 1;
                }

                if address_count > 100 {
                    info!("StoragesTrie has {} entries for address {}", address_count, address);
                }
                    
                count += address_count;
            } else {
                break;
            }


            storage_entry = hashed_storages_cursor.next_no_dup()?;
        }
        info!("StoragesTrie has {} entries", count);

        if batch.items.len() > 0 {
            self.storage.store_preimages_batch(batch).await?;
        }

        Ok(())
    }


    async fn backfill_preimages(&self) -> eyre::Result<()> {
        self.backfill_storages_trie().await?;
        self.backfill_accounts_trie().await?;
        Ok(())
    }

    // fn process_block(
    //     &self,
    //     block: &RecoveredBlock<<<Node::Types as NodeTypes>::Primitives as NodePrimitives>::Block>,
    // ) -> eyre::Result<()> {
    //     // TODO: Implement this

    //     Ok(())
    // }

    /// Main execution loop for the ExEx
    pub async fn run(mut self, _config: ProofHelperConfig) -> eyre::Result<()> {
        self.backfill_preimages().await?;

        while let Some(notification) = self.ctx.notifications.try_next().await? {
            match &notification {
                ExExNotification::ChainCommitted { .. } => {
                    // TODO: store storage updates when we get them
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
        StorageBackend::SQLite => {
            info!("Using SQLite storage backend");
            let sqlite_cfg = config
                .storage
                .sqlite
                .as_ref()
                .ok_or_else(|| eyre::eyre!("SQLite configuration is required when using SQLite backend"))?;

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
