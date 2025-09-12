use futures_util::TryStreamExt;
use op_reth::node::OpNode;
use reth::{
    api::{FullNodeComponents, NodePrimitives}, builder::NodeTypes, chainspec::ChainInfo, core::primitives::AlloyBlockHeader, primitives::RecoveredBlock, providers::{BlockNumReader, DBProvider, DatabaseProviderFactory, StateReader}, revm::primitives::{map::FbBuildHasher, FixedBytes, HashMap}
};

use reth_db_api::{cursor::{DbCursorRO, DbDupCursorRO}, tables, transaction::DbTx};
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_tracing::tracing::{info, warn};
use reth_trie::{updates::{StorageTrieUpdates, TrieUpdates}, StoredNibbles};
use std::sync::Arc;

mod config;
mod sqlite;
mod storage;
mod rpc;

use config::{ProofHelperConfig, StorageBackend};
use sqlite::SqlitePreimageStore;
use storage::{PreimageBatch, PreimageEntry, PreimageStore};

use crate::rpc::{EthApiExt, EthApiOverrideServer};

/// Proof Helper ExEx - processes blocks and tracks state changes
pub struct ProofHelper<Node>
where
    Node: FullNodeComponents,
    Node::Provider: StateReader,
{
    ctx: ExExContext<Node>,
    storage: Arc<dyn PreimageStore>,
}

impl<Node, Primitives> ProofHelper<Node>
where
    Node: FullNodeComponents<Types: NodeTypes<Primitives = Primitives>>,
    Primitives: NodePrimitives,
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
                    branch: Some(entry.1),
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
                        branch: Some(branch.node),
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
        let ChainInfo { best_number, best_hash } = self.ctx.provider().chain_info().unwrap();
        self.storage.set_earliest_block_number(best_number, best_hash).await?;
        Ok(())
    }

    async fn write_storage_trie_updates(&self, account_storage_updates: &HashMap<FixedBytes<32>, StorageTrieUpdates, FbBuildHasher<32>>, block_number: u64) -> eyre::Result<u64> {
        let mut num_entries = 0;
        let mut preimage_batch = PreimageBatch::new(block_number);
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
                updates.storage_nodes_ref().iter().map(|(nibbles, node)| (nibbles, Some(node))),
            );
            for (nibbles, maybe_updated) in storage_updates.into_iter().filter(|(n, _)| !n.is_empty()) {
                num_entries += 1;
                preimage_batch.items.push(PreimageEntry {
                    block_number,
                    path: StoredNibbles(*nibbles),
                    hashed_address: Some(*hashed_address),
                    branch: maybe_updated.map(|node| node.clone()),
                });
            }
        }

        self.storage.store_preimages_batch(preimage_batch).await?;

        Ok(num_entries)
    }

    async fn write_trie_updates(&self, trie_updates: &TrieUpdates, block: &RecoveredBlock<Primitives::Block>) -> eyre::Result<u64> {
        // if no earliest block is set, start a backfill job
        let earliest_block = self.storage.get_earliest_block_number().await?;
        if earliest_block.is_none() {
            self.backfill_preimages().await?;
        }

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
            trie_updates.account_nodes_ref().iter().map(|(nibbles, node)| (nibbles, Some(node))),
        );
        // Sort trie node updates.
        account_updates.sort_unstable_by(|a, b| a.0.cmp(b.0));

        let mut batch_to_store = PreimageBatch::new(block.number());

        for (key, updated_node) in account_updates {
            let nibbles = StoredNibbles(*key);
            match updated_node {
                Some(node) => {
                    if !nibbles.0.is_empty() {
                        num_entries += 1;
                        batch_to_store.items.push(PreimageEntry {
                            block_number: block.number(),
                            path: nibbles,
                            hashed_address: None,
                            branch: Some(node.clone()),
                        });
                    }
                }
                None => {
                    num_entries += 1;
                    batch_to_store.items.push(PreimageEntry {
                        block_number: block.number(),
                        path: nibbles,
                        hashed_address: None,
                        branch: None,
                    });
                }
            }
        }

        self.storage.store_preimages_batch(batch_to_store).await?;
        num_entries += self.write_storage_trie_updates(trie_updates.storage_tries_ref(), block.number()).await?;

        Ok(num_entries)
    }

    /// Main execution loop for the ExEx
    pub async fn run(mut self, _config: ProofHelperConfig) -> eyre::Result<()> {
        if self.storage.get_earliest_block_number().await? == None {
            self.backfill_preimages().await?;
        }

        while let Some(notification) = self.ctx.notifications.try_next().await? {
            match &notification {
                ExExNotification::ChainCommitted { new } => {
                    if let Some(trie_updates) = new.trie_updates() {
                        self.write_trie_updates(trie_updates, new.tip()).await?;
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
                let api_ext = EthApiExt::new(ctx.registry.eth_api().clone(), storage_2);
                ctx.modules.replace_configured(api_ext.into_rpc())?;
                Ok(()) 
            })
            .launch()
            .await?;

        handle.wait_for_node_exit().await
    })
}
