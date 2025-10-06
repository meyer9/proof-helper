#![warn(clippy::future_not_send)]
use futures_util::TryStreamExt;
use op_reth::node::OpNode;
use reth::{
    api::{FullNodeComponents, NodePrimitives},
    builder::NodeTypes,
    chainspec::ChainInfo,
    core::primitives::AlloyBlockHeader,
    providers::{
        BlockNumReader, BlockReader, DBProvider, DatabaseProviderFactory, StateReader,
        TransactionVariant,
    },
};

use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use std::sync::Arc;
use tracing::info;

mod backfill;
mod config;
mod live;
mod proof;
mod provider;
mod rpc;
mod sqlite;
mod storage;

use config::{ProofHelperConfig, StorageBackend};
use sqlite::SqlitePreimageStore;
use storage::{BranchNodeEntry, ExternalStateStore, TrieBranchesBatch};

use crate::{
    backfill::BackfillJob,
    live::LiveTrieCollector,
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

impl<Node, Primitives, PreimageStore> ProofHelper<Node, PreimageStore>
where
    Node: FullNodeComponents<Types: NodeTypes<Primitives = Primitives>>,
    Primitives: NodePrimitives,
    PreimageStore: ExternalStateStore + Clone + 'static,
{
    /// Create a new ProofHelper instance
    pub fn new(ctx: ExExContext<Node>, storage: PreimageStore) -> Self {
        Self { ctx, storage }
    }

    /// Main execution loop for the ExEx
    pub async fn run(mut self, _config: ProofHelperConfig) -> eyre::Result<()> {
        // Run the earliest block job (idempotent)
        let db_provider = self
            .ctx
            .provider()
            .database_provider_ro()?
            .disable_long_read_transaction_safety();
        let db_tx = db_provider.into_tx();
        let ChainInfo {
            best_number,
            best_hash,
        } = self.ctx.provider().chain_info()?;
        BackfillJob::new(self.storage.clone(), &db_tx)
            .run(best_number, best_hash)
            .await?;

        let collector = LiveTrieCollector::<Node, PreimageStore>::new(
            self.ctx.evm_config().clone(),
            self.ctx.provider().clone(),
            self.storage.clone(),
        );

        // TODO: we should disallow processing blocks until the backfill job is complete

        // check if we can process up to the latest block
        let latest_stored_block_number = self.storage.get_latest_block_number().await?;
        let ChainInfo {
            best_number: latest_block_number,
            ..
        } = self.ctx.provider().chain_info()?;

        if latest_stored_block_number < latest_block_number {
            info!(
                "Backfilling blocks from {} to {}",
                latest_stored_block_number, latest_block_number
            );
            for block_number in (latest_stored_block_number + 1)..=latest_block_number {
                let Some(block) = self
                    .ctx
                    .provider()
                    .recovered_block(block_number.into(), TransactionVariant::NoHash)?
                else {
                    return Err(eyre::eyre!("Block {} not found", block_number));
                };
                collector.execute_and_store_block_updates(&block).await?;
            }
        } else {
            info!(
                "Skipping backfill, latest stored block number is up to date (latest stored: {}, latest: {})",
                latest_stored_block_number, latest_block_number
            );
        }

        while let Some(notification) = self.ctx.notifications.try_next().await? {
            match &notification {
                ExExNotification::ChainCommitted { new } => {
                    let latest_stored_block_number = self.storage.get_latest_block_number().await?;
                    if new.tip().number() <= latest_stored_block_number {
                        continue;
                    }
                    for block_number in (latest_stored_block_number + 1)..=new.tip().number() {
                        let block = new.blocks().get(&block_number).unwrap();

                        // By this point, we know that the parent block is stored
                        collector.execute_and_store_block_updates(block).await?;
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
