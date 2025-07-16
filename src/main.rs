use futures_util::TryStreamExt;
use op_reth::node::OpNode;
use op_reth::primitives::OpPrimitives;
use reth::{
    api::{Block, ConfigureEvm, FullNodeComponents, NodePrimitives}, builder::NodeTypes, core::primitives::AlloyBlockHeader, primitives::RecoveredBlock, providers::{BlockReader, StateProviderFactory, StateReader}, revm::{database::StateProviderDatabase, primitives::{keccak256, B256}, witness::ExecutionWitnessRecord, State}
};
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_stateless::ExecutionWitness;
use reth_tracing::tracing::{info, warn};
use reth_trie::{BranchNodeCompact, Nibbles, StoredNibbles, TrieNode};
use reth_db_api::table::Encode;
use reth_trie_db::MerklePatriciaTrie;
use reth_evm::execute::Executor;
use std::sync::Arc;
use alloy_rlp::Decodable;
use std::collections::HashMap;

mod storage;
mod config;
mod dynamodb;

use storage::{PreimageStore, MockPreimageStore};
use config::{ProofHelperConfig, StorageBackend};
use dynamodb::DynamoDbPreimageStore;

/// Proof Helper ExEx - processes blocks and tracks state changes
pub struct ProofHelper<Node>
where
    Node: FullNodeComponents<
        Types: NodeTypes<Primitives = OpPrimitives, StateCommitment = MerklePatriciaTrie>,
    >,
    Node::Provider: StateReader,
{
    ctx: ExExContext<Node>,
    storage: Arc<dyn PreimageStore>,
}

impl<Node> ProofHelper<Node>
where
    Node: FullNodeComponents<
        Types: NodeTypes<Primitives = OpPrimitives, StateCommitment = MerklePatriciaTrie>,
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
        let state_provider = self.ctx.provider().state_by_block_id(block.header().parent_hash().into())?;

        let parent_state_root = self.ctx.provider().block_by_hash(block.header().parent_hash().into())?.ok_or_else(|| eyre::eyre!("Parent block not found"))?.header().state_root();
    
        let db = StateProviderDatabase::new(&state_provider);
        let block_executor = self.ctx.evm_config().batch_executor(db);
    
        let mut witness_record = ExecutionWitnessRecord::default();
    
        let execution_result = block_executor
            .execute_with_state_closure(&(*block).clone(), |statedb: &State<_>| {
                witness_record.record_executed_state(statedb);
            })
            .map_err(|err| eyre::eyre!(err))?;


    
        let ExecutionWitnessRecord {
            hashed_state,
            codes,
            keys,
            ..
        } = witness_record;

        let (state_root, updates) = state_provider.state_root_with_updates(hashed_state.clone())?;
    
        let state = state_provider
            .witness(Default::default(), hashed_state)
            .map_err(|err| eyre::eyre!(err))?;


        // Store witness data - for PR #1 we'll store raw data, proper conversion comes in PR #2
        let mut batch = storage::PreimageBatch::new(block_number);
        let mut stored_count = 0;
    
        // Store state witness data
        for (_index, state_data) in state.iter().enumerate() {
            let hash = keccak256(state_data);
            batch.add_preimage(hash, state_data.to_vec());
            stored_count += 1;
            
            if stored_count % progress_interval == 0 {
                info!("Stored {} state preimages for block {}", stored_count, block_number);
            }
        }

        for (_index, code_data) in codes.iter().enumerate() {
            let hash = keccak256(code_data);
            batch.add_preimage(hash, code_data.to_vec());
            stored_count += 1;
            
            if stored_count % progress_interval == 0 {
                info!("Stored {} code preimages for block {}", stored_count, block_number);
            }
        }

        for (_index, key_data) in keys.iter().enumerate() {
            let hash = keccak256(key_data);
            batch.add_preimage(hash, key_data.to_vec());
            stored_count += 1;

            if stored_count % progress_interval == 0 {
                info!("Stored {} key preimages for block {}", stored_count, block_number);
            }
        }

        let state_map = state.iter().map(|data| (keccak256(data), data.to_vec())).collect::<HashMap<_, _>>();

        let mut branches_to_fetch = updates.removed_nodes.iter().cloned().collect::<Vec<_>>();
        info!("Branches to fetch: {:?}", branches_to_fetch.len());

        while let Some(nibbles) = branches_to_fetch.pop() {
            info!("Fetching branch node at {:?}", nibbles);
            // traverse from the state root to the nibbles path
            let mut current_path = nibbles.clone();
            let mut current_node_hash = B256::from(parent_state_root);

            let mut found = true;
            let mut idx = 0;
            // TODO: do we pop or shift?
            while let Some(nibble) = current_path.get(idx) {
                info!("Current node hash: {:?}, current path: {:?}", current_node_hash, current_path);
                let Some(node) = state_map.get(&current_node_hash) else {
                    warn!("Node {:?} not found in state map", current_node_hash);
                    found = false;
                    break;
                };
                let node = TrieNode::decode(&mut node.as_slice())?;

                match node {
                    TrieNode::Branch(branch) => {
                        let child_hash = branch.stack[nibble as usize].as_hash();
                        if let Some(child_hash) = child_hash {
                            current_node_hash = child_hash;
                            idx += 1;
                        } else {
                            warn!("Child hash not found for path {:?}", current_path);
                            found = false;
                            break;
                        }
                    }
                    _ => {
                        warn!("Non-branch node found at path {:?}", current_path);
                        found = false;
                        break;
                    }
                }
            }

            if found {
                info!("Found branch node at {:?}, value: {:?}", current_node_hash, state_map.get(&current_node_hash));
            } else {
                info!("Branch node not found at {:?}", nibbles);
            }
        }
    
        info!("Processing block {} with {} state nodes, {} code nodes, {} key nodes", 
              block_number, state.len(), codes.len(), keys.len());

        // print state changes and required preimages
        for (index, state_data) in state.iter().enumerate() {
            info!("State change {} - {}", index, state_data);
        }

        for (index, code_data) in codes.iter().enumerate() {
            info!("Code change {} - {}", index, code_data);
        }

        for (index, key_data) in keys.iter().enumerate() {
            info!("Key change {} - {}", index, key_data);
        }
    
    
        // Store the batch
        let storage_clone = Arc::clone(&self.storage);
        tokio::spawn(async move {
            if let Err(e) = storage_clone.store_preimages_batch(batch).await {
                warn!("Failed to store preimages for block {}: {}", block_number, e);
            } else {
                info!("Successfully stored {} preimages for block {}", stored_count, block_number);
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
                            warn!("Block {} is too far behind the head block {}, skipping", block_number, head_block_number);
                            continue;
                        }

                        if let Err(err) = self.process_block(block, progress_interval) {
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
            let dynamodb_config = config.storage.dynamodb.as_ref()
                .ok_or_else(|| eyre::eyre!("DynamoDB configuration is required when using DynamoDB backend"))?;
            
            let store = DynamoDbPreimageStore::new(dynamodb_config).await
                .map_err(|e| eyre::eyre!("Failed to create DynamoDB store: {}", e))?;
            
            // Run health check to ensure connection is working
            store.health_check().await
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
                let storage = create_storage(&config).await
                    .map_err(|e| eyre::eyre!("Failed to create storage backend: {}", e))?;

                
                
                let proof_helper = ProofHelper::new(ctx, storage);
                Ok(proof_helper.run(config))
            })
            .launch()
            .await?;

        handle.wait_for_node_exit().await
    })
}
