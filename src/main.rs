use futures_util::TryStreamExt;
use op_reth::node::OpNode;
use op_reth::primitives::OpPrimitives;
use reth::{
    api::{Block, ConfigureEvm, FullNodeComponents, NodePrimitives}, builder::NodeTypes, core::primitives::AlloyBlockHeader, primitives::RecoveredBlock, providers::{BlockReader, StateProviderFactory, StateReader}, revm::{database::StateProviderDatabase, primitives::{keccak256, B256, KECCAK_EMPTY}, state::AccountInfo, witness::ExecutionWitnessRecord, State}
};
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_stateless::ExecutionWitness;
use reth_tracing::tracing::{info, warn, trace};
use reth_trie::{BranchNodeCompact, HashedStorage, LeafNode, Nibbles, StoredNibbles, StoredNibblesSubKey, TrieAccount, TrieNode};
use reth_db_api::table::Encode;
use reth_trie_db::MerklePatriciaTrie;
use reth_evm::execute::Executor;
use std::sync::Arc;
use alloy_rlp::{Decodable,Encodable};
use std::collections::HashMap;
use std::collections::VecDeque;

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
        let block_executor = self.ctx.evm_config().batch_executor(db.clone());
    
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
            .witness(Default::default(), hashed_state.clone())
            .map_err(|err| eyre::eyre!(err))?;


        // Store witness data - for PR #1 we'll store raw data, proper conversion comes in PR #2
        let mut batch = storage::PreimageBatch::new(block_number);
        let mut stored_count = 0;

        // Get previous leaf nodes of updated accounts


        // Store state witness data
        for (_index, state_data) in state.iter().enumerate() {
            let hash = keccak256(state_data);
            batch.add_preimage(hash, state_data.to_vec());
            stored_count += 1;
            
            if stored_count % progress_interval == 0 {
                trace!("Stored {} state preimages for block {}", stored_count, block_number);
            }
        }

        for (_index, code_data) in codes.iter().enumerate() {
            let hash = keccak256(code_data);
            batch.add_preimage(hash, code_data.to_vec());
            stored_count += 1;
            
            if stored_count % progress_interval == 0 {
                trace!("Stored {} code preimages for block {}", stored_count, block_number);
            }
        }

        for (_index, key_data) in keys.iter().enumerate() {
            let hash = keccak256(key_data);
            batch.add_preimage(hash, key_data.to_vec());
            stored_count += 1;

            if stored_count % progress_interval == 0 {
                trace!("Stored {} key preimages for block {}", stored_count, block_number);
            }
        }

        let state_map = state.iter().chain(codes.iter()).chain(keys.iter()).map(|data| (keccak256(data), data.to_vec())).collect::<HashMap<_, _>>();

        let mut branches_to_fetch = updates.removed_nodes.iter().cloned().collect::<Vec<_>>();
        info!("Branches to fetch: {:?}", branches_to_fetch.len());

        let mut deleted_branch_keys: Vec<_> = Vec::new();

        while let Some(nibbles) = branches_to_fetch.pop() {
            trace!("Fetching branch node at {:?}", nibbles);
            // traverse from the state root to the nibbles path
            let mut current_path = nibbles.clone();
            let mut current_node_hash = B256::from(parent_state_root);

            let mut found = true;
            let mut idx = 0;
            while let Some(nibble) = current_path.get(idx) {
                trace!("Current node hash: {:?}, current path: {:?}", current_node_hash, current_path);
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
                    TrieNode::Extension(extension) => {
                        let child_hash = extension.child.as_hash();
                        if let Some(child_hash) = child_hash {
                            current_node_hash = child_hash;
                            idx += extension.key.len();
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
                trace!("Found branch node at {:?}", current_node_hash);
                deleted_branch_keys.push(current_node_hash);
            } else {
                warn!("Branch node not found at {:?}", nibbles);
            }
        }

        let mut branch_nodes = HashMap::new();

        // starting at prev state root, traverse the state map and find all the branch nodes
        let mut queue = VecDeque::new();
        queue.push_back((Nibbles::new(), parent_state_root));

        while let Some((nibbles, node_hash)) = queue.pop_front() {
            let Some(node) = state_map.get(&node_hash) else {
                // warn!("Node {:?} not found in state map", node_hash);
                continue;
            };
            let node = TrieNode::decode(&mut node.as_slice())?;

            match node {
                TrieNode::Branch(branch) => {
                    branch_nodes.insert(nibbles, node_hash);
                    let mut idx = 0;
                    for bit in 0..15 {
                        if branch.state_mask.is_bit_set(bit) {
                            let mut new_nibbles = nibbles.clone();
                            new_nibbles.push(bit as u8);
                            queue.push_back((new_nibbles, branch.stack[idx].as_hash().unwrap()));
                            idx += 1;
                        }
                    }
                }
                TrieNode::Extension(extension) => {
                    if let Some(child_hash) = extension.child.as_hash() {
                        let mut new_nibbles = nibbles.clone();
                        new_nibbles.extend(&extension.key);
                        branch_nodes.insert(new_nibbles, node_hash);
                        queue.push_back((new_nibbles, child_hash));
                    }
                }
                _ => {}
            }
        }

        deleted_branch_keys.push(parent_state_root);

        let mut changed_accounts = Vec::new();
        for (address, account) in execution_result.state.state.iter() {
            let account = account.info.as_ref().unwrap();
            let account_no_code = AccountInfo {
                nonce: account.nonce,
                balance: account.balance,
                code_hash: account.code_hash,
                code: None,
            };
            let prev_value = db.basic_account(address)?.map(|prev_value| AccountInfo {
                nonce: prev_value.nonce,
                balance: prev_value.balance,
                code_hash: prev_value.bytecode_hash.unwrap_or(KECCAK_EMPTY),
                code: None,
            });

            let changed = prev_value.as_ref().is_none_or(|prev_value| prev_value != &account_no_code);


            if let Some(prev_value) = prev_value {
                let prev_storage_root = db.storage_root(*address, HashedStorage::new(false))?;
                let new_hashed_storage = hashed_state.storages.get(&keccak256(address));
                let new_storage_root = new_hashed_storage.map(|new_hashed_storage| db.storage_root(*address, new_hashed_storage.clone())).transpose()?;

                let new_storage_root = new_storage_root.unwrap_or(B256::ZERO);

                // find the longest common prefix of the address and updates.removed_nodes (branches that were deleted)
                info!("Account {:?} changed from {:?} to {:?}, prev storage root: {:?}, new storage root: {:?}, changed: {:?}", address, prev_value, account_no_code, prev_storage_root, new_storage_root, changed);
                changed_accounts.push((address, prev_value, account_no_code, prev_storage_root, new_storage_root, changed));
            }
        }

        // if an account key was changed, we can find the longest common prefix of the storage key (keccak256(address)) and the deleted branch nibbles
        // this will give us the previous value of the storage key (rest_nibbles, value)

        let prev_account_keys = changed_accounts.iter().map(|(address, prev_value, account_no_code, prev_storage_root, new_storage_root, changed)| {
            let trie_key = Nibbles::unpack(keccak256(address));
            
            // out of updates.removed_nodes that match the trie_key, find the longest one
            let longest_match = branch_nodes.keys().filter(|removed_node| {
                let mut found = true;
                let mut idx = 0;
                while idx < removed_node.len() {
                    if trie_key.get(idx) == Some(removed_node.get_unchecked(idx)) {
                        idx += 1;
                    } else {
                        found = false;
                        break;
                    }
                }
                found
            }).max_by_key(|removed_node| removed_node.len());

            let Some(longest_match) = longest_match else {
                return None;
            };

            for (key, _) in branch_nodes.iter() {
                info!("Branch node: {:?}", key);
            }

            let deleted_trie_node = branch_nodes.get(longest_match).map(|data| state_map.get(data)).flatten();
            let deleted_trie_node = deleted_trie_node.map(|data| TrieNode::decode(&mut data.as_slice())).transpose().map_err(|err| eyre::eyre!(err)).ok()?;

            let child_subkey_prefix_len = match deleted_trie_node {
                Some(TrieNode::Branch(branch)) => {
                    info!("Branch node found at {:?}", longest_match);
                    longest_match.len() + 1
                }
                Some(TrieNode::Extension(extension)) => {
                    info!("Extension node found at {:?}", longest_match);
                    longest_match.len() + extension.key.len()
                }
                _ => {
                    assert!(false, "Non-branch or extension node found in state map");
                    0
                }
            };

            let rest_nibbles = trie_key.slice_unchecked(child_subkey_prefix_len, trie_key.len());

            let prev_account = TrieAccount {
                nonce: prev_value.nonce,
                balance: prev_value.balance,
                code_hash: prev_value.code_hash,
                storage_root: *prev_storage_root,
            };

            let leaf_node = LeafNode {
                key: rest_nibbles,
                value: alloy_rlp::encode(prev_account),
            };

            let prev_encoded = alloy_rlp::encode(leaf_node);

            Some((rest_nibbles, prev_encoded))
        }).filter_map(|result| result).collect::<Vec<_>>();

        for (rest_nibbles, prev_encoded) in prev_account_keys.clone() {
            info!("[PREV] Rest nibbles: {:?}, prev encoded: {:?}, hash: {:?}", rest_nibbles, hex::encode(&prev_encoded), keccak256(&prev_encoded));
        }

        for (address_hash, _account_info) in hashed_state.accounts {
            info!("[POST] Address hash: {:?}", address_hash);
        }
    
        info!("Processing block {} with +{} state nodes, -{} deleted account branches, -{} deleted account leaves, -? deleted storage branches, -? deleted storage leaves", 
              block_number, state.len(), deleted_branch_keys.len(), prev_account_keys.len());

        info!("Block state root: {:?}", parent_state_root);
        info!("Post state root: {:?}", state_root);

        for (key, value) in state_map.iter() {
            info!("[POST] State key: {:?}, value: {:?}", key, hex::encode(value));
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
