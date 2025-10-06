use crate::{
    BranchNodeEntry, ExternalStateStore, TrieBranchesBatch,
    provider::ExternalOverlayStateProviderRef,
};
use alloy_primitives::{FixedBytes, map::FbBuildHasher};
use itertools::Itertools;
use reth::{
    api::{FullNodeComponents, NodePrimitives},
    builder::NodeTypes,
    core::primitives::AlloyBlockHeader,
    primitives::{RecoveredBlock, StorageEntry},
    providers::{DatabaseProviderFactory, StateProviderFactory, StateReader, StateRootProvider},
    revm::database::StateProviderDatabase,
    rpc::types::BlockId,
};
use reth_evm::{ConfigureEvm, execute::Executor};
use reth_trie::{
    HashedPostState, KeccakKeyHasher,
    updates::{StorageTrieUpdates, TrieUpdates},
};
use std::collections::HashMap;
use tracing::{debug, info};

pub struct LiveTrieCollector<Node, PreimageStore>
where
    Node: FullNodeComponents,
    Node::Provider: StateReader + DatabaseProviderFactory + StateProviderFactory,
{
    evm_config: Node::Evm,
    provider: Node::Provider,
    storage: PreimageStore,
}

impl<Node, Store> LiveTrieCollector<Node, Store>
where
    Node: FullNodeComponents,
    Store: ExternalStateStore + Clone + 'static,
{
    /// Create a new ProofHelper instance
    pub fn new(evm_config: Node::Evm, provider: Node::Provider, storage: Store) -> Self {
        Self {
            evm_config,
            provider,
            storage,
        }
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
        block_number: u64,
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

        let mut batch_to_store = TrieBranchesBatch::new(block_number);

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
            .write_storage_trie_updates(trie_updates.storage_tries_ref(), block_number)
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

    pub async fn execute_and_store_block_updates(
        &self,
        block: &RecoveredBlock<<<Node::Types as NodeTypes>::Primitives as NodePrimitives>::Block>,
    ) -> eyre::Result<()> {
        // ensure that we have the state of the parent block
        let (Some((earliest, _)), latest) = (
            self.storage.get_earliest_block_number().await?,
            self.storage.get_latest_block_number().await?,
        ) else {
            return Err(eyre::eyre!("No blocks stored"));
        };

        let parent_block_number = block.number() - 1;
        if parent_block_number < earliest {
            return Err(eyre::eyre!(
                "Parent block number is less than earliest stored block number"
            ));
        }

        if parent_block_number > latest {
            return Err(eyre::eyre!(
                "Cannot execute block updates for block {} without parent state {} (latest stored block number: {})",
                block.number(),
                parent_block_number,
                latest
            ));
        }

        let block_number = block.number();

        let print_sorted_post_state = |hashed_state: HashedPostState| {
            for (address, account) in hashed_state
                .accounts
                .iter()
                .sorted_by_key(|(address, _)| *address)
            {
                info!("Account: {:?}: {:?}", address, account);
            }
            for (address, storage) in hashed_state
                .storages
                .iter()
                .sorted_by_key(|(address, _)| *address)
            {
                info!("Storage: {:?}", address);
                info!("wiped?: {}", storage.wiped);
                for (slot, value) in storage.storage.iter().sorted_by_key(|(slot, _)| *slot) {
                    info!("Slot: {:?}: {:?}", slot, value);
                }
            }
        };

        // TODO: should we check block hash here?

        let state_provider = ExternalOverlayStateProviderRef::new(
            self.provider.latest()?,
            self.storage.clone(),
            self.provider.database_provider_ro()?,
            parent_block_number,
        );

        // let historical_state_provider = self
        //     .provider
        //     .state_by_block_id(BlockId::Number(parent_block_number.into()))?;

        // info!("--- HISTORICAL POST STATE ---");
        // let historical_state_db = StateProviderDatabase::new(&historical_state_provider);
        // let historical_block_executor = self.evm_config.batch_executor(historical_state_db);

        // let historical_execution_result = historical_block_executor
        //     .execute(&(*block).clone())
        //     .map_err(|err| eyre::eyre!(err))?;

        // let historical_hashed_state = HashedPostState::from_bundle_state::<KeccakKeyHasher>(
        //     historical_execution_result.state.state(),
        // );

        // print_sorted_post_state(historical_hashed_state.clone());
        // historical_state_provider.state_root_with_updates(historical_hashed_state.clone())?;

        info!("--- EXTERNAL POST STATE ---");

        let db = StateProviderDatabase::new(&state_provider);
        let block_executor = self.evm_config.batch_executor(db);

        let execution_result = block_executor
            .execute(&(*block).clone())
            .map_err(|err| eyre::eyre!(err))?;

        let hashed_state =
            HashedPostState::from_bundle_state::<KeccakKeyHasher>(execution_result.state.state());
        print_sorted_post_state(hashed_state.clone());
        let (state_root, trie_updates) =
            state_provider.state_root_with_updates(hashed_state.clone())?;

        info!("--- END EXTERNAL POST STATE ---");
        if state_root != block.state_root() {
            return Err(eyre::eyre!(
                "State root mismatch for block {} (have: {}, expected: {})",
                block.number(),
                state_root,
                block.state_root()
            ));
        }

        let num_trie_updates = self.write_trie_updates(&trie_updates, block_number).await?;
        let num_leaf_updates = self.write_leaf_updates(hashed_state, block_number).await?;

        info!(
            "Stored {} trie updates and {} leaf updates for block {}",
            num_trie_updates, num_leaf_updates, block_number
        );

        Ok(())
    }
}
