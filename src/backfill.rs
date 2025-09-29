use reth::{
    api::FullNodeComponents, 
    providers::{DatabaseProviderFactory, StateReader, DBProvider},
};
use reth_db_api::{cursor::{DbCursorRO, DbDupCursorRO}, tables, transaction::DbTx};
use reth_tracing::tracing::{info, warn};

use crate::storage::{TrieBranchesBatch, BranchNodeEntry, ExternalStateStore};

/// Handles backfill operations for the proof helper
pub struct BackfillManager<Node, Storage>
where
    Node: FullNodeComponents,
    Node::Provider: StateReader,
    Storage: ExternalStateStore,
{
    provider: Node::Provider,
    storage: Storage,
}

impl<Node, Storage> BackfillManager<Node, Storage>
where
    Node: FullNodeComponents,
    Node::Provider: StateReader,
    Storage: ExternalStateStore,
{
    /// Create a new BackfillManager instance
    pub fn new(provider: Node::Provider, storage: Storage) -> Self {
        Self { provider, storage }
    }

    /// Backfill all leaf nodes (accounts and storage)
    pub async fn backfill_leaf_nodes(&self) -> eyre::Result<()> {
        self.backfill_hashed_accounts().await?;
        self.backfill_hashed_storage().await?;
        Ok(())
    }

    /// Backfill hashed accounts data
    async fn backfill_hashed_accounts(&self) -> eyre::Result<()> {
        let db_provider = self.provider.database_provider_ro()?;
        let tx = db_provider.tx_ref();
        let mut hashed_accounts_cursor = tx.cursor_read::<tables::HashedAccounts>()?;
        let mut account_entries = Vec::new();
        
        let mut total_accounts: u64 = 0;
        
        info!("Starting hashed accounts backfill");
        
        loop {
            let entry = hashed_accounts_cursor.next()?;

            let Some(entry) = entry else {
                break;
            };

            let hashed_account = entry.0;
            let account = entry.1;
            account_entries.push((hashed_account, Some(account)));
            total_accounts += 1;

            if total_accounts % 100000 == 0 {
                info!("Processed {} accounts", total_accounts);
            }

            if account_entries.len() >= 100000 {
                info!("Storing {} account entries, total accounts: {}", account_entries.len(), total_accounts);
                self.storage.store_hashed_accounts(account_entries, 0).await?;
                account_entries = Vec::new();
            }
        }

        if !account_entries.is_empty() {
            info!("Storing final {} account entries", account_entries.len());
            self.storage.store_hashed_accounts(account_entries, 0).await?;
        }

        info!("Hashed accounts backfill complete: {} accounts", total_accounts);
        Ok(())
    }

    /// Backfill hashed storage data
    async fn backfill_hashed_storage(&self) -> eyre::Result<()> {
        // Find last stored storage slot
        let last_stored_storage_slot = self.storage.find_last_stored_storage_slot()?;
        
        let db_provider = self.provider.database_provider_ro()?;
        let tx = db_provider.tx_ref();
        let mut hashed_storages_cursor = tx.cursor_dup_read::<tables::HashedStorages>()?;
        let mut storage_entries = Vec::new();
        let mut current_hashed_address = None;
        let mut total_storage_slots: u64 = 0;

        if let Some((hashed_address, storage_key)) = last_stored_storage_slot {
            hashed_storages_cursor.seek_by_key_subkey(hashed_address, storage_key)?;
        }

        loop {
            let entry = hashed_storages_cursor.next()?;
            let Some(entry) = entry else {
                break;
            };

            let hashed_address = entry.0;
            let storage_entry = entry.1;

            if current_hashed_address != Some(hashed_address) && !storage_entries.is_empty() && current_hashed_address.is_some() {
                self.storage.store_hashed_storages(current_hashed_address.unwrap(), storage_entries, 0).await?;
                storage_entries = Vec::new();
            }
            current_hashed_address = Some(hashed_address);

            storage_entries.push((storage_entry.key, storage_entry.value));
            total_storage_slots += 1;

            if total_storage_slots % 100000 == 0 {
                info!("Processed {} storage slots, last hashed address: {:?}", total_storage_slots, current_hashed_address);
            }

            if storage_entries.len() >= 100000 {
                self.storage.store_hashed_storages(hashed_address, storage_entries, 0).await?;
                storage_entries = Vec::new();
            }
        }

        if !storage_entries.is_empty() && current_hashed_address.is_some() {
            self.storage.store_hashed_storages(current_hashed_address.unwrap(), storage_entries, 0).await?;
        }

        info!("Hashed storage backfill complete: {} storage slots", total_storage_slots);
        Ok(())
    }

    /// Backfill accounts trie data
    pub async fn backfill_accounts_trie(&self) -> eyre::Result<()> {
        let db_provider = self.provider.database_provider_ro()?;
        let db = db_provider.tx_ref();

        // Process entries in AccountsTrie
        let mut accounts_trie_cursor = db.cursor_read::<tables::AccountsTrie>()?;

        let mut entry = accounts_trie_cursor.first()?;
        let mut batch: TrieBranchesBatch = TrieBranchesBatch::new(0);

        let mut count: u64 = 0;
        info!("Starting accounts trie backfill");

        loop {
            if let Some(entry) = entry {
                batch.items.push(BranchNodeEntry {
                    block_number: 0,
                    path: entry.0.0,
                    hashed_address: None,
                    branch: Some(entry.1),
                });
                count += 1;
            } else {
                break;
            }

            if count % 100000 == 0 {
                info!("AccountsTrie processed {} entries", count);
                self.storage.store_trie_branches(batch).await?;
                batch = TrieBranchesBatch::new(0);
            }

            entry = accounts_trie_cursor.next()?;
        }

        if !batch.items.is_empty() {
            self.storage.store_trie_branches(batch).await?;
        }

        info!("Accounts trie backfill complete: {} entries", count);
        Ok(())
    }

    /// Backfill storage trie data
    pub async fn backfill_storages_trie(&self) -> eyre::Result<()> {
        let db_provider = self.provider.database_provider_ro()?;
        let db = db_provider.tx_ref();

        // Process entries in StoragesTrie, grouped by address
        let mut hashed_storages_cursor = db.cursor_dup_read::<tables::HashedStorages>()?;
        let mut storages_trie_cursor = db.cursor_dup_read::<tables::StoragesTrie>()?;

        let mut storage_entry = hashed_storages_cursor.first()?;
        let mut batch: TrieBranchesBatch = TrieBranchesBatch::new(0);
        let mut count: u64 = 0;

        info!("Starting storage trie backfill");

        loop {
            if let Some(storage_entry) = storage_entry {
                let address = storage_entry.0;
                let mut address_count: u64 = 0;

                // Process all storage trie entries for this address
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

                    batch.items.push(BranchNodeEntry {
                        block_number: 0,
                        path: branch.nibbles.0,
                        hashed_address: Some(account),
                        branch: Some(branch.node),
                    });

                    if (count + address_count) % 100000 == 0 {
                        info!("StoragesTrie processed {} entries", count + address_count);
                        self.storage.store_trie_branches(batch).await?;
                        batch = TrieBranchesBatch::new(0);
                    }

                    address_count += 1;
                }

                if address_count > 100 {
                    info!("StoragesTrie processed {} entries for address {}", address_count, address);
                }
                    
                count += address_count;
            } else {
                break;
            }

            storage_entry = hashed_storages_cursor.next_no_dup()?;
        }

        if !batch.items.is_empty() {
            self.storage.store_trie_branches(batch).await?;
        }

        info!("Storage trie backfill complete: {} entries", count);
        Ok(())
    }

    /// Run complete backfill of all preimage data
    pub async fn backfill_preimages(&self) -> eyre::Result<()> {
        info!("Starting complete preimage backfill");
        
        self.backfill_leaf_nodes().await?;
        self.backfill_storages_trie().await?;
        self.backfill_accounts_trie().await?;
        
        info!("Complete preimage backfill finished");
        Ok(())
    }
}
