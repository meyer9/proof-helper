#![warn(clippy::future_not_send)]

use std::time::{Duration, Instant};

use alloy_primitives::B256;
use reth::primitives::{Account, StorageEntry};
use reth_db_api::{
    DatabaseError,
    cursor::{DbCursorRO, DbDupCursorRO},
    tables,
    transaction::DbTx,
};
use reth_tracing::tracing::info;
use reth_trie::{BranchNodeCompact, StorageTrieEntry, StoredNibbles, StoredNibblesSubKey};

use crate::storage::{BranchNodeEntry, ExternalStateStore, TrieBranchesBatch};

pub struct BackfillJob<'a, Tx: DbTx, S: ExternalStateStore + Send> {
    storage: S,
    tx: &'a Tx,
}

/// Macro to generate simple cursor iterators for tables
macro_rules! define_simple_cursor_iter {
    ($iter_name:ident, $table:ty, $key_type:ty, $value_type:ty) => {
        pub struct $iter_name<C>(C);

        impl<C> $iter_name<C> {
            pub fn new(cursor: C) -> Self {
                Self(cursor)
            }
        }

        impl<C: DbCursorRO<$table>> Iterator for $iter_name<C> {
            type Item = Result<($key_type, $value_type), DatabaseError>;

            fn next(&mut self) -> Option<Self::Item> {
                self.0.next().transpose()
            }
        }
    };
}

/// Macro to generate duplicate cursor iterators for tables with custom logic
macro_rules! define_dup_cursor_iter {
    ($iter_name:ident, $table:ty, $key_type:ty, $value_type:ty) => {
        pub struct $iter_name<C>(C);

        impl<C> $iter_name<C> {
            pub fn new(cursor: C) -> Self {
                Self(cursor)
            }
        }

        impl<C: DbDupCursorRO<$table> + DbCursorRO<$table>> Iterator for $iter_name<C> {
            type Item = Result<($key_type, $value_type), DatabaseError>;

            fn next(&mut self) -> Option<Self::Item> {
                // First try to get the next duplicate value
                if let Some(res) = self.0.next_dup().transpose() {
                    return Some(res);
                }

                // If no more duplicates, find the next key with values
                let Some(Ok((next_key, _))) = self.0.next_no_dup().transpose() else {
                    // If no more entries, return None
                    return None;
                };

                // If found, seek to the first duplicate for this key
                return self.0.seek(next_key).transpose();
            }
        }
    };
}

// Generate iterators for all 4 table types
define_simple_cursor_iter!(HashedAccountsIter, tables::HashedAccounts, B256, Account);
define_dup_cursor_iter!(
    HashedStoragesIter,
    tables::HashedStorages,
    B256,
    StorageEntry
);
define_simple_cursor_iter!(
    AccountsTrieIter,
    tables::AccountsTrie,
    StoredNibbles,
    BranchNodeCompact
);
define_dup_cursor_iter!(
    StoragesTrieIter,
    tables::StoragesTrie,
    B256,
    StorageTrieEntry
);

trait CompletionEstimatable {
    // Returns a progress estimate as a percentage (0.0 to 1.0)
    fn estimate_progress(&self) -> f64;
}

impl CompletionEstimatable for B256 {
    fn estimate_progress(&self) -> f64 {
        // use the first 3 bytes as a progress estimate
        let progress = self.0[..3].to_vec();
        let mut val: u64 = 0;
        for nibble in progress.iter() {
            val = (val << 8) | *nibble as u64;
        }
        val as f64 / (256u64.pow(3)) as f64
    }
}

impl CompletionEstimatable for StoredNibbles {
    fn estimate_progress(&self) -> f64 {
        // use the first 6 nibbles as a progress estimate
        let progress_nibbles = self.0.slice(0..6);
        let mut val: u64 = 0;
        for nibble in progress_nibbles.iter() {
            val = (val << 4) | nibble as u64;
        }
        val as f64 / (16u64.pow(progress_nibbles.len() as u32)) as f64
    }
}

async fn backfill<
    S: Iterator<Item = Result<(Key, Value), DatabaseError>>,
    F: Future<Output = eyre::Result<()>> + Send,
    Key: CompletionEstimatable + Clone,
    Value: Clone,
>(
    name: &str,
    source: S,
    storage_threshold: usize,
    log_threshold: usize,
    save_fn: impl Fn(Vec<(Key, Value)>) -> F,
) -> eyre::Result<u64> {
    let mut entries = Vec::new();

    let mut total_entries: u64 = 0;

    info!("Starting {} backfill", name);
    let start_time = Instant::now();

    for entry in source {
        let entry = entry?;

        entries.push(entry.clone());
        total_entries += 1;

        if total_entries % (log_threshold as u64) == 0 {
            let progress = entry.0.estimate_progress();
            let elapsed = start_time.elapsed();
            let elapsed_secs = elapsed.as_secs_f64();
            let estimated_total_time = if progress > 0.0 {
                elapsed_secs / (progress)
            } else {
                0.0
            };
            let progress_pct = progress * 100.0;
            let remaining_time = estimated_total_time - elapsed_secs;
            let eta_duration = Duration::from_secs(remaining_time as u64);
            info!(
                "Processed {} {}, progress: {progress_pct:.2}%, ETA: {}s",
                name,
                total_entries,
                eta_duration.as_secs()
            );
        }

        if entries.len() >= storage_threshold {
            info!("Storing {} entries, total entries: {}", name, total_entries);
            save_fn(entries).await?;
            entries = Vec::new();
        }
    }

    if !entries.is_empty() {
        info!("Storing final {} entries", name);
        save_fn(entries).await?;
    }

    info!("{} backfill complete: {} entries", name, total_entries);
    Ok(total_entries)
}

impl<'a, Tx: DbTx, S: ExternalStateStore + Send> BackfillJob<'a, Tx, S> {
    pub fn new(storage: S, tx: &'a Tx) -> Self {
        Self { storage, tx }
    }

    /// Backfill all leaf nodes (accounts and storage)
    async fn backfill_leaf_nodes(&self) -> eyre::Result<()> {
        self.backfill_hashed_accounts().await?;
        self.backfill_hashed_storage().await?;
        Ok(())
    }

    /// Backfill hashed accounts data
    async fn backfill_hashed_accounts(&self) -> eyre::Result<()> {
        let mut start_cursor = self.tx.cursor_read::<tables::HashedAccounts>()?;

        let last_account_leaf = self.storage.get_last_account_leaf()?;
        if let Some(last_account_leaf) = last_account_leaf {
            start_cursor.seek(last_account_leaf)?;
        }

        let source = HashedAccountsIter::new(start_cursor);
        let storage_threshold = 100000;
        let log_threshold = 100000;
        let save_fn = async |entries: Vec<(B256, Account)>| -> eyre::Result<()> {
            self.storage
                .store_hashed_accounts(
                    entries
                        .into_iter()
                        .map(|(address, account)| (address, Some(account)))
                        .collect(),
                    0,
                )
                .await?;
            Ok(())
        };

        backfill(
            "hashed accounts",
            source,
            storage_threshold,
            log_threshold,
            save_fn,
        )
        .await?;

        Ok(())
    }

    /// Backfill hashed storage data
    async fn backfill_hashed_storage(&self) -> eyre::Result<()> {
        let mut start_cursor = self.tx.cursor_dup_read::<tables::HashedStorages>()?;

        let last_storage_leaf = self.storage.get_last_storage_leaf()?;
        if let Some((hashed_address, storage_key)) = last_storage_leaf {
            start_cursor.seek_by_key_subkey(hashed_address, storage_key)?;
        }

        let source = HashedStoragesIter::new(start_cursor);
        let storage_threshold = 100000;
        let log_threshold = 100000;
        let save_fn = async |entries: Vec<(B256, StorageEntry)>| -> eyre::Result<()> {
            // Group entries by hashed address
            self.storage.store_hashed_storages(entries, 0).await?;
            Ok(())
        };

        backfill(
            "hashed storage",
            source,
            storage_threshold,
            log_threshold,
            save_fn,
        )
        .await?;

        Ok(())
    }

    /// Backfill accounts trie data
    async fn backfill_accounts_trie(&self) -> eyre::Result<()> {
        let mut start_cursor = self.tx.cursor_read::<tables::AccountsTrie>()?;

        let last_account_branch = self.storage.get_last_account_branch()?;
        if let Some(last_account_branch) = last_account_branch {
            start_cursor.seek(StoredNibbles(last_account_branch))?;
        }

        let source = AccountsTrieIter::new(start_cursor);
        let storage_threshold = 100000;
        let log_threshold = 100000;
        let save_fn = async |entries: Vec<(StoredNibbles, BranchNodeCompact)>| -> eyre::Result<()> {
            self.storage
                .store_trie_branches(TrieBranchesBatch {
                    block_number: 0,
                    items: entries
                        .into_iter()
                        .map(|(path, branch)| BranchNodeEntry {
                            path: path.0,
                            hashed_address: None,
                            branch: Some(branch),
                        })
                        .collect(),
                })
                .await?;
            Ok(())
        };

        backfill(
            "accounts trie",
            source,
            storage_threshold,
            log_threshold,
            save_fn,
        )
        .await?;

        Ok(())
    }

    /// Backfill storage trie data
    pub async fn backfill_storages_trie(&self) -> eyre::Result<()> {
        let mut start_cursor = self.tx.cursor_dup_read::<tables::StoragesTrie>()?;

        let last_storage_branch = self.storage.get_last_storage_branch()?;
        if let Some((hashed_address, nibbles)) = last_storage_branch {
            info!("Seeking to {:?}", (hashed_address, nibbles));
            start_cursor.seek_by_key_subkey(hashed_address, StoredNibblesSubKey(nibbles))?;
        }

        let source = StoragesTrieIter::new(start_cursor);
        let storage_threshold = 100000;
        let log_threshold = 100000;
        let save_fn = async |entries: Vec<(B256, StorageTrieEntry)>| -> eyre::Result<()> {
            self.storage
                .store_trie_branches(TrieBranchesBatch {
                    block_number: 0,
                    items: entries
                        .into_iter()
                        .map(|(hashed_address, storage_entry)| BranchNodeEntry {
                            path: storage_entry.nibbles.0,
                            hashed_address: Some(hashed_address),
                            branch: Some(storage_entry.node),
                        })
                        .collect(),
                })
                .await?;
            Ok(())
        };

        backfill(
            "storage trie",
            source,
            storage_threshold,
            log_threshold,
            save_fn,
        )
        .await?;

        Ok(())
    }

    /// Run complete backfill of all preimage data
    async fn backfill_trie(&self) -> eyre::Result<()> {
        self.backfill_leaf_nodes().await?;
        self.backfill_storages_trie().await?;
        self.backfill_accounts_trie().await?;

        Ok(())
    }

    pub async fn run(&self, best_number: u64, best_hash: B256) -> eyre::Result<()> {
        if self.storage.get_earliest_block_number().await? == None {
            self.backfill_trie().await?;

            self.storage
                .set_earliest_block_number(best_number, best_hash)
                .await?;
        }
        Ok(())
    }
}
