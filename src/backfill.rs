#![warn(clippy::future_not_send)]

use alloy_primitives::B256;
use reth::{
    chainspec::ChainInfo,
    primitives::{Account, StorageEntry},
    providers::{DBProvider, DatabaseProviderFactory, StateProviderFactory},
};
use reth_db_api::{
    DatabaseError,
    cursor::{DbCursorRO, DbDupCursorRO},
    tables,
    transaction::DbTx,
};
use reth_tracing::tracing::info;
use reth_trie::{BranchNodeCompact, StorageTrieEntry, StoredNibbles, StoredNibblesSubKey};

use crate::storage::{BranchNodeEntry, ExternalStateStore, TrieBranchesBatch};

pub enum BackfillOperation {
    SetTargetBlockNumber { target_block_number: u64 },
}

pub struct BackfillJob<P, S> {
    // static for now, but eventually we will add pruning
    target_block_number: Option<u64>,
    storage: S,
    provider: P,
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

async fn backfill<
    S: Iterator<Item = Result<Item, DatabaseError>>,
    Item,
    F: Future<Output = eyre::Result<()>> + Send,
>(
    name: &str,
    source: S,
    storage_threshold: usize,
    log_threshold: usize,
    save_fn: impl Fn(Vec<Item>) -> F,
) -> eyre::Result<u64> {
    let mut entries = Vec::new();

    let mut total_entries: u64 = 0;

    info!("Starting {} backfill", name);

    for entry in source {
        let entry = entry?;

        entries.push(entry);
        total_entries += 1;

        if total_entries % (log_threshold as u64) == 0 {
            info!("Processed {} {}", name, total_entries);
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

impl<P: StateProviderFactory + DatabaseProviderFactory + Send, S: ExternalStateStore + Send>
    BackfillJob<P, S>
{
    pub fn new(storage: S, provider: P) -> Self {
        Self {
            target_block_number: None,
            storage,
            provider,
        }
    }

    /// Backfill all leaf nodes (accounts and storage)
    pub async fn backfill_leaf_nodes(&self) -> eyre::Result<()> {
        self.backfill_hashed_accounts().await?;
        self.backfill_hashed_storage().await?;
        Ok(())
    }

    /// Backfill hashed accounts data
    async fn backfill_hashed_accounts(&self) -> eyre::Result<()> {
        let mut start_cursor = self
            .provider
            .database_provider_ro()?
            .tx_ref()
            .cursor_read::<tables::HashedAccounts>()?;

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
        let mut start_cursor = self
            .provider
            .database_provider_ro()?
            .tx_ref()
            .cursor_dup_read::<tables::HashedStorages>()?;

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
    pub async fn backfill_accounts_trie(&self) -> eyre::Result<()> {
        let mut start_cursor = self
            .provider
            .database_provider_ro()?
            .tx_ref()
            .cursor_read::<tables::AccountsTrie>()?;

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
        let mut start_cursor = self
            .provider
            .database_provider_ro()?
            .tx_ref()
            .cursor_dup_read::<tables::StoragesTrie>()?;

        let last_storage_branch = self.storage.get_last_storage_branch()?;
        if let Some((hashed_address, nibbles)) = last_storage_branch {
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

    pub async fn process_operation(&mut self, operation: BackfillOperation) -> eyre::Result<()> {
        match operation {
            BackfillOperation::SetTargetBlockNumber {
                target_block_number,
            } => {
                self.target_block_number = Some(target_block_number);
            }
        }

        Ok(())
    }

    pub async fn step(&self) -> eyre::Result<()> {
        if self.storage.get_earliest_block_number().await? == None {
            self.backfill_trie().await?;

            let ChainInfo {
                best_number,
                best_hash,
            } = self.provider.chain_info().unwrap();
            self.storage
                .set_earliest_block_number(best_number, best_hash)
                .await?;
        }
        Ok(())
    }
}

// pub struct BackfillManager<Node, Storage>
// where
//     Node: FullNodeComponents,
//     Node::Provider: StateReader,
//     Storage: ExternalStateStore,
// {
//     provider: Node::Provider,
//     storage: Storage,
// }

// impl<Node, Storage> BackfillManager<Node, Storage>
// where
//     Node: FullNodeComponents,
//     Node::Provider: StateReader,
//     Storage: ExternalStateStore,
// {
//     /// Create a new BackfillManager instance
//     pub fn new(provider: Node::Provider, storage: Storage) -> Self {
//         Self { provider, storage }
//     }

// }
