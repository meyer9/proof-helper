use alloy_primitives::U256;
use reth::revm::primitives::B256;
use reth_db_api::DatabaseError;
use reth_trie::{BranchNodeCompact, Nibbles};
use std::fmt::Debug;
use auto_impl::auto_impl;
use reth::primitives::Account;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PreimageEntry {
    pub block_number: u64,
    pub path: Nibbles,
    pub hashed_address: Option<B256>,
    pub branch: Option<BranchNodeCompact>,
}

/// Batch of preimages to be stored together
#[derive(Debug, Clone)]
pub struct TrieBranchesBatch {
    /// Block number for all items in this batch
    pub block_number: u64,
    /// Map of hash to preimage data
    pub items: Vec<PreimageEntry>,
}

/// Error types for preimage storage operations
#[derive(Debug, thiserror::Error)]
pub enum ExternalStorageError {
    #[error("Storage operation failed: {0}")]
    StorageError(String),
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Preimage not found: {0}")]
    NotFound(B256),
    #[error("Batch operation failed: {0}")]
    BatchError(String),
    #[error("Connection error: {0}")]
    ConnectionError(String),
    #[error("Table creation error: {0}")]
    TableCreationError(String),
}

impl Into<DatabaseError> for ExternalStorageError {
    fn into(self) -> DatabaseError {
        DatabaseError::Other(self.to_string())
    }
}

impl From<rusqlite::Error> for ExternalStorageError {
    fn from(error: rusqlite::Error) -> Self {
        ExternalStorageError::StorageError(error.to_string())
    }
}

/// Result type for storage operations
pub type ExternalStorageResult<T> = Result<T, ExternalStorageError>;

pub trait ExternalTrieCursor: Send + Sync {
    fn seek_exact(&mut self, path: Nibbles) -> ExternalStorageResult<Option<(Nibbles, BranchNodeCompact)>>;
    fn seek(&mut self, path: Nibbles) -> ExternalStorageResult<Option<(Nibbles, BranchNodeCompact)>>;
    fn next(&mut self) -> ExternalStorageResult<Option<(Nibbles, BranchNodeCompact)>>;
    fn current(&mut self) -> ExternalStorageResult<Option<Nibbles>>;
}

pub trait ExternalHashedCursor: Send + Sync {
    /// Value returned by the cursor.
    type Value: std::fmt::Debug;

    /// Seek an entry greater or equal to the given key and position the cursor there.
    /// Returns the first entry with the key greater or equal to the sought key.
    fn seek(&mut self, key: B256) -> ExternalStorageResult<Option<(B256, Self::Value)>>;

    /// Move the cursor to the next entry and return it.
    fn next(&mut self) -> ExternalStorageResult<Option<(B256, Self::Value)>>;
}

/// Trait for storing and retrieving preimage data
/// 
/// This trait provides an abstraction over different storage backends (DynamoDB, etc.)
/// and supports batch operations for efficient storage of preimages with block-based indexing.
/// 
/// Storage model: hash (primary key) -> preimage data, with block_number as secondary index
#[async_trait::async_trait]
#[auto_impl(Arc)]
pub trait ExternalStateStore: Send + Sync + Debug {
    type TrieCursor: ExternalTrieCursor;
    type StorageCursor: ExternalHashedCursor<Value = U256>;
    type AccountHashedCursor: ExternalHashedCursor<Value = Account>;

    /// Store a single preimage. Storing None will store a NULL value which will be used to 
    /// signal that the preimage was deleted at that block.
    /// 
    /// # Arguments
    /// * `hash` - Hash of the preimage (used as primary key)
    /// * `preimage` - The preimage data to store
    /// * `block_number` - Block number for secondary indexing and pruning
    async fn store_trie_branch(
        &self,
        block_number: u64,
        path: Nibbles,
        hashed_address: Option<B256>,
        branch: Option<BranchNodeCompact>,
    ) -> ExternalStorageResult<()>;

    /// Store multiple preimages in a batch operation
    /// 
    /// This should be more efficient than multiple individual stores
    /// 
    /// # Arguments
    /// * `batch` - Batch of preimages to store
    async fn store_trie_branches(&self, batch: TrieBranchesBatch) -> ExternalStorageResult<()>;

    async fn store_hashed_accounts(&self, accounts: Vec<(B256, Account)>, block_number: u64) -> ExternalStorageResult<()>;

    async fn store_hashed_storages(&self, hashed_address: B256, storages: Vec<(B256, U256)>, block_number: u64) -> ExternalStorageResult<()>;

    /// Get the earliest block number and hash that has been stored
    /// 
    /// This is used to determine the block number of trie nodes with block number 0.
    /// All earliest block numbers are stored in 0 to reduce updates required to prune trie nodes.
    async fn get_earliest_block_number(&self) -> ExternalStorageResult<Option<(u64, B256)>>;

    /// Set the earliest block number and hash that has been stored
    async fn set_earliest_block_number(&self, block_number: u64, hash: B256) -> ExternalStorageResult<()>;

    /// Health check for the storage backend
    async fn health_check(&self) -> ExternalStorageResult<()>;

    /// Get a cursor for the storage backend
    fn trie_cursor(&self, hashed_address: Option<B256>, max_block_number: u64) -> ExternalStorageResult<Self::TrieCursor>;

    fn storage_hashed_cursor(&self, hashed_address: B256, max_block_number: u64) -> ExternalStorageResult<Self::StorageCursor>;

    fn account_hashed_cursor(&self, max_block_number: u64) -> ExternalStorageResult<Self::AccountHashedCursor>;
}

impl TrieBranchesBatch {
    /// Create a new empty batch for a specific block
    pub fn new(block_number: u64) -> Self {
        Self {
            block_number,
            items: Vec::new(),
        }
    }
}
