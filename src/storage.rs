use reth::revm::primitives::B256;
use reth_trie::{BranchNodeCompact, StoredNibbles};
use std::fmt::Debug;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PreimageEntry {
    pub block_number: u64,
    pub path: StoredNibbles,
    pub hashed_address: Option<B256>,
    pub branch: Option<BranchNodeCompact>,
}

/// Batch of preimages to be stored together
#[derive(Debug, Clone)]
pub struct PreimageBatch {
    /// Block number for all items in this batch
    pub block_number: u64,
    /// Map of hash to preimage data
    pub items: Vec<PreimageEntry>,
}

/// Error types for preimage storage operations
#[derive(Debug, thiserror::Error)]
pub enum PreimageStorageError {
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

/// Result type for storage operations
pub type PreimageStorageResult<T> = Result<T, PreimageStorageError>;

/// Trait for storing and retrieving preimage data
/// 
/// This trait provides an abstraction over different storage backends (DynamoDB, etc.)
/// and supports batch operations for efficient storage of preimages with block-based indexing.
/// 
/// Storage model: hash (primary key) -> preimage data, with block_number as secondary index
#[async_trait::async_trait]
pub trait PreimageStore: Send + Sync + Debug {
    /// Store a single preimage. Storing None will store a NULL value which will be used to 
    /// signal that the preimage was deleted at that block.
    /// 
    /// # Arguments
    /// * `hash` - Hash of the preimage (used as primary key)
    /// * `preimage` - The preimage data to store
    /// * `block_number` - Block number for secondary indexing and pruning
    async fn store_preimage(
        &self,
        block_number: u64,
        path: StoredNibbles,
        hashed_address: Option<B256>,
        branch: Option<BranchNodeCompact>,
    ) -> PreimageStorageResult<()>;

    /// Store multiple preimages in a batch operation
    /// 
    /// This should be more efficient than multiple individual stores
    /// 
    /// # Arguments
    /// * `batch` - Batch of preimages to store
    async fn store_preimages_batch(&self, batch: PreimageBatch) -> PreimageStorageResult<()>;

    async fn get_earliest_block_number(&self) -> PreimageStorageResult<Option<(u64, B256)>>;

    async fn set_earliest_block_number(&self, block_number: u64, hash: B256) -> PreimageStorageResult<()>;

    /// Health check for the storage backend
    async fn health_check(&self) -> PreimageStorageResult<()>;
}

impl PreimageBatch {
    /// Create a new empty batch for a specific block
    pub fn new(block_number: u64) -> Self {
        Self {
            block_number,
            items: Vec::new(),
        }
    }
}
