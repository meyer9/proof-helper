use std::collections::HashMap;

use reth::revm::primitives::B256;

/// Batch of preimages to be stored together
#[derive(Debug, Clone)]
pub struct PreimageBatch {
    /// Block number for all items in this batch
    pub block_number: u64,
    /// Map of hash to preimage data
    pub items: HashMap<B256, Vec<u8>>,
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
pub trait PreimageStore: Send + Sync {
    /// Store a single preimage
    /// 
    /// # Arguments
    /// * `hash` - Hash of the preimage (used as primary key)
    /// * `preimage` - The preimage data to store
    /// * `block_number` - Block number for secondary indexing and pruning
    async fn store_preimage(
        &self,
        hash: B256,
        preimage: Vec<u8>,
        block_number: u64,
    ) -> PreimageStorageResult<()>;

    /// Store multiple preimages in a batch operation
    /// 
    /// This should be more efficient than multiple individual stores
    /// 
    /// # Arguments
    /// * `batch` - Batch of preimages to store
    async fn store_preimages_batch(&self, batch: PreimageBatch) -> PreimageStorageResult<()>;

    /// Retrieve a preimage by its hash
    /// 
    /// # Arguments
    /// * `hash` - Hash of the preimage to retrieve
    async fn get_preimage(&self, hash: &B256) -> PreimageStorageResult<Option<Vec<u8>>>;

    /// Retrieve multiple preimages by their hashes
    /// 
    /// # Arguments
    /// * `hashes` - Vector of hashes to retrieve
    /// 
    /// # Returns
    /// * HashMap with found preimages (missing items are not included)
    async fn get_preimages_batch(
        &self,
        hashes: &[B256],
    ) -> PreimageStorageResult<HashMap<B256, Vec<u8>>>;

    /// Check if a preimage exists
    /// 
    /// # Arguments
    /// * `hash` - Hash to check for existence
    async fn exists(&self, hash: &B256) -> PreimageStorageResult<bool>;

    /// Prune preimages older than the specified block number
    /// 
    /// This uses the secondary index on block number for efficient pruning
    /// 
    /// # Arguments
    /// * `before_block` - Remove preimages from blocks before this number
    /// 
    /// # Returns
    /// * Number of items deleted
    async fn prune_before_block(&self, before_block: u64) -> PreimageStorageResult<u64>;

    /// Get the count of stored preimages for a specific block
    /// 
    /// # Arguments
    /// * `block_number` - Block number to count preimages for
    async fn count_preimages_for_block(&self, block_number: u64) -> PreimageStorageResult<u64>;

    /// Get all preimage hashes for a specific block
    /// 
    /// Useful for verification or debugging
    /// 
    /// # Arguments
    /// * `block_number` - Block number to get hashes for
    async fn get_hashes_for_block(&self, block_number: u64) -> PreimageStorageResult<Vec<B256>>;

    /// Health check for the storage backend
    async fn health_check(&self) -> PreimageStorageResult<()>;
}

impl PreimageBatch {
    /// Create a new empty batch for a specific block
    pub fn new(block_number: u64) -> Self {
        Self {
            block_number,
            items: HashMap::new(),
        }
    }

    /// Add a preimage to the batch
    pub fn add_preimage(&mut self, hash: B256, preimage: Vec<u8>) {
        self.items.insert(hash, preimage);
    }

    pub fn get_preimage(&self, hash: B256) -> Option<Vec<u8>> {
        self.items.get(&hash).cloned()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Get the total size of all preimages in the batch
    pub fn total_size(&self) -> usize {
        self.items.values().map(|preimage| preimage.len()).sum()
    }
}

/// Mock implementation of PreimageStore for testing
#[derive(Debug, Clone, Default)]
pub struct MockPreimageStore {
    /// Storage map: hash -> (preimage, block_number)
    storage: std::sync::Arc<tokio::sync::RwLock<HashMap<B256, (Vec<u8>, u64)>>>,
}

impl MockPreimageStore {
    /// Create a new mock store
    pub fn new() -> Self {
        Self {
            storage: std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl PreimageStore for MockPreimageStore {
    async fn store_preimage(
        &self,
        hash: B256,
        preimage: Vec<u8>,
        block_number: u64,
    ) -> PreimageStorageResult<()> {
        let mut storage = self.storage.write().await;
        storage.insert(hash, (preimage, block_number));
        Ok(())
    }

    async fn store_preimages_batch(&self, batch: PreimageBatch) -> PreimageStorageResult<()> {
        let mut storage = self.storage.write().await;
        for (hash, preimage) in batch.items {
            storage.insert(hash, (preimage, batch.block_number));
        }
        Ok(())
    }

    async fn get_preimage(&self, hash: &B256) -> PreimageStorageResult<Option<Vec<u8>>> {
        let storage = self.storage.read().await;
        Ok(storage.get(hash).map(|(preimage, _)| preimage.clone()))
    }

    async fn get_preimages_batch(
        &self,
        hashes: &[B256],
    ) -> PreimageStorageResult<HashMap<B256, Vec<u8>>> {
        let storage = self.storage.read().await;
        let mut result = HashMap::new();
        
        for hash in hashes {
            if let Some((preimage, _)) = storage.get(hash) {
                result.insert(*hash, preimage.clone());
            }
        }
        
        Ok(result)
    }

    async fn exists(&self, hash: &B256) -> PreimageStorageResult<bool> {
        let storage = self.storage.read().await;
        Ok(storage.contains_key(hash))
    }

    async fn prune_before_block(&self, before_block: u64) -> PreimageStorageResult<u64> {
        let mut storage = self.storage.write().await;
        let mut removed_count = 0;
        
        storage.retain(|_, (_, block_number)| {
            if *block_number < before_block {
                removed_count += 1;
                false
            } else {
                true
            }
        });
        
        Ok(removed_count)
    }

    async fn count_preimages_for_block(&self, block_number: u64) -> PreimageStorageResult<u64> {
        let storage = self.storage.read().await;
        let count = storage
            .values()
            .filter(|(_, bn)| *bn == block_number)
            .count() as u64;
        Ok(count)
    }

    async fn get_hashes_for_block(&self, block_number: u64) -> PreimageStorageResult<Vec<B256>> {
        let storage = self.storage.read().await;
        let hashes = storage
            .iter()
            .filter(|(_, (_, bn))| *bn == block_number)
            .map(|(hash, _)| *hash)
            .collect();
        Ok(hashes)
    }

    async fn health_check(&self) -> PreimageStorageResult<()> {
        // Mock always healthy
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preimage_batch_operations() {
        let mut batch = PreimageBatch::new(12345);
        
        let hash1 = B256::from([1u8; 32]);
        let hash2 = B256::from([2u8; 32]);
        
        batch.add_preimage(hash1, vec![1, 2, 3]);
        batch.add_preimage(hash2, vec![4, 5, 6, 7, 8]);
        
        assert!(!batch.is_empty());
        assert_eq!(batch.block_number, 12345);
        assert_eq!(batch.total_size(), 8); // 3 + 5 bytes
    }

    #[test]
    fn test_empty_batch() {
        let batch = PreimageBatch::new(0);
        
        assert!(batch.is_empty());
        assert_eq!(batch.total_size(), 0);
    }

    #[tokio::test]
    async fn test_mock_store_basic_operations() {
        let store = MockPreimageStore::new();
        let hash = B256::from([1u8; 32]);
        let preimage = vec![1, 2, 3, 4, 5];
        
        // Store a preimage
        store.store_preimage(hash, preimage.clone(), 100).await.unwrap();
        
        // Check existence
        assert!(store.exists(&hash).await.unwrap());
        
        // Retrieve preimage
        let retrieved = store.get_preimage(&hash).await.unwrap();
        assert_eq!(retrieved, Some(preimage));
        
        // Check count
        assert_eq!(store.count_preimages_for_block(100).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_mock_store_batch_operations() {
        let store = MockPreimageStore::new();
        let mut batch = PreimageBatch::new(200);
        
        let hash1 = B256::from([1u8; 32]);
        let hash2 = B256::from([2u8; 32]);
        let preimage1 = vec![1, 2, 3];
        let preimage2 = vec![4, 5, 6];
        
        batch.add_preimage(hash1, preimage1.clone());
        batch.add_preimage(hash2, preimage2.clone());
        
        // Store batch
        store.store_preimages_batch(batch).await.unwrap();
        
        // Retrieve batch
        let retrieved = store.get_preimages_batch(&[hash1, hash2]).await.unwrap();
        assert_eq!(retrieved.len(), 2);
        assert_eq!(retrieved.get(&hash1), Some(&preimage1));
        assert_eq!(retrieved.get(&hash2), Some(&preimage2));
        
        // Check block count
        assert_eq!(store.count_preimages_for_block(200).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_mock_store_pruning() {
        let store = MockPreimageStore::new();
        
        // Store preimages from different blocks
        let hash1 = B256::from([1u8; 32]);
        let hash2 = B256::from([2u8; 32]);
        let hash3 = B256::from([3u8; 32]);
        
        store.store_preimage(hash1, vec![1], 100).await.unwrap();
        store.store_preimage(hash2, vec![2], 200).await.unwrap();
        store.store_preimage(hash3, vec![3], 300).await.unwrap();
        
        // Prune before block 250
        let removed = store.prune_before_block(250).await.unwrap();
        assert_eq!(removed, 2); // Should remove blocks 100 and 200
        
        // Only block 300 should remain
        assert!(store.exists(&hash3).await.unwrap());
        assert!(!store.exists(&hash1).await.unwrap());
        assert!(!store.exists(&hash2).await.unwrap());
    }
} 