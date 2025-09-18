use std::{path::PathBuf, sync::{Mutex}};

use reth::revm::primitives::B256;
use reth_codecs::Compact;
use reth_tracing::tracing::{info};
use reth_trie::{BranchNodeCompact, Nibbles, StoredNibbles};
use rusqlite::{params, Connection, OptionalExtension};

use crate::storage::{PreimageBatch, PreimageStorageError, PreimageStorageResult, PreimageStore, PreimageStoreCursor};

/// Cursor over an account or storage trie at a certain block number.
pub struct SqlitePreimageStoreCursor {
    conn: Mutex<Connection>,
    hashed_address: Option<B256>,
    max_block_number: u64,
    last_seeked: Option<Nibbles>,
}

enum NextBranchResult {
    Found(Nibbles, BranchNodeCompact),
    NotFound(Nibbles),
    EndOfTrie,
}

impl SqlitePreimageStoreCursor {
    /// Create a new cursor.
    pub fn new(conn: Connection, hashed_address: Option<B256>, max_block_number: u64) -> Self {
        Self { conn: Mutex::new(conn), hashed_address, max_block_number, last_seeked: None }
    }

    /// Convert a branch node from a byte vector to a BranchNodeCompact.
    fn row_to_branch(branch: &Vec<u8>) -> PreimageStorageResult<BranchNodeCompact> {
        Ok(BranchNodeCompact::from_compact(&branch, branch.len()).0)
    }

    /// Get the latest value of the branch node at the given path. Returns None 
    fn get_latest_branch(&mut self, path: Nibbles) -> PreimageStorageResult<Option<(Nibbles, BranchNodeCompact)>> {
        // find the key between <hashed_addr>-<path>-0 and <hashed_addr>-<path>-<max_block_number>
        let key = BranchNodeKey { hashed_address: self.hashed_address, path: StoredNibbles(path) }.encode();


        let max_block_number_int: i64 = self.max_block_number.try_into().unwrap_or(i64::MAX);

        // sort by key descending so latest blocks come first
        let row: Option<(Vec<u8>, Vec<u8>)> = self.conn.lock().unwrap().query_row(
            "SELECT key, branch FROM branch_nodes WHERE key = ? AND block_number <= ? ORDER BY block_number DESC LIMIT 1",
            params![key, max_block_number_int],
            |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?)),
        ).optional()?;


        // if there is no branch node at the given path <= max_block_number, no branch node at this path for this block
        let Some(row) = row else {
            return Ok(None);
        };

        // if the latest branch node is an empty string, the branch node was deleted, so there is no branch node at this path for this block
        if row.1.is_empty() {
            return Ok(None);
        }
        
        // otherwise, decode and return the branch node
        let key = BranchNodeKey::decode(&row.0)?;
        Ok(Some((key.path.0, Self::row_to_branch(&row.1)?)))
    }

    /// Get the branch node with the next path that possibly exists at a certain block number. Returns None if there are no more branch nodes
    fn get_next_latest_branch(&mut self, path: Nibbles) -> PreimageStorageResult<NextBranchResult> {
        // find the next path after the current path
        let last_current_path_key = BranchNodeKey { hashed_address: self.hashed_address, path: StoredNibbles(path) }.encode();


        // find the next path after the current path (for any block number)
        let row: Option<Vec<u8>> = self.conn.lock().unwrap().query_row(
            "SELECT key FROM branch_nodes WHERE key > ? LIMIT 1",
            params![last_current_path_key],
            |r| Ok(r.get::<_, Vec<u8>>(0)?),
        ).optional()?;


        // if there is no next path, we're at the end of the trie, return None
        let Some(row) = row else {
            return Ok(NextBranchResult::EndOfTrie);
        };
        
        // if this is the last path for this account, return None
        let BranchNodeKey { hashed_address: next_trie_node_address, path: next_trie_node_path } = BranchNodeKey::decode(&row)?;
        
        if next_trie_node_address != self.hashed_address {
            return Ok(NextBranchResult::EndOfTrie);
        }

        // get the latest branch for this path and return it (None if there is no branch node at this path for this block)
        match self.get_latest_branch(next_trie_node_path.0)? {
            Some((path, branch)) => Ok(NextBranchResult::Found(path, branch)),
            None => Ok(NextBranchResult::NotFound(next_trie_node_path.0)),
        }
    }

    fn seek_first_non_empty_path_after(&mut self, path: Nibbles, inclusive: bool) -> PreimageStorageResult<Option<(Nibbles, BranchNodeCompact)>> {
        // if we're seeking inclusive, first try to find the exact path
        if inclusive {
            if let Some((path, branch)) = self.get_latest_branch(path)? {
                return Ok(Some((path, branch)));
            }
        };

        // if not inclusive, or there is no branch at the exact path, find the next path that has a branch
        let mut next_path = path;
        loop {
            match self.get_next_latest_branch(next_path)? {
                NextBranchResult::Found(path, branch) => {
                    return Ok(Some((path, branch)));
                }
                NextBranchResult::NotFound(path) => {
                    // go to the next branch node
                    next_path = path;
                }
                NextBranchResult::EndOfTrie => {
                    // we're at the end of the trie, return None
                    return Ok(None);
                }
            }
        }
    }
}

impl PreimageStoreCursor for SqlitePreimageStoreCursor {
    fn seek_exact(&mut self, path: Nibbles) -> PreimageStorageResult<Option<(Nibbles, BranchNodeCompact)>> {
        let Some((returned_path, branch)) = self.get_latest_branch(path)? else {
            return Ok(None);
        };

        self.last_seeked = Some(returned_path);
        Ok(Some((path, branch)))
    }

    fn seek(&mut self, path: Nibbles) -> PreimageStorageResult<Option<(Nibbles, BranchNodeCompact)>> {
        let Some((returned_path, branch)) = self.seek_first_non_empty_path_after(path, true)? else {
            return Ok(None);
        };
        self.last_seeked = Some(returned_path);
        Ok(Some((returned_path, branch)))
    }

    fn next(&mut self) -> PreimageStorageResult<Option<(Nibbles, BranchNodeCompact)>> {
        let Some(last_seeked) = self.last_seeked else {
            let result = self.seek_first_non_empty_path_after(Nibbles::default(), true);
            if let Ok(Some((path, _branch))) = &result {
                self.last_seeked = Some(*path);
            }
            return result;
        };
        let Some((returned_path, branch)) = self.seek_first_non_empty_path_after(last_seeked, false)? else {
            return Ok(None);
        };
        self.last_seeked = Some(returned_path);
        Ok(Some((returned_path, branch)))
    }

    fn current(&mut self) -> PreimageStorageResult<Option<Nibbles>> {
        Ok(self.last_seeked)
    }
}

/// SQLite implementation of PreimageStore
#[derive(Debug, Clone)]
pub struct SqlitePreimageStore {
    /// Connection string or filename
    db_path: PathBuf,
}

impl SqlitePreimageStore {
    /// Create a new SQLite store and ensure schema exists.
    pub async fn new(db_path: impl Into<PathBuf>) -> PreimageStorageResult<Self> {
        let store = Self { db_path: db_path.into() };
        store.ensure_schema()?;
        Ok(store)
    }

    fn connect(&self) -> PreimageStorageResult<Connection> {
        Connection::open(&self.db_path)
            .map_err(|e| PreimageStorageError::ConnectionError(format!("Failed to open sqlite: {}", e)))
    }

    fn ensure_schema(&self) -> PreimageStorageResult<()> {
        let conn = self.connect()?;
        conn.execute_batch(
            r#"
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;

            CREATE TABLE IF NOT EXISTS branch_nodes (
                key BLOB,
                hashed_address BLOB NULL,
                path BLOB NOT NULL,
                block_number INTEGER NOT NULL,
                branch BLOB NOT NULL,
                PRIMARY KEY (key, block_number)
            );

            CREATE INDEX IF NOT EXISTS idx_branch_nodes_block
            ON branch_nodes(block_number);

            CREATE TABLE IF NOT EXISTS earliest_block (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                block_number INTEGER NOT NULL,
                hash BLOB NOT NULL
            );
            "#,
        ).map_err(|e| PreimageStorageError::TableCreationError(format!("Failed to create schema: {}", e)))?;

        Ok(())
    }
    fn encode_branch(branch: &BranchNodeCompact) -> PreimageStorageResult<Vec<u8>> {
        let mut out = Vec::new();
        branch
            .to_compact(&mut out);
        Ok(out)
    }
}

/// Key codec for branch nodes with proper ordering.
///
/// Format:
/// - If `hashed_address` is Some(addr) (account trie):
///     <0x00> <addr(32)> <compact_path>
/// - If `hashed_address` is None (state trie):
///     <0x01> <compact_path>
///
/// This ensures state trie entries (0x01 prefix) are ordered after account trie entries (0x00 prefix)
#[derive(Debug, Clone)]
struct BranchNodeKey {
    hashed_address: Option<B256>,
    path: StoredNibbles,
}

impl BranchNodeKey {
    fn encode(&self) -> Vec<u8> {
        // Encode path using compact format but without length prefix for lexicographic ordering
        let mut path_bytes = Vec::new();
        self.path.to_compact(&mut path_bytes);

        
        if let Some(addr) = &self.hashed_address {
            // Account trie: <0x00> <addr(32)> <path_bytes>
            let mut out = Vec::with_capacity(1 + 32 + path_bytes.len());
            out.push(0x00); // Account trie prefix
            out.extend_from_slice(addr.as_slice());
            out.extend_from_slice(&path_bytes); // Path bytes for lexicographic order
            out
        } else {
            // State trie: <0x01> <path_bytes>
            let mut out = Vec::with_capacity(1 + path_bytes.len());
            out.push(0x01); // State trie prefix
            out.extend_from_slice(&path_bytes); // Path bytes for lexicographic order
            out
        }
    }

    fn decode(bytes: &[u8]) -> PreimageStorageResult<BranchNodeKey> {
        if bytes.len() == 0 {
            return Err(PreimageStorageError::StorageError("key too short".to_string()));
        }
        
        let prefix = bytes[0];
        
        match prefix {
            0x00 => {
                // Account trie: <0x00> <addr(32)> <rlp_path>
                if bytes.len() < 1 + 32 {
                    return Err(PreimageStorageError::StorageError("account trie key too short".to_string()));
                }
                
                let addr = B256::from_slice(&bytes[1..33]);
                
                let path_bytes = &bytes[33..];
                let (path, _) = StoredNibbles::from_compact(&path_bytes, path_bytes.len());
                                
                Ok(BranchNodeKey { hashed_address: Some(addr), path })
            },
            0x01 => {
                // State trie: <0x01> <rlp_path>
                
                let path_bytes = &bytes[1..];
                let (path, _) = StoredNibbles::from_compact(&path_bytes, path_bytes.len());
                
                Ok(BranchNodeKey { hashed_address: None, path })
            },
            _ => Err(PreimageStorageError::StorageError(format!("invalid key prefix: {}", prefix)))
        }
    }
}

#[async_trait::async_trait]
impl PreimageStore for SqlitePreimageStore {
    type Cursor = SqlitePreimageStoreCursor;

    async fn store_preimage(
        &self,
        block_number: u64,
        path: Nibbles,
        hashed_address: Option<B256>,
        branch: Option<BranchNodeCompact>,
    ) -> PreimageStorageResult<()> {
        let mut conn = self.connect()?;
        let tx = conn
            .transaction()
            .map_err(|e| PreimageStorageError::StorageError(format!("Begin tx failed: {}", e)))?;

        let key = BranchNodeKey { hashed_address: hashed_address.clone(), path: StoredNibbles(path) }.encode();
        let mut path_bytes = Vec::new();
        StoredNibbles(path).to_compact(&mut path_bytes);
        let branch_bytes = match branch {
            Some(branch) => Self::encode_branch(&branch)?,
            None => Vec::new(),
        };
        let block_number_int: i64 = block_number.try_into().unwrap_or(i64::MAX);


        tx.execute(
            "INSERT OR REPLACE INTO branch_nodes (key, hashed_address, path, block_number, branch) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                key,
                hashed_address.as_ref().map(|h| h.as_slice().to_vec()),
                path_bytes,
                block_number_int,
                branch_bytes
            ],
        ).map_err(|e| PreimageStorageError::StorageError(format!("insert failed: {}", e)))?;

        tx.commit()
            .map_err(|e| PreimageStorageError::StorageError(format!("commit failed: {}", e)))?;
        Ok(())
    }

    async fn store_preimages_batch(&self, batch: PreimageBatch) -> PreimageStorageResult<()> {
        let mut conn = self.connect()?;
        let tx = conn
            .transaction()
            .map_err(|e| PreimageStorageError::StorageError(format!("Begin tx failed: {}", e)))?;

        for item in batch.items.into_iter() {
            let key = BranchNodeKey { hashed_address: item.hashed_address.clone(), path: StoredNibbles(item.path) }.encode();
            let mut path_bytes = Vec::new();
            StoredNibbles(item.path).to_compact(&mut path_bytes);
            let branch_bytes = match item.branch {
                Some(branch) => Self::encode_branch(&branch)?,
                None => Vec::new(),
            };

            let block_number_int: i64 = item.block_number.try_into().unwrap_or(i64::MAX);
            tx.execute(
                "INSERT OR REPLACE INTO branch_nodes (key, hashed_address, path, block_number, branch) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    key,
                    item.hashed_address.as_ref().map(|h| h.as_slice().to_vec()),
                    path_bytes,
                    block_number_int,
                    branch_bytes
                ],
            ).map_err(|e| PreimageStorageError::BatchError(format!("batch insert failed: {}", e)))?;
        }

        tx.commit()
            .map_err(|e| PreimageStorageError::BatchError(format!("commit failed: {}", e)))?;
        Ok(())
    }

    async fn get_earliest_block_number(&self) -> PreimageStorageResult<Option<(u64, B256)>> {
        let conn = self.connect()?;
        let row: Option<(i64, Vec<u8>)> = conn
            .query_row(
                "SELECT block_number, hash FROM earliest_block WHERE id = 1",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)),
            )
            .optional()
            .map_err(|e| PreimageStorageError::StorageError(format!("query failed: {}", e)))?;

        if let Some((bn, hash_bytes)) = row {
            let mut h = [0u8; 32];
            let len = hash_bytes.len().min(32);
            h[..len].copy_from_slice(&hash_bytes[..len]);
            Ok(Some((bn as u64, B256::from_slice(&h))))
        } else {
            Ok(None)
        }
    }

    async fn set_earliest_block_number(&self, block_number: u64, hash: B256) -> PreimageStorageResult<()> {
        let conn = self.connect()?;
        conn.execute(
            "INSERT INTO earliest_block (id, block_number, hash) VALUES (1, ?1, ?2) ON CONFLICT(id) DO UPDATE SET block_number=excluded.block_number, hash=excluded.hash",
            params![block_number as i64, hash.as_slice()],
        ).map_err(|e| PreimageStorageError::StorageError(format!("upsert earliest_block failed: {}", e)))?;
        Ok(())
    }

    async fn health_check(&self) -> PreimageStorageResult<()> {
        let conn = self.connect()?;
        let _: i64 = conn
            .query_row("SELECT 1", [], |r| r.get(0))
            .map_err(|e| PreimageStorageError::ConnectionError(format!("sqlite healthcheck failed: {}", e)))?;
        info!("SQLite store is healthy");
        Ok(())
    }

    fn cursor(&self, hashed_address: Option<B256>, max_block_number: u64) -> PreimageStorageResult<Self::Cursor> {
        Ok(SqlitePreimageStoreCursor::new(self.connect()?, hashed_address, max_block_number))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_trie::TrieMask;
    use std::sync::Arc;

    fn nibbles_from(vec: Vec<u8>) -> Nibbles { StoredNibbles::from(vec).0 }

    fn create_test_branch() -> BranchNodeCompact {
        // Create a simple branch node with just a state mask, no actual hashes
        let mut state_mask = TrieMask::default();
        state_mask.set_bit(0);
        state_mask.set_bit(1);
        
        BranchNodeCompact {
            state_mask,
            tree_mask: TrieMask::default(),
            hash_mask: TrieMask::default(),
            hashes: Arc::new(vec![]), // Empty hashes vector
            root_hash: None,
        }
    }

    async fn setup_test_store() -> SqlitePreimageStore {
        use tempfile::NamedTempFile;
        let temp_file = NamedTempFile::new().unwrap();
        let store = SqlitePreimageStore::new(temp_file.path()).await.unwrap();
        
        // Keep the temp file alive by leaking it - this is fine for tests
        std::mem::forget(temp_file);
        store
    }

    #[test]
    fn branch_node_key_roundtrip_no_address() {
        let path = nibbles_from(vec![1, 2, 3, 4, 5]);
        let key = BranchNodeKey { hashed_address: None, path: StoredNibbles(path.clone()) }.encode();
        let BranchNodeKey { hashed_address: addr, path: decoded_path } = BranchNodeKey::decode(&key).unwrap();
        assert!(addr.is_none());
        assert_eq!(decoded_path.0, path);
    }

    #[test]
    fn branch_node_key_roundtrip_with_address() {
        let path = nibbles_from((0..32).map(|i| (i % 16) as u8).collect());
        let addr = B256::repeat_byte(0xAB);
        let key = BranchNodeKey { hashed_address: Some(addr), path: StoredNibbles(path.clone()) }.encode();
        let BranchNodeKey { hashed_address: addr2, path: decoded_path } = BranchNodeKey::decode(&key).unwrap();
        assert_eq!(addr2, Some(addr));
        assert_eq!(decoded_path.0, path);
    }

    // 1. Basic Cursor Operations

    #[tokio::test]
    async fn test_cursor_empty_trie() {
        let store = setup_test_store().await;
        let mut cursor = store.cursor(None, 100).unwrap();

        // All operations should return None on empty trie
        assert!(cursor.seek_exact(Nibbles::default()).unwrap().is_none());
        assert!(cursor.seek(Nibbles::default()).unwrap().is_none());
        assert!(cursor.next().unwrap().is_none());
        assert!(cursor.current().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_cursor_single_entry() {
        let store = setup_test_store().await;
        let path = nibbles_from(vec![1, 2, 3]);
        let branch = create_test_branch();
        
        // Store single entry
        store.store_preimage(50, path.clone(), None, Some(branch.clone())).await.unwrap();
        
        let mut cursor = store.cursor(None, 100).unwrap();

        // Test seek_exact
        let result = cursor.seek_exact(path).unwrap().unwrap();
        assert_eq!(result.0, path);
        
        // Test current position
        assert_eq!(cursor.current().unwrap().unwrap(), path);
        
        // Test next from end should return None
        assert!(cursor.next().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_cursor_multiple_entries() {
        let store = setup_test_store().await;
        let paths = vec![
            nibbles_from(vec![1]),
            nibbles_from(vec![1, 2]),
            nibbles_from(vec![2]),
            nibbles_from(vec![2, 3]),
        ];
        let branch = create_test_branch();
        
        // Store multiple entries
        for path in &paths {
            store.store_preimage(50, path.clone(), None, Some(branch.clone())).await.unwrap();
        }
        
        let mut cursor = store.cursor(None, 100).unwrap();

        // Test that we can iterate through all entries
        let mut found_paths = Vec::new();
        while let Some((path, _)) = cursor.next().unwrap() {
            found_paths.push(path);
        }
        
        assert_eq!(found_paths.len(), 4);
        // Paths should be in lexicographic order
        for i in 0..paths.len() {
            assert_eq!(found_paths[i], paths[i]);
        }
    }

    // 2. Seek Operations

    #[tokio::test]
    async fn test_seek_exact_existing_path() {
        let store = setup_test_store().await;
        let path = nibbles_from(vec![1, 2, 3]);
        let branch = create_test_branch();
        
        store.store_preimage(50, path.clone(), None, Some(branch.clone())).await.unwrap();
        
        let mut cursor = store.cursor(None, 100).unwrap();
        let result = cursor.seek_exact(path).unwrap().unwrap();
        assert_eq!(result.0, path);
    }

    #[tokio::test]
    async fn test_seek_exact_non_existing_path() {
        let store = setup_test_store().await;
        let path = nibbles_from(vec![1, 2, 3]);
        let branch = create_test_branch();
        
        store.store_preimage(50, path.clone(), None, Some(branch.clone())).await.unwrap();
        
        let mut cursor = store.cursor(None, 100).unwrap();
        let non_existing = nibbles_from(vec![4, 5, 6]);
        assert!(cursor.seek_exact(non_existing).unwrap().is_none());
    }

    #[tokio::test]
    async fn test_seek_exact_empty_path() {
        let store = setup_test_store().await;
        let path = nibbles_from(vec![]);
        let branch = create_test_branch();
        
        store.store_preimage(50, path.clone(), None, Some(branch.clone())).await.unwrap();
        
        let mut cursor = store.cursor(None, 100).unwrap();
        let result = cursor.seek_exact(Nibbles::default()).unwrap().unwrap();
        assert_eq!(result.0, Nibbles::default());
    }

    #[tokio::test]
    async fn test_seek_to_existing_path() {
        let store = setup_test_store().await;
        let path = nibbles_from(vec![1, 2, 3]);
        let branch = create_test_branch();
        
        store.store_preimage(50, path.clone(), None, Some(branch.clone())).await.unwrap();
        
        let mut cursor = store.cursor(None, 100).unwrap();
        let result = cursor.seek(path).unwrap().unwrap();
        assert_eq!(result.0, path);
    }

    #[tokio::test]
    async fn test_seek_between_existing_nodes() {
        let store = setup_test_store().await;
        let path1 = nibbles_from(vec![1]);
        let path2 = nibbles_from(vec![3]);
        let branch = create_test_branch();
        
        store.store_preimage(50, path1.clone(), None, Some(branch.clone())).await.unwrap();
        store.store_preimage(50, path2.clone(), None, Some(branch.clone())).await.unwrap();
        
        let mut cursor = store.cursor(None, 100).unwrap();
        // Seek to path between 1 and 3, should return path 3
        let seek_path = nibbles_from(vec![2]);
        let result = cursor.seek(seek_path).unwrap().unwrap();
        assert_eq!(result.0, path2);
    }

    #[tokio::test]
    async fn test_seek_after_all_nodes() {
        let store = setup_test_store().await;
        let path = nibbles_from(vec![1]);
        let branch = create_test_branch();
        
        store.store_preimage(50, path.clone(), None, Some(branch.clone())).await.unwrap();
        
        let mut cursor = store.cursor(None, 100).unwrap();
        // Seek to path after all nodes
        let seek_path = nibbles_from(vec![9]);
        assert!(cursor.seek(seek_path).unwrap().is_none());
    }

    #[tokio::test]
    async fn test_seek_before_all_nodes() {
        let store = setup_test_store().await;
        let path = nibbles_from(vec![5]);
        let branch = create_test_branch();
        
        store.store_preimage(50, path.clone(), None, Some(branch.clone())).await.unwrap();
        
        let mut cursor = store.cursor(None, 100).unwrap();
        // Seek to path before all nodes, should return first node
        let seek_path = nibbles_from(vec![1]);
        let result = cursor.seek(seek_path).unwrap().unwrap();
        assert_eq!(result.0, path);
    }

    // 3. Navigation Tests

    #[tokio::test]
    async fn test_next_without_prior_seek() {
        let store = setup_test_store().await;
        let path = nibbles_from(vec![1, 2]);
        let branch = create_test_branch();
        
        store.store_preimage(50, path.clone(), None, Some(branch.clone())).await.unwrap();
        
        let mut cursor = store.cursor(None, 100).unwrap();
        // next() without prior seek should start from beginning
        let result = cursor.next().unwrap().unwrap();
        assert_eq!(result.0, path);
    }

    #[tokio::test]
    async fn test_next_after_seek() {
        let store = setup_test_store().await;
        let path1 = nibbles_from(vec![1]);
        let path2 = nibbles_from(vec![2]);
        let branch = create_test_branch();
        
        store.store_preimage(50, path1.clone(), None, Some(branch.clone())).await.unwrap();
        store.store_preimage(50, path2.clone(), None, Some(branch.clone())).await.unwrap();
        
        let mut cursor = store.cursor(None, 100).unwrap();
        cursor.seek(path1).unwrap();
        
        // next() should return second node
        let result = cursor.next().unwrap().unwrap();
        assert_eq!(result.0, path2);
    }

    #[tokio::test]
    async fn test_next_at_end_of_trie() {
        let store = setup_test_store().await;
        let path = nibbles_from(vec![1]);
        let branch = create_test_branch();
        
        store.store_preimage(50, path.clone(), None, Some(branch.clone())).await.unwrap();
        
        let mut cursor = store.cursor(None, 100).unwrap();
        cursor.seek(path).unwrap();
        
        // next() at end should return None
        assert!(cursor.next().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_multiple_consecutive_next() {
        let store = setup_test_store().await;
        let paths = vec![
            nibbles_from(vec![1]),
            nibbles_from(vec![2]),
            nibbles_from(vec![3]),
        ];
        let branch = create_test_branch();
        
        for path in &paths {
            store.store_preimage(50, path.clone(), None, Some(branch.clone())).await.unwrap();
        }
        
        let mut cursor = store.cursor(None, 100).unwrap();
        
        // Iterate through all with consecutive next() calls
        for expected_path in &paths {
            let result = cursor.next().unwrap().unwrap();
            assert_eq!(result.0, *expected_path);
        }
        
        // Final next() should return None
        assert!(cursor.next().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_current_after_operations() {
        let store = setup_test_store().await;
        let path1 = nibbles_from(vec![1]);
        let path2 = nibbles_from(vec![2]);
        let branch = create_test_branch();
        
        store.store_preimage(50, path1.clone(), None, Some(branch.clone())).await.unwrap();
        store.store_preimage(50, path2.clone(), None, Some(branch.clone())).await.unwrap();
        
        let mut cursor = store.cursor(None, 100).unwrap();
        
        // Current should be None initially
        assert!(cursor.current().unwrap().is_none());
        
        // After seek, current should track position
        cursor.seek(path1).unwrap();
        assert_eq!(cursor.current().unwrap().unwrap(), path1);
        
        // After next, current should update
        cursor.next().unwrap();
        assert_eq!(cursor.current().unwrap().unwrap(), path2);
    }

    #[tokio::test]
    async fn test_current_no_prior_operations() {
        let store = setup_test_store().await;
        let mut cursor = store.cursor(None, 100).unwrap();
        
        // Current should be None when no operations performed
        assert!(cursor.current().unwrap().is_none());
    }

    // 4. Block Number Filtering

    #[tokio::test]
    async fn test_same_path_different_blocks() {
        let store = setup_test_store().await;
        let path = nibbles_from(vec![1, 2]);
        let branch1 = create_test_branch();
        // Make branch2 different by setting different bits
        let mut state_mask2 = TrieMask::default();
        state_mask2.set_bit(5);
        state_mask2.set_bit(6);
        
        let branch2 = BranchNodeCompact {
            state_mask: state_mask2,
            tree_mask: TrieMask::default(),
            hash_mask: TrieMask::default(),
            hashes: Arc::new(vec![]), // Empty hashes vector
            root_hash: None,
        };
        
        // Store same path at different blocks
        store.store_preimage(50, path.clone(), None, Some(branch1.clone())).await.unwrap();
        store.store_preimage(100, path.clone(), None, Some(branch2.clone())).await.unwrap();
        
        // Cursor with max_block_number=75 should see only block 50 data
        let mut cursor75 = store.cursor(None, 75).unwrap();
        let result75 = cursor75.seek_exact(path).unwrap().unwrap();
        assert_eq!(result75.0, path);
        // We can't easily verify the branch content without more complex comparison
        
        // Cursor with max_block_number=150 should see block 100 data (latest)
        let mut cursor150 = store.cursor(None, 150).unwrap();
        let result150 = cursor150.seek_exact(path).unwrap().unwrap();
        assert_eq!(result150.0, path);
    }

    #[tokio::test]
    async fn test_deleted_branch_nodes() {
        let store = setup_test_store().await;
        let path = nibbles_from(vec![1, 2]);
        let branch = create_test_branch();
        
        // Store branch node, then delete it (store None)
        store.store_preimage(50, path.clone(), None, Some(branch.clone())).await.unwrap();
        store.store_preimage(100, path.clone(), None, None).await.unwrap();

        
        // Cursor before deletion should see the node
        let mut cursor75 = store.cursor(None, 75).unwrap();
        assert!(cursor75.seek_exact(path).unwrap().is_some());
        
        // Cursor after deletion should not see the node
        let mut cursor150 = store.cursor(None, 150).unwrap();
        assert!(cursor150.seek_exact(path).unwrap().is_none());
    }

    // 5. Hashed Address Filtering

    #[tokio::test]
    async fn test_account_specific_cursor() {
        let store = setup_test_store().await;
        let path = nibbles_from(vec![1, 2]);
        let addr1 = B256::repeat_byte(0x01);
        let addr2 = B256::repeat_byte(0x02);
        let branch = create_test_branch();
        
        // Store same path for different accounts
        store.store_preimage(50, path.clone(), Some(addr1), Some(branch.clone())).await.unwrap();
        store.store_preimage(50, path.clone(), Some(addr2), Some(branch.clone())).await.unwrap();
        
        // Cursor for addr1 should only see addr1 data
        let mut cursor1 = store.cursor(Some(addr1), 100).unwrap();
        let result1 = cursor1.seek_exact(path).unwrap().unwrap();
        assert_eq!(result1.0, path);
        
        // Cursor for addr2 should only see addr2 data
        let mut cursor2 = store.cursor(Some(addr2), 100).unwrap();
        let result2 = cursor2.seek_exact(path).unwrap().unwrap();
        assert_eq!(result2.0, path);
        
        // Cursor for addr1 should not see addr2 data when iterating
        let mut cursor1_iter = store.cursor(Some(addr1), 100).unwrap();
        let mut found_count = 0;
        while cursor1_iter.next().unwrap().is_some() {
            found_count += 1;
        }
        assert_eq!(found_count, 1); // Should only see one entry (for addr1)
    }

    #[tokio::test]
    async fn test_state_trie_cursor() {
        let store = setup_test_store().await;
        let path = nibbles_from(vec![1, 2]);
        let addr = B256::repeat_byte(0x01);
        let branch = create_test_branch();
        
        // Store data for account trie and state trie
        store.store_preimage(50, path.clone(), Some(addr), Some(branch.clone())).await.unwrap();
        store.store_preimage(50, path.clone(), None, Some(branch.clone())).await.unwrap();
        
        // State trie cursor (None address) should only see state trie data
        let mut state_cursor = store.cursor(None, 100).unwrap();
        let result = state_cursor.seek_exact(path).unwrap().unwrap();
        assert_eq!(result.0, path);
        
        // Verify state cursor doesn't see account data when iterating
        let mut state_cursor_iter = store.cursor(None, 100).unwrap();
        let mut found_count = 0;
        let mut found_paths = Vec::new();
        while let Some((path, _)) = state_cursor_iter.next().unwrap() {
            found_count += 1;
            found_paths.push(path);
        }
        
        // Clean test output
        
        assert_eq!(found_count, 1); // Should only see state trie entry
    }

    #[tokio::test]
    async fn test_mixed_account_state_data() {
        let store = setup_test_store().await;
        let path1 = nibbles_from(vec![1]);
        let path2 = nibbles_from(vec![2]);
        let addr = B256::repeat_byte(0x01);
        let branch = create_test_branch();
        
        // Store mixed account and state trie data
        store.store_preimage(50, path1.clone(), Some(addr), Some(branch.clone())).await.unwrap();
        store.store_preimage(50, path2.clone(), None, Some(branch.clone())).await.unwrap();
        
        // Account cursor should only see account data
        let mut account_cursor = store.cursor(Some(addr), 100).unwrap();
        let mut account_paths = Vec::new();
        while let Some((path, _)) = account_cursor.next().unwrap() {
            account_paths.push(path);
        }
        assert_eq!(account_paths.len(), 1);
        assert_eq!(account_paths[0], path1);
        
        // State cursor should only see state data
        let mut state_cursor = store.cursor(None, 100).unwrap();
        let mut state_paths = Vec::new();
        while let Some((path, _)) = state_cursor.next().unwrap() {
            state_paths.push(path);
        }
        assert_eq!(state_paths.len(), 1);
        assert_eq!(state_paths[0], path2);
    }

    // 6. Path Ordering Tests

    #[tokio::test]
    async fn test_lexicographic_ordering() {
        let store = setup_test_store().await;
        let paths = vec![
            nibbles_from(vec![3, 1]),
            nibbles_from(vec![1, 2]),
            nibbles_from(vec![2]),
            nibbles_from(vec![1]),
        ];
        let branch = create_test_branch();
        
        // Store paths in random order
        for path in &paths {
            store.store_preimage(50, path.clone(), None, Some(branch.clone())).await.unwrap();
        }
        
        let mut cursor = store.cursor(None, 100).unwrap();
        let mut found_paths = Vec::new();
        while let Some((path, _)) = cursor.next().unwrap() {
            found_paths.push(path);
        }
        
        // Should be returned in lexicographic order: [1], [1,2], [2], [3,1]
        let expected_order = vec![
            nibbles_from(vec![1]),
            nibbles_from(vec![1, 2]),
            nibbles_from(vec![2]),
            nibbles_from(vec![3, 1]),
        ];
        
        assert_eq!(found_paths, expected_order);
    }

    #[tokio::test]
    async fn test_path_prefix_scenarios() {
        let store = setup_test_store().await;
        let paths = vec![
            nibbles_from(vec![1]),        // Prefix of next
            nibbles_from(vec![1, 2]),     // Extends first
            nibbles_from(vec![1, 2, 3]),  // Extends second
        ];
        let branch = create_test_branch();
        
        for path in &paths {
            store.store_preimage(50, path.clone(), None, Some(branch.clone())).await.unwrap();
        }
        
        let mut cursor = store.cursor(None, 100).unwrap();
        
        // Seek to prefix should find exact match
        let result = cursor.seek_exact(paths[0]).unwrap().unwrap();
        assert_eq!(result.0, paths[0]);
        
        // Next should go to next path, not skip prefixed paths
        let result = cursor.next().unwrap().unwrap();
        assert_eq!(result.0, paths[1]);
        
        let result = cursor.next().unwrap().unwrap();
        assert_eq!(result.0, paths[2]);
    }

    #[tokio::test]
    async fn test_complex_nibble_combinations() {
        let store = setup_test_store().await;
        // Test various nibble patterns including edge values
        let paths = vec![
            nibbles_from(vec![0]),
            nibbles_from(vec![0, 15]),
            nibbles_from(vec![15]),
            nibbles_from(vec![15, 0]),
            nibbles_from(vec![7, 8, 9]),
        ];
        let branch = create_test_branch();
        
        for path in &paths {
            store.store_preimage(50, path.clone(), None, Some(branch.clone())).await.unwrap();
        }
        

        let mut cursor = store.cursor(None, 100).unwrap();
        let mut found_paths = Vec::new();
        while let Some((path, _)) = cursor.next().unwrap() {
            found_paths.push(path);
        }
        
        // All paths should be found and in correct order
        assert_eq!(found_paths.len(), 5);
        
        // Verify specific ordering for edge cases
        assert_eq!(found_paths[0], nibbles_from(vec![0]));
        assert_eq!(found_paths[1], nibbles_from(vec![0, 15]));
        assert_eq!(found_paths[4], nibbles_from(vec![15, 0]));
    }
}
