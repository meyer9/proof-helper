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
    NotFound,
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
        let min_key = BranchNodeKey { hashed_address: self.hashed_address, path: StoredNibbles(path), block_number: 0 }.encode();
        let max_key = BranchNodeKey { hashed_address: self.hashed_address, path: StoredNibbles(path), block_number: self.max_block_number }.encode();

        // sort by key descending so latest blocks come first
        let row: Option<(Vec<u8>, Vec<u8>)> = self.conn.lock().unwrap().query_row(
            "SELECT key, branch FROM branch_nodes WHERE key >= ? AND key <= ? ORDER BY key DESC LIMIT 1",
            params![min_key, max_key],
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
        let (_, path, _) = BranchNodeKey::decode(&row.0)?;
        Ok(Some((path.0, Self::row_to_branch(&row.1)?)))
    }

    /// Get the branch node with the next path that possibly exists at a certain block number. Returns None if there are no more branch nodes
    fn get_next_latest_branch(&mut self, path: Nibbles) -> PreimageStorageResult<NextBranchResult> {
        // find the next path after the current path
        let last_current_path_key = BranchNodeKey { hashed_address: self.hashed_address, path: StoredNibbles(path), block_number: u64::MAX }.encode();

        // find the next path after the current path (for any block number)
        let row: Option<Vec<u8>> = self.conn.lock().unwrap().query_row(
            "SELECT key FROM branch_nodes WHERE key > ? ORDER BY key ASC LIMIT 1",
            params![last_current_path_key],
            |r| Ok(r.get::<_, Vec<u8>>(0)?),
        ).optional()?;

        // if there is no next path, we're at the end of the trie, return None
        let Some(row) = row else {
            return Ok(NextBranchResult::EndOfTrie);
        };
        
        // if this is the last path for this account, return None
        let (next_trie_node_address, next_trie_node_path, _) = BranchNodeKey::decode(&row)?;
        if next_trie_node_address != self.hashed_address {
            return Ok(NextBranchResult::EndOfTrie);
        }

        // get the latest branch for this path and return it (None if there is no branch node at this path for this block)
        match self.get_latest_branch(next_trie_node_path.0)? {
            Some((path, branch)) => Ok(NextBranchResult::Found(path, branch)),
            None => Ok(NextBranchResult::NotFound),
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
                    NextBranchResult::NotFound => {
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
        let Some((returned_path, branch)) = self.seek_first_non_empty_path_after(path, true)? else {
            return Ok(None);
        };

        if returned_path != path {
            return Ok(None);
        }
        self.last_seeked = Some(path);
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
            return self.seek_first_non_empty_path_after(Nibbles::default(), true);
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
                key BLOB PRIMARY KEY,
                hashed_address BLOB NULL,
                path BLOB NOT NULL,
                block_number INTEGER NOT NULL,
                branch BLOB NOT NULL
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

/// Key codec for branch nodes implementing Option B layout.
///
/// Format:
/// - If `hashed_address` is Some(addr):
///     <addr(32)> <len_with_flag(1, MSB=1)> <path(len)> <block_number(8, BE)>
/// - If `hashed_address` is None:
///     <len_with_flag(1, MSB=0)> <path(len)> <block_number(8, BE)>
#[derive(Debug, Clone)]
struct BranchNodeKey {
    hashed_address: Option<B256>,
    path: StoredNibbles,
    block_number: u64,
}

impl BranchNodeKey {
    fn encode(&self) -> Vec<u8> {
        let mut path_bytes = Vec::new();
        let path_len = self.path.to_compact(&mut path_bytes);

        // 1-byte len header plus optional 32-byte address and 8 bytes block number
        let capacity = (if self.hashed_address.is_some() { 32 } else { 0 }) + 1 + path_len + 8;
        let mut out = Vec::with_capacity(capacity);

        if let Some(addr) = &self.hashed_address {
            out.extend_from_slice(addr.as_slice());
            out.push(0x80 | (path_len as u8));
        } else {
            out.push(path_len as u8);
        }
        out.extend_from_slice(&path_bytes);
        out.extend_from_slice(&self.block_number.to_be_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> PreimageStorageResult<(Option<B256>, StoredNibbles, u64)> {
        if bytes.len() < 1 + 8 {
            return Err(PreimageStorageError::StorageError("key too short".to_string()));
        }
        let total_len = bytes.len();
        let block_offset = total_len - 8;
        let block_number = u64::from_be_bytes(bytes[block_offset..].try_into().unwrap());

        // Try address-present case first
        if total_len >= 32 + 1 + 8 {
            let header = bytes[32];
            if header & 0x80 == 0x80 {
                let path_len = (header & 0x7f) as usize;
                let expected = 32 + 1 + path_len + 8;
                if expected == total_len {
                    let addr = B256::from_slice(&bytes[0..32]);
                    let path_start = 33;
                    let path_end = path_start + path_len;
                    let (path, _) = StoredNibbles::from_compact(&bytes[path_start..path_end], path_len);
                    return Ok((Some(addr), path, block_number));
                }
            }
        }

        // Fallback: no address case
        let header = bytes[0];
        if header & 0x80 != 0 {
            return Err(PreimageStorageError::StorageError("invalid key header for no-address variant".to_string()));
        }
        let path_len = header as usize;
        let expected = 1 + path_len + 8;
        if expected != total_len {
            return Err(PreimageStorageError::StorageError("key length mismatch".to_string()));
        }
        let path_start = 1;
        let path_end = path_start + path_len;
        let (path, _) = StoredNibbles::from_compact(&bytes[path_start..path_end], path_len);
        Ok((None, path, block_number))
    }
}

#[async_trait::async_trait]
impl PreimageStore for SqlitePreimageStore {
    type Cursor = SqlitePreimageStoreCursor;

    async fn store_preimage(
        &self,
        block_number: u64,
        path: StoredNibbles,
        hashed_address: Option<B256>,
        branch: Option<BranchNodeCompact>,
    ) -> PreimageStorageResult<()> {
        let mut conn = self.connect()?;
        let tx = conn
            .transaction()
            .map_err(|e| PreimageStorageError::StorageError(format!("Begin tx failed: {}", e)))?;

        let key = BranchNodeKey { hashed_address: hashed_address.clone(), path: path.clone(), block_number }.encode();
        let mut path_bytes = Vec::new();
        path.to_compact(&mut path_bytes);
        let branch_bytes = match branch {
            Some(branch) => Self::encode_branch(&branch)?,
            None => Vec::new(),
        };

        tx.execute(
            "INSERT OR REPLACE INTO branch_nodes (key, hashed_address, path, block_number, branch) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                key,
                hashed_address.as_ref().map(|h| h.as_slice().to_vec()),
                path_bytes,
                block_number as i64,
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
            let key = BranchNodeKey { hashed_address: item.hashed_address.clone(), path: item.path.clone(), block_number: item.block_number }.encode();
            let mut path_bytes = Vec::new();
            item.path.to_compact(&mut path_bytes);
            let branch_bytes = match item.branch {
                Some(branch) => Self::encode_branch(&branch)?,
                None => Vec::new(),
            };

            tx.execute(
                "INSERT OR REPLACE INTO branch_nodes (key, hashed_address, path, block_number, branch) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    key,
                    item.hashed_address.as_ref().map(|h| h.as_slice().to_vec()),
                    path_bytes,
                    item.block_number as i64,
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

    fn nibbles_from(vec: Vec<u8>) -> StoredNibbles { StoredNibbles::from(vec) }

    #[test]
    fn branch_node_key_roundtrip_no_address() {
        let path = nibbles_from(vec![1, 2, 3, 4, 5]);
        let key = BranchNodeKey { hashed_address: None, path: path.clone(), block_number: 42 }.encode();
        let (addr, decoded_path, block) = BranchNodeKey::decode(&key).unwrap();
        assert!(addr.is_none());
        assert_eq!(decoded_path, path);
        assert_eq!(block, 42);
    }

    #[test]
    fn branch_node_key_roundtrip_with_address() {
        let path = nibbles_from((0..32).map(|i| (i % 16) as u8).collect());
        let addr = B256::repeat_byte(0xAB);
        let bn = u64::MAX - 1234;
        let key = BranchNodeKey { hashed_address: Some(addr), path: path.clone(), block_number: bn }.encode();
        let (addr2, decoded_path, bn2) = BranchNodeKey::decode(&key).unwrap();
        assert_eq!(addr2, Some(addr));
        assert_eq!(decoded_path, path);
        assert_eq!(bn2, bn);
    }
}
