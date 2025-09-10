use std::path::PathBuf;

use reth::revm::primitives::B256;
use reth_codecs::Compact;
use reth_tracing::tracing::{info};
use reth_trie::{BranchNodeCompact, StoredNibbles};
use rusqlite::{params, Connection, OptionalExtension};

use crate::storage::{PreimageBatch, PreimageStore, PreimageStorageError, PreimageStorageResult};

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

    /// Build the compound key as <hashed_address_optional> ++ <path> ++ <block_number>.
    /// - hashed_address_optional: 32 bytes if Some, 0 bytes if None
    /// - path: compact bytes representation from StoredNibbles
    /// - block_number: 8-byte big-endian
    fn build_key(hashed_address: &Option<B256>, path: &StoredNibbles, block_number: u64) -> Vec<u8> {
        let mut path_bytes = Vec::new();
        let path_len = path.to_compact(&mut path_bytes);
        let mut key = Vec::with_capacity(32 + path_len + 8);
        if let Some(addr) = hashed_address {
            key.extend_from_slice(addr.as_slice());
        }
        key.extend_from_slice(&path_bytes);
        key.extend_from_slice(&block_number.to_be_bytes());
        key
    }

    fn encode_branch(branch: &BranchNodeCompact) -> PreimageStorageResult<Vec<u8>> {
        let mut out = Vec::new();
        branch
            .to_compact(&mut out);
        Ok(out)
    }
}

#[async_trait::async_trait]
impl PreimageStore for SqlitePreimageStore {
    async fn store_preimage(
        &self,
        block_number: u64,
        path: StoredNibbles,
        hashed_address: Option<B256>,
        branch: BranchNodeCompact,
    ) -> PreimageStorageResult<()> {
        let mut conn = self.connect()?;
        let tx = conn
            .transaction()
            .map_err(|e| PreimageStorageError::StorageError(format!("Begin tx failed: {}", e)))?;

        let key = Self::build_key(&hashed_address, &path, block_number);
        let mut path_bytes = Vec::new();
        path.to_compact(&mut path_bytes);
        let branch_bytes = Self::encode_branch(&branch)?;

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

            let key = Self::build_key(&item.hashed_address, &item.path, item.block_number);
            let mut path_bytes = Vec::new();
            item.path.to_compact(&mut path_bytes);
            let branch_bytes = Self::encode_branch(&item.branch)?;

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

    async fn get_earliest_block_number(&self) -> PreimageStorageResult<(u64, B256)> {
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
            Ok((bn as u64, B256::from_slice(&h)))
        } else {
            Ok((0, B256::ZERO))
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
}


