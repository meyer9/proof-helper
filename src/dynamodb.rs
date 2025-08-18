use std::collections::HashMap;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::{
    Client,
    types::{
        AttributeValue, WriteRequest, PutRequest, DeleteRequest, Select
    },
    error::SdkError,
};
use reth::revm::primitives::B256;
use reth_trie::Nibbles;
use crate::config::DynamoDbConfig;
use crate::storage::{PreimageStore, PreimageStorageError, PreimageStorageResult, PreimageBatch};
use alloy_rlp::Encodable;

/// DynamoDB implementation of PreimageStore
#[derive(Debug, Clone)]
pub struct DynamoDbPreimageStore {
    client: Client,
    table_name: String,
    block_index_name: String,
}

impl DynamoDbPreimageStore {
    /// Create a new DynamoDB store with the given configuration
    pub async fn new(config: &DynamoDbConfig) -> PreimageStorageResult<Self> {
        let aws_config = if let Some(endpoint_url) = &config.endpoint_url {
            // For DynamoDB Local or custom endpoint
            aws_config::defaults(BehaviorVersion::latest())
                .endpoint_url(endpoint_url)
                .region(aws_config::Region::new(config.region.clone()))
                .load()
                .await
        } else {
            // For AWS DynamoDB
            aws_config::defaults(BehaviorVersion::latest())
                .region(aws_config::Region::new(config.region.clone()))
                .load()
                .await
        };

        let client = Client::new(&aws_config);
        let table_name = config.table_name.clone();
        let block_index_name = "hash_index".to_string();

        let store = Self {
            client,
            table_name,
            block_index_name,
        };

        Ok(store)
    }

    /// Convert AttributeValue to bytes
    fn attribute_to_bytes(&self, attr: &AttributeValue) -> PreimageStorageResult<Vec<u8>> {
        match attr {
            AttributeValue::B(blob) => Ok(blob.as_ref().to_vec()),
            _ => Err(PreimageStorageError::SerializationError(
                "Expected binary attribute".to_string()
            )),
        }
    }
}

#[async_trait::async_trait]
impl PreimageStore for DynamoDbPreimageStore {
    async fn store_preimage(
        &self,
        hash: B256,
        preimage: Vec<u8>,
        hashed_address: Option<B256>,
        path: Nibbles,
        block_number: u64,
    ) -> PreimageStorageResult<()> {
        let full_path = if let Some(hashed_address) = hashed_address {
            let mut storage_path = Vec::new();
            hashed_address.encode(&mut storage_path);
            path.encode(&mut storage_path);
            storage_path
        } else {
            let mut account_path = Vec::new();
            path.encode(&mut account_path);
            account_path
        };

        
        let put_request = self.client.put_item()
            .table_name(&self.table_name)
            .item("full_path", AttributeValue::B(full_path.into()))
            .item("hash", AttributeValue::B(hash.as_slice().to_vec().into()))
            .item("block_number", AttributeValue::N(block_number.to_string()))
            .item("data", AttributeValue::B(preimage.into()));

        match put_request.send().await {
            Ok(_) => Ok(()),
            Err(e) => Err(PreimageStorageError::StorageError(
                format!("Failed to store preimage: {}", e)
            )),
        }
    }

    async fn store_preimages_batch(&self, batch: PreimageBatch) -> PreimageStorageResult<()> {
        if batch.items.is_empty() {
            return Ok(());
        }

        // DynamoDB batch write supports up to 25 items per request
        const BATCH_SIZE: usize = 25;
        let items: Vec<_> = batch.items.into_iter().collect();
        
        for chunk in items.chunks(BATCH_SIZE) {
            let mut write_requests = Vec::new();
            
            for item in chunk {
                let full_path = if let Some(hashed_address) = item.hashed_address {
                    let mut storage_path = Vec::new();
                    hashed_address.encode(&mut storage_path);
                    item.path.encode(&mut storage_path);
                    storage_path
                } else {
                    let mut account_path = Vec::new();
                    item.path.encode(&mut account_path);
                    account_path
                };
                
                let put_request = PutRequest::builder()
                    .item("full_path", AttributeValue::B(full_path.into()))
                    .item("hash", AttributeValue::B(item.hash.as_slice().to_vec().into()))
                    .item("block_number", AttributeValue::N(batch.block_number.to_string()))
                    .item("data", AttributeValue::B(item.preimage.clone().into()))
                    .build()
                    .map_err(|e| PreimageStorageError::StorageError(format!("Failed to build put request: {}", e)))?;

                write_requests.push(WriteRequest::builder()
                    .put_request(put_request)
                    .build());
            }

            let batch_write_request = self.client.batch_write_item()
                .request_items(&self.table_name, write_requests);

            match batch_write_request.send().await {
                Ok(_) => {},
                Err(e) => return Err(PreimageStorageError::BatchError(
                    format!("Failed to batch write preimages: {}", e)
                )),
            }
        }

        Ok(())
    }

    async fn get_preimage(&self, hash: &B256) -> PreimageStorageResult<Option<Vec<u8>>> {
        // Query the hash GSI to find items with this hash
        let query_request = self.client.query()
            .table_name(&self.table_name)
            .index_name(&self.block_index_name)
            .key_condition_expression("#hash = :hash_val")
            .expression_attribute_names("#hash", "hash")
            .expression_attribute_values(":hash_val", AttributeValue::B(hash.as_slice().to_vec().into()))
            .limit(1); // We only need one result

        match query_request.send().await {
            Ok(output) => {
                if let Some(items) = output.items {
                    if let Some(item) = items.first() {
                        if let Some(data_attr) = item.get("data") {
                            let preimage = self.attribute_to_bytes(data_attr)?;
                            return Ok(Some(preimage));
                        }
                    }
                }
                Ok(None)
            }
            Err(e) => Err(PreimageStorageError::StorageError(
                format!("Failed to get preimage: {}", e)
            )),
        }
    }

    async fn get_preimages_batch(
        &self,
        hashes: &[B256],
    ) -> PreimageStorageResult<HashMap<B256, Vec<u8>>> {
        if hashes.is_empty() {
            return Ok(HashMap::new());
        }

        let mut result = HashMap::new();
        
        // DynamoDB batch get supports up to 100 items per request
        const BATCH_SIZE: usize = 100;
        
        for chunk in hashes.chunks(BATCH_SIZE) {
            // Use individual queries on the GSI since batch_get_item doesn't work with GSI
            for hash in chunk {
                match self.get_preimage(hash).await {
                    Ok(Some(preimage)) => {
                        result.insert(*hash, preimage);
                    }
                    Ok(None) => {
                        // Hash not found, continue
                    }
                    Err(e) => return Err(PreimageStorageError::BatchError(
                        format!("Failed to get preimage for hash {:x}: {}", hash, e)
                    )),
                }
            }
        }

        Ok(result)
    }

    async fn exists(&self, hash: &B256) -> PreimageStorageResult<bool> {
        // Query the hash GSI to check if item exists
        let query_request = self.client.query()
            .table_name(&self.table_name)
            .index_name(&self.block_index_name)
            .key_condition_expression("#hash = :hash_val")
            .expression_attribute_names("#hash", "hash")
            .expression_attribute_values(":hash_val", AttributeValue::B(hash.as_slice().to_vec().into()))
            .limit(1)
            .projection_expression("#hash"); // Only retrieve the hash to check existence

        match query_request.send().await {
            Ok(output) => Ok(output.count() > 0),
            Err(e) => Err(PreimageStorageError::StorageError(
                format!("Failed to check existence: {}", e)
            )),
        }
    }

    async fn prune_before_block(&self, before_block: u64) -> PreimageStorageResult<u64> {
        let mut removed_count = 0u64;
        let mut last_evaluated_key: Option<HashMap<String, AttributeValue>> = None;

        loop {
            let mut query = self.client.scan()
                .table_name(&self.table_name)
                .filter_expression("block_number < :before_block")
                .expression_attribute_values(":before_block", AttributeValue::N(before_block.to_string()))
                .select(Select::AllAttributes);

            if let Some(ref key) = last_evaluated_key {
                query = query.set_exclusive_start_key(Some(key.clone()));
            }

            match query.send().await {
                Ok(output) => {
                    let items = output.items();
                    if !items.is_empty() {
                        // Delete items in batches
                        const DELETE_BATCH_SIZE: usize = 25;
                        
                        for chunk in items.chunks(DELETE_BATCH_SIZE) {
                            let mut delete_requests = Vec::new();
                            
                            for item in chunk {
                                if let (Some(full_path_attr), Some(block_attr)) = 
                                    (item.get("full_path"), item.get("block_number")) {
                                    let mut key = HashMap::new();
                                    key.insert("full_path".to_string(), full_path_attr.clone());
                                    key.insert("block_number".to_string(), block_attr.clone());
                                    
                                    let delete_request = DeleteRequest::builder()
                                        .set_key(Some(key))
                                        .build()
                                        .map_err(|e| PreimageStorageError::StorageError(format!("Failed to build delete request: {}", e)))?;
                                    
                                    delete_requests.push(WriteRequest::builder()
                                        .delete_request(delete_request)
                                        .build());
                                }
                            }

                            if !delete_requests.is_empty() {
                                let batch_delete_request = self.client.batch_write_item()
                                    .request_items(&self.table_name, delete_requests);

                                match batch_delete_request.send().await {
                                    Ok(_) => removed_count += chunk.len() as u64,
                                    Err(e) => return Err(PreimageStorageError::BatchError(
                                        format!("Failed to batch delete items: {}", e)
                                    )),
                                }
                            }
                        }
                    }

                    last_evaluated_key = output.last_evaluated_key().cloned();
                    if last_evaluated_key.is_none() {
                        break;
                    }
                }
                Err(e) => return Err(PreimageStorageError::StorageError(
                    format!("Failed to scan for pruning: {}", e)
                )),
            }
        }

        Ok(removed_count)
    }

    async fn count_preimages_for_block(&self, block_number: u64) -> PreimageStorageResult<u64> {
        // Since we don't have a GSI on block_number anymore, we need to scan
        let mut count = 0u64;
        let mut last_evaluated_key: Option<HashMap<String, AttributeValue>> = None;

        loop {
            let mut scan = self.client.scan()
                .table_name(&self.table_name)
                .filter_expression("block_number = :block_number")
                .expression_attribute_values(":block_number", AttributeValue::N(block_number.to_string()))
                .select(Select::Count);

            if let Some(ref key) = last_evaluated_key {
                scan = scan.set_exclusive_start_key(Some(key.clone()));
            }

            match scan.send().await {
                Ok(output) => {
                    count += output.count() as u64;
                    last_evaluated_key = output.last_evaluated_key().cloned();
                    if last_evaluated_key.is_none() {
                        break;
                    }
                }
                Err(e) => return Err(PreimageStorageError::StorageError(
                    format!("Failed to count preimages for block: {}", e)
                )),
            }
        }

        Ok(count)
    }

    async fn get_hashes_for_block(&self, block_number: u64) -> PreimageStorageResult<Vec<B256>> {
        let mut hashes = Vec::new();
        let mut last_evaluated_key: Option<HashMap<String, AttributeValue>> = None;

        loop {
            let mut scan = self.client.scan()
                .table_name(&self.table_name)
                .filter_expression("block_number = :block_number")
                .expression_attribute_values(":block_number", AttributeValue::N(block_number.to_string()))
                .projection_expression("#hash")
                .expression_attribute_names("#hash", "hash");

            if let Some(ref key) = last_evaluated_key {
                scan = scan.set_exclusive_start_key(Some(key.clone()));
            }

            match scan.send().await {
                Ok(output) => {
                    let items = output.items();
                    for item in items {
                        if let Some(AttributeValue::B(hash_bytes)) = item.get("hash") {
                            if hash_bytes.as_ref().len() == 32 {
                                let mut array = [0u8; 32];
                                array.copy_from_slice(hash_bytes.as_ref());
                                hashes.push(B256::from(array));
                            }
                        }
                    }

                    last_evaluated_key = output.last_evaluated_key().cloned();
                    if last_evaluated_key.is_none() {
                        break;
                    }
                }
                Err(e) => return Err(PreimageStorageError::StorageError(
                    format!("Failed to get hashes for block: {}", e)
                )),
            }
        }

        Ok(hashes)
    }

    async fn health_check(&self) -> PreimageStorageResult<()> {
        match self.client.describe_table()
            .table_name(&self.table_name)
            .send()
            .await
        {
            Ok(output) => {
                if let Some(table) = output.table() {
                    if table.table_status() == Some(&aws_sdk_dynamodb::types::TableStatus::Active) {
                        Ok(())
                    } else {
                        Err(PreimageStorageError::ConnectionError(
                            "Table is not active".to_string()
                        ))
                    }
                } else {
                    Err(PreimageStorageError::ConnectionError(
                        "Table not found".to_string()
                    ))
                }
            }
            Err(e) => Err(PreimageStorageError::ConnectionError(
                format!("Health check failed: {}", e)
            )),
        }
    }
} 