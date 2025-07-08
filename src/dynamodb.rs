use std::collections::HashMap;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::{
    Client,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType, 
        ScalarAttributeType, GlobalSecondaryIndex, Projection, ProjectionType, 
        WriteRequest, PutRequest, Select
    },
    error::SdkError,
};
use reth::revm::primitives::B256;
use crate::config::DynamoDbConfig;
use crate::storage::{PreimageStore, PreimageStorageError, PreimageStorageResult, PreimageBatch};

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
        let block_index_name = format!("{}-block-index", table_name);

        let store = Self {
            client,
            table_name,
            block_index_name,
        };

        // Ensure table exists
        store.ensure_table_exists(config).await?;

        Ok(store)
    }

    /// Ensure the DynamoDB table exists, create it if it doesn't
    async fn ensure_table_exists(&self, config: &DynamoDbConfig) -> PreimageStorageResult<()> {
        // Check if table exists
        match self.client.describe_table()
            .table_name(&self.table_name)
            .send()
            .await
        {
            Ok(_) => {
                // Table exists, we're good
                return Ok(());
            }
            Err(SdkError::ServiceError(service_error)) => {
                if service_error.err().meta().code() != Some("ResourceNotFoundException") {
                    return Err(PreimageStorageError::ConnectionError(
                        format!("Failed to describe table: {:?}", service_error)
                    ));
                }
                // Table doesn't exist, we'll create it below
            }
            Err(e) => {
                return Err(PreimageStorageError::ConnectionError(
                    format!("Failed to describe table: {}", e)
                ));
            }
        }

        // Create the table
        let create_table_request = self.client.create_table()
            .table_name(&self.table_name)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("hash")
                    .key_type(KeyType::Hash)
                    .build()
                    .map_err(|e| PreimageStorageError::StorageError(format!("Failed to build key schema: {}", e)))?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("block_number")
                    .key_type(KeyType::Range)
                    .build()
                    .map_err(|e| PreimageStorageError::StorageError(format!("Failed to build key schema: {}", e)))?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("hash")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|e| PreimageStorageError::StorageError(format!("Failed to build attribute definition: {}", e)))?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("block_number")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .map_err(|e| PreimageStorageError::StorageError(format!("Failed to build attribute definition: {}", e)))?,
            )
            .global_secondary_indexes(
                GlobalSecondaryIndex::builder()
                    .index_name(&self.block_index_name)
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("block_number")
                            .key_type(KeyType::Hash)
                            .build()
                            .map_err(|e| PreimageStorageError::StorageError(format!("Failed to build GSI key schema: {}", e)))?,
                    )
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("hash")
                            .key_type(KeyType::Range)
                            .build()
                            .map_err(|e| PreimageStorageError::StorageError(format!("Failed to build GSI key schema: {}", e)))?,
                    )
                    .projection(
                        Projection::builder()
                            .projection_type(ProjectionType::All)
                            .build()
                    )
                    .build()
                    .map_err(|e| PreimageStorageError::StorageError(format!("Failed to build GSI: {}", e)))?,
            );

        // Set billing mode based on configuration
        let create_table_request = if let (Some(read_capacity), Some(write_capacity)) = 
            (config.read_capacity, config.write_capacity) {
            create_table_request.billing_mode(BillingMode::Provisioned)
                .provisioned_throughput(
                    aws_sdk_dynamodb::types::ProvisionedThroughput::builder()
                        .read_capacity_units(read_capacity as i64)
                        .write_capacity_units(write_capacity as i64)
                        .build()
                        .map_err(|e| PreimageStorageError::StorageError(format!("Failed to build provisioned throughput: {}", e)))?,
                )
        } else {
            create_table_request.billing_mode(BillingMode::PayPerRequest)
        };

        match create_table_request.send().await {
            Ok(_) => {
                // Wait for table to be active
                self.wait_for_table_active().await?;
                Ok(())
            }
            Err(SdkError::ServiceError(service_error)) => {
                if service_error.err().meta().code() == Some("ResourceInUseException") {
                    // Table already exists (race condition)
                    Ok(())
                } else {
                    Err(PreimageStorageError::StorageError(
                        format!("Failed to create table: {:?}", service_error)
                    ))
                }
            }
            Err(e) => Err(PreimageStorageError::StorageError(
                format!("Failed to create table: {}", e)
            )),
        }
    }

    /// Wait for the table to become active
    async fn wait_for_table_active(&self) -> PreimageStorageResult<()> {
        let mut attempts = 0;
        const MAX_ATTEMPTS: u32 = 30;
        const WAIT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

        while attempts < MAX_ATTEMPTS {
            match self.client.describe_table()
                .table_name(&self.table_name)
                .send()
                .await
            {
                Ok(output) => {
                    if let Some(table) = output.table() {
                        if table.table_status() == Some(&aws_sdk_dynamodb::types::TableStatus::Active) {
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    return Err(PreimageStorageError::ConnectionError(
                        format!("Failed to describe table while waiting: {}", e)
                    ));
                }
            }

            attempts += 1;
            tokio::time::sleep(WAIT_INTERVAL).await;
        }

        Err(PreimageStorageError::StorageError(
            "Table did not become active within timeout".to_string()
        ))
    }

    /// Convert B256 hash to DynamoDB string format
    fn hash_to_string(&self, hash: &B256) -> String {
        format!("0x{}", hex::encode(hash.as_slice()))
    }

    /// Convert DynamoDB string back to B256 hash
    fn string_to_hash(&self, s: &str) -> PreimageStorageResult<B256> {
        let hex_str = s.strip_prefix("0x").unwrap_or(s);
        let bytes = hex::decode(hex_str)
            .map_err(|e| PreimageStorageError::SerializationError(format!("Invalid hash format: {}", e)))?;
        
        if bytes.len() != 32 {
            return Err(PreimageStorageError::SerializationError(
                "Hash must be 32 bytes".to_string()
            ));
        }

        let mut array = [0u8; 32];
        array.copy_from_slice(&bytes);
        Ok(B256::from(array))
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
        block_number: u64,
    ) -> PreimageStorageResult<()> {
        let hash_str = self.hash_to_string(&hash);
        
        let put_request = self.client.put_item()
            .table_name(&self.table_name)
            .item("hash", AttributeValue::S(hash_str))
            .item("block_number", AttributeValue::N(block_number.to_string()))
            .item("preimage_data", AttributeValue::B(preimage.into()));

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
            
            for (hash, preimage) in chunk {
                let hash_str = self.hash_to_string(hash);
                
                let put_request = PutRequest::builder()
                    .item("hash", AttributeValue::S(hash_str))
                    .item("block_number", AttributeValue::N(batch.block_number.to_string()))
                    .item("preimage_data", AttributeValue::B(preimage.clone().into()))
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
        let hash_str = self.hash_to_string(hash);
        
        let get_request = self.client.get_item()
            .table_name(&self.table_name)
            .key("hash", AttributeValue::S(hash_str));

        match get_request.send().await {
            Ok(output) => {
                if let Some(item) = output.item() {
                    if let Some(preimage_attr) = item.get("preimage_data") {
                        let preimage = self.attribute_to_bytes(preimage_attr)?;
                        Ok(Some(preimage))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
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
            let mut keys = Vec::new();
            
            for hash in chunk {
                let hash_str = self.hash_to_string(hash);
                let mut key = HashMap::new();
                key.insert("hash".to_string(), AttributeValue::S(hash_str));
                keys.push(key);
            }

            let batch_get_request = self.client.batch_get_item()
                .request_items(&self.table_name, 
                    aws_sdk_dynamodb::types::KeysAndAttributes::builder()
                        .set_keys(Some(keys))
                        .build()
                        .map_err(|e| PreimageStorageError::StorageError(format!("Failed to build keys and attributes: {}", e)))?);

            match batch_get_request.send().await {
                Ok(output) => {
                    if let Some(responses) = output.responses() {
                        if let Some(items) = responses.get(&self.table_name) {
                            for item in items {
                                if let (Some(hash_attr), Some(preimage_attr)) = 
                                    (item.get("hash"), item.get("preimage_data")) {
                                    if let AttributeValue::S(hash_str) = hash_attr {
                                        let hash = self.string_to_hash(&hash_str)?;
                                        let preimage = self.attribute_to_bytes(preimage_attr)?;
                                        result.insert(hash, preimage);
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => return Err(PreimageStorageError::BatchError(
                    format!("Failed to batch get preimages: {}", e)
                )),
            }
        }

        Ok(result)
    }

    async fn exists(&self, hash: &B256) -> PreimageStorageResult<bool> {
        let hash_str = self.hash_to_string(hash);
        
        let get_request = self.client.get_item()
            .table_name(&self.table_name)
            .key("hash", AttributeValue::S(hash_str))
            .projection_expression("hash"); // Only retrieve the key to check existence

        match get_request.send().await {
            Ok(output) => Ok(output.item().is_some()),
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
                                if let (Some(hash_attr), Some(block_attr)) = 
                                    (item.get("hash"), item.get("block_number")) {
                                    let mut key = HashMap::new();
                                    key.insert("hash".to_string(), hash_attr.clone());
                                    key.insert("block_number".to_string(), block_attr.clone());
                                    
                                    let delete_request = aws_sdk_dynamodb::types::DeleteRequest::builder()
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
        let query = self.client.query()
            .table_name(&self.table_name)
            .index_name(&self.block_index_name)
            .key_condition_expression("block_number = :block_number")
            .expression_attribute_values(":block_number", AttributeValue::N(block_number.to_string()))
            .select(Select::Count);

        match query.send().await {
            Ok(output) => Ok(output.count() as u64),
            Err(e) => Err(PreimageStorageError::StorageError(
                format!("Failed to count preimages for block: {}", e)
            )),
        }
    }

    async fn get_hashes_for_block(&self, block_number: u64) -> PreimageStorageResult<Vec<B256>> {
        let mut hashes = Vec::new();
        let mut last_evaluated_key: Option<HashMap<String, AttributeValue>> = None;

        loop {
            let mut query = self.client.query()
                .table_name(&self.table_name)
                .index_name(&self.block_index_name)
                .key_condition_expression("block_number = :block_number")
                .expression_attribute_values(":block_number", AttributeValue::N(block_number.to_string()))
                .projection_expression("hash");

            if let Some(ref key) = last_evaluated_key {
                query = query.set_exclusive_start_key(Some(key.clone()));
            }

            match query.send().await {
                Ok(output) => {
                    let items = output.items();
                    for item in items {
                        if let Some(AttributeValue::S(hash_str)) = item.get("hash") {
                            let hash = self.string_to_hash(&hash_str)?;
                            hashes.push(hash);
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