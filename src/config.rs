use serde::{Deserialize, Serialize};
use std::env;

/// Configuration for the proof helper application
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofHelperConfig {
    /// Storage backend configuration
    pub storage: StorageConfig,
    
    /// Logging configuration
    pub logging: LoggingConfig,
    
    /// Processing configuration
    pub processing: ProcessingConfig,
}

/// Storage backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Backend type
    pub backend: StorageBackend,

    /// SQLite specific configuration (optional)
    pub sqlite: Option<SqliteConfig>,
}

/// Storage backend types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    /// Embedded SQLite storage
    SQLite,
}

/// DynamoDB specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamoDbConfig {
    /// DynamoDB table name
    pub branch_table_name: String,

    /// DynamoDB table name
    pub earliest_block_number_table_name: String,
    
    /// AWS region
    pub region: String,
    
    /// AWS endpoint URL (optional, for DynamoDB Local)
    pub endpoint_url: Option<String>,
    
    /// Read capacity units for table creation
    pub read_capacity: Option<u32>,
    
    /// Write capacity units for table creation
    pub write_capacity: Option<u32>,
}

/// SQLite specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteConfig {
    /// Path to SQLite database file
    pub db_path: String,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level: "trace", "debug", "info", "warn", "error"
    pub level: String,
}

/// Processing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingConfig {
    /// Maximum number of blocks to process behind head
    pub max_block_diff: u64,
}

/// Configuration errors
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Configuration file not found: {0}")]
    FileNotFound(String),
    #[error("Failed to read configuration file: {0}")]
    ReadError(#[from] std::io::Error),
    #[error("Failed to parse configuration: {0}")]
    ParseError(#[from] toml::de::Error),
    #[error("Invalid configuration: {0}")]
    ValidationError(String),
    #[error("Environment variable error: {0}")]
    EnvError(String),
}

/// Configuration result type
pub type ConfigResult<T> = Result<T, ConfigError>;

impl ProofHelperConfig {
    /// Load configuration from environment variables only
    pub fn load_from_env() -> ConfigResult<Self> {
        let mut config = Self::default();
        config.apply_env_overrides()?;
        config.validate()?;
        Ok(config)
    }
    
    /// Apply environment variable overrides
    fn apply_env_overrides(&mut self) -> ConfigResult<()> {
        // Storage backend
        if let Ok(backend) = env::var("PROOF_HELPER_STORAGE_BACKEND") {
            self.storage.backend = match backend.to_lowercase().as_str() {
                // "dynamodb" => StorageBackend::DynamoDB,
                "sqlite" => StorageBackend::SQLite,
                _ => return Err(ConfigError::EnvError(format!("Invalid storage backend: {}", backend))),
            };
        }
        
        // DynamoDB configuration
        // if let Ok(table_name) = env::var("PROOF_HELPER_DYNAMODB_BRANCH_TABLE_NAME") {
        //     let dynamodb_config = self.storage.dynamodb.get_or_insert_with(Default::default);
        //     dynamodb_config.branch_table_name = table_name;
        // }

        // if let Ok(table_name) = env::var("PROOF_HELPER_DYNAMODB_EARLIEST_BLOCK_NUMBER_TABLE_NAME") {
        //     let dynamodb_config = self.storage.dynamodb.get_or_insert_with(Default::default);
        //     dynamodb_config.earliest_block_number_table_name = table_name;
        // }
        
        // if let Ok(region) = env::var("PROOF_HELPER_DYNAMODB_REGION") {
        //     if let Some(dynamodb_config) = &mut self.storage.dynamodb {
        //         dynamodb_config.region = region;
        //     }
        // }
        
        // if let Ok(endpoint_url) = env::var("PROOF_HELPER_DYNAMODB_ENDPOINT_URL") {
        //     if let Some(dynamodb_config) = &mut self.storage.dynamodb {
        //         dynamodb_config.endpoint_url = Some(endpoint_url);
        //     }
        // }

        // SQLite configuration
        if let Ok(db_path) = env::var("PROOF_HELPER_SQLITE_DB_PATH") {
            let sqlite_config = self.storage.sqlite.get_or_insert_default();
            sqlite_config.db_path = db_path;
        }
        
        // Logging configuration
        if let Ok(level) = env::var("PROOF_HELPER_LOG_LEVEL") {
            self.logging.level = level;
        }
        
        // Processing configuration
        if let Ok(max_block_diff) = env::var("PROOF_HELPER_MAX_BLOCK_DIFF") {
            self.processing.max_block_diff = max_block_diff.parse()
                .map_err(|_| ConfigError::EnvError("Invalid max block diff".to_string()))?;
        }
        
        Ok(())
    }
    
    /// Validate configuration
    fn validate(&self) -> ConfigResult<()> {
        // Validate DynamoDB configuration if backend is DynamoDB
        // if self.storage.backend == StorageBackend::DynamoDB {
            // let dynamodb_config = self.storage.dynamodb.as_ref()
            //     .ok_or_else(|| ConfigError::ValidationError("DynamoDB configuration required when backend is 'dynamodb'".to_string()))?;
            
            // if dynamodb_config.table_name.is_empty() {
            //     return Err(ConfigError::ValidationError("DynamoDB table name cannot be empty".to_string()));
            // }
            
            // if dynamodb_config.region.is_empty() {
            //     return Err(ConfigError::ValidationError("DynamoDB region cannot be empty".to_string()));
            // }
        // }
        
        // Validate logging level
        match self.logging.level.to_lowercase().as_str() {
            "trace" | "debug" | "info" | "warn" | "error" => {},
            _ => return Err(ConfigError::ValidationError(format!("Invalid log level: {}", self.logging.level))),
        }
        
        // Validate processing configuration
        if self.processing.max_block_diff == 0 {
            return Err(ConfigError::ValidationError("Max block diff must be greater than 0".to_string()));
        }
        
        Ok(())
    }
}

impl Default for ProofHelperConfig {
    fn default() -> Self {
        Self {
            storage: StorageConfig {
                backend: StorageBackend::SQLite,
                // dynamodb: Default::default(),
                sqlite: None,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
            },
            processing: ProcessingConfig {
                max_block_diff: 2000,
            },
        }
    }
}

impl Default for DynamoDbConfig {
    fn default() -> Self {
        Self {
            branch_table_name: "proof-helper-preimages".to_string(),
            earliest_block_number_table_name: "proof-helper-preimages-earliest-block-number".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: None,
            read_capacity: Some(5),
            write_capacity: Some(5),
        }
    }
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self { db_path: "proof-helper.db".to_string() }
    }
}
