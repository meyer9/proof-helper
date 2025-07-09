use serde::{Deserialize, Serialize};
use std::path::Path;
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
    /// Backend type: "mock" or "dynamodb"
    pub backend: StorageBackend,
    
    /// DynamoDB specific configuration (optional)
    pub dynamodb: Option<DynamoDbConfig>,
}

/// Storage backend types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    /// Mock storage for testing/development
    Mock,
    /// DynamoDB storage for production
    DynamoDB,
}

/// DynamoDB specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamoDbConfig {
    /// DynamoDB table name
    pub table_name: String,
    
    /// AWS region
    pub region: String,
    
    /// AWS endpoint URL (optional, for DynamoDB Local)
    pub endpoint_url: Option<String>,
    
    /// Read capacity units for table creation
    pub read_capacity: Option<u32>,
    
    /// Write capacity units for table creation
    pub write_capacity: Option<u32>,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level: "trace", "debug", "info", "warn", "error"
    pub level: String,
    
    /// Log progress every N items
    pub progress_interval: u64,
}

/// Processing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingConfig {
    /// Maximum number of blocks to process behind head
    pub max_block_diff: u64,
    
    /// Batch size for storage operations
    pub batch_size: usize,
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
    /// Load configuration from file with environment variable overrides
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> ConfigResult<Self> {
        let path = path.as_ref();
        
        // Read TOML file
        let config_str = std::fs::read_to_string(path)
            .map_err(|_| ConfigError::FileNotFound(path.to_string_lossy().to_string()))?;
        
        // Parse TOML
        let mut config: ProofHelperConfig = toml::from_str(&config_str)?;
        
        // Apply environment variable overrides
        config.apply_env_overrides()?;
        
        // Validate configuration
        config.validate()?;
        
        Ok(config)
    }
    
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
                "mock" => StorageBackend::Mock,
                "dynamodb" => StorageBackend::DynamoDB,
                _ => return Err(ConfigError::EnvError(format!("Invalid storage backend: {}", backend))),
            };
        }
        
        // DynamoDB configuration
        if let Ok(table_name) = env::var("PROOF_HELPER_DYNAMODB_TABLE_NAME") {
            let dynamodb_config = self.storage.dynamodb.get_or_insert_with(|| DynamoDbConfig {
                table_name: String::new(),
                region: "us-east-1".to_string(),
                endpoint_url: None,
                read_capacity: Some(5),
                write_capacity: Some(5),
            });
            dynamodb_config.table_name = table_name;
        }
        
        if let Ok(region) = env::var("PROOF_HELPER_DYNAMODB_REGION") {
            if let Some(dynamodb_config) = &mut self.storage.dynamodb {
                dynamodb_config.region = region;
            }
        }
        
        if let Ok(endpoint_url) = env::var("PROOF_HELPER_DYNAMODB_ENDPOINT_URL") {
            if let Some(dynamodb_config) = &mut self.storage.dynamodb {
                dynamodb_config.endpoint_url = Some(endpoint_url);
            }
        }
        
        // Logging configuration
        if let Ok(level) = env::var("PROOF_HELPER_LOG_LEVEL") {
            self.logging.level = level;
        }
        
        if let Ok(progress_interval) = env::var("PROOF_HELPER_PROGRESS_INTERVAL") {
            self.logging.progress_interval = progress_interval.parse()
                .map_err(|_| ConfigError::EnvError("Invalid progress interval".to_string()))?;
        }
        
        // Processing configuration
        if let Ok(max_block_diff) = env::var("PROOF_HELPER_MAX_BLOCK_DIFF") {
            self.processing.max_block_diff = max_block_diff.parse()
                .map_err(|_| ConfigError::EnvError("Invalid max block diff".to_string()))?;
        }
        
        if let Ok(batch_size) = env::var("PROOF_HELPER_BATCH_SIZE") {
            self.processing.batch_size = batch_size.parse()
                .map_err(|_| ConfigError::EnvError("Invalid batch size".to_string()))?;
        }
        
        Ok(())
    }
    
    /// Validate configuration
    fn validate(&self) -> ConfigResult<()> {
        // Validate DynamoDB configuration if backend is DynamoDB
        if self.storage.backend == StorageBackend::DynamoDB {
            let dynamodb_config = self.storage.dynamodb.as_ref()
                .ok_or_else(|| ConfigError::ValidationError("DynamoDB configuration required when backend is 'dynamodb'".to_string()))?;
            
            if dynamodb_config.table_name.is_empty() {
                return Err(ConfigError::ValidationError("DynamoDB table name cannot be empty".to_string()));
            }
            
            if dynamodb_config.region.is_empty() {
                return Err(ConfigError::ValidationError("DynamoDB region cannot be empty".to_string()));
            }
        }
        
        // Validate logging level
        match self.logging.level.to_lowercase().as_str() {
            "trace" | "debug" | "info" | "warn" | "error" => {},
            _ => return Err(ConfigError::ValidationError(format!("Invalid log level: {}", self.logging.level))),
        }
        
        // Validate processing configuration
        if self.processing.max_block_diff == 0 {
            return Err(ConfigError::ValidationError("Max block diff must be greater than 0".to_string()));
        }
        
        if self.processing.batch_size == 0 {
            return Err(ConfigError::ValidationError("Batch size must be greater than 0".to_string()));
        }
        
        Ok(())
    }
}

impl Default for ProofHelperConfig {
    fn default() -> Self {
        Self {
            storage: StorageConfig {
                backend: StorageBackend::Mock,
                dynamodb: None,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                progress_interval: 100,
            },
            processing: ProcessingConfig {
                max_block_diff: 2000,
                batch_size: 1000,
            },
        }
    }
}

impl Default for DynamoDbConfig {
    fn default() -> Self {
        Self {
            table_name: "proof-helper-preimages".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: None,
            read_capacity: Some(5),
            write_capacity: Some(5),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::NamedTempFile;
    
    #[test]
    fn test_default_config() {
        let config = ProofHelperConfig::default();
        assert_eq!(config.storage.backend, StorageBackend::Mock);
        assert_eq!(config.logging.level, "info");
        assert_eq!(config.logging.progress_interval, 100);
        assert_eq!(config.processing.max_block_diff, 1000);
        assert_eq!(config.processing.batch_size, 1000);
    }
    
    #[test]
    fn test_config_validation() {
        let mut config = ProofHelperConfig::default();
        
        // Valid config should pass
        assert!(config.validate().is_ok());
        
        // Invalid log level should fail
        config.logging.level = "invalid".to_string();
        assert!(config.validate().is_err());
        
        // Reset to valid
        config.logging.level = "info".to_string();
        
        // DynamoDB without config should fail
        config.storage.backend = StorageBackend::DynamoDB;
        assert!(config.validate().is_err());
        
        // DynamoDB with empty table name should fail
        config.storage.dynamodb = Some(DynamoDbConfig {
            table_name: String::new(),
            region: "us-east-1".to_string(),
            endpoint_url: None,
            read_capacity: Some(5),
            write_capacity: Some(5),
        });
        assert!(config.validate().is_err());
    }
    
    #[test]
    fn test_config_from_toml() {
        let toml_content = r#"
[storage]
backend = "mock"

[logging]
level = "debug"
progress_interval = 50

[processing]
max_block_diff = 500
batch_size = 2000
"#;
        
        let temp_file = NamedTempFile::new().unwrap();
        fs::write(temp_file.path(), toml_content).unwrap();
        
        let config = ProofHelperConfig::load_from_file(temp_file.path()).unwrap();
        assert_eq!(config.storage.backend, StorageBackend::Mock);
        assert_eq!(config.logging.level, "debug");
        assert_eq!(config.logging.progress_interval, 50);
        assert_eq!(config.processing.max_block_diff, 500);
        assert_eq!(config.processing.batch_size, 2000);
    }
    
    #[test]
    fn test_env_overrides() {
        // Set environment variables (unsafe operations in newer Rust)
        unsafe {
            env::set_var("PROOF_HELPER_STORAGE_BACKEND", "dynamodb");
            env::set_var("PROOF_HELPER_DYNAMODB_TABLE_NAME", "test-table");
            env::set_var("PROOF_HELPER_DYNAMODB_REGION", "us-west-2");
            env::set_var("PROOF_HELPER_LOG_LEVEL", "debug");
            env::set_var("PROOF_HELPER_PROGRESS_INTERVAL", "200");
        }
        
        let config = ProofHelperConfig::load_from_env().unwrap();
        assert_eq!(config.storage.backend, StorageBackend::DynamoDB);
        assert_eq!(config.storage.dynamodb.as_ref().unwrap().table_name, "test-table");
        assert_eq!(config.storage.dynamodb.as_ref().unwrap().region, "us-west-2");
        assert_eq!(config.logging.level, "debug");
        assert_eq!(config.logging.progress_interval, 200);
        
        // Clean up
        unsafe {
            env::remove_var("PROOF_HELPER_STORAGE_BACKEND");
            env::remove_var("PROOF_HELPER_DYNAMODB_TABLE_NAME");
            env::remove_var("PROOF_HELPER_DYNAMODB_REGION");
            env::remove_var("PROOF_HELPER_LOG_LEVEL");
            env::remove_var("PROOF_HELPER_PROGRESS_INTERVAL");
        }
    }
} 