# Proof Helper ExEx

A blockchain Execution Extension (ExEx) for Reth that processes blocks and stores witness data preimages in a scalable storage backend. Built for Optimism-compatible chains, this ExEx extracts and stores trie preimages that can be used for proof generation and verification.

## Features

- **Flexible Storage Backends**: Support for both mock (development) and DynamoDB (production) storage
- **Efficient Batch Operations**: Optimized batch storage for high-throughput block processing
- **Block-based Indexing**: Secondary indexing on block numbers for efficient pruning and queries
- **Configuration Management**: TOML files and environment variable support
- **Comprehensive Error Handling**: Robust error handling with detailed logging
- **Production Ready**: Health checks, metrics, and monitoring capabilities

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Storage Backends](#storage-backends)
- [Usage](#usage)
- [DynamoDB Setup](#dynamodb-setup)
- [Environment Variables](#environment-variables)
- [Development](#development)
- [Testing](#testing)
- [Deployment](#deployment)
- [Troubleshooting](#troubleshooting)

## Installation

### Prerequisites

- Rust 1.70+ with 2024 edition
- AWS CLI configured (for DynamoDB backend)
- Docker (optional, for DynamoDB Local)

### Build from Source

```bash
git clone <repository-url>
cd proof-helper
cargo build --release
```

## Configuration

The proof helper supports flexible configuration through TOML files and environment variables.

### Configuration File

Create a `config.toml` file:

```toml
[storage]
# Storage backend: "mock" or "dynamodb"
backend = "mock"

# DynamoDB configuration (only used when backend = "dynamodb")
[storage.dynamodb]
table_name = "proof-helper-preimages"
region = "us-east-1"
# endpoint_url = "http://localhost:8000"  # Uncomment for DynamoDB Local
read_capacity = 5    # Optional: for provisioned mode
write_capacity = 5   # Optional: for provisioned mode

[logging]
# Log level: "trace", "debug", "info", "warn", "error"
level = "info"
# Log progress every N stored preimages
progress_interval = 100

[processing]
# Maximum number of blocks to process behind head
max_block_diff = 1000
# Batch size for storage operations
batch_size = 1000
```

### Environment Variables

All configuration options can be overridden with environment variables:

```bash
# Storage configuration
export PROOF_HELPER_STORAGE_BACKEND=dynamodb
export PROOF_HELPER_DYNAMODB_TABLE_NAME=my-table
export PROOF_HELPER_DYNAMODB_REGION=us-west-2
export PROOF_HELPER_DYNAMODB_ENDPOINT_URL=http://localhost:8000

# Logging configuration
export PROOF_HELPER_LOG_LEVEL=debug
export PROOF_HELPER_PROGRESS_INTERVAL=50

# Processing configuration
export PROOF_HELPER_MAX_BLOCK_DIFF=500
export PROOF_HELPER_BATCH_SIZE=25
```

## Storage Backends

### Mock Storage (Development)

The mock storage backend stores preimages in memory and is perfect for development and testing:

```toml
[storage]
backend = "mock"
```

**Features:**
- Zero configuration required
- In-memory storage
- All operations supported
- Perfect for development and testing

### DynamoDB Storage (Production)

The DynamoDB backend provides scalable, persistent storage for production deployments:

```toml
[storage]
backend = "dynamodb"

[storage.dynamodb]
table_name = "proof-helper-preimages"
region = "us-east-1"
```

**Features:**
- Automatic table creation with optimal schema
- Batch operations for high throughput
- Block-based secondary indexing for efficient pruning
- Support for both provisioned and on-demand billing
- Health checks and error handling

## Usage

### Basic Usage

Run with default configuration (environment variables):
```bash
./proof-helper
```

Run with a configuration file:
```bash
./proof-helper --config config.toml
```

Run with DynamoDB backend:
```bash
./proof-helper --config config-dynamodb.toml
```

### Integration with Reth

The proof helper runs as an ExEx (Execution Extension) within Reth. It automatically:

1. **Processes Blocks**: Listens for committed blocks from Reth
2. **Extracts Witness Data**: Generates execution witness records
3. **Stores Preimages**: Converts witness data to trie preimages and stores them
4. **Manages Storage**: Handles batch operations and error recovery

## DynamoDB Setup

### AWS DynamoDB (Production)

1. **Configure AWS Credentials**:
   ```bash
   aws configure
   # OR set environment variables:
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_DEFAULT_REGION=us-east-1
   ```

2. **Create IAM Policy**:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "dynamodb:CreateTable",
           "dynamodb:DescribeTable",
           "dynamodb:GetItem",
           "dynamodb:PutItem",
           "dynamodb:BatchGetItem",
           "dynamodb:BatchWriteItem",
           "dynamodb:Query",
           "dynamodb:Scan",
           "dynamodb:DeleteItem"
         ],
         "Resource": "arn:aws:dynamodb:*:*:table/proof-helper-*"
       }
     ]
   }
   ```

3. **Use Production Configuration**:
   ```toml
   [storage]
   backend = "dynamodb"
   
   [storage.dynamodb]
   table_name = "proof-helper-preimages"
   region = "us-east-1"
   # Omit endpoint_url for AWS DynamoDB
   ```

### DynamoDB Local (Development)

For local development with DynamoDB Local:

1. **Start DynamoDB Local**:
   ```bash
   docker run -p 8000:8000 amazon/dynamodb-local
   ```

2. **Configure for Local Development**:
   ```toml
   [storage]
   backend = "dynamodb"
   
   [storage.dynamodb]
   table_name = "proof-helper-preimages-local"
   region = "us-east-1"
   endpoint_url = "http://localhost:8000"
   read_capacity = 5
   write_capacity = 5
   ```

### Table Schema

The DynamoDB table is automatically created with the following schema:

- **Primary Key**: 
  - Partition Key: `hash` (String) - Keccak256 hash of preimage data
  - Sort Key: `block_number` (Number) - Block number when preimage was stored
- **Attributes**:
  - `preimage_data` (Binary) - The actual preimage data
- **Global Secondary Index**: 
  - `block-number-index`: Partition Key: `block_number`, Sort Key: `hash`
  - Used for efficient block-based queries and pruning

## Development

### Running Tests

```bash
# Run all tests
cargo test

# Run specific test modules
cargo test storage
cargo test config
```

### Mock Storage Development

For development, use the mock storage backend:

```bash
export PROOF_HELPER_STORAGE_BACKEND=mock
./proof-helper
```

### Adding New Storage Backends

To add a new storage backend:

1. Implement the `PreimageStore` trait in a new module
2. Add the backend to the `StorageBackend` enum in `src/config.rs`
3. Update the `create_storage()` function in `src/main.rs`
4. Add configuration options as needed

## Testing

### Unit Tests

```bash
cargo test
```

### Integration Tests with DynamoDB Local

1. Start DynamoDB Local:
   ```bash
   docker run -p 8000:8000 amazon/dynamodb-local
   ```

2. Run tests with DynamoDB backend:
   ```bash
   export PROOF_HELPER_STORAGE_BACKEND=dynamodb
   export PROOF_HELPER_DYNAMODB_ENDPOINT_URL=http://localhost:8000
   export PROOF_HELPER_DYNAMODB_TABLE_NAME=test-table
   cargo test
   ```

## Deployment

### Docker Deployment

Create a `Dockerfile`:

```dockerfile
FROM rust:1.70 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/proof-helper /usr/local/bin/
COPY config-dynamodb.toml /etc/proof-helper/config.toml
CMD ["proof-helper", "--config", "/etc/proof-helper/config.toml"]
```


## Monitoring and Observability

### Logging

The proof helper provides structured logging with configurable levels:

- `trace`: Extremely detailed debugging information
- `debug`: Detailed information for debugging
- `info`: General operational information (default)
- `warn`: Warning messages for potential issues
- `error`: Error messages for failures

### Metrics (Future Enhancement)

Planned metrics for production monitoring:

- **Blocks Processed**: Number of blocks successfully processed
- **Preimages Stored**: Number of preimages stored per block
- **Storage Latency**: Time taken for storage operations
- **Error Rate**: Frequency of storage or processing errors
- **Health Check Status**: Storage backend health status
- **Prometheus Endpoint**: HTTP endpoint for metrics collection

## Troubleshooting

### Common Issues

#### DynamoDB Connection Issues

```
Error: Failed to create DynamoDB store: Failed to describe table
```

**Solutions:**
- Check AWS credentials are configured correctly
- Verify IAM permissions for DynamoDB operations
- Ensure the AWS region is correct
- For DynamoDB Local, verify the service is running on the specified endpoint

#### Configuration Issues

```
Error: Failed to load configuration: Invalid configuration
```

**Solutions:**
- Validate TOML syntax in configuration file
- Check that all required fields are present
- Verify environment variable names and values
- Use `--config` flag to specify configuration file path

#### Memory Usage

For high-throughput scenarios, monitor memory usage and adjust:
- Reduce `batch_size` to lower memory consumption
- Increase `progress_interval` to reduce logging overhead
- Consider implementing streaming for large preimages

### Performance Tuning

#### DynamoDB Optimization

- **Batch Size**: Set to 25 (DynamoDB limit) for optimal throughput
- **Billing Mode**: Use on-demand for variable workloads, provisioned for predictable loads
- **Read/Write Capacity**: Start with conservative values and scale based on monitoring

#### Processing Optimization

- **Max Block Diff**: Adjust based on acceptable lag behind chain head
- **Concurrency**: Monitor CPU usage and adjust processing concurrency as needed

## Architecture

### Component Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│     Reth        │───▶│  Proof Helper    │───▶│  Storage        │
│   (Blockchain)  │    │     (ExEx)       │    │   Backend       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │                        │
                              ▼                        ▼
                       ┌──────────────┐         ┌─────────────┐
                       │ Configuration│         │   DynamoDB  │
                       │   Management │         │     or      │
                       └──────────────┘         │    Mock     │
                                               └─────────────┘
```

### Data Flow

1. **Block Processing**: Reth notifies ExEx of new committed blocks
2. **Witness Generation**: ExEx generates execution witness records
3. **Preimage Extraction**: Witness data is converted to trie preimages
4. **Batch Storage**: Preimages are stored in batches for efficiency
5. **Error Handling**: Failed operations are logged and retried as appropriate

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

### Development Guidelines

- Follow the incremental PR approach outlined in `IMPLEMENTATION_PLAN.md`
- Add comprehensive tests for new features
- Update documentation for configuration changes
- Use structured logging with appropriate log levels
- Handle errors gracefully with detailed error messages

## License

[Add your license information here]

## Support

For issues and questions:
- Check the [Troubleshooting](#troubleshooting) section
- Review logs with debug level enabled
- Open an issue with detailed error messages and configuration 