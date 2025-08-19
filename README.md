# Proof Helper ExEx

A blockchain Execution Extension (ExEx) for Reth that processes blocks and stores witness data preimages in a scalable storage backend. Built for Optimism-compatible chains, this ExEx extracts and stores trie preimages that can be used for proof generation and verification.

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [DynamoDB Local (Development)](#dynamodb-local-development)

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

## Usage

### Basic Usage

First, run the `db dump-trie` command using the `reth` CLI to dump the current state trie to the DynamoDB table.

Then, to collect preimages for blocks going forward, run the `proof-helper` ExEx from the same snapshot (or any snapshot at the same block).
This will ensure that you are able to prove all blocks going forward.

Run with default configuration (environment variables):
```bash
./proof-helper
```

## DynamoDB Local (Development)

For local development with DynamoDB Local:

1. **Start DynamoDB Local**:
   ```bash
   docker run -p 8000:8000 amazon/dynamodb-local
   ```

2. **Configure for Local Development**:
    ```bash
    export PROOF_HELPER_DYNAMODB_ENDPOINT_URL=http://localhost:8000
    export AWS_ACCESS_KEY_ID=dummy
    export AWS_SECRET_ACCESS_KEY=dummy
    export AWS_DEFAULT_REGION=us-east-1
    ```
