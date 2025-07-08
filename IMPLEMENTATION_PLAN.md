# Proof Helper Implementation Plan - PR Sequence

## Overview
This document outlines the implementation plan for storing witness data (preimages) in DynamoDB using the `PreimageStore` trait, organized as a series of incremental PRs.

## Completed ✅
- [x] **PR #0**: Created `PreimageStore` trait with clean interface for hash -> preimage mapping
- [x] Added support for batch operations for efficient storage
- [x] Implemented block number secondary indexing for pruning
- [x] Created `MockPreimageStore` implementation for testing
- [x] Added comprehensive test coverage for the trait

## PR Sequence

### **PR #1: Basic ProofHelper Integration** ✅ Complete
*Goal: Wire up ProofHelper to accept and use a PreimageStore (using mock)*

**Changes:**
- [x] Add `PreimageStore` field to `ProofHelper` struct
- [x] Update `ProofHelper::new()` to accept a `PreimageStore` implementation
- [x] Update `main()` to create and pass a `MockPreimageStore`
- [x] Add basic logging when storing preimages
- [x] Enhanced `process_block` to store all witness data types (state, codes, keys) using proper keccak256 hashing

**Testing:**
- [x] Verify ProofHelper compiles and runs with mock store
- [x] All storage tests passing (5/5)

**Implementation Summary:**
- Modified `ProofHelper` struct to include `storage: Arc<dyn PreimageStore>` field
- Updated constructor to accept storage parameter
- Enhanced `process_block()` method to store all witness data using batch operations
- Added progress logging every 100 items
- Uses proper keccak256 hashing for all data types
- Successfully integrated with mock storage implementation

**Dependencies:** None

---

### **PR #2: Preimage Conversion Logic** ✅ Complete (Implemented in PR #1)
*Goal: Convert witness data to proper trie preimages*

**Changes:**
- [x] ~~Create `src/preimage.rs` module~~ - Integrated directly into `process_block`
- [x] Use `keccak256` for hashing preimage data
- [x] Update `process_block` to use proper conversion (state, codes, keys)
- [x] Add proper error handling for conversion failures

**Implementation Summary:**
- Proper `keccak256` hashing implemented directly in `process_block()`
- All witness data types (state, codes, keys) properly converted to preimages
- No separate module needed - conversion logic is straightforward

**Dependencies:** PR #1

---

### **PR #3: Configuration Management** ✅ Complete
*Goal: Add configuration system for storage backends*

**Changes:**
- [x] Create `src/config.rs` module with comprehensive configuration system:
  - `ProofHelperConfig`, `StorageConfig`, `LoggingConfig`, `ProcessingConfig` structs
  - Environment variable support with `PROOF_HELPER_*` prefix
  - Configuration validation for all settings
  - Support for DynamoDB-specific configuration
- [x] Add `--config` CLI argument to specify config file
- [x] Support both TOML files and environment variables with override capability
- [x] Add `backend` setting to choose between "mock" and "dynamodb"
- [x] Integration with main application using configuration values

**Testing:**
- [x] Configuration parsing tests (4/4 passing)
- [x] Environment variable override tests
- [x] Configuration validation tests
- [x] TOML file loading tests

**Implementation Summary:**
- Created comprehensive configuration system supporting TOML files and environment variables
- Added CLI argument parsing for config file path
- Updated `ProofHelper::run()` to accept and use configuration
- Updated `process_block()` to use configurable progress interval
- Created sample `config.toml` file with all options documented
- Storage backend creation based on configuration (currently mock with DynamoDB placeholder)

**Dependencies:** PR #2

---

### **PR #4: Basic DynamoDB Implementation** ✅ Complete
*Goal: Add DynamoDB backend (without advanced features)*

**Changes:**
- [x] Add AWS SDK dependencies to `Cargo.toml` (`aws-config`, `aws-sdk-dynamodb`, `hex`)
- [x] Create `src/dynamodb.rs` with `DynamoDbPreimageStore` struct
- [x] Implement complete CRUD operations (including batch operations)
- [x] Add DynamoDB table creation utility with GSI support
- [x] Comprehensive error handling (map AWS errors to `PreimageStorageError`)
- [x] Support for both AWS DynamoDB and DynamoDB Local
- [x] Automatic table creation and health checks

**Implementation Details:**
- **Table Schema**: Primary key: `hash` (String), `block_number` (Number)
- **GSI**: `block_number` (partition key), `hash` (sort key) for block-based queries
- **Batch Operations**: Implemented with DynamoDB's 25-item batch limits
- **Connection Management**: Automatic table creation and health checks
- **Error Handling**: Comprehensive error mapping to `PreimageStorageError`

**Testing:**
- [x] All existing tests pass (9/9)
- [x] Integration with configuration system
- [x] Proper async/await implementation
- [x] Compilation verified with AWS SDK dependencies

**Note**: This PR actually includes functionality originally planned for PR #5 and #6, providing a complete DynamoDB implementation ready for production use.

**Dependencies:** PR #3

---

### **PR #5: DynamoDB Integration Testing** ✅ Complete (Included in PR #4)
*Goal: Test DynamoDB backend with local instance*

**Changes:**
- [x] ~~Implement batch operations~~ - Already implemented in PR #4
- [x] ~~Add batch size optimization~~ - Already implemented in PR #4  
- [x] ~~Implement batch get operations~~ - Already implemented in PR #4
- [x] Test with DynamoDB Local to verify all functionality works

**Testing:**
- [x] All operations work with DynamoDB Local
- [x] Batch operations handle DynamoDB limits correctly
- [x] Table creation and schema validation
- [x] Error handling with real DynamoDB responses

**Note**: Originally planned as separate PR, but functionality was included in the comprehensive PR #4 implementation.

**Dependencies:** PR #4

---

### **PR #6: Secondary Index and Pruning** ✅ Complete (Included in PR #4)
*Goal: Add block-based indexing for efficient pruning*

**Changes:**
- [x] ~~Add GSI creation for block_number~~ - Already implemented in PR #4
- [x] ~~Implement `prune_before_block`~~ - Already implemented in PR #4
- [x] ~~Implement `count_preimages_for_block` and `get_hashes_for_block`~~ - Already implemented in PR #4
- [x] ~~Add pruning job scheduling~~ - Not needed for basic functionality

**Note**: GSI and all pruning functionality was included in the comprehensive PR #4 implementation.

**Dependencies:** PR #4

---

### **PR #7: Enhanced Error Handling**
*Goal: Add retry logic and resilience for production*

**Changes:**
- [ ] Implement exponential backoff retry logic for DynamoDB operations
- [ ] Add DynamoDB throttling detection and handling
- [ ] Add detailed error logging with correlation IDs
- [ ] Graceful degradation (continue processing on storage errors)

**Testing:**
- [ ] Retry logic tests with simulated failures
- [ ] Throttling simulation tests  
- [ ] Error recovery tests

**Dependencies:** PR #4

---

### **PR #8: Performance Optimizations**
*Goal: Optimize for production workloads*

**Changes:**
- [ ] Optimize batch sizes based on data size and latency
- [ ] Implement async processing with configurable concurrency
- [ ] Memory usage optimizations for large preimages
- [ ] Connection pooling optimizations

**Testing:**
- [ ] Performance benchmarks vs mock storage
- [ ] Memory usage tests under load
- [ ] Concurrency stress tests
- [ ] Latency optimization validation

**Dependencies:** PR #7

---

### **PR #9: Prometheus Metrics**
*Goal: Add basic observability for production*

**Changes:**
- [ ] Add Prometheus metrics for all storage operations
- [ ] Add block processing metrics (blocks/sec, preimages/block)
- [ ] Add error rate and latency metrics
- [ ] Add health check metrics
- [ ] Simple metrics HTTP endpoint

**Testing:**
- [ ] Metrics collection tests
- [ ] Metrics accuracy validation
- [ ] HTTP endpoint tests

**Dependencies:** PR #8


## Getting Started

### Current State
We have a complete trait definition and mock implementation ready. The next step is **PR #1** which integrates the mock store with ProofHelper.

### Quick Start for PR #1
```bash
# The basic structure needed:
1. Add PreimageStore field to ProofHelper 
2. Update constructor to accept the store
3. Update main() to create MockPreimageStore
4. Add basic storage calls in process_block
5. Add logging to verify it works
```

### Key Design Decisions
- **Incremental**: Each PR adds one focused capability
- **Testable**: Every PR includes comprehensive tests
- **Deployable**: PRs 1-3 can be deployed to staging
- **Fallback**: Mock store allows development without AWS setup

### Reference Information

**DynamoDB Table Schema (for PR #4+):**
```
Table: proof-helper-preimages
Primary Key: hash (String) - B256 hex encoded
Attributes: preimage (Binary), block_number (Number)
GSI: block-number-index (partition: block_number, sort: hash)
```

**Configuration Example (for PR #3+):**
```toml
[storage]
backend = "mock"  # or "dynamodb"
table_name = "proof-helper-preimages"
region = "us-east-1"
batch_size = 25
```

**Dependencies Added Per PR:**
- PR #4: `aws-config`, `aws-sdk-dynamodb`
- PR #3: `serde`, `toml`, `config`
- PR #8: `flate2` (compression)
- PR #9: `prometheus`, `tracing`

This PR-based approach ensures steady progress with deployable milestones. 