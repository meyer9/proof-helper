
use std::sync::Arc;

use async_trait::async_trait;
use jsonrpsee::{
    proc_macros::rpc,
};
use jsonrpsee_core::RpcResult;
use reth::{primitives::{Account, Bytecode}, providers::{AccountReader, BlockHashReader, BlockIdReader, BytecodeReader, HashedPostStateProvider, ProviderError, ProviderResult, StateProofProvider, StateProviderBox, StateRootProvider, StorageRootProvider}, revm::{db::BundleState, primitives::{alloy_primitives::BlockNumber, Address, Bytes, StorageValue, B256}}, rpc::{api::eth::helpers::FullEthApi, server_types::eth::EthApiError, types::{serde_helpers::JsonStorageKey, BlockId, EIP1186AccountProofResponse}}};
use reth::providers::StateProvider;
use op_alloy_network::Optimism;
use reth_trie::{updates::TrieUpdates, AccountProof, HashedPostState, HashedStorage, MultiProof, MultiProofTargets, StorageMultiProof, TrieInput};

use crate::storage::PreimageStore;

#[cfg_attr(not(test), rpc(server, namespace = "eth"))]
#[cfg_attr(test, rpc(server, client, namespace = "eth"))]
pub trait EthApiOverride {
    /// Returns the account and storage values of the specified account including the Merkle-proof.
    /// This call can be used to verify that the data you are pulling from is not tampered with.
    #[method(name = "getProof")]
    async fn get_proof(
        &self,
        address: Address,
        keys: Vec<JsonStorageKey>,
        block_number: Option<BlockId>,
    ) -> RpcResult<EIP1186AccountProofResponse>;
}


// #[cfg_attr(not(test), rpc(server, namespace = "debug"))]
// #[cfg_attr(test, rpc(server, client, namespace = "debug"))]
// pub trait DebugApiOverride<Attributes> {
//     #[method(name = "executePayload")]
//     async fn execute_payload(
//         &self,
//         parent_block_hash: B256,
//         attributes: Attributes,
//     ) -> RpcResult<ExecutionWitness>;


//     #[method(name = "executionWitness")]
//     async fn execution_witness(&self, block: BlockNumberOrTag)
//         -> RpcResult<ExecutionWitness>;
// }

pub struct ExternalOverlayStateProviderRef<
    'a,
> {
    /// Historical state provider for non-trie related tasks.
    pub(crate) historical: Box<dyn StateProvider + 'a>,

    /// Storage provider for state lookups.
    pub(crate) storage: Arc<dyn PreimageStore>,

    pub(crate) block_number: BlockNumber,
}

impl<'a> ExternalOverlayStateProviderRef<'a> {
    fn new(historical: Box<dyn StateProvider + 'a>, storage: Arc<dyn PreimageStore>, block_number: BlockNumber) -> Self {
        Self {
            historical,
            storage,
            block_number,
        }
    }
}


impl<'a> BlockHashReader for ExternalOverlayStateProviderRef<'a> {
    fn block_hash(&self, number: BlockNumber) -> ProviderResult<Option<B256>> {
        self.historical.block_hash(number)
    }

    fn canonical_hashes_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> ProviderResult<Vec<B256>> {
        self.historical.canonical_hashes_range(start, end)
    }
}

impl<'a> AccountReader for ExternalOverlayStateProviderRef<'a> {
    fn basic_account(&self, address: &Address) -> ProviderResult<Option<Account>> {
        self.historical.basic_account(address)
    }
}

impl<'a> StateRootProvider for ExternalOverlayStateProviderRef<'a> {
    fn state_root(&self, state: HashedPostState) -> ProviderResult<B256> {
        self.state_root_from_nodes(TrieInput::from_state(state))
    }

    fn state_root_from_nodes(&self, input: TrieInput) -> ProviderResult<B256> {
        self.historical.state_root_from_nodes(input)
    }

    fn state_root_with_updates(
        &self,
        state: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        self.state_root_from_nodes_with_updates(TrieInput::from_state(state))
    }

    fn state_root_from_nodes_with_updates(
        &self,
        input: TrieInput,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        self.historical.state_root_from_nodes_with_updates(input)
    }
}

impl<'a> StorageRootProvider for ExternalOverlayStateProviderRef<'a> {
    // TODO: Currently this does not reuse available in-memory trie nodes.
    fn storage_root(&self, address: Address, storage: HashedStorage) -> ProviderResult<B256> {
        self.historical.storage_root(address, storage)
    }

    // TODO: Currently this does not reuse available in-memory trie nodes.
    fn storage_proof(
        &self,
        address: Address,
        slot: B256,
        storage: HashedStorage,
    ) -> ProviderResult<reth_trie::StorageProof> {
        self.historical.storage_proof(address, slot, storage)
    }

    // TODO: Currently this does not reuse available in-memory trie nodes.
    fn storage_multiproof(
        &self,
        address: Address,
        slots: &[B256],
        storage: HashedStorage,
    ) -> ProviderResult<StorageMultiProof> {
        self.historical.storage_multiproof(address, slots, storage)
    }
}

impl<'a> StateProofProvider for ExternalOverlayStateProviderRef<'a> {
    fn proof(
        &self,
        input: TrieInput,
        address: Address,
        slots: &[B256],
    ) -> ProviderResult<AccountProof> {
        self.historical.proof(input, address, slots)
    }

    fn multiproof(
        &self,
        input: TrieInput,
        targets: MultiProofTargets,
    ) -> ProviderResult<MultiProof> {
        self.historical.multiproof(input, targets)
    }

    fn witness(&self, input: TrieInput, target: HashedPostState) -> ProviderResult<Vec<Bytes>> {
        self.historical.witness(input, target)
    }
}

impl<'a> HashedPostStateProvider for ExternalOverlayStateProviderRef<'a> {
    fn hashed_post_state(&self, bundle_state: &BundleState) -> HashedPostState {
        self.historical.hashed_post_state(bundle_state)
    }
}

impl<'a> StateProvider for ExternalOverlayStateProviderRef<'a> {
    fn storage(
        &self,
        address: Address,
        storage_key: B256,
    ) -> ProviderResult<Option<StorageValue>> {
        self.historical.storage(address, storage_key)
    }
}

impl<'a> BytecodeReader for ExternalOverlayStateProviderRef<'a> {
    fn bytecode_by_hash(&self, code_hash: &B256) -> ProviderResult<Option<Bytecode>> {
        self.historical.bytecode_by_hash(code_hash)
    }
}



#[derive(Debug)]
pub struct EthApiExt<Eth> {
    eth_api: Eth,
    preimage_store: Arc<dyn PreimageStore>,
}

impl<Eth> EthApiExt<Eth>
where
    Eth: FullEthApi<NetworkTypes = Optimism> + Send + Sync + 'static,
    jsonrpsee_types::error::ErrorObject<'static>: From<Eth::Error>,
 {
    async fn state_provider(&self, block_id: Option<BlockId>) -> ProviderResult<StateProviderBox> {
        let block_id = block_id.unwrap_or_default();
        // Check whether the distance to the block exceeds the maximum configured window.
        let block_number = self.eth_api.provider()
            .block_number_for_id(block_id)?
            .ok_or(EthApiError::HeaderNotFound(block_id))
            .map_err(|e| ProviderError::other(e))?;

        let historical_provider = self.eth_api.state_at_block_id(block_id)
            .await
            .map_err(|e| ProviderError::other(e))?;
        let external_overlay_provider = ExternalOverlayStateProviderRef::new(historical_provider, self.preimage_store.clone(), block_number);

        Ok(Box::new(external_overlay_provider))
    }
}

impl<Eth> EthApiExt<Eth> {
    pub fn new(eth_api: Eth, preimage_store: Arc<dyn PreimageStore>) -> Self {
        Self {
            eth_api,
            preimage_store,
        }
    }
}

#[async_trait]
impl<Eth> EthApiOverrideServer for EthApiExt<Eth>
where
    Eth: FullEthApi<NetworkTypes = Optimism> + Send + Sync + 'static,
    jsonrpsee_types::error::ErrorObject<'static>: From<Eth::Error>,
{

    async fn get_proof(
        &self,
        address: Address,
        keys: Vec<JsonStorageKey>,
        block_number: Option<BlockId>,
    ) -> RpcResult<EIP1186AccountProofResponse> {
        // TODO:
        let state = self.state_provider(block_number)
            .await
            .map_err(Into::into)?;
        let storage_keys = keys.iter().map(|key| key.as_b256()).collect::<Vec<_>>();

        let proof = state
            .proof(Default::default(), address, &storage_keys)
            .map_err(Into::into)?;

        return Ok(proof.into_eip1186_response(keys))
    }
}