use async_trait::async_trait;
use jsonrpsee::{
    proc_macros::rpc,
};
use jsonrpsee_core::RpcResult;
use reth::{providers::{BlockIdReader, DatabaseProviderFactory, ProviderError, ProviderResult, StateProviderBox}, revm::{primitives::{Address}}, rpc::{api::eth::helpers::FullEthApi, server_types::eth::EthApiError, types::{serde_helpers::JsonStorageKey, BlockId, EIP1186AccountProofResponse}}};


use crate::{storage::ExternalStateStore, provider::ExternalOverlayStateProviderRef};

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



#[derive(Debug)]
pub struct EthApiExt<Eth, P, Provider> {
    eth_api: Eth,
    preimage_store: P,
    provider: Provider,
}

impl<Eth, P, Provider> EthApiExt<Eth, P, Provider>
where
    Eth: FullEthApi + Send + Sync + 'static,
    jsonrpsee_types::error::ErrorObject<'static>: From<Eth::Error>,
    P: ExternalStateStore + Clone + 'static,
    Provider: DatabaseProviderFactory + 'static,
    Provider::Provider: Send + Sync + 'static,
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
        let external_overlay_provider = ExternalOverlayStateProviderRef::new(historical_provider, self.preimage_store.clone(), self.provider.database_provider_ro()?, block_number);

        Ok(Box::new(external_overlay_provider))
    }
}

impl<Eth, P, Provider> EthApiExt<Eth, P, Provider> {
    pub fn new(eth_api: Eth, preimage_store: P, provider: Provider) -> Self {
        Self {
            eth_api,
            preimage_store,
            provider,
        }
    }
}

#[async_trait]
impl<Eth, P, Provider> EthApiOverrideServer for EthApiExt<Eth, P, Provider>
where
    Eth: FullEthApi + Send + Sync + 'static,
    jsonrpsee_types::error::ErrorObject<'static>: From<Eth::Error>,
    P: ExternalStateStore + Clone + 'static,
    Provider: DatabaseProviderFactory + 'static,
    Provider::Provider: Send + Sync + 'static,
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