use std::sync::Arc;

use reth::{primitives::{Account, Bytecode}, providers::{AccountReader, BlockHashReader, BytecodeReader, HashedPostStateProvider, ProviderResult, StateProofProvider, StateRootProvider, StorageRootProvider}, revm::{db::BundleState, primitives::{alloy_primitives::BlockNumber, Address, Bytes, StorageValue, B256}}};
use reth::providers::StateProvider;
use reth_trie::{updates::TrieUpdates, AccountProof, HashedPostState, HashedStorage, MultiProof, MultiProofTargets, StorageMultiProof, TrieInput};

use crate::storage::PreimageStore;

pub struct ExternalOverlayStateProviderRef<
    'a,
    P: PreimageStore,
> {
    /// Historical state provider for non-trie related tasks.
    pub(crate) historical: Box<dyn StateProvider + 'a>,

    /// Storage provider for state lookups.
    pub(crate) storage: P,

    pub(crate) block_number: BlockNumber,
}

impl<'a, P: PreimageStore> ExternalOverlayStateProviderRef<'a, P> {
    pub fn new(historical: Box<dyn StateProvider + 'a>, storage: P, block_number: BlockNumber) -> Self {
        Self {
            historical,
            storage,
            block_number,
        }
    }
}


impl<'a, P: PreimageStore> BlockHashReader for ExternalOverlayStateProviderRef<'a, P> {
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

impl<'a, P: PreimageStore> AccountReader for ExternalOverlayStateProviderRef<'a, P> {
    fn basic_account(&self, address: &Address) -> ProviderResult<Option<Account>> {
        self.historical.basic_account(address)
    }
}

impl<'a, P: PreimageStore> StateRootProvider for ExternalOverlayStateProviderRef<'a, P> {
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

impl<'a, P: PreimageStore> StorageRootProvider for ExternalOverlayStateProviderRef<'a, P> {
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

impl<'a, P: PreimageStore> StateProofProvider for ExternalOverlayStateProviderRef<'a, P> {
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

impl<'a, P: PreimageStore> HashedPostStateProvider for ExternalOverlayStateProviderRef<'a, P> {
    fn hashed_post_state(&self, bundle_state: &BundleState) -> HashedPostState {
        self.historical.hashed_post_state(bundle_state)
    }
}

impl<'a, P: PreimageStore> StateProvider for ExternalOverlayStateProviderRef<'a, P> {
    fn storage(
        &self,
        address: Address,
        storage_key: B256,
    ) -> ProviderResult<Option<StorageValue>> {
        self.historical.storage(address, storage_key)
    }
}

impl<'a, P: PreimageStore> BytecodeReader for ExternalOverlayStateProviderRef<'a, P> {
    fn bytecode_by_hash(&self, code_hash: &B256) -> ProviderResult<Option<Bytecode>> {
        self.historical.bytecode_by_hash(code_hash)
    }
}
