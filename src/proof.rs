use crate::storage::{ExternalHashedCursor, ExternalStateStore, ExternalTrieCursor as ExternalDBTrieCursor};
use alloy_primitives::{keccak256, map::HashMap, Address, B256, U256};
use reth::primitives::Account;
use reth_db_api::{DatabaseError};
use reth_execution_errors::StateProofError;
use reth_trie::{
    hashed_cursor::{HashedCursor, HashedCursorFactory, HashedPostStateCursorFactory, HashedStorageCursor}, proof::{Proof, StorageProof}, trie_cursor::{InMemoryTrieCursorFactory, TrieCursor, TrieCursorFactory}, AccountProof, BranchNodeCompact, HashedPostStateSorted, HashedStorage, MultiProof, MultiProofTargets, Nibbles, StorageMultiProof, TrieInput
};

pub struct ExternalTrieCursor<C>(pub(crate) C);

impl<C> ExternalTrieCursor<C> {
    pub fn new(preimage_cursor: C) -> Self {
        Self(preimage_cursor)
    }
}

impl<C: ExternalDBTrieCursor + Send + Sync> TrieCursor for ExternalTrieCursor<C> {
    fn seek_exact(&mut self, key: Nibbles) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        self.0.seek_exact(key)
            .map_err(Into::into)
    }
    
    
    fn seek(&mut self, key: Nibbles) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        self.0.seek(key)
            .map_err(Into::into)
    }

    fn next(&mut self) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        self.0.next()
            .map_err(Into::into)
    }

    fn current(&mut self) -> Result<Option<Nibbles>, DatabaseError> {
        self.0.current()
            .map_err(Into::into)
    }
}

#[derive(Clone)]
pub struct ExternalTrieCursorFactory<P> {
    preimage_store: P,
    block_number: u64,
}

impl<P> ExternalTrieCursorFactory<P> {
    pub fn new(preimage_store: P, block_number: u64) -> Self {
        Self { preimage_store, block_number }
    }
}

impl<P: ExternalStateStore> TrieCursorFactory for ExternalTrieCursorFactory<P> {
    type AccountTrieCursor = ExternalTrieCursor<P::TrieCursor>;
    type StorageTrieCursor = ExternalTrieCursor<P::TrieCursor>;

    fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor, DatabaseError> {
        Ok(ExternalTrieCursor::new(self.preimage_store.trie_cursor(None, self.block_number).map_err(Into::<DatabaseError>::into)?))
    }
    
    fn storage_trie_cursor(&self, hashed_address: B256) -> Result<Self::StorageTrieCursor, DatabaseError> {
        Ok(ExternalTrieCursor::new(self.preimage_store.trie_cursor(Some(hashed_address), self.block_number).map_err(Into::<DatabaseError>::into)?))
    }
}

#[derive(Clone)]
pub struct ExternalHashedAccountCursor<C>(pub(crate) C);

impl<C> ExternalHashedAccountCursor<C> {
    pub fn new(cursor: C) -> Self {
        Self(cursor)
    }
}

impl<C: ExternalHashedCursor<Value = Account> + Send + Sync> HashedCursor for ExternalHashedAccountCursor<C> {
    type Value = Account;

    fn seek(&mut self, key: B256) -> Result<Option<(B256, Self::Value)>, DatabaseError> {
        self.0.seek(key)
            .map_err(Into::into)
    }

    fn next(&mut self) -> Result<Option<(B256, Self::Value)>, DatabaseError> {
        self.0.next()
            .map_err(Into::into)
    }
}

#[derive(Clone)]
pub struct ExternalHashedStorageCursor<C>(pub(crate) C);

impl<C> ExternalHashedStorageCursor<C> {
    pub fn new(cursor: C) -> Self {
        Self(cursor)
    }
}

impl<C: ExternalHashedCursor<Value = U256> + Send + Sync> HashedCursor for ExternalHashedStorageCursor<C> {
    type Value = U256;

    fn seek(&mut self, key: B256) -> Result<Option<(B256, Self::Value)>, DatabaseError> {
        self.0.seek(key)
            .map_err(Into::into)
    }

    fn next(&mut self) -> Result<Option<(B256, Self::Value)>, DatabaseError> {
        self.0.next()
            .map_err(Into::into)
    }
}

impl<C: ExternalHashedCursor<Value = U256> + Send + Sync> HashedStorageCursor for ExternalHashedStorageCursor<C> {
    fn is_storage_empty(&mut self) -> Result<bool, DatabaseError> {
        self.0.is_storage_empty()
            .map_err(Into::into)
    }
}

#[derive(Clone)]
pub struct ExternalHashedAccountCursorFactory<P> {
    preimage_store: P,
    block_number: u64,
}

impl<P> ExternalHashedAccountCursorFactory<P> {
    pub fn new(preimage_store: P, block_number: u64) -> Self {
        Self { preimage_store, block_number }
    }
}

impl<P: ExternalStateStore> HashedCursorFactory for ExternalHashedAccountCursorFactory<P> {
    type AccountCursor = ExternalHashedAccountCursor<P::AccountHashedCursor>;
    type StorageCursor = ExternalHashedStorageCursor<P::StorageCursor>;
    
    fn hashed_account_cursor(&self) -> Result<Self::AccountCursor, DatabaseError> {
        Ok(ExternalHashedAccountCursor::new(self.preimage_store.account_hashed_cursor(self.block_number).map_err(Into::<DatabaseError>::into)?))
    }

    fn hashed_storage_cursor(&self, hashed_address: B256) -> Result<Self::StorageCursor, DatabaseError> {
        Ok(ExternalHashedStorageCursor::new(self.preimage_store.storage_hashed_cursor(hashed_address, self.block_number).map_err(Into::<DatabaseError>::into)?))
    }
}

/// Extends [`Proof`] with operations specific for working with a database transaction.
pub trait DatabaseProof<P> {
    fn from_tx(preimage_store: P, block_number: u64) -> Self;

    /// Generates the state proof for target account based on [`TrieInput`].
    fn overlay_account_proof(
        preimage_store: P,
        block_number: u64,
        input: TrieInput,
        address: Address,
        slots: &[B256],
    ) -> Result<AccountProof, StateProofError>;

    /// Generates the state [`MultiProof`] for target hashed account and storage keys.
    fn overlay_multiproof(
        preimage_store: P,
        block_number: u64,
        input: TrieInput,
        targets: MultiProofTargets,
    ) -> Result<MultiProof, StateProofError>;
}

impl<P: ExternalStateStore + Clone> DatabaseProof<P>
    for Proof<ExternalTrieCursorFactory<P>, ExternalHashedAccountCursorFactory<P>>
{
    /// Create a new [Proof] instance from database transaction.
    fn from_tx(preimage_store: P, block_number: u64) -> Self {
        Self::new(ExternalTrieCursorFactory::new(preimage_store.clone(), block_number), ExternalHashedAccountCursorFactory::new(preimage_store, block_number))
    }

    fn overlay_account_proof(
        preimage_store: P,
        block_number: u64,
        input: TrieInput,
        address: Address,
        slots: &[B256],
    ) -> Result<AccountProof, StateProofError> {
        let nodes_sorted = input.nodes.into_sorted();
        let state_sorted = input.state.into_sorted();
        Self::from_tx(preimage_store.clone(), block_number)
            .with_trie_cursor_factory(InMemoryTrieCursorFactory::new(
                ExternalTrieCursorFactory::new(preimage_store.clone(), block_number),
                &nodes_sorted,
            ))
            .with_hashed_cursor_factory(HashedPostStateCursorFactory::new(
                ExternalHashedAccountCursorFactory::new(preimage_store, block_number),
                &state_sorted,
            ))
            .with_prefix_sets_mut(input.prefix_sets)
            .account_proof(address, slots)
    }

    fn overlay_multiproof(
        preimage_store: P,
        block_number: u64,
        input: TrieInput,
        targets: MultiProofTargets,
    ) -> Result<MultiProof, StateProofError> {
        let nodes_sorted = input.nodes.into_sorted();
        let state_sorted = input.state.into_sorted();
        Self::from_tx(preimage_store.clone(), block_number)
            .with_trie_cursor_factory(InMemoryTrieCursorFactory::new(
                ExternalTrieCursorFactory::new(preimage_store.clone(), block_number),
                &nodes_sorted,
            ))
            .with_hashed_cursor_factory(HashedPostStateCursorFactory::new(
                ExternalHashedAccountCursorFactory::new(preimage_store, block_number),
                &state_sorted,
            ))
            .with_prefix_sets_mut(input.prefix_sets)
            .multiproof(targets)
    }
}

/// Extends [`StorageProof`] with operations specific for working with a database transaction.
pub trait DatabaseStorageProof<P> {
    /// Create a new [`StorageProof`] from database transaction and account address.
    fn from_tx(preimage_store: P, block_number: u64, address: Address) -> Self;

    /// Generates the storage proof for target slot based on [`TrieInput`].
    fn overlay_storage_proof(
        preimage_store: P,
        block_number: u64,
        address: Address,
        slot: B256,
        storage: HashedStorage,
    ) -> Result<reth_trie::StorageProof, StateProofError>;

    /// Generates the storage multiproof for target slots based on [`TrieInput`].
    fn overlay_storage_multiproof(
        preimage_store: P,
        block_number: u64,
        address: Address,
        slots: &[B256],
        storage: HashedStorage,
    ) -> Result<StorageMultiProof, StateProofError>;
}

impl<P: ExternalStateStore + Clone> DatabaseStorageProof<P>
    for StorageProof<ExternalTrieCursorFactory<P>, ExternalHashedAccountCursorFactory<P>>
{
    fn from_tx(preimage_store: P, block_number: u64, address: Address) -> Self {
        Self::new(ExternalTrieCursorFactory::new(preimage_store.clone(), block_number), ExternalHashedAccountCursorFactory::new(preimage_store, block_number), address)
    }

    fn overlay_storage_proof(
        preimage_store: P,
        block_number: u64,
        address: Address,
        slot: B256,
        storage: HashedStorage,
    ) -> Result<reth_trie::StorageProof, StateProofError> {
        let hashed_address = keccak256(address);
        let prefix_set = storage.construct_prefix_set();
        let state_sorted = HashedPostStateSorted::new(
            Default::default(),
            HashMap::from_iter([(hashed_address, storage.into_sorted())]),
        );
        Self::from_tx(preimage_store.clone(), block_number, address)
            .with_hashed_cursor_factory(HashedPostStateCursorFactory::new(
                ExternalHashedAccountCursorFactory::new(preimage_store, block_number),
                &state_sorted,
            ))
            .with_prefix_set_mut(prefix_set)
            .storage_proof(slot)
    }

    fn overlay_storage_multiproof(
        preimage_store: P,
        block_number: u64,
        address: Address,
        slots: &[B256],
        storage: HashedStorage,
    ) -> Result<StorageMultiProof, StateProofError> {
        let hashed_address = keccak256(address);
        let targets = slots.iter().map(keccak256).collect();
        let prefix_set = storage.construct_prefix_set();
        let state_sorted = HashedPostStateSorted::new(
            Default::default(),
            HashMap::from_iter([(hashed_address, storage.into_sorted())]),
        );
        Self::from_tx(preimage_store.clone(), block_number, address)
            .with_hashed_cursor_factory(HashedPostStateCursorFactory::new(
                ExternalHashedAccountCursorFactory::new(preimage_store, block_number),
                &state_sorted,
            ))
            .with_prefix_set_mut(prefix_set)
            .storage_multiproof(targets)
    }
}
