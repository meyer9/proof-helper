use std::sync::Arc;

use crate::{storage::{PreimageStore, PreimageStoreCursor}};
use alloy_primitives::{keccak256, map::HashMap, Address, B256};
use reth_db_api::{transaction::DbTx, DatabaseError};
use reth_execution_errors::StateProofError;
use reth_trie::{
    hashed_cursor::HashedPostStateCursorFactory, proof::{Proof, StorageProof}, trie_cursor::{InMemoryTrieCursorFactory, TrieCursor, TrieCursorFactory}, AccountProof, BranchNodeCompact, HashedPostStateSorted, HashedStorage, MultiProof, MultiProofTargets, Nibbles, StorageMultiProof, StoredNibbles, TrieInput
};
use reth_trie_db::DatabaseHashedCursorFactory;

pub struct ExternalTrieCursor<C>(pub(crate) C);

impl<C> ExternalTrieCursor<C> {
    pub fn new(preimage_cursor: C) -> Self {
        Self(preimage_cursor)
    }
}

impl<C: PreimageStoreCursor + Send + Sync> TrieCursor for ExternalTrieCursor<C> {
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

impl<P: PreimageStore> TrieCursorFactory for ExternalTrieCursorFactory<P> {
    type AccountTrieCursor = ExternalTrieCursor<P::Cursor>;
    type StorageTrieCursor = ExternalTrieCursor<P::Cursor>;

    fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor, DatabaseError> {
        Ok(ExternalTrieCursor::new(self.preimage_store.cursor(None, self.block_number).map_err(Into::<DatabaseError>::into)?))
    }
    
    fn storage_trie_cursor(&self, hashed_address: B256) -> Result<Self::StorageTrieCursor, DatabaseError> {
        Ok(ExternalTrieCursor::new(self.preimage_store.cursor(Some(hashed_address), self.block_number).map_err(Into::<DatabaseError>::into)?))
    }
}

/// Extends [`Proof`] with operations specific for working with a database transaction.
pub trait DatabaseProof<'a, TX, P> {
    fn from_tx(tx: &'a TX, preimage_store: P, block_number: u64) -> Self;

    /// Generates the state proof for target account based on [`TrieInput`].
    fn overlay_account_proof(
        tx: &'a TX,
        preimage_store: P,
        block_number: u64,
        input: TrieInput,
        address: Address,
        slots: &[B256],
    ) -> Result<AccountProof, StateProofError>;

    /// Generates the state [`MultiProof`] for target hashed account and storage keys.
    fn overlay_multiproof(
        tx: &'a TX,
        preimage_store: P,
        block_number: u64,
        input: TrieInput,
        targets: MultiProofTargets,
    ) -> Result<MultiProof, StateProofError>;
}

impl<'a, TX: DbTx, P: PreimageStore + Clone> DatabaseProof<'a, TX, P>
    for Proof<ExternalTrieCursorFactory<P>, DatabaseHashedCursorFactory<'a, TX>>
{
    /// Create a new [Proof] instance from database transaction.
    fn from_tx(tx: &'a TX, preimage_store: P, block_number: u64) -> Self {
        Self::new(ExternalTrieCursorFactory::new(preimage_store, block_number), DatabaseHashedCursorFactory::new(tx))
    }

    fn overlay_account_proof(
        tx: &'a TX,
        preimage_store: P,
        block_number: u64,
        input: TrieInput,
        address: Address,
        slots: &[B256],
    ) -> Result<AccountProof, StateProofError> {
        let nodes_sorted = input.nodes.into_sorted();
        let state_sorted = input.state.into_sorted();
        Self::from_tx(tx, preimage_store.clone(), block_number)
            .with_trie_cursor_factory(InMemoryTrieCursorFactory::new(
                ExternalTrieCursorFactory::new(preimage_store, block_number),
                &nodes_sorted,
            ))
            .with_hashed_cursor_factory(HashedPostStateCursorFactory::new(
                DatabaseHashedCursorFactory::new(tx),
                &state_sorted,
            ))
            .with_prefix_sets_mut(input.prefix_sets)
            .account_proof(address, slots)
    }

    fn overlay_multiproof(
        tx: &'a TX,
        preimage_store: P,
        block_number: u64,
        input: TrieInput,
        targets: MultiProofTargets,
    ) -> Result<MultiProof, StateProofError> {
        let nodes_sorted = input.nodes.into_sorted();
        let state_sorted = input.state.into_sorted();
        Self::from_tx(tx, preimage_store.clone(), block_number)
            .with_trie_cursor_factory(InMemoryTrieCursorFactory::new(
                ExternalTrieCursorFactory::new(preimage_store, block_number),
                &nodes_sorted,
            ))
            .with_hashed_cursor_factory(HashedPostStateCursorFactory::new(
                DatabaseHashedCursorFactory::new(tx),
                &state_sorted,
            ))
            .with_prefix_sets_mut(input.prefix_sets)
            .multiproof(targets)
    }
}

/// Extends [`StorageProof`] with operations specific for working with a database transaction.
pub trait DatabaseStorageProof<'a, TX, P> {
    /// Create a new [`StorageProof`] from database transaction and account address.
    fn from_tx(tx: &'a TX, preimage_store: P, block_number: u64, address: Address) -> Self;

    /// Generates the storage proof for target slot based on [`TrieInput`].
    fn overlay_storage_proof(
        tx: &'a TX,
        preimage_store: P,
        block_number: u64,
        address: Address,
        slot: B256,
        storage: HashedStorage,
    ) -> Result<reth_trie::StorageProof, StateProofError>;

    /// Generates the storage multiproof for target slots based on [`TrieInput`].
    fn overlay_storage_multiproof(
        tx: &'a TX,
        preimage_store: P,
        block_number: u64,
        address: Address,
        slots: &[B256],
        storage: HashedStorage,
    ) -> Result<StorageMultiProof, StateProofError>;
}

impl<'a, TX: DbTx, P: PreimageStore> DatabaseStorageProof<'a, TX, P>
    for StorageProof<ExternalTrieCursorFactory<P>, DatabaseHashedCursorFactory<'a, TX>>
{
    fn from_tx(tx: &'a TX, preimage_store: P, block_number: u64, address: Address) -> Self {
        Self::new(ExternalTrieCursorFactory::new(preimage_store, block_number), DatabaseHashedCursorFactory::new(tx), address)
    }

    fn overlay_storage_proof(
        tx: &'a TX,
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
        Self::from_tx(tx, preimage_store, block_number, address)
            .with_hashed_cursor_factory(HashedPostStateCursorFactory::new(
                DatabaseHashedCursorFactory::new(tx),
                &state_sorted,
            ))
            .with_prefix_set_mut(prefix_set)
            .storage_proof(slot)
    }

    fn overlay_storage_multiproof(
        tx: &'a TX,
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
        Self::from_tx(tx, preimage_store, block_number, address)
            .with_hashed_cursor_factory(HashedPostStateCursorFactory::new(
                DatabaseHashedCursorFactory::new(tx),
                &state_sorted,
            ))
            .with_prefix_set_mut(prefix_set)
            .storage_multiproof(targets)
    }
}
