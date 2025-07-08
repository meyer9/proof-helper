use futures_util::TryStreamExt;
use op_reth::node::OpNode;
use op_reth::primitives::OpPrimitives;
use reth::{
    api::{ConfigureEvm, FullNodeComponents, NodePrimitives}, builder::NodeTypes, core::primitives::AlloyBlockHeader, primitives::{RecoveredBlock}, providers::{StateProviderFactory, StateReader}, revm::{database::StateProviderDatabase, witness::ExecutionWitnessRecord, State}
};
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_tracing::tracing::{info, warn};
use reth_trie_db::MerklePatriciaTrie;
use reth_evm::execute::Executor;

/// Proof Helper ExEx - processes blocks and tracks state changes
pub struct ProofHelper<Node>
where
    Node: FullNodeComponents<
        Types: NodeTypes<Primitives = OpPrimitives, StateCommitment = MerklePatriciaTrie>,
    >,
    Node::Provider: StateReader,
{
    ctx: ExExContext<Node>,
}

impl<Node> ProofHelper<Node>
where
    Node: FullNodeComponents<
        Types: NodeTypes<Primitives = OpPrimitives, StateCommitment = MerklePatriciaTrie>,
    >,
    Node::Provider: StateReader + StateProviderFactory,
{
    /// Create a new ProofHelper instance
    pub fn new(ctx: ExExContext<Node>) -> Self {
        Self { ctx }
    }


    fn process_block(
        &self,
        block: &RecoveredBlock<<<Node::Types as NodeTypes>::Primitives as NodePrimitives>::Block>,
    ) -> eyre::Result<()> {
        let state_provider = self.ctx.provider().state_by_block_id(block.header().number().saturating_sub(1).into())?;
    
        let db = StateProviderDatabase::new(&state_provider);
        let block_executor = self.ctx.evm_config().batch_executor(db);
    
        let mut witness_record = ExecutionWitnessRecord::default();
    
        let _ = block_executor
            .execute_with_state_closure(&(*block).clone(), |statedb: &State<_>| {
                witness_record.record_executed_state(statedb);
            })
            .map_err(|err| eyre::eyre!(err))?;
    
        let ExecutionWitnessRecord {
            hashed_state,
            codes,
            keys,
            ..
        } = witness_record;
    
        let state = state_provider
            .witness(Default::default(), hashed_state)
            .map_err(|err| eyre::eyre!(err))?;
    
        info!("Got {:?} state nodes, {:?} code nodes, {:?} key nodes", state.len(), codes.len(), keys.len());
    
        Ok(())
    }

    /// Main execution loop for the ExEx
    pub async fn run(mut self) -> eyre::Result<()> {
        const MAX_BLOCK_DIFF: u64 = 1000;

        while let Some(notification) = self.ctx.notifications.try_next().await? {
            match &notification {
                ExExNotification::ChainCommitted { new } => {
                    let head_block_number = new.tip().num_hash().number;

                    for (block_number, block) in new.blocks() {
                        if head_block_number.saturating_sub(*block_number) > MAX_BLOCK_DIFF {
                            warn!("Block {} is too far behind the head block {}, skipping", block_number, head_block_number);
                            continue;
                        }

                        if let Err(err) = self.process_block(block) {
                            warn!("Error processing block {}: {}", block_number, err);
                        }
                    }
                }
                _ => {}
            };

            // Send finish event for committed chain
            if let Some(committed_chain) = notification.committed_chain() {
                self.ctx
                    .events
                    .send(ExExEvent::FinishedHeight(committed_chain.tip().num_hash()))?;
            }
        }

        Ok(())
    }
}

fn main() -> eyre::Result<()> {
    op_reth::cli::Cli::parse_args().run(async move |builder, _| {
        let handle = builder
            .node(OpNode::default())
            .install_exex("proof-helper", async move |ctx| {
                let proof_helper = ProofHelper::new(ctx);
                Ok(proof_helper.run())
            })
            .launch()
            .await?;

        handle.wait_for_node_exit().await
    })
}
