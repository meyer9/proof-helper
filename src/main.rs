use futures_util::TryStreamExt;
use reth::{
    api::FullNodeComponents, 
    builder::NodeTypes, 
    primitives::{EthPrimitives, SealedBlock}, 
    providers::{StateReader}, 
};
use reth_execution_types::Chain;
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_node_ethereum::EthereumNode;
use reth_trie_db::{MerklePatriciaTrie};
use reth_tracing::tracing::info;

/// Proof Helper ExEx - processes blocks and tracks state changes
pub struct ProofHelper<Node> 
where 
    Node: FullNodeComponents<
        Types: NodeTypes<Primitives = EthPrimitives, StateCommitment = MerklePatriciaTrie>
    >,
    Node::Provider: StateReader,
{
    ctx: ExExContext<Node>,
}

impl<Node> ProofHelper<Node>
where
    Node: FullNodeComponents<
        Types: NodeTypes<Primitives = EthPrimitives, StateCommitment = MerklePatriciaTrie>
    >,
    Node::Provider: StateReader,
{
    /// Create a new ProofHelper instance
    pub fn new(ctx: ExExContext<Node>) -> Self {
        Self { ctx }
    }

    /// Process a single block and extract state changes
    async fn process_block(&self, block: &SealedBlock) -> eyre::Result<()> {
        let block_number = block.number;
        let block_hash = block.hash();
        
        info!(
            block_number = %block_number,
            block_hash = %block_hash,
            "Processing block"
        );

        // Get the execution outcome for this block
        if let Ok(Some(execution_outcome)) = self.ctx.components.provider().get_state(block_number) {
            // Process account changes from reverts
            for (address, account) in execution_outcome.bundle.reverts.iter().flatten() {
                info!(
                    address = %address,
                    account = ?account,
                    "State change detected"
                );
            }
        }

        Ok(())
    }

    /// Process a chain of committed blocks
    async fn process_chain(&self, chain: &Chain) -> eyre::Result<()> {
        info!(
            chain_range = ?chain.range(),
            "Processing committed chain"
        );

        // Process each block in the chain
        for (_, block) in chain.blocks().iter() {
            self.process_block(block).await?;
        }

        Ok(())
    }

    /// Main execution loop for the ExEx
    pub async fn run(mut self) -> eyre::Result<()> {
        while let Some(notification) = self.ctx.notifications.try_next().await? {
            match &notification {
                ExExNotification::ChainCommitted { new } => {
                    info!(committed_chain = ?new.range(), "Received commit");
                    self.process_chain(new).await?;
                }
                ExExNotification::ChainReorged { old, new } => {
                    info!(
                        from_chain = ?old.range(), 
                        to_chain = ?new.range(), 
                        "Received reorg"
                    );
                    // Process the new chain after reorg
                    self.process_chain(new).await?;
                }
                ExExNotification::ChainReverted { old } => {
                    info!(reverted_chain = ?old.range(), "Received revert");
                    // Handle reverted blocks if needed
                }
            };

            // Send finish event for committed chain
            if let Some(committed_chain) = notification.committed_chain() {
                self.ctx.events.send(ExExEvent::FinishedHeight(committed_chain.tip().num_hash()))?;
            }
        }

        Ok(())
    }
}

fn main() -> eyre::Result<()> {
    reth::cli::Cli::parse_args().run(async move |builder, _| {
        let handle = builder
            .node(EthereumNode::default())
            .install_exex("proof-helper", async move |ctx| {
                let proof_helper = ProofHelper::new(ctx);
                Ok(proof_helper.run())
            })
            .launch()
            .await?;

        handle.wait_for_node_exit().await
    })
}