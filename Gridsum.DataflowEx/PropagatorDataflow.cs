using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx
{
    public class PropagatorDataflow<TIn, TOut> : Dataflow<TIn, TOut>
    {
        private readonly IPropagatorBlock<TIn, TOut> m_block;
        
        public PropagatorDataflow(IPropagatorBlock<TIn, TOut> block) : this(block, BlockContainerOptions.Default)
        {}

        public PropagatorDataflow(IPropagatorBlock<TIn, TOut> block, BlockContainerOptions containerOptions)
            : base(containerOptions)
        {
            m_block = block;
            RegisterChild(m_block, null);
        }

        public override ITargetBlock<TIn> InputBlock
        {
            get { return m_block; }
        }

        public override ISourceBlock<TOut> OutputBlock
        {
            get { return m_block; }
        }

    }
}
