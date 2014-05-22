using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx
{
    public class TargetDataflow<TIn> : Dataflow<TIn>
    {
        private readonly ITargetBlock<TIn> m_block;

        public TargetDataflow(ITargetBlock<TIn> block) : this(block, BlockContainerOptions.Default)
        {
        }

        public TargetDataflow(ITargetBlock<TIn> block, BlockContainerOptions containerOptions)
            : base(containerOptions)
        {
            m_block = block;
            RegisterChild(m_block);
        }

        public override ITargetBlock<TIn> InputBlock
        {
            get { return m_block; }
        }
    }
}
