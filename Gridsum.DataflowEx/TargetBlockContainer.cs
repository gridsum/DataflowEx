using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx
{
    public class TargetBlockContainer<TIn> : BlockContainer<TIn>
    {
        private readonly ITargetBlock<TIn> m_block;

        public TargetBlockContainer(ITargetBlock<TIn> block) : this(block, BlockContainerOptions.Default)
        {
        }

        public TargetBlockContainer(ITargetBlock<TIn> block, BlockContainerOptions containerOptions)
            : base(containerOptions)
        {
            m_block = block;
            RegisterBlock(m_block);
        }

        public override ITargetBlock<TIn> InputBlock
        {
            get { return m_block; }
        }
    }
}
