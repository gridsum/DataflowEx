using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx
{
    /// <summary>
    /// Merges two underlying block containers so that the combined one looks like a single block container from outside
    /// </summary>
    /// <typeparam name="T1"></typeparam>
    /// <typeparam name="T2"></typeparam>
    /// <typeparam name="T3"></typeparam>
    public class BlockContainerMerger<T1, T2, T3> : BlockContainerBase<T1, T3>
    {
        private BlockContainerBase<T1, T2> m_b1;
        private BlockContainerBase<T2, T3> m_b2;
        
        public BlockContainerMerger(BlockContainerBase<T1, T2> b1, BlockContainerBase<T2, T3> b2) : base(BlockContainerOptions.Default)
        {
            m_b1 = b1;
            m_b2 = b2;

            m_b1.Link(m_b2);
        }

        public override ISourceBlock<T3> OutputBlock
        {
            get { return m_b2.OutputBlock; }
        }

        public override ITargetBlock<T1> InputBlock
        {
            get { return m_b1.InputBlock; }
        }

        protected override async Task GetCompletionTask()
        {
            await Task.WhenAll(m_b1.CompletionTask, m_b2.CompletionTask);
            this.CleanUp();            
        }
        
        public override void Fault(Exception exception, bool propagateException = false)
        {
            m_b1.Fault(exception, propagateException);
        }

        public override IEnumerable<IDataflowBlock> Blocks
        {
            get
            {
                return m_b1.Blocks.Concat(m_b2.Blocks);
            }
        }
    }
}
