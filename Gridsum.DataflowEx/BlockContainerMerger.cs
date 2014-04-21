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
    /// <typeparam name="T1">Block1 In Type</typeparam>
    /// <typeparam name="T2">Block1 Out Type & Block2 In Type</typeparam>
    /// <typeparam name="T3">Block2 Out Type</typeparam>
    public class BlockContainerMerger<T1, T2, T3> : BlockContainer<T1, T3>
    {
        protected readonly BlockContainer<T1, T2> m_b1;
        protected readonly BlockContainer<T2, T3> m_b2;
        
        public BlockContainerMerger(BlockContainer<T1, T2> b1, BlockContainer<T2, T3> b2) : base(BlockContainerOptions.Default)
        {
            m_b1 = b1;
            m_b2 = b2;

            m_b1.LinkTo(m_b2);

            RegisterChildContainer(m_b1);
            RegisterChildContainer(m_b2);
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
            //wait for the blocks
            await base.GetCompletionTask();

            //wait for the containers
            await Task.WhenAll(m_b1.CompletionTask, m_b2.CompletionTask);
            this.CleanUp();            
        }
        
        public override void Fault(Exception exception)
        {
            m_b1.Fault(exception);
        }

        public override IEnumerable<IDataflowBlock> Blocks
        {
            get
            {
                return m_b1.Blocks.Concat(m_b2.Blocks);
            }
        }

        public override int BufferedCount
        {
            get { return m_b1.BufferedCount + m_b2.BufferedCount; }
        }
    }

    /// <summary>
    /// Merges 3 underlying block containers so that the combined one looks like a single block container from outside
    /// </summary>
    /// <typeparam name="T1">Block1 In Type</typeparam>
    /// <typeparam name="T2">Block1 Out Type & Block2 In Type</typeparam>
    /// <typeparam name="T3">Block2 Out Type & Block3 In Type</typeparam>
    /// <typeparam name="T4">Block3 Out Type</typeparam>
    public class BlockContainerMerger<T1, T2, T3, T4> : BlockContainer<T1, T4>
    {
        protected readonly BlockContainer<T1, T2> m_b1;
        protected readonly BlockContainer<T2, T3> m_b2;
        protected readonly BlockContainer<T3, T4> m_b3;

        public BlockContainerMerger(BlockContainer<T1, T2> b1, BlockContainer<T2, T3> b2, BlockContainer<T3, T4> b3)
            : base(BlockContainerOptions.Default)
        {
            m_b1 = b1;
            m_b2 = b2;
            m_b3 = b3;

            m_b1.LinkTo(m_b2);
            m_b2.LinkTo(m_b3);

            RegisterChildContainer(m_b1);
            RegisterChildContainer(m_b2);
            RegisterChildContainer(m_b3);
        }

        public override ISourceBlock<T4> OutputBlock
        {
            get { return m_b3.OutputBlock; }
        }

        public override ITargetBlock<T1> InputBlock
        {
            get { return m_b1.InputBlock; }
        }

        protected override async Task GetCompletionTask()
        {
            //wait for the blocks
            await base.GetCompletionTask();

            //wait for the containers
            await Task.WhenAll(m_b1.CompletionTask, m_b2.CompletionTask, m_b3.CompletionTask);
            
            this.CleanUp();
        }

        public override void Fault(Exception exception)
        {
            m_b1.Fault(exception);
        }

        public override IEnumerable<IDataflowBlock> Blocks
        {
            get
            {
                return m_b1.Blocks.Concat(m_b2.Blocks).Concat(m_b3.Blocks);
            }
        }

        public override int BufferedCount
        {
            get { return m_b1.BufferedCount + m_b2.BufferedCount + m_b3.BufferedCount; }
        }
    }
}
