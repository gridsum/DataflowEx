using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx
{
    /// <summary>
    /// BroadcastBlock only pushes latest data (if destination is full) and causes data loss.
    /// That's why we need DataCopier which preserves a 100% same copy of the data stream through CopiedOutputBlock
    /// </summary>
    /// <typeparam name="T">The input and output type of the data flow</typeparam>
    public class DataBrancher<T> : BlockContainer<T, T>
    {
        private readonly BufferBlock<T> m_copyBuffer;
        private readonly TransformBlock<T, T> m_transformBlock;

        public DataBrancher() : this(BlockContainerOptions.Default) {}

        public DataBrancher(BlockContainerOptions containerOptions) : this(null, containerOptions) {}

        public DataBrancher(Func<T,T> copyFunc, BlockContainerOptions containerOptions) : base(containerOptions)
        {
            m_copyBuffer = new BufferBlock<T>(new DataflowBlockOptions()
            {
                BoundedCapacity = containerOptions.RecommendedCapacity ?? int.MaxValue
            });

            m_transformBlock = new TransformBlock<T, T>(arg =>
            {
                m_copyBuffer.SafePost(copyFunc == null ? arg : copyFunc(arg)); //todo: should post safely
                return arg;
            });

            m_transformBlock.Completion.ContinueWith(t =>
            {
                //propagate completion only the task succeeded (RegisterBlock already takes care of Faulted and Canceled)
                if (t.Status == TaskStatus.RanToCompletion) 
                {
                    m_copyBuffer.Complete();
                }
            });

            RegisterBlock(m_copyBuffer);
            RegisterBlock(m_transformBlock);
        }

        public override ITargetBlock<T> InputBlock
        {
            get { return m_transformBlock; }
        }

        public override ISourceBlock<T> OutputBlock
        {
            get { return m_transformBlock; }
        }

        /// <summary>
        /// The copied data stream
        /// </summary>
        public ISourceBlock<T> CopiedOutputBlock
        {
            get { return m_copyBuffer; }
        }

        /// <summary>
        /// Link the copied data stream to another block
        /// </summary>
        public void LinkSecondlyTo(IBlockContainer<T> other)
        {
            LinkBlockToContainer(this.CopiedOutputBlock, other);
        }
    }
}
