using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx
{
    public class DataCopier<T> : BlockContainer<T, T>
    {
        private readonly BufferBlock<T> m_copyBuffer;
        private readonly TransformBlock<T, T> m_transformBlock;

        public DataCopier(Func<T,T> copyFunc, BlockContainerOptions containerOptions) : base(containerOptions)
        {
            m_copyBuffer = new BufferBlock<T>(new DataflowBlockOptions()
            {
                BoundedCapacity = containerOptions.RecommendedCapacity ?? int.MaxValue
            });

            m_transformBlock = new TransformBlock<T, T>(arg =>
            {
                m_copyBuffer.Post(copyFunc == null ? arg : copyFunc(arg)); //todo: should post safely
                return arg;
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
    }
}
