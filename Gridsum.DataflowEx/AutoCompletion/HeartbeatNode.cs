using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Gridsum.DataflowEx;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx.AutoCompletion
{
    public interface IHeartbeatNode : IDataflow
    {
        long ProcessedItemCount { get; }

        void Complete();
    }

    public class HeartbeatNode<T> : Dataflow<T, T>, IHeartbeatNode
    {
        private long m_beats;
        private TransformBlock<T, T> m_block;

        public HeartbeatNode() : base(DataflowOptions.Default)
        {
            m_beats = 0;

            Func<T, T> f = arg =>
                {
                    m_beats++;
                    return arg;
                };

            m_block = new TransformBlock<T, T>(f);
            RegisterChild(m_block);
        }

        public override ITargetBlock<T> InputBlock
        {
            get
            {
                return m_block;
            }
        }

        public override ISourceBlock<T> OutputBlock
        {
            get
            {
                return m_block;
            }
        }

        public long ProcessedItemCount
        {
            get
            {
                return m_beats;
            }
        }

        public void Complete()
        {
            this.InputBlock.Complete();
        }
    }
}
