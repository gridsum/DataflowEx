using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Gridsum.DataflowEx;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx.AutoCompletion
{
    /// <summary>
    /// A dataflow node used in ring completion detection infrastructure. 
    /// </summary>
    public interface IHeartbeatNode : IRingNode
    {
        long ProcessedItemCount { get; }

        bool NoHeartbeatDuring(Action action);
        Task<bool> NoHeartbeatDuring(Func<Task> action);
    }

    /// <summary>
    /// A default implementation of IHeartbeatNode, used in ring completion detection.
    /// </summary>
    public class HeartbeatNode<T> : Dataflow<T, T>, IHeartbeatNode
    {
        private long m_beats;
        private TransformBlock<T, T> m_block;

        public HeartbeatNode(DataflowOptions options) : base(options)
        {
            m_beats = 0;

            Func<T, T> f = arg =>
                {
                    IsBusy = true;
                    m_beats++;
                    IsBusy = false;
                    return arg;
                };

            m_block = new TransformBlock<T, T>(f, options.ToExecutionBlockOption());
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

        public override void Complete()
        {
            this.InputBlock.Complete();
        }

        public bool NoHeartbeatDuring(Action action)
        {
            long before = m_beats;
            action();
            long after = m_beats;
            return after == before;
        }

        public async Task<bool> NoHeartbeatDuring(Func<Task> action)
        {
            long before = m_beats;
            await action().ConfigureAwait(false);
            long after = m_beats;
            return after == before;
        }

        public bool IsBusy { get; private set; }
    }
}
