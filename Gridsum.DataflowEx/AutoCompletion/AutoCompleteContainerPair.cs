using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Timers;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx.AutoCompletion
{
    public class AutoCompleteContainerPair<TIn, TOut>
        where TIn : ITracableItem
        where TOut : ITracableItem 
    {
        private readonly TimeSpan m_processTimeout;
        private readonly Timer m_timer;
        private Guid? m_last;
        private BlockContainerBase<TIn, TIn> m_before;
        private BlockContainerBase<TOut, TOut> m_after;

        public AutoCompleteContainerPair(TimeSpan processTimeout)
        {
            m_processTimeout = processTimeout;
            m_timer = new Timer();
            m_timer.Interval = m_processTimeout.TotalMilliseconds;
            m_timer.Elapsed += m_timer_Elapsed;

            var before = new TransformBlock<TIn, TIn>(@in =>
            {
                if (m_last != null && @in.UniqueId == m_last.Value)
                {
                    m_timer.Start();
                }
                return @in;
            });

            m_before = BlockContainerUtils.FromBlock(before);

            var after = new TransformBlock<TOut, TOut>(@out =>
            {
                @out.UniqueId = Guid.NewGuid();
                m_last = @out.UniqueId;
                m_timer.Stop();
                return @out;
            });

            m_after = BlockContainerUtils.FromBlock(after);
        }

        void m_timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            LogHelper.Logger.InfoFormat("Auto complete timer elapsed. Shutting down block containers..");

            m_before.InputBlock.Complete(); //pass completion down to the chain
        }

        public BlockContainerBase<TIn, TIn> Before
        {
            get { return m_before; }
        }

        public BlockContainerBase<TOut, TOut> After
        {
            get { return m_after; }
        }
    }
}
