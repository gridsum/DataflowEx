using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Timers;

namespace Gridsum.DataflowEx.AutoCompletion
{
    public class AutoCompleteContainerPair2<TIn, TOut>
        where TIn : ITracableItem
        where TOut : ITracableItem
    {
        private readonly TimeSpan m_processTimeout;
        private readonly Timer m_timer;
        private Guid? m_last;
        private BlockContainer<TIn, ITracableItem> m_before;
        private BlockContainer<ITracableItem, TOut> m_after;

        public AutoCompleteContainerPair2(TimeSpan processTimeout)
        {
            m_processTimeout = processTimeout;
            m_timer = new Timer();
            m_timer.Interval = m_processTimeout.TotalMilliseconds;
            m_timer.Elapsed += OnTimerElapsed;

            var before = new TransformManyBlock<TIn, ITracableItem>(@in =>
            {
                if (m_last != null && @in.UniqueId == m_last.Value)
                {
                    //The last one is back, so there is nothing else in the pipeline.
                    //output: before, @in, after
                    return new ITracableItem[] {
                        new ControlItem { Type = ControlType.BeforeLast},
                        @in,
                        new ControlItem { Type = ControlType.AfterLast}
                    };
                }
                return new ITracableItem[] { @in };
            });

            m_before = BlockContainerUtils.FromBlock(before);

            var after = new TransformManyBlock<ITracableItem, TOut>(@out =>
            {
                var control = @out as ControlItem;

                if (control != null)
                {

                }


                m_last = @out.UniqueId;
                m_timer.Stop();
                return @out;
            });

            m_after = BlockContainerUtils.FromBlock(after);
        }

        void OnTimerElapsed(object sender, ElapsedEventArgs e)
        {
            LogHelper.Logger.InfoFormat("Auto complete timer elapsed. Shutting down block containers..");

            m_before.InputBlock.Complete(); //pass completion down to the chain
        }

        public BlockContainer<TIn, TIn> Before
        {
            get { return m_before; }
        }

        public BlockContainer<TOut, TOut> After
        {
            get { return m_after; }
        }
    }
}
