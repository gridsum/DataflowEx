using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Timers;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx.AutoCompletion
{
    public class AutoCompleteWrapper<TIn, TOut> : BlockContainer<TIn, TOut>
        where TIn : ITracableItem
        where TOut : ITracableItem 
    {
        private readonly TimeSpan m_processTimeout;
        private readonly Timer m_timer;
        private Guid? m_last;
        private BlockContainer<TIn, TIn> m_before;
        private BlockContainer<TOut, TOut> m_after;
        private BlockContainer<TIn, TOut> m_container;

        public AutoCompleteWrapper(BlockContainer<TIn, TOut> container, TimeSpan processTimeout) : base(BlockContainerOptions.Default)
        {
            m_container = container;
            m_processTimeout = processTimeout;
            m_timer = new Timer();
            m_timer.Interval = m_processTimeout.TotalMilliseconds;
            m_timer.Elapsed += OnTimerElapsed;

            var before = new TransformBlock<TIn, TIn>(@in =>
            {
                if (m_last == null || @in.UniqueId == m_last.Value)
                {
                    //The last one is back, so there is nothing else in the pipeline.
                    //Set a timer: if nothing new produced when timer expires, the whole loop ends.
                    m_timer.Start();
                }
                return @in;
            });

            m_before = BlockContainerUtils.FromBlock(before);

            var after = new TransformBlock<TOut, TOut>(@out =>
            {
                if (@out.UniqueId != Guid.Empty)
                {
                    m_last = @out.UniqueId;
                    m_timer.Stop();    
                }
                else
                {
                    LogHelper.Logger.WarnFormat("Empty guid found in output. You may have forgotten to set it.");
                }
                
                return @out;
            });

            m_after = BlockContainerUtils.FromBlock(after);

            m_before.LinkTo(container);
            container.LinkTo(m_after);

            RegisterChild(m_before);
            RegisterChild(container);
            RegisterChild(m_after);
        }

        void OnTimerElapsed(object sender, ElapsedEventArgs e)
        {
            LogHelper.Logger.InfoFormat("Auto complete timer elapsed. Shutting down block containers..");

            m_before.InputBlock.Complete(); //pass completion down to the chain
        }

        public override ISourceBlock<TOut> OutputBlock
        {
            get { return m_after.OutputBlock; }
        }

        public override ITargetBlock<TIn> InputBlock
        {
            get { return m_before.InputBlock; }
        }

        public override string Name
        {
            get
            {
                return string.Format("{0}-AutoComplete", m_container.Name);
            }
        }
    }
}
