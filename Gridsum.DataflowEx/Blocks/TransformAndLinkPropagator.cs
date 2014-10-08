using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx.Blocks
{
    internal sealed class TransformAndLinkPropagator<TIn, TOut> : IPropagatorBlock<TIn, TOut>, ITargetBlock<TIn>, ISourceBlock<TOut>, IDataflowBlock
    {
        private readonly ISourceBlock<TIn> m_source;
        private readonly ITargetBlock<TOut> m_target;
        private readonly Predicate<TIn> m_userProvidedPredicate;
        private readonly Func<TIn, TOut> m_transform;

        Task IDataflowBlock.Completion
        {
            get
            {
                return this.m_source.Completion;
            }
        }

        internal TransformAndLinkPropagator(ISourceBlock<TIn> source, ITargetBlock<TOut> target, Predicate<TIn> predicate, Func<TIn, TOut> transform)
        {
            this.m_source = source;
            this.m_target = target;
            this.m_transform = transform;
            this.m_userProvidedPredicate = predicate;
        }
        
        private bool RunPredicate(TIn item)
        {
            return this.m_userProvidedPredicate(item);
        }

        DataflowMessageStatus ITargetBlock<TIn>.OfferMessage(DataflowMessageHeader messageHeader, TIn messageValue, ISourceBlock<TIn> source, bool consumeToAccept)
        {
            if (!messageHeader.IsValid)
                throw new ArgumentException("message header is invalid", "messageHeader");
            if (source == null)
                throw new ArgumentNullException("source");
            if (this.RunPredicate(messageValue))
                return this.m_target.OfferMessage(messageHeader, m_transform(messageValue), (ISourceBlock<TOut>)this, consumeToAccept);
            else
                return DataflowMessageStatus.Declined;
        }

        TOut ISourceBlock<TOut>.ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<TOut> target, out bool messageConsumed)
        {
            TIn raw = this.m_source.ConsumeMessage(messageHeader, (ITargetBlock<TIn>)this, out messageConsumed);
            return m_transform(raw);
        }

        bool ISourceBlock<TOut>.ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<TOut> target)
        {
            return this.m_source.ReserveMessage(messageHeader, (ITargetBlock<TIn>)this);
        }

        void ISourceBlock<TOut>.ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<TOut> target)
        {
            this.m_source.ReleaseReservation(messageHeader, (ITargetBlock<TIn>)this);
        }

        void IDataflowBlock.Complete()
        {
            this.m_target.Complete();
        }

        void IDataflowBlock.Fault(Exception exception)
        {
            this.m_target.Fault(exception);
        }

        IDisposable ISourceBlock<TOut>.LinkTo(ITargetBlock<TOut> target, DataflowLinkOptions linkOptions)
        {
            throw new NotSupportedException();
        }
    }
}
