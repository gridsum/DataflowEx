using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx
{
    using System.Collections.Concurrent;
    using System.Collections.Immutable;
    using System.Threading.Tasks.Dataflow;

    /// <summary>
    /// Provides an abstract flow that dispatch inputs to multiple child flows by a special dispatch function, which is 
    /// useful in situations that you want to group inputs by a certain property and let specific child flows to take
    /// care of different groups independently. DataDispatcher also helps creating and maintaining child flows dynamically 
    /// in a thread-safe way.
    /// </summary>
    /// <typeparam name="TIn">Type of input items of this dispatcher flow</typeparam>
    /// <typeparam name="TKey">Type of the dispatch key to group input items</typeparam>
    /// <remarks>
    /// This flow guarantees an input goes to only ONE of the child flows. Notice the difference comparing to DataBroadcaster, which 
    /// gives the input to EVERY flow it is linked to.
    /// </remarks>
    public abstract class DataDispatcher<TIn, TKey> : Dataflow<TIn>
    {
        protected ActionBlock<TIn> m_dispatcherBlock;
        protected ConcurrentDictionary<TKey, Lazy<Dataflow<TIn>>> m_destinations;
        private Func<TKey, Lazy<Dataflow<TIn>>> m_initer;

        /// <summary>
        /// Construct an DataDispatcher instance 
        /// </summary>
        /// <param name="dispatchFunc">The dispatch function</param>
        public DataDispatcher(Func<TIn, TKey> dispatchFunc) : this(dispatchFunc, DataflowOptions.Default)
        {
        }

        /// <summary>
        /// Construct an DataDispatcher instance 
        /// </summary>
        /// <param name="dispatchFunc">The dispatch function</param>
        /// <param name="option">Option for this dataflow</param>
        public DataDispatcher(Func<TIn, TKey> dispatchFunc, DataflowOptions option)
            : this(dispatchFunc, EqualityComparer<TKey>.Default, option)
        {
        }

        /// <summary>
        /// Construct an DataDispatcher instance 
        /// </summary>
        /// <param name="dispatchFunc">The dispatch function</param>
        /// <param name="keyComparer">The key comparer for this dataflow</param>
        /// <param name="option">Option for this dataflow</param>
        public DataDispatcher(Func<TIn, TKey> dispatchFunc, EqualityComparer<TKey> keyComparer, DataflowOptions option)
            : base(option)
        {
            m_destinations = new ConcurrentDictionary<TKey, Lazy<Dataflow<TIn>>>(keyComparer);

            m_initer = key => new Lazy<Dataflow<TIn>>(
                                  () =>
                                  {
                                      var child = this.CreateChildFlow(key);
                                      RegisterChild(child);
                                      child.RegisterDependency(m_dispatcherBlock);
                                      return child;
                                  });

            m_dispatcherBlock = new ActionBlock<TIn>(async
                input =>
            {
                var childFlow = m_destinations.GetOrAdd(dispatchFunc(input), m_initer).Value;
                await childFlow.SendAsync(input).ConfigureAwait(false);
            }, option.ToExecutionBlockOption());

            RegisterChild(m_dispatcherBlock);
        }

        /// <summary>
        /// Create the child flow on-the-fly by the dispatch key
        /// </summary>
        /// <param name="dispatchKey">The unique key to create and identify the child flow</param>
        /// <returns>A new child dataflow which is responsible for processing items having the given dispatch key</returns>
        /// <remarks>
        /// The dispatch key should have a one-one relatioship with child flow
        /// </remarks>
        protected abstract Dataflow<TIn> CreateChildFlow(TKey dispatchKey);
        
        /// <summary>
        /// See <see cref="Dataflow{T}.InputBlock"/>
        /// </summary>
        public override ITargetBlock<TIn> InputBlock
        {
            get
            {
                return m_dispatcherBlock;
            }
        }
    }
}
