using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx
{
    using System.Collections.Immutable;

    /// <summary>
    /// BroadcastBlock in TPL Dataflow only pushes latest data (if destination is full) and causes data loss.
    /// That's why we need this DataBroadcaster which preserves a 100% same copy of the data stream
    /// </summary>
    /// <typeparam name="T">The input and output type of the data flow</typeparam>
    public class DataBroadcaster<T> : Dataflow<T, T>
    {
        //todo: fix race condition
        private ImmutableList<Dataflow<T, T>> m_copyBuffers;

        private readonly TransformBlock<T, T> m_transformBlock;

        /// <summary>
        /// Construct an DataBroadcaster instance 
        /// </summary>
        public DataBroadcaster() : this(DataflowOptions.Default) {}

        /// <summary>
        /// Construct an DataBroadcaster instance 
        /// </summary>
        /// <param name="dataflowOptions">the option of this dataflow</param>
        public DataBroadcaster(DataflowOptions dataflowOptions) : this(null, dataflowOptions) {}

        /// <summary>
        /// Construct an DataBroadcaster instance 
        /// </summary>
        /// <param name="copyFunc">The copy function when broadcasting</param>
        /// <param name="dataflowOptions">the option of this dataflow</param>
        public DataBroadcaster(Func<T,T> copyFunc, DataflowOptions dataflowOptions) : base(dataflowOptions)
        {
            m_copyBuffers = ImmutableList<Dataflow<T, T>>.Empty;

            m_transformBlock = new TransformBlock<T, T>(
                async arg =>
                    {
                        T copy = copyFunc == null ? arg : copyFunc(arg);
                        foreach (var buffer in m_copyBuffers)
                        {
                            await buffer.SendAsync(copy).ConfigureAwait(false);
                        }
                        return arg;
                    }, dataflowOptions.ToExecutionBlockOption());
            
            RegisterChild(m_transformBlock);
        }

        /// <summary>
        /// See <see cref="Dataflow{T}.InputBlock"/>
        /// </summary>
        public override ITargetBlock<T> InputBlock
        {
            get { return m_transformBlock; }
        }

        /// <summary>
        /// See <see cref="IOutputDataflow{T}.OutputBlock"/>
        /// </summary>
        public override ISourceBlock<T> OutputBlock
        {
            get { return m_transformBlock; }
        }

        /// <summary>
        /// Link the copied data stream to another block
        /// </summary>
        private void LinkCopyTo(IDataflow<T> other)
        {
            //first, create a new copy block
            Dataflow<T, T> copyBuffer = new BufferBlock<T>(m_dataflowOptions.ToGroupingBlockOption()).ToDataflow(m_dataflowOptions);

            RegisterChild(copyBuffer);
            copyBuffer.RegisterDependency(m_transformBlock);

            m_copyBuffers = m_copyBuffers.Add(copyBuffer);
            copyBuffer.Name = "Buffer" + m_copyBuffers.Count;
            copyBuffer.LinkTo(other);
        }

        /// <summary>
        /// See <see cref="Dataflow{TIn, TOut}.GoTo"/>
        /// </summary>
        public override IDataflow<T> GoTo(IDataflow<T> other, Predicate<T> predicate)
        {
            if (predicate != null)
            {
                throw new ArgumentException("DataBroadcaster does not support predicate linking", "predicate");
            }

            if (m_condBuilder.Count == 0) //not linked to any target yet
            {
                //link first output as primary output
                base.GoTo(other);    
            }
            else
            {
                this.LinkCopyTo(other);
            }

            LogHelper.Logger.InfoFormat("{0} now links to its {1}th target ({2})", this.FullName, m_copyBuffers.Count + 1, other.Name);
            return other;
        }
    }
}
