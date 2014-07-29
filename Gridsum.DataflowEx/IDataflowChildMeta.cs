using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Gridsum.DataflowEx.Exceptions;

namespace Gridsum.DataflowEx
{
    /// <summary>
    /// Represents a unit/child of a dataflow whose lifecycle should be observed and managed
    /// by a host dataflow
    /// </summary>
    public interface IDataflowChildMeta
    {
        IEnumerable<IDataflowBlock> Blocks { get; }
        Task ChildCompletion { get; }
        Tuple<int,int> BufferStatus { get; }
        string DisplayName { get; }
        void Fault(Exception e);

        /// <summary>
        /// Return the real inner child object wrapped by the metadata (IDataflowBlock or Dataflow)
        /// </summary>
        object Unwrap();
    }

    internal abstract class ChildMetaBase : IDataflowChildMeta
    {
        protected readonly Dataflow m_host;
        private readonly Action<Task> m_completionCallback;

        public abstract IEnumerable<IDataflowBlock> Blocks { get; }
        public abstract Task ChildCompletion { get; }
        public abstract Tuple<int,int> BufferStatus { get; }
        public abstract string DisplayName { get; }
        public abstract void Fault(Exception e);
        public abstract object Unwrap();

        protected ChildMetaBase(Dataflow host, Action<Task> completionCallback)
        {
            m_host = host;
            m_completionCallback = completionCallback;
        }

        /// <summary>
        /// The Wrapping does 2 things:
        /// (1) propagate error to other children of the host 
        /// (2) call completion back of the child
        /// </summary>
        protected Task GetWrappedCompletion(Task rawCompletion)
        {
            var tcs = new TaskCompletionSource<object>();

            rawCompletion.ContinueWith(task =>
            {
                if (task.Status == TaskStatus.Faulted)
                {
                    var exception = TaskEx.UnwrapWithPriority(task.Exception);
                    tcs.SetException(exception);

                    if (!(exception is PropagatedException))
                    {
                        m_host.Fault(exception); //fault other blocks if this is an original exception
                        //todo: log this original exception
                    }
                }
                else if (task.Status == TaskStatus.Canceled)
                {
                    tcs.SetCanceled();
                    m_host.Fault(new TaskCanceledException());
                }
                else //success
                {
                    try
                    {
                        //call callback
                        if (m_completionCallback != null)
                        {
                            m_completionCallback(task);
                        }
                        tcs.SetResult(string.Empty);
                    }
                    catch (Exception e)
                    {
                        LogHelper.Logger.Error(h => h("{0} Error when callback {1} on its completion", m_host.FullName, this.DisplayName), e);
                        tcs.SetException(e);
                        m_host.Fault(e);
                    }
                }
            });

            return tcs.Task;
        }
    }

    /// <summary>
    /// A block as child
    /// </summary>
    internal class BlockMeta : ChildMetaBase
    {
        private readonly IDataflowBlock m_block;
        private readonly Task m_completion;

        public BlockMeta(IDataflowBlock block, Dataflow host, Action<Task> completionCallback = null) : base(host, completionCallback)
        {
            m_block = block;
            m_completion = GetWrappedCompletion(m_block.Completion);
        }

        public IDataflowBlock Block { get { return m_block; } }

        public override IEnumerable<IDataflowBlock> Blocks { get { return new [] {m_block}; } }
        public override Task ChildCompletion { get { return m_completion; } }
        public override Tuple<int,int> BufferStatus { get { return m_block.GetBufferCount(); } }

        public override string DisplayName
        {
            get { return string.Format("{0}->({1})", m_host.FullName, this.m_block.GetType().GetFriendlyName()); }
        }

        public override void Fault(Exception e)
        {
            m_block.Fault(e);
        }

        public override object Unwrap()
        {
            return m_block;
        }
    }

    /// <summary>
    /// A data flow as child
    /// </summary>
    internal class ChildDataflowMeta : ChildMetaBase
    {
        private readonly Dataflow m_childFlow;
        private readonly Task m_completion;

        public ChildDataflowMeta(Dataflow childFlow, Dataflow host, Action<Task> completionCallback = null) : base(host, completionCallback)
        {
            m_childFlow = childFlow;
            m_completion = GetWrappedCompletion(m_childFlow.CompletionTask);
        }

        public Dataflow Flow { get { return m_childFlow; } }

        public override IEnumerable<IDataflowBlock> Blocks { get { return m_childFlow.Blocks; } }
        public override Task ChildCompletion { get { return m_completion; } }
        public override Tuple<int, int> BufferStatus { get { return m_childFlow.BufferStatus; } }

        public override string DisplayName
        {
            get { return m_childFlow.FullName; }
        }

        public override void Fault(Exception e)
        {
            m_childFlow.Fault(e);
        }

        public override object Unwrap()
        {
            return m_childFlow;
        }
    }
}
