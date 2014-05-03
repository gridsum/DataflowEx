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
    /// Represents a unit/child of a container whose lifecycle should be observed and managed
    /// by a host container
    /// </summary>
    public interface IChildMeta
    {
        IEnumerable<IDataflowBlock> Blocks { get; }
        Task ChildCompletion { get; }
        int BufferCount { get; }
        string DisplayName { get; }
        void Fault(Exception e);
    }

    internal abstract class ChildMetaBase : IChildMeta
    {
        protected readonly BlockContainer m_host;
        private readonly Action<Task> m_completionCallback;

        public abstract IEnumerable<IDataflowBlock> Blocks { get; }
        public abstract Task ChildCompletion { get; }
        public abstract int BufferCount { get; }
        public abstract string DisplayName { get; }
        public abstract void Fault(Exception e);

        protected ChildMetaBase(BlockContainer host, Action<Task> completionCallback)
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
                        LogHelper.Logger.Error(h => h("[{0}] Error when callback {1} on its completion", m_host.Name, this.DisplayName), e);
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

        public BlockMeta(IDataflowBlock block, BlockContainer host, Action<Task> completionCallback = null) : base(host, completionCallback)
        {
            m_block = block;
            m_completion = GetWrappedCompletion(m_block.Completion);
        }

        public IDataflowBlock Block { get { return m_block; } }

        public override IEnumerable<IDataflowBlock> Blocks { get { return new [] {m_block}; } }
        public override Task ChildCompletion { get { return m_completion; } }
        public override int BufferCount { get { return m_block.GetBufferCount(); } }

        public override string DisplayName
        {
            get { return string.Format("[{0}]->[{1}]", m_host.Name, Utils.GetFriendlyName(m_block.GetType())); }
        }

        public override void Fault(Exception e)
        {
            m_block.Fault(e);
        }
    }

    /// <summary>
    /// A block container as child
    /// </summary>
    internal class BlockContainerMeta : ChildMetaBase
    {
        private readonly BlockContainer m_childContainer;
        private readonly Task m_completion;

        public BlockContainerMeta(BlockContainer childContainer, BlockContainer host, Action<Task> completionCallback = null) : base(host, completionCallback)
        {
            m_childContainer = childContainer;
            m_completion = GetWrappedCompletion(m_childContainer.CompletionTask);
        }

        public BlockContainer Container { get { return m_childContainer; } }

        public override IEnumerable<IDataflowBlock> Blocks { get { return m_childContainer.Blocks; } }
        public override Task ChildCompletion { get { return m_completion; } }
        public override int BufferCount { get { return m_childContainer.BufferedCount; } }

        public override string DisplayName
        {
            get { return string.Format("[{0}]->[{1}]", m_host.Name, m_childContainer.Name); }
        }

        public override void Fault(Exception e)
        {
            m_childContainer.Fault(e);
        }
    }
}
