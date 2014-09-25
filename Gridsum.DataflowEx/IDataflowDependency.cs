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
    /// Represents a unit/child of a dataflow whose lifecycle should be observed by an observer.
    /// The observer may be a host parent dataflow or a dependent sibling.
    /// </summary>
    public interface IDataflowDependency
    {
        IEnumerable<IDataflowBlock> Blocks { get; }
        Task Completion { get; }
        Tuple<int,int> BufferStatus { get; }
        string DisplayName { get; }
        void Fault(Exception e);

        /// <summary>
        /// Return the real inner child object wrapped by the metadata (IDataflowBlock or Dataflow)
        /// </summary>
        object Unwrap();

        /// <summary>
        /// Type of this dependency
        /// </summary>
        DependencyKind Kind { get; }
    }

    public enum DependencyKind
    {
        /// <summary>
        /// The dependency is the child of the observer
        /// </summary>
        Internal,

        /// <summary>
        /// The dependency is external to the observer
        /// </summary>
        External
    }

    internal abstract class DependencyBase : IDataflowDependency
    {
        protected readonly Dataflow m_host;
        private readonly Action<Task> m_completionCallback;
        public abstract IEnumerable<IDataflowBlock> Blocks { get; }
        public abstract Task Completion { get; }
        public abstract Tuple<int,int> BufferStatus { get; }
        public abstract string DisplayName { get; }
        public abstract void Fault(Exception e);
        public abstract object Unwrap();

        public DependencyKind Kind { get; private set; }

        protected DependencyBase(Dataflow host, Action<Task> completionCallback, DependencyKind kind)
        {
            m_host = host;
            m_completionCallback = completionCallback;
            this.Kind = kind;
        }

        /// <summary>
        /// The Wrapping does 2 things for children:
        /// (1) propagate error to other children of the host 
        /// (2) call completion back of the child
        /// </summary>
        protected Task GetWrappedCompletion(Task rawCompletion)
        {
            if (Kind == DependencyKind.External)
            {
                return rawCompletion;
            }

            //for internal dependencies, we have something to do
            var tcs = new TaskCompletionSource<object>();

            rawCompletion.ContinueWith(
                task =>
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
                            {
                                //todo: extension point
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
                                LogHelper.Logger.Error(
                                    h =>
                                    h(
                                        "{0} Error when callback {1} on its completion",
                                        m_host.FullName,
                                        this.DisplayName),
                                    e);
                                tcs.SetException(e);
                                m_host.Fault(e);
                            }
                        }
                    });

            return tcs.Task;
        }
    }

    /// <summary>
    /// A block as dependency
    /// </summary>
    internal class BlockDependency : DependencyBase
    {
        private readonly IDataflowBlock m_block;
        private readonly string m_displayName;
        private readonly Task m_completion;

        public BlockDependency(IDataflowBlock block, Dataflow host, DependencyKind kind, Action<Task> completionCallback = null, string displayName = null) : base(host, completionCallback, kind)
        {
            m_block = block;
            this.m_displayName = displayName;
            m_completion = GetWrappedCompletion(m_block.Completion);
        }

        public IDataflowBlock Block { get { return m_block; } }

        public override IEnumerable<IDataflowBlock> Blocks { get { return new [] {m_block}; } }
        public override Task Completion { get { return m_completion; } }
        public override Tuple<int,int> BufferStatus { get { return m_block.GetBufferCount(); } }

        public override string DisplayName
        {
            get { return string.Format("{0}->({1})", m_host.FullName, m_displayName ?? m_block.GetType().GetFriendlyName()); }
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
    /// A data flow as dependency
    /// </summary>
    internal class DataflowDependency : DependencyBase
    {
        private readonly Dataflow m_dependentFlow;
        private readonly Task m_completion;

        public DataflowDependency(Dataflow dependentFlow, Dataflow host, DependencyKind kind, Action<Task> completionCallback = null) : base(host, completionCallback, kind)
        {
            this.m_dependentFlow = dependentFlow;
            m_completion = GetWrappedCompletion(this.m_dependentFlow.CompletionTask);
        }

        public Dataflow Flow { get { return this.m_dependentFlow; } }

        public override IEnumerable<IDataflowBlock> Blocks { get { return this.m_dependentFlow.Blocks; } }
        public override Task Completion { get { return m_completion; } }
        public override Tuple<int, int> BufferStatus { get { return this.m_dependentFlow.BufferStatus; } }

        public override string DisplayName
        {
            get { return this.m_dependentFlow.FullName; }
        }

        public override void Fault(Exception e)
        {
            this.m_dependentFlow.Fault(e);
        }

        public override object Unwrap()
        {
            return this.m_dependentFlow;
        }
    }
}
