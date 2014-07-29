using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Gridsum.DataflowEx.Exceptions;
using Gridsum.DataflowEx.PatternMatch;

namespace Gridsum.DataflowEx
{
    using System.IO;

    /// <summary>
    /// Core concept of DataflowEx. Represents a reusable dataflow component with its processing logic, which
    /// may contain one or multiple children. A child could be either a block or a dataflow
    /// Inheritors of this class should call RegisterBlock in their constructors.
    /// </summary>
    public class Dataflow : IDataflow
    {
        private static ConcurrentDictionary<string, IntHolder> s_nameDict = new ConcurrentDictionary<string, IntHolder>();
        protected readonly DataflowOptions m_dataflowOptions;
        protected readonly DataflowLinkOptions m_defaultLinkOption;
        protected Lazy<Task> m_completionTask;
        protected ImmutableList<IDataflowChildMeta> m_children = ImmutableList.Create<IDataflowChildMeta>();
        protected ImmutableList<Dataflow> m_parents = ImmutableList.Create<Dataflow>();
        protected ImmutableList<Func<Task>> m_postDataflowTasks = ImmutableList.Create<Func<Task>>();
        protected ImmutableList<CancellationTokenSource> m_ctsList = ImmutableList.Create<CancellationTokenSource>();
        protected string m_defaultName;

        public Dataflow(DataflowOptions dataflowOptions)
        {
            m_dataflowOptions = dataflowOptions;
            m_defaultLinkOption = new DataflowLinkOptions() { PropagateCompletion = true };
            m_completionTask = new Lazy<Task>(GetCompletionTask, LazyThreadSafetyMode.ExecutionAndPublication);

            string friendlyName = Utils.GetFriendlyName(this.GetType());
            int count = s_nameDict.GetOrAdd(friendlyName, new IntHolder()).Increment();
            m_defaultName = friendlyName + count;
            
            if (m_dataflowOptions.FlowMonitorEnabled || m_dataflowOptions.BlockMonitorEnabled)
            {
                StartPerformanceMonitorAsync();
            }
        }

        /// <summary>
        /// Display name of the dataflow
        /// </summary>
        public virtual string Name
        {
            get { return m_defaultName; }
        }

        public string FullName
        {
            get
            {
                if (m_parents.IsEmpty)
                {
                    return string.Format("[{0}]", this.Name);
                }
                else if (m_parents.Count == 1)
                {
                    return string.Format("{0}->[{1}]", m_parents[0].FullName, this.Name);
                }
                else
                {
                    var parentPaths = string.Join("|", m_parents.Select(p => p.FullName));
                    return string.Format("({0})->[{1}]", parentPaths, this.Name);
                }
            }
        }

        public ImmutableList<IDataflowChildMeta> Children
        {
            get
            {
                return m_children;
            }
        }

        public ImmutableList<Dataflow> Parents
        {
            get
            {
                return m_parents;
            }
        }
        
        /// <summary>
        /// Register this block to block meta. Also make sure the dataflow will fail if the registered block fails.
        /// </summary>
        public void RegisterChild(IDataflowBlock block, Action<Task> blockCompletionCallback = null, bool allowDuplicate = false)
        {
            if (block == null)
            {
                throw new ArgumentNullException("block");
            }

            RegisterChild(new BlockMeta(block, this, blockCompletionCallback), allowDuplicate);
        }

        public void RegisterChild(Dataflow childFlow, Action<Task> dataflowCompletionCallback = null, bool allowDuplicate = false)
        {
            if (childFlow == null)
            {
                throw new ArgumentNullException("childFlow");
            }

            if (this.HasCircularDependency(childFlow))
            {
                throw new ArgumentException(
                    string.Format("{0} Cannot register a child {1} who is already my ancestor", this.FullName, childFlow.FullName));
            }
         
            //add myself as parents
            childFlow.m_parents = childFlow.m_parents.Add(this);
            RegisterChild(new ChildDataflowMeta(childFlow, this, dataflowCompletionCallback), allowDuplicate);
        }

        private bool HasCircularDependency(Dataflow flow)
        {
            if (object.ReferenceEquals(flow, this)) return true;

            foreach (var subFlow in flow.Children.OfType<ChildDataflowMeta>().Select(_ => _.Flow))
            {
                if (this.HasCircularDependency(subFlow))
                {
                    return true;
                }
            }

            return false;
        }

        internal void RegisterChild(IDataflowChildMeta childMeta, bool allowDuplicate)
        {
            var child = childMeta.Unwrap();

            if (m_children.Any(cm => object.ReferenceEquals(cm.Unwrap(), child)))
            {
                if (allowDuplicate)
                {
                    LogHelper.Logger.DebugFormat("Duplicate child registration ignored in {0}: {1}", this.FullName, childMeta.DisplayName);
                    return;
                }
                else
                {
                    throw new ArgumentException("Duplicate child to register in " + this.FullName);
                }
            }

            m_children = m_children.Add(childMeta);

            if (! m_completionTask.IsValueCreated)
            {
                //eagerly initialize completion task
                //would look better if Lazy<T> provides an EagerEvaluate() method
                var t = m_completionTask.Value; 
            }
        }

        /// <summary>
        /// Register a post dataflow job, which will be executed and awaited after all children of the dataflow is done.
        /// The gived job effects the completion task of the dataflow
        /// </summary>
        /// <param name="postDataflowTask"></param>
        public void RegisterPostDataflowTask(Func<Task> postDataflowTask)
        {
            m_postDataflowTasks = m_postDataflowTasks.Add(postDataflowTask);
        }

        /// <summary>
        /// Register a cancellation token source which will be signaled to external components 
        /// if this dataflow runs into error. (Useful to terminate input stream to the dataflow
        /// together with PullFromAsync)
        /// </summary>
        public void RegisterCancellationTokenSource(CancellationTokenSource cts)
        {
            m_ctsList = m_ctsList.Add(cts);

            if (m_children.Count != 0)
            {
            }
        }

        private async void StartPerformanceMonitorAsync()
        {
            try
            {
                while (m_children.Count == 0 || !this.CompletionTask.IsCompleted)
                {
                    if (m_dataflowOptions.FlowMonitorEnabled)
                    {
                        var bufferStatus = this.BufferStatus;

                        if (bufferStatus.Total() != 0 || m_dataflowOptions.PerformanceMonitorMode == DataflowOptions.PerformanceLogMode.Verbose)
                        {
                            LogHelper.PerfMon.Debug(h => h("{0} has {1} todo items (in:{2}, out:{3}) at this moment.", this.FullName, bufferStatus.Total(), bufferStatus.Item1, bufferStatus.Item2));
                        }
                    }

                    if (m_dataflowOptions.BlockMonitorEnabled)
                    {
                        foreach(var child in m_children)
                        {
                            var bufferStatus = child.BufferStatus;

                            if (bufferStatus.Total() != 0 || m_dataflowOptions.PerformanceMonitorMode == DataflowOptions.PerformanceLogMode.Verbose)
                            {
                                IDataflowChildMeta c = child;
                                LogHelper.PerfMon.Debug(h => h("{0} has {1} todo items (in:{2}, out:{3}) at this moment. ", c.DisplayName, bufferStatus.Total(), bufferStatus.Item1, bufferStatus.Item2));
                            }
                        }
                    }

                    CustomPerfMonBehavior();

                    await Task.Delay(m_dataflowOptions.MonitorInterval ?? TimeSpan.FromSeconds(10));
                }
            }
            catch (Exception e)
            {
                LogHelper.Logger.ErrorFormat("{0} Error occurred in my performance monitor loop. Monitoring stopped.", e, this.FullName);
            }
        }

        protected virtual async Task GetCompletionTask()
        {
            //waiting until some children is registered
            while (m_children.Count == 0)
            {
                await Task.Delay(m_dataflowOptions.MonitorInterval ?? TimeSpan.FromSeconds(10));
            }

            try
            {
                await TaskEx.AwaitableWhenAll(() => m_children, b => b.ChildCompletion);
                await TaskEx.AwaitableWhenAll(() => m_postDataflowTasks, f => f());
                
                this.CleanUp();
                LogHelper.Logger.Info(string.Format("Dataflow {0} completed", this.FullName));
            }
            catch (Exception)
            {
                foreach (var cts in m_ctsList)
                {
                    cts.Cancel();
                }
                throw;
            }
        }

        protected virtual void CleanUp()
        {
            //
        }

        /// <summary>
        /// Customization entry point for custom performance monitoring codes. Default impl is empty
        /// </summary>
        protected virtual void CustomPerfMonBehavior()
        {
        }

        /// <summary>
        /// Represents the completion of the whole dataflow
        /// </summary>
        public Task CompletionTask
        {
            get
            {
                return m_completionTask.Value;
            }
        }

        public virtual IEnumerable<IDataflowBlock> Blocks { get { return m_children.SelectMany(bm => bm.Blocks); } }

        public virtual void Fault(Exception exception)
        {
            LogHelper.Logger.ErrorFormat("{0} Exception occur. Shutting down my children...", exception, this.FullName);

            foreach (var child in m_children)
            {
                if (!child.ChildCompletion.IsCompleted)
                {
                    string msg = string.Format("{0} is shutting down", child.DisplayName);
                    LogHelper.Logger.Error(msg);

                    //just pass on PropagatedException (do not use original exception here)
                    if (exception is PropagatedException)
                    {
                        child.Fault(exception);
                    }
                    else if (exception is TaskCanceledException)
                    {
                        child.Fault(new SiblingUnitCanceledException());
                    }
                    else
                    {
                        child.Fault(new SiblingUnitFailedException());
                    }
                }
            }
        }

        /// <summary>
        /// By default, sum of the buffer size of all children in the dataflow
        /// </summary>
        public virtual Tuple<int, int> BufferStatus
        {
            get
            {
                int i = 0;
                int o = 0;
                foreach(var c in m_children)
                {
                    var pair = c.BufferStatus;
                    i += pair.Item1;
                    o += pair.Item2;
                }

                return new Tuple<int, int>(i, o);
            }
        }

        public int BufferedCount
        {
            get
            {
                return BufferStatus.Total();                
            }
        }
    }

    public abstract class Dataflow<TIn> : Dataflow, IDataflow<TIn>
    {
        protected Dataflow(DataflowOptions dataflowOptions) : base(dataflowOptions)
        {
        }

        public abstract ITargetBlock<TIn> InputBlock { get; }
        
        /// <summary>
        /// Helper method to pull from a data source and post everything from the data source to the pipeline
        /// </summary>
        public Task<long> PullFromAsync(IEnumerable<TIn> reader, CancellationToken ct)
        {
            return Task.Run(
                () =>
                    {
                        long count = 0;
                        try
                        {
                            foreach (var item in reader)
                            {
                                ct.ThrowIfCancellationRequested();
                                InputBlock.SafePost(item);
                                count++;
                            }
                        }
                        catch (Exception)
                        {
                            LogHelper.Logger.WarnFormat(
                                "{0} Pulled and posted {1} {2}s to {3} before an exception",
                                this.FullName,
                                count,
                                typeof(TIn).GetFriendlyName(),
                                ReceiverDisplayName);

                            throw;
                        }

                        LogHelper.Logger.InfoFormat(
                            "{0} Successfully pulled and posted {1} {2}s to {3}.",
                            this.FullName,
                            count,
                            typeof(TIn).GetFriendlyName(),
                            ReceiverDisplayName);

                        return count;
                    },
                ct);
        }

        public void LinkFrom(ISourceBlock<TIn> block)
        {
            block.LinkTo(this.InputBlock, m_defaultLinkOption);
        }

        protected string ReceiverDisplayName
        {
            get
            {
                foreach (var dataflowMeta in m_children.OfType<ChildDataflowMeta>())
                {
                    if (dataflowMeta.Blocks.Contains(this.InputBlock))
                    {
                        return dataflowMeta.Flow.FullName;
                    }
                }
                return "my input block " + this.InputBlock.GetType().GetFriendlyName();
            }
        }
    }

    public abstract class Dataflow<TIn, TOut> : Dataflow<TIn>, IDataflow<TIn, TOut>
    {
        protected ImmutableList<Predicate<TOut>>.Builder m_condBuilder = ImmutableList<Predicate<TOut>>.Empty.ToBuilder();
        protected Lazy<ImmutableList<Predicate<TOut>>> m_frozenConditions;

        /// <summary>
        /// History recorder of the objects dumped to null from this dataflow (by using LinkLeftToNull() ).
        /// </summary>
        public StatisticsRecorder GarbageRecorder { get; private set; }

        protected Dataflow(DataflowOptions dataflowOptions) : base(dataflowOptions)
        {
            this.GarbageRecorder = new StatisticsRecorder();
            m_condBuilder = ImmutableList<Predicate<TOut>>.Empty.ToBuilder();
            m_frozenConditions = new Lazy<ImmutableList<Predicate<TOut>>>(() =>
            {
                return m_condBuilder.ToImmutable();
            });
        }

        public abstract ISourceBlock<TOut> OutputBlock { get; }
        
        protected void LinkBlockToFlow<T>(ISourceBlock<T> block, IDataflow<T> otherDataflow)
        {
            block.LinkTo(otherDataflow.InputBlock, new DataflowLinkOptions { PropagateCompletion = false });

            //manullay handle inter-dataflow problem
            //we use WhenAll here to make sure this dataflow fails before propogating to other dataflow
            Task.WhenAll(block.Completion, this.CompletionTask).ContinueWith(whenAllTask => 
                {
                    if (!otherDataflow.CompletionTask.IsCompleted)
                    {
                        if (whenAllTask.IsFaulted)
                        {
                            otherDataflow.Fault(new LinkedDataflowFailedException());
                        }
                        else if (whenAllTask.IsCanceled)
                        {
                            otherDataflow.Fault(new LinkedDataflowCanceledException());
                        }
                        else
                        {
                            otherDataflow.InputBlock.Complete();
                        }
                    }
                });

            //Make sure other dataflow also fails me
            otherDataflow.CompletionTask.ContinueWith(otherTask =>
                {
                    if (this.CompletionTask.IsCompleted)
                    {
                        return;
                    }

                    if (otherTask.IsFaulted)
                    {
                        LogHelper.Logger.InfoFormat("{0} Downstream dataflow faulted before I am done. Fault myself.", this.FullName);
                        this.Fault(new LinkedDataflowFailedException());
                    }
                    else if (otherTask.IsCanceled)
                    {
                        LogHelper.Logger.InfoFormat("{0} Downstream dataflow canceled before I am done. Cancel myself.", this.FullName);
                        this.Fault(new LinkedDataflowCanceledException());
                    }
                });
        }

        public void LinkTo(IDataflow<TOut> other)
        {
            m_condBuilder.Add(new Predicate<TOut>(@out => true));
            LinkBlockToFlow(this.OutputBlock, other);
        }

        public void TransformAndLink<TTarget>(IDataflow<TTarget> other, Func<TOut, TTarget> transform, IMatchCondition<TOut> condition)
        {
            this.TransformAndLink(other, transform, new Predicate<TOut>(condition.Matches));
        }

        public void TransformAndLink<TTarget>(IDataflow<TTarget> other, Func<TOut, TTarget> transform, Predicate<TOut> predicate)
        {
            if (m_frozenConditions.IsValueCreated)
            {
                throw new InvalidOperationException("You cannot call TransformAndLink after LinkLeftToNull has been called");
            }

            m_condBuilder.Add(predicate);
            var converter = new TransformBlock<TOut, TTarget>(transform);
            this.OutputBlock.LinkTo(converter, m_defaultLinkOption, predicate);
            
            LinkBlockToFlow(converter, other);            
        }

        public void TransformAndLink<TTarget>(IDataflow<TTarget> other, Func<TOut, TTarget> transform)
        {
            this.TransformAndLink(other, transform, @out => true);
        }

        public void TransformAndLink<TTarget>(IDataflow<TTarget> other) where TTarget : TOut
        {
            this.TransformAndLink(other, @out => { return ((TTarget)@out); }, @out => @out is TTarget);
        }

        public void TransformAndLink<TTarget, TOutSubType>(IDataflow<TTarget> other, Func<TOutSubType, TTarget> transform) where TOutSubType : TOut
        {
            this.TransformAndLink(other, @out => { return transform(((TOutSubType)@out)); }, @out => @out is TOutSubType);
        }

        /// <summary>
        /// Link all outputs that are in a certain sub type to the given dataflow which accepts the sub type
        /// </summary>
        /// <typeparam name="TTarget">The type the given dataflow accepts. Must be subtype of the output type of this dataflow</typeparam>
        /// <param name="other">The given dataflow which is linked to</param>
        public void LinkSubTypeTo<TTarget>(IDataflow<TTarget> other) where TTarget : TOut
        {
            this.TransformAndLink(other, @out => (TTarget)@out, @out => @out is TTarget);
        }
        
        /// <summary>
        /// After this dataflow has linked to some targets conditionally, it is possible that some output objects
        /// are 'left' (i.e. matches none of the conditions). This methods provides a way to easily dump these left objects
        /// as garbage to the Null target. Otherwise these objects will stay in the buffer this dataflow which will never
        /// come to an end state.
        /// </summary>
        public void LinkLeftToNull()
        {
            var frozenConds = m_frozenConditions.Value;
            var left = new Predicate<TOut>(@out =>
                {
                    if (frozenConds.All(condition => !condition(@out)))
                    {
                        OnOutputToNull(@out);
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
                );

            this.OutputBlock.LinkTo(DataflowBlock.NullTarget<TOut>(), m_defaultLinkOption, left);
        }

        /// <summary>
        /// After this dataflow has linked to some targets conditionally, it is possible that some output objects
        /// are 'left' (i.e. matches none of the conditions). This methods provides a way to fail the whole dataflow 
        /// fast in case any output object is 'left' behind, which is unexpected.
        /// </summary>
        public void LinkLeftToError()
        {
            var frozenConds = m_frozenConditions.Value;
            var left = new Predicate<TOut>(@out => frozenConds.All(condition => !condition(@out)));

            var actionBlock = new ActionBlock<TOut>(
                (survivor) =>
                    {
                        LogHelper.Logger.ErrorFormat(
                            "{0} This is my error destination. Data should not arrive here: {1}", 
                            this.FullName, 
                            survivor);
                        throw new InvalidDataException(string.Format("An object came to error region of {0}: {1}", this.FullName, survivor));
                    });

            this.OutputBlock.LinkTo(actionBlock, m_defaultLinkOption, left);
            this.RegisterChild(actionBlock);
        }

        protected virtual void OnOutputToNull(TOut output)
        {
            this.GarbageRecorder.Record(output);
        }
    }
}