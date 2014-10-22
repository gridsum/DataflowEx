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

    using Gridsum.DataflowEx.AutoCompletion;

    /// <summary>
    /// Core concept of DataflowEx. Represents a reusable dataflow component with its processing logic, which
    /// may contain one or multiple children. A child could be either a block or a dataflow.
    /// Inheritors of this class should call RegisterChild in their constructors.
    /// </summary>
    public class Dataflow : IDataflow
    {
        private static ConcurrentDictionary<string, IntHolder> s_nameDict = new ConcurrentDictionary<string, IntHolder>();
        protected internal readonly DataflowOptions m_dataflowOptions;
        protected readonly DataflowLinkOptions m_defaultLinkOption;
        protected Lazy<Task> m_completionTask;
        protected ImmutableList<IDataflowDependency> m_children = ImmutableList.Create<IDataflowDependency>();
        protected ImmutableList<Dataflow> m_parents = ImmutableList.Create<Dataflow>();
        protected ImmutableList<Func<Task>> m_postDataflowTasks = ImmutableList.Create<Func<Task>>();
        protected ImmutableList<CancellationTokenSource> m_ctsList = ImmutableList.Create<CancellationTokenSource>();
        protected ImmutableList<IDataflowDependency> m_dependencies = ImmutableList.Create<IDataflowDependency>();
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
            set
            {
                if (value != null)
                {
                    m_defaultName = value;
                }
            }
        }

        public DataflowOptions DataflowOptions
        {
            get
            {
                return m_dataflowOptions;
            }
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

        public ImmutableList<IDataflowDependency> Children
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
        /// Register this block as child. Also make sure the dataflow will fail if the registered block fails.
        /// </summary>
        public void RegisterChild(IDataflowBlock block, Action<Task> blockCompletionCallback = null, bool allowDuplicate = false, string displayName = null)
        {
            if (block == null)
            {
                throw new ArgumentNullException("block");
            }

            RegisterChild(new BlockDependency(block, this, DependencyKind.Internal, blockCompletionCallback, displayName), allowDuplicate);
        }

        /// <summary>
        /// Register a sub dataflow as child. Also make sure the dataflow will fail if the registered dataflow fails.
        /// </summary>
        public void RegisterChild(Dataflow childFlow, Action<Task> dataflowCompletionCallback = null, bool allowDuplicate = false)
        {
            if (childFlow == null)
            {
                throw new ArgumentNullException("childFlow");
            }

            if (childFlow.IsMyChild(this))
            {
                throw new ArgumentException(
                    string.Format("{0} Cannot register a child {1} who is already my ancestor", this.FullName, childFlow.FullName));
            }
         
            //add myself as parents
            childFlow.m_parents = childFlow.m_parents.Add(this);
            RegisterChild(new DataflowDependency(childFlow, this, DependencyKind.Internal, dataflowCompletionCallback), allowDuplicate);
        }

        /// <summary>
        /// Register multiple blocks as children
        /// </summary>
        public void RegisterChildren(params IDataflowBlock[] blocks)
        {
            foreach (var dataflowBlock in blocks)
            {
                this.RegisterChild(dataflowBlock);
            }
        }

        /// <summary>
        /// Register multiple sub-flows as children
        /// </summary>
        public void RegisterChildren(params Dataflow[] subFlows)
        {
            foreach (var flow in subFlows)
            {
                this.RegisterChild(flow);
            }
        }
        
        /// <summary>
        /// Check if the flow is my child or myself
        /// </summary>
        public bool IsMyChild(IDataflow flow)
        {
            return this.IsMyChildImpl(flow);
        }

        /// <summary>
        /// Check if the flow is my child or myself
        /// </summary>
        public bool IsMyChild(IDataflowBlock block)
        {
            return this.IsMyChildImpl(block);
        }

        private bool IsMyChildImpl(object obj)
        {
            if (object.ReferenceEquals(obj, this)) return true;

            foreach (IDataflowDependency child in m_children)
            {
                if (child is BlockDependency)
                {
                    if (object.ReferenceEquals(obj, child.Unwrap()))
                    {
                        return true;
                    }
                }
                else if (child is DataflowDependency)
                {
                    if ((child as DataflowDependency).Flow.IsMyChildImpl(obj))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        internal void RegisterChild(IDataflowDependency childMeta, bool allowDuplicate)
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
                        foreach(IDataflowDependency child in m_children)
                        {
                            var bufferStatus = child.BufferStatus;

                            if (bufferStatus.Total() != 0 || m_dataflowOptions.PerformanceMonitorMode == DataflowOptions.PerformanceLogMode.Verbose)
                            {
                                IDataflowDependency c = child;
                                LogHelper.PerfMon.Debug(h => h("{0} has {1} todo items (in:{2}, out:{3}) at this moment. ", c.DisplayName, bufferStatus.Total(), bufferStatus.Item1, bufferStatus.Item2));
                            }
                        }
                    }

                    CustomPerfMonBehavior();

                    await Task.Delay(m_dataflowOptions.MonitorInterval).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                LogHelper.Logger.ErrorFormat("{0} Error occurred in my performance monitor loop. Monitoring stopped.", e, this.FullName);
            }
        }

        /// <summary>
        /// Register a continuous ring completion check after the preTask ends.
        /// When the checker thinks the ring is O.K. to shut down, it will pull the trigger 
        /// of heartbeat node in the ring. Then nodes in the ring will shut down one by one
        /// </summary>
        /// <param name="preTask">The task after which ring check begins</param>
        /// <param name="ringNodes">Child dataflows that forms a ring</param>
        protected async void RegisterChildRing(Task preTask, params IRingNode[] ringNodes)
        {
            var ring = new RingMonitor(this, ringNodes);
            //todo: check if it is a real ring

            ring.StartMonitoring(preTask);
        }

        protected virtual async Task GetCompletionTask()
        {
            //waiting until some children is registered
            if (m_children.Count == 0)
            {
                LogHelper.Logger.WarnFormat("{0} still has no children. Will check again soon.", this.FullName);
                await Task.Delay(m_dataflowOptions.MonitorInterval).ConfigureAwait(false);

                if (m_children.Count == 0)
                {
                    throw new NoChildRegisteredException(this);
                }
            }

            try
            {
                await TaskEx.AwaitableWhenAll(() => m_children, b => b.Completion).ConfigureAwait(false);
                await TaskEx.AwaitableWhenAll(() => m_postDataflowTasks, f => f()).ConfigureAwait(false);
                
                //todo: move it to finally
                this.CleanUp();
                LogHelper.Logger.Info(string.Format("{0} completed", this.FullName));
            }
            catch (AggregateException e)
            {
                foreach (var cts in m_ctsList)
                {
                    cts.Cancel();
                }
                throw; //TaskEx.UnwrapWithPriority(e);
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
                if (!child.Completion.IsCompleted)
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

        public virtual void Complete()
        {
            throw new NotSupportedException(string.Format("{0} doesn't know how to explicitly complete.", this.FullName));
        }

        //Linkd MY block to OHTER dataflow
        protected void LinkBlockToFlow<T>(ISourceBlock<T> block, IDataflow<T> otherDataflow, Predicate<T> predicate = null)
        {
            if (!IsMyChild(block))
            {
                throw new InvalidOperationException(string.Format("{0} Cannot link block to flow as the output block is not my child.", this.FullName));
            }

            if (predicate == null)
            {
                block.LinkTo(otherDataflow.InputBlock);
            }
            else
            {
                block.LinkTo(otherDataflow.InputBlock, predicate);
            }

            otherDataflow.RegisterDependency(this);

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

        /// <summary>
        /// Register an external dataflow as dependency, whose completion will trigger completion of this dataflow
        /// if this dependencies finishes as the last amony all dependencies.
        /// i.e. Completion of this dataflow will only be triggered after ALL dependencies finish.
        /// </summary>
        public void RegisterDependency(IDataflow dependencyDataflow)
        {
            if (this.IsMyChild(dependencyDataflow))
            {
                throw new ArgumentException("Cannot register a child as dependency. Child: " + dependencyDataflow.FullName, "dependencyDataflow");
            }

            this.RegisterDependency(new DataflowDependency(dependencyDataflow, this, DependencyKind.External));
        }

        public void RegisterDependency(IDataflowBlock upstreamBlock)
        {
            this.RegisterDependency(new BlockDependency(upstreamBlock, this, DependencyKind.External));
        }

        /// <summary>
        /// Register an external block as dependency, whose completion will trigger completion of this dataflow
        /// if this dependencies finishes as the last amony all dependencies.
        /// i.e. Completion of this dataflow will only be triggered after ALL dependencies finish.
        /// </summary>
        public void RegisterDependency(IDataflowDependency dependency)
        {
            bool isFirstDependency = m_dependencies.IsEmpty;

            m_dependencies = m_dependencies.Add(dependency);

            if (isFirstDependency)
            {
                TaskEx.AwaitableWhenAll(() => m_dependencies, f => f.Completion).ContinueWith(
                    upstreamTask =>
                    {
                        if (!this.CompletionTask.IsCompleted)
                        {
                            if (upstreamTask.IsFaulted)
                            {
                                this.Fault(new LinkedDataflowFailedException());
                            }
                            else if (upstreamTask.IsCanceled)
                            {
                                this.Fault(new LinkedDataflowCanceledException());
                            }
                            else
                            {
                                if (m_dependencies.Count > 1)
                                {
                                    LogHelper.Logger.InfoFormat("{0} All of my dependencies are done. Completing myself.", this.FullName);
                                }
                                this.Complete();
                            }
                        }
                    });
            }
            else
            {
                LogHelper.Logger.InfoFormat("{0} now has {1} dependencies.", this.FullName, m_dependencies.Count);
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
                async () =>
                    {
                        long count = 0;
                        try
                        {
                            foreach (var item in reader)
                            {
                                ct.ThrowIfCancellationRequested();
                                await this.SendAsync(item).ConfigureAwait(false);
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

        public void LinkFromBlock(ISourceBlock<TIn> block)
        {
            block.LinkTo(this.InputBlock, m_defaultLinkOption);
        }

        protected string ReceiverDisplayName
        {
            get
            {
                foreach (var dataflowMeta in m_children.OfType<DataflowDependency>())
                {
                    if (dataflowMeta.Blocks.Contains(this.InputBlock))
                    {
                        return dataflowMeta.Flow.FullName;
                    }
                }
                return "my input block " + this.InputBlock.GetType().GetFriendlyName();
            }
        }

        /// <summary>
        /// Asynchronously read from the enumerable and process items in the underlying dataflow.
        /// </summary>
        /// <param name="enumerable">The enumerable to read from</param>
        /// <param name="completeFlowOnFinish">
        /// Whether a complete signal should be sent to the dataflow. 
        /// If yes, it also ensures that the whole processing dataflow is completed before the ProcessAsync() task ends.
        /// Default to yes. Set the param to false if the dataflow will read other enumerables/streams after this operation.
        /// </param>
        /// <returns>A task representing state of the async operation which returns the total count of items processed in this method</returns>
        public virtual async Task<long> ProcessAsync(IEnumerable<TIn> enumerable, bool completeFlowOnFinish = true)
        {
            var cts = new CancellationTokenSource();
            Task<long> readAndPostTask = this.PullFromAsync(enumerable, cts.Token);
            this.RegisterCancellationTokenSource(cts);

            long count;
            try
            {
                count = await readAndPostTask.ConfigureAwait(false);
                LogHelper.Logger.InfoFormat("{0} Finished reading from enumerable and posting to the dataflow.", this.FullName);
            }
            catch (OperationCanceledException oce)
            {
                LogHelper.Logger.InfoFormat("{0} Reading from enumerable canceled halfway. Possibly there is something wrong with dataflow processing.", this.FullName);
                throw;
            }

            if (completeFlowOnFinish)
            {
                await this.SignalAndWaitForCompletionAsync().ConfigureAwait(false);
            }

            return count;
        }

        /// <summary>
        /// Send a complete signal to the dataflow and return a task representing the completion task
        /// of the dataflow. The task will be completed when all queued jobs are done.
        /// </summary>
        public async Task SignalAndWaitForCompletionAsync()
        {
            LogHelper.Logger.InfoFormat("{0} Telling myself there is no more input and wait for children completion", this.FullName);
            this.InputBlock.Complete(); //no more input
            await this.CompletionTask.ConfigureAwait(false);
        }

        /// <summary>
        /// Asynchronously read from the enumerables sequentially and process items in the underlying dataflow.
        /// </summary>
        /// <param name="enumerables">The enumerables to read from</param>
        /// <param name="completeLogReaderOnFinish">
        /// Whether a complete signal should be sent to the dataflow. 
        /// If yes, it also ensures that the whole processing dataflow is completed before the ProcessAsync() task ends.
        /// Default to yes. Set the param to false if the log reader will read other enumerables after this operation.
        /// </param>
        /// <returns>A task representing the state of the async operation which returns the total count of items processed in this method</returns>
        public virtual async Task<long> ProcessMultipleAsync(IEnumerable<IEnumerable<TIn>> enumerables, bool completeLogReaderOnFinish = true)
        {
            long count = 0;
            foreach (var enumerable in enumerables)
            {
                count += await ProcessAsync(enumerable, false).ConfigureAwait(false);
            }

            if (completeLogReaderOnFinish)
            {
                await this.SignalAndWaitForCompletionAsync().ConfigureAwait(false);
            }

            return count;
        }

        /// <summary>
        /// Asynchronously read from the enumerables sequentially and process items in the underlying dataflow.
        /// </summary>
        /// <param name="completeLogReaderOnFinish">
        /// Whether a complete signal should be sent to the dataflow. 
        /// If yes, it also ensures that the whole processing dataflow is completed before the ProcessAsync() task ends.
        /// Default to yes. Set the param to false if the log reader will read other enumerables after this operation.
        /// </param>
        /// <param name="enumerables">The enumerables to read from</param>
        /// <returns>A task representing the state of the async operation which returns the total count of items processed in this method</returns>
        public virtual Task<long> ProcessMultipleAsync(bool completeLogReaderOnFinish, params IEnumerable<TIn>[] enumerables)
        {
            return ProcessMultipleAsync(enumerables, completeLogReaderOnFinish);
        }

        public override void Complete()
        {
            this.InputBlock.Complete();
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

        /// <summary>
        /// Link data stream to the given dataflow and propagates completion
        /// </summary>
        /// <param name="other">The dataflow to connect to</param>
        /// <param name="predicate">The filtering condition, if any</param>
        public void LinkTo(IDataflow<TOut> other, Predicate<TOut> predicate = null)
        {
            this.GoTo(other, predicate);
        }

        /// <summary>
        /// Link data stream to the given dataflow and propagates completion. 
        /// </summary>
        /// <remarks>
        /// Same as LinkTo() but this method helps you to chain calls like a.GoTo(b).GoTo(c);
        /// </remarks>
        /// <param name="other">The dataflow to connect to</param>
        /// <returns>returns the dataflow to connect to</returns>
        public virtual IDataflow<TOut> GoTo(IDataflow<TOut> other, Predicate<TOut> predicate = null)
        {
            m_condBuilder.Add(predicate ?? new Predicate<TOut>(@out => true));
            LinkBlockToFlow(this.OutputBlock, other, predicate);
            return other;
        }

        public Dataflow<TOut, TNext> GoTo<TNext>(Dataflow<TOut, TNext> other)
        {
            return GoTo(other as IDataflow<TOut>) as Dataflow<TOut, TNext>;
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

            this.OutputBlock.LinkTo(other.InputBlock, new DataflowLinkOptions { PropagateCompletion = false }, predicate, transform);

            other.RegisterDependency(this);
        }

        public void TransformAndLink<TTarget>(IDataflow<TTarget> other, Func<TOut, TTarget> transform)
        {
            this.TransformAndLink(other, transform, @out => true);
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

        //
        public void LinkLeftTo(IDataflow<TOut> target)
        {
            var frozenConds = m_frozenConditions.Value;
            var left = new Predicate<TOut>(@out => frozenConds.All(condition => !condition(@out)));

            this.LinkBlockToFlow(this.OutputBlock, target, left);
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
                    }, m_dataflowOptions.ToExecutionBlockOption());

            this.OutputBlock.LinkTo(actionBlock, m_defaultLinkOption, left);
            this.RegisterChild(actionBlock);
        }

        protected virtual void OnOutputToNull(TOut output)
        {
            this.GarbageRecorder.Record(output);
        }
    }
}