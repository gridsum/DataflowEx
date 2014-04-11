using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx
{
    public abstract class BlockContainerBase
    {
        private static ConcurrentDictionary<string, StatisticsRecorder.IntHolder> s_nameDict = new ConcurrentDictionary<string, StatisticsRecorder.IntHolder>();

        protected Lazy<string> m_lazyName;

        public BlockContainerBase()
        {
            m_lazyName = new Lazy<string>(() =>
            {
                string friendlyName = Utils.GetFriendlyName(this.GetType());
                int count = s_nameDict.GetOrAdd(friendlyName, new StatisticsRecorder.IntHolder()).Increment();
                return friendlyName + count;
            });
        }

        public virtual string Name
        {
            get { return m_lazyName.Value; }
        }
    }

    public abstract class BlockContainerBase<TIn> : BlockContainerBase, IBlockContainer<TIn>
    {
        protected readonly BlockContainerOptions m_containerOptions;
        protected readonly DataflowLinkOptions m_defaultLinkOption;
        private IList<BlockMeta> m_blockMetas = new List<BlockMeta>();
        
        private class BlockMeta
        {
            public IDataflowBlock Block { get; set; }
            public Func<int> CountGetter { get; set; }
            public Task CompletionTask { get; set; }
        }

        public BlockContainerBase(BlockContainerOptions containerOptions)
        {
            m_containerOptions = containerOptions;
            m_defaultLinkOption = new DataflowLinkOptions() {PropagateCompletion = true};
            m_completionTask = new Lazy<Task>(GetCompletionTask);

            if (m_containerOptions.ContainerMonitorEnabled || m_containerOptions.BlockMonitorEnabled)
            {
                StartPerformanceMonitorAsync();
            }
        }

        //todo: needs a mandatory way to force block registeration
        protected void RegisterBlock(IDataflowBlock block, Func<int> countGetter, Action<Task> blockCompletionCallback = null)
        {
            if (m_completionTask.IsValueCreated)
            {
                throw new InvalidOperationException("You cannot register block after completion task has been generated. Please ensure you are calling RegisterBlock() inside constructor.");
            }

            var tcs = new TaskCompletionSource<object>();
            Exception exception = null;

            block.Completion.ContinueWith(task =>
            {
                try
                {
                    if (task.Status == TaskStatus.Faulted && task.Exception != null)
                    {
                        task.Exception.Flatten().Handle(e =>
                            {
                                if (exception == null) exception = e;

                                if (this.CompletionTask.IsCompleted || e is OtherBlockFailedException || e is OtherBlockContainerFailedException)
                                {
                                    //do nothing
                                }
                                else
                                {                                    
                                    this.Fault(e); //make sure everything in the container is down.
                                }
                                return true;
                            });
                    }

                    if (blockCompletionCallback != null)
                    {
                        blockCompletionCallback(task);
                    }                    
                }
                catch (Exception e)
                {
                    LogHelper.Logger.Error(h => h("[{0}] Error when shutting down working blocks in block container", this.Name), e);
                    if (exception == null) exception = e;
                }
            }).ContinueWith(t =>
            {
                if (exception != null)
                {
                    tcs.SetException(exception);
                }
                else
                    tcs.SetResult(string.Empty);                
            });

            m_blockMetas.Add(new BlockMeta { Block = block, CompletionTask = tcs.Task, CountGetter = countGetter ?? (() => -1) });                   
        }

        //todo: add completion condition and cancellation token support
        private async void StartPerformanceMonitorAsync()
        {
            while (true)
            {
                await Task.Delay(m_containerOptions.MonitorInterval ?? TimeSpan.FromSeconds(10));

                if (m_containerOptions.ContainerMonitorEnabled)
                {
                    int count = this.BufferedCount;

                    if (count != 0 || m_containerOptions.PerformanceMonitorMode == BlockContainerOptions.PerformanceLogMode.Verbose)
                    {
                        LogHelper.Logger.Debug(h => h("[{0}] has {1} todo items at this moment.", this.Name, count));
                    }
                }

                if (m_containerOptions.BlockMonitorEnabled)
                {
                    foreach( BlockMeta bm in m_blockMetas)
                    {
                        IDataflowBlock block = bm.Block;
                        var count = bm.CountGetter();

                        if (count != 0 || m_containerOptions.PerformanceMonitorMode == BlockContainerOptions.PerformanceLogMode.Verbose)
                        {
                            LogHelper.Logger.Debug(h => h("[{0}->{1}] has {2} todo items at this moment.", this.Name, Utils.GetFriendlyName(block.GetType()), count));    
                        }
                    }
                }
            }
        }

        private Lazy<Task> m_completionTask;

        protected virtual Task GetCompletionTask()
        {
            var taskAll = Task.WhenAll(m_blockMetas.Select(b => b.CompletionTask));

            //the following is just to flatten exceptions.
            var tcs = new TaskCompletionSource<string>();
            taskAll.ContinueWith(t =>
            {
                if (t.IsFaulted)
                {
                    var highPriorityException = t.Exception.Flatten().InnerExceptions.OrderByDescending(e => e, new ExceptionComparer()).First();
                    tcs.SetException(highPriorityException);
                }
                else if (t.IsCanceled)
                {
                    tcs.SetCanceled();
                }
                else
                {
                    try
                    {
                        this.CleanUp();
                        tcs.SetResult(string.Empty);
                    }
                    catch (Exception e)
                    {
                        tcs.SetException(e);
                    }                    
                }
            });

            return tcs.Task;
        }

        protected virtual void CleanUp()
        {
            //
        }
        
        private class ExceptionComparer : IComparer<Exception>
        {
            public int Compare(Exception x, Exception y)
            {
                if (x.GetType() == y.GetType())
                {
                    return 0;
                }

                if (x is OtherBlockFailedException)
                {
                    return -1;
                }

                if (y is OtherBlockFailedException)
                {
                    return 1;
                }

                if (x is OtherBlockContainerFailedException)
                {
                    return -1;
                }

                if (y is OtherBlockContainerFailedException)
                {
                    return 1;
                }

                return 0;
            }
        }

        public Task CompletionTask { 
            get 
            {
                //todo: check multiple access and block registration
                return m_completionTask.Value;
            } 
        }

        public virtual IEnumerable<IDataflowBlock> Blocks { get { return m_blockMetas.Select(bm => bm.Block); } }

        public abstract ITargetBlock<TIn> InputBlock { get; }

        public virtual void Fault(Exception exception, bool propagateException = false)
        {
            LogHelper.Logger.ErrorFormat("<{0}>Unrecoverable exception received. Shutting down my blocks...", exception, this.Name);

            foreach (var dataflowBlock in Blocks)
            {
                if (!dataflowBlock.Completion.IsCompleted)
                {
                    string msg = string.Format("<{0}>Shutting down {1}", this.Name, Utils.GetFriendlyName(dataflowBlock.GetType()));
                    LogHelper.Logger.Error(msg);
                    dataflowBlock.Fault(propagateException ? exception : new OtherBlockFailedException()); //just use aggregation exception just like native link
                }
            }
        }

        public virtual int BufferedCount
        {
            get
            {
                return m_blockMetas.Select(bm => bm.CountGetter).Sum(countGetter => countGetter());
            }
        }

        /// <summary>
        /// Helper method to read from a text reader and post everything in the text reader to the pipeline
        /// </summary>
        public void PullFrom(IEnumerable<TIn> reader, bool allowPostFailure = false)
        {
            long count = 0;
            foreach(var item in reader)
            {
                bool posted = InputBlock.Post(item);

                if (posted)
                {
                    count++;
                }
                else
                {
                    if (InputBlock.Completion.IsCompleted)
                    {
                        LogHelper.Logger.Info(h => h("<{0}> Post to the input block failed because it is completed. Stop posting...", this.Name));
                        break;
                    }
                    else
                    {
                        if (!allowPostFailure)
                        {
                            throw new PostToInputBlockFailedException(string.Format("<{0}> Post to the input block failed", this.Name));
                        }
                        else
                        {
                            LogHelper.Logger.Warn(h => h("<{0}> Post to the input block failed. Ignore this item.", this.Name));
                        }
                    }
                }
            }
            LogHelper.Logger.Info(h => h("<{0}> Posted {1} {2}s to the input block {3}.", 
                this.Name, 
                count, 
                Utils.GetFriendlyName(typeof(TIn)), 
                Utils.GetFriendlyName(this.InputBlock.GetType())
                ));
        }
    }

    public abstract class BlockContainerBase<TIn, TOut> : BlockContainerBase<TIn>, IBlockContainer<TIn, TOut>
    {
        protected List<Predicate<TOut>> m_conditions = new List<Predicate<TOut>>();
        protected StatisticsRecorder GarbageRecorder { get; private set; }

        protected BlockContainerBase(BlockContainerOptions containerOptions) : base(containerOptions)
        {
            this.GarbageRecorder = new StatisticsRecorder();
        }

        public abstract ISourceBlock<TOut> OutputBlock { get; }
        
        protected void LinkBlockToContainer<T>(ISourceBlock<T> block, IBlockContainer<T> otherBlockContainer)
        {
            block.LinkTo(otherBlockContainer.InputBlock, new DataflowLinkOptions { PropagateCompletion = false });

            //manullay handle inter-container problem
            Task.WhenAll(block.Completion, this.CompletionTask).ContinueWith(blockTask =>
                {
                    if (!otherBlockContainer.CompletionTask.IsCompleted)
                    {
                        if (blockTask.IsCanceled || blockTask.IsFaulted)
                        {
                            otherBlockContainer.Fault(new OtherBlockContainerFailedException(), true);
                        }
                        else
                        {
                            otherBlockContainer.InputBlock.Complete();
                        }
                    }
                });

            //Make sure 
            otherBlockContainer.CompletionTask.ContinueWith(otherTask =>
                {
                    if (this.CompletionTask.IsCompleted)
                    {
                        return;
                    }

                    if (otherTask.IsCanceled || otherTask.IsFaulted)
                    {
                        LogHelper.Logger.InfoFormat("<{0}>Downstream block container faulted before I am done. Shut down myself.", this.Name);
                        this.Fault(new OtherBlockContainerFailedException(), true);
                    }
                });
        }

        public void Link(IBlockContainer<TOut> other)
        {
            //this.OutputBlock.LinkTo(other.InputBlock, m_defaultOption);
            LinkBlockToContainer(this.OutputBlock, other);
        }

        public void TransformAndLink<TTarget>(IBlockContainer<TTarget> other, Func<TOut, TTarget> transform, Predicate<TOut> predicate)
        {
            m_conditions.Add(predicate);
            var converter = new TransformBlock<TOut, TTarget>(transform);
            this.OutputBlock.LinkTo(converter, m_defaultLinkOption, predicate);
            //converter.LinkTo(other.InputBlock, m_defaultOption); 

            LinkBlockToContainer(converter, other);            
        }

        public void TransformAndLink<TTarget>(IBlockContainer<TTarget> other, Func<TOut, TTarget> transform)
        {
            this.TransformAndLink(other, transform, @out => true);
        }

        public void TransformAndLink<TTarget>(IBlockContainer<TTarget> other) where TTarget : TOut
        {
            this.TransformAndLink(other, @out => { return ((TTarget)@out); }, @out => @out is TTarget);
        }

        public void TransformAndLink<TTarget, TOutSubType>(IBlockContainer<TTarget> other, Func<TOutSubType, TTarget> transform) where TOutSubType : TOut
        {
            this.TransformAndLink(other, @out => { return transform(((TOutSubType)@out)); }, @out => @out is TOutSubType);
        }

        public void LinkLeftToNull()
        {
            var left = new Predicate<TOut>(@out =>
                {
                    if (m_conditions.All(condition => !condition(@out)))
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

        protected virtual void OnOutputToNull(TOut output)
        {
            this.GarbageRecorder.RecordType(output.GetType());
        }
    }
}