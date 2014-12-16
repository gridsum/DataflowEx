using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx
{
    /// <summary>
    /// Represents a dataflow graph
    /// </summary>
    public interface IDataflow
    {
        IEnumerable<IDataflowBlock> Blocks { get; }
        Task CompletionTask { get; }
        void Fault(Exception exception);
        string Name { get; }
        string FullName { get; }
        int BufferedCount { get; }

        /// <summary>
        /// Signals to the IDataflow that it should not accept any more messages
        /// </summary>
        void Complete();

        void RegisterDependency(IDataflow dependencyDataflow);
    }

    /// <summary>
    /// Represents a dataflow graph that has a typed input node
    /// </summary>
    public interface IDataflow<in TIn> : IDataflow
    {
        /// <summary>
        /// The input node of the dataflow graph
        /// </summary>
        ITargetBlock<TIn> InputBlock { get; }
    }

    /// <summary>
    /// Represents a dataflow graph that has a typed output node
    /// </summary>
    public interface IOutputDataflow<out TOut> : IDataflow
    {
        /// <summary>
        /// /// The output node of the dataflow graph
        /// </summary>
        ISourceBlock<TOut> OutputBlock { get; }

        /// <summary>
        /// Links output of this dataflow to the input of another dataflow graph
        /// </summary>
        void LinkTo(IDataflow<TOut> other, Predicate<TOut> predicate);
    }

    /// <summary>
    /// Represents a dataflow graph that has a typed input node and a typed output node
    /// </summary>
    public interface IDataflow<in TIn, out TOut> : IDataflow<TIn>, IOutputDataflow<TOut>
    {
    }

    /// <summary>
    /// Represents a dataflow graph that has an internal batch buffer
    /// </summary>
    public interface IBatchedDataflow : IDataflow
    {
        void TriggerBatch();
    }
}
