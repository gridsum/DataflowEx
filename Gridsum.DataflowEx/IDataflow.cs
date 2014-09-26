using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx
{
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
    }

    public interface IDataflow<in TIn> : IDataflow
    {
        ITargetBlock<TIn> InputBlock { get; }
    }

    public interface IOutputDataflow<out TOut> : IDataflow
    {
        ISourceBlock<TOut> OutputBlock { get; }
        void LinkTo(IDataflow<TOut> other);
    }

    public interface IDataflow<in TIn, out TOut> : IDataflow<TIn>, IOutputDataflow<TOut>
    {
    }

    public interface IBatchedDataflow : IDataflow
    {
        void TriggerBatch();
    }
}
