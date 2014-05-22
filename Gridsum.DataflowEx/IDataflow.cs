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
    }

    public interface IDataflow<in TIn> : IDataflow
    {
        ITargetBlock<TIn> InputBlock { get; }
    }

    public interface IDataflow<in TIn, out TOut> : IDataflow<TIn>
    {
        ISourceBlock<TOut> OutputBlock { get; }
    }
}
