using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx
{
    public interface IBlockContainer
    {
        IEnumerable<IDataflowBlock> Blocks { get; }
        Task CompletionTask { get; }
        void Fault(Exception exception, bool propagateException);
        string Name { get; }
    }

    public interface IBlockContainer<in TIn> : IBlockContainer
    {
        ITargetBlock<TIn> InputBlock { get; }
    }

    public interface IBlockContainer<in TIn, out TOut> : IBlockContainer<TIn>
    {
        ISourceBlock<TOut> OutputBlock { get; }
    }
}
