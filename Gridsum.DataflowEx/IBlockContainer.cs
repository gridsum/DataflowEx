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

//    public class TransformBlockContainer<TIn, TOut> : BlockContainerBase<TIn, TOut>
//    {
//        private TransformBlock<TIn, TOut> m_transformBlock;
//
//        public TransformBlockContainer(Func<TIn, TOut> func)
//        {
//            m_transformBlock = new TransformBlock<TIn, TOut>(func);
//        }
//
//        public override ITargetBlock<TIn> InputBlock
//        {
//            get { return m_transformBlock; }
//        }
//
//        public override ISourceBlock<TOut> OutputBlock
//        {
//            get { return m_transformBlock; }
//        }
//    }
}
