using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx
{
    using Gridsum.DataflowEx.AutoCompletion;

    public class PropagatorDataflow<TIn, TOut> : Dataflow<TIn, TOut>
    {
        private readonly IPropagatorBlock<TIn, TOut> m_block;
        
        public PropagatorDataflow(IPropagatorBlock<TIn, TOut> block) : this(block, DataflowOptions.Default)
        {}

        public PropagatorDataflow(IPropagatorBlock<TIn, TOut> block, DataflowOptions dataflowOptions)
            : base(dataflowOptions)
        {
            m_block = block;
            RegisterChild(m_block, null);
        }

        public override ITargetBlock<TIn> InputBlock
        {
            get { return m_block; }
        }

        public override ISourceBlock<TOut> OutputBlock
        {
            get { return m_block; }
        }
    }

    public class TransformManyDataflow<TIn, TOut> : Dataflow<TIn, TOut>, IRingNode
    {
        private readonly Func<TIn, Task<IEnumerable<TOut>>> m_transformMany;
        private TransformManyBlock<TIn, TOut> m_block;
        
        public TransformManyDataflow(Func<TIn, Task<IEnumerable<TOut>>> transformMany, DataflowOptions options)
            : base(options)
        {
            this.m_transformMany = transformMany;
            m_block = new TransformManyBlock<TIn, TOut>(new Func<TIn, Task<IEnumerable<TOut>>>(Transform), options.ToExecutionBlockOption());
            RegisterChild(m_block);
        }

        private async Task<IEnumerable<TOut>> Transform(TIn @in)
        {
            IsBusy = true;
            var outs = await m_transformMany(@in);
            IsBusy = false;
            return outs;
        }

        public override ITargetBlock<TIn> InputBlock
        {
            get
            {
                return m_block;
            }
        }

        public override ISourceBlock<TOut> OutputBlock
        {
            get
            {
                return m_block;
            }
        }

        public bool IsBusy { get; private set; }
    }
}
