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
        private readonly Func<TIn, IEnumerable<TOut>> m_transformMany;
        private TransformManyBlock<TIn, TOut> m_block;

        public TransformManyDataflow(Func<TIn, IEnumerable<TOut>> transformMany)
            : base(DataflowOptions.Default)
        {
            this.m_transformMany = transformMany;
            m_block = new TransformManyBlock<TIn, TOut>(new Func<TIn, IEnumerable<TOut>>(Transform));
            RegisterChild(m_block);
        }

        private IEnumerable<TOut> Transform(TIn @in)
        {
            IsBusy = true;
            var outs = m_transformMany(@in);
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
