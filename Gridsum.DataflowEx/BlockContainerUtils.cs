using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx
{
    public class BlockContainerUtils
    {
        public static BlockContainerBase<TIn, TOut> FromBlock<TIn, TOut>(IPropagatorBlock<TIn, TOut> block)
        {
            return new PropagatorBlockContainer<TIn, TOut>(block);
        }

        public static BlockContainerBase<TIn, TOut> FromBlock<TIn, TOut>(IPropagatorBlock<TIn, TOut> block, BlockContainerOptions options)
        {
            return new PropagatorBlockContainer<TIn, TOut>(block, options);
        }
    }
}
