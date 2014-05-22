using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Gridsum.DataflowEx.AutoCompletion;
using Gridsum.DataflowEx.Exceptions;
using Microsoft.CSharp.RuntimeBinder;

namespace Gridsum.DataflowEx
{
    public static class BlockContainerUtils
    {
        public static Dataflow<TIn> FromBlock<TIn>(ITargetBlock<TIn> block)
        {
            return new TargetDataflow<TIn>(block);
        }

        public static Dataflow<TIn> FromBlock<TIn>(ITargetBlock<TIn> block, BlockContainerOptions options)
        {
            return new TargetDataflow<TIn>(block, options);
        }

        public static Dataflow<TIn, TOut> FromBlock<TIn, TOut>(IPropagatorBlock<TIn, TOut> block)
        {
            return new PropagatorDataflow<TIn, TOut>(block);
        }

        public static Dataflow<TIn, TOut> FromBlock<TIn, TOut>(IPropagatorBlock<TIn, TOut> block, BlockContainerOptions options)
        {
            return new PropagatorDataflow<TIn, TOut>(block, options);
        }

        public static Dataflow<TIn, TOut> AutoComplete<TIn, TOut>(this Dataflow<TIn, TOut> dataflow, TimeSpan timeout)
            where TIn : ITracableItem
            where TOut : ITracableItem 
        {
            return new AutoCompleteWrapper<TIn, TOut>(dataflow, timeout);
        }
    }
}
