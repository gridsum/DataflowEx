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
    using System.IO;

    public static class DataflowUtils
    {
        public static Dataflow<TIn> FromDelegate<TIn>(Action<TIn> action)
        {
            return FromBlock(new ActionBlock<TIn>(action));
        }

        public static Dataflow<TIn> FromBlock<TIn>(ITargetBlock<TIn> block)
        {
            return new TargetDataflow<TIn>(block);
        }

        public static Dataflow<TIn> ToDataflow<TIn>(this ITargetBlock<TIn> block, string name = null)
        {
            var flow = FromBlock(block);
            flow.Name = name;
            return flow;
        }

        public static Dataflow<TIn, TOut> ToDataflow<TIn, TOut>(this IPropagatorBlock<TIn, TOut> block, string name = null)
        {
            var flow = FromBlock(block, name);
            return flow;
        }

        public static Dataflow<TIn> FromDelegate<TIn>(Action<TIn> action, DataflowOptions options)
        {
            return FromBlock(new ActionBlock<TIn>(action), options);
        }

        public static Dataflow<TIn> FromBlock<TIn>(ITargetBlock<TIn> block, DataflowOptions options)
        {
            return new TargetDataflow<TIn>(block, options);
        }

        public static Dataflow<TIn> ToDataflow<TIn>(this ITargetBlock<TIn> block)
        {
            return FromBlock(block);
        }

        public static Dataflow<TIn, TOut> FromDelegate<TIn, TOut>(Func<TIn, TOut> transform)
        {
            return FromBlock(new TransformBlock<TIn, TOut>(transform));
        }

        public static Dataflow<TIn, TOut> FromDelegate<TIn, TOut>(Func<TIn, IEnumerable<TOut>> transformMany)
        {
            return FromBlock(new TransformManyBlock<TIn, TOut>(transformMany));
        }

        public static Dataflow<TIn, TOut> FromBlock<TIn, TOut>(IPropagatorBlock<TIn, TOut> block, string name = null)
        {
            var flow = new PropagatorDataflow<TIn, TOut>(block) { Name = name };
            return flow;
        }

        public static Dataflow<TIn, TOut> ToDataflow<TIn, TOut>(this IPropagatorBlock<TIn, TOut> block)
        {
            return FromBlock(block);
        }

        public static Dataflow<TIn, TOut> FromDelegate<TIn, TOut>(Func<TIn, TOut> func, DataflowOptions options)
        {
            return FromBlock(new TransformBlock<TIn, TOut>(func), options);
        }

        public static Dataflow<TIn, TOut> FromDelegate<TIn, TOut>(Func<TIn, IEnumerable<TOut>> transformMany, DataflowOptions options)
        {
            return FromBlock(new TransformManyBlock<TIn, TOut>(transformMany), options);
        }

        public static Dataflow<TIn, TOut> FromBlock<TIn, TOut>(IPropagatorBlock<TIn, TOut> block, DataflowOptions options)
        {
            return new PropagatorDataflow<TIn, TOut>(block, options);
        }

        public static Dataflow<TIn, TOut> ToDataflow<TIn, TOut>(this IPropagatorBlock<TIn, TOut> block, DataflowOptions options)
        {
            return FromBlock(block, options);
        }

        public static Dataflow<TIn, TOut> AutoComplete<TIn, TOut>(this Dataflow<TIn, TOut> dataflow, TimeSpan timeout)
            where TIn : ITracableItem
            where TOut : ITracableItem 
        {
            return new AutoCompleteWrapper<TIn, TOut>(dataflow, timeout);
        }
                
        public static void LinkToMultiple<TIn, TOut>(this Dataflow<TIn, TOut> dataflow, IDataflow<TOut> out1, IDataflow<TOut> out2, Func<TOut, TOut> copyFunc = null)
        {
            var brancher = new DataBrancher<TOut>(copyFunc, DataflowOptions.Default);
            dataflow.GoTo(brancher);
            brancher.GoTo(out1);
            brancher.GoTo(out2);
        }

        public static void LinkToMultiple<TIn, TOut>(this Dataflow<TIn, TOut> dataflow, Func<TOut, TOut> copyFunc, params IDataflow<TOut>[] outs)
        {
            var brancher = new DataBrancher<TOut>(copyFunc, DataflowOptions.Default);
            dataflow.GoTo(brancher);

            foreach (var output in outs)
            {
                brancher.GoTo(output);
            }
        }

        public static void LinkToMultiple<TIn, TOut>(this Dataflow<TIn, TOut> dataflow, params Dataflow<TOut>[] outs)
        {
            LinkToMultiple(dataflow, null, outs);
        }

        //todo: from delegate, from existing dataflows
        public static int Total(this Tuple<int, int> tuple)
        {
            return tuple.Item1 + tuple.Item2;
        }

        /// <summary>
        /// Read from the reader line by line to act as an IEnumerable of string.
        /// </summary>
        public static IEnumerable<string> ToEnumerable(this TextReader reader)
        {
            string s;

            while ((s = reader.ReadLine()) != null)
            {
                yield return s;
            }
        }
    }
}
