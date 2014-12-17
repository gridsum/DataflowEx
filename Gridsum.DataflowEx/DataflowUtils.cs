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

    /// <summary>
    /// Static class which contains a number of helper methods to construct Dataflow instance
    /// </summary>
    public static class DataflowUtils
    {
        /// <summary>
        /// Constructs a simple dataflow from an Action delegate 
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <param name="action">an Action delegate to wrap</param>
        /// <returns>A dataflow that wraps the action</returns>
        public static Dataflow<TIn> FromDelegate<TIn>(Action<TIn> action)
        {
            return FromBlock(new ActionBlock<TIn>(action));
        }

        /// <summary>
        /// Constructs a simple dataflow from an Action delegate 
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <param name="action">an Action delegate to wrap</param>
        /// <param name="options">The dataflow option for the new dataflow instance</param>
        /// <returns>A dataflow that wraps the action</returns>
        public static Dataflow<TIn> FromDelegate<TIn>(Action<TIn> action, DataflowOptions options)
        {
            return FromBlock(new ActionBlock<TIn>(action), options);
        }

        /// <summary>
        /// Constructs a simple dataflow from an transform delegate 
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <typeparam name="TOut">The output type of the Dataflow</typeparam>
        /// <param name="transform">an transform delegate to wrap</param>
        /// <returns>A dataflow that wraps the transform func</returns>
        public static Dataflow<TIn, TOut> FromDelegate<TIn, TOut>(Func<TIn, TOut> transform)
        {
            return FromBlock(new TransformBlock<TIn, TOut>(transform));
        }

        /// <summary>
        /// Constructs a simple dataflow from an transformMany delegate 
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <typeparam name="TOut">The output type of the Dataflow</typeparam>
        /// <param name="transform">an transformMany delegate to wrap</param>
        /// <returns>A dataflow that wraps the transformMany func</returns>
        /// <remarks>Uses a TransformManyBlock internally</remarks>
        public static Dataflow<TIn, TOut> FromDelegate<TIn, TOut>(Func<TIn, IEnumerable<TOut>> transformMany)
        {
            return FromBlock(new TransformManyBlock<TIn, TOut>(transformMany));
        }

        /// <summary>
        /// Constructs a simple dataflow from an transform delegate 
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <typeparam name="TOut">The output type of the Dataflow</typeparam>
        /// <param name="func">an transform delegate to wrap</param>
        /// <param name="options">The dataflow option for the new dataflow instance</param>
        /// <returns>A dataflow that wraps the transform func</returns>
        public static Dataflow<TIn, TOut> FromDelegate<TIn, TOut>(Func<TIn, TOut> func, DataflowOptions options)
        {
            return FromBlock(new TransformBlock<TIn, TOut>(func), options);
        }

        /// <summary>
        /// Constructs a simple dataflow from an transformMany delegate 
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <typeparam name="TOut">The output type of the Dataflow</typeparam>
        /// <param name="transformMany">an transformMany delegate to wrap</param>
        /// <param name="options">The dataflow option for the new dataflow instance</param>
        /// <returns>A dataflow that wraps the transformMany func</returns>
        /// <remarks>Uses a TransformManyBlock internally</remarks>
        public static Dataflow<TIn, TOut> FromDelegate<TIn, TOut>(Func<TIn, IEnumerable<TOut>> transformMany, DataflowOptions options)
        {
            return FromBlock(new TransformManyBlock<TIn, TOut>(transformMany), options);
        }

        /// <summary>
        /// Constructs a simple dataflow from an target block
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <param name="block">an target block to wrap</param>
        /// <returns>A dataflow that wraps the target block</returns>
        public static Dataflow<TIn> FromBlock<TIn>(ITargetBlock<TIn> block)
        {
            return new TargetDataflow<TIn>(block);
        }

        /// <summary>
        /// Constructs a simple dataflow from an target block
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <param name="block">an target block to wrap</param>
        /// <param name="options">The dataflow option for the new dataflow instance</param>
        /// <returns>A dataflow that wraps the target block</returns>
        public static Dataflow<TIn> FromBlock<TIn>(ITargetBlock<TIn> block, DataflowOptions options)
        {
            return new TargetDataflow<TIn>(block, options);
        }

        /// <summary>
        /// Constructs a simple dataflow from an propagator block
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <typeparam name="TOut">The output type of the Dataflow</typeparam>
        /// <param name="block">an propagator block to wrap</param>
        /// <param name="name">Name of the new dataflow instance</param>
        /// <returns>A dataflow that wraps the propagator block</returns>
        public static Dataflow<TIn, TOut> FromBlock<TIn, TOut>(this IPropagatorBlock<TIn, TOut> block, string name = null)
        {
            var flow = new PropagatorDataflow<TIn, TOut>(block) { Name = name };
            return flow;
        }

        /// <summary>
        /// Constructs a simple dataflow from an propagator block
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <typeparam name="TOut">The output type of the Dataflow</typeparam>
        /// <param name="block">an propagator block to wrap</param>
        /// <param name="options">The dataflow option for the new dataflow instance</param>
        /// <returns>A dataflow that wraps the propagator block</returns>
        public static Dataflow<TIn, TOut> FromBlock<TIn, TOut>(IPropagatorBlock<TIn, TOut> block, DataflowOptions options)
        {
            return new PropagatorDataflow<TIn, TOut>(block, options);
        }

        /// <summary>
        /// Constructs a simple dataflow from an target block
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <param name="block">an target block to wrap</param>
        /// <param name="options">The dataflow option for the new dataflow instance</param>
        /// <param name="name">Name of the returned dataflow</param>
        /// <returns>A dataflow that wraps the target block</returns>
        public static Dataflow<TIn> ToDataflow<TIn>(this ITargetBlock<TIn> block, DataflowOptions options = null, string name = null)
        {
            var flow = FromBlock(block, options ?? DataflowOptions.Default);
            flow.Name = name;
            return flow;
        }

        /// <summary>
        /// Constructs a simple dataflow from an propagator block
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <typeparam name="TOut">The output type of the Dataflow</typeparam>
        /// <param name="block">an propagator block to wrap</param>
        /// <param name="options">The dataflow option for the new dataflow instance</param>
        /// <param name="name">Name of the returned dataflow</param>
        /// <returns>A dataflow that wraps the propagator block</returns>
        public static Dataflow<TIn, TOut> ToDataflow<TIn, TOut>(this IPropagatorBlock<TIn, TOut> block, DataflowOptions options = null, string name = null)
        {
            var flow = FromBlock(block, options ?? DataflowOptions.Default);
            if (name != null)
            {
                flow.Name = name;
            }
            return flow;
        }
        
        /// <summary>
        /// Constructs a simple dataflow from an target block
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <param name="block">an target block to wrap</param>
        /// <returns>A dataflow that wraps the target block</returns>
        public static Dataflow<TIn> ToDataflow<TIn>(this ITargetBlock<TIn> block)
        {
            return FromBlock(block);
        }

        /// <summary>
        /// Constructs a simple dataflow from an propagator block
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <typeparam name="TOut">The output type of the Dataflow</typeparam>
        /// <param name="block">an propagator block to wrap</param>
        /// <returns>A dataflow that wraps the propagator block</returns>
        public static Dataflow<TIn, TOut> ToDataflow<TIn, TOut>(this IPropagatorBlock<TIn, TOut> block)
        {
            return FromBlock(block);
        }

        /// <summary>
        /// Constructs a simple dataflow from an propagator block
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <typeparam name="TOut">The output type of the Dataflow</typeparam>
        /// <param name="block">an propagator block to wrap</param>
        /// <param name="options">The dataflow option for the new dataflow instance</param>
        /// <returns>A dataflow that wraps the propagator block</returns>
        public static Dataflow<TIn, TOut> ToDataflow<TIn, TOut>(this IPropagatorBlock<TIn, TOut> block, DataflowOptions options)
        {
            return FromBlock(block, options);
        }

        /// <summary>
        /// Wraps an existing dataflow with auto complete feature
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <typeparam name="TOut">The output type of the Dataflow</typeparam>
        /// <param name="dataflow">The inner dataflow to wrap</param>
        /// <param name="timeout">The last-survival-timeout value for auto complete to trigger</param>
        /// <returns></returns>
        public static Dataflow<TIn, TOut> AutoComplete<TIn, TOut>(this Dataflow<TIn, TOut> dataflow, TimeSpan timeout)
            where TIn : ITracableItem
            where TOut : ITracableItem 
        {
            return new AutoCompleteWrapper<TIn, TOut>(dataflow, timeout, dataflow.DataflowOptions);
        }
                
        /// <summary>
        /// Links a dataflow to multiple targets. Uses DataBroadcaster internally.
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <typeparam name="TOut">The output type of the Dataflow</typeparam>
        /// <param name="dataflow">The source dataflow</param>
        /// <param name="out1">The first target dataflow</param>
        /// <param name="out2">The second target dataflow</param>
        /// <param name="copyFunc">The copy function</param>
        public static void LinkToMultiple<TIn, TOut>(this Dataflow<TIn, TOut> dataflow, IDataflow<TOut> out1, IDataflow<TOut> out2, Func<TOut, TOut> copyFunc = null)
        {
            var brancher = new DataBroadcaster<TOut>(copyFunc, DataflowOptions.Default);
            dataflow.GoTo(brancher);
            brancher.LinkTo(out1);
            brancher.LinkTo(out2);
        }

        /// <summary>
        /// Links a dataflow to multiple targets. Uses DataBroadcaster internally.
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <typeparam name="TOut">The output type of the Dataflow</typeparam>
        /// <param name="dataflow">The source dataflow</param>
        /// <param name="copyFunc">The copy function</param>
        /// <param name="outs">The target dataflows</param>
        public static void LinkToMultiple<TIn, TOut>(this Dataflow<TIn, TOut> dataflow, Func<TOut, TOut> copyFunc, params IDataflow<TOut>[] outs)
        {
            var brancher = new DataBroadcaster<TOut>(copyFunc, DataflowOptions.Default);
            dataflow.GoTo(brancher);

            foreach (var output in outs)
            {
                brancher.LinkTo(output);
            }
        }

        /// <summary>
        /// Links a dataflow to multiple targets. Uses DataBroadcaster internally.
        /// </summary>
        /// <typeparam name="TIn">The input type of the Dataflow</typeparam>
        /// <typeparam name="TOut">The output type of the Dataflow</typeparam>
        /// <param name="dataflow">The source dataflow</param>
        /// <param name="outs">The target dataflows</param>
        public static void LinkToMultiple<TIn, TOut>(this Dataflow<TIn, TOut> dataflow, params Dataflow<TOut>[] outs)
        {
            LinkToMultiple(dataflow, null, outs);
        }

        /// <summary>
        /// Calculate the sum of a int Tuple
        /// </summary>
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

        /// <summary>
        /// Asynchronously offer a message to a dataflow
        /// </summary>
        /// <typeparam name="TIn">The input type of the dataflow</typeparam>
        /// <param name="dataflow">The dataflow to accept message</param>
        /// <param name="item">The message to send</param>
        /// <returns>The completion status of the async operation</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static async Task SendAsync<TIn>(this Dataflow<TIn> dataflow, TIn item)
        {
            bool success = await dataflow.InputBlock.SendAsync(item).ConfigureAwait(false);

            if (!success)
            {
                ThrowSendFailedException(dataflow);
            }
        }

        private static void ThrowSendFailedException<TIn>(Dataflow<TIn> dataflow)
        {
            if (dataflow.InputBlock.Completion.IsCompleted)
            {
                string msg = string.Format(
                    "SendAsync to {0} failed as its input block is {1}",
                    dataflow.FullName,
                    dataflow.InputBlock.Completion.Status);

                throw new PostToBlockFailedException(msg);
            }
            else
            {
                var bufferStatus = dataflow.BufferStatus;
                string msg = string.Format(
                    "SendAsync to {0} failed. Its buffer state is (in:{1}, out:{2})",
                    dataflow.FullName,
                    bufferStatus.Item1,
                    bufferStatus.Item2);
                throw new PostToBlockFailedException(msg);
            }
        }

        /// <summary>
        /// Synchronously post an item to a dataflow
        /// </summary>
        /// <typeparam name="TIn">The input type of the dataflow</typeparam>
        /// <param name="dataflow">The dataflow to accept message</param>
        /// <param name="item">The message to post</param>
        /// <returns>Whether the post operation succeeds</returns>
        public static bool Post<TIn>(this Dataflow<TIn> dataflow, TIn item)
        {
            return dataflow.InputBlock.Post(item);
        }
    }
}
