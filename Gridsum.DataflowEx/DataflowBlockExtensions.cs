using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Gridsum.DataflowEx.Exceptions;

namespace Gridsum.DataflowEx
{
    using System.Collections.Immutable;
    using Gridsum.DataflowEx.Blocks;

    public static class DataflowBlockExtensions
    {
        [Obsolete]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SafePost<TIn>(this ITargetBlock<TIn> target, TIn item, int interval = 200, int retryCount = 3)
        {
            bool posted = target.Post(item);
            if (posted) return;

            if (target.Completion.IsCompleted)
            {
                string msg = string.Format(
                    "Safe post to {0} failed as the block is {1}",
                    Utils.GetFriendlyName(target.GetType()),
                    target.Completion.Status);

                throw new PostToBlockFailedException(msg);
            }

            for (int i = 1; i <= retryCount; i++)
            {
                Thread.Sleep(interval * i);
                posted = target.Post(item);
                if (posted) return;
            }

            throw new PostToBlockFailedException(
                string.Format("Safe post to {0} failed after {1} retries", Utils.GetFriendlyName(target.GetType()), retryCount));
        }
        
        public static Tuple<int,int> GetBufferCount(this IDataflowBlock block)
        {
            dynamic b = block;

            var blockGenericType = block.GetType().GetGenericTypeDefinition();
            if (blockGenericType == typeof(TransformBlock<,>) || blockGenericType == typeof(TransformManyBlock<,>))
            {
                return new Tuple<int, int>(b.InputCount, b.OutputCount);
            }

            if (blockGenericType == typeof(ActionBlock<>))
            {
                return new Tuple<int, int>(b.InputCount, 0);
            }

            if (blockGenericType == typeof (BufferBlock<>))
            {
                return new Tuple<int, int>(0, b.Count);;
            }

            if (blockGenericType == typeof (BatchBlock<>))
            {
                return new Tuple<int, int>(0, b.OutputCount * b.BatchSize);                
            }

            if (block.GetType().Name.StartsWith("EncapsulatingPropagator"))
            {
//                we wish we could do this: (but it is not feasible due to TPL Dataflow design)
//                var t1 = (b.Source as IDataflowBlock).GetBufferCount();
//                var t2 = (b.Target as IDataflowBlock).GetBufferCount();
//                return new Tuple<int, int>(t1.Item1 + t2.Item1, t1.Item2 + t2.Item2);
                
                //return 0 for encapuslated block otherwise exception will be thrown
                //to monitor encapsulated blocks, user should register underlying blocks as children
                return new Tuple<int, int>(0, 0);
            }

            throw new ArgumentException("Fail to auto-detect buffer count of block: " + Utils.GetFriendlyName(block.GetType()), "block");
        }


        public static IDisposable LinkTo<TOutput, TTarget>(this ISourceBlock<TOutput> source, ITargetBlock<TTarget> target, DataflowLinkOptions linkOptions, Predicate<TOutput> predicate, Func<TOutput, TTarget> transform)
        {
            if (source == null)
                throw new ArgumentNullException("source");
            if (target == null)
                throw new ArgumentNullException("target");
            if (linkOptions == null)
                throw new ArgumentNullException("linkOptions");
            if (predicate == null)
                throw new ArgumentNullException("predicate");
            if (transform == null)
                throw new ArgumentNullException("transform");
            var TransformAndLinkPropagator = new TransformAndLinkPropagator<TOutput, TTarget>(source, target, predicate, transform);
            return source.LinkTo((ITargetBlock<TOutput>)TransformAndLinkPropagator, linkOptions);
        }
    }
}
