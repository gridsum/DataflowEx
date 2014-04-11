using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx
{
    public static class DataflowBlockExtensions
    {
        public static int GetBufferCount(this IDataflowBlock block)
        {
            dynamic b = block;

            var blockGenericType = block.GetType().GetGenericTypeDefinition();
            if (blockGenericType == typeof(TransformBlock<,>) || blockGenericType == typeof(TransformManyBlock<,>))
            {
                return b.InputCount + b.OutputCount;
            }

            if (blockGenericType == typeof(ActionBlock<>))
            {
                return b.InputCount;
            }

//            if (typeof(ISourceBlock<>).IsInstanceOfType(block))
//            {
//                return b.OutputCount;
//            }

            throw new ArgumentException("Fail to auto-detect buffer count of block: " + Utils.GetFriendlyName(block.GetType()), "block");
        }
    }
}
