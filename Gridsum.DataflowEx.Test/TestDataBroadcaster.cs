using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Gridsum.DataflowEx.Exceptions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gridsum.DataflowEx.Test
{
    [TestClass]
    public class TestDataBroadcaster
    {
        [TestMethod]
        public async Task TestDataBroadcaster1()
        {
            var random = new Random();
            var dataCopier = new DataBroadcaster<int>();

            int sum1 = 0;
            int sum2 = 0;

            var action1 = new ActionBlock<int>(i => sum1 = sum1 + i);
            var action2 = new ActionBlock<int>(i => sum2 = sum2 + i);

            dataCopier.LinkTo(DataflowUtils.FromBlock(action1));
            dataCopier.LinkTo(DataflowUtils.FromBlock(action2));

            for (int j = 0; j < 1000; j++)
            {
                dataCopier.InputBlock.Post((int) (random.NextDouble()*10000));
            }

            dataCopier.Complete();

            await TaskEx.AwaitableWhenAll(action1.Completion, action2.Completion);

            Console.WriteLine("sum1 = {0} , sum2 = {1}", sum1, sum2);
            Assert.AreEqual(sum1, sum2);
        }

        [TestMethod]
        public async Task TestDataBroadcaster2()
        {
            var random = new Random();
            var buffer = new BufferBlock<int>();
            
            int sum1 = 0;
            int sum2 = 0;
            int sum3 = 0;

            var action1 = new ActionBlock<int>(i => sum1 = sum1 + i);
            var action2 = new ActionBlock<int>(i => sum2 = sum2 + i);
            var action3 = new ActionBlock<int>(i => sum3 = sum3 + i);

            buffer.ToDataflow().LinkToMultiple(new []{action1, action2, action3}.Select(a => a.ToDataflow()).ToArray());
            
            for (int j = 0; j < 1000; j++)
            {
                buffer.Post((int)(random.NextDouble() * 10000));
            }

            buffer.Complete();

            await TaskEx.AwaitableWhenAll(action1.Completion, action2.Completion, action3.Completion);

            Console.WriteLine("sum1 = {0} , sum2 = {1}", sum1, sum2);
            Assert.AreEqual(sum1, sum2);
            Assert.AreEqual(sum1, sum3);
        }
    }
}
