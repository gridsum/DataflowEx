using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gridsum.DataflowEx.Test
{
    [TestClass]
    public class BlockExtensionTest
    {
        [TestMethod]
        public async Task TestBlockBufferCount()
        {
            var block1 = new TransformBlock<int, int>(i => 2 * i);
            var block2 = new TransformManyBlock<int, int>(i => new [] { i });
            var block3 = new ActionBlock<int>(i => { Thread.Sleep(1000); });

            block1.Post(0);
            block2.Post(0);
            block2.Post(0);
            block3.Post(0);
            block3.Post(0);
            block3.Post(0);

            await Task.Delay(200);

            Assert.AreEqual(1, block1.GetBufferCount());
            Assert.AreEqual(2, block2.GetBufferCount());
            Assert.AreEqual(2, block3.GetBufferCount());
        }
    }
}
