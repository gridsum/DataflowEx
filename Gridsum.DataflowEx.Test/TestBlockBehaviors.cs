using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gridsum.DataflowEx.Test
{
    /// <summary>
    /// This class is used to test and confirm TPL Dataflow block behaviors
    /// </summary>
    [TestClass]
    public class TestBlockBehaviors
    {
        [TestMethod]
        public void TestBroadcast()
        {
            var bb = new BroadcastBlock<int>(null);

            bb.LinkTo(new ActionBlock<int>(i =>
            {
                Thread.Sleep(500);
                Console.WriteLine("Receiver1:" + i);
            }, new ExecutionDataflowBlockOptions() { BoundedCapacity = 5}));

            bb.LinkTo(new ActionBlock<int>(i =>
            {
                Thread.Sleep(500);
                Console.WriteLine("Receiver2:" + i);
            }, new ExecutionDataflowBlockOptions(){BoundedCapacity = 2}));

            bb.Post(1);
            bb.Post(2);
            bb.Post(3);
            bb.Post(4);
            bb.Post(5);

            //todo: ensures actionblock1 receives only part of the elements

            Thread.Sleep(3000);
        }

        [TestMethod]
        public async Task TestWriteOnce()
        {
            var bb = new WriteOnceBlock<int>(null);

            bb.LinkTo(new ActionBlock<int>(i =>
            {
                Thread.Sleep(1000);
                Console.WriteLine("Receiver1:" + i);
            }, new ExecutionDataflowBlockOptions() { BoundedCapacity = 1 }));

            bb.LinkTo(new ActionBlock<int>(i =>
            {
                Thread.Sleep(1000);
                Console.WriteLine("Receiver2:" + i);
            }));

            Assert.IsTrue(bb.Post(1));

            Assert.IsFalse(bb.Post(2));
            Assert.IsFalse(await bb.SendAsync(3));
            Assert.IsFalse(bb.Post(4));
            Assert.IsFalse(await bb.SendAsync(5));
        }

        [TestMethod]
        public void TestBufferBlock()
        {
            var buffer = new BufferBlock<int>(new DataflowBlockOptions() {BoundedCapacity = 2});

            Assert.IsTrue(buffer.Post(1));
            Assert.IsTrue(buffer.Post(1));
            Assert.IsFalse(buffer.Post(1));
        }
    }
}
