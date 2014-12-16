using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Gridsum.DataflowEx;
using Gridsum.DataflowEx.Exceptions;
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

        [TestMethod]
        public async Task TestCancellation()
        {
            var cts = new CancellationTokenSource();
            var actionBlock = new ActionBlock<int>(i => Console.WriteLine(i), new ExecutionDataflowBlockOptions {CancellationToken = cts.Token});

            actionBlock.Post(1);
            actionBlock.Post(2);
            actionBlock.Post(3);

            await Task.Delay(200);

            cts.Cancel();

            var c = actionBlock.Completion;
            await c.ContinueWith(t =>
            {
                Assert.IsTrue(t.IsCanceled);
                Assert.IsTrue(t.Exception == null);
            });
        }

        [TestMethod]
        public async Task TestMultipleLinks()
        {
            var b1 = new BufferBlock<int>();
            var b2 = new BufferBlock<int>();
            var a1 = new ActionBlock<int>((i) => Console.WriteLine(i));

            b1.LinkTo(a1, new DataflowLinkOptions() { PropagateCompletion = true });
            b2.LinkTo(a1, new DataflowLinkOptions() { PropagateCompletion = true });

            b1.Post(1);
            b2.Post(2);
            b1.Complete();

            await a1.Completion;
        }

        [TestMethod]
        public async Task TestBatchBlockBuffer()
        {
            var b = new BatchBlock<int>(1000);
            b.Post(1);
            b.Post(2);
            b.TriggerBatch();
            Assert.AreEqual(b.BatchSize, b.GetBufferCount().Total());
        }

        [TestMethod]
        public void TestEncapsulateBlockBuffer()
        {
            var b1 = new ActionBlock<int>(i => Console.WriteLine(i));
            var b2 = new BufferBlock<int>();

            IPropagatorBlock<int, int> b3 = DataflowBlock.Encapsulate(b1, b2);

            
            Assert.AreEqual(0, b3.GetBufferCount().Total());
        }

        [TestMethod]
        public async Task TestBoundedCapacity()
        {
            //var b = new BatchBlock<int>(2, new GroupingDataflowBlockOptions() { BoundedCapacity = 10 });
            var b = new BufferBlock<int>(new DataflowBlockOptions() { BoundedCapacity = 10 });

            for (int i = 0; i < 10; i++)
            {
                b.Post(i);
            }

            Assert.AreEqual(false, b.Post(999));

            var status = (b as ITargetBlock<int>).OfferMessage(new DataflowMessageHeader(1L), 999, null, false);
            Console.WriteLine(status);
            //await b.SafePost(999);
            
        }

        public async Task TestLinkTo()
        {
            var b1 = new BufferBlock<int>().ToDataflow();
            var b2 = new BufferBlock<int>().ToDataflow();

            b1.OutputBlock.LinkTo(b2.InputBlock);

            //b1.TransformAndLink(b2, _ => _, _ => _ % 2 == 0);

            //b1.Post(1);
            //b1.Post(2);

            await Task.Delay(3000);
        }

        public async Task TestLinkTo2()
        {
            var b1 = new BufferBlock<int>();
            var b2 = new BufferBlock<int>();

            //b1.TransformAndLink(b2, _ => _, _ => _ % 2 == 0);

            b1.Post(1);
            b1.Post(2);

            await Task.Delay(3000);
        }

        public async Task TestLinkToBlocking()
        {
            BufferBlock<int> last = null;
            BufferBlock<int> first =  null;
            for (int i = 0; i < 500; i++)
            {
                var nb = new BufferBlock<int>(new DataflowBlockOptions { BoundedCapacity = 200 });

                if (last != null)
                {
                    last.LinkTo(nb);
                }
                else
                {
                    first = nb;
                }

                last = nb;
            }

            last.LinkTo(new ActionBlock<int>(_ => { Console.WriteLine(_); Thread.Sleep(500); },
                new ExecutionDataflowBlockOptions { BoundedCapacity = 100 }));

            for (int j = 0; j < 100000000; j++)
            {
                await first.SendAsync(j);
                Console.WriteLine("Sent " + j);
            }

            first.Complete();
            await first.Completion;
        }

        public async Task TestLinkToBlocking2()
        {
            ActionBlock<int> target = new ActionBlock<int>(_ => { 
                Console.WriteLine(_); 
                Thread.Sleep(100); 
            },
                new ExecutionDataflowBlockOptions { BoundedCapacity = 100 });

            List<BufferBlock<int>> l = new List<BufferBlock<int>>();

            for (int i = 0; i < 50; i++)
            {
                var nb = new BufferBlock<int>(new DataflowBlockOptions { BoundedCapacity = 2 });
                nb.LinkTo(target);
                l.Add(nb);
            }
            
            for (int j = 0; j < 100000000; j++)
            {
                foreach (var nb in l)
                {
                    await nb.SendAsync(j);
                }
                
                Console.WriteLine("Sent " + j);
            }

        }

        public async Task TestLinkToBlocking3()
        {
            ActionBlock<int> target = new ActionBlock<int>(_ =>
            {
                Console.WriteLine(_);
                Thread.Sleep(100);
            },
                new ExecutionDataflowBlockOptions { BoundedCapacity = 10 });

            var nb = new BufferBlock<int>(new DataflowBlockOptions { BoundedCapacity = 50 });

            nb.LinkTo(target, new DataflowLinkOptions(), _ => _ < 100, _ => _);

            for (int j = 0; j < 100000; j++)
            {
                await nb.SendAsync(j);
                Console.WriteLine("Sent " + j);
            }
        }
    }
}
