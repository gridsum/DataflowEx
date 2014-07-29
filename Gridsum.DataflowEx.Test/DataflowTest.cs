using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Gridsum.DataflowEx.Exceptions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gridsum.DataflowEx.Test
{
    using System.IO;

    [TestClass]
    public class DataflowTest
    {
        private async Task EnsureTaskFail<TE>(Task task) where TE : Exception
        {
            Exception caught = null;
            try
            {
                await task;
            }
            catch (Exception e)
            {
                caught = e;
            }

            if (caught == null)
            {
                Assert.Fail("should have caught {0} but there was no exception.", typeof(TE).Name);
            }
            else if (!(caught is TE))
            {
                Assert.Fail("should have caught {0} but {1} occurred", typeof(TE).Name, caught.GetType().Name);
            }
        }

        [TestMethod]
        public async Task TestCompletionPropagation()
        {
            var block1 = new TransformBlock<int, int>(i => 2*i);
            var block2 = new TransformBlock<int, int>(i => 2*i);
            var container1 = DataflowUtils.FromBlock(block1);
            var container2 = DataflowUtils.FromBlock(block2);

            bool postTaskDone = false;
            container2.RegisterPostDataflowTask(() => Task.Run(
                () =>
                    {
                        postTaskDone = true;
                    }));

            container1.LinkTo(container2);
            container2.LinkLeftToNull();

            container1.InputBlock.SafePost(1);
            container1.InputBlock.Complete();

            Assert.IsTrue(await container2.CompletionTask.FinishesIn(TimeSpan.FromSeconds(1)));
            Assert.IsTrue(postTaskDone);
        }

        [TestMethod]
        public async Task TestCompletionPropagation2()
        {
            var block1 = new TransformManyBlock<int, int>(i =>
            {
                int j = i + 1;
                Console.WriteLine("block1: i = {0}, j = {1}", i, j);
                if (j < 100) return new[] { j };
                else return Enumerable.Empty<int>();
            });
            var block2 = new TransformManyBlock<int, int>(i =>
            {
                int j = i + 1;
                Console.WriteLine("block2: i = {0}, j = {1}", i, j);
                if (j < 100) return new[] { j };
                else return Enumerable.Empty<int>();
            });

            var container1 = DataflowUtils.FromBlock(block1);
            var container2 = DataflowUtils.FromBlock(block2);

            container1.LinkTo(container2);
            container2.LinkTo(container1); //circular

            container1.InputBlock.Post(1);
            await Task.Delay(1000); //IMPORTANT: wait for block work done (nothing left in their input/output queue)
            container1.InputBlock.Complete();

            Assert.IsTrue(await container2.CompletionTask.FinishesIn(TimeSpan.FromSeconds(1)));
        }

        [TestMethod]
        public void TestContainerName()
        {
            var block1 = new TransformBlock<string, string>(i => i);
            var block2 = new TransformBlock<string, string>(i => i);
            var container1 = DataflowUtils.FromBlock(block1);
            var container2 = DataflowUtils.FromBlock(block2);
            var container3 = new FaultyBlocks();
            var container4 = new FaultyBlocks();

            Assert.AreEqual("[PropagatorDataflow<String, String>1]", container1.FullName);
            Assert.AreEqual("[PropagatorDataflow<String, String>2]", container2.FullName);
            Assert.IsTrue(container3.Name.StartsWith("FaultyBlocks"));
            Assert.IsTrue(container4.Name.StartsWith("FaultyBlocks"));

            container3.RegisterChild(container1);
            container4.RegisterChild(container2);

            Assert.IsTrue(container1.FullName.StartsWith("[FaultyBlocks"));
            Assert.IsTrue(container2.FullName.StartsWith("[FaultyBlocks"));
            
        }
        
        [TestMethod]
        public async Task TestTermination()
        {
            var faultyContainer = new FaultyBlocks();
            var involvedContainer = new InnocentBlocks();
            faultyContainer.TransformAndLink(involvedContainer);
            faultyContainer.LinkLeftToNull();

            var cts = new CancellationTokenSource();
            faultyContainer.RegisterCancellationTokenSource(cts);
            faultyContainer.InputBlock.Post("test");

            var sleepingTask = Task.Delay(TimeSpan.FromDays(1), cts.Token);

            await EnsureTaskFail<SystemException>(faultyContainer.CompletionTask);
            await EnsureTaskFail<LinkedDataflowFailedException>(involvedContainer.CompletionTask);

            Assert.IsTrue(await sleepingTask.FinishesIn(TimeSpan.FromSeconds(1)));
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestLinkLeftToNull()
        {
            var faultyContainer = new FaultyBlocks();
            var involvedContainer = new InnocentBlocks();
            faultyContainer.TransformAndLink(involvedContainer);
            faultyContainer.LinkLeftToNull();
            faultyContainer.TransformAndLink(involvedContainer);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestLinkLeftToNull2()
        {
            var faultyContainer = new FaultyBlocks();
            var involvedContainer = new InnocentBlocks();
            faultyContainer.LinkLeftToNull();
            faultyContainer.TransformAndLink(involvedContainer);
        }

        [TestMethod]
        public void TestLinkLeftToNull3()
        {
            var faultyContainer = new FaultyBlocks();
            faultyContainer.LinkLeftToNull();
        }

        [TestMethod]
        public async Task TestLinkLeftToNull4()
        {
            var flow = DataflowUtils.FromBlock(new BufferBlock<object>());
            flow.LinkSubTypeTo(DataflowUtils.FromBlock(DataflowBlock.NullTarget<string>()));
            flow.LinkLeftToNull();
            flow.InputBlock.SafePost("abc");
            flow.InputBlock.SafePost(new object());
            flow.InputBlock.Complete();
            await flow.CompletionTask;
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidDataException))]
        public async Task TestLinkLeftToError()
        {
            var flow = DataflowUtils.FromBlock(new BufferBlock<object>());
            flow.LinkSubTypeTo(DataflowUtils.FromBlock(DataflowBlock.NullTarget<string>()));
            flow.LinkLeftToError();
            flow.InputBlock.SafePost("abc");
            flow.InputBlock.SafePost(new object());
            flow.InputBlock.Complete();
            await flow.CompletionTask;
        }
        
        [TestMethod]
        public async Task TestDynamicRegistering()
        {
            var container = new DynamicContainer();
            var completion = container.CompletionTask;
            var dynamicBlock = container.RegisterBlockDynamically();
            container.InputBlock.Complete();
            Assert.IsFalse(await completion.FinishesIn(TimeSpan.FromMilliseconds(100)));
            
            dynamicBlock.Complete();
            Assert.IsTrue(await completion.FinishesIn(TimeSpan.FromMilliseconds(100)));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestCircularDependency()
        {
            var d1 = new Dataflow(DataflowOptions.Default);
            var d2 = new Dataflow(DataflowOptions.Default);
            var block = new ActionBlock<int>(i => { });
            d2.RegisterChild(block);
            
            d1.RegisterChild(d2);
            d2.RegisterChild(d1);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestCircularDependency2()
        {
            var d1 = new Dataflow(DataflowOptions.Default);
            var d2 = new Dataflow(DataflowOptions.Default);
            var d3 = new Dataflow(DataflowOptions.Default);
            var block = new ActionBlock<int>(i => { });
            d2.RegisterChild(block);

            d1.RegisterChild(d2);
            d2.RegisterChild(d3);
            d3.RegisterChild(d1);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestDuplicateRegistration()
        {
            var d1 = new Dataflow(DataflowOptions.Default);
            var d2 = new Dataflow(DataflowOptions.Default);

            d1.RegisterChild(d2);
            d1.RegisterChild(d2);
        }

        [TestMethod]
        public async Task TestDuplicateRegistration2()
        {
            var d1 = new Dataflow(DataflowOptions.Default);
            var d2 = new Dataflow(DataflowOptions.Default);

            var block = new ActionBlock<int>(i => { });

            d2.RegisterChild(block);
            d1.RegisterChild(d2);
            d1.RegisterChild(d2, allowDuplicate: true);

            Task.Run(
                async () =>
                    {
                        await Task.Delay(500);
                        block.Complete();
                    });

            await d1.CompletionTask;
            await d2.CompletionTask;
        }
    }

    class FaultyBlocks : Dataflow<string, string>
    {
        private TransformBlock<string, string> m_inputBlock;
        private TransformBlock<string, string> m_block2;

        public FaultyBlocks()
            : base(DataflowOptions.Default)
        {
            m_inputBlock = new TransformBlock<string, string>(s =>
            {
                Thread.Sleep(1000);
                return s;
            });
            
            m_block2 = new TransformBlock<string, string>(s =>
            {
                throw new SystemException("I'm done.");
                return s;
            });
            
            m_inputBlock.LinkTo(m_block2, m_defaultLinkOption);

            this.RegisterChild(m_inputBlock, null);
            this.RegisterChild(m_block2, null);
        }

        public override ITargetBlock<string> InputBlock
        {
            get { return m_inputBlock; }
        }

        public override ISourceBlock<string> OutputBlock
        {
            get { return m_block2; }
        }

        public void Register(IDataflowBlock block)
        {
            this.RegisterChild(block);
        }
    }

    class InnocentBlocks : Dataflow<string>
    {
        private ActionBlock<string> m_inputBlock;

        public InnocentBlocks()
            : base(DataflowOptions.Default)
        {
            m_inputBlock = new ActionBlock<string>(s =>
            {
                Thread.Sleep(500);
                return;
            });

            this.RegisterChild(m_inputBlock, null);
        }

        public override ITargetBlock<string> InputBlock
        {
            get { return m_inputBlock; }
        }
    }

    class DynamicContainer : Dataflow<int, int>
    {
        private IPropagatorBlock<int, int> m_block;

        public DynamicContainer() : base(DataflowOptions.Default)
        {
            m_block = RegisterBlockDynamically();
        }

        public IPropagatorBlock<int, int> RegisterBlockDynamically()
        {
            var block = CreateBlock();
            RegisterChild(block);
            return block;
        }

        public IPropagatorBlock<int, int> CreateBlock()
        {
            return new BufferBlock<int>();
        }
        
        public override ITargetBlock<int> InputBlock
        {
            get { return m_block; }
        }

        public override ISourceBlock<int> OutputBlock
        {
            get { return m_block; }
        }
    }
}