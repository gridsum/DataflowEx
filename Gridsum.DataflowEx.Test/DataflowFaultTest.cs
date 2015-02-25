using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gridsum.DataflowEx.Test
{
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    using Gridsum.DataflowEx.Exceptions;

    [TestClass]
    public class DataflowFaultTest
    {
        public class MyDataflow<T> : Dataflow<T>
        {
            public Dataflow<T, T> m_block1;
            public Dataflow<T> m_block2;
            public int cleanUpSignal = 0;

            public MyDataflow(): base(DataflowOptions.Default)
            {
                m_block1 = new BufferBlock<T>().ToDataflow(name: "innerflow1");
                m_block2 = new BatchBlock<T>(100).ToDataflow(name: "innerflow2");
                m_block1.LinkTo(m_block2);
                RegisterChild(this.m_block1);
                RegisterChild(this.m_block2);
            }

            public override ITargetBlock<T> InputBlock
            {
                get
                {
                    return this.m_block1.InputBlock;
                }
            }

            protected override void CleanUp(Exception dataflowException)
            {
                if (dataflowException == null)
                {
                    cleanUpSignal = 1;
                }
                else
                {
                    cleanUpSignal = -1;
                }
            }
        }

        [TestMethod]
        public async Task TestCleanUp()
        {
            var f = new MyDataflow<int>();
            await f.SignalAndWaitForCompletionAsync();
            Assert.AreEqual(1, f.cleanUpSignal);
        }

        [TestMethod]
        public async Task TestCleanUpOnFault()
        {
            var f = new MyDataflow<int>() {Name = "dataflow1"};
            f.m_block2.RegisterChild(new BufferBlock<int>());

            Task.Run(
                async () =>
                    {
                        await Task.Delay(200);
                        (f.m_block2.InputBlock).Fault(new InvalidOperationException());
                    });

            var f2 = new Dataflow(DataflowOptions.Default) {Name = "dataflow2"};
            f2.RegisterChild(f.m_block1);
            f2.RegisterChild(f.m_block2);

            var f3 = new Dataflow(DataflowOptions.Default) { Name = "dataflow3" };
            f3.RegisterChild(f);
            f3.RegisterChild(f2);

            await this.AssertFlowError<InvalidOperationException>(f);

            Assert.AreEqual(-1, f.cleanUpSignal);

            await this.AssertFlowError<InvalidOperationException>(f2);

            await this.AssertFlowError<InvalidOperationException>(f3);
        }

        public async Task AssertFlowError<T>(Dataflow f) where T : Exception
        {
            try
            {
                await f.CompletionTask;
            }
            catch (Exception e)
            {
                Assert.IsTrue(TaskEx.UnwrapWithPriority(e as AggregateException) is T);
            }
        }
    }
}
