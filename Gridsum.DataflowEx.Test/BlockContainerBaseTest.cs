using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gridsum.DataflowEx.Test
{
    [TestClass]
    public class BlockContainerBaseTest
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
        public async Task TestTermination()
        {
            var faultyContainer = new FaultyBlocks();
            var involvedContainer = new InnocentBlocks();
            faultyContainer.TransformAndLink(involvedContainer);
            faultyContainer.LinkLeftToNull();

            faultyContainer.InputBlock.Post("test");

            await EnsureTaskFail<SystemException>(faultyContainer.CompletionTask);
            await EnsureTaskFail<OtherBlockContainerFailedException>(involvedContainer.CompletionTask);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException), AllowDerivedTypes = false)]
        public void TestTaskRegistration()
        {
            var faultyContainer = new FaultyBlocks();
            var involvedContainer = new InnocentBlocks();
            faultyContainer.TransformAndLink(involvedContainer);
            faultyContainer.LinkLeftToNull();

            faultyContainer.Register(new BufferBlock<int>());
        }
    }

    class FaultyBlocks : BlockContainerBase<string, string>
    {
        private TransformBlock<string, string> m_inputBlock;
        private TransformBlock<string, string> m_block2;

        public FaultyBlocks()
            : base(BlockContainerOptions.Default)
        {
            m_inputBlock = new TransformBlock<string, string>(s =>
            {
                Thread.Sleep(1000);
                return s;
            });

            this.RegisterBlock(m_inputBlock, null);

            m_block2 = new TransformBlock<string, string>(s =>
            {
                throw new SystemException("I'm done.");

                return s;
            });

            this.RegisterBlock(m_block2, null);
            m_inputBlock.LinkTo(m_block2, m_defaultLinkOption);
        }

        public override System.Threading.Tasks.Dataflow.ITargetBlock<string> InputBlock
        {
            get { return m_inputBlock; }
        }

        public override ISourceBlock<string> OutputBlock
        {
            get { return m_block2; }
        }

        public void Register(IDataflowBlock block)
        {
            this.RegisterBlock(block, delegate { return 0; });
        }
    }

    class InnocentBlocks : BlockContainerBase<string>
    {
        private ActionBlock<string> m_inputBlock;

        public InnocentBlocks()
            : base(BlockContainerOptions.Default)
        {
            m_inputBlock = new ActionBlock<string>(s =>
            {
                Thread.Sleep(500);
                return;
            });

            this.RegisterBlock(m_inputBlock, null);
        }

        public override System.Threading.Tasks.Dataflow.ITargetBlock<string> InputBlock
        {
            get { return m_inputBlock; }
        }
    }
}