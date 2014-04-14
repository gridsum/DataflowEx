using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Gridsum.DataflowEx.AutoCompletion;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gridsum.DataflowEx.Test
{
    [TestClass]
    public class TestAutoComplete
    {
        public class Int : ITracableItem
        {
            public Guid UniqueId { get; set; }
            public int Val { get; set; }
        }

        [TestMethod]
        public async Task TestAutoComplete1()
        {
            var block1 = new TransformManyBlock<Int, Int>(i =>
            {
                return new [] {i}; //preserve the guid
            });
            var block2 = new TransformManyBlock<Int, Int>(i =>
            {
                int j = i.Val + 1;
                Console.WriteLine("block2: i = {0}, j = {1}", i.Val, j);
                Thread.Sleep(500);
                if (j < 10) return new[] { new Int { Val = j}};
                else return Enumerable.Empty<Int>();
            });

            var container1 = BlockContainerUtils.FromBlock(block1);
            var container2 = BlockContainerUtils.FromBlock(block2);

            var pair = new AutoCompleteContainerPair<Int, Int>(TimeSpan.FromSeconds(1));

            container1.Link(pair.Before);
            pair.Before.Link(container2);
            container2.Link(pair.After);
            pair.After.Link(container1);

            container1.InputBlock.Post(new Int() {Val = 1});
            Assert.IsTrue(await container2.CompletionTask.FinishesIn(TimeSpan.FromSeconds(10)));
        }
    }
}
