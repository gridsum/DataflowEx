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
        public class Int : TracableItemBase
        {
            public int Val { get; set; }
        }
    
        [TestMethod]
        public async Task TestAutoComplete1()
        {
            var block = new TransformManyBlock<Int, Int>(i =>
            {
                int j = i.Val + 1;
                Console.WriteLine("block2: i = {0}, j = {1}", i.Val, j);
                Thread.Sleep(500);
                if (j < 10) return new[] { new Int { Val = j } };
                else return Enumerable.Empty<Int>();
            });

            var dataflow1 = DataflowUtils.FromDelegate<Int, Int>(i => i);
            var dataflow2 = DataflowUtils.FromBlock(block).AutoComplete(TimeSpan.FromSeconds(1));
            
            dataflow1.GoTo(dataflow2).GoTo(dataflow1);
            
            dataflow1.InputBlock.Post(new Int() { Val = 1 });
            Assert.IsTrue(await dataflow2.CompletionTask.FinishesIn(TimeSpan.FromSeconds(10)));
        }

        [TestMethod]
        public async Task TestAutoComplete2()
        {
            var block2 = new TransformManyBlock<Int, Int>(i =>
            {
                return Enumerable.Empty<Int>();
            });

            var dataflow1 = DataflowUtils.FromDelegate<Int, Int>(i => new[] { i }); //preserve the guid
            var dataflow2 = DataflowUtils.FromBlock(block2).AutoComplete(TimeSpan.FromSeconds(1));

            dataflow1.GoTo(dataflow2).GoTo(dataflow1);
            
            dataflow1.InputBlock.Post(new Int() { Val = 1 });
            Assert.IsTrue(await dataflow2.CompletionTask.FinishesIn(TimeSpan.FromSeconds(2)));
            Assert.IsTrue(dataflow2.Name.EndsWith("AutoComplete"));
        }
    }
}
