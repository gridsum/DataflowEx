namespace Gridsum.DataflowEx.Test
{
    using System;
    using System.Threading.Tasks;

    using Gridsum.DataflowEx.Test.Demo;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class TestFlowBlocking
    {
        [TestMethod]
        public async Task TestBlocking1()
        {
            var slowFlow = new SlowFlow(new DataflowOptions
                                            {
                                                FlowMonitorEnabled = true,
                                                MonitorInterval = TimeSpan.FromSeconds(5),
                                                RecommendedCapacity = 2
                                            });

            Assert.IsTrue(slowFlow.Post("a"));
            Assert.IsTrue(slowFlow.Post("a"));
            Assert.IsFalse(slowFlow.Post("a"));
            Assert.IsFalse(slowFlow.Post("a"));
            Assert.IsFalse(slowFlow.Post("a"));
            Assert.IsFalse(slowFlow.Post("a"));

            await Task.Delay(2000);

            Assert.IsTrue(slowFlow.Post("a"));
        }

        [TestMethod]
        public async Task TestBlocking2()
        {
            var slowFlow = new SlowFlow(new DataflowOptions
                                            {
                                                FlowMonitorEnabled = true,
                                                MonitorInterval = TimeSpan.FromSeconds(5),
                                                RecommendedCapacity = 2
                                            });

            await slowFlow.SendAsync("a");
            await slowFlow.SendAsync("a");
            await slowFlow.SendAsync("a");
            await slowFlow.SendAsync("a");
            await slowFlow.SendAsync("a");
            await slowFlow.SendAsync("a");
            
            //Assert.IsTrue(slowFlow.Post("a"));
        }
    }
}