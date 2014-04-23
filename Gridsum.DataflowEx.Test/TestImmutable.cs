using System;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gridsum.DataflowEx.Test
{
    [TestClass]
    public class TestImmutable
    {
        [TestMethod]
        public void TestImmutableList()
        {
            var l = ImmutableList.Create<int>(1, 3, 2);

            Assert.AreEqual(1, l.First());
            Assert.AreEqual(2, l.Last());

            l = l.Add(1);

            Assert.AreEqual(1, l.Last());
        }
    }
}
