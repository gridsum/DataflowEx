using System;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gridsum.DataflowEx.Test
{
    using System.Threading.Tasks;

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

        [TestMethod]
        public void TestListAdd()
        {
            const int c = 1000000;
            var l = ImmutableList<int>.Empty;

            Parallel.For(0, c, i => { l = l.Add(i); });

            Assert.IsTrue(l.Count < c);
            Console.WriteLine(l.Count);
        }

        [TestMethod]
        public void TestListAdd2()
        {
            const int c = 1000000;
            var l = ImmutableList<int>.Empty;

            Parallel.For(0, c, i => ImmutableUtils.AddOptimistically(ref l, i));

            Assert.IsTrue(l.Count == c);
            Console.WriteLine(l.Count);
        }

        [TestMethod]
        public void TestSetAdd()
        {
            var iset = ImmutableHashSet.Create<int>(1,2,3);

            Assert.IsFalse(ImmutableUtils.TryAddOptimistically(ref iset, 3));
            Assert.IsTrue(ImmutableUtils.TryAddOptimistically(ref iset, 4));
        }

        [TestMethod]
        public void TestSetAdd2()
        {
            const int c = 1000000;
            var l = ImmutableHashSet<int>.Empty;

            Parallel.For(0, c, i => Assert.IsTrue(ImmutableUtils.TryAddOptimistically(ref l, i)));
            Parallel.For(0, c, i => Assert.IsFalse(ImmutableUtils.TryAddOptimistically(ref l, i)));

            Assert.IsTrue(l.Count == c);
            Console.WriteLine(l.Count);
        }
    }
}
