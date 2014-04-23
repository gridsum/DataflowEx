using System;
using Gridsum.DataflowEx.PatternMatch;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gridsum.DataflowEx.Test
{
    [TestClass]
    public class TestStringCondition
    {
        [TestMethod]
        public void TestStringCondition1()
        {
            var cond = new StringMatchCondition("abc");
            Assert.IsTrue(cond.Matches("abc"));
            Assert.IsFalse(cond.Matches("abc1"));
        }

        [TestMethod]
        public void TestStringCondition2()
        {
            var cond = new StringMatchCondition("abc", MatchType.BeginsWith);
            Assert.IsTrue(cond.Matches("abc"));
            Assert.IsTrue(cond.Matches("abc1"));
            Assert.IsFalse(cond.Matches("1abc"));
        }

        [TestMethod]
        public void TestStringCondition3()
        {
            var cond = new StringMatchCondition("abc", MatchType.Contains);
            Assert.IsTrue(cond.Matches("abc"));
            Assert.IsTrue(cond.Matches("abc1"));
            Assert.IsTrue(cond.Matches("1abc"));
            Assert.IsFalse(cond.Matches("abd"));
        }

        [TestMethod]
        public void TestStringCondition4()
        {
            var cond = new StringMatchCondition(".*ab[cd].*", MatchType.RegexMatch);
            Assert.IsTrue(cond.Matches("abc"));
            Assert.IsTrue(cond.Matches("abc1"));
            Assert.IsTrue(cond.Matches("1abc"));
            Assert.IsTrue(cond.Matches("abd"));
            Assert.IsFalse(cond.Matches("abef"));
        }

        [TestMethod]
        public void TestUrlStringMatchCondition()
        {
            var cond = new UrlStringMatchCondition(@"((http)|(https)://)?www.gridsum.com/.*\.txt$", MatchType.RegexMatch, true);
            Assert.IsTrue(cond.Matches("www.gridsum.com/1.txt?a=b"));
            Assert.IsTrue(cond.Matches("http://www.gridsum.com/2.txt#section2"));
            Assert.IsTrue(cond.Matches("https://www.gridsum.com/3.txt"));
            Assert.IsFalse(cond.Matches("https://www.gridsum.com/3.txta"));
        }

        [TestMethod]
        public void TestUrlStringMatchCondition2()
        {
            var cond = new UrlStringMatchCondition(@"((http)|(https)://)?www.gridsum.com/.*\.txt$", MatchType.RegexMatch, false);
            Assert.IsFalse(cond.Matches("www.gridsum.com/1.txt?a=b"));
            Assert.IsFalse(cond.Matches("http://www.gridsum.com/2.txt#section2"));
            Assert.IsTrue(cond.Matches("https://www.gridsum.com/3.txt"));
            Assert.IsFalse(cond.Matches("https://www.gridsum.com/3.txta"));
        }
    }
}
