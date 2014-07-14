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

        [TestMethod]
        public void TestExactMatch()
        {
            var cond1 = new StringMatchCondition("1", MatchType.Contains);
            var cond2 = new StringMatchCondition("2", MatchType.Contains);
            var cond3 = new StringMatchCondition("3", MatchType.Contains);

            var multi = new MultiMatchCondition<string>(new[] { cond1, cond2, cond3 });

            Assert.IsTrue(multi.Matches("432"));
            Assert.IsTrue(multi.Matches("123"));
            Assert.AreEqual(cond2, multi.MatchesExact("432"));
            Assert.AreEqual(cond1, multi.MatchesExact("123"));

        }

        [TestMethod]
        public void TestExactMatch2()
        {
            var cond1 = new StringMatchable("1", MatchType.Contains);
            var cond2 = new StringMatchable("2", MatchType.Contains);
            var cond3 = new StringMatchable("3", MatchType.EndsWith);

            var multi = new MultiMatcher<string, StringMatchable>(new[] { cond1, cond2, cond3 });

            Assert.IsTrue(multi.Matches("432"));
            Assert.IsTrue(multi.Matches("123"));
            Assert.AreEqual(cond2.Condition, multi.MatchesExact("432"));
            Assert.AreEqual(cond1.Condition, multi.MatchesExact("123"));

            StringMatchable m;
            IMatchCondition<string> c;
            Assert.IsTrue(multi.TryMatchExact("9873", out m, out c));
            Assert.AreEqual(cond3, m);
            Assert.AreEqual(cond3.Condition, c);
        }

        internal class StringMatchable : IMatchable<string>
        {
            private readonly string m_pattern;
            private UrlStringMatchCondition m_cond;

            public StringMatchable(string pattern, MatchType matchType)
            {
                this.m_pattern = pattern;
                m_cond = new UrlStringMatchCondition(pattern, matchType, false);
            }

            public string Pattern
            {
                get
                {
                    return m_pattern;
                }
            }

            public IMatchCondition<string> Condition
            {
                get
                {
                    return m_cond;
                }
            }
        }
    }
}
