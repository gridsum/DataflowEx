using System;
using System.Collections.Generic;
using System.Linq;

namespace Gridsum.DataflowEx.PatternMatch
{
    public class MultiMatchCondition<TInput> : IMatchCondition<TInput>
    {
        protected readonly IMatchCondition<TInput>[] m_matchConditions;

        public MultiMatchCondition(IMatchCondition<TInput>[] matchConditions)
        {
            m_matchConditions = matchConditions;
        }
        
        public bool Matches(TInput input)
        {
            return m_matchConditions.Any(matchCondition => matchCondition.Matches(input));
        }

        public IMatchCondition<TInput> MatchesExact(TInput input)
        {
            foreach (IMatchCondition<TInput> child in m_matchConditions)
            {
                IMatchCondition<TInput> childMatch = child.MatchesExact(input);

                if (childMatch != null)
                {
                    return childMatch;
                }
            }

            return null;
        }

    }

    //todo:  input-output caching
    //todo:? speed up exact string matching by using dictionary
    //todo:? speed up endswith/beginwith string matching by using tree
    public class MultiMatcher<TInput, TOutput> : MultiMatchCondition<TInput> where TOutput : IMatchable<TInput>
    {
        private readonly TOutput[] m_matchables;

        public MultiMatcher(TOutput[] matchables) : base(matchables.Select(o => o.Condition).ToArray())
        {
            m_matchables = matchables;
        }

        public bool TryMatch(TInput input, out TOutput matchable)
        {
            for (int i = 0; i < m_matchables.Length; i++)
            {
                if (m_matchables[i].Condition.Matches(input))
                {
                    matchable = m_matchables[i];
                    return true;
                }
            }

            matchable = default (TOutput);
            return false;
        }


        public bool TryMatchExact(TInput input, out TOutput matchable, out IMatchCondition<TInput> exactMatch)
        {
            for (int i = 0; i < m_matchables.Length; i++)
            {
                IMatchCondition<TInput> cond = m_matchables[i].Condition.MatchesExact(input);

                if (cond != null)
                {
                    matchable = m_matchables[i];
                    exactMatch = cond;
                    return true;
                }
            }

            matchable = default(TOutput);
            exactMatch = null;
            return false;
        }

        public TOutput Match(TInput input, Func<TOutput> defaultValueFactory)
        {
            TOutput output;
            return TryMatch(input, out output) ? output : defaultValueFactory();
        }

        public TOutput Match(TInput input, TOutput defaultValue)
        {
            TOutput output;
            return TryMatch(input, out output) ? output : defaultValue;
        }

        public IEnumerable<TOutput> MatchMultiple(TInput input)
        {
            for (int i = 0; i < m_matchables.Length; i++)
            {
                if (m_matchables[i].Condition.Matches(input))
                {
                    yield return m_matchables[i];
                }
            }
        }

        public TOutput this[int index]
        {
            get
            {
                return m_matchables[index];
            }
        }
    }

    public class FastMultiMatcher<TInput, TOutput> : MultiMatchCondition<TInput> where TOutput : IMatchable<TInput>
    {
        private readonly TOutput[] m_matchables;

        public FastMultiMatcher(TOutput[] matchables) : base(matchables.Select(o => o.Condition).ToArray())
        {
            m_matchables = matchables;
        }


    }
}
