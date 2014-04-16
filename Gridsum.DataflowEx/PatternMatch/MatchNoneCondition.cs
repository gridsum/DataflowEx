namespace Gridsum.DataflowEx.PatternMatch
{
    public class MatchNoneCondition<T> : IMatchCondition<T>
    {
        static readonly MatchNoneCondition<T> s_matchNone = new MatchNoneCondition<T>();

        public bool Matches(T input)
        {
            return false;
        }

        public static MatchNoneCondition<T> Instance
        {
            get
            {
                return s_matchNone;
            }
        }
    }
}
