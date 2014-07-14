namespace Gridsum.DataflowEx.PatternMatch
{
    public class MatchNoneCondition<T> : IMatchCondition<T>
    {
        static readonly MatchNoneCondition<T> s_matchNone = new MatchNoneCondition<T>();

        public bool Matches(T input)
        {
            return false;
        }

        public IMatchCondition<T> MatchesExact(T input)
        {
            if (this.Matches(input)) return this;
            else return null;
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
