namespace Gridsum.DataflowEx.PatternMatch
{
    public interface IMatchCondition<in T>
    {
        bool Matches(T input);
    }
}
