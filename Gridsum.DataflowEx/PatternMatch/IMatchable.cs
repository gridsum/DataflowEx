namespace Gridsum.DataflowEx.PatternMatch
{
    public interface IMatchable<in T>
    {
        IMatchCondition<T> Condition { get; }
    }
}
