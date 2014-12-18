namespace Gridsum.DataflowEx.PatternMatch
{
    /// <summary>
    /// Represents a match condition
    /// </summary>
    /// <typeparam name="T">Type of the input the condition can accept</typeparam>
    public interface IMatchCondition<in T>
    {
        bool Matches(T input);

        /// <summary>
        /// Check if the condition matches the given input
        /// </summary>
        /// <returns>The exact matched condition for the input (e.g. a child condition for MultiMatchCondition) </returns>
        /// <remarks>For a normal condition, returns itself if input is matched</remarks>
        IMatchCondition<T> MatchesExact(T input);
    }
}
