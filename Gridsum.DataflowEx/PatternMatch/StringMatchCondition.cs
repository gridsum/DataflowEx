using System;
using System.Text.RegularExpressions;

namespace Gridsum.DataflowEx.PatternMatch
{
    /// <summary>
    /// A simple condition implementation for string. 
    /// </summary>
	public class StringMatchCondition : IMatchCondition<string>
	{
	    public StringMatchCondition(string matchPattern, MatchType matchType = MatchType.ExactMatch)
		{
            if (matchPattern == null)
            {
                throw new ArgumentNullException("matchPattern");
            }

			this.MatchPattern = matchPattern;
			this.MatchType = matchType;
			
            if (matchType == MatchType.RegexMatch)
            {
                this.Regex = new Regex(matchPattern, RegexOptions.IgnoreCase | RegexOptions.Compiled);    
            }            
		}

		public string MatchPattern { get; private set; }
        public MatchType MatchType { get; private set; }
        
		public Regex Regex { get; set; }

	    public virtual bool Matches(string input)
	    {
	        if (input == null)
	        {
	            return false;
	        }

            switch (MatchType)
            {
                case MatchType.ExactMatch:
                    // 精确匹配
                    return MatchPattern == input;
                case MatchType.BeginsWith:
                    // 处理左匹配
                    return input.StartsWith(MatchPattern, StringComparison.Ordinal);
                case MatchType.EndsWith:
                    // 处理右匹配
                    return input.EndsWith(MatchPattern, StringComparison.Ordinal);
                case MatchType.Contains:
                    // 处理包含情况
                    return input.Contains(MatchPattern);
                case MatchType.RegexMatch:
                    return Regex.IsMatch(input);
                default:
                    if (LogHelper.Logger.IsWarnEnabled)
                    {
                        LogHelper.Logger.WarnFormat("Invalid given enum value MatchType {0}. Using 'Contains' instead.", MatchType);
                    }
                    return input.Contains(MatchPattern);
            }
	    }

        public IMatchCondition<string> MatchesExact(string input)
        {
            if (this.Matches(input)) return this;
            else return null;
        }
	}
}