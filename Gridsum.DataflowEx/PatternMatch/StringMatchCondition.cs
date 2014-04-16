// ======================================================================
// 
//      Copyright (C) 北京国双科技有限公司        
//                    http://www.gridsum.com
// 
//      保密性声明：此文件属北京国双科技有限公司所有，仅限拥有由国双科技
//      授予了相应权限的人所查看和所修改。如果你没有被国双科技授予相应的
//      权限而得到此文件，请删除此文件。未得国双科技同意，不得查看、修改、
//      散播此文件。
// 
// 
// ======================================================================

using System;
using System.Text.RegularExpressions;

namespace Gridsum.DataflowEx.PatternMatch
{
	public class StringMatchCondition : IMatchCondition<string>
	{
	    public StringMatchCondition(string matchPattern, MatchType matchType = MatchType.ExactMatch, bool excludeParam = false)
		{
            if (matchPattern == null)
            {
                throw new ArgumentNullException("matchPattern");
            }

			this.MatchPattern = matchPattern;
			this.MatchType = matchType;
			this.ExcludeParam = excludeParam;

            if (matchType == MatchType.RegexMatch)
            {
                this.Regex = new Regex(matchPattern, RegexOptions.IgnoreCase | RegexOptions.Compiled);    
            }            
		}

		public string MatchPattern { get; private set; }
        public MatchType MatchType { get; private set; }
        public bool ExcludeParam { get; private set; }

		public Regex Regex { get; set; }

	    public bool Matches(string input)
	    {
	        if (input == null)
	        {
	            return false;
	        }

            if (this.ExcludeParam)
            {
                input = GetUrlWithoutParam(input);
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

        private static char[] s_urlParamChars = new[] { '?', '#' };

        public static string GetUrlWithoutParam(string url)
        {
            url = url.Trim();

            int index = url.IndexOfAny(s_urlParamChars);

            if (index >= 0)
            {
                url = url.Substring(0, index);
            }

            return url;
        }
	}
}