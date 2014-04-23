using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.PatternMatch
{
    public class UrlStringMatchCondition : StringMatchCondition
    {
        public UrlStringMatchCondition(string matchPattern, MatchType matchType, bool excludeParam) : base(matchPattern, matchType)
        {
            ExcludeParam = excludeParam;
        }

        public override bool Matches(string input)
        {
            if (this.ExcludeParam)
            {
                input = GetUrlWithoutParam(input);
            }

            return base.Matches(input);
        }

        public bool ExcludeParam { get; private set; }

        private static readonly char[] s_urlParamChars = new[] { '?', '#' };

        public static string GetUrlWithoutParam(string url)
        {
            if (url == null) return null;

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
