namespace Gridsum.DataflowEx.PatternMatch
{
	/// <summary>
	/// MatchType enumeration for match strategies
	/// </summary>
	public enum MatchType : byte
	{
		/// <summary>
		/// 精确匹配。
		/// </summary>
		ExactMatch = 0, 

		/// <summary>
		/// 以指定字符串开始。
		/// </summary>
		BeginsWith = 1, 

		/// <summary>
		/// 以指定字符串结束。
		/// </summary>
		EndsWith = 2, 

		/// <summary>
		/// 包含特定字符串。
		/// </summary>
		Contains = 3, 

		/// <summary>
		/// 正则匹配
		/// </summary>
		RegexMatch = 4, 
	}
}