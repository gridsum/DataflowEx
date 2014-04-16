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
namespace Gridsum.DataflowEx.PatternMatch
{
	/// <summary>
	/// 在作URL归类时，URL的匹配类型。
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