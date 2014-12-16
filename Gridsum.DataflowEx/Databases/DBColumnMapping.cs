using System;

namespace Gridsum.DataflowEx.Databases
{
    /// <summary>
    /// Represents a mapping from C# property to a DB column
    /// </summary> 
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true, Inherited = true)]
    public class DBColumnMapping : Attribute
    {
        /// <summary>
        /// 用途： 1、标记ValueType与String类型，表明将与数据库表的字段名为PropertyInfo.Name的匹配。
        /// </summary>
        /// <param name="destLabel">产品名称</param>
        public DBColumnMapping(string destLabel)
            : this(destLabel, -1, null, null, ColumnMappingOption.Mandatory)
        {
        }

        /// <summary>
        /// 用途： 用于将该属性匹配到数据库表的特定的列，采用字段的位置参数进行匹配
        /// </summary>
        /// <param name="destLabel">产品名称</param>
        /// <param name="destColumnOffset">将要匹配的数据库表的列位置</param>
        /// <param name="defaultValue">默认值，如果不填，将采用default(T)的形式得到该类型的默认值</param>
        /// <param name="destTableName">数据库表：默认为null，将匹配所有该产品的数据库表</param>
        public DBColumnMapping(string destLabel, int destColumnOffset, object defaultValue = null, ColumnMappingOption option = ColumnMappingOption.Mandatory) 
            : this(destLabel, destColumnOffset, null, defaultValue, option) { }

        /// <summary>
        /// 用途：
        /// 用于将该属性匹配到数据库表的特定的列，采用字段名称进行匹配
        /// </summary>
        /// <param name="destLabel">产品名称 </param>
        /// <param name="destColumnName">将要匹配的数据库表的列名称</param>
        /// <param name="defaultValue">默认值，如果不填，将采用default(T)的形式得到该类型的默认值</param>
        /// <param name="destTableName">数据库表：默认为null，将匹配所有该产品的数据库表</param>
        public DBColumnMapping(string destLabel, string destColumnName, object defaultValue = null, ColumnMappingOption option = ColumnMappingOption.Mandatory)
            : this(destLabel, -1, destColumnName, defaultValue, option)
        {
        }
        
        protected DBColumnMapping(string destLabel, int destColumnOffset, string destColumnName, object defaultValue, ColumnMappingOption option)
        {
            DestLabel = destLabel;
            DestColumnOffset = destColumnOffset;
            DestColumnName = destColumnName;
            DefaultValue = defaultValue;
            this.Option = option;
        }

        /// <summary>
        /// 产品类型
        /// </summary>
        public string DestLabel { get; private set; }
        
        /// <summary>
        /// 对应数据库表的第几列
        /// </summary>
        public int DestColumnOffset { get; set; }
        /// <summary>
        /// 对应数据库表的列名称
        /// </summary>
        public string DestColumnName { get; set; }

        /// <summary>
        /// 默认的值
        /// </summary>
        public object DefaultValue { get; internal set; }

        public ColumnMappingOption Option { get; set; }

        public PropertyTreeNode Host { get; set; }

        public bool IsDestColumnOffsetOk()
        {
            return DestColumnOffset >= 0;
        }

        public bool IsDestColumnNameOk()
        {
            return string.IsNullOrWhiteSpace(DestColumnName) == false;
        }
        
        public override string ToString()
        {
            return string.Format(
                "[ColName:{0}, ColOffset:{1}, DefaultValue:{2}, Option: {3}]",
                this.DestColumnName,
                this.DestColumnOffset,
                this.DefaultValue,
                this.Option);
        }
    }

    /// <summary>
    /// The option of a db column mapping
    /// </summary>
    public enum ColumnMappingOption
    {
        /// <summary>
        /// Used typically by external mappings to overwrite Mandatory option on tagged attribute. The column must exist in the destination table.
        /// </summary>
        Overwrite = short.MinValue,

        /// <summary>
        /// The column must exist in the destination table
        /// </summary>
        Mandatory = 1,

        /// <summary>
        /// The column mapping can be ignored if the column is not found in the destination table
        /// This option is useful when you have one dest label for multiple different tables
        /// </summary>
        Optional = 2
    }
}
