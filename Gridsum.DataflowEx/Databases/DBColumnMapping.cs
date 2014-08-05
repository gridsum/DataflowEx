using System;

namespace Gridsum.DataflowEx.Databases
{
    /// <summary>
    /// Represents a mapping from C# property to a DB column
    /// </summary> 
    /// <remarks>
    /// 采用尽可能匹配的原则，即如果发现用户设置的参数与数据库的不匹配，一般情况下打印Warn级别的日志，不会直接报错退出。
    /// Warn:由于C#里的继承的属性的Attribute是不继承的，因此，想要添加DBColumnMapping，必须添加到子类中。示例：
    /// public abstract class Base{
    ///     public abstract int Data{get;set;}
    /// 
    ///     [DBColumnMapping()]
    ///     public float Price{get;set;}
    /// }
    /// 
    /// public class Drived: Base{
    ///     [DBColumnMapping()]
    ///     public override int Data{get;set;}
    /// }
    /// 如果将DBColumnMapping标签在Base类的Data中，则不能实现匹配。必须 标记在Drived的Data属性中
    /// </remarks>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true, Inherited = true)]
    public class DBColumnMapping : Attribute
    {
        /// <summary>
        /// 用途： 1、标记ValueType与String类型，表明将与数据库表的字段名为PropertyInfo.Name的匹配。
        /// </summary>
        /// <param name="destLabel">产品名称</param>
        public DBColumnMapping(string destLabel)
            : this(destLabel, -1, null, null)
        {
        }

        /// <summary>
        /// 用途： 用于将该属性匹配到数据库表的特定的列，采用字段的位置参数进行匹配
        /// </summary>
        /// <param name="destLabel">产品名称</param>
        /// <param name="destColumnOffset">将要匹配的数据库表的列位置</param>
        /// <param name="defaultValue">默认值，如果不填，将采用default(T)的形式得到该类型的默认值</param>
        /// <param name="destTableName">数据库表：默认为null，将匹配所有该产品的数据库表</param>
        public DBColumnMapping(string destLabel, int destColumnOffset, object defaultValue = null) 
            : this(destLabel, destColumnOffset, null, defaultValue) { }

        /// <summary>
        /// 用途：
        /// 用于将该属性匹配到数据库表的特定的列，采用字段名称进行匹配
        /// </summary>
        /// <param name="destLabel">产品名称 </param>
        /// <param name="destColumnName">将要匹配的数据库表的列名称</param>
        /// <param name="defaultValue">默认值，如果不填，将采用default(T)的形式得到该类型的默认值</param>
        /// <param name="destTableName">数据库表：默认为null，将匹配所有该产品的数据库表</param>
        public DBColumnMapping(string destLabel, string destColumnName, object defaultValue = null)
            : this(destLabel, -1, destColumnName, defaultValue)
        {
        }
        
        protected DBColumnMapping(string destLabel, int destColumnOffset, string destColumnName, object defaultValue)
        {
            DestLabel = destLabel;
            DestColumnOffset = destColumnOffset;
            DestColumnName = destColumnName;
            DefaultValue = defaultValue;
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
                "[ColName:{0}, ColOffset:{1}, DefaultValue:{2}]",
                this.DestColumnName,
                this.DestColumnOffset,
                this.DefaultValue);
        }
    }
}
