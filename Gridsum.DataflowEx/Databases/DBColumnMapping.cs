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
        /// Constructs a mapping from the tagged property to a db column name by property name
        /// </summary>
        /// <param name="destLabel">The label to help categorize all column mappings</param>
        public DBColumnMapping(string destLabel)
            : this(destLabel, -1, null, null, ColumnMappingOption.Mandatory)
        {
        }

        /// <summary>
        /// Constructs a mapping from the tagged property to a db column by column offset in the db table
        /// </summary>
        /// <param name="destLabel">The label to help categorize all column mappings</param>
        /// <param name="destColumnOffset">the column offset in the db table</param>
        /// <param name="defaultValue">default value for the property if the propety is null</param>
        /// <param name="option">The option, or priority of this mapping</param>
        public DBColumnMapping(string destLabel, int destColumnOffset, object defaultValue = null, ColumnMappingOption option = ColumnMappingOption.Mandatory) 
            : this(destLabel, destColumnOffset, null, defaultValue, option) { }

        /// <summary>
        /// Constructs a mapping from the tagged property to a db column by column name in the db table
        /// </summary>
        /// <param name="destLabel">The label to help categorize all column mappings</param>
        /// <param name="destColumnName">the column name in the db table</param>
        /// <param name="defaultValue">default value for the property if the propety is null</param>
        /// <param name="option">The option, or priority of this mapping</param>
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
        /// The label to help categorize all column mappings
        /// </summary>
        public string DestLabel { get; private set; }
        
        /// <summary>
        /// Column offset in the db table
        /// </summary>
        public int DestColumnOffset { get; set; }

        /// <summary>
        /// Column name in the db table
        /// </summary>
        public string DestColumnName { get; set; }

        /// <summary>
        /// Default value for the property if the propety is null
        /// </summary>
        public object DefaultValue { get; internal set; }

        /// <summary>
        /// The option, or priority of this mapping
        /// </summary>
        public ColumnMappingOption Option { get; set; }

        internal PropertyTreeNode Host { get; set; }

        internal bool IsDestColumnOffsetOk()
        {
            return DestColumnOffset >= 0;
        }

        internal bool IsDestColumnNameOk()
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
