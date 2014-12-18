namespace Gridsum.DataflowEx.Databases
{
    using System;

    /// <summary>
    /// Represents a node in the middle of a column property path, tagged on a expandable property
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true, Inherited = true)]
    public class DBColumnPath : Attribute
    {
        protected readonly DBColumnPathOptions m_options;

        /// <summary>
        /// Constructs a DBColumnPath instance
        /// </summary>
        /// <param name="options">Options for a property path</param>
        public DBColumnPath(DBColumnPathOptions options)
        {
            this.m_options = options;
        }

        internal bool HasOption(DBColumnPathOptions option)
        {
            return m_options.HasFlag(option);
        }
    }

    /// <summary>
    /// Options for a property path
    /// </summary>
    [Flags]
    public enum DBColumnPathOptions
    {
        /// <summary>
        /// Tells the type accessor engine that a property tagged with this attribute will never
        /// be null so that the engine will produce faster code and deliver better performance in DbBulkInserter.
        /// </summary>
        /// <remarks>
        /// If a tagged property happens to be null. NullReferenceException will be thrown at runtime by DbBulkInserter.
        /// This is the cost of performance.
        /// </remarks>
        DoNotGenerateNullCheck = 1,

        /// <summary>
        /// Tells the type accessor engine not to expand and look into any property tagged with this attribute.
        /// </summary>
        DoNotExpand = 2,
    }
}