namespace Gridsum.DataflowEx.Databases
{
    using System;

    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true, Inherited = true)]
    public class DBColumnPath : Attribute
    {
        protected readonly DBColumnPathOptions m_options;

        public DBColumnPath(DBColumnPathOptions options)
        {
            this.m_options = options;
        }

        public bool HasOption(DBColumnPathOptions option)
        {
            return m_options.HasFlag(option);
        }
    }

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