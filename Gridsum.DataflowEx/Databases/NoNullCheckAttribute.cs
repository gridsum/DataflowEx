using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Databases
{
    /// <summary>
    /// Tells the type accessor engine that a property tagged with this attribute will never
    /// be null so that the engine will produce faster code and deliver better performance in DbBulkInserter.
    /// </summary>
    /// <remarks>
    /// If a tagged property happens to be null. NullReferenceException will be thrown at runtime by DbBulkInserter.
    /// This is the cost of performance.
    /// </remarks>
    [Obsolete("Please use [DBColumnPath(DBColumnPathOptions.DoNotGenerateNullCheck)] instead")]
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true, Inherited = true)]
    public class NoNullCheckAttribute : Attribute 
    {
        public NoNullCheckAttribute()
        {
        }
    }
}
