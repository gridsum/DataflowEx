namespace Gridsum.DataflowEx.Databases
{
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Threading.Tasks;

    /// <summary>
    /// The handler which allows you to take control after a bulk insertion succeeds. (e.g. you may want to 
    /// execute a stored prodecure after every bulk insertion)
    /// </summary>
    /// <param name="connection">The connection used by previous bulk insert (already opened)</param>
    /// <param name="target">The destination table of the bulk insertion</param>
    /// <param name="insertedData">The inserted data of this round of bulk insertion</param>
    /// <returns>A task represents the state of the post bulk insert job (so you can use await in the delegate)</returns>
    public delegate Task PostBulkInsertDelegate<T>(SqlConnection connection, TargetTable target, ICollection<T> insertedData);
}