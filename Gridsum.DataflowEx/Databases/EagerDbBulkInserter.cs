using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Databases
{
    using System.Data.SqlClient;

    /// <summary>
    /// A bulk inserter which treats every batch as a standalone transaction.
    /// </summary>
    /// <typeparam name="T">Type of the strongly-typed objects to insert</typeparam>
    public class EagerDbBulkInserter<T> : DbBulkInserterBase<T> where T: class 
    {
        public EagerDbBulkInserter(string connectionString, string destTable, DataflowOptions options, string destLabel, int bulkSize = 8192, string dbBulkInserterName = null, PostBulkInsertDelegate<T> postBulkInsert = null)
            : base(connectionString, destTable, options, destLabel, bulkSize, dbBulkInserterName, postBulkInsert)
        {
        }

        public EagerDbBulkInserter(TargetTable targetTable, DataflowOptions options, int bulkSize = 8192, string dbBulkInserterName = null, PostBulkInsertDelegate<T> postBulkInsert = null)
            : base(targetTable, options, bulkSize, dbBulkInserterName, postBulkInsert)
        {
        }

        protected async override Task DumpToDBAsync(T[] data, TargetTable targetTable)
        {
            m_logger.Debug(h => h("{3} starts bulk-inserting {0} {1} to db table {2}", data.Length, typeof(T).Name, targetTable.TableName, this.FullName));

            using (var bulkReader = new BulkDataReader<T>(m_typeAccessor, data))
            {
                using (var conn = new SqlConnection(targetTable.ConnectionString))
                {
                    await conn.OpenAsync().ConfigureAwait(false);

                    var transaction = conn.BeginTransaction();
                    try
                    {
                        using (var bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, transaction))
                        {
                            foreach (SqlBulkCopyColumnMapping map in bulkReader.ColumnMappings)
                            {
                                bulkCopy.ColumnMappings.Add(map);
                            }

                            bulkCopy.DestinationTableName = targetTable.TableName;
                            bulkCopy.BulkCopyTimeout = (int)TimeSpan.FromMinutes(30).TotalMilliseconds;
                            bulkCopy.BatchSize = m_bulkSize;

                            // Write from the source to the destination.
                            await bulkCopy.WriteToServerAsync(bulkReader).ConfigureAwait(false);
                        }
                    }
                    catch (Exception e)
                    {
                        if (e is NullReferenceException)
                        {
                            m_logger.ErrorFormat(
                                "{0} NullReferenceException occurred in bulk insertion. This is probably caused by forgetting assigning value to a [NoNullCheck] attribute when constructing your object.", this.FullName);
                        }

                        m_logger.ErrorFormat("{0} Bulk insertion failed. Rolling back all changes...", this.FullName, e);
                        transaction.Rollback();
                        m_logger.InfoFormat("{0} Changes successfully rolled back", this.FullName);

                        //As this is an unrecoverable exception, rethrow it
                        throw new AggregateException(e);
                    }

                    transaction.Commit();
                    await this.OnPostBulkInsert(conn, targetTable, data).ConfigureAwait(false);
                }
            }

            m_logger.Info(h => h("{3} bulk-inserted {0} {1} to db table {2}", data.Length, typeof(T).Name, targetTable.TableName, this.FullName));
        }
    }
}
