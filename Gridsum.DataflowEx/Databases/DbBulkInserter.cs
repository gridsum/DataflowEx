using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Databases
{
    using System;

    /// <summary>
    /// A bulk inserter that treats the whole lifecycle of the dataflow as a whole transaction
    /// (no matter how many inserted batches)
    /// </summary>
    /// <typeparam name="T">Type of the strongly-typed objects to insert</typeparam>
    public class DbBulkInserter<T> : DbBulkInserterBase<T> where T : class
    {
        protected SqlConnection m_longConnection;
        protected SqlTransaction m_transaction;

        public DbBulkInserter(string connectionString, string destTable, DataflowOptions options, string destLabel, int bulkSize = 8192, string dbBulkInserterName = null, PostBulkInsertDelegate<T> postBulkInsert = null)
            : this(new TargetTable(destLabel, connectionString, destTable), options, bulkSize, dbBulkInserterName, postBulkInsert)
        {
        }

        public DbBulkInserter(TargetTable targetTable, DataflowOptions options, int bulkSize = 8192, string dbBulkInserterName = null, PostBulkInsertDelegate<T> postBulkInsert = null)
            : base(targetTable, options, bulkSize, dbBulkInserterName, postBulkInsert)
        {
            m_longConnection = new SqlConnection(targetTable.ConnectionString);
            m_longConnection.Open();

            m_transaction = m_longConnection.BeginTransaction();
        }

        protected override void CleanUp(Exception dataflowException)
        {
            if (dataflowException != null)
            {
                m_logger.ErrorFormat("{0} Rolling back all changes...", this.FullName, dataflowException);
                m_transaction.Rollback();
                m_logger.InfoFormat("{0} Changes successfully rolled back", this.FullName);
            }
            else
            {
                m_logger.InfoFormat("{0} bulk insertions are done. Committing transaction...", this.FullName);
                m_transaction.Commit();
                m_logger.DebugFormat("{0} Transaction successfully committed.", this.FullName);
            }

            m_longConnection.Close();
        }

        protected override async Task DumpToDBAsync(T[] data, TargetTable targetTable)
        {
            m_logger.Debug(h => h("{3} starts bulk-inserting {0} {1} to db table {2}", data.Length, typeof(T).Name, targetTable.TableName, this.FullName));

            using (var bulkReader = new BulkDataReader<T>(m_typeAccessor, data))
            {
                try
                {
                    using (var bulkCopy = new SqlBulkCopy(m_longConnection, SqlBulkCopyOptions.TableLock, m_transaction))
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
                            "{0} NullReferenceException occurred in bulk insertion. This is probably caused by forgetting assigning value to a [NoNullCheck] attribute when constructing your object.",
                            this.FullName);
                    }

                    m_logger.ErrorFormat("{0} Bulk insertion error in the first place", e, this.FullName);

                    //As this is an unrecoverable exception, rethrow it
                    throw new AggregateException(e);
                }

                await this.OnPostBulkInsert(m_longConnection, targetTable, data).ConfigureAwait(false);
            }

            m_logger.Debug(h => h("{3} bulk-inserted {0} {1} to db table {2}", data.Length, typeof(T).Name, targetTable.TableName, this.FullName));
        }
    }
}
