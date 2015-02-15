namespace Gridsum.DataflowEx.Databases
{
    using System;
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    using Common.Logging;

    /// <summary>
    /// The class helps you to bulk insert strongly typed objects to the sql server database. 
    /// It generates object-relational mappings automatically according to the given DBColumnMappings.
    /// </summary>
    /// <typeparam name="T">The db-mapped type of objects</typeparam>
    public abstract class DbBulkInserterBase<T> : Dataflow<T>, IBatchedDataflow where T : class
    {
        protected readonly TargetTable m_targetTable;
        protected readonly TypeAccessor<T> m_typeAccessor;
        protected readonly int m_bulkSize;
        protected readonly string m_dbBulkInserterName;
        protected readonly PostBulkInsertDelegate<T> m_postBulkInsert;
        protected readonly BatchBlock<T> m_batchBlock;
        protected readonly ActionBlock<T[]> m_actionBlock;
        protected readonly ILog m_logger;
        protected Timer m_timer;

        /// <summary>
        /// Constructs an instance of DbBulkInserter
        /// </summary>
        /// <param name="connectionString">The connection string to the output database</param>
        /// <param name="destTable">The table name in database to bulk insert into</param>
        /// <param name="options">Options to use for this dataflow</param>
        /// <param name="destLabel">The mapping label to help choose among all column mappings</param>
        /// <param name="bulkSize">The bulk size to insert in a batch. Default to 8192.</param>
        /// <param name="dbBulkInserterName">A given name of this bulk inserter (would be nice for logging)</param>
        /// <param name="postBulkInsert">A delegate that enables you to inject some customized work whenever a bulk insert is done</param>
        public DbBulkInserterBase(
            string connectionString,
            string destTable,
            DataflowOptions options,
            string destLabel,
            int bulkSize = 4096 * 2,
            string dbBulkInserterName = null,
            PostBulkInsertDelegate<T> postBulkInsert = null)
            : this(new TargetTable(destLabel, connectionString, destTable), options, bulkSize, dbBulkInserterName, postBulkInsert)
        {
        }

        /// <summary>
        /// Constructs an instance of DbBulkInserter
        /// </summary>
        /// <param name="targetTable">Information about the database target and mapping label</param>
        /// <param name="options">Options to use for this dataflow</param>
        /// <param name="bulkSize">The bulk size to insert in a batch. Default to 8192.</param>
        /// <param name="dbBulkInserterName">A given name of this bulk inserter (would be nice for logging)</param>
        /// <param name="postBulkInsert">A delegate that enables you to inject some customized work whenever a bulk insert is done</param>
        public DbBulkInserterBase(TargetTable targetTable, 
                                  DataflowOptions options, 
                                  int bulkSize = 4096 * 2, 
                                  string dbBulkInserterName = null,
                                  PostBulkInsertDelegate<T> postBulkInsert = null) 
            : base(options)
        {
            this.m_targetTable = targetTable;
            this.m_typeAccessor = TypeAccessorManager<T>.GetAccessorForTable(targetTable);
            this.m_bulkSize = bulkSize;
            this.m_dbBulkInserterName = dbBulkInserterName;
            this.m_postBulkInsert = postBulkInsert;
            this.m_batchBlock = new BatchBlock<T>(bulkSize, options.ToGroupingBlockOption());
            
            var bulkInsertOption = options.ToExecutionBlockOption();
            //Action block deal with array references
            if (bulkInsertOption.BoundedCapacity != DataflowBlockOptions.Unbounded)
            {
                bulkInsertOption.BoundedCapacity = bulkInsertOption.BoundedCapacity / bulkSize;
            }

            this.m_actionBlock = new ActionBlock<T[]>(array => this.DumpToDBAsync(array, targetTable), bulkInsertOption);
            this.m_batchBlock.LinkTo(this.m_actionBlock, this.m_defaultLinkOption);
            this.m_logger = Utils.GetNamespaceLogger();

            this.RegisterChild(this.m_batchBlock);
            this.RegisterChild(this.m_actionBlock);
            
            this.m_timer = new Timer(
                state =>
                    {
                        this.TriggerBatch();
                    }, null, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));
        }

        protected abstract Task DumpToDBAsync(T[] data, TargetTable targetTable);

        /// <summary>
        /// See <see cref="Dataflow{T}.InputBlock"/>
        /// </summary>
        public override ITargetBlock<T> InputBlock { get { return this.m_batchBlock; } }

        /// <summary>
        /// See <see cref="Dataflow{T}.Name"/>
        /// </summary>
        public override string Name
        {
            get {
                return this.m_dbBulkInserterName ?? base.Name;
            }
        }

        /// <summary>
        /// The internal property-column mapper used by this bulk inserter
        /// </summary>
        public TypeAccessor<T> TypeAccessor
        {
            get
            {
                return this.m_typeAccessor;
            }
        }

        /// <summary>
        /// See <see cref="Dataflow{T}.BufferStatus"/>
        /// </summary>
        public override Tuple<int, int> BufferStatus
        {
            get
            {
                var bs = base.BufferStatus;
                return new Tuple<int, int>(bs.Item1 * this.m_bulkSize, bs.Item2);
            }
        }

        protected virtual async Task OnPostBulkInsert(SqlConnection sqlConnection, TargetTable target, T[] insertedData)
        {
            if (this.m_postBulkInsert != null)
            {
                await this.m_postBulkInsert(sqlConnection, this.m_targetTable, insertedData).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Explicitly triggers a bulk insert immediately even if the internal buffer has fewer items than the given bulk size
        /// </summary>
        public void TriggerBatch()
        {
            this.m_batchBlock.TriggerBatch();
        }
    }
}