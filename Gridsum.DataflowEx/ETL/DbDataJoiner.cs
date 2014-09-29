namespace Gridsum.DataflowEx.ETL
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    using Common.Logging;

    using Gridsum.DataflowEx.AutoCompletion;
    using Gridsum.DataflowEx.Databases;

    public struct JoinBatch<TIn> where TIn : class
    {
        public readonly TIn[] Data;
        public readonly CacheLookupStrategy Strategy;
        
        public JoinBatch(TIn[] batch, CacheLookupStrategy strategy)
        {
            Strategy = strategy;
            this.Data = batch;
        }
    }

    public enum CacheLookupStrategy
    {
        NoLookup,
        LocalLookup,
        RemoteLookup,
    }

    public abstract class DbDataJoiner<TIn, TLookupKey> : Dataflow<TIn, TIn> where TIn : class 
    {
        /// <summary>
        /// Used by the joiner to insert unmatched rows to the dim table.
        /// 2 differences between this and standard db bulkinserter:
        /// (1) It has a preprocess step to select distinct rows
        /// (2) It will output original data out tagged with CacheLookupStrategy.NoLookup
        /// </summary>
        protected class DimTableInserter : DbBulkInserter<TIn>, IEqualityComparer<TIn>, IOutputDataflow<JoinBatch<TIn>>, IRingNode
        {
            private readonly DbDataJoiner<TIn, TLookupKey> m_host;
            private Func<TIn, TLookupKey> m_keyGetter;
            private IEqualityComparer<TLookupKey> m_keyComparer;
            private readonly TargetTable m_tmpTargetTable;
            private readonly string m_createTmpTable;
            private readonly string m_mergeTmpToDimTable;
            private Dataflow<JoinBatch<TIn>, JoinBatch<TIn>> m_outputBuffer;

            public DimTableInserter(DbDataJoiner<TIn, TLookupKey> host, TargetTable targetTable, Expression<Func<TIn, TLookupKey>> joinBy, DataflowOptions option)
                : base(targetTable, option, host.m_batchSize)
            {
                this.m_host = host;
                m_keyGetter = joinBy.Compile();
                m_keyComparer = m_host.m_keyComparer;
                m_outputBuffer = new BufferBlock<JoinBatch<TIn>>(option.ToGroupingBlockOption()).ToDataflow();
                m_outputBuffer.Name = "OutputBuffer";
                RegisterChild(m_outputBuffer);
                m_outputBuffer.RegisterDependency(m_actionBlock);
                
                m_tmpTargetTable = new TargetTable(
                    targetTable.DestLabel,
                    targetTable.ConnectionString,
                    targetTable.TableName + "_tmp");

                //create tmp table
                m_createTmpTable = string.Format(
                    "if OBJECT_ID('{0}', 'U') is not null drop table {0};"
                    + "select * into {0} from {1} where 0 = 1",
                    this.m_tmpTargetTable.TableName,
                    targetTable.TableName);

                //merge
                m_mergeTmpToDimTable =
                    string.Format(
                        "MERGE INTO {0} as TGT "
                        + "USING {1} as SRC on TGT.[{2}] = SRC.[{2}] "
                        + "WHEN MATCHED THEN UPDATE SET TGT.[{2}] = TGT.[{2}] "
                        + "WHEN NOT MATCHED THEN INSERT {3} VALUES {4} "
                        + "OUTPUT inserted.[{5}], inserted.[{2}] ;",
                        targetTable.TableName,
                        this.m_tmpTargetTable.TableName,
                        m_host.m_joinOnMapping.DestColumnName,
                        Utils.FlattenColumnNames(m_typeAccessor.SchemaTable.Columns, ""),
                        Utils.FlattenColumnNames(m_typeAccessor.SchemaTable.Columns, "SRC"),
                        Utils.GetAutoIncrementColumn(m_typeAccessor.SchemaTable.Columns)
                        );
            }
            
            protected override async Task DumpToDBAsync(TIn[] data, TargetTable targetTable)
            {
                IsBusy = true;

                int oldCount = data.Length;
                var deduplicatedData = data.Distinct(this).ToArray();
                int newCount = deduplicatedData.Length;
                m_host.m_logger.DebugFormat("{0} batch distincted from {1} down to {2}", this.FullName, oldCount, newCount);
                
                using (var connection = new SqlConnection(targetTable.ConnectionString))
                {
                    connection.Open();
                    SqlCommand command = new SqlCommand(m_createTmpTable, connection);
                    command.ExecuteNonQuery();
                }

                await base.DumpToDBAsync(deduplicatedData, m_tmpTargetTable);
                
                m_host.m_logger.DebugFormat("{0} Executing merge as server-side lookup: {1}", this.FullName, m_mergeTmpToDimTable);

                var subCache = new Dictionary<TLookupKey, PartialDimRow<TLookupKey>>(deduplicatedData.Length, m_keyComparer);

                using (var connection = new SqlConnection(targetTable.ConnectionString))
                {
                    connection.Open();

                    SqlCommand command = new SqlCommand(this.m_mergeTmpToDimTable, connection);

                    var autoKeyColumn = Utils.GetAutoIncrementColumn(m_typeAccessor.SchemaTable.Columns);
                    bool is64BitAutoKey = autoKeyColumn.DataType == typeof(long);

                    SqlDataReader reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        long dimKey = is64BitAutoKey ? reader.GetInt64(0) : reader.GetInt32(0); //$inserted.AutoKey
                        TLookupKey joinColumnValue = (TLookupKey) reader[1]; //$inserted.JoinOnColumn

                        //add to the subcache no matter it is an "UPDATE" or "INSERT"
                        subCache.Add(
                            joinColumnValue, 
                            new PartialDimRow<TLookupKey> {AutoIncrementKey =  dimKey, JoinOn = joinColumnValue});
                    }
                }

                var globalCache = m_host.m_rowCache;

                //update global cache using the sub cache
                lock (globalCache)
                {
                    foreach (var dimRow in subCache)
                    {
                        globalCache.TryAdd(dimRow.Key, dimRow.Value);
                    }
                }
                
                m_host.m_logger.DebugFormat("{0} Global cache now has {1} items after merging sub cache", FullName, globalCache.Count);

                //lookup from the sub cache (no global cache lookup)
                var host = m_host;
                var keyGetter = m_keyGetter;
                foreach (var item in data)
                {
                    host.OnSuccessfulLookup(item, subCache[keyGetter(item)]);
                }

                //and output as a already-looked-up batch
                var doneBatch = new JoinBatch<TIn>(data, CacheLookupStrategy.NoLookup);
                this.m_outputBuffer.SendAsync(doneBatch);

                IsBusy = false;
            }

            public bool Equals(TIn x, TIn y)
            {
                return m_keyComparer.Equals(m_keyGetter(x),m_keyGetter(y));
            }

            public int GetHashCode(TIn obj)
            {
                return m_keyComparer.GetHashCode(m_keyGetter(obj));
            }
            
            public ISourceBlock<JoinBatch<TIn>> OutputBlock { get
            {
                return m_outputBuffer.OutputBlock;
            } }

            public void LinkTo(IDataflow<JoinBatch<TIn>> other)
            {
                this.LinkBlockToFlow(this.m_outputBuffer.OutputBlock, other);
            }

            public bool IsBusy { get; private set; }
        }
        
        protected readonly TargetTable m_dimTableTarget;
        protected readonly int m_batchSize;
        protected BatchBlock<TIn> m_batchBlock;
        protected TransformManyDataflow<JoinBatch<TIn>, TIn> m_lookupNode;
        protected DBColumnMapping m_joinOnMapping;
        protected TypeAccessor<TIn> m_typeAccessor;
        protected DimTableInserter m_dimInserter;
        protected RowCache<TLookupKey> m_rowCache;
        protected IEqualityComparer<TLookupKey> m_keyComparer;
        protected ILog m_logger;

        public DbDataJoiner(Expression<Func<TIn, TLookupKey>> joinOn, TargetTable dimTableTarget, DataflowOptions option, int batchSize = 8 * 1024, int cacheSize = 1024 * 1024)
            : base(option)
        {
            m_dimTableTarget = dimTableTarget;
            m_batchSize = batchSize;
            m_batchBlock = new BatchBlock<TIn>(batchSize, option.ToGroupingBlockOption());
            m_lookupNode = new TransformManyDataflow<JoinBatch<TIn>, TIn>(this.JoinBatch, option);
            m_lookupNode.Name = "LookupNode";
            m_typeAccessor = TypeAccessorManager<TIn>.GetAccessorForTable(dimTableTarget);
            m_keyComparer = typeof(TLookupKey) == typeof(byte[])
                                    ? (IEqualityComparer<TLookupKey>)((object)new ByteArrayEqualityComparer())
                                    : EqualityComparer<TLookupKey>.Default;
            m_rowCache = new RowCache<TLookupKey>(cacheSize, m_keyComparer);
            m_logger = Utils.GetNamespaceLogger();

            m_joinOnMapping = m_typeAccessor.DbColumnMappings.First(m => m.Host.PropertyInfo == this.ExtractPropertyInfo(joinOn));
            
            var transformer =
                new TransformBlock<TIn[], JoinBatch<TIn>>(
                    array => new JoinBatch<TIn>(array, CacheLookupStrategy.RemoteLookup), option.ToExecutionBlockOption()).ToDataflow();
            transformer.Name = "ArrayToJoinBatchConverter";

            transformer.LinkFromBlock(m_batchBlock);
            transformer.LinkTo(m_lookupNode);
            
            RegisterChild(m_batchBlock);
            RegisterChild(transformer);
            RegisterChild(m_lookupNode);

            m_dimInserter = new DimTableInserter(this, dimTableTarget, joinOn, option) {Name = "DimInserter"};
            var hb = new HeartbeatNode<JoinBatch<TIn>>(option);

            m_dimInserter.RegisterDependency(m_lookupNode);

            m_dimInserter.LinkTo(hb);
            hb.LinkTo(m_lookupNode);

            RegisterChild(m_dimInserter);
            RegisterChild(hb);
            RegisterChildRing(transformer.CompletionTask, m_lookupNode, m_dimInserter, hb);
        }

        public PropertyInfo ExtractPropertyInfo<T1, T2>(Expression<Func<T1, T2>> expression)
        {
            var me = expression.Body as MemberExpression;

            if (me == null)
            {
                throw new ArgumentException("Expression must be a simple property getter: " + expression);
            }

            var pi = me.Member as PropertyInfo;

            if (pi == null)
            {
                throw new ArgumentException("Expression must be a simple property getter: " + expression);
            }

            return pi;
        }

        protected virtual async Task<IEnumerable<TIn>> JoinBatch(JoinBatch<TIn> batch)
        {
            if (m_rowCache.Count == 0)
            {
                InitializeCache(m_rowCache);
            }
            
            if (batch.Strategy == CacheLookupStrategy.NoLookup)
            {
                return batch.Data;
            }
            
            Func<TIn, object> accessor = m_typeAccessor.GetPropertyAccessor(this.m_joinOnMapping.DestColumnOffset);

            var outputList = new List<TIn>(batch.Data.Length / 2);
            var remoteLookupList = new List<TIn>(batch.Data.Length / 2);
            int missCount = 0;
            
            lock (m_rowCache) //may have race condition with dimtableinserter who will update the global cache
            {
                foreach (var input in batch.Data)
                {
                    IDimRow<TLookupKey> row;
                    if (m_rowCache.TryGetValue((TLookupKey)accessor(input), out row))
                    {
                        this.OnSuccessfulLookup(input, row);
                        outputList.Add(input);
                    }
                    else
                    {
                        missCount ++;

                        if (batch.Strategy == CacheLookupStrategy.RemoteLookup)
                        {
                            remoteLookupList.Add(input);
                        }
                        else
                        {
                            //Local lookup failed. record in garbage recorder as the input is discarded.
                            this.GarbageRecorder.Record(input);
                        }
                    }
                }
            }

            foreach (var cacheMiss in remoteLookupList)
            {
                //post to dim inserter to do remote lookup (insert to tmp table and do a MERGE)
                await m_dimInserter.SendAsync(cacheMiss);
            }

            m_logger.DebugFormat("{0} {1} cache miss among {2} lookup in this round of JoinBatch()", FullName, missCount, batch.Data.Length);

            return outputList;
        }

        /// <summary>
        /// Override this method to customize row cache initialization logic
        /// </summary>
        protected virtual void InitializeCache(RowCache<TLookupKey> cache)
        {
            string selectPartial = string.Format(
                "select TOP {3} [{0}], [{1}] from {2} order by [{0}] desc",
                Utils.GetAutoIncrementColumn(m_typeAccessor.SchemaTable.Columns).ColumnName,
                m_joinOnMapping.DestColumnName,
                m_dimTableTarget.TableName,
                cache.SizeLimit);

            m_logger.DebugFormat("[{0}] Start to initialize cache using sql: {1}", FullName, selectPartial);

            using (var connection = new SqlConnection(m_dimTableTarget.ConnectionString))
            {
                connection.Open();

                SqlCommand command = new SqlCommand(selectPartial, connection);

                var autoKeyColumn = Utils.GetAutoIncrementColumn(m_typeAccessor.SchemaTable.Columns);
                bool is64BitAutoKey = autoKeyColumn.DataType == typeof(long);

                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    long dimKey = is64BitAutoKey ? reader.GetInt64(0) : reader.GetInt32(0); //$inserted.AutoKey
                    TLookupKey joinColumnValue = (TLookupKey)reader[1]; //$inserted.JoinOnColumn

                    //add to the subcache no matter it is an "UPDATE" or "INSERT"
                    cache.TryAdd(
                        joinColumnValue,
                        new PartialDimRow<TLookupKey> { AutoIncrementKey = dimKey, JoinOn = joinColumnValue });
                }
            }

            m_logger.DebugFormat("[{0}] Global cache initialized with {1} items", FullName, cache.Count);
        }

        /// <summary>
        /// The function to call when an input item finds a row in the dimension table by joining condition.
        /// Usually the implementation of this method should assign to some fields/properties(e.g. key column) of the input item according to the dimension row.
        /// </summary>
        protected abstract void OnSuccessfulLookup(TIn input, IDimRow<TLookupKey> rowInDimTable);
        
        public override ITargetBlock<TIn> InputBlock
        {
            get
            {
                return this.m_batchBlock;
            }
        }

        public override ISourceBlock<TIn> OutputBlock
        {
            get
            {
                return m_lookupNode.OutputBlock;
            }
        }
    }
}
