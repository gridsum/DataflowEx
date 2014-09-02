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
        class DimTableInserter : DbBulkInserter<TIn>, IEqualityComparer<TIn>, IOutputDataflow<JoinBatch<TIn>>, IRingNode
        {
            private readonly DbDataJoiner<TIn, TLookupKey> m_host;
            private readonly DBColumnMapping m_joinOnMapping;
            private Func<TIn, TLookupKey> m_keyGetter;
            private BufferBlock<JoinBatch<TIn>> m_outputBlock;
            private IEqualityComparer<TLookupKey> m_keyComparer;

            public DimTableInserter(DbDataJoiner<TIn, TLookupKey> host, TargetTable targetTable, Expression<Func<TIn, TLookupKey>> joinBy, DBColumnMapping joinOnMapping)
                : base(targetTable, DataflowOptions.Default)
            {
                this.m_host = host;
                this.m_joinOnMapping = joinOnMapping;
                m_keyComparer = typeof(TLookupKey) == typeof(byte[])
                                    ? (IEqualityComparer<TLookupKey>)((object)new ByteArrayEqualityComparer())
                                    : EqualityComparer<TLookupKey>.Default;
                m_keyGetter = joinBy.Compile();
                m_outputBlock = new BufferBlock<JoinBatch<TIn>>();
                RegisterChild(m_outputBlock);
                m_actionBlock.LinkNormalCompletionTo(m_outputBlock);
            }
            
            protected override async Task DumpToDB(TIn[] data, TargetTable targetTable)
            {
                IsBusy = true;

                int oldCount = data.Length;
                var newArray = data.Distinct(this).ToArray();
                int newCount = newArray.Length;
                LogHelper.Logger.DebugFormat("{0} batch distincted from {1} down to {2}", this.FullName, oldCount, newCount);

                var tmpTargetTable = new TargetTable(
                    targetTable.DestLabel,
                    targetTable.ConnectionString,
                    targetTable.TableName + "_tmp");

                //create tmp table
                string createTmpTable = string.Format(
                    "if OBJECT_ID('{0}', 'U') is not null drop table dbo.profile2;"
                    + "select * into {0} from {1} where 0 = 1",
                    tmpTargetTable.TableName,
                    targetTable.TableName);

                using (var connection = new SqlConnection(targetTable.ConnectionString))
                {
                    SqlCommand command = new SqlCommand(createTmpTable, connection);
                    command.ExecuteNonQuery();
                }

                await base.DumpToDB(newArray, tmpTargetTable);

                //merge
                string mergeTmpToDimTable =
                    string.Format(
                        "MERGE INTO {0} as TGT" + "USING {1} as SRC on TGT.{2} = SRC.{2}"
                        + "WHEN NOT MATCHED THEN INSERT {3} VALUES {4}"
                        + "OUTPUT $action, inserted.{5}, deleted.{5}, inserted.{2}, deleted.{2}",
                        targetTable.TableName,
                        tmpTargetTable.TableName,
                        m_joinOnMapping.DestColumnName,
                        Utils.FlattenColumnNames(m_typeAccessor.SchemaTable.Columns, ""),
                        Utils.FlattenColumnNames(m_typeAccessor.SchemaTable.Columns, "SRC"),
                        Utils.GetAutoIncrementColumn(m_typeAccessor.SchemaTable.Columns)
                        );

                var subCache = new Dictionary<TLookupKey, PartialDimRow<TLookupKey>>();

                using (var connection = new SqlConnection(targetTable.ConnectionString))
                {
                    SqlCommand command = new SqlCommand(mergeTmpToDimTable, connection);

                    SqlDataReader reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        long dimKey = reader.GetInt64(1);
                        TLookupKey joinColumnValue = (TLookupKey) reader[3];
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
                        globalCache.Add(dimRow.Key, dimRow.Value);
                    }
                }
                
                //todo: lookup from the sub cache and output directly (no global cache lookup)
                foreach (var item in data)
                {
                    m_host.OnSuccessfulLookup(item, subCache[m_keyGetter(item)]);
                }

                //todo: add CacheLookupStrategy.NoLookup/LocalLookup/RemoteLookup
                var redoBatch = new JoinBatch<TIn>(data, CacheLookupStrategy.NoLookup);
                m_outputBlock.SafePost(redoBatch);

                IsBusy = false;
            }

            public bool Equals(TIn x, TIn y)
            {
                return this.m_keyComparer.Equals(m_keyGetter(x),m_keyGetter(y));
            }

            public int GetHashCode(TIn obj)
            {
                return this.m_keyComparer.GetHashCode(m_keyGetter(obj));
            }
            
            public ISourceBlock<JoinBatch<TIn>> OutputBlock { get
            {
                return m_outputBlock;
            } }

            public void LinkTo(IDataflow<JoinBatch<TIn>> other)
            {
                this.LinkBlockToFlow(m_outputBlock, other);
            }

            public bool IsBusy { get; private set; }
        }
        
        private readonly TargetTable m_dimTableTarget;
        private BatchBlock<TIn> m_batchBlock;
        private TransformManyDataflow<JoinBatch<TIn>, TIn> m_lookupNode;
        private DBColumnMapping m_joinOnMapping;
        private TypeAccessor<TIn> m_typeAccessor;
        private DimTableInserter m_dimInserter;
        private RowCache<TLookupKey> m_rowCache;

        public DbDataJoiner(Expression<Func<TIn, TLookupKey>> joinOn, TargetTable dimTableTarget, int batchSize = 8 * 1024, int cacheSize = 1024 * 1024)
            : base(DataflowOptions.Default)
        {
            m_dimTableTarget = dimTableTarget;
            m_batchBlock = new BatchBlock<TIn>(batchSize);
            m_lookupNode = new TransformManyDataflow<JoinBatch<TIn>, TIn>(this.JoinBatch);
            m_lookupNode.Name = "LookupNode";
            m_typeAccessor = TypeAccessorManager<TIn>.GetAccessorForTable(dimTableTarget);
            m_rowCache = new RowCache<TLookupKey>(cacheSize);

            m_joinOnMapping = m_typeAccessor.DbColumnMappings.First(m => m.Host.PropertyInfo == this.ExtractPropertyInfo(joinOn));
            
            var transformer =
                new TransformBlock<TIn[], JoinBatch<TIn>>(
                    array => new JoinBatch<TIn>(array, CacheLookupStrategy.RemoteLookup)).ToDataflow();
            transformer.Name = "ArrayToJoinBatchConverter";

            transformer.LinkFromBlock(m_batchBlock);
            transformer.LinkTo(m_lookupNode);
            
            RegisterChild(m_batchBlock);
            RegisterChild(transformer);
            RegisterChild(m_lookupNode);

            m_dimInserter = new DimTableInserter(this, dimTableTarget, joinOn, m_joinOnMapping);
            var hb = new HeartbeatNode<JoinBatch<TIn>>();

            m_lookupNode.OutputBlock.LinkNormalCompletionTo(m_dimInserter.InputBlock);
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

        protected virtual IEnumerable<TIn> JoinBatch(JoinBatch<TIn> batch)
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

            lock (m_rowCache) //may have race condition with dimtableinserter who will update the global cache
            {
                foreach (var input in batch.Data)
                {
                    IDimRow<TLookupKey> row;
                    if (m_rowCache.TryGetValue((TLookupKey)accessor(input), out row))
                    {
                        this.OnSuccessfulLookup(input, row);
                    }
                    else
                    {
                        if (batch.Strategy == CacheLookupStrategy.RemoteLookup)
                        {
                            //post to dim inserter to do remote lookup (insert to tmp table and do a MERGE)
                            m_dimInserter.InputBlock.SafePost(input);
                        }
                        else
                        {
                            //Local lookup failed. record in garbage recorder as the input is discarded.
                            this.GarbageRecorder.Record(input);
                        }
                    }
                }
            }

            return batch.Data;
        }

        /// <summary>
        /// Override this method to customize row cache initialization logic
        /// </summary>
        protected virtual void InitializeCache(RowCache<TLookupKey> cache)
        {
            //no op by default
        }

        /// <summary>
        /// The function to call when an input item finds a row in the dimension table by joining condition.
        /// Usually the implementation of this method should assign to some fields/properties(e.g. key column) of the input item according to the dimension row.
        /// </summary>
        protected abstract void OnSuccessfulLookup(TIn input, IDimRow<TLookupKey> rowInDimTable);
        
        protected virtual string GetDimTableQueryString(string tableName)
        {
            return string.Format("select * from {0}", tableName);
        }

        protected virtual DataView RegenerateJoinTable()
        {
            LogHelper.Logger.DebugFormat("{0} Pulling join table '{1}' to memory. Label: {2}",
                this.FullName, m_dimTableTarget.TableName, m_dimTableTarget.DestLabel);

            string selectStatement = this.GetDimTableQueryString(m_dimTableTarget.TableName);

            DataTable cacheTable;
            using (var conn = new SqlConnection(this.m_dimTableTarget.ConnectionString))
            {
                cacheTable = new DataTable();
                using (var adapter = new SqlDataAdapter(selectStatement, conn))
                {
                    adapter.Fill(cacheTable);
                }
            }
            
            LogHelper.Logger.DebugFormat("{0} Join table '{1}' pulled ({3} rows). Label: {2}",
                this.FullName, m_dimTableTarget.TableName, m_dimTableTarget.DestLabel, cacheTable.Rows.Count);

            DataView indexedTable = new DataView(
                cacheTable,
                null,
                this.m_joinOnMapping.DestColumnName,
                DataViewRowState.CurrentRows);

            LogHelper.Logger.DebugFormat("{0} Indexing done for in-memory join table '{1}'. Label: {2}",
                this.FullName, m_dimTableTarget.TableName, m_dimTableTarget.DestLabel);

            return indexedTable;
        }

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
