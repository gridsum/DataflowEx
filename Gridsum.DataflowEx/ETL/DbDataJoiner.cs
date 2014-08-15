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
        public readonly CacheRefreshStrategy Strategy;
        
        public JoinBatch(TIn[] batch, CacheRefreshStrategy strategy)
        {
            Strategy = strategy;
            this.Data = batch;
        }
    }

    public enum CacheRefreshStrategy
    {
        Never,
        Auto,
        Always,
    }

    public class DbDataJoiner<TIn, TKey, TUpdate> : Dataflow<TIn, KeyValuePair<TIn, TUpdate>> where TIn : class 
    {
        /// <summary>
        /// Used by the joiner to insert unmatched rows to the dim table.
        /// 2 differences between this and standard db bulkinserter:
        /// (1) It has a preprocess step to select distinct rows
        /// (2) It will output original data out tagged with CacheRefreshStrategy.Always
        /// </summary>
        class DimTableInserter : DbBulkInserter<TIn>, IEqualityComparer<TIn>, IOutputDataflow<JoinBatch<TIn>>
        {
            private Func<TIn, TKey> m_keyGetter;
            private BufferBlock<JoinBatch<TIn>> m_outputBlock;

            public DimTableInserter(TargetTable targetTable, Expression<Func<TIn, TKey>> joinBy)
                : base(targetTable, DataflowOptions.Default)
            {
                m_keyGetter = joinBy.Compile();
                m_outputBlock = new BufferBlock<JoinBatch<TIn>>();
                RegisterChild(m_outputBlock);
                m_actionBlock.LinkNormalCompletionTo(m_outputBlock);
            }
            
            protected override async Task DumpToDB(TIn[] data, TargetTable targetTable)
            {
                int oldCount = data.Length;
                var newArray = data.Distinct(this).ToArray();
                int newCount = newArray.Length;
                LogHelper.Logger.DebugFormat("{0} batch distincted from {1} down to {2}", this.FullName, oldCount, newCount);

                await base.DumpToDB(newArray, targetTable);

                var redoBatch = new JoinBatch<TIn>(data, CacheRefreshStrategy.Always);
                m_outputBlock.SafePost(redoBatch);
            }

            public bool Equals(TIn x, TIn y)
            {
                return m_keyGetter(x).Equals(m_keyGetter(y));
            }

            public int GetHashCode(TIn obj)
            {
                return m_keyGetter(obj).GetHashCode();
            }
            
            public ISourceBlock<JoinBatch<TIn>> OutputBlock { get
            {
                return m_outputBlock;
            } }

            public void LinkTo(IDataflow<JoinBatch<TIn>> other)
            {
                this.LinkBlockToFlow(m_outputBlock, other);
            }
        }
        
        private readonly TargetTable m_dimTableTarget;
        private readonly int m_batchSize;
        private BatchBlock<TIn> m_batchBlock;
        private Dataflow<JoinBatch<TIn>, KeyValuePair<TIn, TUpdate>> m_lookupNode;
        private DBColumnMapping m_joinOnMapping;
        private DBColumnMapping m_updateOnMapping;
        private TypeAccessor<TIn> m_typeAccessor;
        private DimTableInserter m_dimInserter;
        private DataView m_indexedTable;

        public DbDataJoiner(Expression<Func<TIn, TKey>> joinOn, Expression<Func<TIn, TUpdate>> updateOn,
            TargetTable dimTableTarget, int batchSize)
            : base(DataflowOptions.Default)
        {
            m_dimTableTarget = dimTableTarget;
            m_batchSize = batchSize;
            m_batchBlock = new BatchBlock<TIn>(this.m_batchSize);
            m_lookupNode = new TransformManyBlock<JoinBatch<TIn>, KeyValuePair<TIn, TUpdate>>(new Func<JoinBatch<TIn>, IEnumerable<KeyValuePair<TIn, TUpdate>>>(this.JoinBatch)).ToDataflow("LookupNode");
            m_typeAccessor = TypeAccessorManager<TIn>.GetAccessorForTable(dimTableTarget);

            m_joinOnMapping = this.m_typeAccessor.DbColumnMappings.First(m => m.Host.PropertyInfo == this.ExtractPropertyInfo(joinOn));
            m_updateOnMapping = this.m_typeAccessor.DbColumnMappings.First(m => m.Host.PropertyInfo == this.ExtractPropertyInfo(updateOn));

            var transformer =
                new TransformBlock<TIn[], JoinBatch<TIn>>(
                    array => new JoinBatch<TIn>(array, CacheRefreshStrategy.Never));

            m_batchBlock.LinkTo(transformer);
            m_lookupNode.LinkFrom(transformer);

            RegisterChild(m_batchBlock);
            RegisterChild(transformer);
            RegisterChild(m_lookupNode);

            m_dimInserter = new DimTableInserter(dimTableTarget, joinOn);
            var hb = new HeartbeatNode<JoinBatch<TIn>>();

            m_lookupNode.OutputBlock.LinkNormalCompletionTo(m_dimInserter.InputBlock);
            m_dimInserter.LinkTo(hb);
            hb.LinkTo(m_lookupNode);

            RegisterChild(m_dimInserter);
            RegisterChild(hb);
            RegisterChildRing(transformer.Completion, m_lookupNode, m_dimInserter, hb);
        }

        public PropertyInfo ExtractPropertyInfo<T1, T2>(Expression<Func<T1, T2>> expression)
        {
            var me = expression.Body as MemberExpression;
            
            return me.Member as PropertyInfo;
        }

        private IEnumerable<KeyValuePair<TIn, TUpdate>> JoinBatch(JoinBatch<TIn> batch)
        {
            if (m_indexedTable == null || batch.Strategy == CacheRefreshStrategy.Always)
            {
                m_indexedTable = this.RegenerateJoinTable();
            }

            foreach (var input in batch.Data)
            {
                int idx = this.m_indexedTable.Find(m_typeAccessor.GetPropertyAccessor(this.m_joinOnMapping.DestColumnOffset)(input));

                if (idx != -1)
                {
                    DataRowView row = this.m_indexedTable[idx];
                    TUpdate updateValue = (TUpdate) row[m_updateOnMapping.DestColumnName];

                    yield return new KeyValuePair<TIn, TUpdate>(input, updateValue);
                }
                else
                {
                    m_dimInserter.InputBlock.SafePost(input);
                }
            }
        }

        protected virtual DataView RegenerateJoinTable()
        {
            LogHelper.Logger.DebugFormat("{0} pulling join table to memory... Table name: {1} Label: {2}",
                this.FullName, m_dimTableTarget.TableName, m_dimTableTarget.DestLabel);

            //select a part of the underlying table by given column filtering
            string select = string.Format(
                "select {0}, {1} from {2}",
                m_joinOnMapping.DestColumnName,
                m_updateOnMapping.DestColumnName,
                m_dimTableTarget.TableName);

            DataTable cacheTable;
            using (var conn = new SqlConnection(this.m_dimTableTarget.ConnectionString))
            {
                cacheTable = new DataTable();
                using (var adapter = new SqlDataAdapter(@select, conn))
                {
                    adapter.Fill(cacheTable);
                }
            }

            if (!typeof(TUpdate).IsAssignableFrom(cacheTable.Columns[this.m_updateOnMapping.DestColumnName].DataType))
            {
                throw new InvalidDBColumnMappingException(
                    "Generic type is not assignable from db column type: "
                    + cacheTable.Columns[this.m_updateOnMapping.DestColumnName].DataType,
                    this.m_updateOnMapping,
                    null);
            }

            DataView indexedTable = new DataView(
                cacheTable,
                null,
                this.m_joinOnMapping.DestColumnName,
                DataViewRowState.CurrentRows);

            LogHelper.Logger.DebugFormat("{0} Join table pulled. Table name: {1} Label: {2}",
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

        public override ISourceBlock<KeyValuePair<TIn, TUpdate>> OutputBlock
        {
            get
            {
                return m_lookupNode.OutputBlock;
            }
        }
    }
}
