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

    public class DbDataJoiner<TIn, TKey> : Dataflow<TIn, KeyValuePair<TIn, DataRowView>> where TIn : class 
    {
        /// <summary>
        /// Used by the joiner to insert unmatched rows to the dim table.
        /// 2 differences between this and standard db bulkinserter:
        /// (1) It has a preprocess step to select distinct rows
        /// (2) It will output original data out tagged with CacheRefreshStrategy.Always
        /// </summary>
        class DimTableInserter : DbBulkInserter<TIn>, IEqualityComparer<TIn>, IOutputDataflow<JoinBatch<TIn>>, IRingNode
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
                IsBusy = true;

                int oldCount = data.Length;
                var newArray = data.Distinct(this).ToArray();
                int newCount = newArray.Length;
                LogHelper.Logger.DebugFormat("{0} batch distincted from {1} down to {2}", this.FullName, oldCount, newCount);

                await base.DumpToDB(newArray, targetTable);

                var redoBatch = new JoinBatch<TIn>(data, CacheRefreshStrategy.Always);
                m_outputBlock.SafePost(redoBatch);

                IsBusy = false;
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

            public bool IsBusy { get; private set; }
        }
        
        private readonly TargetTable m_dimTableTarget;
        private readonly int m_batchSize;
        private BatchBlock<TIn> m_batchBlock;
        private TransformManyDataflow<JoinBatch<TIn>, KeyValuePair<TIn, DataRowView>> m_lookupNode;
        private DBColumnMapping m_joinOnMapping;
        private TypeAccessor<TIn> m_typeAccessor;
        private DimTableInserter m_dimInserter;
        private DataView m_indexedTable;

        public DbDataJoiner(Expression<Func<TIn, TKey>> joinOn, TargetTable dimTableTarget, int batchSize)
            : base(DataflowOptions.Default)
        {
            m_dimTableTarget = dimTableTarget;
            m_batchSize = batchSize;
            m_batchBlock = new BatchBlock<TIn>(this.m_batchSize);
            m_lookupNode = new TransformManyDataflow<JoinBatch<TIn>, KeyValuePair<TIn, DataRowView>>(this.JoinBatch);
            m_lookupNode.Name = "LookupNode";
            m_typeAccessor = TypeAccessorManager<TIn>.GetAccessorForTable(dimTableTarget);

            m_joinOnMapping = m_typeAccessor.DbColumnMappings.First(m => m.Host.PropertyInfo == this.ExtractPropertyInfo(joinOn));
            
            var transformer =
                new TransformBlock<TIn[], JoinBatch<TIn>>(
                    array => new JoinBatch<TIn>(array, CacheRefreshStrategy.Never)).ToDataflow();
            transformer.Name = "ArrayToJoinBatchConverter";

            transformer.LinkFromBlock(m_batchBlock);
            transformer.LinkTo(m_lookupNode);
            
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

        protected virtual IEnumerable<KeyValuePair<TIn, DataRowView>> JoinBatch(JoinBatch<TIn> batch)
        {
            if (m_indexedTable == null || batch.Strategy == CacheRefreshStrategy.Always)
            {
                m_indexedTable = this.RegenerateJoinTable();
            }

            var outputList = new List<KeyValuePair<TIn, DataRowView>>(m_batchSize);

            Func<TIn, object> accessor = m_typeAccessor.GetPropertyAccessor(this.m_joinOnMapping.DestColumnOffset);

            foreach (var input in batch.Data)
            {
                int idx = this.m_indexedTable.Find(accessor(input));

                if (idx != -1)
                {
                    DataRowView row = this.m_indexedTable[idx];
                    outputList.Add(new KeyValuePair<TIn, DataRowView>(input, row));
                }
                else
                {
                    m_dimInserter.InputBlock.SafePost(input);
                }
            }

            return outputList;
        }

        protected virtual DataView RegenerateJoinTable()
        {
            LogHelper.Logger.DebugFormat("{0} Pulling join table '{1}' to memory. Label: {2}",
                this.FullName, m_dimTableTarget.TableName, m_dimTableTarget.DestLabel);

//            string select = string.Format(
//                "select {0}, {1} from {2}",
//                m_joinOnMapping.DestColumnName,
//                m_updateOnMapping.DestColumnName,
//                m_dimTableTarget.TableName);

            //todo: select partially here
            string select = string.Format("select * from {0}", m_dimTableTarget.TableName);

            DataTable cacheTable;
            using (var conn = new SqlConnection(this.m_dimTableTarget.ConnectionString))
            {
                cacheTable = new DataTable();
                using (var adapter = new SqlDataAdapter(@select, conn))
                {
                    adapter.Fill(cacheTable);
                }
            }

//            if (!typeof(TUpdate).IsAssignableFrom(cacheTable.Columns[this.m_updateOnMapping.DestColumnName].DataType))
//            {
//                throw new InvalidDBColumnMappingException(
//                    "Generic type is not assignable from db column type: "
//                    + cacheTable.Columns[this.m_updateOnMapping.DestColumnName].DataType,
//                    this.m_updateOnMapping,
//                    null);
//            }

            DataView indexedTable = new DataView(
                cacheTable,
                null,
                this.m_joinOnMapping.DestColumnName,
                DataViewRowState.CurrentRows);

            LogHelper.Logger.DebugFormat("{0} Join table '{1}' pulled ({3} rows). Label: {2}",
                this.FullName, m_dimTableTarget.TableName, m_dimTableTarget.DestLabel, cacheTable.Rows.Count);

            return indexedTable;
        }

        public override ITargetBlock<TIn> InputBlock
        {
            get
            {
                return this.m_batchBlock;
            }
        }

        public override ISourceBlock<KeyValuePair<TIn, DataRowView>> OutputBlock
        {
            get
            {
                return m_lookupNode.OutputBlock;
            }
        }
    }
}
