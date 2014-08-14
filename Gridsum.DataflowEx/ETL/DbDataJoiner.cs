namespace Gridsum.DataflowEx.ETL
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Threading.Tasks.Dataflow;

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

    public class DbDataJoiner<TIn, TKey, TUpdate> : Dataflow<TIn, TIn> where TIn : class 
    {
        class DimTableInserter : DbBulkInserter<TIn>, IEqualityComparer<TIn>
        {
            private Func<TIn, TKey> m_keyGetter;

            public DimTableInserter(TargetTable targetTable, Expression<Func<TIn, TKey>> joinBy)
                : base(targetTable, DataflowOptions.Default)
            {
                m_keyGetter = joinBy.Compile();
            }
            
            protected override IEnumerable<TIn> PreprocessBatch(TIn[] array)
            {
                //todo: deduplicate by Expression<Func<TIn, TKey>> joinBy
                return array.Distinct(this);
            }

            public bool Equals(TIn x, TIn y)
            {
                return m_keyGetter(x).Equals(m_keyGetter(y));
            }

            public int GetHashCode(TIn obj)
            {
                return m_keyGetter(obj).GetHashCode();
            }
        }
        
        private readonly TargetTable m_dimTableTarget;
        private readonly int m_batchSize;
        private BatchBlock<TIn> m_batchBlock;
        private TransformManyBlock<JoinBatch<TIn>, TIn> m_lookupBlock;
        private DBColumnMapping m_joinByMapping;
        private DBColumnMapping m_updateOnMapping;
        private TypeAccessor<TIn> m_typeAccessor;
        
        public DbDataJoiner(Expression<Func<TIn, TKey>> joinBy, Expression<Func<TIn, TUpdate>> updateOn,
            TargetTable dimTableTarget, int batchSize)
            : base(DataflowOptions.Default)
        {
            this.m_dimTableTarget = dimTableTarget;
            m_batchSize = batchSize;
            m_batchBlock = new BatchBlock<TIn>(this.m_batchSize);
            m_lookupBlock = new TransformManyBlock<JoinBatch<TIn>, TIn>(new Func<JoinBatch<TIn>, IEnumerable<TIn>>(this.JoinBatch));
            m_typeAccessor = TypeAccessorManager<TIn>.GetAccessorForTable(dimTableTarget);

            m_joinByMapping = this.m_typeAccessor.DbColumnMappings.First(m => m.Host.PropertyInfo == this.ExtractPropertyInfo(joinBy));
            m_updateOnMapping = this.m_typeAccessor.DbColumnMappings.First(m => m.Host.PropertyInfo == this.ExtractPropertyInfo(updateOn));

            var transformer =
                new TransformBlock<TIn[], JoinBatch<TIn>>(
                    array => new JoinBatch<TIn>(array, CacheRefreshStrategy.Never));

            m_batchBlock.LinkTo(transformer);
            transformer.LinkTo(m_lookupBlock);

            RegisterChild(this.m_batchBlock);
            RegisterChild(this.m_lookupBlock, displayName: "LookupBlock");
        }

        public PropertyInfo ExtractPropertyInfo<T1, T2>(Expression<Func<T1, T2>> expression)
        {
            var me = expression.Body as MemberExpression;
            return me.Member as PropertyInfo;
        }

        private IEnumerable<TIn> JoinBatch(JoinBatch<TIn> batch)
        {
            //select a part of the underlying table by given column filtering

            string select = string.Format(
                "select {0}, {1} from {2}",
                m_joinByMapping.DestColumnName,
                m_updateOnMapping.DestColumnName,
                m_dimTableTarget.TableName);

            DataTable cacheTable;
            using (var conn = new SqlConnection(m_dimTableTarget.ConnectionString))
            {
                cacheTable = new DataTable();
                using (var adapter = new SqlDataAdapter(select, conn))
                {
                    adapter.Fill(cacheTable);
                }               
            }

            DataView indexedTable = new DataView(cacheTable, null, this.m_joinByMapping.DestColumnName, DataViewRowState.CurrentRows);

            foreach (var input in batch.Data)
            {
                var idx = indexedTable.Find(this.m_typeAccessor.GetPropertyAccessor(this.m_joinByMapping.DestColumnOffset)(input));

                if (idx != -1)
                {
                    var row = indexedTable[idx];
                    var updateValue = row[this.m_updateOnMapping.DestColumnName];

                    //todo: set the value to input
                    yield return input;
                }
                else
                {
                    //todo: not found in the cache table, as secondly output
                }
            }
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
                return this.m_lookupBlock;
            }
        }
    }
}
