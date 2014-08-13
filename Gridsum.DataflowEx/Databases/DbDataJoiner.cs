using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Databases
{
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Threading.Tasks.Dataflow;

    public class DbDataJoiner<TIn, TKey, TUpdate> : Dataflow<TIn, TIn> where TIn : class 
    {
        private readonly string m_dimTableName;

        private readonly string m_connectionString;

        private readonly int m_batchSize;
        private BatchBlock<TIn> m_batchBlock;
        private TransformManyBlock<TIn[], TIn> m_lookupBlock;
        private DBColumnMapping m_joinByMapping;
        private DBColumnMapping m_updateOnMapping;
        private TypeAccessor<TIn> m_typeAccessor;

        public DbDataJoiner(Expression<Func<TIn, TKey>> joinBy, Expression<Func<TIn, TUpdate>> updateOn,
            string dimTableLabel, string dimTableName, string connectionString, int batchSize)
            : base(DataflowOptions.Default)
        {
            m_dimTableName = dimTableName;
            m_connectionString = connectionString;
            m_batchSize = batchSize;
            m_batchBlock = new BatchBlock<TIn>(m_batchSize);
            this.m_lookupBlock = new TransformManyBlock<TIn[], TIn>(new Func<TIn[], IEnumerable<TIn>>(this.JoinBatch));
            m_typeAccessor = TypeAccessorManager<TIn>.GetAccessorByDestLabel(dimTableLabel, connectionString, dimTableName);

            m_joinByMapping = m_typeAccessor.DbColumnMappings.First(m => m.Host.PropertyInfo == ExtractPropertyInfo(joinBy));
            m_updateOnMapping = m_typeAccessor.DbColumnMappings.First(m => m.Host.PropertyInfo == ExtractPropertyInfo(updateOn));
        }

        public PropertyInfo ExtractPropertyInfo<T1, T2>(Expression<Func<T1, T2>> expression)
        {
            var me = expression.Body as MemberExpression;
            return me.Member as PropertyInfo;
        }

        private IEnumerable<TIn> JoinBatch(TIn[] inputs)
        {
            //select a part of the underlying table by given column filtering

            string select = string.Format(
                "select {0}, {1} from {2}",
                m_joinByMapping.DestColumnName,
                m_updateOnMapping.DestColumnName,
                m_dimTableName);

            DataTable cacheTable;
            using (var conn = new SqlConnection(m_connectionString))
            {
                cacheTable = new DataTable();
                using (var adapter = new SqlDataAdapter(select, conn))
                {
                    adapter.Fill(cacheTable);
                }               
            }

            DataView indexedTable = new DataView(cacheTable, null, m_joinByMapping.DestColumnName, DataViewRowState.CurrentRows);

            foreach (var input in inputs)
            {
                var idx = indexedTable.Find(m_typeAccessor.GetPropertyAccessor(m_joinByMapping.DestColumnOffset)(input));

                if (idx != -1)
                {
                    var row = indexedTable[idx];
                    var updateValue = row[m_updateOnMapping.DestColumnName];

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
                return m_batchBlock;
            }
        }

        public override ISourceBlock<TIn> OutputBlock
        {
            get
            {
                throw new NotImplementedException();
            }
        }
    }
}
