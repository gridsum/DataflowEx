using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Gridsum.DataflowEx.Databases
{
    public class TypeAccessorManager<T> where T : class
    {
        private static readonly Dictionary<string, TypeAccessor<T>> m_accessors;

        static TypeAccessorManager()
        {
            m_accessors = new Dictionary<string, TypeAccessor<T>>();
        }

        private TypeAccessorManager()
        {
        }

        /// <summary>
        ///     if the typeAccessor is exist, just return it; else create a new one with parameter: destLabel, connectionString,
        ///     dataTableName
        /// </summary>
        /// <param name="destLabel"></param>
        /// <param name="connectionString"></param>
        /// <param name="dataTableName"></param>
        /// <returns></returns>
        public static TypeAccessor<T> GetAccessorByDestLabel(string destLabel, string connectionString,
            string dataTableName)
        {
            lock (m_accessors)
            {
                TypeAccessor<T> accessor = null;
                if (m_accessors.TryGetValue(destLabel, out accessor) == false)
                {
                    accessor = new TypeAccessor<T>(destLabel, connectionString, dataTableName);
                    m_accessors.Add(destLabel, accessor);
                }
                return accessor;
            }
        }
    }

    public class TypeAccessor<T> where T : class
    {
        private readonly string m_connectionString;
        private readonly IList<DBColumnMapping> m_dbColumnMappings;
        private readonly string m_destinationTablename;
        private readonly string m_destLabel;
        private readonly Dictionary<int, Func<T, dynamic>> m_properties;

        private DataTable m_schemaTable;

        #region ctor and init

        public TypeAccessor(string destLabel, string connectionString, string destinationTableName)
        {
            m_destLabel = destLabel;
            m_connectionString = connectionString;
            m_destinationTablename = string.IsNullOrWhiteSpace(destinationTableName)
                ? typeof (T).Name
                : destinationTableName;
            m_schemaTable = null;


            //create property accessor delegate for properties and coloumn mapping to database

            m_properties = new Dictionary<int, Func<T, dynamic>>();
            m_dbColumnMappings = new List<DBColumnMapping>();

            CreateTypeVistor();
        }

        private void CreateTypeVistor()
        {
            ParameterExpression paraExpression = Expression.Parameter(typeof (T), "t");

            Tuple<IDictionary<int, ReferenceTypeDepthExpression>, IList<ValueTypeMapping>> selected =
                RecursiveGetAllSelectedProperties();

            IDictionary<int, ReferenceTypeDepthExpression> referenceTypes = selected.Item1;
            IList<ValueTypeMapping> valueTypes = selected.Item2;

            #region 首先，根据深度对引用类型进行处理，生成相应的Expression

            IOrderedEnumerable<ReferenceTypeDepthExpression> values = referenceTypes.Values.OrderBy(t => t.Depth);
            foreach (ReferenceTypeDepthExpression value in values)
            {
                //父节点类型为根类型，即T
                if (value.ParentKey==0)
                {
                    value.Expression = CreatePropertyAccesorExpression(value.PropertyInfo, paraExpression, null);
                }
                else
                {
                    ReferenceTypeDepthExpression parent = null;
                    if (
                        referenceTypes.TryGetValue(value.ParentKey, out parent) == false)
                    {
                        LogHelper.Logger.Error("failed to get parent ReferenceTypeDepthException.");
                    }
                    value.Expression = CreatePropertyAccesorExpression(value.PropertyInfo, parent.Expression, null);
                }
            }

            #endregion

            #region 其次，处理所有的值类型或字符串类型，生成相应的访问Func等。

            foreach (ValueTypeMapping valueType in valueTypes)
            {
                Expression parentExpression = null;
                //父节点为根类型，即T：typeof(T)
                if (valueType.ParentKey == 0)
                {
                    parentExpression = paraExpression;
                }
                else
                {
                    ReferenceTypeDepthExpression parent = null;
                    if (
                        referenceTypes.TryGetValue(valueType.ParentKey,out parent) == false)
                    {
                        LogHelper.Logger.Error("failed to get parent ReferenceTypeDepthException.");
                    }
                    parentExpression = parent.Expression;
                }

                    BlockExpression curExpression = CreatePropertyAccesorExpression(valueType.CurrentPropertyInfo,
                        parentExpression,
                        valueType.DbColumnMapping.DefaultValue);
                    m_dbColumnMappings.Add(valueType.DbColumnMapping);
                    Expression<Func<T, object>> lambda =
                        Expression.Lambda<Func<T, Object>>(Expression.Convert(curExpression, typeof (object)),
                            paraExpression);
                    Func<T, object> func = lambda.Compile();

                    m_properties.Add(valueType.DbColumnMapping.DestColumnOffset, func);
                
            }

            #endregion
        }


        private BlockExpression CreatePropertyAccesorExpression(PropertyInfo prop, Expression parentExpr,
            object defaultValue)
        {
            #region 对于值类型， Nullable<值类型>的默认值进行处理

            Type underType = prop.PropertyType;
            if (underType.IsGenericType && underType.GetGenericTypeDefinition() == typeof (Nullable<>))
            {
                underType = Nullable.GetUnderlyingType(underType);
            }
            if (underType.IsValueType)
            {
                if (defaultValue != null)
                {
                    try
                    {
                        //测试是否可以进行转换
                        defaultValue = Convert.ChangeType(defaultValue, underType);
                    }
                    catch (Exception e)
                    {
                        LogHelper.Logger.WarnFormat(
                            "failed to parse value from object for type: {0}, name:{1}, namespace:{2}", e,
                            prop.PropertyType, prop.Name, prop.ToString());
                        defaultValue = null;
                    }
                }
                if (defaultValue == null && prop.PropertyType.IsValueType)
                {
                    defaultValue = Activator.CreateInstance(prop.PropertyType);
                }
            }

            #endregion

            ConstantExpression defaultExpr = null;
            try
            {
                //默认返回Expression
                defaultExpr = Expression.Constant(defaultValue, prop.PropertyType);
            }
            catch (Exception e)
            {
                LogHelper.Logger.ErrorFormat(
                    "failed to create constant expression for property: {0}, with default value:{1}", e,
                    prop.PropertyType, defaultValue);
            }


            //测试当前属性的“父属性”是否非空
            BinaryExpression test = Expression.NotEqual(parentExpr, Expression.Constant(null));

            //“父属性”非空时的返回值
            MemberExpression propExpr = Expression.Property(parentExpr, prop);

            LabelTarget labelTarget = Expression.Label(prop.PropertyType);

            //局部变量，用于存放最终的返回值
            ParameterExpression locExpr = Expression.Variable(prop.PropertyType);

            //测试并赋值
            ConditionalExpression ifesleTestExpr = Expression.IfThenElse(test, Expression.Assign(locExpr, propExpr),
                Expression.Assign(locExpr, defaultExpr));

            //返回值
            GotoExpression retExpr = Expression.Return(labelTarget, locExpr);

            LabelExpression labelExpr = Expression.Label(labelTarget, locExpr);

            BlockExpression block = Expression.Block(
                new[] {locExpr},
                ifesleTestExpr,
                retExpr,
                labelExpr
                );
            return block;
        }


        private DataTable GetSchemaTable()
        {
            if (string.IsNullOrWhiteSpace(m_connectionString))
            {
                LogHelper.Logger.Warn("connection string is null or empty, so database table can not be found.");
                m_schemaTable = new DataTable();
            }
            if (m_schemaTable != null) return m_schemaTable;
            using (var conn = new SqlConnection(m_connectionString))
            {
                m_schemaTable = new DataTable();
                new SqlDataAdapter(string.Format("SELECT * FROM {0}", m_destinationTablename), conn).FillSchema(
                    m_schemaTable, SchemaType.Source);
            }
            return m_schemaTable;
        }


        /// <summary>
        ///     递归获得该类型的所有被选择的属性。
        ///     如果出现一个值类型或String添加了相应DestLabel的DbColumnMapping。则选取所有的带DbColumnMapping的属性。
        ///     否则选取所有值类型或String。
        ///     再利用与数据库进行匹配，对应到相应的列。如果出现多属性对应同一个列，则报错。
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private Tuple<IDictionary<int, ReferenceTypeDepthExpression>, IList<ValueTypeMapping>>
            RecursiveGetAllSelectedProperties()
        {
            Type stringType = typeof (string);
            Type type = typeof (T);


            bool hasMapping = false;

            #region 递归判断是否有包含DbColumnMapping的值类型或String

            var pq = new Queue<Type>();
            var visitReferenceType = new List<Type>();
            pq.Enqueue(type);
            while (pq.Count > 0)
            {
                Type firstType = pq.Dequeue();

                PropertyInfo[] props = firstType.GetProperties();
                foreach (PropertyInfo prop in props)
                {
                    if (prop.PropertyType.IsValueType || prop.PropertyType == stringType)
                    {
                        var attrs = (DBColumnMapping[]) prop.GetCustomAttributes(typeof (DBColumnMapping), true);
                        bool has = attrs.Any(attr => attr.DestLabel == m_destLabel);
                        if (has == false) continue;
                        hasMapping = true;
                        break;
                    }
                    //reference type
                    if (visitReferenceType.Contains(prop.PropertyType) == false)
                        pq.Enqueue(prop.PropertyType);
                }
                visitReferenceType.Add(firstType);
            }

            #endregion

            //所有的引用类型（不包括string）
            //key: parentKey:一个利用自增来避免重复的int参数
            var referenceDict = new Dictionary<int, ReferenceTypeDepthExpression>();
            //所有的值类型
            var valueList = new List<ValueTypeMapping>();

            #region 读取所有的引用类型、值类型及String类型的属性

            int rootKey = 0;
            int parentKey = 0;
            var depthQueue = new Queue<Tuple<int, Type, int>>();
            depthQueue.Enqueue(new Tuple<int,Type, int>(parentKey++,type, 0));
            

            while (depthQueue.Count > 0)
            {
                var firstNode = depthQueue.Dequeue();
                int currentKey = firstNode.Item1;
                Type currentType = firstNode.Item2;
                int currentDepth = firstNode.Item3;

                if (currentType.IsAbstract || currentType.IsInterface)
                {
                    LogHelper.Logger.WarnFormat("getting properties for interface or abstract class type: {0}",
                        currentType);
                }

                PropertyInfo[] props = currentType.GetProperties();
                foreach (PropertyInfo prop in props)
                {
                    //值类型或引用类型
                    if (prop.PropertyType.IsValueType || prop.PropertyType == stringType)
                    {
                        valueList.Add(new ValueTypeMapping(prop, currentKey, currentDepth + 1));
                    }
                    else
                    {
                        var propTypeDepthException = new ReferenceTypeDepthExpression(prop, currentKey, currentDepth + 1);
                        if (prop.PropertyType == type || HasReferenceLoop(referenceDict, propTypeDepthException))
                        {
                            LogHelper.Logger.WarnFormat(
                                "there is reference loop(like A.B.C.A) for property type:{0} in declare type:{1}",
                                prop.PropertyType, prop.DeclaringType);
                            continue;
                        }
                        depthQueue.Enqueue(new Tuple<int,Type, int>(parentKey, prop.PropertyType, currentDepth + 1));
                        referenceDict.Add(parentKey++, propTypeDepthException);
                    }
                }
            }

            #endregion

            //所有被选取的值类型
            IList<ValueTypeMapping> selectedValueList = SelectedValueProperties(valueList, hasMapping);
            IList<ValueTypeMapping> filteredValueList = FilterValuePropertiesByDbColumn(selectedValueList);
            return
                new Tuple<IDictionary<int, ReferenceTypeDepthExpression>, IList<ValueTypeMapping>>(referenceDict,filteredValueList);
        }

        /// <summary>
        ///     对所有的值类型与stirng类型进行“格式化”，并生成相应的属性到Column的映射关系。
        ///     在此：只是简单的将属性映射到Column，不考虑两者多对多的关系的判断。
        /// </summary>
        /// <param name="valueTypeMappings"></param>
        /// <param name="hasMapping"></param>
        /// <returns></returns>
        private IList<ValueTypeMapping> SelectedValueProperties(IList<ValueTypeMapping> valueTypeMappings,
            bool hasMapping)
        {
            var rightTypeMappings = new List<ValueTypeMapping>();


            if (hasMapping)
            {
                #region 有属性存在DbColumnMapping

                foreach (ValueTypeMapping valueTypeMapping in valueTypeMappings)
                {
                    #region format DBColumnMappings

                    var attrs =
                        (DBColumnMapping[])
                            valueTypeMapping.CurrentPropertyInfo.GetCustomAttributes(typeof (DBColumnMapping), true);

                    DBColumnMapping[] destAttrs = attrs.Where(attr => attr.DestLabel == m_destLabel).ToArray();

                    DBColumnMapping[] selectedAttrs =
                        destAttrs.Where(attr => attr.IsTableNameMatch(m_destinationTablename)).ToArray();

                    //说明：如果已经有满足数据库表匹配的Attribute存在，则选择这些Attribute；
                    //否则，选择数据库表名称为默认值（即null）的Attribute。
                    selectedAttrs = selectedAttrs.Length != 0
                        ? selectedAttrs
                        : destAttrs.Where(attr => attr.IsDefaultDestTableName()).ToArray();

                    if (selectedAttrs.Length == 0) continue;

                    for (int i = 0; i < selectedAttrs.Length; ++i)
                    {
                        LogHelper.Logger.TraceFormat("starting format DBColumnMapping: {0} for property: {1}",
                            selectedAttrs[i], valueTypeMapping.CurrentPropertyInfo);
                        FormatDbColumnMapping(valueTypeMapping.CurrentPropertyInfo, ref selectedAttrs[i]);
                        LogHelper.Logger.TraceFormat("ending format DBColumnMapping:{0} for property:{1}",
                            selectedAttrs[i], valueTypeMapping.CurrentPropertyInfo);
                        if (selectedAttrs[i].IsDestColumnOffsetOk() == false ||
                            selectedAttrs[i].IsDestColumnNameOk() == false)
                        {
                            LogHelper.Logger.WarnFormat(
                                "failed to format DBColumnMapping:{0} for prop:{1}. check whether its Attribute is mapping to database table correctly. and it's not added to mapping table.",
                                selectedAttrs[i], valueTypeMapping.CurrentPropertyInfo);
                        }
                    }

                    #endregion

                    DBColumnMapping[] rightAttrs =
                        selectedAttrs.Where(attr => attr.IsDestColumnOffsetOk() && attr.IsDestColumnNameOk()).ToArray();
                    if (rightAttrs.Length == 0)
                    {
                        LogHelper.Logger.WarnFormat("mapping to column failed for property:{0}",
                            valueTypeMapping.CurrentPropertyInfo);
                        continue;
                    }


                    foreach (DBColumnMapping rightAttr in rightAttrs)
                    {
                        rightTypeMappings.Add(new ValueTypeMapping(valueTypeMapping.CurrentPropertyInfo,
                            valueTypeMapping.ParentKey, valueTypeMapping.Depth, rightAttr));
                    }
                }

                #endregion

                return rightTypeMappings;
            }

            //there isn't property with DestLabel attribute,so we can get it from database table.
            LogHelper.Logger.WarnFormat(
                "mapping property by schema table for current type: {0}, which do not have attribute on all properties.",
                typeof (T));

            #region 没有属性存在DbColumnMapping。因此，采用属性名称匹配数据库

            DataTable dataTable = GetSchemaTable();

            foreach (DataColumn column in dataTable.Columns)
            {
                if (column == null || column.ReadOnly) continue;

                IEnumerable<ValueTypeMapping> tempMappings =
                    valueTypeMappings.Where(
                        t =>
                            string.Equals(t.CurrentPropertyInfo.Name, column.ColumnName,
                                StringComparison.OrdinalIgnoreCase));
                ValueTypeMapping[] typeMappings = tempMappings as ValueTypeMapping[] ?? tempMappings.ToArray();
                foreach (ValueTypeMapping typeMapping in typeMappings)
                {
                    var dbMapping = new DBColumnMapping(m_destLabel, column.Ordinal, null, m_destinationTablename)
                    {
                        DestColumnName = column.ColumnName
                    };
                    typeMapping.DbColumnMapping = dbMapping;
                    rightTypeMappings.Add(typeMapping);
                }
            }

            #endregion

            return rightTypeMappings;
        }

        /// <summary>
        ///     说明：此方法用于利用数据库表的列字段的：位置或名称，将DbColumnMapping补全。
        ///     如果输入的propertyInfo.PropertyType不是“原子类型”或“String”，显然在数据库中不会有匹配的列；所以直接返回
        /// </summary>
        /// <param name="propertyInfo"></param>
        /// <param name="mapping"></param>
        private void FormatDbColumnMapping(PropertyInfo propertyInfo, ref DBColumnMapping mapping)
        {
            Type type = propertyInfo.PropertyType;
            bool tag = propertyInfo.PropertyType.IsValueType || propertyInfo.PropertyType == typeof (string);
            if (tag == false) return;

            if (mapping.IsDestColumnNameOk() && mapping.IsDestColumnOffsetOk()) return;

            DataTable schemaTable = GetSchemaTable();
            //说明当前的mapping的列名称出错（null），而位置参数正确。则读取数据库表获得要相应的列名称
            if (mapping.IsDestColumnNameOk() == false && mapping.IsDestColumnOffsetOk())
            {
                DataColumn col = schemaTable.Columns[mapping.DestColumnOffset];
                if (col == null)
                {
                    LogHelper.Logger.WarnFormat(
                        "can not find column with column offset:{0} with property name:{1} in table: {2}, currnet db mapping:{3}",
                        mapping.DestColumnOffset, propertyInfo.Name, m_destinationTablename, mapping);
                    return;
                }
                mapping.DestColumnName = col.ColumnName;
                return;
            }

            //说明当前的mapping的列名称存在，而位置参数出错（-1）。则读取数据库表获得相应的列位置参数
            if (mapping.IsDestColumnNameOk() && mapping.IsDestColumnOffsetOk() == false)
            {
                DataColumn col = schemaTable.Columns[mapping.DestColumnName];
                if (col == null)
                {
                    LogHelper.Logger.WarnFormat(
                        "can not find column with column name:{0} with property name:{1} in table:{2}, current db mapping:{3}",
                        mapping.DestColumnName, propertyInfo.Name, m_destinationTablename, mapping);
                    return;
                }

                mapping.DestColumnOffset = col.Ordinal;
                return;
            }

            //说明当前的mapping列名称不存在，位置参数也不存在，因此，根据PropertyInfo.Name读取数据库
            DataColumn col2 = schemaTable.Columns[propertyInfo.Name];
            if (col2 == null)
            {
                LogHelper.Logger.WarnFormat(
                    "can not find column with property name:{0} in table:{1}, current db mapping:{2}", propertyInfo.Name,
                    m_destinationTablename, mapping);
                return;
            }
            mapping.DestColumnOffset = col2.Ordinal;
            mapping.DestColumnName = col2.ColumnName;
        }

        /// <summary>
        ///     判断当前的属性类型是否与已经存在的形成“闭环”。即
        ///     A.B.A。 当前的最后一个A，此时形成闭环
        /// </summary>
        /// <param name="dicts"></param>
        /// <param name="current"></param>
        /// <returns></returns>
        private bool HasReferenceLoop(Dictionary<int, ReferenceTypeDepthExpression> dicts,
            ReferenceTypeDepthExpression current)
        {

            ReferenceTypeDepthExpression parent = null;
            if (dicts.TryGetValue(current.ParentKey, out parent) == false)
                return false;

            while (parent!=null)
            {
                if (current.PropertyInfo.PropertyType == parent.PropertyInfo.PropertyType) return true;
                if (dicts.TryGetValue(parent.ParentKey, out parent) == false)
                    return false;
            }
            return false;
        }

        /// <summary>
        ///     利用DbColumn的信息，去除匹配到相同列的多余属性，只选择其中一个。选择的规定为：
        ///     1、当只有一个属性匹配时，则选择之；
        ///     2、当有多个属性匹配，则深度不同时，选择深度最小者。
        ///     如A.B.C.D, A.B.D。则选择后者
        ///     3、如果有多个属性匹配，且最小深度有多个，则默认选择第一个。
        /// </summary>
        /// <param name="sourceMappings"></param>
        /// <returns></returns>
        private IList<ValueTypeMapping> FilterValuePropertiesByDbColumn(IList<ValueTypeMapping> sourceMappings)
        {
            var filtered = new List<ValueTypeMapping>();
            foreach (var group in sourceMappings.GroupBy(t => t.DbColumnMapping.DestColumnOffset))
            {
                //规则一
                if (group.Count() == 1)
                {
                    ValueTypeMapping first = group.FirstOrDefault();
                    filtered.Add(first);
                }
                else
                {
                    //规则二、三
                    int minDepth = group.Min(t => t.Depth);
                    var first = group.FirstOrDefault(t => t.Depth == minDepth);
                    if (first == null)
                    {
                        LogHelper.Logger.ErrorFormat(
                            "failed to find mapping property for column with columnOffset:{0}", group.Key);
                        continue;
                    }
                    filtered.Add(first);
                    LogHelper.Logger.WarnFormat(
                        "there are more than one property mapping to the same column with columnOffset:{0}, columnName:{1}",first.DbColumnMapping.DestColumnOffset, first.DbColumnMapping.DestColumnName);
                }
            }
            return filtered;
        }

        #endregion

        #region instance public properties, methods

        public string CurrentDestLabel
        {
            get { return m_destLabel; }
        }

        public string ConnectionString
        {
            get { return m_connectionString; }
        }

        public string DestinationTableName
        {
            get { return m_destinationTablename; }
        }

        public List<SqlBulkCopyColumnMapping> ColumnMappings
        {
            get { return m_properties.Keys.Select(prop => new SqlBulkCopyColumnMapping(prop, prop)).ToList(); }
        }

        public int FieldCount
        {
            get { return m_properties.Count; }
        }

        public Func<T, dynamic> GetPropertyAccessor(int columnOffset)
        {
            return m_properties[columnOffset];
        }

        public string GetName(int columnOffset)
        {
            DBColumnMapping columnAttr = m_dbColumnMappings.FirstOrDefault(col => col.DestColumnOffset == columnOffset);
            return columnAttr == null ? null : columnAttr.DestColumnName;
        }

        public int GetColumnOffset(string name)
        {
            DBColumnMapping columnAttr =
                m_dbColumnMappings.FirstOrDefault(
                    col => string.Equals(col.DestColumnName, name, StringComparison.OrdinalIgnoreCase));
            return columnAttr == null ? -1 : columnAttr.DestColumnOffset;
        }

        public DBColumnMapping GetColumnMapping(int colId)
        {
            return m_dbColumnMappings.FirstOrDefault(b => b.DestColumnOffset == colId);
        }

        #endregion
    }


    /// <summary>
    ///     用于存放一个Property在当前的类结构中的深度及表达式
    /// </summary>
    public class ReferenceTypeDepthExpression
    {
        public ReferenceTypeDepthExpression(PropertyInfo propertyInfo, int parentKey, int depth,
            Expression expression = null)
        {
            PropertyInfo = propertyInfo;
            ParentKey = parentKey;
            Depth = depth;
            Expression = expression;
        }

        public PropertyInfo PropertyInfo { get; set; }

        /// <summary>
        /// 父类型对应的节点Key
        /// </summary>
        public int ParentKey { get; set; }

        public int Depth { get; set; }
        public Expression Expression { get; set; }
    }

    /// <summary>
    ///     用于存放一个值类型或String类型的DbColumnMapping
    /// </summary>
    public class ValueTypeMapping
    {
        public ValueTypeMapping(PropertyInfo currentPropertyInfo, int parentKey, int depth,
            DBColumnMapping dbColumnMapping = null)
        {
            CurrentPropertyInfo = currentPropertyInfo;
            ParentKey = parentKey;
            Depth = depth;
            DbColumnMapping = dbColumnMapping;
        }

        public PropertyInfo CurrentPropertyInfo { get; set; }

        /// <summary>
        /// 父类型对应的节点Key
        /// </summary>
        public int ParentKey { get; set; }
        public int Depth { get; set; }
        public DBColumnMapping DbColumnMapping { get; set; }
    }
}