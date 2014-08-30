using System;
using System.Linq;

namespace Gridsum.DataflowEx
{
    using System.Data;
    using System.Text;
    using System.Threading.Tasks;

    internal static class Utils
    {
        public static string GetFriendlyName(this Type type)
        {
            if (type.IsGenericType)
                return string.Format("{0}<{1}>", type.Name.Split('`')[0], string.Join(", ", type.GetGenericArguments().Select(GetFriendlyName)));
            else
                return type.Name;
        }

        public static bool IsNullableType(this Type type)
        {
            Type tmp;
            return IsNullableType(type, out tmp);
        }

        public static bool IsNullableType(this Type type, out Type innerValueType)
        {
            innerValueType = null;
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                innerValueType = Nullable.GetUnderlyingType(type);
                return true;
            }
            return false;
        }

        public static string FlattenColumnNames(DataColumnCollection columns, string tableName)
        {
            var sb = new StringBuilder();
            sb.Append('(');
            foreach (DataColumn column in columns)
            {
                sb.Append(tableName);
                sb.Append('.');
                sb.Append(column.ColumnName);
                sb.Append(',');
            }

            sb.Remove(sb.Length - 1, 1);
            sb.Append(')');
            return sb.ToString();
        }

        public static DataColumn GetAutoIncrementColumn(DataColumnCollection columns)
        {
            return columns.OfType<DataColumn>().First(c => c.AutoIncrement);
        }
    }
}