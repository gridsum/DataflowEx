using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Test
{
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Reflection;

    public static class TestUtils
    {
        public static async Task<bool> FinishesIn(this Task t,TimeSpan ts)
        {
            var timeOutTask = Task.Delay(ts);
            return t == await Task.WhenAny(t, timeOutTask);
        }

        public static SqlConnection GetLocalDB(string dbName)
        {
            using (var conn = new SqlConnection(GetLocalDBConnectionString("master", false)))
            {
                conn.Open();
                var command = conn.CreateCommand();
                command.CommandText = $"IF db_id('DataflowEx-TestDB-{dbName}') is null begin CREATE DATABASE [DataflowEx-TestDB-{dbName}] end;";
                command.ExecuteNonQuery();
            }

            var conn2 = new SqlConnection(GetLocalDBConnectionString(dbName));
            conn2.Open();
            return conn2;
        }

        public static string GetLocalDBConnectionString(string dbName = null, bool addPrefix = true)
        {
            if (dbName == null)
            {
                var callingMethod = new StackTrace().GetFrame(1).GetMethod() as MethodInfo;
                dbName = callingMethod.DeclaringType.Name + "-" + callingMethod.Name;
            }

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = "localhost";   // update me
            builder.UserID = "sa";              // update me
            builder.Password = "UpLow123-+";      // update me
            builder.InitialCatalog = addPrefix ? $"DataflowEx-TestDB-{dbName}" : dbName;
            return builder.ConnectionString;

            AppDomain.CurrentDomain.SetData("DataDirectory", AppDomain.CurrentDomain.BaseDirectory);
            var connectString = string.Format(
@"Data Source=(LocalDB)\v11.0;AttachDbFilename=|DataDirectory|\TestDB-{0}.mdf;Initial Catalog={0};Integrated Security=True;Connect Timeout=30",
               dbName );
            return connectString;
        }

        public static byte[] ToByteArray(this string s)
        {
            return Encoding.UTF8.GetBytes(s);
        }

        public static bool ArrayEqual(byte[] a, byte[] b)
        {
            if (a.Length == b.Length)
            {
                for (int i = 0; i < a.Length; i++)
                {
                    if (a[i] != b[i]) return false;
                }

                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
