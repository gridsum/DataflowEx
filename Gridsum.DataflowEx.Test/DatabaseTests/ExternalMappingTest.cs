using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;

namespace Gridsum.DataflowEx.Test.DatabaseTests
{
    using System.Data.SqlClient;

    using Gridsum.DataflowEx.Databases;
    using Gridsum.DataflowEx.Demo;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ExternalMappingTest
    {
        [TestMethod]
        public async Task TestPersonInsertion()
        {
            string connStr;

            //initialize table
            using (var conn = LocalDB.GetLocalDB("ExternalMappingTest"))
            {
                var cmd = new SqlCommand(@"
                IF OBJECT_id('dbo.People', 'U') IS NOT NULL
                    DROP TABLE dbo.People;
                
                CREATE TABLE dbo.People
                (
                    Id INT IDENTITY(1,1) NOT NULL,
                    NameCol nvarchar(50) NOT NULL,
                    AgeCol INT           NOT NULL
                )
                ", conn);
                cmd.ExecuteNonQuery();
                connStr = conn.ConnectionString;
            }

            var f = new PeopleFlow(DataflowOptions.Default);

            TypeAccessorConfig.RegisterMapping<Person, string>(p => p.Name, new DBColumnMapping("PersonTarget2", "NameCol", "N/A2"));
            TypeAccessorConfig.RegisterMapping<Person, int?>(p => p.Age, new DBColumnMapping("PersonTarget2", "AgeCol", -2));

            var dbInserter = new DbBulkInserter<Person>(connStr, "dbo.People", DataflowOptions.Default, "PersonTarget2");
            f.LinkTo(dbInserter);

            f.Post("{Name: 'aaron', Age: 20}");
            f.Post("{Name: 'bob', Age: 30}");
            f.Post("{Age: 80}"); //Name will be default value: "N/A"
            f.Post("{Name: 'neo' }"); // Age will be default value: -2
            await f.SignalAndWaitForCompletionAsync();
            await dbInserter.CompletionTask;

            using (var conn = LocalDB.GetLocalDB("ExternalMappingTest"))
            {
                Assert.AreEqual(4, conn.ExecuteScalar<int>("select count(*) from dbo.People"));
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.People where NameCol = 'N/A2'"));
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.People where AgeCol = -2"));
            }
        }

        [TestMethod]
        public async Task TestPersonInsertion2()
        {
            string connStr;

            //initialize table
            using (var conn = LocalDB.GetLocalDB("ExternalMappingTest"))
            {
                var cmd = new SqlCommand(@"
                IF OBJECT_id('dbo.People', 'U') IS NOT NULL
                    DROP TABLE dbo.People;
                
                CREATE TABLE dbo.People
                (
                    Id INT IDENTITY(1,1) NOT NULL,
                    NameCol nvarchar(50) NOT NULL,
                    AgeCol INT           NOT NULL
                )
                ", conn);
                cmd.ExecuteNonQuery();
                connStr = conn.ConnectionString;
            }

            var f = new PeopleFlow(DataflowOptions.Default);

            //will not take effect
            TypeAccessorConfig.RegisterMapping<Person, string>(p => p.Name, new DBColumnMapping("PersonTarget", "NameCol", "N/A2"));

            //will take effect
            TypeAccessorConfig.RegisterMapping<Person, int?>(p => p.Age, new DBColumnMapping("PersonTarget", "AgeCol", -2));

            var dbInserter = new DbBulkInserter<Person>(connStr, "dbo.People", DataflowOptions.Default, "PersonTarget");
            f.LinkTo(dbInserter);

            f.Post("{Name: 'aaron', Age: 20}");
            f.Post("{Name: 'bob', Age: 30}");
            f.Post("{Age: 80}"); //Name will be default value: "N/A"
            f.Post("{Name: 'neo' }"); // Age will be default value: -2
            await f.SignalAndWaitForCompletionAsync();
            await dbInserter.CompletionTask;

            using (var conn = LocalDB.GetLocalDB("ExternalMappingTest"))
            {
                Assert.AreEqual(4, conn.ExecuteScalar<int>("select count(*) from dbo.People"));
                Assert.AreEqual(0, conn.ExecuteScalar<int>("select count(*) from dbo.People where NameCol = 'N/A2'"));
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.People where NameCol = 'N/A'"));
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.People where AgeCol = -2"));
            }
        }
    }
}
