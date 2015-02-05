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
            string connStr = CreatePeopleTable(1);

            var f = new PeopleFlow(DataflowOptions.Default);

            TypeAccessorConfig.RegisterMapping<Person, string>(p => p.Name, new DBColumnMapping("PersonTarget2", "NameCol", "N/A2"));
            TypeAccessorConfig.RegisterMapping<Person, int?>(p => p.Age, new DBColumnMapping("PersonTarget2", "AgeCol", -2));

            var dbInserter = new DbBulkInserter<Person>(connStr, "dbo.People", DataflowOptions.Default, "PersonTarget2");
            f.LinkTo(dbInserter);

            f.Post("{Name: 'aaron', Age: 20}");
            f.Post("{Name: 'bob', Age: 30}");
            f.Post("{Age: 80}"); //Name will be default value: "N/A2"
            f.Post("{Name: 'neo' }"); // Age will be default value: -2
            await f.SignalAndWaitForCompletionAsync();
            await dbInserter.CompletionTask;

            using (var conn = LocalDB.GetLocalDB("ExternalMappingTest1"))
            {
                Assert.AreEqual(4, conn.ExecuteScalar<int>("select count(*) from dbo.People"));
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.People where NameCol = 'N/A2'"));
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.People where AgeCol = -2"));
            }
        }

        

        [TestMethod]
        public async Task TestPersonInsertion2()
        {
            string connStr = CreatePeopleTable(2);
            
            var f = new PeopleFlow(DataflowOptions.Default);

            //will NOT take effect
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

            using (var conn = LocalDB.GetLocalDB("ExternalMappingTest2"))
            {
                Assert.AreEqual(4, conn.ExecuteScalar<int>("select count(*) from dbo.People"));
                Assert.AreEqual(0, conn.ExecuteScalar<int>("select count(*) from dbo.People where NameCol = 'N/A2'"));
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.People where NameCol = 'N/A'"));
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.People where AgeCol = -2"));
            }
        }

        [TestMethod]
        public async Task TestPersonInsertion3()
        {
            string connStr = CreatePeopleTable(3);

            var f = new PeopleFlow(DataflowOptions.Default);

            //will take effect
            TypeAccessorConfig.RegisterMapping<Person, string>(p => p.Name, new DBColumnMapping("PersonTarget", "NameCol", "N/A2", ColumnMappingOption.Overwrite));
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

            using (var conn = LocalDB.GetLocalDB("ExternalMappingTest3"))
            {
                Assert.AreEqual(4, conn.ExecuteScalar<int>("select count(*) from dbo.People"));
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.People where NameCol = 'N/A2'"));
                Assert.AreEqual(0, conn.ExecuteScalar<int>("select count(*) from dbo.People where NameCol = 'N/A'"));
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.People where AgeCol = -2"));
            }
        }

        [TestMethod]
        public async Task TestOrderInsertion()
        {
            string connStr;

            //initialize table
            using (var conn = LocalDB.GetLocalDB("ExternalMappingTest"))
            {
                var cmd = new SqlCommand(@"
                IF OBJECT_id('dbo.Orders', 'U') IS NOT NULL
                    DROP TABLE dbo.Orders;
                
                CREATE TABLE dbo.Orders
                (
                    Id INT IDENTITY(1,1) NOT NULL,
                    Date DATETIME NOT NULL,
                    Value FLOAT NOT NULL,
                    CustomerName NVARCHAR(50) NOT NULL,
                    CustomerAge INT NOT NULL
                )
                ", conn);
                cmd.ExecuteNonQuery();
                connStr = conn.ConnectionString;
            }

            //will take effect
            TypeAccessorConfig.RegisterMapping<Order, string>(p => p.Customer.Name, new DBColumnMapping("OrderTarget", "CustomerName", "N/A2"));
            //will take effect
            TypeAccessorConfig.RegisterMapping<Order, int?>(p => p.Customer.Age, new DBColumnMapping("OrderTarget", "CustomerAge", -99));

            var dbInserter = new DbBulkInserter<Order>(connStr, "dbo.Orders", DataflowOptions.Default, "OrderTarget", bulkSize: 3);

            dbInserter.Post(new Order { OrderDate = DateTime.Now, OrderValue = 15, Customer = new Person() { Name = "Aaron", Age = 38 } });
            dbInserter.Post(new Order { OrderDate = DateTime.Now, OrderValue = 25, Customer = new Person() { Name = "Bob", Age = 30 } });
            dbInserter.Post(new Order { OrderDate = DateTime.Now, OrderValue = 35, Customer = new Person() { Age = 48 } });
            dbInserter.Post(new Order { OrderDate = DateTime.Now, OrderValue = 45, Customer = new Person() { Name = "Neo" } });

            await dbInserter.SignalAndWaitForCompletionAsync();

            using (var conn = LocalDB.GetLocalDB("ExternalMappingTest"))
            {
                Assert.AreEqual(4, conn.ExecuteScalar<int>("select count(*) from dbo.Orders"));
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.Orders where CustomerAge = 48 and CustomerName = 'N/A2'"));
                Assert.AreEqual(0, conn.ExecuteScalar<int>("select count(*) from dbo.Orders where CustomerName = 'N/A'"));
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.Orders where CustomerName = 'Neo' and CustomerAge = -99"));
            }
        }

        public static string CreatePeopleTable(int i)
        {
            string connStr;
            //initialize table
            using (var conn = LocalDB.GetLocalDB("ExternalMappingTest" + i))
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
            return connStr;
        }
    }
}
