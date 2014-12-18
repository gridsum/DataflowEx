using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Test.DatabaseTests
{
    using Dapper;

    using Gridsum.DataflowEx.Databases;
    using Gridsum.DataflowEx.Demo;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DbColumnPathTest
    {
        public class PersonEx
        {
            [DBColumnMapping("PersonTarget", "NameCol", "N/A", ColumnMappingOption.Optional)]
            public string Name { get; set; }

            [DBColumnMapping("PersonTarget", "AgeCol", -1, ColumnMappingOption.Optional)]
            public int? Age { get; set; }
            
            [DBColumnPath(DBColumnPathOptions.DoNotGenerateNullCheck)]
            [DBColumnPath(DBColumnPathOptions.DoNotExpand)]
            public Person Me { get; set; }
        }

        public class PersonEx2
        {
            [DBColumnMapping("PersonTarget", "NameCol", "N/A", ColumnMappingOption.Optional)]
            public string Name { get; set; }

            [DBColumnMapping("PersonTarget", "AgeCol", -1, ColumnMappingOption.Optional)]
            public int? Age { get; set; }

            [DBColumnPath(DBColumnPathOptions.DoNotGenerateNullCheck)]
            public Person Me { get; set; }
        }

        public class Person
        {
            [DBColumnMapping("PersonTarget", "NameCol", "N/A", ColumnMappingOption.Overwrite)]
            public string Name { get; set; }

            [DBColumnMapping("PersonTarget", "AgeCol", -1, ColumnMappingOption.Overwrite)]
            public int? Age { get; set; }
        }

        //should access level 1
        [TestMethod]
        public async Task TestDoNoExpand()
        {
            string connStr = ExternalMappingTest.CreatePeopleTable(4);

            var dbInserter = new DbBulkInserter<PersonEx>(connStr, "dbo.People", DataflowOptions.Default, "PersonTarget");

            dbInserter.Post(new PersonEx { Age = 10, Name = "Aaron", Me = new Person { Age = 20, Name = "Bob" } });
            await dbInserter.SignalAndWaitForCompletionAsync();

            using (var conn = LocalDB.GetLocalDB("ExternalMappingTest4"))
            {
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.People"));
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.People where NameCol = 'Aaron'"));
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.People where AgeCol = 10"));
            }
        }

        //should access level 2
        [TestMethod]
        public async Task TestDoNoExpand2()
        {
            string connStr = ExternalMappingTest.CreatePeopleTable(4);

            var dbInserter = new DbBulkInserter<PersonEx2>(connStr, "dbo.People", DataflowOptions.Default, "PersonTarget");

            dbInserter.Post(new PersonEx2 { Age = 10, Name = "Aaron", Me = new Person { Age = 20, Name = "Bob" } });
            await dbInserter.SignalAndWaitForCompletionAsync();

            using (var conn = LocalDB.GetLocalDB("ExternalMappingTest4"))
            {
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.People"));
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.People where NameCol = 'Bob'"));
                Assert.AreEqual(1, conn.ExecuteScalar<int>("select count(*) from dbo.People where AgeCol = 20"));
            }
        }
    }
}
