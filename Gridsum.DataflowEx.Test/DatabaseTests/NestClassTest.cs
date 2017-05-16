using System;
using System.Data.SqlClient;
using System.Linq;
using Gridsum.DataflowEx.Databases;
using Microsoft.EntityFrameworkCore;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gridsum.DataflowEx.Test.DatabaseTests
{
    public class A
    {
        public int Id { get; set; }

        [DBColumnMapping("NestClassTest", "Value1", "-")]
        public string Value1 { get; set; }
    }

    public class AEF : A
    {
        public string Value2 { get; set; }
    }

    public class ABulk : A
    {
        public B B { get; set; }
    }

    public class B
    {
        [DBColumnMapping("NestClassTest", "Value2", "--")]
        public string Value2 { get; set; }
    }
    
    [TestClass]
    public class NestClassTest
    {
        [TestMethod]
        public void TestNestClass()
        {
            var connectString = TestUtils.GetLocalDBConnectionString();
            var context = new NestContext(connectString);
            context.NestInits.Add(new NestInit());
            context.SaveChanges();

            var a1 = new ABulk {};
            var a2 = new ABulk {B = new B { } };
            var a3 = new ABulk {B = new B { Value2 = "a" } };
            var a4 = new ABulk {Value1 = "a", B = new B { Value2 = "a" } };
            
            using (var connection = new SqlConnection(connectString))
            {
                connection.Open();
                var aTypeAccessor = TypeAccessorManager<ABulk>.GetAccessorForTable(new TargetTable("NestClassTest", connectString, "dbo.AEFs"));
                var aReader = new BulkDataReader<ABulk>(aTypeAccessor, new[] { a1, a2, a3, a4 });
                using (var aCopy = new SqlBulkCopy(connection))
                {
                    foreach (var map in aReader.ColumnMappings)
                    {
                        aCopy.ColumnMappings.Add(map);
                    }
                    aCopy.DestinationTableName = "dbo.AEFs";
                    aCopy.WriteToServer(aReader);
                }
            }

            Assert.AreEqual(1, context.AAs.FirstOrDefault().Id);
            Assert.AreEqual(1, context.AAs.Count(a => a.Value1 == "a"));
            Assert.AreEqual(3, context.AAs.Count(a => a.Value1 == "-"));
            Assert.AreEqual(2, context.AAs.Count(a => a.Value2 == "a"));
            Assert.AreEqual(2, context.AAs.Count(a => a.Value2 == "--"));
        }
    }

    public class NestContext : DbContextBase<NestContext>
    {
        public NestContext(string config)
            : base(config)
        {
        }

        public DbSet<NestInit> NestInits { get; set; }
        public DbSet<AEF> AAs { get; set; }
    }

    public class NestInit
    {
        public int Id { get; set; }
    }
}
