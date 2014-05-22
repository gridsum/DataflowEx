using System;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using Gridsum.DataflowEx.Database;
using Gridsum.DataflowEx.Databases;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gridsum.DataflowEx.Test.DatabaseTests
{
    public class A
    {
        public int AId { get; set; }
        public B B { get; set; }
    }

    public class B
    {
        public int BId { get; set; }

        public A A { get; set; }
    }


    [TestClass]
    public class NestClassTest
    {
        [TestMethod]
        public void TestNestClass()
        {
            AppDomain.CurrentDomain.SetData("DataDirectory", AppDomain.CurrentDomain.BaseDirectory);
            var connectString =
                @"Data Source=(LocalDB)\v11.0;AttachDbFilename=|DataDirectory|\nestClassTest.mdf;Initial Catalog=nestClassTest;Integrated Security=True;Connect Timeout=30";

            System.Data.Entity.Database.SetInitializer(new DropCreateDatabaseAlways<NestContext>());
            var context = new NestContext(connectString);
            context.NestInits.Add(new NestInit());
            context.SaveChanges();

            var a = new A
            {
                AId = 1,
                B = new B
                {
                    BId = 2,
                    A = new A { AId = 3 }
                }
            };

            var b = new B
            {
                BId = 1,
                A = new A
                {
                    AId = 2,
                    B = new B { BId = 3 }
                }
            };

            using (var connection = new SqlConnection(connectString))
            {
                connection.Open();
                var aTypeAccessor = TypeAccessorManager<A>.GetAccessorByDestLabel("test1", connectString, "dbo.AAs");
                var aReader = new BulkDataReader<A>(aTypeAccessor, new[] { a });
                using (var aCopy = new SqlBulkCopy(connection))
                {
                    foreach (var map in aReader.ColumnMappings)
                    {
                        aCopy.ColumnMappings.Add(map);
                    }
                    aCopy.DestinationTableName = "dbo.AAs";
                    aCopy.WriteToServer(aReader);
                }

                var bTypeAccessor = TypeAccessorManager<B>.GetAccessorByDestLabel("test2", connectString, "dbo.BBs");
                var bReader = new BulkDataReader<B>(bTypeAccessor, new[] { b });
                using (var bCopy = new SqlBulkCopy(connection))
                {
                    foreach (var map in bReader.ColumnMappings)
                    {
                        bCopy.ColumnMappings.Add(map);
                    }
                    bCopy.DestinationTableName = "dbo.BBs";
                    bCopy.WriteToServer(aReader);
                }
            }

            Assert.AreEqual(1, context.AAs.FirstOrDefault().AId);
            Assert.AreEqual(1, context.BBs.FirstOrDefault().BId);
        }
    }

    public class NestContext : DbContext
    {
        public NestContext(string config)
            : base(config)
        {
        }

        public DbSet<NestInit> NestInits { get; set; }
        public DbSet<AA> AAs { get; set; }

        public DbSet<BB> BBs { get; set; }
    }

    public class NestInit
    {
        public int Id { get; set; }
    }

    public class AA
    {
        public int Id { get; set; }
        public int AId { get; set; }
    }

    public class BB
    {
        public int Id { get; set; }
        public int BId { get; set; }
    }
}
