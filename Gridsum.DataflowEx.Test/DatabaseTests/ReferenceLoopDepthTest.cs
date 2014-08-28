using System;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using Gridsum.DataflowEx.Databases;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gridsum.DataflowEx.Test.DatabaseTests
{
    public class LoopBase
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    public class LoopA : LoopBase
    {
        public LoopB LoopB { get; set; }
    }

    public class LoopB : LoopBase
    {
        public LoopC LoopC { get; set; }
    }

    public class LoopC
    {
        public LoopA LoopA { get; set; }
    }
    /// <summary>
    /// 主要测试当对象引用有闭环时，以及多个属性同时对应到数据库列时。是否能正常
    /// </summary>

    [TestClass]
    public class ReferenceLoopDepthTest
    {
        [TestMethod]
        public void TestReferenceLoopDepth()
        {
            var connectString = TestUtils.GetLocalDBConnectionString();
            //init database
            Database.SetInitializer(new DropCreateDatabaseAlways<LoopContext>());
            var context = new LoopContext(connectString);
            context.LoopInits.Add(new LoopInit());
            context.SaveChanges();

            var loopA = new LoopA
            {
                Name = "loopA",
                LoopB = new LoopB
                {
                    Name = "loopB",
                    LoopC = new LoopC
                    {
                        LoopA = new LoopA
                        {
                            Name = "loopA2"
                        }
                    }
                }
            };

            using (var connection = new SqlConnection(connectString))
            {
                connection.Open();

                var tableName = "dbo.LoopEntities";
                var accessor = TypeAccessorManager<LoopA>.GetAccessorForTable(new TargetTable(GSProduct.AD, connectString, tableName));
                var reader = new BulkDataReader<LoopA>(accessor, new[] { loopA });
                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    foreach (SqlBulkCopyColumnMapping map in reader.ColumnMappings)
                    {
                        bulkCopy.ColumnMappings.Add(map);
                    }
                    bulkCopy.DestinationTableName = tableName;

                    bulkCopy.WriteToServer(reader);
                }
            }

            Assert.AreEqual("loopA", context.LoopEntities.FirstOrDefault().Name);

        }
    }


    public class LoopContext : DbContext
    {
        public LoopContext(string con) : base(con) { }

        public DbSet<LoopInit> LoopInits { get; set; }

        public DbSet<LoopEntity> LoopEntities { get; set; }
    }

    public class LoopInit
    {
        public int Id { get; set; }
    }

    public class LoopEntity
    {
        public int Id { get; set; }

        public string Name { get; set; }

    }
}
