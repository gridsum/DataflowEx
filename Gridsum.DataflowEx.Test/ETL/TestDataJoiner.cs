using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Test.ETL
{
    using System.Data;
    using System.Data.Entity;
    using System.Diagnostics;
    using System.Threading.Tasks.Dataflow;

    using Gridsum.DataflowEx.Databases;
    using Gridsum.DataflowEx.ETL;
    using Gridsum.DataflowEx.Test.DatabaseTests;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class TestDataJoiner
    {
        [TestMethod]
        public async Task TestDataJoinerJoining()
        {
            Database.SetInitializer(new DropCreateDatabaseAlways<InsertContext>());
            var connectString = TestUtils.GetLocalDBConnectionString("TestDataJoinerJoining");
            var context = new InsertContext(connectString);

            for (int i = 0; i < 10; i++)
            {
                context.Leafs.Add(new Leaf() {IntValue = i, Key = i.ToString(), StrValue = "Str" + i});    
            }
            
            context.SaveChanges();

            var joiner = new DbDataJoiner<Trunk, string>(
                t => t.Pointer,
                new TargetTable(Leaf.DimLeaf, connectString, "Leaves"),
                8192);

            int count = 0;
            var asserter =
                new ActionBlock<KeyValuePair<Trunk, DataRowView>>(
                    delegate(KeyValuePair<Trunk, DataRowView> pair)
                        {
                            Assert.AreEqual(pair.Key.Pointer, pair.Value["Key"]);
                            Assert.AreEqual("Str" + pair.Key.Pointer, pair.Value["StrValue"]);
                            count ++;
                        }).ToDataflow();

            asserter.Name = "asserter";
            
            joiner.LinkTo(asserter);

            var dataArray = new[]
                                {
                                    new Trunk() { Pointer = "3" }, 
                                    new Trunk() { Pointer = "5" },
                                    new Trunk() { Pointer = "7" }, 
                                    new Trunk() { Pointer = "7" },
                                    new Trunk() { Pointer = "9" },
                                    new Trunk() { Pointer = "99", Leaf = new Leaf() { StrValue = "Str99" } },
                                    new Trunk() { Pointer = "99", Leaf = new Leaf() { StrValue = "Str99" } },
                                };

            await joiner.ProcessAsync(dataArray);

            await asserter.CompletionTask;

            Assert.AreEqual(dataArray.Length, count);
        }
    }

    public class Trunk
    {
        public int Id { get; set; }

        [DBColumnMapping(Leaf.DimLeaf, "Key")]
        public string Pointer { get; set; }

        public string Name { get; set; }
        public Leaf Leaf { get; set; }
    }

    public class Leaf
    {
        public const string DimLeaf = "DimLeaf";

        public int Id { get; set; }

        [DBColumnMapping(DimLeaf)]
        public string Key { get; set; }

        [DBColumnMapping(DimLeaf)]
        public string StrValue { get; set; }

        [DBColumnMapping(DimLeaf)]
        public int IntValue { get; set; }
    }

    public class InsertContext : DbContext
    {
        public InsertContext(string conn)
            : base(conn)
        {
        }

        public DbSet<Trunk> Trunks { get; set; }
        public DbSet<Leaf> Leafs { get; set; }
    }
}
