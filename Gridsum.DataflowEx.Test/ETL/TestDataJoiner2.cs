namespace Gridsum.DataflowEx.Test.ETL.Test2
{
    using System;
    using System.Data;
    using System.Data.Entity;
    using System.Data.Entity.Core.Common.CommandTrees.ExpressionBuilder;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text;
    using System.Threading.Tasks;

    using Gridsum.DataflowEx.Databases;
    using Gridsum.DataflowEx.ETL;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class TestDataJoiner2
    {
        class JoinerWithAsserter : DbDataJoiner<Trunk, byte[]>
        {
            public int Count;

            public JoinerWithAsserter(Expression<Func<Trunk, byte[]>> joinOn, TargetTable dimTableTarget, int batchSize)
                : base(joinOn, dimTableTarget, batchSize)
            {
                this.Count = 0;
            }

            protected override void OnSuccessfulLookup(Trunk input, IDimRow<byte[]> rowInDimTable)
            {
                Assert.IsTrue(TestUtils.ArrayEqual(input.Pointer, rowInDimTable.JoinOn));
                //Assert.AreEqual("Str" + Encoding.UTF8.GetString(input.Pointer), rowInDimTable["StrValue"]);
                this.Count++;
            }
        }

        [TestMethod]
        public async Task TestDataJoinerJoining()
        {
            Database.SetInitializer(new DropCreateDatabaseAlways<InsertContext>());
            var connectString = TestUtils.GetLocalDBConnectionString("TestDataJoinerJoining");
            var context = new InsertContext(connectString);

            for (int i = 0; i < 10; i++)
            {
                context.Leafs.Add(new Leaf() { IntValue = i, Key = i.ToString().ToByteArray(), StrValue = "Str" + i });
            }

            context.SaveChanges();

            var joiner = new JoinerWithAsserter(
                t => t.Pointer,
                new TargetTable(Leaf.DimLeaf, connectString, "Leaves"),
                8192);

            joiner.DataflowOptions.MonitorInterval = TimeSpan.FromSeconds(3);

            joiner.LinkLeftToNull();

            var dataArray = new[]
                                {
                                    new Trunk() { Pointer = "3".ToByteArray() }, 
                                    new Trunk() { Pointer = "5".ToByteArray() },
                                    new Trunk() { Pointer = "7".ToByteArray() }, 
                                    new Trunk() { Pointer = "7".ToByteArray() },
                                    new Trunk() { Pointer = "9".ToByteArray() },
                                    new Trunk() { Pointer = "99".ToByteArray(), Leaf = new Leaf() { StrValue = "Str99" } },
                                    new Trunk() { Pointer = "99".ToByteArray(), Leaf = new Leaf() { StrValue = "Str99" } },
                                };

            await joiner.ProcessAsync(dataArray);

            Assert.AreEqual(dataArray.Length, joiner.Count);

            Assert.AreEqual(11, context.Leafs.Count());
        }
    }


    public class Trunk
    {
        public int Id { get; set; }

        [DBColumnMapping(Leaf.DimLeaf, "Key")]
        public byte[] Pointer { get; set; }

        public string Name { get; set; }
        public Leaf Leaf { get; set; }
    }

    public class Leaf
    {
        public const string DimLeaf = "DimLeaf";

        public int Id { get; set; }

        [DBColumnMapping(DimLeaf)]
        public byte[] Key { get; set; }

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