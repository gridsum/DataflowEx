using System;
using System.Linq;
using System.Threading.Tasks.Dataflow;
using Gridsum.DataflowEx.Databases;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gridsum.DataflowEx.Test.DatabaseTests
{
    using System.Runtime.InteropServices;
    using System.Text;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.EntityFrameworkCore.Storage;
    using System.Threading;

    [TestClass]
    public class BulkInserterTest
    {
        [TestMethod]
        public void TestDbBulkInserter()
        {
            //init db
            var connectString = TestUtils.GetLocalDBConnectionString();
            var context = new InsertContext(connectString);
            context.Inits.Add(new Init2());
            context.SaveChanges();
            
            var inserter = new DbBulkInserter<Entity2>(
                connectString,
                "dbo.Seods",
                DataflowOptions.Default,
                GSProduct.SEOD);

            var entities = new Entity2[]
                               {
                                   new Entity2(1) { Key = "a", Value = 0.5f },
                                   new Entity2(2) { Key = "b", MyLeg = new Leg() { LegInt = 5, LegString = "bleg" } },
                                   new Entity2(3) { Value = 0.7f },
                               };

            inserter.ProcessAsync(entities, true).Wait();

            //test result
            Assert.AreEqual(3, context.Seods.Count());
            Assert.AreEqual("b", context.Seods.Find(2).Name);
            Assert.AreEqual("b", context.Seods.Find(2).Name2);
            Assert.AreEqual(5, context.Seods.Find(2).LegInt);
            Assert.AreEqual("bleg", context.Seods.Find(2).LegString);
            Assert.AreEqual(-2f, context.Seods.First(s => s.Id == 2).Price);
            Assert.AreEqual("default", context.Seods.First(s => s.Id == 3).Name);
            Assert.AreEqual(0.7f, context.Seods.First(s => s.Id == 3).Price);
            Assert.AreEqual(2, context.Seods.Find(3).LegInt);
            Assert.AreEqual("LegString", context.Seods.Find(3).LegString);

        }

        [TestMethod]
        public void TestDbBulkInserter_NoNullCheck()
        {
            //init db
            var connectString = TestUtils.GetLocalDBConnectionString();
            var context = new InsertContext(connectString);
            context.Inits.Add(new Init2());
            context.SaveChanges();

            var inserter = new DbBulkInserter<Entity3>(
                connectString,
                "dbo.Seods",
                DataflowOptions.Default,
                GSProduct.SEOD);

            var guid = Guid.NewGuid();
            var entities = new Entity3[]
                               {
                                   new Entity3(1) { Key = "a", Value = 0.5f, MyLeg = new Leg() {}},
                                   new Entity3(2) { Key = "b", MyLeg = new Leg() { LegInt = 5, LegString = "bleg" } }
                               };

            var entities2 = new[]
                                {
                                    new Entity3(3)
                                        {
                                            Value = 0.7f,
                                            MyLeg = new Leg(),
                                            UID = guid,
                                            RawData = Encoding.ASCII.GetBytes("abc")
                                        }
                                };

            inserter.ProcessMultipleAsync(true, entities, entities2).Wait();

            //test result
            Assert.AreEqual(3, context.Seods.Count());
            Assert.AreEqual("b", context.Seods.Find(2).Name);
            Assert.AreEqual("b", context.Seods.Find(2).Name2);
            Assert.AreEqual(5, context.Seods.Find(2).LegInt);
            Assert.AreEqual("bleg", context.Seods.Find(2).LegString);
            Assert.AreEqual("LegString2", context.Seods.Find(2).LegString2);
            Assert.AreEqual(-2f, context.Seods.First(s => s.Id == 2).Price);
            Assert.AreEqual("default", context.Seods.First(s => s.Id == 3).Name);
            Assert.AreEqual(0.7f, context.Seods.First(s => s.Id == 3).Price);
            Assert.AreEqual(2, context.Seods.Find(3).LegInt);
            Assert.AreEqual("LegString", context.Seods.Find(3).LegString);
            Assert.AreEqual("LegString2", context.Seods.Find(3).LegString2);
            Assert.AreEqual(guid, context.Seods.Find(3).Uid);
            Assert.AreEqual("abc", Encoding.ASCII.GetString(context.Seods.Find(3).RawData));
        }

        [TestMethod]
        public void TestMultiDbBulkInserter()
        {
            var connectString = TestUtils.GetLocalDBConnectionString("TestMultiDbBulkInserter_{0}");
            //init db
            int profileIdCount = 3;
            InsertContext[] contexts = new InsertContext[profileIdCount + 1];
            for (int i = 1; i <= profileIdCount; ++i)
            {
                var init = new Init2();
                contexts[i] = new InsertContext(string.Format(connectString, i, i));
                contexts[i].Inits.Add(init);
                contexts[i].SaveChanges();
            }

            var profileDispatch = new Func<Entity2, int>(e => e.ProfileId);
            var connectionGetter = new Func<int, string>(id => string.Format(connectString, id, id));

            var multiDbBulkInserter = new MultiDbBulkInserter<Entity2>(DataflowOptions.Default, profileDispatch,
                connectionGetter, "dbo.Seods", GSProduct.SEOD);

            var entities = new[]
            {
                new Entity2(1, 1f, "a"),
                new Entity2(2, 2f, "b"),
                new Entity2(2, 2f, "c"),
                new Entity2(3, 3f, "d"),
                new Entity2(3, 3f, "e"),
                new Entity2(3, 3f, "f")
            };

            multiDbBulkInserter.ProcessAsync(entities).Wait();

            //assert result
            for (int i = 1; i <= profileIdCount; ++i)
            {
                Assert.AreEqual(i,  contexts[i].Seods.Count());
                Assert.AreEqual(i,  contexts[i].Seods.FirstOrDefault().Price);
                Assert.IsTrue(      contexts[i].Seods.FirstOrDefault().Name.Length == 1);
                Assert.IsTrue(      contexts[i].Seods.FirstOrDefault().Name2.Length == 1);
            }
        }
    }
    
    public class Entity2
    {
        public Entity2() { }

        public Entity2(int id)
        {
            this.ProfileId = id;
        }

        public Entity2(int id, float value, string key)
        {
            this.ProfileId = id;
            this.Value = value;
            this.Key = key;
        }

        public int ProfileId { get; set; }

        [DBColumnMapping(GSProduct.SEOD, "Price", -2.0f)]
        public float? Value { get; set; }

        [DBColumnMapping(GSProduct.SEOD, "Name", "default")]
        [DBColumnMapping(GSProduct.SEOD, "Name2", "default")]
        public string Key { get; set; }

        public Leg MyLeg { get; set; }

        [DBColumnMapping(GSProduct.SEOD, "Uid")]
        public Guid UID { get; set; }

        [DBColumnMapping(GSProduct.SEOD, "A Bad Column", null, ColumnMappingOption.Optional)]
        public string BadColumn { get; set; }
    }

    public class Entity3
    {
        public Entity3() { }

        public Entity3(int id)
        {
            this.ProfileId = id;
        }

        public Entity3(int id, float value, string key)
        {
            this.ProfileId = id;
            this.Value = value;
            this.Key = key;
        }

        public int ProfileId { get; set; }

        [DBColumnMapping(GSProduct.SEOD, "Price", -2.0f)]
        public float? Value { get; set; }

        [DBColumnMapping(GSProduct.SEOD, "Name", "default")]
        [DBColumnMapping(GSProduct.SEOD, "Name2", "default")]
        public string Key { get; set; }

        [DBColumnPath(DBColumnPathOptions.DoNotGenerateNullCheck)]
        public Leg MyLeg { get; set; }

        [DBColumnMapping(GSProduct.SEOD, "Uid")]
        public Guid UID { get; set; }

        [DBColumnMapping(GSProduct.SEOD, "Uid")]
        public Guid UID2 { get; set; }

        [DBColumnMapping(GSProduct.SEOD)]
        public byte[] RawData { get; set; }
    }

    public class Leg
    {
        [DBColumnMapping(GSProduct.SEOD, "LegInt", 2)]
        public int? LegInt { get; set; }

        [DBColumnMapping(GSProduct.SEOD, "LegString", "LegString")]
        public string LegString { get; set; }

        [DBColumnMapping(GSProduct.SEOD, "LegString2", "LegString2")]
        public string LegString2 { get; set; }
    }

    #region test entity and dbcontext

    public class Init2
    {
        public int Id { get; set; }
    }

    public class Seod
    {
        public int Id { get; set; }

        public string Name { get; set; }
        public string Name2 { get; set; }

        public float Price { get; set; }
        
        public string LegString { get; set; }
        public string LegString2 { get; set; }
        public int LegInt { get; set; }

        public Guid Uid { get; set; }

        public byte[] RawData { get; set; }
    }

    public class InsertContext : DbContextBase<InsertContext>
    {
        public InsertContext(string conn)
            : base(conn)
        {
        }

        public DbSet<Init2> Inits { get; set; }
        public DbSet<Seod> Seods { get; set; }

    }
    #endregion
}
