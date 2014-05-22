using System;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks.Dataflow;
using Gridsum.DataflowEx.Databases;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gridsum.DataflowEx.Test.DatabaseTests
{
    [TestClass]
    public class BulkInserterTest
    {
        [TestMethod]
        public void TestDbBulkInserter()
        {
            //init db
            System.Data.Entity.Database.SetInitializer(new DropCreateDatabaseAlways<InsertContext>());
            AppDomain.CurrentDomain.SetData("DataDirectory", AppDomain.CurrentDomain.BaseDirectory);
            var connectString =
               @"Data Source=(LocalDB)\v11.0;AttachDbFilename=|DataDirectory|\TestDbBulkInserter.mdf;Initial Catalog=dbinserter;Integrated Security=True;Connect Timeout=30";

            var context = new InsertContext(connectString);
            context.Inits.Add(new Init2());
            context.SaveChanges();

            var inserter = new DbBulkInserter<Entity2>(connectString, "dbo.Seods", DataflowOptions.Default,
                GSProduct.SEOD);

            var entities = new Entity2[]
            {
                new Entity2(1, 0.5f, "a"),
                new Entity2(2, 0.6f, "b"),
                new Entity2(3, 0.7f, "c"),
            };

            foreach (var entity in entities)
            {
                inserter.InputBlock.SafePost(entity);
            }
            inserter.InputBlock.Complete();
            inserter.CompletionTask.Wait();

            //test result
            Assert.AreEqual(3, context.Seods.Count());
            Assert.AreEqual("b", context.Seods.FirstOrDefault(s => s.Id == 2).Name);
            Assert.AreEqual(0.7f, context.Seods.FirstOrDefault(s => s.Id == 3).Price);
        }

        [TestMethod]
        public void TestMultiDbBulkINserter()
        {
            System.Data.Entity.Database.SetInitializer(new DropCreateDatabaseAlways<InsertContext>());
            AppDomain.CurrentDomain.SetData("DataDirectory", AppDomain.CurrentDomain.BaseDirectory);
            var connectString =
               @"Data Source=(LocalDB)\v11.0;AttachDbFilename=|DataDirectory|\TestMultiDbBulkInserter_{0}.mdf;Initial Catalog=multidbinserter_{1};Integrated Security=True;Connect Timeout=30";
            //init db
            int profileIdCount = 3;
            for (int i = 1; i <= profileIdCount; ++i)
            {
                var init = new Init2();
                var context = new InsertContext(string.Format(connectString, i, i));
                context.Inits.Add(init);
                context.SaveChanges();
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

            foreach (var x in entities)
            {
                multiDbBulkInserter.InputBlock.Post(x);
            }
            multiDbBulkInserter.InputBlock.Complete();
            multiDbBulkInserter.CompletionTask.Wait();

            //assert result
            for (int i = 1; i <= profileIdCount; ++i)
            {
                var connString = connectionGetter(i);
                var curContext = new InsertContext(connString);
                Assert.AreEqual(i, curContext.Seods.Count());
                Assert.AreEqual(i, curContext.Seods.FirstOrDefault().Price);
            }

        }
    }





    public class Entity2
    {
        public Entity2() { }

        public Entity2(int id, float value, string key)
        {
            this.ProfileId = id;
            this.Value = value;
            this.Key = key;
        }

        public int ProfileId { get; set; }

        [DBColumnMapping(GSProduct.SEOD, "Price", -2.0f, null)]
        public float Value { get; set; }

        [DBColumnMapping(GSProduct.SEOD, "Name", "default", null)]
        public string Key { get; set; }
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

        public float Price { get; set; }
    }

    public class InsertContext : DbContext
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
