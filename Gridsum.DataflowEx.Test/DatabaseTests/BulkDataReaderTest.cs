using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using Gridsum.DataflowEx.Databases;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gridsum.DataflowEx.Test.DatabaseTests
{
    /// <summary>
    /// 选择WD与CD两种产品进行测试。各个属性的测试目的如下：
    /// Id：　测试在不同的产品对应到数据库的列名称不同
    /// FirstName：　测试相同的产品对应到数据库的列名称相同
    /// PhoneNumber: 测试只有加标记的产品才能得到数据
    /// </summary>
    public class Entity
    {
        [DBColumnMapping(GSProduct.WD, 0, "wdId")]
        [DBColumnMapping(GSProduct.CD, 0, "cdId")]
        public int Id { get; set; }

        [DBColumnMapping(GSProduct.WD, 1, "Name")]
        [DBColumnMapping(GSProduct.CD, 1, "Name")]
        public string PersonName { get; set; }

        [DBColumnMapping(GSProduct.CD, 2, "PhoneNum")]
        [DBColumnMapping(GSProduct.WD)]
        [DBColumnMapping(GSProduct.WD, "PhoneNumber2")]
        public string PhoneNumber { get; set; }

        [DBColumnMapping(GSProduct.CD)]
        public Location Location { get; set; }

        [DBColumnMapping(GSProduct.CD)]
        public Address Address { get; set; }
    }

    public class Location
    {
        [DBColumnMapping(GSProduct.CD, "roadnum", -1, null)]
        public int RoadNumber { get; set; }

        [DBColumnMapping(GSProduct.CD, "roadname")]
        public string RoadName { get; set; }


    }

    /// <summary>
    /// 用于测试，即使属性没有加Product信息，但是由于整个类都没有加，而且这个属性与数据库的某一列“名称相同”，则此数据也可以被进数据库
    /// </summary>
    public class Address
    {
        [DBColumnMapping(GSProduct.CD)]
        public int AddressNumber { get; set; }
    }

    /// <summary>
    /// this class is used to test BulkDataReader
    /// </summary>
    /// 
    [TestClass]
    public class BulkDataReaderTest
    {

        /// <summary>
        /// 测试BulkDataReader可以正确地从对象中提取出相应的数据
        /// </summary>
        [TestMethod]
        public void TestReader()
        {

            AppDomain.CurrentDomain.SetData("DataDirectory", AppDomain.CurrentDomain.BaseDirectory);
            var connectString =
                @"Data Source=(LocalDB)\v11.0;AttachDbFilename=|DataDirectory|\TestReader.mdf;Initial Catalog=mytest;Integrated Security=True;Connect Timeout=30";

            //init database
            System.Data.Entity.Database.SetInitializer(new DropCreateDatabaseAlways<MyContext>());
            var context = new MyContext(connectString);
            context.Inits.Add(new Init());
            context.SaveChanges();

            #region init sample data

            var entities = new List<Entity>
             {
                 new Entity
                 {
                     Id = 1,
                     PersonName = "zhang san",
                     PhoneNumber = "110",
                     Location = new Location{RoadNumber = 1, RoadName = "first"}
                 },
                 new Entity
                 {
                     Id = 2,
                     PersonName = "li si",
                     PhoneNumber = "119",
               //      Location = new Location{RoadNumber = 2, RoadName = "second", RoadMap = new RoadMap{Id=2}}
                    Address = new Address{AddressNumber = 10}
                 }
             };

            var entities2 = new List<Entity>
            {
                new Entity
                {
                    Id = 3,
                    PersonName = "wang er",
                    PhoneNumber = "120"
                }
            };

            #endregion
            #region add to db

            using (var connection = new SqlConnection(connectString))
            {
                connection.Open();

                // Wd:
                var wdAccessor = TypeAccessorManager<Entity>.GetAccessorByDestLabel(GSProduct.WD, connectString, "dbo.Wds");
                var wdReader2 = new BulkDataReader<Entity>(wdAccessor, entities);
                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    foreach (SqlBulkCopyColumnMapping map in wdReader2.ColumnMappings)
                    {
                        bulkCopy.ColumnMappings.Add(map);
                    }
                    bulkCopy.DestinationTableName = "dbo.Wds";

                    bulkCopy.WriteToServer(wdReader2);
                }

                //Cd:
                var cdAccessor = TypeAccessorManager<Entity>.GetAccessorByDestLabel(GSProduct.CD, connectString,
                    "dbo.Cds");
                var cdReader2 = new BulkDataReader<Entity>(cdAccessor, entities);
                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    foreach (SqlBulkCopyColumnMapping map in cdReader2.ColumnMappings)
                    {
                        bulkCopy.ColumnMappings.Add(map);
                    }
                    bulkCopy.DestinationTableName = "dbo.Cds";

                    bulkCopy.WriteToServer(cdReader2);
                }

                //Wd 2: use debug model to test whether this new WdAccessor is new build or not
                //right: this WdAccessor should just get from TypeAccessorManager, it can not be rebuild.
                var wdAccessor2 = TypeAccessorManager<Entity>.GetAccessorByDestLabel(GSProduct.WD, connectString, "dbo.Wds");
                var wdReader22 = new BulkDataReader<Entity>(wdAccessor2, entities2);
                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    foreach (var map in wdReader22.ColumnMappings)
                    {
                        bulkCopy.ColumnMappings.Add(map);
                    }
                    bulkCopy.DestinationTableName = "dbo.Wds";
                    bulkCopy.WriteToServer(wdReader22);
                }
            }
            #endregion

            #region reader from db and test whether data is exist.

            //wd
            Assert.AreEqual(3, context.Wds.Count());
            Assert.AreEqual("li si", context.Wds.FirstOrDefault(w => w.WdId == 2).Name);
            Assert.AreEqual("110", context.Wds.FirstOrDefault(w => w.WdId == 1).PhoneNumber);
            var entity = context.Wds.FirstOrDefault();
            Assert.AreEqual(entity.PhoneNumber, entity.PhoneNumber2);

            //Cd
            Assert.AreEqual(2, context.Cds.Count());
            Assert.AreEqual("110", context.Cds.FirstOrDefault(c => c.CdId == 1).PhoneNum);
            Assert.AreEqual("first", context.Cds.FirstOrDefault(c => c.CdId == 1).RoadName);
            Assert.AreEqual(null, context.Cds.FirstOrDefault(c => c.CdId == 2).RoadName);

            //Cd default value,the roadnum's default value is -1
            Assert.AreEqual(-1, context.Cds.FirstOrDefault(c => c.CdId == 2).RoadNum);
            Assert.AreEqual(null, context.Cds.FirstOrDefault(c => c.CdId == 2).RoadName);

            //Cd not Product Property
            Assert.AreEqual(10, context.Cds.FirstOrDefault(c => c.CdId == 2).AddressNumber);
            #endregion

            context.Dispose();

        }
    }




    public class Init
    {
        public int InitId { get; set; }
    }

    public class Wd
    {
        public int WdId { get; set; }
        public string Name { get; set; }

        public string PhoneNumber { get; set; }

        public string PhoneNumber2 { get; set; }
        public override string ToString()
        {
            return "WdId:" + WdId + ", Name:" + Name;
        }
    }

    public class Cd
    {
        public int CdId { get; set; }
        public string Name { get; set; }
        public string PhoneNum { get; set; }

        public int RoadNum { get; set; }

        public string RoadName { get; set; }

        public int AddressNumber { get; set; }


        public override string ToString()
        {
            return "CdId:" + CdId + ", Name:" + Name + ", PhoneNum:" + PhoneNum + ", RoadNum:" + RoadNum + ", RoadName:" +
                   RoadName;
        }
    }

    public class MyContext : DbContext
    {
        public MyContext(string connectString)
            : base(connectString)
        {
        }

        public DbSet<Init> Inits { get; set; }
        public DbSet<Wd> Wds { get; set; }
        public DbSet<Cd> Cds { get; set; }
    }
}
