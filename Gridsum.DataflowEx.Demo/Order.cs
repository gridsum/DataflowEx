using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Demo
{
    using System.Linq.Expressions;

    using Gridsum.DataflowEx.Databases;
    using Gridsum.DataflowEx.ETL;

    public class Order
    {
        [DBColumnMapping("OrderTarget", "Date", null)]
        public DateTime OrderDate { get; set; }

        [DBColumnMapping("OrderTarget", "Value", 0.0f)]
        public float? OrderValue { get; set; }

        [NoNullCheck]
        public Person Customer { get; set; }
    }

    public class OrderEx : Order
    {
        public OrderEx()
        {
            this.OrderDate = DateTime.Now;
        }

        // This is the field we want to populate
        [DBColumnMapping("LookupDemo", "ProductKey")]
        [DBColumnMapping("OrderTarget", "ProductKey")]
        public long? ProductKey { get; set; }

        public Product Product { get; set; }
    }

    public class Product
    {
        public Product(string category, string name)
        {
            this.Category = category;
            this.Name = name;
        }

        [DBColumnMapping("LookupDemo", "Category")]
        public string Category { get; private set; }

        [DBColumnMapping("LookupDemo", "ProductName")]
        public string Name { get; private set; }

        [DBColumnMapping("LookupDemo", "ProductFullName")]
        public string FullName { get { return string.Format("{0}-{1}", Category, Name); } }
    }

    public class ProductLookupFlow : DbDataJoiner<OrderEx, string>
    {
        public ProductLookupFlow(TargetTable dimTableTarget, int batchSize = 8192, int cacheSize = 1048576)
            : base(
            //Tells DbDataJoiner the join condition is 'OrderEx.Product.FullName = TABLE.ProductFullName'
            o => o.Product.FullName, 
            dimTableTarget, DataflowOptions.Default, batchSize, cacheSize)
        {
        }

        protected override void OnSuccessfulLookup(OrderEx input, IDimRow<string> rowInDimTable)
        {
            //what we want is just the key column of the dimension row
            input.ProductKey = rowInDimTable.AutoIncrementKey;
        }
    }
}
