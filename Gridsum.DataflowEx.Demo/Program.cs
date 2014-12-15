namespace Gridsum.DataflowEx.Demo
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    using Common.Logging;

    using Gridsum.DataflowEx.Databases;

    class Program
    {
        static void Main(string[] args)
        {
            string s =
                "http://cn.bing.com/search?q=MD5CryptoServiceProvider+slow&qs=n&pq=md5cryptoserviceprovider+slow&sc=0-25&sp=-1&sk=&cvid=67d40cbd8c424d55a3db83e6e9868267&first=51&FORM=PERE4";
            using (MD5CryptoServiceProvider md5 = new MD5CryptoServiceProvider())
            {
                byte[] inBytes = Encoding.UTF8.GetBytes(s);
                var bytes = md5.ComputeHash(inBytes);
                Console.WriteLine(bytes.Length);
            }


            var splitter = new TransformBlock<string, KeyValuePair<string, int>>(
                input =>
                    {
                        var splitted = input.Split('=');
                        return new KeyValuePair<string, int>(splitted[0], int.Parse(splitted[1]));
                    });

            var dict = new Dictionary<string, int>();
            var aggregater = new ActionBlock<KeyValuePair<string, int>>(
                pair =>
                    {
                        int oldValue;
                        dict[pair.Key] = dict.TryGetValue(pair.Key, out oldValue) ? oldValue + pair.Value : pair.Value;
                    });

            splitter.LinkTo(aggregater, new DataflowLinkOptions() { PropagateCompletion = true});

            splitter.Post("a=1");
            splitter.Post("b=2");
            splitter.Post("a=5");

            splitter.Complete();
            aggregater.Completion.Wait();
            Console.WriteLine("sum(a) = {0}", dict["a"]); //prints sum(a) = 6


            //CalcAsync().Wait();
            //SlowFlowAsync().Wait();
            //FailDemoAsync().Wait();
            //TransformAndLinkDemo().Wait();
            //LinkLeftToDemo().Wait();
            //CircularFlowAutoComplete().Wait();
            //RecorderDemo().Wait();
            BulkInserterDemo().Wait();
            //BulkInserterDemo2().Wait();
            //BroadcasterDemo().Wait();
            //MyLoggerDemo().Wait();
            //ETLLookupDemo().Wait();
        }

        public static async Task CalcAsync()
        {
            var aggregatorFlow = new AggregatorFlow();
            aggregatorFlow.Post("a=1");
            aggregatorFlow.Post("b=2");
            aggregatorFlow.Post("a=5");
            aggregatorFlow.Complete();
            await aggregatorFlow.CompletionTask;
            Console.WriteLine("sum(a) = {0}", aggregatorFlow.Result["a"]); //prints sum(a) = 6


            //await aggregatorFlow.ProcessAsync(new[] { "a=1", "b=2", "a=5" }, completeFlowOnFinish: true);

            var lineAggregator = new LineAggregatorFlow();
            await lineAggregator.ProcessAsync(new[] { "a=1 b=2 a=5", "c=6 b=8" });
            Console.WriteLine("sum(a) = {0}", lineAggregator["a"]); //prints sum(a) = 6


            var intFlow = new ComplexIntFlow();
            await intFlow.ProcessAsync(new[] { 1, 2, 3});
        }

        public static async Task SlowFlowAsync()
        {
            var slowFlow = new SlowFlow( new DataflowOptions
                        {
                            FlowMonitorEnabled = true, 
                            BlockMonitorEnabled = true,
                            MonitorInterval = TimeSpan.FromSeconds(2),
                            PerformanceMonitorMode = DataflowOptions.PerformanceLogMode.Verbose
                        });
            
            await slowFlow.ProcessAsync(new[]
                                            {
                                                "abcd", 
                                                "abc", 
                                                "ab", 
                                                "a"
                                            });
            
        }

        public static async Task FailDemoAsync()
        {
            var aggregatorFlow = new AggregatorFlow();
            aggregatorFlow.Post("a=1");
            aggregatorFlow.Post("b=2");
            aggregatorFlow.Post("a=5");
            aggregatorFlow.Post("a=badstring");
            aggregatorFlow.Complete();
            await aggregatorFlow.CompletionTask;
        }

        public static async Task TransformAndLinkDemo()
        {
            var d1 = new BufferBlock<int>().ToDataflow();
            var d2 = new ActionBlock<string>(s => Console.WriteLine(s)).ToDataflow();
            var d3 = new ActionBlock<string>(s => Console.WriteLine(s)).ToDataflow();

            d1.TransformAndLink(d2, _ => "Odd: "+ _, _ => _ % 2 == 1);
            d1.TransformAndLink(d3, _ => "Even: " + _, _ => _ % 2 == 0);

            for (int i = 0; i < 10; i++)
            {
                d1.Post(i);
            }

            await d1.SignalAndWaitForCompletionAsync();
            await d2.CompletionTask;
            await d3.CompletionTask;
        }

        public static async Task LinkLeftToDemo()
        {
            var d1 = new BufferBlock<int>().ToDataflow(name: "IntGenerator");
            var d2 = new ActionBlock<int>(s => Console.WriteLine(s)).ToDataflow();
            var d3 = new ActionBlock<int>(s => Console.WriteLine(s)).ToDataflow();
            var d4 = new ActionBlock<int>(s => Console.WriteLine(s + "[Left]")).ToDataflow();

            d1.LinkTo(d2, _ => _ % 3 == 0);
            d1.LinkTo(d3, _ => _ % 3 == 1);
            d1.LinkLeftToError();//d1.LinkLeftTo(d4);

            for (int i = 0; i < 10; i++)
            {
                d1.Post(i);
            }

            await d1.SignalAndWaitForCompletionAsync();
            await d2.CompletionTask;
            await d3.CompletionTask;
        }

        public static async Task CircularFlowAutoComplete()
        {
            var f = new CircularFlow(DataflowOptions.Default);
            f.Post(10);
            await f.SignalAndWaitForCompletionAsync();
        }

        public static async Task RecorderDemo()
        {
            var f = new PeopleFlow(DataflowOptions.Default);
            var sayHello = new ActionBlock<Person>(p => Console.WriteLine("Hello, I am {0}, {1}", p.Name, p.Age)).ToDataflow(name: "sayHello");
            f.LinkTo(sayHello, p => p.Age > 0);
            f.LinkLeftToNull(); //object flowing here will be recorded by GarbageRecorder
            
            f.Post("{Name: 'aaron', Age: 20}");
            f.Post("{Name: 'bob', Age: 30}");
            f.Post("{Name: 'carmen', Age: 80}");
            f.Post("{Name: 'neo', Age: -1}");
            await f.SignalAndWaitForCompletionAsync();
            await sayHello.CompletionTask;

            Console.WriteLine("Total people count: " + f.PeopleRecorder[typeof(Person)]);
            Console.WriteLine(f.PeopleRecorder.DumpStatistics());
            Console.WriteLine(f.GarbageRecorder.DumpStatistics());
        }

        public static async Task BulkInserterDemo()
        {
            string connStr;

            //initialize table
            using (var conn = LocalDB.GetLocalDB("BulkInserterDemo"))
            {
                var cmd = new SqlCommand(@"
                IF OBJECT_id('dbo.People', 'U') IS NOT NULL
                    DROP TABLE dbo.People;
                
                CREATE TABLE dbo.People
                (
                    Id INT IDENTITY(1,1) NOT NULL,
                    NameCol nvarchar(50) NOT NULL,
                    AgeCol INT           NOT NULL
                )
                ", conn);
                cmd.ExecuteNonQuery();
                connStr = conn.ConnectionString;
            }

            var f = new PeopleFlow(DataflowOptions.Default);
            var dbInserter = new DbBulkInserter<Person>(connStr, "dbo.People", DataflowOptions.Default, "PersonTarget");
            f.LinkTo(dbInserter);

            f.Post("{Name: 'aaron', Age: 20}");
            f.Post("{Name: 'bob', Age: 30}");
            f.Post("{Age: 80}"); //Name will be default value: "N/A"
            f.Post("{Name: 'neo' }"); // Age will be default value: -1
            await f.SignalAndWaitForCompletionAsync();
            await dbInserter.CompletionTask;
        }

        public static async Task BulkInserterDemo2()
        {
            string connStr;

            //initialize table
            using (var conn = LocalDB.GetLocalDB("BulkInserterDemo"))
            {
                var cmd = new SqlCommand(@"
                IF OBJECT_id('dbo.Orders', 'U') IS NOT NULL
                    DROP TABLE dbo.Orders;
                
                CREATE TABLE dbo.Orders
                (
                    Id INT IDENTITY(1,1) NOT NULL,
                    Date DATETIME NOT NULL,
                    Value FLOAT NOT NULL,
                    CustomerName NVARCHAR(50) NOT NULL,
                    CustomerAge INT NOT NULL
                )
                ", conn);
                cmd.ExecuteNonQuery();
                connStr = conn.ConnectionString;
            }

            var dbInserter = new DbBulkInserter<Order>(connStr, "dbo.Orders", DataflowOptions.Default, "OrderTarget");

            dbInserter.Post(new Order {OrderDate = DateTime.Now, OrderValue = 15, Customer = new Person() {Name = "Aaron", Age = 38}}); 
            dbInserter.Post(new Order {OrderDate = DateTime.Now, OrderValue = 25, Customer = new Person() {Name = "Bob", Age = 30}}); 
            dbInserter.Post(new Order {OrderDate = DateTime.Now, OrderValue = 35, Customer = new Person() {Age = 48}}); 
            dbInserter.Post(new Order {OrderDate = DateTime.Now, OrderValue = 45, Customer = new Person() {Name = "Neo"}});

            await dbInserter.SignalAndWaitForCompletionAsync();
        }

        public static async Task BroadcasterDemo()
        {
            var broadcaster = new DataBroadcaster<string>();

            var printer1 = new ActionBlock<string>(s => Console.WriteLine("Printer1: {0}", s)).ToDataflow();
            var printer2 = new ActionBlock<string>(s => Console.WriteLine("Printer2: {0}", s)).ToDataflow();
            var printer3 = new ActionBlock<string>(s => Console.WriteLine("Printer3: {0}", s)).ToDataflow();

            broadcaster.LinkTo(printer1);
            broadcaster.LinkTo(printer2);
            broadcaster.LinkTo(printer3);

            broadcaster.Post("first message");
            broadcaster.Post("second message");
            broadcaster.Post("third message");

            await broadcaster.SignalAndWaitForCompletionAsync();
            await printer1.CompletionTask;
            await printer2.CompletionTask;
            await printer3.CompletionTask;
        }

        public static async Task MyLoggerDemo()
        {
            var mylogger = new MyLogger();

            mylogger.Post(new MyLog { Level = LogLevel.Error, Message = "I am Error!" });
            mylogger.Post(new MyLog { Level = LogLevel.Warn, Message = "I am Warn!" });
            mylogger.Post(new MyLog { Level = LogLevel.Error, Message = "I am Error2!" });
            mylogger.Post(new MyLog { Level = LogLevel.Warn, Message = "I am Warn2!" });
            mylogger.Post(new MyLog { Level = LogLevel.Info, Message = "I am Info!" });

            await mylogger.SignalAndWaitForCompletionAsync();
        }

        public static async Task ETLLookupDemo()
        {
            string connStr;

            //initialize table
            using (var conn = LocalDB.GetLocalDB("ETLLookupDemo"))
            {
                //We will create a dimension table and a fact table
                //We also populate the dimension table with some pre-defined rows
                var cmd = new SqlCommand(@"
                IF OBJECT_id('dbo.Product', 'U') IS NOT NULL
                    DROP TABLE dbo.Product;
                
                CREATE TABLE dbo.Product
                (
                    ProductKey INT IDENTITY(1,1) NOT NULL,
                    Category nvarchar(50) NOT NULL,
                    ProductName nvarchar(50) NOT NULL,
                    ProductFullName nvarchar(100) NOT NULL                    
                )

                INSERT INTO dbo.Product VALUES ('Books', 'The Great Gatsby', 'Books-The Great Gatsby');
                INSERT INTO dbo.Product VALUES ('Games', 'Call of Duty', 'Games-Call of Duty');

                IF OBJECT_id('dbo.FactOrders', 'U') IS NOT NULL
                    DROP TABLE dbo.FactOrders;
                
                CREATE TABLE dbo.FactOrders
                (
                    Id INT IDENTITY(1,1) NOT NULL,
                    Date DATETIME NOT NULL,
                    Value FLOAT NOT NULL,
                    ProductKey INT NULL,
                )
                ", conn);
                cmd.ExecuteNonQuery();
                connStr = conn.ConnectionString;
            }

            var lookupNode = new ProductLookupFlow(new TargetTable("LookupDemo", connStr, "dbo.Product"));

            lookupNode.Post(new OrderEx { OrderValue = 10, Product = new Product("Books", "The Call of the Wild") });
            lookupNode.Post(new OrderEx { OrderValue = 20, Product = new Product("Games", "Call of Duty") });
            lookupNode.Post(new OrderEx { OrderValue = 30, Product = new Product("Games", "Call of Duty") });
            lookupNode.Post(new OrderEx { OrderValue = 20, Product = new Product("Books", "The Call of the Wild") });
            lookupNode.Post(new OrderEx { OrderValue = 20, Product = new Product("Books", "The Great Gatsby") });

            var factInserter = new DbBulkInserter<OrderEx>(connStr, "dbo.FactOrders", DataflowOptions.Default, "OrderTarget");
            lookupNode.LinkTo(factInserter);

            await lookupNode.SignalAndWaitForCompletionAsync();
            await factInserter.CompletionTask;
        }
    }
}
