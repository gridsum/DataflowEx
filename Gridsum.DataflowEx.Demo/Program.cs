namespace Gridsum.DataflowEx.Demo
{
    using System;
    using System.Collections.Generic;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

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
            SlowFlowAsync().Wait();
        }

        public static async Task CalcAsync()
        {
            var aggregatorFlow = new AggregatorFlow();
            aggregatorFlow.InputBlock.Post("a=1");
            aggregatorFlow.InputBlock.Post("b=2");
            aggregatorFlow.InputBlock.Post("a=5");
            aggregatorFlow.InputBlock.Complete();
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
                            MonitorInterval = TimeSpan.FromSeconds(5) 
                        });

            slowFlow.Post("a");
            slowFlow.Post("ab");
            slowFlow.Post("abc");
            slowFlow.Post("abcd");
            slowFlow.Post("abcde");
            slowFlow.Post("abcdef");
            slowFlow.Post("abcdefg");
            slowFlow.Post("abcdefgh");
            slowFlow.Post("abcdefghi");
            await slowFlow.SignalAndWaitForCompletionAsync();
        }
    }
}
