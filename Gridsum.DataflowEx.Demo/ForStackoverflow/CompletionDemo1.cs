using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Demo.ForStackoverflow
{
    using System.Threading;
    using System.Threading.Tasks.Dataflow;

    /// <summary>
    /// For http://stackoverflow.com/questions/13510094/tpl-dataflow-guarantee-completion-only-when-all-source-data-blocks-completed
    /// </summary>
    public class CompletionDemo1
    {
        private Dataflow<int,int> broadCaster;
        private TransformBlock<int, string> transformBlock1;
        private TransformBlock<int, string> transformBlock2;
        private Dataflow<string> processor;

        public CompletionDemo1()
        {
            broadCaster = new BroadcastBlock<int>(
                i =>
                    {
                        return i;
                    }).ToDataflow();

            transformBlock1 = new TransformBlock<int, string>(
                i =>
                    {
                        Console.WriteLine("1 input count: " + transformBlock1.InputCount);
                        Thread.Sleep(50);
                        return ("1_" + i);
                    });

            transformBlock2 = new TransformBlock<int, string>(
                i =>
                    {
                        Console.WriteLine("2 input count: " + transformBlock2.InputCount);
                        Thread.Sleep(20);
                        return ("2_" + i);
                    });

            processor = new ActionBlock<string>(
                i =>
                    {
                        Console.WriteLine(i);
                    }).ToDataflow();

            /** rather than TPL linking
              broadCastBlock.LinkTo(transformBlock1, new DataflowLinkOptions { PropagateCompletion = true });
              broadCastBlock.LinkTo(transformBlock2, new DataflowLinkOptions { PropagateCompletion = true });
              transformBlock1.LinkTo(processorBlock, new DataflowLinkOptions { PropagateCompletion = true });
              transformBlock2.LinkTo(processorBlock, new DataflowLinkOptions { PropagateCompletion = true });
             **/

            //Use DataflowEx linking
            var transform1 = transformBlock1.ToDataflow();
            var transform2 = transformBlock2.ToDataflow();

            broadCaster.LinkTo(transform1);
            broadCaster.LinkTo(transform2);
            transform1.LinkTo(processor);
            transform2.LinkTo(processor);
        }
        
        public void Start()
        {
            const int numElements = 100;

            for (int i = 1; i <= numElements; i++)
            {
                broadCaster.SendAsync(i);
            }

            //mark completion
            broadCaster.Complete();

            processor.CompletionTask.Wait();

            Console.WriteLine("Finished");
            Console.ReadLine();
        }

/*  
        public static void Main()
        {
            var demo = new CompletionDemo1();
            demo.Start();
        }    
    */
    }
}
