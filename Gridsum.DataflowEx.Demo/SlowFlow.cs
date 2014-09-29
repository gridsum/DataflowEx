using System;
using System.Collections.Generic;

namespace Gridsum.DataflowEx.Demo
{
    using System.Threading;
    using System.Threading.Tasks.Dataflow;
    
    public class SlowFlow : Dataflow<string>
    {
        private Dataflow<string, char> _splitter;
        private Dataflow<char> _printer;

        public SlowFlow(DataflowOptions dataflowOptions)
            : base(dataflowOptions)
        {
            _splitter = new TransformManyBlock<string, char>(new Func<string, IEnumerable<char>>(this.SlowSplit))
                .ToDataflow(dataflowOptions, "SlowSplitter");
                
            _printer = new ActionBlock<char>(c =>
                {
                    Thread.Sleep(2000); 
                    Console.WriteLine(c); 
                })
                .ToDataflow(dataflowOptions, "Printer");

            RegisterChild(_splitter);
            RegisterChild(_printer);

            _splitter.LinkTo(_printer);
        }

        private IEnumerable<char> SlowSplit(string s)
        {
            foreach (var c in s)
            {
                Thread.Sleep(1000);
                yield return c;
            }
        }

        public override ITargetBlock<string> InputBlock
        {
            get
            {
                return _splitter.InputBlock;
            }
        }
    }
}
