using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Demo
{
    using System.Threading.Tasks.Dataflow;

    using Gridsum.DataflowEx.AutoCompletion;

    public class CircularFlow : Dataflow<int>
    {
        private Dataflow<int, int> _buffer;

        public CircularFlow(DataflowOptions dataflowOptions) : base(dataflowOptions)
        {
            //a no-op node to demonstrate the usage of preTask param in RegisterChildRing
            _buffer = new BufferBlock<int>().ToDataflow(name: "NoOpBuffer"); 

            var heater = new TransformManyDataflow<int, int>(async i =>
                    {
                        await Task.Delay(200); 
                        Console.WriteLine("Heated to {0}", i + 1);
                        return new [] {i + 1};
                    }, dataflowOptions);
            heater.Name = "Heater";

            var cooler = new TransformManyDataflow<int, int>(async i =>
                    {
                        await Task.Delay(200);
                        int cooled = i - 2;
                        Console.WriteLine("Cooled to {0}", cooled);

                        if (cooled < 0) //time to stop
                        {
                            return Enumerable.Empty<int>(); 
                        }

                        return new [] {cooled};
                    }, dataflowOptions);
            cooler.Name = "Cooler";

            var heartbeat = new HeartbeatNode<int>(dataflowOptions) {Name = "HeartBeat"};
            
            _buffer.LinkTo(heater);

            //circular
            heater.LinkTo(cooler);
            cooler.LinkTo(heartbeat);
            heartbeat.LinkTo(heater);

            RegisterChildren(_buffer, heater, cooler, heartbeat);
            
            //ring registration
            RegisterChildRing(_buffer.CompletionTask, heater, cooler, heartbeat);
        }

        public override ITargetBlock<int> InputBlock { get { return _buffer.InputBlock; } }
    }
}
