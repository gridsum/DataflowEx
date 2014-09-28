namespace Gridsum.DataflowEx.Demo
{
    using System;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;
    using Gridsum.DataflowEx;
    
    public class ComplexIntFlow : Dataflow<int>
    {
        private ITargetBlock<int> _headBlock;
        public ComplexIntFlow() : base(DataflowOptions.Default)
        {
            Dataflow<int, int> node2 = DataflowUtils.FromDelegate<int, int>(i => i);
            Dataflow<int, int> node3 = DataflowUtils.FromDelegate<int, int>(i => i * -1);

            Dataflow<int, int> node1 = DataflowUtils.FromDelegate<int, int>(
                i => {
                        if (i % 2 == 0) { node2.Post(i); }
                        else { node3.Post(i); }
                        return 999;
                    });
            
            Dataflow<int> printer = DataflowUtils.FromDelegate<int>(Console.WriteLine);

            node1.Name = "node1";
            node2.Name = "node2";
            node3.Name = "node3";
            printer.Name = "printer";

            this.RegisterChild(node1);
            this.RegisterChild(node2);
            this.RegisterChild(node3);
            this.RegisterChild(printer, t =>
            {
                if (t.Status == TaskStatus.RanToCompletion)
                    Console.WriteLine("Printer done!");
            });

            node1.LinkTo(printer);
            node2.LinkTo(printer);
            node3.LinkTo(printer);

            //Completion propagation: node1 ---> node2
            node2.RegisterDependency(node1);
            //Completion propagation: node1 + node2 ---> node3
            node3.RegisterDependency(node1);
            node3.RegisterDependency(node2);
            
            this._headBlock = node1.InputBlock;
        }

        public override ITargetBlock<int> InputBlock { get { return this._headBlock; } }
    }
}
