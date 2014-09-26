namespace Gridsum.DataflowEx.Demo
{
    using System.Threading.Tasks.Dataflow;
    using Gridsum.DataflowEx;

    public class LineAggregatorFlow : Dataflow<string>
    {
        private Dataflow<string, string> _lineProcessor;
        private AggregatorFlow _aggregator;
        public LineAggregatorFlow() : base(DataflowOptions.Default)
        {
            this._lineProcessor = new TransformManyBlock<string, string>(line => line.Split(' ')).ToDataflow();
            this._aggregator = new AggregatorFlow();
            this._lineProcessor.LinkTo(this._aggregator);
            this.RegisterChild(this._lineProcessor);
            this.RegisterChild(this._aggregator);
        }

        public override ITargetBlock<string> InputBlock { get { return this._lineProcessor.InputBlock; } }
        public int this[string key] { get { return this._aggregator.Result[key]; } }
    }
}
