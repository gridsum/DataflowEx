namespace Gridsum.DataflowEx.Demo
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks.Dataflow;

    using Gridsum.DataflowEx;

    public class AggregatorFlow : Dataflow<string>
    {
        //Blocks
        private TransformBlock<string, KeyValuePair<string, int>> _splitter; 
        private ActionBlock<KeyValuePair<string, int>> _aggregater;

        //Data
        private Dictionary<string, int> _dict;

        public AggregatorFlow() : base(DataflowOptions.Default)
        {
            this._splitter = new TransformBlock<string, KeyValuePair<string, int>>((Func<string, KeyValuePair<string, int>>)this.Split);
            this._dict = new Dictionary<string, int>();
            this._aggregater = new ActionBlock<KeyValuePair<string, int>>((Action<KeyValuePair<string, int>>)this.Aggregate);

            //Block linking
            this._splitter.LinkTo(this._aggregater, new DataflowLinkOptions() { PropagateCompletion = true });

            /* IMPORTANT */
            this.RegisterChild(this._splitter);
            this.RegisterChild(this._aggregater);
        }

        protected virtual void Aggregate(KeyValuePair<string, int> pair)
        {
            int oldValue;
            this._dict[pair.Key] = this._dict.TryGetValue(pair.Key, out oldValue) ? oldValue + pair.Value : pair.Value;
        }

        protected virtual KeyValuePair<string, int> Split(string input)
        {
            string[] splitted = input.Split('=');
            return new KeyValuePair<string, int>(splitted[0], int.Parse(splitted[1]));
        }

        public override ITargetBlock<string> InputBlock { get { return this._splitter; } }

        public IDictionary<string, int> Result { get { return this._dict; } }
    }
}
