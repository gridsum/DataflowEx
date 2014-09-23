Gridsum.DataflowEx
==========

Gridsum.DataflowEx is Gridsum's Object-Oriented extensions to TPL Dataflow library.

TPL Dataflow is simply great. But the low-level fundamental blocks are a bit tedious to use in real world scenarioes because 
1. Blocks are sealed and only accept delegates, which looks awkward in the Object-Oriented world where we need to maintain mutable states and reuse our data processing logic. Ever found it difficult to build a reusable library upon TPL Dataflow? 
2. Blocks need to interop with each other (e.g. should be linked carefully) and you get a chain/graph. In many times the chain need to be treated as a single processing unit but you have to construct it tediously from ground up here and there, whereever you need it. These boilerplate codes are far from graceful due to the non-OO design.

By introducing the core concept of IDataflow, Gridsum.DataflowEx is born to solve all this with an OO design on top of TPL Dataflow. You can now easily write reusable components with extension points along with TPL Dataflow! Cool features include:

* Inheritance and polymorphism for dataflows and their hehaviors
* Block chain encapsulation as a reusable unit
* Easy conditional chaining 
* Upstream failure propagation within dataflow
* Built-in performance metrics monitor
* Auto complete support for circular dataflow graph (NEW!)
* Dataflow friendly sql bulk inserter (NEW!)
* Helper methods to convert raw blocks to dataflows

Simply download Gridsum.DataflowEx and have a try!

UNDER CONSTRUCTION..

Welcome to DataflowEx
===================
DataflowEx is a collection of extensions to TPL Dataflow library with Object-Oriented Programming in mind. It does not replace TPL Dataflow but provides abstraction/management on top of dataflow blocks to make your life easier.

If you are not familiar with [TPL Dataflow](http://msdn.microsoft.com/en-us/library/hh228603(v=vs.110).aspx) yet, please take your time to watch two videos:

Beginner (15min):
http://channel9.msdn.com/posts/TPL-Dataflow-Tour

Advanced (63min):
http://channel9.msdn.com/posts/TPL-Dataflow-Tour

Background
-------------

So, what's wrong with TPL Dataflow? Nothing. The library from Microsoft library looks simply great. However, in the tough real world there are some obstacles when we apply **RAW** TPL Dataflow. Let's look at an example:
```c#
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
```
A wonderful Dataflow demo, right? A splitter block who cuts kv pair strings connects to an aggregator block who sums value for every key. So far so good if we need this dataflow only **once**. But what if I need the same dataflow graph somewhere else in my application? Or, expose the same functionality as reusable components in a library?

Things are getting complicated. Obviously Copy&Paste is not an acceptable choice. What about put everything about the dataflow construction in a static method? Hmmm, this is a step forward to reuse the code but, what should be the return value of the static method as the **handle** of the graph? Returning the head block helps posting new data to the pipeline but then we miss the tail block which we need to wait completion on. Not to mention the <kbd>dict</kbd> variable which contains our state/data... Last but not least, static method is an anti-pattern for testing as you can hardly change any behavior of underlying blocks.

Clearly we need a **class** representing the graph and being the handle of all the stakeholders. Object oriented design is a perfect fit here to solve all problems mentioned above. That is why we gave birth to Gridsum.DataflowEx.

Introduction to DataflowEx
-------------
Code tells a lot. Let's migrate the above example to DataflowEx and see what it looks like:

```c#
using Gridsum.DataflowEx;
using System.Threading.Tasks.Dataflow;

public class AggregatorFlow : Dataflow<string>
{
    //Blocks
    private TransformBlock<string, KeyValuePair<string, int>> _splitter; 
    private ActionBlock<KeyValuePair<string, int>> _aggregater;

    //Data
    private Dictionary<string, int> _dict;

    public AggregatorFlow() : base(DataflowOptions.Default)
    {
        _splitter = new TransformBlock<string, KeyValuePair<string, int>>((Func<string, KeyValuePair<string, int>>)t
        _dict = new Dictionary<string, int>();
        _aggregater = new ActionBlock<KeyValuePair<string, int>>((Action<KeyValuePair<string, int>>)this.Aggregate);

        //Block linking
        _splitter.LinkTo(_aggregater, new DataflowLinkOptions() { PropagateCompletion = true });

        /* IMPORTANT */
        RegisterChild(_splitter);
        RegisterChild(_aggregater);
    }

    protected virtual void Aggregate(KeyValuePair<string, int> pair)
    {
        int oldValue;
        _dict[pair.Key] = this._dict.TryGetValue(pair.Key, out oldValue) ? oldValue + pair.Value : pair.Value;
    }

    protected virtual KeyValuePair<string, int> Split(string input)
    {
        string[] splitted = input.Split('=');
        return new KeyValuePair<string, int>(splitted[0], int.Parse(splitted[1]));
    }

    public override ITargetBlock<string> InputBlock { get { return _splitter; } }

    public IDictionary<string, int> Result { get { return _dict; } }
}
```
Though there seems to be more code, it is quite clear. We have a class AggregatorFlow representing our flow finally, which inherits from Dataflow&lt;TIn&gt; with type parameter **string**. This means the AggregatorFlow class reprensents a dataflow graph itself and accepts strings as input. 

In this form, dataflow blocks and data become class members. Block behaviors become class methods (which allows the outside to override!). We also implemented the abstract <kbd>InputBlock</kbd> property of Dataflow&lt;TIn&gt; and exposes our internal data as an extra Result property. 

Now let's come to the consumer side of the AggregatorFlow class:
```c#
var aggregatorFlow = new AggregatorFlow();
aggregatorFlow.InputBlock.Post("a=1");
aggregatorFlow.InputBlock.Post("b=2");
aggregatorFlow.InputBlock.Post("a=5");
aggregatorFlow.InputBlock.Complete();
await aggregatorFlow.CompletionTask;
Console.WriteLine("sum(a) = {0}", aggregatorFlow.Result["a"]); //prints sum(a) = 6
```
You see that we now operate on a single instance of AggregatorFlow, without knowing the implementation detail of it. This gives you the possibility to encapsulate complex dataflow logic in your Dataflow class and pass on your consumers a clean high-level view of the graph.

> **Note:** The Dataflow class exposes a **CompletionTask** property (just like IDataflowBlock.Completion) to represent the life of the whole dataflow. The whole dataflow won't complete till every single child block in the flow completes . Here in the example we await on the task to make sure the dataflow completes. More on this topic below.

By the way, Dataflow&lt;TIn&gt; provides some helper methods to boost productivity:
```c#
var aggregatorFlow = new AggregatorFlow();
await aggregatorFlow.ProcessAsync(new[] { "a=1", "b=2", "a=5" }, completeFlowOnFinish:true);
Console.WriteLine("sum(a) = {0}", aggregatorFlow.Result["a"]); //prints sum(a) = 6
```
It is now that easy with <kbd>ProcessAsync</kbd> :)

This is the basic idea of DataflowEx which empowers you with a fully functional handle of your dataflow graph. Find more in the following topics.
 
UNDER CONSTRUCTION..