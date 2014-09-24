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

There are two important calls to RegisterChild() in the constructor. We will come back to this later.

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

By the way, Dataflow&lt;TIn&gt; provides some helper methods to boost productivity. To achieve the same effect:
```c#
var aggregatorFlow = new AggregatorFlow();
await aggregatorFlow.ProcessAsync(new[] { "a=1", "b=2", "a=5" }, completeFlowOnFinish:true);
Console.WriteLine("sum(a) = {0}", aggregatorFlow.Result["a"]); //prints sum(a) = 6
```
It is now that easy with <kbd>ProcessAsync</kbd> as DataflowEx handles the tedious Post-and-complete boilerplate code for you :)

This is the basic idea of DataflowEx which empowers you with a fully functional handle of your dataflow graph. Find more in the following topics.
 
Understanding class Dataflow 
-------------
IDataflowBlock is the fundamental piece in TPL Dataflow while IDataflow is the counterpart in DataflowEx. Take a look at the IDataflow design:
```c#
public interface IDataflow
{
    IEnumerable<IDataflowBlock> Blocks { get; }
    Task CompletionTask { get; }
    void Fault(Exception exception);
    string Name { get; }    
}

public interface IDataflow<in TIn> : IDataflow
{
    ITargetBlock<TIn> InputBlock { get; }
}

public interface IOutputDataflow<out TOut> : IDataflow
{
    ISourceBlock<TOut> OutputBlock { get; }
    void LinkTo(IDataflow<TOut> other);
}

public interface IDataflow<in TIn, out TOut> : IDataflow<TIn>, IOutputDataflow<TOut>
{
}
```
IDataflow looks like IDataflowBlock, doesn't it? Well, remember IDataflow now represents a dataflow graph which may contain one or more low-level blocks. We think a graph may have inputs and outputs so those strongly-typed generic IDataflows are designed. 

> **Note:** If you see IOutputDataflow&lt;TOut&gt;.**LinkTo**(IDataflow&lt;TOut&gt; other), congratulations to you as you find out the API supports (and encourages) graph level data linking.

So on top of IDataflow there is an implementation called **Dataflow**, which should be the base class for all DataflowEx flows. Besides acting as the handle of the graph, it has many useful functionalities built-in. Let's explore them one by one.

### 1. Lifecycle management

A key role of the Dataflow base class is to monitor the health of its children and provides a single completion state to the outside, namely the CompletionTask property.

So first things first, we need a way to tell the dataflow who is its child. That is done through the **Dataflow.RegisterChild** method (you have seen it in the last example). Dataflow class will now keep the reference of the child in its internal data structure and the lifecycle of child will start to affect its parent.

> **Note:** RegisterChild() method is not restricted to be called inside dataflow constructor. In fact, it can be used wherever necessary. Dataflow class uses a smart mechanism here to ensure dynamically registered child will affect the Dataflow's CompletionTask, even if you acquire the CompletionTask reference beforehand. This feature empowers the scenario that your dataflow is changing its shape at runtime. Just don't forget to call RegisterChild() when new child is created on demand.

There are 2 kinds of child that you can register, a dataflow block or a sub dataflow. The latter means Dataflow nesting is supported! Feel free to build different levels of dataflow components to provide even better modularity and encapsulation. Let's look at an example:
```C#
using System.Threading.Tasks.Dataflow;
using Gridsum.DataflowEx;

public class LineAggregatorFlow : Dataflow<string>
{
    private Dataflow<string, string> _lineProcessor;
    private AggregatorFlow _aggregator;
    public LineAggregatorFlow() : base(DataflowOptions.Default)
    {
        _lineProcessor = new TransformManyBlock<string, string>(line => line.Split(' ')).ToDataflow();
        _aggregator = new AggregatorFlow();
        _lineProcessor.LinkTo(_aggregator);
        RegisterChild(_lineProcessor);
        RegisterChild(_aggregator);
    }

    public override ITargetBlock<string> InputBlock { get { return _lineProcessor.InputBlock; } }
    public int this[string key] { get { return _aggregator.Result[key]; } }
}

//consumer here
var lineAggregator = new LineAggregatorFlow();
await lineAggregator.ProcessAsync(new[] { "a=1 b=2 a=5", "c=6 b=8" });
Console.WriteLine("sum(a) = {0}", lineAggregator["a"]); //prints sum(a) = 6
```
The example builds a LineAggregatorFlow on top of the existing AggregatorFlow to provide further functionalities. You get the idea how existing modules can be reused and seamlessly integrated to build a clearly-designed sophisticated dataflow graph.

> **Tip:** Notice that instead of creating a Dataflow class for <kbd>_lineProcessor</kbd>, IPropagatorBlock&lt;TIn, TOut&gt;.ToDataflow() is used to avoid class creation as we just want a trivial wrapper over the delegate. This extension method is defined in DataflowUtils.cs where there are more helpers to convert from blocks and delegates to Dataflows.

Back to the topic of lifecycle, when a child is registered (no matter it is a block or sub flow), how will it affect the parent? The following rules answer the question:

- The parent comes to its completion only when **all** of its children completes.
- The parent fails if **any** of its children fails.
- When one of the children fails, the parent will notify and wait other children to shutdown, and then comes to Faulted state.

So, in this form, the parent takes care of each child to guarantee nothing is wrong. Whenever something bad happens, the parent dataflow takes a fast-fail approach while those normal children still have a chance to gracefully stop.

> **Tip:** To provide custom shutdown behavior on sibling failure, override Dataflow.Fault().

This is all about the lifecyle management. A parent keeps his child under umbrella and never leaves any baby behind. 

### 2. Graph construction

Normally you construct your graph in the constructor of your own dataflow class which inherits from Dataflow. There are typically 3 steps to construct a dataflow graph:

>1. Create dataflow blocks or sub-flow instances
>2. Connect flows and blocks to shape a network
>3. Register flows and blocks as children

As you can see, previous examples all follows the same pattern. 

The 2nd step is worth digging into here. If you are dealing with raw blocks, ISourceBlock.LinkTo is your friend. And probably you want to set DataflowLinkOptions.PropagateCompletion to true if you want completion to be passed down automatically. This is traditional TPL Dataflow linking, as demonstrated by class <kbd>AggregatorFlow</kbd>.

> **Tip:** When programming TPL Dataflow, how many times do you find your blocks never complete? And how many times do you find out the reason to be forgetting to set PropagateCompletion? :) 

But the real connecting power resides in the dataflow level connecting. DataflowEx put some effort here to provide a number of utilites and best practices to help graph construction: Dataflow classes have rich linking APIs. So you don't bother call block-level linking any more.

The first to mention is IOutputDataflow.LinkTo (as demonstrated in <kbd>LineAggregatorFlow</kbd>), counterpart of the low level ISourceBlock.LinkTo. As its name implies, it connects the output block to the input block of the given parameter, and **propagates completion by default**. DataflowEx encourages completion propagation.

There is one more secret about IOutputDataflow&lt;TOut&gt;.LinkTo (default implementation in Dataflow&lt;TIn, TOut&gt;): it supports one target dataflow being linked-to multiple times and **guarantees the target dataflow receives a completion signal only when ALL upstream dataflows complete**. Notice this behavior is different from block level linking with PropagateCompletion set to true, which means the target block receives a completion signal when **any** of the upstream blocks completes. We think our choice is what you need in most cases.

Does Dataflow class allow an orphan child that links to no one and is not linked to? Yes. Your graph need not be a fully-connected one if you wish. You can have 'islands'. But be careful to how these islands receives a completion signal. Always ensure that completion signal should be correctly propagated along the dataflow chain when your job is done. If a child never gets a completion signal, the parent's CompletionTask will never come to an end.

> **Note:** Dataflow class doesn't require children to be connected. The dataflow network/linking is constructed at your wish. So if a children will not get completion signal automatically through linking, you need to manually complete it on some condition, or you can use **RegisterDependency()**. The orphan child will complete when all of its dependencies complete. RegisterDependency() has nothing to do with data. It only handles completion.

>**Tip:** To tell you the truth, Dataflow.LinkTo() also uses RegisterDependency() internally to achieve the 'WhenAll' behavior.

UNDER CONSTRUCTION..