Welcome to DataflowEx
===================
DataflowEx is a high-level dataflow framework redesigned on top of Microsoft TPL Dataflow library with Object-Oriented Programming in mind. It does not replace TPL Dataflow but provides reusability, abstraction and management for underlying dataflow blocks to make your life easier. You can get it on [Nuget.org](http://www.nuget.org/packages/Gridsum.DataflowEx/).

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
 
Understanding Dataflow class
-------------
Just like IDataflowBlock is the fundamental piece in TPL Dataflow, IDataflow is the counterpart in DataflowEx library. Take a look at the IDataflow design:
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

Does Dataflow class allow an orphan child that links to no one and is not linked to? Yes. Your graph need not be a fully-connected one if you wish. You can have 'islands'. But you take care of by yourself how these islands receives a completion signal. Please always ensure that completion signal will be correctly propagated along the dataflow chain when your job is done. If a child never gets a completion signal, the parent's CompletionTask will never come to an end.

> **Note:** Dataflow class doesn't require children to be connected. The dataflow network/linking is constructed at your wish. So if a children will not get completion signal automatically through linking, you need to manually complete it on some condition, or you can use **RegisterDependency()**. The orphan child will complete when all of its dependencies complete. RegisterDependency() has nothing to do with data. It only handles completion.

>**Tip:** To tell you the truth, Dataflow.LinkTo() also uses RegisterDependency() internally to achieve the 'WhenAll' behavior.

All right. Time for a demo to show the graph construction and complex completion propagation.

```C#
public class ComplexIntFlow : Dataflow<int>
{
    private ITargetBlock<int> _headBlock;
    public ComplexIntFlow() : base(DataflowOptions.Default)
    {
        Dataflow<int, int> node2 = DataflowUtils.FromDelegate<int, int>(i => i);
        Dataflow<int, int> node3 = DataflowUtils.FromDelegate<int, int>(i => i * -1)

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

        node1.LinkTo(printer);
        node2.LinkTo(printer);
        node3.LinkTo(printer);

        //Completion propagation: node1 ---> node2
        node2.RegisterDependency(node1);
        //Completion propagation: node1 + node2 ---> node3
        node3.RegisterDependency(node1);
        node3.RegisterDependency(node2);

        this.RegisterChild(node1);
        this.RegisterChild(node2);
        this.RegisterChild(node3);
        this.RegisterChild(printer, t => { 
            if (t.Status == TaskStatus.RanToCompletion) 
                Console.WriteLine("Printer done!");
        });

        this._headBlock = node1.InputBlock;
    }

    public override ITargetBlock<int> InputBlock { get { return this._headBlock; } }
}

//Consumer
var intFlow = new ComplexIntFlow();
await intFlow.ProcessAsync(new[] { 1, 2, 3});
```

Code tells :) In this example (1) Node1, Node2 and Node3 all flow to the printer node. (2) Node2's life cycle depends on Node1. (3) Node3 depends on Node1 and Node2. So when the graph comes to it completion, the order will be Node1 -> Node2 -> Node3 -> Printer. This is powered by linking and dependency registration built-in in DataflowEx. 

### 3. Logging is your friend

To help you better understand how DataflowEx works and sometimes diagnose your application, DataflowEx provides extensive logging where we think necessary. So you get insights of the blocks/dataflows managed by DataflowEx without tedious debugging. Please just check the log, which is always our first advice.

Technically [Common.Logging](http://www.nuget.org/packages/Common.Logging/) is used as the underlying logging framework so that it can easily be integrated to your own logging system with an adapter, no matter you are using Log4Net, NLog or Enterprise Library Logging. For detailed documentation on Common.Logging, please checkout [this link](http://netcommon.sourceforge.net/docs/2.1.0/reference/html/index.html).

I'll use NLog as an example to print out logging of ComplexIntFlow. In the app.config, configure a NLogLoggerFactoryAdapter to route common.logging messages to NLog.

```xml
<configuration>
  <configSections>
    <sectionGroup name="common">
      <section name="logging" type="Common.Logging.ConfigurationSectionHandler, Common.Logging" requirePermission="false" />
    </sectionGroup>
  </configSections>
  
  <common>
    <logging>
      <factoryAdapter type="Common.Logging.NLog.NLogLoggerFactoryAdapter, Common.Logging.NLog20">
        <arg key="configType" value="FILE" />
        <arg key="configFile" value="~/NLog.config" />
      </factoryAdapter>
    </logging>
  </common>
</configuration>
```

Meanwhile, set up NLog in you NLog.config:

```xml
<nlog xmlns="http://www.nlog-project.org/schemas/NLog.xsd"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">

  <variable name="logFormat" value="${date:format=yy/MM/dd HH\:mm\:ss} [${logger}].[${level}] ${message} ${exception:format=tostring} "/>

  <targets>
    <target xsi:type="Console" name="console" layout="${logFormat}"/>
    <target xsi:type="File" name ="file" fileName="Gridsum.DataflowEx.Demo.log" layout="${logFormat}" keepFileOpen="true"/>
  </targets>

  <rules>
    <logger name ="Gridsum.DataflowEx*" minlevel="Trace" writeTo="console,file"></logger>
  </rules>
</nlog>
```

Then the logging infomation will be written to both the console and a log file. 

> **Note:** Notice that "Gridsum.DataflowEx*" is used as the logger name in the rules section. This represents the logging categories used by DataflowEx. In fact, most logging of DataflowEx is recorded under category "Gridsum.DataflowEx" and a few uses subcategories with the prefix (e.g. "Gridsum.DataflowEx.Databases"). So an extra wildcard ‘*’ includes every message from the library.

O.K. Let's take a look of the log file after executing the ComplexIntFlow demo:

```
14/09/28 11:18:33 [Gridsum.DataflowEx].[Info] [ComplexIntFlow1]->[printer] now has 2 dependencies.  
14/09/28 11:18:33 [Gridsum.DataflowEx].[Info] [ComplexIntFlow1]->[printer] now has 3 dependencies.  
14/09/28 11:18:33 [Gridsum.DataflowEx].[Info] [ComplexIntFlow1]->[node3] now has 2 dependencies.  
14/09/28 11:18:33 [Gridsum.DataflowEx].[Info] [ComplexIntFlow1] Successfully pulled and posted 3 Int32s to [ComplexIntFlow1]->[node1].  
14/09/28 11:18:33 [Gridsum.DataflowEx].[Info] [ComplexIntFlow1] Finished reading from enumerable and posting to the dataflow.  
14/09/28 11:18:33 [Gridsum.DataflowEx].[Info] [ComplexIntFlow1] Telling myself there is no more input and wait for children completion  
14/09/28 11:18:33 [Gridsum.DataflowEx].[Info] [ComplexIntFlow1]->[node1] completed  
14/09/28 11:18:33 [Gridsum.DataflowEx].[Info] [ComplexIntFlow1]->[node2] completed  
14/09/28 11:18:33 [Gridsum.DataflowEx].[Info] [ComplexIntFlow1]->[node3] All of my dependencies are done. Completing myself.  
14/09/28 11:18:33 [Gridsum.DataflowEx].[Info] [ComplexIntFlow1]->[node3] completed  
14/09/28 11:18:33 [Gridsum.DataflowEx].[Info] [ComplexIntFlow1]->[printer] All of my dependencies are done. Completing myself.  
14/09/28 11:18:33 [Gridsum.DataflowEx].[Info] [ComplexIntFlow1]->[printer] completed  
14/09/28 11:18:33 [Gridsum.DataflowEx].[Info] [ComplexIntFlow1] completed  
```

Nice and clear, isn't it? The output also proves that dataflow dependency works correctly introduced in the last topic.

Logging provides more than lifecycle information. It includes **block/dataflow buffer information** as well, which is extremely helpful when checking dataflow health, finding bottlenecks and diagnosing deadlock problems. If you need flow-level buffer monitor, set DataflowOptions.FlowMonitorEnabled to true and then pass the DataflowOptions object to the constructor of dataflow classes. If you need block-level buffer information, set DataflowOptions.BlockMonitorEnabled to true. With everything set up, the Dataflow base class will start an asynchronous loop to check and log states of all its children with an given interval (the default interval is 10 seconds).

To demonstrate buffer monitoring logging, let's create a SlowFlow on purpose:

```c#
public class SlowFlow : Dataflow<string>
{
    private Dataflow<string, char> _splitter;
    private Dataflow<char> _printer;

    public SlowFlow(DataflowOptions dataflowOptions)
        : base(dataflowOptions)
    {
        _splitter = new TransformManyBlock<string, char>(new Func<string, IEnumerable<char>>(this.SlowSplit), 
            dataflowOptions.ToExecutionBlockOption())
            .ToDataflow(dataflowOptions, "SlowSplitter");

        _printer = new ActionBlock<char>(c => Console.WriteLine(c),
            dataflowOptions.ToExecutionBlockOption())
            .ToDataflow(dataflowOptions, "Printer");

        RegisterChild(_splitter);
        RegisterChild(_printer);

        _splitter.LinkTo(_printer);
    }

    private IEnumerable<char> SlowSplit(string s)
    {
        foreach (var c in s)
        {
            Thread.Sleep(1000); //slow down
            yield return c;
        }
    }

    public override ITargetBlock<string> InputBlock { get { return _splitter.InputBlock; } }
}

//consumer
var slowFlow = new SlowFlow( new DataflowOptions
            {
                FlowMonitorEnabled = true, 
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
```
The slow flow class has a slow splitter block if you see the Thread.Sleep(). Then we properly set some options when constructing a SlowFlow instance:

>1. **FlowMonitorEnabled** tells the flow to log its buffer status periodically. Since the dataflow option is normally passed on to its children. Child flows will also log their buffer status.
>2. **MonitorInterval** sets the time interval between two adjacent buffer status calls.
>3. **PerformanceMonitorMode** sets whether or not to output logging information if a flow has 0 items buffered. Setting it to **Verbose** will log empty flows while setting to **Succint** will not.

Let's have a look at the log after executing the above code:

```
14/09/29 11:38:00 [Gridsum.DataflowEx].[Info] [SlowFlow1] Telling myself there is no more input and wait for children completion  
14/09/29 11:38:02 [Gridsum.DataflowEx.PerfMon].[Debug] [SlowFlow1]->[SlowSplitter] has 3 todo items (in:3, out:0) at this moment.  
14/09/29 11:38:02 [Gridsum.DataflowEx.PerfMon].[Debug] [SlowFlow1] has 3 todo items (in:3, out:0) at this moment.  
14/09/29 11:38:02 [Gridsum.DataflowEx.PerfMon].[Debug] [SlowFlow1]->[Printer] has 0 todo items (in:0, out:0) at this moment.  
14/09/29 11:38:04 [Gridsum.DataflowEx.PerfMon].[Debug] [SlowFlow1]->[Printer] has 0 todo items (in:0, out:0) at this moment.  
14/09/29 11:38:04 [Gridsum.DataflowEx.PerfMon].[Debug] [SlowFlow1] has 3 todo items (in:3, out:0) at this moment.  
14/09/29 11:38:04 [Gridsum.DataflowEx.PerfMon].[Debug] [SlowFlow1]->[SlowSplitter] has 3 todo items (in:3, out:0) at this moment.  
14/09/29 11:38:06 [Gridsum.DataflowEx.PerfMon].[Debug] [SlowFlow1]->[Printer] has 0 todo items (in:0, out:0) at this moment.  
14/09/29 11:38:06 [Gridsum.DataflowEx.PerfMon].[Debug] [SlowFlow1] has 2 todo items (in:2, out:0) at this moment.  
14/09/29 11:38:06 [Gridsum.DataflowEx.PerfMon].[Debug] [SlowFlow1]->[SlowSplitter] has 2 todo items (in:2, out:0) at this moment.  
14/09/29 11:38:08 [Gridsum.DataflowEx.PerfMon].[Debug] [SlowFlow1]->[SlowSplitter] has 1 todo items (in:1, out:0) at this moment.  
14/09/29 11:38:08 [Gridsum.DataflowEx.PerfMon].[Debug] [SlowFlow1] has 1 todo items (in:1, out:0) at this moment.  
14/09/29 11:38:08 [Gridsum.DataflowEx.PerfMon].[Debug] [SlowFlow1]->[Printer] has 0 todo items (in:0, out:0) at this moment.  
14/09/29 11:38:10 [Gridsum.DataflowEx.PerfMon].[Debug] [SlowFlow1]->[Printer] has 0 todo items (in:0, out:0) at this moment.  
14/09/29 11:38:10 [Gridsum.DataflowEx.PerfMon].[Debug] [SlowFlow1] has 0 todo items (in:0, out:0) at this moment.  
14/09/29 11:38:10 [Gridsum.DataflowEx.PerfMon].[Debug] [SlowFlow1]->[SlowSplitter] has 0 todo items (in:0, out:0) at this moment.  
14/09/29 11:38:10 [Gridsum.DataflowEx].[Info] [SlowFlow1]->[SlowSplitter] completed  
14/09/29 11:38:10 [Gridsum.DataflowEx].[Info] [SlowFlow1]->[Printer] completed  
14/09/29 11:38:10 [Gridsum.DataflowEx].[Info] [SlowFlow1] completed  
```

Now we easily find out the bottle neck of the flow to be [SlowFlow1]->[SlowSplitter], which has more todo items than other modules.

What if you have a flow that contains many blocks? Fortunately there is another property on DataflowOptions you can set, **BlockMonitorEnabled**, to disclose block level buffer status. After setting it to true, you get logs like:

```
14/09/29 11:53:55 [Gridsum.DataflowEx.PerfMon].[Debug] [SlowFlow1]->[SlowSplitter]->(TransformManyBlock<String, Char>) has 3 todo items (in:3, out:0) at this moment.   
```

The evil block has nowhere to escape :)

> **Note:** DataflowEx provides a default DataflowOptions instance: DataflowOptions.Default. It has FlowMonitorEnabled set to true, BlockMonitorEnabled set to false, MonitorInterval as 10 seconds and PerformanceMonitorMode being Succint.

### 4. Error handling

If we look back to the very first Dataflow implementation, the AggregatorFlow class, there is a couple of conditions required for the input, in a silent way. For example, in the Split method:
```
protected virtual KeyValuePair<string, int> Split(string input)
{
    string[] splitted = input.Split('=');
    return new KeyValuePair<string, int>(splitted[0], int.Parse(splitted[1]));
}
```
If the input does not obey the "key={int}" format, an exception will be thrown. So, what is next? What will happen to the Dataflow graph?

DataflowEx takes a **fast-fail** approach on exception handling just like TPL Dataflow. When an exception is thrown, the low-level block ends to the Faulted state first. Then the Dataflow instance who is the parent of the failing block gets notified. It will immediately propagate the fatal error: notify its other children to shutdown immediately. After all its children is done/completed, the parent Dataflow also comes to its completion, with the original exception wrapped in the CompletionTask whose status is also Faulted.

In the case of Dataflow nesting, the exception will be raised all the way from leaf to root, according to the child registration tree. One thing to remember is that, **DataflowEx never ever swallows an unhandled exception**. The library propagates it, store it in the CompletionTask and finally throw it where the top level Dataflow's CompletionTask is awaited.

Thus for AggregatorFlow, if we pass in some invalid input (e.g. "a=badstring"), an unhandled exception is thrown on Main method (Exception will also be logged by DataflowEx internally):
```
Unhandled Exception: System.AggregateException: One or more errors occurred. ---> System.AggregateException: One or more
 errors occurred. ---> System.FormatException: Input string was not in a correct format.
   at System.Number.StringToNumber(String str, NumberStyles options, NumberBuffer& number, NumberFormatInfo info, Boolea
n parseDecimal)
   at System.Number.ParseInt32(String s, NumberStyles style, NumberFormatInfo info)
   at Gridsum.DataflowEx.Demo.AggregatorFlow.Split(String input) in c:\Users\karld_000\Documents\SourceTree\DataflowEx\G
ridsum.DataflowEx.Demo\AggregatorFlow.cs:line 41
   at System.Threading.Tasks.Dataflow.TransformBlock`2.ProcessMessage(Func`2 transform, KeyValuePair`2 messageWithId)
   at System.Threading.Tasks.Dataflow.TransformBlock`2.<>c__DisplayClass10.<.ctor>b__3(KeyValuePair`2 messageWithId)
   at System.Threading.Tasks.Dataflow.Internal.TargetCore`1.ProcessMessagesLoopCore()
--- End of stack trace from previous location where exception was thrown ---
   at System.Runtime.CompilerServices.TaskAwaiter.ThrowForNonSuccess(Task task)
   at System.Runtime.CompilerServices.TaskAwaiter.HandleNonSuccessAndDebuggerNotification(Task task)
   at Gridsum.DataflowEx.Exceptions.TaskEx.<AwaitableWhenAll>d__3`1.MoveNext() in c:\Users\karld_000\Documents\SourceTre
e\DataflowEx\Gridsum.DataflowEx\Exceptions\TaskEx.cs:line 59
--- End of stack trace from previous location where exception was thrown ---
   at System.Runtime.CompilerServices.TaskAwaiter.ThrowForNonSuccess(Task task)
   at System.Runtime.CompilerServices.TaskAwaiter.HandleNonSuccessAndDebuggerNotification(Task task)
   at Gridsum.DataflowEx.Exceptions.TaskEx.<AwaitableWhenAll>d__3`1.MoveNext() in c:\Users\karld_000\Documents\SourceTre
e\DataflowEx\Gridsum.DataflowEx\Exceptions\TaskEx.cs:line 59
   --- End of inner exception stack trace ---
   at Gridsum.DataflowEx.Exceptions.TaskEx.<AwaitableWhenAll>d__3`1.MoveNext() in c:\Users\karld_000\Documents\SourceTre
e\DataflowEx\Gridsum.DataflowEx\Exceptions\TaskEx.cs:line 71
--- End of stack trace from previous location where exception was thrown ---
   at System.Runtime.CompilerServices.TaskAwaiter.ThrowForNonSuccess(Task task)
   at System.Runtime.CompilerServices.TaskAwaiter.HandleNonSuccessAndDebuggerNotification(Task task)
   at Gridsum.DataflowEx.Dataflow.<GetCompletionTask>d__1c.MoveNext() in c:\Users\karld_000\Documents\SourceTree\Dataflo
wEx\Gridsum.DataflowEx\Dataflow.cs:line 345
--- End of stack trace from previous location where exception was thrown ---
   at System.Runtime.CompilerServices.TaskAwaiter.ThrowForNonSuccess(Task task)
   at System.Runtime.CompilerServices.TaskAwaiter.HandleNonSuccessAndDebuggerNotification(Task task)
   at Gridsum.DataflowEx.Demo.Program.<CalcAsync>d__6.MoveNext() in c:\Users\karld_000\Documents\SourceTree\DataflowEx\G
ridsum.DataflowEx.Demo\Program.cs:line 66
   --- End of inner exception stack trace ---
   at System.Threading.Tasks.Task.ThrowIfExceptional(Boolean includeTaskCanceledExceptions)
   at System.Threading.Tasks.Task.Wait(Int32 millisecondsTimeout, CancellationToken cancellationToken)
   at Gridsum.DataflowEx.Demo.Program.Main(String[] args) in c:\Users\karld_000\Documents\SourceTree\DataflowEx\Gridsum.
DataflowEx.Demo\Program.cs:line 50
```

Hence, if you think the exception is expected or tolerable, please catch it in the very beginning in the delegate passed to the low level block. Otherwise, the domino effect will spread and fail the whole dataflow graph. 

Summary
-------------
To sum up, DataflowEx enables you to write reusable components along with TPL Dataflow. Cool features include:

* Inheritance and polymorphism for dataflows and their hehaviors
* Block chain encapsulation as a reusable unit
* Easy conditional chaining 
* Automatic failure propagation within dataflow
* Built-in performance metrics monitor
* Auto complete support for circular dataflow graph
* Dataflow friendly sql bulk inserter
* Helper methods to convert raw blocks to dataflows

Simply download [Gridsum.DataflowEx](http://www.nuget.org/packages/Gridsum.DataflowEx/) and have a try!

DataflowEx In Depth
-------------

### 1. Advanced Linking

LinkTo() is the most powerful mechanism TPL Dataflow provides to help intuitive and efficient dataflow graph construction. There is also an overload of LinkTo() that accepts a predicate as the filter for conditional linking.

DataflowEx also provides similar LinkTo() at higher level (i.e. the Dataflow level), as you already see in previous demos. It uses block level LinkTo under the hood but you just easily operate on Dataflow nodes no matter how complex these node are internally. As completion propagation is concerned, DataflowEx's LinkTo() ensures completion propagation by default and goes beyond that: if A->C and B->C, Dataflow C will complete after both A and B complete. This feature has been mentioned in the previous Graph construction section but I want to re-emphasize here.

Anything more on linking? Yes. Let's consider how many times you have a dataflow generating a serious of base class objects and wants to redirect different child types to different specific components that only handles the child type? **LinkSubTypeTo()** is born for this:

```C#
//public void LinkSubTypeTo<TTarget>(IDataflow<TTarget> other) where TTarget : TOut

Dataflow<TIn, TOut> flow1;
Dataflow<TOutSubType1> flow2;
Dataflow<TOutSubType2> flow3;

flow1.LinkSubTypeTo(flow2);
flow1.LinkSubTypeTo(flow3);
```
LinkSubTypeTo() make our life so much easier :)

Internally LinkSubTypeTo() uses a even more powerful linking mechanism called **TransformAndLink()** which allows you to indicate a transform function on output objects as well as a filtering predicate. The transform function is a perfect bridge between output flow and downstream flows. It can be as simple as a type cast (LinkSubTypeTo), or a complex mapping function that converts, for example, a weakly-typed json object to your strongly typed domain objects.

> **Note:** Previously to implement TransformAndLink(), DataflowEx used an extra TransformBlock to achieve this. But starting from 1.0.9.6, we avoid the overhead of TransformBlock and push transform function down to the LinkPropagator level (a no-buffer block wrapper TPL dataflow uses to achieve link filtering), which brings better performance.

Let's look at a demo for TransformAndLink():

```C#
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
```

By using TransformAndLink, we direct odd numbers to d1 and even numbers to d2, and at the same time converts integers to meaningful strings to be printed out:

```
Even: 0
Even: 2
Even: 4
Even: 6
Even: 8
Odd: 1
Odd: 3
Odd: 5
Odd: 7
Odd: 9
```

The final linking helper in DataflowEx to introduce is **LinkLeftTo()**. When we use conditional linking extensively in our application, there is a common trap that if one output matches NONE of output linking predicates, it will remain forever in the upstream dataflow, and thus our dataflow graph never completes. To address the issue, DataflowEx trackes all predicates the dataflow used previously and allows you to redirect LEFT objects (i.e. output objects that match no predicates) to a given target:

Demo time:

```C#
public static async Task LinkLeftToDemo()
{
    var d1 = new BufferBlock<int>().ToDataflow(name: "IntGenerator");
    var d2 = new ActionBlock<int>(s => Console.WriteLine(s)).ToDataflow();
    var d3 = new ActionBlock<int>(s => Console.WriteLine(s)).ToDataflow();
    var d4 = new ActionBlock<int>(s => Console.WriteLine(s + "[Left]")).ToDataflow();

    d1.LinkTo(d2, _ => _ % 3 == 0);
    d1.LinkTo(d3, _ => _ % 3 == 1);
    d1.LinkLeftTo(d4); // same as d1.LinkTo(d4, _ => _ % 3 == 2);

    for (int i = 0; i < 10; i++)
    {
        d1.Post(i);
    }

    await d1.SignalAndWaitForCompletionAsync();
    await d2.CompletionTask;
    await d3.CompletionTask;
}
//output:
//1
//2[Left]
//5[Left]
//8[Left]
//0
//3
//4
//7
//6
//9
```
> **Tip:** In this demo, LinkLeftTo is based on the predicates previously used in LinkTo(). But to clarify, predicates used in TransformAndLink() will also be taken into account when DataflowEx calculates what stands for 'Left'.

Besides LinkLeftTo(), you could also use the handy **LinkLeftToNull()** if you just want to silently ignore those unmatched objects (will be linked to DataflowBlock.NullTarget<T>()). Or, you can use **LinkLeftToError()** if it is a fatal error if there is an output object that matches none predicate conditions. This is a quite useful diagnosing tip to make sure your graph doesn't have a linking leak.

> **Note:** When using LinkLeftToNull() there is an garbage **recorder** which counts different types of the ignored output. Override Dataflow.OnOutputToNull() to customize the behavior. Regarding recorders and the statistics you can get from recorder, we will dig into that later.
  
If we slightly modify the above demo to use LinkLeftToError():

```C#
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
```
Exception will be thrown and d1 fails as expected:

```
14/10/22 12:28:39 [Gridsum.DataflowEx].[Error] [IntGenerator] This is my error destination. Data should not arrive here: 2

Unhandled Exception: 14/10/22 12:28:39 [Gridsum.DataflowEx].[Error] [IntGenerator] Exception occur. Shutting down my chi
ldren... System.IO.InvalidDataException: An object came to error region of [IntGenerator]: 2
   at Gridsum.DataflowEx.Dataflow`2.<LinkLeftToError>b__5c(TOut survivor) in c:\Users\karld_000\Documents\SourceTree\Dat
aflowEx\Gridsum.DataflowEx\Dataflow.cs:line 846
   at System.Threading.Tasks.Dataflow.ActionBlock`1.ProcessMessageWithTask(Func`2 action, KeyValuePair`2 messageWithId)
--- End of stack trace from previous location where exception was thrown ---
   at System.Runtime.CompilerServices.TaskAwaiter.ThrowForNonSuccess(Task task)
   at System.Runtime.CompilerServices.TaskAwaiter.HandleNonSuccessAndDebuggerNotification(Task task)
   at Gridsum.DataflowEx.Exceptions.TaskEx.<AwaitableWhenAll>d__3`1.MoveNext() in c:\Users\karld_000\Documents\SourceTre
e\DataflowEx\Gridsum.DataflowEx\Exceptions\TaskEx.cs:line 59
...
```

### 2. Cyclic graph and ring completion detection

Your dataflow graph becomes really, really complicated when there is a circult after linking components properly according to your application logic. Consider a real world example, a web crawler, which has an http request maker component and a link analysis component: they consume messages from and provide resources to each other.

A cyclic graph, or a ring, is well supported by the linking mechanism of TPL Dataflow, **as far as data propagation is concerned**. Messages do flow fluently along the linking arrows, even if there is a ring. But when it comes to graph **completion**, things become tricky: A depends on B's output, so probably A shouldn't end until B does. Well, B also depends on A... So now, the graph never has a chance to complete? Hmmm...

You may want to shutdown one of the ring components manually and expect the completion propagates along the ring. But chances are that other components are still busy processing and producing new messages. When these new messages flow to the component that are already shut down, they cannot continue their trip and will stay in the buffer of working blocks. So some components never complete because of thier buffer status. So it is a dead lock.

Clearly, a subtle mechanism is needed to **automatically** shutdown one **proper** component **at the right time**. The right time is, when there is nothing left in the ring and every node is idle; 'Automatcially' means you could hardly tell when to start the ring-completion domino safely so a smart background scheduler is needed to pull the trigger at 'the right time'; 'Proper' means we should choose properly which node is the first to complete in the ring and its completion can lead to the full completion of the ring.

It is difficult that you implement this mechanism yourself. Luckily DataflowEx comes with a solution out of the box: **RegisterChildRing()**. It tells the Dataflow that some children is forming a ring and should enable the automatic smart auto-complete mechanism for the ring. Take a look at the method signature:

```C#
/// <summary>
/// Register a continuous ring completion check after the preTask ends.
/// When the checker thinks the ring is O.K. to shut down, it will pull the trigger 
/// of heartbeat node in the ring. Then nodes in the ring will shut down one by one
/// </summary>
/// <param name="preTask">The task after which ring check begins</param>
/// <param name="ringNodes">Child dataflows that forms a ring</param>
protected async void RegisterChildRing(Task preTask, params IRingNode[] ringNodes)
```

The preTask parameter gives you a way to delay the child ring monitoring loop until a pre-condition is satisfied. The ring nodes parameter array, as its name implies, is the child components that forms a ring. Notice that the required type of a ring node is IRingNode which inherits from IDataflow.

> **Note:** IRingNode only add one property **IsBusy** to IDataflow. As TPL Dataflow doesn't expose too much information of a running block, this interface helps the ring monitor to tell whether the dataflow/block is working (e.g. executing an item transform in the TransformBlock). So a typical implementation is to set IsBusy to true in the first line of the transform method and set it back to false in the last line. We know this is a bit tedious but it's the best we can do at this moment without Microsoft TPL Dataflow improvement.

O.K. Demo time again. Let's consider we have a heater and a more powerful cooler that changes the air temperature. At last, when the temperature drops below 0 degree the flow should quit.

```C#
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

public static async Task CircularFlowAutoComplete()
{
    var f = new CircularFlow(DataflowOptions.Default);
    f.Post(20);
    await f.SignalAndWaitForCompletionAsync();
}
```

As you can see, there is a circular dependency in the example graph which theoratically never ends. But RegisterChildRing() does some magic to ensure the flow will end after jobs are done.

> **Note:** RegisterChildRing() requires a heartbeat node in the ring to function correctly. You can use the HeartbeatNode<T> class to initialize an instance like the example does. This heartbeat node, together with IsBusy property mentioned above, helps the ring monitor to correctly count the status of underlying blocks in case the flow is still running (internally ring monitor use a 2-round ring scan to ensure correctness of the ring status). The heartbeat node is also the first node to complete in the completion chain.  

The output of the above code is listed below:

```
14/10/27 15:55:34 [Gridsum.DataflowEx].[Info] [Heater] now has 2 dependencies.
14/10/27 15:55:34 [Gridsum.DataflowEx].[Info] [CircularFlow1] A ring is set up: (Heater=>Cooler=>HeartBeat)
14/10/27 15:55:34 [Gridsum.DataflowEx].[Info] [CircularFlow1] Telling myself there is no more input and wait for childre
n completion
14/10/27 15:55:34 [Gridsum.DataflowEx].[Info] [CircularFlow1]->[NoOpBuffer] completed
14/10/27 15:55:34 [Gridsum.DataflowEx].[Info] [CircularFlow1] Ring pretask done. Starting check loop for (Heater=>Cooler
=>HeartBeat)
Heated to 11
Cooled to 9
Heated to 10
Cooled to 8
Heated to 9
Cooled to 7
Heated to 8
Cooled to 6
Heated to 7
Cooled to 5
Heated to 6
Cooled to 4
Heated to 5
Cooled to 3
Heated to 4
Cooled to 2
Heated to 3
Cooled to 1
Heated to 2
Cooled to 0
Heated to 1
Cooled to -1
14/10/27 15:55:44 [Gridsum.DataflowEx].[Debug] [CircularFlow1] 1st level empty ring check passed for (Heater=>Cooler=>He
artBeat)
14/10/27 15:55:54 [Gridsum.DataflowEx].[Debug] [CircularFlow1] 2nd level empty ring check passed for (Heater=>Cooler=>He
artBeat)
14/10/27 15:56:04 [Gridsum.DataflowEx].[Info] [CircularFlow1] Ring completion detected! Completion triggered on heartbea
t node: (Heater=>Cooler=>HeartBeat)
14/10/27 15:56:04 [Gridsum.DataflowEx].[Info] [CircularFlow1]->[HeartBeat] completed
14/10/27 15:56:04 [Gridsum.DataflowEx].[Info] [CircularFlow1]->[Heater] All of my dependencies are done. Completing myse
lf.
14/10/27 15:56:04 [Gridsum.DataflowEx].[Info] [CircularFlow1]->[Heater] completed
14/10/27 15:56:04 [Gridsum.DataflowEx].[Info] [CircularFlow1]->[Cooler] completed
14/10/27 15:56:04 [Gridsum.DataflowEx].[Info] [CircularFlow1] completed
```

DataflowEx provides extensive logging on cyclic flow events. Please don't forget to turn them on to help debugging.  

> **Note:** In this demo, the completion condition is relatively simple (temperature < 0) so yes it could be easily replaced by manual completion. But there are complex real world situations that are really hard to find an ending condition, especially when the count of output items of the transform-many function in some of the ring nodes is indeterministic. A spider is one of the complex examples. The DataflowEx built-in **DbDataJoiner** class is another one. These scenarios are where ring completion detection feature really shines.

### 3. Introducing StatisticsRecorder

Logging is our friend. It provides messages with detail to help diagnosing our applications. But sometimes we need **overviews** rather than detailed logging. This requirement is particularly important in a data flow because it might process billions of items per day and you don't want to drawn in the sea of details. For example, for a log parser program, you probably need an aggregater to sum up counts of error/warning messages in each category, rather than simply dumping everything into a single large log file.

That is why StatisticsRecorder is introduced, to be the built-in event counter/aggregator in DataflowEx, where you get overviews of your interest on your running dataflow. You can choose to access and dump statistics from the class at the end of your program, or at any other checkpoints.

StatisticsRecorder supports two kinds of recording: Type recording and event recording. Type recording is simply used to count the number of a particular type while event recording is an general-purpose extension feature where you can inject custom 'events' into the statistics recorder. 

> **Note:** StatisticsRecorder uses DataflowEvent class to store event information, which is composed of two strings: Level1 and Level2. Level1 is the parent node and Level2 is the child node of the hierarchy. StatisticsRecorder will aggregate event count on both attributes and supports queries like count(L1 = level1) and count(L1 = level1 && L2 = level2).

To record a type once, simply call StatisticsRecorder.RecordType(Type). To record an event, call RecordEvent(DataflowEvent) or its overloads. Both methods are thread-safe.

Built on top of RecordType() and RecordEvent(), StatisticsRecorder provides a Record(object) method that accepts any object:

```C#
/// <summary>
/// Records a processed object in dataflow pipeline
/// </summary>
/// <param name="instance">The object passing dataflow</param>
public virtual void Record(object instance)
{
    this.RecordType(instance.GetType());

    var eventProvider = instance as IEventProvider;
    if (eventProvider != null)
    {
        this.RecordEvent(eventProvider.GetEvent());
    }
}
```

In most cases you simply use Record() to monitor the object stream in your flow. It records the object type and extracts valuable event information wherever applicable. If the default implementation needs some adjustment in your scenario, don't forget the method is virtual.   

> **Note:** As shown, objects that carries event information should implement **IEventProvider** which allows *Record()* to get corresponding event information from the object. This is quite useful if you wish to include more information than just the type of the object in the statistics.

Let's look at a demo which processes person information stream. In this demo StatisticsRecorder not only keeps track of people count processed but also treats old person as special event.  

```C#
public class Person : IEventProvider
{
    public string Name { get; set; }
    public int Age { get; set; }

    public DataflowEvent GetEvent()
    {
        if (Age > 70)
        {
            return new DataflowEvent("OldPerson");
        }
        else
        {
            //returning empty so it will not be recorded as an event
            return DataflowEvent.Empty; 
        }
    }
}

public class PeopleFlow : Dataflow<string, Person>
{
    private TransformBlock<string, Person> m_converter;
    private TransformBlock<Person, Person> m_recorder;
    private StatisticsRecorder m_peopleRecorder;
    
    public PeopleFlow(DataflowOptions dataflowOptions)
        : base(dataflowOptions)
    {
        m_peopleRecorder = new StatisticsRecorder(this) { Name = "PeopleRecorder"};

        m_converter = new TransformBlock<string, Person>(s => JsonConvert.DeserializeObject<Person>(s));
        m_recorder = new TransformBlock<Person, Person>(
            p =>
                {
                    //record every person
                    m_peopleRecorder.Record(p);
                    return p;
                });

        m_converter.LinkTo(m_recorder, new DataflowLinkOptions() { PropagateCompletion = true});

        RegisterChild(m_converter);
        RegisterChild(m_recorder);
    }

    public override ITargetBlock<string> InputBlock { get { return m_converter; } }
    public override ISourceBlock<Person> OutputBlock { get { return m_recorder; } }
    public StatisticsRecorder PeopleRecorder { get { return m_peopleRecorder; } }
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
```
> **Tip:** Although StatisticsRecorder allows you to access its data by indexers programatically, *DumpStatistics()* is the most convenient way to print out beautiful formatted overview gathered by StatisticsRecorder. 

And its output:
```
Hello, I am aaron, 20
Hello, I am bob, 30
Hello, I am carmen, 80
Total people count: 4
[[PeopleFlow1]-PeopleRecorder] Entities: Person(4)
[[PeopleFlow1]-PeopleRecorder] Events: OldPerson(1)

[[PeopleFlow1]-GarbageRecorder] Entities: Person(1)
[[PeopleFlow1]-GarbageRecorder] Events:
```

As expected, people recorder captures the old person event as well as the total person object count. In addition, the *GarbageRecorder* is a handy statistics recorder embedded in the Dataflow class to monitor objects flowing to null target when using **LinkLeftToNull()**. It starts and functions implicitly. 

To sum up, StatisticsRecorder is the aggregation engine in DataflowEx for reporting/monitoring purpose. Feel free to extend it and enrich your dataflow statistics.

Built-in Components
-------------
Here is another big reason why many projects inside Gridsum use DataflowEx: its powerful built-in components. Provided as generic reusable Dataflow classes, you get the their power out of the box. Data bulk insertion, data branching, 

### 1. Bulk insertion support

The very 1st feature/component in DataflowEx we strongly recommend is the **DbBulkInserter** which enables bulk insertion to SQL Server. Though there are many ORM solutions out there but when it comes to insertion, they are just not designed to use the most efficient way: bulk insert. Using *SqlBulkCopy* internally, DbBulkInserter is born to solve the problem in a speedy way. And it nicely fits in dataflow style programming.

Inheriting from Dataflow<T>, DbBulkInserter is a generic class and accepts a generic parameter T, which is the type of your domain objects. Just connect your output flows to DbBulkInserter. Bulk insertion magic will then happen internally in DbBulkInserter. The only extra thing to do is to set up column mapping from your domain object properties to database table columns, taking advantage of C# attributes.

In the last demo, we have a PeopleFlow that outputs Person objects. Let's dump those objects to SQL Server!

```C#
//Please note the added attributes to the person class
public class Person : IEventProvider
{
    [DBColumnMapping("LocalDbTarget", "NameCol", "N/A", ColumnMappingOption.Mandatory)]
    public string Name { get; set; }

    [DBColumnMapping("LocalDbTarget", "AgeCol", -1, ColumnMappingOption.Optional)]
    public int? Age { get; set; }

    public DataflowEvent GetEvent()
    {
        if (Age > 70)
        {
            return new DataflowEvent("OldPerson");
        }
        else
        {
            //returning empty so it will not be recorded as an event
            return DataflowEvent.Empty; 
        }
    }
}

public static async Task BulkInserterDemo()
{
    string connStr;

    //initialize table
    using (var conn = LocalDB.GetLocalDB("People"))
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
    var dbInserter = new DbBulkInserter<Person>(connStr, "dbo.People", DataflowOptions.Default, "LocalDbTarget");
    f.LinkTo(dbInserter);

    f.Post("{Name: 'aaron', Age: 20}");
    f.Post("{Name: 'bob', Age: 30}");
    f.Post("{Age: 80}"); //Name will be default value: "N/A"
    f.Post("{Name: 'neo' }"); // Age will be default value: -1
    await f.SignalAndWaitForCompletionAsync();
    await dbInserter.CompletionTask;
}
```

Bulk insertion in .Net (namely SqlBulkCopy) used to be a very complex and heavyweight component to use. Now with DbBulkInserter, it's that easy! DbBulkInserter uses SqlBulkCopy internally and auto-generates the underlying property accessors to be used by SqlBulkCopy. 

Let's put some words on the parameters of DBColumnMapping attribute constructor.

* The 1st parameter is a string of your choice called 'destLabel' that defines a db target (i.e. your destination table). Mappings under same dest label form a group which defines a specific object relational mapper. Using different dest labels enables a type to be mapped to multiple table schemas: simply tag the attributes with multiple DBColumnMapping.
* The 2nd parameter is the column name of your destination table. That's it.
* The 3rd parameter is the default value to output instead if the property's value happen to be null. This works for reference types and nullable value types. 
* The last parameter of DBColumnMapping attribute constructor is a ColumnMappingOption, where you can indicate the mapping to be mandatory or optional. In the case of 'Optional', DataflowEx will simply ignore the given mapping (and warn you in the log) when a corresponding DB column is not found for the mapping. In the case of 'Mandatory', exception will be thrown. 
 
> **Tip:** The 'Optional' option could be quite useful when you want to maintain only one set of mappings (i.e. mappings with same destlabel) to match multiple tables that share a great part of their columns in common but has some minor schema difference.  

Now take a glimpse of what's been put in the table, just as expected (Notice the default values are also taking effect):

[[images/sqlserver_screenshot1.jpg]]

If you want more insights of how DbBulkInserter works, check the log where you get the internals of DbBulkInserter, especially how type properties are mapped to columns of a database table. 

```
14/11/13 17:55:08 [Gridsum.DataflowEx.Databases.TypeAccessor<Person>].[Debug] Populated column offset for DBColumnMapping: [ColName:NameCol, ColOffset:1, DefaultValue:N/A] on property node: Person->Name by table dbo.People  
14/11/13 17:55:08 [Gridsum.DataflowEx.Databases.TypeAccessor<Person>].[Debug] Populated column offset for DBColumnMapping: [ColName:AgeCol, ColOffset:2, DefaultValue:-1] on property node: Person->Age by table dbo.People  
14/11/13 17:55:08 [Gridsum.DataflowEx].[Info] [PeopleFlow1] Telling myself there is no more input and wait for children completion  
14/11/13 17:55:08 [Gridsum.DataflowEx].[Info] [PeopleFlow1] completed  
14/11/13 17:55:08 [Gridsum.DataflowEx.Databases].[Debug] [DbBulkInserter<Person>1] starts bulk-inserting 4 Person to db table dbo.People  
14/11/13 17:55:08 [Gridsum.DataflowEx.Databases].[Info] [DbBulkInserter<Person>1] bulk-inserted 4 Person to db table dbo.People  
14/11/13 17:55:08 [Gridsum.DataflowEx].[Info] [DbBulkInserter<Person>1] completed  
```

So, be sure to check the log to diagnose issues!

One demo is not enough to show the real power of DbBulkInserter, which supports recursive property expansion for complex custom type. Look at an example of deep mapping searching: 



In this case, type Order is the root type rather than Person. But it has a property whose type is Person. DbBulkInserter expands the property and grabs some mapping deep in the property tree.

One final point: DbBulkInserter is very careful about 'null's when generating deep property accessors like A.B.C. Since A.B could be null, DbBulkInserter generates something like 'A.B == null ?　D : (A.B.C ?? D)' rather than A.B.C ?? D (D is the default value defined on C's DBColumnMapping) to avoid NullReferenceException. This affects the performance, of course, especially when your property tree is tall. So DataflowEx gives you an attribute, **[NoNullCheck]**, to turn off the null check in the IL of the generated property accessor. Simply tag it on A.B and the null check is stripped out: only A.B.C ?? D is generated but you take the risk to guarantee A.B is not null (otherwise NullReferenceException will be thrown at runtime). 

In the last demo, if you are sure each of the Order objects has a non-null Customer propety, try tagging **NoNullCheck** like this:

 

### 2. DataBrancher
When beginners touch Microsoft TPL Dataflow, one thing they complain is that an item can only travel to one of the many destinations. This is due  to the design principle of TPL Dataflow but admittedly yes, there are scenarios this feature could be quite useful. That's why DataflowEx brings **DataBrancher** to make your life easier.

DataBrancher acts simply like a copy machine. When it is linked to multiple targets, whenever an item flows in, it passes the reference of the same item to multiple targets. Optionally you can indicate a clone function to DataBrancher if you want to copy the item before handing to targets.

Code talks: 

### 3. DataDispatcher
DataDispatcher also falls into the 'one source multi targets' category. But it is different from DataBrancher in several aspects:
>1. One input item only goes to one target
>2. Target dataflow nodes are created dynamically on demand, depending on the input items and the dispatch function.

You can use LinkTo but what if dynamic, 

demo

MultiDbBulkInserter

### 4. DbDataJoiner

Cautions and Best Practices
-------------

### 1. Building your own Dataflow<>

Don't forget to register child.
Tips Build your own dataflows / Design principle
create, register and link
block or dataflow? （and their linking) when should I use block level linking and when should I use flow level linking  

### 2. What you should know about DataflowOptions

One thing that we touched but haven't yet explored is the option class of DataflowEx.

when to use Default and when not

boundedcapacity on big load
total parallelism / parallelism setting

**DataflowOptions** and how to respect it (pass it on)

ToDataflow()

### 3. Avoid too many blocks

overhead: buffer, threading.  negligible

try your best to avoid simple blocks
fix wd return null problem using IDataflowEvent.
Performance considerations： don't have too many blocks.

Have a try now!
-------------
any issue contact karldodd , or publish on github forum



Still considering:
utils classes
Use linking stuff ([<i class="icon-upload"></i> Publish a document](#publish-a-document))
misc faqs

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