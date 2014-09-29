Welcome to DataflowEx
===================
DataflowEx is a collection of extensions to TPL Dataflow library with Object-Oriented Programming in mind. It does not replace TPL Dataflow but provides abstraction/management on top of dataflow blocks to make your life easier. You can get it on [Nuget.org](http://www.nuget.org/packages/Gridsum.DataflowEx/).

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

> **Note:** Notice that "Gridsum.DataflowEx*" is used as the logger name in the rules section. This represents the logging categories used by DataflowEx. In fact, most logging of DataflowEx is recorded under category "Gridsum.DataflowEx" and a few uses subcategories with the prefix (e.g. "Gridsum.DataflowEx.Databases"). So an extra wildcard ¡®*¡¯ includes every message from the library.

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

//example on 1 to M to 1?

Advanced DataflowEx
-------------

UNDER CONSTRUCTION..


### 5. Tips Build your own dataflows
create, register and link
block or dataflow?


Design principle

error prapagates to the root level


Btw, two level.

//example on 1 to M to 1?




todo: LinkSubTypeTo() & TransformAndLink()

Graph Linking()
LinkTo()


logging
**DataflowOptions** and how to respect it (pass it on)
dynamic registration

Lifecycle management

a powerful dispatch and gather example

UNDER CONSTRUCTION..

parallelism setting

Nesting
graph linking
graph
reuseable
life cycle management (normal exit, error handling)
performance monitor
Dataflow class 
Dataflow<TIn, TOut> class
Flow nesting
Common classes
DataBrancher
DataDispatcher
utils classes
Database bulk insertion
Ring support

Advanced topic:
Cyclic graph support
ETL: Db Data joiner
StatisticsRecorder
//RegisterDependency?
Performance considerations£º don't have too many blocks.

any issue contact karldodd

&lt;, and &amp;.

Old words:

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