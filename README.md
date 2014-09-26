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