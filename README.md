DataflowEx
==========

Gridsum's Object-Oriented extensions to TPL Dataflow library

TPL Dataflow is great. But the low-level fundamental blocks are a bit tedious to use in real world scenarioes because 
(1) Blocks are sealed and only accept delegates, which looks awkward in the Object-Oriented world where we need to maintain mutable states and reuse our data processing logic. Ever found it difficult to build a reusable library upon TPL Dataflow? 
(2) Blocks needs to be chained carefully and a bit tediously (also don't forget to link conditionally to null target otherwise the flow will never end..). These boilerplate codes are far from graceful but inevitable due to the non-OO design.
(3) Consider you have a block chain which does a particular task. It is difficult to provide a single reusable processing unit to the outside. Whenever you need it, you build a whole chain from ground up.

By introducing the core concept of BlockContainer, Gridsum.DataflowEx is born to solve all this with an OO design on top of TPL Dataflow. You can now easily write reusable components with extension points along with TPL Dataflow! Cool features include:

* Inheritance and polymorphism for block containers and their hehaviors
* Block chain encapsulation as a reusable unit
* Easy conditional chaining 
* Upstream failure propagation within block container
* Built-in performance metrics monitor
* Auto complete support for circular dataflow graph (NEW!)
* Helper methods to convert raw blocks to block containers

Simply download Gridsum.DataflowEx and look at the examples!