using System;

namespace Gridsum.DataflowEx.Exceptions
{
    public class NoChildRegisteredException : Exception
    {
        public NoChildRegisteredException(Dataflow dataflow) : base("No child has been registered in " + dataflow.FullName)
        {
        }
    }
}