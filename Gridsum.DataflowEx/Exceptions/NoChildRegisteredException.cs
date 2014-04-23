using System;

namespace Gridsum.DataflowEx.Exceptions
{
    public class NoChildRegisteredException : Exception
    {
        public NoChildRegisteredException(BlockContainer blockContainer) : base("No child has been registered in container " + blockContainer.Name)
        {
        }
    }
}