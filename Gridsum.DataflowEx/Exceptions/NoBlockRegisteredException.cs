using System;

namespace Gridsum.DataflowEx.Exceptions
{
    public class NoBlockRegisteredException : Exception
    {
        public NoBlockRegisteredException(BlockContainer blockContainer) : base("No block has been registered in container " + blockContainer.Name)
        {
        }
    }
}