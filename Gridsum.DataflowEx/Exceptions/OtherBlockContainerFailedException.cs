using System;

namespace Gridsum.DataflowEx.Exceptions
{
    public class OtherBlockContainerFailedException : Exception
    {
        public OtherBlockContainerFailedException() : base("Some exception from other block container brings me down")
        {
        }
    }

    public class OtherBlockFailedException : Exception
    {
        public OtherBlockFailedException()
            : base("Some exception from other block in the same block container brings me down")
        {
        }
    }

    public class OtherBlockCanceledException : Exception
    {
        public OtherBlockCanceledException()
            : base("Some other block was canceled so I am down")
        {
        }
    }

    public class NoBlockRegisteredException : Exception
    {
        public NoBlockRegisteredException(BlockContainer blockContainer) : base("No block has been registered in container " + blockContainer.Name)
        {
        }
    }
}
