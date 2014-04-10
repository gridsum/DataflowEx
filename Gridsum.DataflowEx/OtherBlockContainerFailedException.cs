using System;

namespace Gridsum.DataflowEx
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
}
