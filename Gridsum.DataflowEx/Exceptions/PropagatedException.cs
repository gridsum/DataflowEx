using System;
using System.Collections.Generic;

namespace Gridsum.DataflowEx.Exceptions
{
    public abstract class PropagatedException : Exception
    {
        protected PropagatedException(string message) : base(message)
        {
        }
    }

    public class OtherBlockContainerFailedException : PropagatedException
    {
        public OtherBlockContainerFailedException() : base("Some exception from other block container brings me down")
        {
        }
    }

    public class OtherBlockFailedException : PropagatedException
    {
        public OtherBlockFailedException()
            : base("Some exception from other block in the same block container brings me down")
        {
        }
    }

    public class OtherBlockContainerCanceledException : PropagatedException
    {
        public OtherBlockContainerCanceledException()
            : base("Some other block container was canceled so I am down")
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

    public class ExceptionComparer : IComparer<Exception>
    {
        public int Compare(Exception x, Exception y)
        {
            if (x.GetType() == y.GetType())
            {
                return 0;
            }

            if (x is PropagatedException)
            {
                return -1;
            }

            if (y is PropagatedException)
            {
                return 1;
            }

            return 0;
        }
    }
}
