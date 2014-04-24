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

    public class LinkedContainerFailedException : PropagatedException
    {
        public LinkedContainerFailedException()
            : base("Some other block container went wrong so I am down")
        {
        }
    }

    public class SiblingUnitFailedException : PropagatedException
    {
        public SiblingUnitFailedException()
            : base("Some sibling went wrong so I am down")
        {
        }
    }

    public class LinkedContainerCanceledException : PropagatedException
    {
        public LinkedContainerCanceledException()
            : base("Some other block container was canceled so I am down")
        {
        }
    }

    public class SiblingUnitCanceledException : Exception
    {
        public SiblingUnitCanceledException()
            : base("Some sibling was canceled so I am down")
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
