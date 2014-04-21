using System;

namespace Gridsum.DataflowEx.Exceptions
{
    public class PostToInputBlockFailedException : Exception
    {
        public PostToInputBlockFailedException(string message) : base(message)
        {
        }
    }
}
