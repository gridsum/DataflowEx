using System;

namespace Gridsum.DataflowEx.Exceptions
{
    public class PostToBlockFailedException : Exception
    {
        public PostToBlockFailedException(string message) : base(message)
        {
        }
    }
}
