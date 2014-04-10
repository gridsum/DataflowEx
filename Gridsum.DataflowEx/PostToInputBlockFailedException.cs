using System;

namespace Gridsum.DataflowEx
{
    public class PostToInputBlockFailedException : Exception
    {
        public PostToInputBlockFailedException(string message) : base(message)
        {
        }
    }
}
