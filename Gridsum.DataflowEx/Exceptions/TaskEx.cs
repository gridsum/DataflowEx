using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Exceptions
{
    public static class TaskEx
    {
        public static ExceptionComparer s_ExceptionComparer = new ExceptionComparer();

        /// <summary>
        /// This impl is better than Task.WhenAll because it avoids meaningful exception being swallowed when awaiting on error
        /// </summary>
        public static Task AwaitableWhenAll(params Task[] tasks)
        {
            var whenall = Task.WhenAll(tasks);
            var tcs = new TaskCompletionSource<string>();

            whenall.ContinueWith(t =>
            {
                try
                {
                    if (t.IsFaulted)
                    {
                        var highPriorityException = t.Exception.Flatten().InnerExceptions.OrderByDescending(e => e, s_ExceptionComparer).First();
                        tcs.SetException(highPriorityException);
                    }
                    else if (t.IsCanceled)
                    {
                        tcs.SetCanceled();
                    }
                    else
                    {
                        tcs.SetResult(string.Empty);
                    }
                }
                catch (Exception e)
                {
                    tcs.SetException(e);
                }
            });

            return tcs.Task;
        }

    }
}
