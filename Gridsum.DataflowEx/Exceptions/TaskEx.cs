using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Exceptions
{
    using System.Collections.Immutable;

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
                        var highPriorityException = UnwrapWithPriority(t.Exception);
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

        public static async Task AwaitableWhenAll<T>(Func<ImmutableList<T>> listGetter, Func<T, Task> itemCompletion)
        {
            ImmutableList<T> childrenSnapShot;
            Exception cachedException = null;
            do
            {
                childrenSnapShot = listGetter();
                try
                {
                    await AwaitableWhenAll(childrenSnapShot.Select(itemCompletion).ToArray()).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    cachedException = e;
                }
            }
            while (!object.ReferenceEquals(listGetter(), childrenSnapShot));

            if (cachedException != null)
            {
                //should not throw cacheException directly here, otherwise the original stack trace is lost.
                throw new AggregateException(cachedException);
            }
        }

        public static Exception UnwrapWithPriority(AggregateException ae)
        {
            if (ae == null)
            {
                throw new ArgumentNullException("ae");
            };

            return ae.Flatten().InnerExceptions.OrderByDescending(e => e, s_ExceptionComparer).FirstOrDefault() ?? ae;
        }
    }
}
