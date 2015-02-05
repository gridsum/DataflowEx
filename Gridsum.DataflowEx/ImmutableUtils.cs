using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx
{
    using System.Collections.Immutable;
    using System.Threading;

    public static class ImmutableUtils
    {
        public static ImmutableList<T> AddOptimistically<T>(ref ImmutableList<T> list, T item)
        {
            ImmutableList<T> old, added;
            ImmutableList<T> beforeExchange;
            do
            {
                old = Volatile.Read(ref list);
                added = old.Add(item);
                beforeExchange = Interlocked.CompareExchange(ref list, added, old);

                //todo: reuse beforeExchange for next old value
            }
            while (beforeExchange != old);

            return added;
        }

        public static bool TryAddOptimistically<T>(ref ImmutableHashSet<T> set, T item)
        {
            ImmutableHashSet<T> old, added;
            ImmutableHashSet<T> beforeExchange;
            do
            {
                old = Volatile.Read(ref set);
                added = old.Add(item);

                if (object.ReferenceEquals(added, old))
                {
                    return false;
                }

                beforeExchange = Interlocked.CompareExchange(ref set, added, old);

                //todo: reuse beforeExchange for next old value
            }
            while (beforeExchange != old);

            return true;
        }

        public static bool TryAddManyOptimistically<T>(ref ImmutableHashSet<T> set, T[] items)
        {
            ImmutableHashSet<T> old, added;
            ImmutableHashSet<T> beforeExchange;
            do
            {
                old = Volatile.Read(ref set);

                var builder = old.ToBuilder();
                foreach (var item in items)
                {
                    builder.Add(item);
                }

                added = builder.ToImmutable();

                if (object.ReferenceEquals(added, old))
                {
                    return false; //added nothing (all items exists)
                }

                beforeExchange = Interlocked.CompareExchange(ref set, added, old);

                //todo: reuse beforeExchange for next old value
            }
            while (beforeExchange != old);

            return true;
        }
    }
}
