using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Gridsum.DataflowEx
{
    /// <summary>
    /// This class collects parsing result/information and aggregates parsing statistics. You may also override its Record() method
    /// to monitor what you need.
    /// </summary>
    public class StatisticsRecorder
    {
        public class IntHolder
        {
            private int m_count;
            public int Count
            {
                get { return m_count; }
            }

            public void Increment()
            {
                Interlocked.Increment(ref m_count);
            }
        }

        protected ConcurrentDictionary<Type, IntHolder> m_typeRecordDict = new ConcurrentDictionary<Type, IntHolder>();
        
        public int this[Type objectType]
        {
            get
            {
                IntHolder counter;
                if (m_typeRecordDict.TryGetValue(objectType, out counter))
                {
                    return counter.Count;
                }
                else
                {
                    int count = 0;
                    foreach (var subType in m_typeRecordDict.Keys.Where(k => k.IsSubclassOf(objectType) || k.GetInterfaces().Contains(objectType)))
                    {
                        count += m_typeRecordDict[subType].Count;
                    }
                    return count;
                }
            }
        }

        public int ExceptionCount
        {
            get
            {
                int count = 0;
                foreach (KeyValuePair<Type, IntHolder> keyValuePair in m_typeRecordDict)
                {
                    if (typeof(Exception).IsAssignableFrom(keyValuePair.Key))
                    {
                        count += keyValuePair.Value.Count;
                    }
                }
                return count;
            }
        }

        public int TotalCount
        {
            get
            {
                int count = 0;
                foreach (KeyValuePair<Type, IntHolder> keyValuePair in m_typeRecordDict)
                {
                    count += keyValuePair.Value.Count;
                }
                return count;
            }
        }

        public void RecordType(Type t)
        {
            var intHolder = m_typeRecordDict.GetOrAdd(t, new IntHolder());
            intHolder.Increment();
        }
        
        public KeyValuePair<Type, int>[] GetTypeDetail()
        {
            return m_typeRecordDict
                .Select(p => new KeyValuePair<Type, int>(p.Key, p.Value.Count))
                .ToArray();
        }
    }
}
