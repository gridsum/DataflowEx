using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Gridsum.DataflowEx
{
    using System.Collections.Immutable;
    using System.Text;

    /// <summary>
    /// This class collects parsing result/information and aggregates parsing statistics. You may also override its Record() method
    /// to monitor what you need.
    /// </summary>
    /// <remarks>
    /// Dataflows always involve a lot of object processing. Use this class to monitor and aggregate objects processed by your dataflow graph
    /// so that you can get an overview how many jobs finished by the pipeline.
    /// Another scenario is for event counting/aggregation (using RecordEvent) to get an idea of how many warnings/errors happened.
    /// </remarks>
    public class StatisticsRecorder
    {
        protected ConcurrentDictionary<Type, IntHolder> m_typeCounter = new ConcurrentDictionary<Type, IntHolder>();
        protected ConcurrentDictionary<DataflowEvent, IntHolder> m_eventCounter = new ConcurrentDictionary<DataflowEvent, IntHolder>(new DataflowEventComparer());
        
        public int this[Type objectType]
        {
            get
            {
                IntHolder counter;
                if (this.m_typeCounter.TryGetValue(objectType, out counter))
                {
                    return counter.Count;
                }
                else
                {
                    int count = 0;
                    foreach (var subType in this.m_typeCounter.Keys.Where(k => k.IsSubclassOf(objectType) || k.GetInterfaces().Contains(objectType)))
                    {
                        count += this.m_typeCounter[subType].Count;
                    }
                    return count;
                }
            }
        }

        public int this[DataflowEvent fr]
        {
            get
            {
                IntHolder counter;
                if (this.m_eventCounter.TryGetValue(fr, out counter))
                {
                    return counter.Count;
                }
                else
                {
                    if (fr.Level2 == null)
                    {
                        //sum up all counters with same level1 if the query only has level1
                        return this.m_eventCounter.
                            Where(kvPair => kvPair.Key.Level1 == fr.Level1).
                            Sum(kvPair => kvPair.Value.Count);
                    }
                    else
                    {
                        return 0;    
                    }
                }
            }
        }

        public int this[string level1]
        {
            get
            {
                return this[new DataflowEvent(level1)];
            }
        }

        public int ExceptionCount
        {
            get
            {
                int count = 0;
                foreach (KeyValuePair<Type, IntHolder> keyValuePair in this.m_typeCounter)
                {
                    if (typeof(Exception).IsAssignableFrom(keyValuePair.Key))
                    {
                        count += keyValuePair.Value.Count;
                    }
                }
                return count;
            }
        }

        public int TotalTypeCount
        {
            get
            {
                int count = 0;
                foreach (KeyValuePair<Type, IntHolder> keyValuePair in this.m_typeCounter)
                {
                    count += keyValuePair.Value.Count;
                }
                return count;
            }
        }

        /// <summary>
        /// Records a processed object in dataflow pipeline
        /// </summary>
        /// <param name="instance">The object passing dataflow</param>
        public virtual void Record(object instance)
        {
            this.RecordType(instance.GetType());

            var eventProvider = instance as IEventProvider;
            if (eventProvider != null)
            {
                this.RecordEvent(eventProvider.GetEvent());
            }
        }

        public void RecordType(Type t)
        {
            IntHolder intHolder;
            if (!m_typeCounter.TryGetValue(t, out intHolder))
            {
                intHolder = m_typeCounter.GetOrAdd(t, new IntHolder());    
            }
            intHolder.Increment();
        }

        public void RecordEvent(string level1, string level2 = null)
        {
            RecordEvent(new DataflowEvent(level1, level2));
        }

        public void RecordEvent(DataflowEvent eventToAggregate)
        {
            IntHolder intHolder;
            if (!m_eventCounter.TryGetValue(eventToAggregate, out intHolder))
            {
                intHolder = this.m_eventCounter.GetOrAdd(eventToAggregate, new IntHolder());
            }
            intHolder.Increment();
        }

        public ImmutableDictionary<Type, int> SnapshotTypeCounter()
        {
            return this.m_typeCounter.ToImmutableDictionary(kv => kv.Key, kv => kv.Value.Count);
        }

        public ImmutableDictionary<DataflowEvent, int> SnapshotEventCounter()
        {
            return this.m_eventCounter.ToImmutableDictionary(kv => kv.Key, kv => kv.Value.Count);
        }

        public virtual string DumpStatistics()
        {
            string recorderName = GetFriendlyName(GetType());

            StringBuilder sb = new StringBuilder();

            sb.AppendFormat("[{0}] Entities:", recorderName);
            foreach (KeyValuePair<Type, IntHolder> keyValuePair in this.m_typeCounter)
            {
                if (!typeof(Exception).IsAssignableFrom(keyValuePair.Key))
                {
                    sb.Append(' ');
                    sb.Append(GetFriendlyName(keyValuePair.Key));
                    sb.Append('(');
                    sb.Append(keyValuePair.Value.Count);
                    sb.Append(')');
                }
            }
            sb.AppendLine();

            sb.AppendFormat("[{0}] Events:", recorderName);
            foreach (KeyValuePair<DataflowEvent, IntHolder> keyValuePair in this.m_eventCounter)
            {
                sb.Append(' ');
                sb.Append(keyValuePair.Key);
                sb.Append('(');
                sb.Append(keyValuePair.Value.Count);
                sb.Append(')');
            }
            sb.AppendLine();

            return sb.ToString();
        }

        private static string GetFriendlyName(Type type)
        {
            if (type.IsGenericType)
                return string.Format("{0}<{1}>", type.Name.Split('`')[0], string.Join(", ", type.GetGenericArguments().Select(GetFriendlyName)));
            else
                return type.Name;
        }

        public virtual void Clear()
        {
            this.m_typeCounter.Clear();
            this.m_eventCounter.Clear();
        }
    }

    /// <summary>
    /// Represents an 2-level event that you want <see cref="StatisticsRecorder"/> to aggregate for you
    /// </summary>
    public struct DataflowEvent
    {
        private string m_level1;
        private string m_level2;

        /// <summary>
        /// Constructs a DataflowEvent
        /// </summary>
        /// <param name="level1">The first level information of the event</param>
        /// <param name="level2">The second level information of the event</param>
        public DataflowEvent(string level1, string level2 = null)
        {
            m_level1 = level1;
            m_level2 = level2;
        }

        public string Level1 { get { return m_level1; } }
        public string Level2 { get { return m_level2; } }

        public override string ToString()
        {
            return m_level2 == null ? m_level1 : string.Concat(m_level1, "-", m_level2);
        }
    }

    internal class DataflowEventComparer : IEqualityComparer<DataflowEvent>
    {
        public bool Equals(DataflowEvent x, DataflowEvent y)
        {
            return x.Level1 == y.Level1 && x.Level2 == y.Level2;
        }

        public int GetHashCode(DataflowEvent obj)
        {
            int i1 = obj.Level1 == null ? 0 : obj.Level1.GetHashCode();
            int i2 = obj.Level2 == null ? 0 : obj.Level2.GetHashCode();
            return i1 ^ i2;
        }
    }

    /// <summary>
    /// Classes that will be recorded should consider implementing this interface if they 
    /// also represents an event to aggregate.
    /// </summary>
    public interface IEventProvider
    {
        DataflowEvent GetEvent();
    }
}
