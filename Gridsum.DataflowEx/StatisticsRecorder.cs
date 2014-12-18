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
        protected readonly IDataflow m_parent;
        protected ConcurrentDictionary<Type, IntHolder> m_typeCounter = new ConcurrentDictionary<Type, IntHolder>();
        protected ConcurrentDictionary<DataflowEvent, IntHolder> m_eventCounter = new ConcurrentDictionary<DataflowEvent, IntHolder>(new DataflowEventComparer());

        /// <summary>
        /// Constructs a statistics recorder instance without a parent
        /// </summary>
        public StatisticsRecorder() : this(null)
        {
        }

        /// <summary>
        /// Constructs a statistics recorder instance
        /// </summary>
        /// <param name="parent">The dataflow that this recorder belongs to</param>
        public StatisticsRecorder(IDataflow parent)
        {
            this.m_parent = parent;
        }

        /// <summary>
        /// Get the total count of the given type recorded
        /// </summary>
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

        /// <summary>
        /// Get the total count of the given event.
        /// </summary>
        /// <param name="fr">The event (level2 is optional)</param>
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

        /// <summary>
        /// Get the total count of events with given level1 
        /// </summary>
        public int this[string level1]
        {
            get
            {
                return this[new DataflowEvent(level1)];
            }
        }

        /// <summary>
        /// Get the count of exceptions received by the recorder
        /// </summary>
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

        /// <summary>
        /// Total count of objects in all types received
        /// </summary>
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
        /// Name of this recorder
        /// </summary>
        public string Name { get; set; }

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

        /// <summary>
        /// Record once for a certain type 
        /// </summary>
        /// <param name="t">Type of the object passing dataflow</param>
        public void RecordType(Type t)
        {
            IntHolder intHolder;
            if (!m_typeCounter.TryGetValue(t, out intHolder))
            {
                intHolder = m_typeCounter.GetOrAdd(t, new IntHolder());    
            }
            intHolder.Increment();
        }

        /// <summary>
        /// Record once for a certain dataflow event
        /// </summary>
        /// <param name="level1">The first level information of the event</param>
        /// <param name="level2">The second level information of the event</param>
        public void RecordEvent(string level1, string level2 = null)
        {
            RecordEvent(new DataflowEvent(level1, level2));
        }

        /// <summary>
        /// Record once for a certain dataflow event 
        /// </summary>
        /// <param name="eventToAggregate">The event to record</param>
        public void RecordEvent(DataflowEvent eventToAggregate)
        {
            if (DataflowEvent.IsEmpty(eventToAggregate)) return; //ignore empty event

            IntHolder intHolder;
            if (!m_eventCounter.TryGetValue(eventToAggregate, out intHolder))
            {
                intHolder = this.m_eventCounter.GetOrAdd(eventToAggregate, new IntHolder());
            }
            intHolder.Increment();
        }

        /// <summary>
        /// Get a snapshot of current status of the internal type counter
        /// </summary>
        public ImmutableDictionary<Type, int> SnapshotTypeCounter()
        {
            return this.m_typeCounter.ToImmutableDictionary(kv => kv.Key, kv => kv.Value.Count);
        }

        /// <summary>
        /// Get a snapshot of current status of the internal event counter
        /// </summary>
        public ImmutableDictionary<DataflowEvent, int> SnapshotEventCounter()
        {
            return this.m_eventCounter.ToImmutableDictionary(kv => kv.Key, kv => kv.Value.Count);
        }

        /// <summary>
        /// Generate a multi-line string which describes the current aggregated statistics of this recorder
        /// </summary>
        /// <remarks>
        /// Typically you want to call this method at the end of your application to get
        /// an overview of how many objects are processed in your dataflow.
        /// </remarks>
        /// <returns>A multi-line statistics string</returns>
        public virtual string DumpStatistics()
        {
            string recorderName = this.Name ?? this.GetType().GetFriendlyName();

            if (m_parent != null)
            {
                recorderName = string.Format("{0}-{1}", m_parent.FullName, recorderName);
            }

            StringBuilder sb = new StringBuilder();

            sb.AppendFormat("[{0}] Entities:", recorderName);
            foreach (KeyValuePair<Type, int> keyValuePair in this.SnapshotTypeCounter())
            {
                if (!typeof(Exception).IsAssignableFrom(keyValuePair.Key))
                {
                    sb.Append(' ');
                    sb.Append(keyValuePair.Key.GetFriendlyName());
                    sb.Append('(');
                    sb.Append(keyValuePair.Value);
                    sb.Append(')');
                }
            }
            sb.AppendLine();

            sb.AppendFormat("[{0}] Events:", recorderName);
            foreach (KeyValuePair<DataflowEvent, int> keyValuePair in this.SnapshotEventCounter())
            {
                sb.Append(' ');
                sb.Append(keyValuePair.Key);
                sb.Append('(');
                sb.Append(keyValuePair.Value);
                sb.Append(')');
            }
            sb.AppendLine();

            return sb.ToString();
        }
        
        public virtual void Clear()
        {
            this.m_typeCounter.Clear();
            this.m_eventCounter.Clear();
        }
    }

    /// <summary>
    /// A value type which represents an 2-level event that you want <see cref="StatisticsRecorder"/> to aggregate
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

        /// <summary>
        /// The first level information of the event
        /// </summary>
        public string Level1 { get { return m_level1; } }

        /// <summary>
        /// The second level information of the event
        /// </summary>
        public string Level2 { get { return m_level2; } }

        /// <summary>
        /// String representation of the event
        /// </summary>
        public override string ToString()
        {
            return m_level2 == null ? m_level1 : string.Concat(m_level1, "-", m_level2);
        }

        /// <summary>
        /// An empty event that will not be recorded by statistics recorder
        /// </summary>
        public static DataflowEvent Empty = new DataflowEvent(null, null);

        /// <summary>
        /// Checks if the given dataflow event is an empty event
        /// </summary>
        /// <param name="flowEvent">The dataflow event to check</param>
        public static bool IsEmpty(DataflowEvent flowEvent)
        {
            return flowEvent.Level1 == null && flowEvent.Level2 == null;
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
