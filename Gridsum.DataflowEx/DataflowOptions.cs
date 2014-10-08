using System;
using System.Threading.Tasks.Dataflow;

namespace Gridsum.DataflowEx
{
    /// <summary>
    /// Provides hints and configurations to dataflows. 
    /// </summary>
    /// <remarks>
    /// This class provides hints & suggestions. The corrent adoption of the configurations depends
    /// on the dataflow implementations.
    /// </remarks>
    public class DataflowOptions
    {
        public int? RecommendedCapacity { get; set; }
        public bool FlowMonitorEnabled { get; set; }
        public bool BlockMonitorEnabled { get; set; }
        public PerformanceLogMode PerformanceMonitorMode { get; set; }

        /// <summary>
        /// A hint to dataflow implementation on parallelism of underlying block if feasible
        /// </summary>
        public int? RecommendedParallelismIfMultiThreaded { get; set; }

        private TimeSpan m_monitorInterval;
        public TimeSpan MonitorInterval
        {
            get
            {
                return m_monitorInterval == TimeSpan.Zero ? DefaultInterval : m_monitorInterval;
            }
            set
            {
                this.m_monitorInterval = value;
            }
        }

        private static DataflowOptions s_defaultOptions = new DataflowOptions()
        {
            BlockMonitorEnabled = false,
            FlowMonitorEnabled = true,
            PerformanceMonitorMode = PerformanceLogMode.Succinct,
            MonitorInterval = DefaultInterval,
            RecommendedCapacity = 100000
        };

        private static DataflowOptions s_verboseOptions = new DataflowOptions()
        {
            BlockMonitorEnabled = true,
            FlowMonitorEnabled = true,
            PerformanceMonitorMode = PerformanceLogMode.Verbose,
            MonitorInterval = DefaultInterval,
            RecommendedCapacity = 100000
        };

        public static DataflowOptions Default
        {
            get
            {
                return s_defaultOptions;
            }
        }

        public static DataflowOptions Verbose
        {
            get
            {
                return s_verboseOptions;
            }
        }

        public static TimeSpan DefaultInterval
        {
            get
            {
                return TimeSpan.FromSeconds(10);
            }
        }

        public ExecutionDataflowBlockOptions ToExecutionBlockOption(bool isBlockMultiThreaded = false)
        {
            var option = new ExecutionDataflowBlockOptions();

            if (this.RecommendedCapacity != null)
            {
                option.BoundedCapacity = this.RecommendedCapacity.Value;
            }

            if (isBlockMultiThreaded)
            {
                //todo: modify the default 
                option.MaxDegreeOfParallelism = this.RecommendedParallelismIfMultiThreaded ?? Environment.ProcessorCount;
            }

            return option;
        }

        public GroupingDataflowBlockOptions ToGroupingBlockOption()
        {
            var option = new GroupingDataflowBlockOptions();

            if (this.RecommendedCapacity != null)
            {
                option.BoundedCapacity = this.RecommendedCapacity.Value;
            }
            
            return option;
        }

        public enum PerformanceLogMode
        {
            /// <summary>
            /// Only dump performance statistics for dataflow/block when it has non-zero buffer count
            /// </summary>
            Succinct = 0,

            /// <summary>
            /// Always dump performance statistics for dataflow/block
            /// </summary>
            Verbose = 1
        }
    }
}
