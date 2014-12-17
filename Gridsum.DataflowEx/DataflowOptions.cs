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
        /// <summary>
        /// A hint to the dataflow implementation about the in-memory buffer size of underlying blocks
        /// </summary>
        public int? RecommendedCapacity { get; set; }

        /// <summary>
        /// Whether the monitor logging is enabled for parent dataflow
        /// </summary>
        public bool FlowMonitorEnabled { get; set; }

        /// <summary>
        /// Whether the monitor logging is enabled for childrens of the dataflow
        /// </summary>
        public bool BlockMonitorEnabled { get; set; }
        
        /// <summary>
        /// The monitor logging mode 
        /// </summary>
        public PerformanceLogMode PerformanceMonitorMode { get; set; }

        /// <summary>
        /// A hint to dataflow implementation on parallelism of underlying block if feasible
        /// </summary>
        public int? RecommendedParallelismIfMultiThreaded { get; set; }

        private TimeSpan m_monitorInterval;

        /// <summary>
        /// The interval of the async monitor loop
        /// </summary>
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
            RecommendedCapacity = 100000,
            RecommendedParallelismIfMultiThreaded = Environment.ProcessorCount
        };

        private static DataflowOptions s_verboseOptions = new DataflowOptions()
        {
            BlockMonitorEnabled = true,
            FlowMonitorEnabled = true,
            PerformanceMonitorMode = PerformanceLogMode.Verbose,
            MonitorInterval = DefaultInterval,
            RecommendedCapacity = 100000
        };

        /// <summary>
        /// A predefined default setting for DataflowOptions
        /// </summary>
        public static DataflowOptions Default
        {
            get
            {
                return s_defaultOptions;
            }
        }

        /// <summary>
        /// A predefined verbose setting for DataflowOptions
        /// </summary>
        public static DataflowOptions Verbose
        {
            get
            {
                return s_verboseOptions;
            }
        }

        /// <summary>
        /// The default monitor interval, 10 seconds
        /// </summary>
        public static TimeSpan DefaultInterval
        {
            get
            {
                return TimeSpan.FromSeconds(10);
            }
        }

        /// <summary>
        /// Extract relative information from dataflow option to a block-level ExecutionDataflowBlockOptions 
        /// </summary>
        /// <param name="isBlockMultiThreaded">Whether the block using return value is multi-threaded</param>
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

        /// <summary>
        /// Extract relative information from dataflow option to a block-level GroupingDataflowBlockOptions 
        /// </summary>
        public GroupingDataflowBlockOptions ToGroupingBlockOption()
        {
            var option = new GroupingDataflowBlockOptions();

            if (this.RecommendedCapacity != null)
            {
                option.BoundedCapacity = this.RecommendedCapacity.Value;
            }
            
            return option;
        }

        /// <summary>
        /// Mode of performance logging
        /// </summary>
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
