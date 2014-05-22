using System;

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

        public TimeSpan? MonitorInterval { get; set; }

        private static DataflowOptions s_defaultOptions = new DataflowOptions()
        {
            BlockMonitorEnabled = false,
            FlowMonitorEnabled = true,
            PerformanceMonitorMode = PerformanceLogMode.Succinct,
            MonitorInterval = TimeSpan.FromSeconds(10),
            RecommendedParallelismIfMultiThreaded = Environment.ProcessorCount
        };

        public static DataflowOptions Default
        {
            get
            {
                return s_defaultOptions;
            }
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
