using System;

namespace Gridsum.DataflowEx
{
    public class BlockContainerOptions
    {
        public int? RecommendedCapacity { get; set; }
        public bool ContainerMonitorEnabled { get; set; }
        public bool BlockMonitorEnabled { get; set; }
        public PerformanceLogMode PerformanceMonitorMode { get; set; }
        public int? RecommendedParallelismIfMultiThreaded { get; set; }
        public TimeSpan? MonitorInterval { get; set; }

        private static BlockContainerOptions s_defaultOptions = new BlockContainerOptions()
        {
            BlockMonitorEnabled = false,
            ContainerMonitorEnabled = true,
            PerformanceMonitorMode = PerformanceLogMode.Succinct,
            MonitorInterval = TimeSpan.FromSeconds(10),
            RecommendedParallelismIfMultiThreaded = Environment.ProcessorCount
        };

        public static BlockContainerOptions Default
        {
            get
            {
                return s_defaultOptions;
            }
        }

        public enum PerformanceLogMode
        {
            /// <summary>
            /// Only dump performance statistics for container/block when it has non-zero buffer count
            /// </summary>
            Succinct = 0,

            /// <summary>
            /// Always dump performance statistics for container/block
            /// </summary>
            Verbose = 1
        }
    }
}
