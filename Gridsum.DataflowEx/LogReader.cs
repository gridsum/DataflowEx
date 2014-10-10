namespace Gridsum.DataflowEx
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Abstraction of a log reader dataflow that accepts strings as input lines. You should inherit from this class
    /// and construct your database graph inside the constructor.
    /// </summary>
    /// <remarks>
    /// The class comes with a powerful ProcessAsync() implemention which will pull text from readers and process the lines.
    /// If something goes wrong within the dataflow, ProcessAsync() will ensure the reading part is canceled as soon as possible.
    /// </remarks>
    public abstract class LogReader : Dataflow<string>
    {
        /// <summary>
        /// Constructs an instance of a log reader
        /// </summary>
        public LogReader(DataflowOptions dataflowOptions)
            : base(dataflowOptions)
        {
        }

        /// <summary>
        /// The recorder which records and aggregates the output of the log reader
        /// </summary>
        public abstract StatisticsRecorder Recorder { get; }

        /// <summary>
        /// Asynchronously pull from text reader into the log reader
        /// </summary>
        public Task<long> PullFromAsync(TextReader reader, CancellationToken ct)
        {
            return this.PullFromAsync(reader.ToEnumerable(), ct);
        }

        /// <summary>
        /// Asynchronously read from the text stream and process lines in the underlying dataflow.
        /// </summary>
        /// <param name="reader">The text reader to read from</param>
        /// <param name="completeLogReaderOnFinish">
        /// Whether a complete signal should be sent to the log reader dataflow. 
        /// If yes, it also ensures that the whole processing dataflow is completed before the ProcessAsync() task ends.
        /// Default to yes. Set the param to false if the log reader will read other text streams after this operation.
        /// </param>
        /// <returns>A task representing the state of the async operation which returns the total count of items processed in this method</returns>
        public virtual async Task<long> ProcessAsync(TextReader reader, bool completeLogReaderOnFinish = true)
        {
            return await ProcessAsync(reader.ToEnumerable(), completeLogReaderOnFinish).ConfigureAwait(false);
        }

        /// <summary>
        /// Asynchronously read from the text streams sequentially and process lines in the underlying dataflow.
        /// </summary>
        /// <param name="reader">The text readers to read from</param>
        /// <param name="completeLogReaderOnFinish">
        /// Whether a complete signal should be sent to the log reader dataflow. 
        /// If yes, it also ensures that the whole processing dataflow is completed before the ProcessMultipleAsync() task ends.
        /// Default to yes. Set the param to false if the log reader will read other text streams after this operation.
        /// </param>
        /// <returns>A task representing the state of the async operation which returns the total count of items processed in this method</returns>
        public virtual Task<long> ProcessMultipleAsync(IEnumerable<TextReader> readers, bool completeLogReaderOnFinish = true)
        {
            return ProcessMultipleAsync(readers.Select(DataflowUtils.ToEnumerable), completeLogReaderOnFinish);
        }
        
    }
}
