using Common.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Demo
{
    using System.IO;
    using System.Threading.Tasks.Dataflow;

    public class MyLog
    {
        public LogLevel Level { get; set; }
        public string Message { get; set; }
    }

    /// <summary>
    /// Logger that accepts logs and dispatch them to appropriate dynamically created log writer
    /// </summary>
    public class MyLogger : DataDispatcher<MyLog, LogLevel>
    {
        public MyLogger() : base(log => log.Level)
        {
        }
        
        /// <summary>
        /// This function will only be called once for each distinct dispatchKey (the first time)
        /// </summary>
        protected override Dataflow<MyLog> CreateChildFlow(LogLevel dispatchKey)
        {
            //dynamically create a log writer by the dispatchKey (i.e. the log level)
            var writer = new LogWriter(string.Format(@".\MyLogger-{0}.log", dispatchKey));

            //no need to call RegisterChild(writer) here as DataDispatcher will call automatically
            return writer;
        }
    }

    /// <summary>
    /// Log writer node for a single destination file
    /// </summary>
    internal class LogWriter : Dataflow<MyLog>
    {
        private readonly ActionBlock<MyLog> m_writerBlock;
        private readonly StreamWriter m_writer;

        public LogWriter(string fileName) : base(DataflowOptions.Default)
        {
            this.m_writer = new StreamWriter(new FileStream(fileName, FileMode.Append));

            m_writerBlock = new ActionBlock<MyLog>(log => m_writer.WriteLine("[{0}] {1}", log.Level, log.Message));

            RegisterChild(m_writerBlock);
        }

        public override ITargetBlock<MyLog> InputBlock { get { return m_writerBlock; } }

        protected override void CleanUp(Exception e)
        {
            base.CleanUp(e);
            m_writer.Flush();
        }
    }
}
