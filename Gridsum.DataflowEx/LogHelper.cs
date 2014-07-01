using System;
using System.Reflection;
using Common.Logging;

namespace Gridsum.DataflowEx
{
    internal static class LogHelper
    {
        private static readonly ILog s_logger = LogManager.GetLogger(Assembly.GetExecutingAssembly().GetName().Name);
        private static readonly ILog s_perfMon = LogManager.GetLogger(Assembly.GetExecutingAssembly().GetName().Name + ".PerfMon");

        internal static ILog Logger
        {
            get
            {
                return s_logger;
            }
        }

        internal static ILog PerfMon
        {
            get
            {
                return s_perfMon;
            }
        }

        public static void LogByLevel(this ILog logger, LogLevel logLevel, Action<FormatMessageHandler> formatMessageCallback)
        {
            switch (logLevel)
            {
                case LogLevel.All:
                case LogLevel.Trace:
                    if (logger.IsTraceEnabled)
                    {
                        logger.Trace(formatMessageCallback);
                    }
                    break;
                case LogLevel.Debug:
                    if (logger.IsDebugEnabled)
                    {
                        logger.Debug(formatMessageCallback);
                    }
                    break;
                case LogLevel.Info:
                    if (logger.IsInfoEnabled)
                    {
                        logger.Info(formatMessageCallback);
                    }
                    break;
                case LogLevel.Warn:
                    if (logger.IsWarnEnabled)
                    {
                        logger.Warn(formatMessageCallback);
                    }
                    break;
                case LogLevel.Error:
                    if (logger.IsErrorEnabled)
                    {
                        logger.Error(formatMessageCallback);
                    }
                    break;
                case LogLevel.Fatal:
                    if (logger.IsFatalEnabled)
                    {
                        logger.Fatal(formatMessageCallback);
                    }
                    break;
                case LogLevel.Off:
                default:
                    //do nothing
                    break;
            }
        }
    }
}