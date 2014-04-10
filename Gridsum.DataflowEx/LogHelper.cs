// ======================================================================
// 
//      Copyright (C) 北京国双科技有限公司        
//                    http://www.gridsum.com
// 
//      保密性声明：此文件属北京国双科技有限公司所有，仅限拥有由国双科技
//      授予了相应权限的人所查看和所修改。如果你没有被国双科技授予相应的
//      权限而得到此文件，请删除此文件。未得国双科技同意，不得查看、修改、
//      散播此文件。
// 
// 
// ======================================================================

using System;
using System.Reflection;
using Common.Logging;

namespace Gridsum.DataflowEx
{
    internal static class LogHelper
    {
        private static readonly ILog s_logger = LogManager.GetLogger(Assembly.GetExecutingAssembly().GetName().Name);

        internal static ILog Logger
        {
            get
            {
                return s_logger;
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