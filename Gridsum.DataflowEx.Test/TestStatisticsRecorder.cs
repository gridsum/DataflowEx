using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gridsum.DataflowEx.Test
{
    [TestClass]
    public class TestStatisticsRecorder
    {
        [TestMethod]
        public void TestMethod1()
        {
            var recorder = new StatisticsRecorder();
            recorder.Record("a");
            recorder.Record("a");
            recorder.Record("a");
            recorder.Record(1);
            recorder.Record(1);
            recorder.Record(1);
            recorder.Record(1);
            recorder.Record(new EventHolder());
            recorder.Record(new EventHolder());
            recorder.Record(new EventHolder());
            recorder.RecordEvent("ev1");
            recorder.RecordEvent("ev1");
            recorder.RecordEvent("ev1");
            recorder.RecordEvent("ev1");
            recorder.RecordEvent("ev1");
            
            Assert.AreEqual(3, recorder[typeof(string)]);
            Assert.AreEqual(3, recorder[typeof(EventHolder)]);
            Assert.AreEqual(4, recorder[typeof(int)]);
            Assert.AreEqual(5 + 3, recorder[new DataflowEvent("ev1")]);
        }

        internal class EventHolder : IEventProvider
        {
            public DataflowEvent GetEvent()
            {
                return new DataflowEvent("ev1");
            }
        }
    }
}
