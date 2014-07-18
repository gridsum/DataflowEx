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
            recorder.RecordEvent("ev2", "1");
            recorder.RecordEvent("ev2", "2");
            recorder.RecordEvent("ev3", "1");
            recorder.RecordEvent("ev3", "1");
            recorder.RecordEvent("ev3", "1");
            
            Assert.AreEqual(3, recorder[typeof(string)]);
            Assert.AreEqual(3, recorder[typeof(EventHolder)]);
            Assert.AreEqual(4, recorder[typeof(int)]);
            Assert.AreEqual(5 + 3, recorder["ev1"]);
            Assert.AreEqual(2, recorder["ev2"]);
            Assert.AreEqual(3, recorder[new DataflowEvent("ev3", "1")]);

            Assert.AreEqual("a-b", new DataflowEvent("a", "b").ToString());
            Assert.AreEqual("a", new DataflowEvent("a").ToString());
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
