namespace Gridsum.DataflowEx.Test
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;
    using System.Xml.Serialization;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    ///     Summary description for TestLogReader
    /// </summary>
    [TestClass]
    public class TestLogReader
    {
        #region Public Methods and Operators

        [TestMethod]
        public async Task TestMethod1()
        {
            var logReader = new SimpleLogReader(DataflowOptions.Default);

            long count = await logReader.ProcessAsync(new[] { "a", "a b", "a b c", "a b c d" }, false);
            long count2 = await logReader.ProcessAsync(new[] { "a", "a b", "a b c", "a b c d" });

            Assert.AreEqual(4 * 2, count + count2);
            Assert.AreEqual(4 * 2, logReader.Recorder[new DataflowEvent("a")]);
            Assert.AreEqual(3 * 2, logReader.Recorder[new DataflowEvent("b")]);
            Assert.AreEqual(2 * 2, logReader.Recorder[new DataflowEvent("c")]);
            Assert.AreEqual(1 * 2, logReader.Recorder[new DataflowEvent("d")]);
        }

        [TestMethod]
        public async Task TestMethod2()
        {
            var logReader = new SimpleLogReader(DataflowOptions.Default);

            try
            {
                long count = await logReader.ProcessAsync(this.GetStringSequence());
                Assert.Fail("shouldn't arrive here");
            }
            catch (Exception e)
            {
                Assert.AreEqual(typeof(OperationCanceledException), e.GetType());
            }
        }
        #endregion

        IEnumerable<string> GetStringSequence()
        {
            yield return "a";
            yield return "b";
            yield return "ERROR";
            Thread.Sleep(5000);
            yield return "c";
            Thread.Sleep(5000);
            yield return "d";
        }
    }

    public class SimpleLogReader : LogReader
    {
        #region Fields

        private readonly TransformManyBlock<string, string> m_parsingBlock;

        private readonly StatisticsRecorder m_recorder;

        private ActionBlock<string> m_recordBlock;

        static readonly char[] Splitor = { ' ' };

        #endregion

        #region Constructors and Destructors

        public SimpleLogReader(DataflowOptions dataflowOptions)
            : base(dataflowOptions)
        {
            this.m_parsingBlock = new TransformManyBlock<string, string>(
                s =>
                    {
                        if (string.IsNullOrEmpty(s))
                        {
                            return Enumerable.Empty<string>();
                        }

                        if (s == "ERROR")
                        {
                            throw new InvalidDataException("evil data :)");
                        }

                        return s.Split(Splitor);
                    });

            this.m_recorder = new StatisticsRecorder(this);
            this.m_recordBlock = new ActionBlock<string>(
                s =>
                    {
                        if (s == "ERROR")
                        {
                            throw new InvalidDataException("evil data :)");
                        }

                        this.m_recorder.RecordEvent(s);
                    });
            
            var df1 = DataflowUtils.FromBlock(m_parsingBlock);
            var df2 = DataflowUtils.FromBlock(m_recordBlock);
            df1.LinkTo(df2);

            RegisterChild(df1);
            RegisterChild(df2);
        }

        #endregion

        #region Public Properties

        public override ITargetBlock<string> InputBlock
        {
            get
            {
                return this.m_parsingBlock;
            }
        }

        public override StatisticsRecorder Recorder
        {
            get
            {
                return this.m_recorder;
            }
        }

        #endregion
    }
}