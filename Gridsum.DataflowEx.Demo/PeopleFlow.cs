using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Demo
{
    using System.Threading;
    using System.Threading.Tasks.Dataflow;

    using Gridsum.DataflowEx.Databases;

    using Newtonsoft.Json;

    public class Person : IEventProvider
    {
        [DBColumnMapping("LocalDbTarget", "NameCol", "N/A", ColumnMappingOption.Mandatory)]
        public string Name { get; set; }

        [DBColumnMapping("LocalDbTarget", "AgeCol", -1, ColumnMappingOption.Optional)]
        public int? Age { get; set; }

        public DataflowEvent GetEvent()
        {
            if (Age > 70)
            {
                return new DataflowEvent("OldPerson");
            }
            else
            {
                //returning empty so it will not be recorded as an event
                return DataflowEvent.Empty; 
            }
        }
    }

    public class PeopleFlow : Dataflow<string, Person>
    {
        private TransformBlock<string, Person> m_converter;
        private TransformBlock<Person, Person> m_recorder;
        private StatisticsRecorder m_peopleRecorder;
        
        public PeopleFlow(DataflowOptions dataflowOptions)
            : base(dataflowOptions)
        {
            m_peopleRecorder = new StatisticsRecorder(this) { Name = "PeopleRecorder"};

            m_converter = new TransformBlock<string, Person>(s => JsonConvert.DeserializeObject<Person>(s));
            m_recorder = new TransformBlock<Person, Person>(
                p =>
                    {
                        //record every person
                        m_peopleRecorder.Record(p);
                        return p;
                    });

            m_converter.LinkTo(m_recorder, new DataflowLinkOptions() { PropagateCompletion = true});

            RegisterChild(m_converter);
            RegisterChild(m_recorder);
        }

        public override ITargetBlock<string> InputBlock { get { return m_converter; } }
        public override ISourceBlock<Person> OutputBlock { get { return m_recorder; } }
        public StatisticsRecorder PeopleRecorder { get { return m_peopleRecorder; } }
    }
}


