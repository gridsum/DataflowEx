using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Databases
{
    public class InvalidDBColumnMappingException : Exception
    {
        private readonly string m_description;
        private readonly DBColumnMapping m_mapping;
        private readonly LeafPropertyNode m_node;

        public InvalidDBColumnMappingException(string description, DBColumnMapping mapping, LeafPropertyNode node) : base()
        {
            this.m_description = description;
            this.m_mapping = mapping;
            this.m_node = node;
        }

        public override string Message
        {
            get
            {
                return string.Format("{0} Mapping: {1} LeafNode: {2}", m_description, m_mapping, m_node);
            }
        }
    }
}
