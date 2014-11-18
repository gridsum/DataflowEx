using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Demo
{
    using Gridsum.DataflowEx.Databases;

    public class Order
    {
        [DBColumnMapping("OrderTarget", "Date", null)]
        public DateTime OrderDate { get; set; }

        [DBColumnMapping("OrderTarget", "Value", 0.0f)]
        public float? OrderValue { get; set; }

        [NoNullCheck]
        public Person Customer { get; set; }
    }
}
