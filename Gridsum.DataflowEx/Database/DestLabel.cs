using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Database
{
    public class DestLabel
    {
        public string[] Destinations { get; private set; }

        public DestLabel(params string[] destinations)
        {
            Destinations = destinations.Distinct().ToArray();
        }

        public bool HasFlag(DestLabel label)
        {
            var intersect = this.Destinations.Intersect(label.Destinations);
            return intersect.Count() == label.Destinations.Length;
        }
    }
}
