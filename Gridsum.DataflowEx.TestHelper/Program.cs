using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.TestHelper
{
    using Gridsum.DataflowEx.Test.ETL;

    class Program
    {
        static void Main(string[] args)
        {
            new TestDataJoiner().TestDataJoinerJoining().Wait();
        }
    }
}
