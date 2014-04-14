using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Test
{
    public static class TestUtils
    {
        public static async Task<bool> FinishesIn(this Task t,TimeSpan ts)
        {
            var timeOutTask = Task.Delay(ts);
            return t == await Task.WhenAny(t, timeOutTask);
        }
    }
}
