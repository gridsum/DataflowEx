using System.Threading;

namespace Gridsum.DataflowEx
{
    public class IntHolder
    {
        private int m_count;
        public int Count
        {
            get { return m_count; }
        }

        public int Increment()
        {
            return Interlocked.Increment(ref m_count);
        }
    }
}