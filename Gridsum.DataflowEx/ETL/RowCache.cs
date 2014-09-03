namespace Gridsum.DataflowEx.ETL
{
    using System;
    using System.Collections.Generic;

    using C5;

    public class RowCache<TJoinKey>
    {
        class RowComparer : Comparer<IDimRow<TJoinKey>>
        {
            public override int Compare(IDimRow<TJoinKey> x, IDimRow<TJoinKey> y)
            {
                TimeSpan span = x.LastHitTime - y.LastHitTime;

                if (span.Ticks < 0) return -1;
                else if (span.Ticks > 0) return 1;
                else
                {
                    return 0;
                }
            }
        }

        private readonly int m_size;

        private IntervalHeap<IDimRow<TJoinKey>> m_rowPriorityQueue;
        private Dictionary<TJoinKey, IDimRow<TJoinKey>> m_dict;

        public RowCache(int size, IEqualityComparer<TJoinKey> keyEqualityComparer)
        {
            this.m_size = size;
            this.m_rowPriorityQueue = new IntervalHeap<IDimRow<TJoinKey>>(new RowComparer());
            this.m_dict = new Dictionary<TJoinKey, IDimRow<TJoinKey>>(keyEqualityComparer);
        }

        public bool TryAdd(TJoinKey key, IDimRow<TJoinKey> row)
        {
            if (this.m_dict.ContainsKey(key))
            {
                return false;
            }
            else
            {
                this.m_dict.Add(key, row);
                IPriorityQueueHandle<IDimRow<TJoinKey>> handle = null;
                this.m_rowPriorityQueue.Add(ref handle, row);
                row.Handle = handle;

                while (this.m_dict.Count > this.m_size)
                {
                    var discardRow = this.m_rowPriorityQueue.DeleteMin();
                    this.m_dict.Remove(discardRow.JoinOn);
                }
                return true;
            }
        }

        public bool TryGetValue(TJoinKey key, out IDimRow<TJoinKey> value)
        {
            IDimRow<TJoinKey> hit;
            if (this.m_dict.TryGetValue(key, out hit))
            {
                hit.LastHitTime = DateTime.UtcNow;
                this.m_rowPriorityQueue.Replace(hit.Handle, hit);
                value = hit;
                return true;
            }
            else
            {
                value = null;
                return false;
            }
        }

        public int Count
        {
            get
            {
                return this.m_dict.Count;
            }
        }
    }
}