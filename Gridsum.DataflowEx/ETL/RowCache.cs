namespace Gridsum.DataflowEx.ETL
{
    using System;
    using System.Collections.Generic;

    using C5;

    public class RowCache<TJoinKey>
    {
        private readonly int m_size;

        private IntervalHeap<IDimRow<TJoinKey>> m_rowPriorityQueue;
        private Dictionary<TJoinKey, IDimRow<TJoinKey>> m_dict;

        public RowCache(int size)
        {
            this.m_size = size;
            this.m_rowPriorityQueue = new IntervalHeap<IDimRow<TJoinKey>>();
            this.m_dict = new Dictionary<TJoinKey, IDimRow<TJoinKey>>();
        }

        public void Add(TJoinKey key, IDimRow<TJoinKey> row)
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