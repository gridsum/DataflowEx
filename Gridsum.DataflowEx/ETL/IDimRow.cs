namespace Gridsum.DataflowEx.ETL
{
    using System;

    using C5;

    public interface IDimRow<TJoinKey>
    {
        long AutoIncrementKey { get; }
        TJoinKey JoinOn { get; }
        bool IsFullRow { get; }
        IPriorityQueueHandle<IDimRow<TJoinKey>> Handle { get; set; }
        DateTime LastHitTime { get; set; }
    }
}