using System;

namespace Gridsum.DataflowEx.AutoCompletion
{
    public class TracableItemBase : ITracableItem
    {
        public TracableItemBase() : this(Guid.NewGuid())
        {
        }

        public TracableItemBase(Guid guid)
        {
            this.UniqueId = guid;
        }

        public Guid UniqueId { get; private set; }
    }
}