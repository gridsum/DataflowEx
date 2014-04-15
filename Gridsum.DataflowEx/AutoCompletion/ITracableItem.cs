using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.AutoCompletion
{
    public interface ITracableItem
    {
        Guid UniqueId { get; }
    }
}
