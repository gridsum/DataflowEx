using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.AutoCompletion
{
    public class ControlItem : ITracableItem
    {
        public Guid UniqueId
        {
            get;
            set;
        }

        public ControlType Type { get; set; }
    }

    //todo:  ISpiderComponent1Item -> SpiderRequest, Control
    //todo:  ISpiderComponent2Item -> SpiderResponse, Control


}
