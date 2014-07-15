using System;
using System.Linq;

namespace Gridsum.DataflowEx
{
    using System.Threading.Tasks;

    internal class Utils
    {
        public static string GetFriendlyName(Type type)
        {
            if (type.IsGenericType)
                return string.Format("{0}<{1}>", type.Name.Split('`')[0], string.Join(", ", type.GetGenericArguments().Select(GetFriendlyName)));
            else
                return type.Name;
        }
    }
}