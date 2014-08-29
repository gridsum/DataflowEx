using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.ETL
{
    public sealed class ByteArrayEqualityComparer : IEqualityComparer<byte[]>
    {
        public bool Equals(byte[] first, byte[] second)
        {
            if (first == second)
                return true;
            if (first == null || second == null || first.Length != second.Length)
                return false;
            for (int index = 0; index < first.Length; ++index)
            {
                if ((int)first[index] != (int)second[index])
                    return false;
            }
            return true;
        }

        public int GetHashCode(byte[] array)
        {
            if (array == null)
                return 0;
            int num1 = 17;
            foreach (byte num2 in array)
                num1 = num1 * 31 + (int)num2;
            return num1;
        }
    }
}
