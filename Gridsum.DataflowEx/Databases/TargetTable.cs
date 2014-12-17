using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gridsum.DataflowEx.Databases
{
    /// <summary>
    /// Represents a target for db bulk insertion and the chosen mapping category
    /// </summary>
    public class TargetTable
    {
        public readonly string ConnectionString;
        public readonly string TableName;
        public readonly string DestLabel;

        public TargetTable(string destLabel, string connectionString, string tableName)
        {
            this.DestLabel = destLabel;
            this.ConnectionString = connectionString;
            this.TableName = tableName;
        }

        protected bool Equals(TargetTable other)
        {
            return string.Equals(this.ConnectionString, other.ConnectionString) && string.Equals(this.TableName, other.TableName) && string.Equals(this.DestLabel, other.DestLabel);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }
            if (ReferenceEquals(this, obj))
            {
                return true;
            }
            if (obj.GetType() != this.GetType())
            {
                return false;
            }
            return Equals((TargetTable)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = (this.ConnectionString != null ? this.ConnectionString.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (this.TableName != null ? this.TableName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (this.DestLabel != null ? this.DestLabel.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}
