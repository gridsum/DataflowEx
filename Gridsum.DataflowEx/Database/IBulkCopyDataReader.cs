using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace Gridsum.DataflowEx
{
    public interface IBulkCopyDataReader : IDataReader, IDisposable, IDataRecord
    {
        string DestinationTableName { get; set; }

        List<SqlBulkCopyColumnMapping> ColumnMapping { get; }
    }
}