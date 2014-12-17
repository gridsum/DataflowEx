using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace Gridsum.DataflowEx.Databases
{
    /// <summary>
    /// A class wrapping typeaccesor which implements IDataReader.
    /// </summary>
    /// <remarks>
    /// This class was initially inspired by https://github.com/WimAtIHomer/SqlBulkCopyListReader
    /// and then we took further steps to improve it.
    /// </remarks>
    /// <typeparam name="T">The type to dissect and auto-generate column visitors</typeparam>
    public class BulkDataReader<T> : IDataReader where T:class
    {
        #region private fields (instance and static)

        private readonly IEnumerable<T> m_source;
        private readonly TypeAccessor<T> m_typeAccessor;

        private int m_affectedRows;
        private IEnumerator<T> m_sourceEnumerator;

        #endregion

        #region ctor and init the bulk data reader

        public BulkDataReader(TypeAccessor<T> typeAccessor, IEnumerable<T> source)
        {
            if (typeAccessor == null || source == null)
            {
                throw new ArgumentException("typeAccessor or source can not be null");
            }

            m_typeAccessor = typeAccessor;
            m_source = source;
        }

        #endregion

        /// <summary>
        /// current object
        /// </summary>
        protected T Current
        {
            get
            {
                if (m_source == null)
                {
                    throw new InvalidOperationException("The reader is closed");
                }
                return m_sourceEnumerator.Current;
            }
        }

        /// <summary>
        /// The ColumnMapping
        /// </summary>
        public List<SqlBulkCopyColumnMapping> ColumnMappings
        {
            get { return m_typeAccessor.ColumnMappings; }
        }

        #region IDataReader Members

        public void Close()
        {
            Dispose();
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        public int Depth
        {
            get { return 1; }
        }

        public bool IsClosed
        {
            get { return m_sourceEnumerator == null; }
        }

        public bool NextResult()
        {
            return false;
        }

        public bool Read()
        {
            if (m_sourceEnumerator == null)
                m_sourceEnumerator = m_source.GetEnumerator();

            m_affectedRows++;
            bool result = m_sourceEnumerator.MoveNext();

            if (!result) // reset the cursor
            {
                m_sourceEnumerator = null;
                m_affectedRows = 0;
            }

            return result;
        }

        public int RecordsAffected
        {
            get { return m_affectedRows; }
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            //_source = null;
            m_sourceEnumerator = null;
        }

        #endregion

        #region IDataRecord Members

        public bool GetBoolean(int i)
        {
            return (bool) GetValue(i);
        }

        public byte GetByte(int i)
        {
            return (byte) GetValue(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException();
        }

        public char GetChar(int i)
        {
            return (char) GetValue(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException();
        }

        public int FieldCount
        {
            get { return m_typeAccessor.FieldCount; }
        }
        
        public IDataReader GetData(int i)
        {
            throw new NotSupportedException();
        }
        
        public Type GetFieldType(int i)
        {
            throw new NotImplementedException();
        }

        public string GetName(int i)
        {
            return m_typeAccessor.GetName(i);
        }


        public int GetOrdinal(string name)
        {
            return m_typeAccessor.GetColumnOffset(name);
        }

        public bool IsDBNull(int i)
        {
            return GetValue(i) == null;
        }

        public object this[string name]
        {
            get { return GetValue(GetOrdinal(name)); }
        }

        public object this[int i]
        {
            get { return GetValue(i); }
        }

        public int GetValues(object[] values)
        {
            throw new NotSupportedException();
        }

        public DateTime GetDateTime(int i)
        {
            return (DateTime) GetValue(i);
        }

        public decimal GetDecimal(int i)
        {
            return (decimal) GetValue(i);
        }

        public double GetDouble(int i)
        {
            return (double) GetValue(i);
        }

        public float GetFloat(int i)
        {
            return (float) GetValue(i);
        }

        public Guid GetGuid(int i)
        {
            return (Guid) GetValue(i);
        }

        public short GetInt16(int i)
        {
            return (short) GetValue(i);
        }

        public int GetInt32(int i)
        {
            return (int) GetValue(i);
        }

        public long GetInt64(int i)
        {
            return (long) GetValue(i);
        }

        public string GetString(int i)
        {
            return (string) GetValue(i);
        }

        #endregion

        /// <summary>
        /// Retrieves the property value of column index i using the delegates stored
        /// </summary>
        /// <param name="i">column index</param>
        /// <returns></returns>
        public object GetValue(int i)
        {
            Func<T, object> func = m_typeAccessor.GetPropertyAccessor(i);
            /*var value = func(Current);
            var mapping = m_typeAccessor.GetColumnMapping(i);

            Console.WriteLine("default value: "+mapping.DefaultValue);
            Console.WriteLine("computed value: "+value);
            Console.WriteLine("are equal: " + object.ReferenceEquals(value, mapping.DefaultValue));
            Console.WriteLine("mapping: " + mapping.DestColumnName + ", " + mapping.DestColumnOffset);
            Console.ReadKey();*/
            return func(Current);
        }
    }
}