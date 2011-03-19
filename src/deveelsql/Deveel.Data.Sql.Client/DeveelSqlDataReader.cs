using System;
using System.Data;
using System.Globalization;

namespace Deveel.Data.Sql.Client {
	public sealed class DeveelSqlDataReader : IDataReader {
		private QueryResult result;
		private readonly DeveelSqlCommand command;
		private bool closed;
		private bool disposed;
		private readonly CommandBehavior behavior;
		private int recordCount;

		internal DeveelSqlDataReader(DeveelSqlCommand command, QueryResult result, CommandBehavior behavior) {
			this.command = command;
			this.behavior = behavior;
			this.result = result;
		}

		public void Dispose() {
			if (!closed)
				Close();

			if (!disposed) {
				result.Dispose();
				disposed = true;
				GC.SuppressFinalize(this);
			}
		}

		public string GetName(int i) {
			return result.GetColumn(i).Name;
		}

		public string GetDataTypeName(int i) {
			return result.GetColumn(i).TypeName;
		}

		public Type GetFieldType(int i) {
			throw new NotImplementedException();
		}

		public object GetValue(int i) {
			return result.GetRawColumn(i);
		}

		public int GetValues(object[] values) {
			int toCopy = System.Math.Min(FieldCount, values.Length);
			for (int i = 0; i < toCopy; i++) {
				values[i] = GetValue(i);
			}
			return toCopy;
		}

		public int GetOrdinal(string name) {
			return result.FindColumnIndex(name);
		}

		public bool GetBoolean(int i) {
			return (bool) GetValue(i);
		}

		public byte GetByte(int i) {
			return (byte) GetValue(i);
		}

		long IDataRecord.GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) {
			//TODO: Support LOBs
			throw new NotSupportedException();
		}

		public char GetChar(int i) {
			throw new NotImplementedException();
		}

		long IDataRecord.GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) {
			//TODO: Support LOBs
			throw new NotSupportedException();
		}

		public Guid GetGuid(int i) {
			string s = GetString(i);
			if (s == null)
				throw new InvalidCastException("Cannot return a valid GUID");

			try {
				return new Guid(s);
			} catch (Exception) {
				throw new InvalidCastException("Cannot return a valid GUID");
			}
		}

		public BigNumber GetBigNumber(int i) {
			object value = GetValue(i);
			if (value == null)
				return null;

			if (value is BigNumber)
				return (BigNumber) value;
			if (value is string)
				return BigNumber.Parse((string) value);

			return null;
		}

		public short GetInt16(int i) {
			BigNumber number = GetBigNumber(i);
			return number == null ? (short)0 : number.ToInt16();
		}

		public int GetInt32(int i) {
			BigNumber number = GetBigNumber(i);
			return number == null ? 0 : number.ToInt32();
		}

		public long GetInt64(int i) {
			BigNumber number = GetBigNumber(i);
			return number == null ? 0L : number.ToInt64();
		}

		public float GetFloat(int i) {
			BigNumber number = GetBigNumber(i);
			return number == null ? 0.0f : number.ToSingle();
		}

		public double GetDouble(int i) {
			BigNumber number = GetBigNumber(i);
			return number == null ? 0.0 : number.ToDouble();
		}

		public string GetString(int i) {
			object value = GetValue(i);
			if (value == null)
				return null;
			if (value is string)
				return (string)value;
			return value.ToString();
		}

		decimal IDataRecord.GetDecimal(int i) {
			//TODO: convert a BigNumber to decimal
			throw new NotSupportedException();
		}

		public DateTime GetDateTime(int i) {
			object value = GetValue(i);
			if (value is DateTime)
				return (DateTime) GetValue(i);
			if (value is string)
				return DateTime.Parse((string) value, CultureInfo.InvariantCulture);

			throw new InvalidOperationException();
		}

		IDataReader IDataRecord.GetData(int i) {
			throw new NotImplementedException();
		}

		public bool IsDBNull(int i) {
			object value = result.GetRawColumn(i);
			return value == null || DBNull.Value == value;
		}

		public int FieldCount {
			get { return result.ColumnCount; }
		}

		public object this[int i] {
			get { return GetValue(i); }
		}

		public object this[string name] {
			get { return GetValue(GetOrdinal(name)); }
		}

		public void Close() {
			try {
				result.Close();

				if ((behavior & CommandBehavior.CloseConnection) != 0)
					command.Connection.Close();
			} finally {
				closed = true;
				command.Connection.OnStateChange(ConnectionState.Open);
			}
		}

		public DataTable GetSchemaTable() {
			throw new NotImplementedException();
		}

		public bool NextResult() {
			if ((behavior & CommandBehavior.SingleResult) != 0)
				return false;

			bool hasMore = command.HasMoreResults();
			if (hasMore)
				result = command.GetCurrentContext();
			return hasMore;
		}

		public bool Read() {
			if ((behavior & CommandBehavior.SingleRow) != 0 && recordCount > 0)
				return false;

			bool hasNext = result.Next();
			if (hasNext)
				recordCount++;

			return hasNext;
		}

		int IDataReader.Depth {
			get { return 0; }
		}

		public bool IsClosed {
			get { return closed; }
		}

		public int RecordsAffected {
			get { return result.IsUpdate ? (int) result.UpdateCount : -1; }
		}
	}
}