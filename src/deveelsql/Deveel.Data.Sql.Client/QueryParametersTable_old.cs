using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Client {
	internal class QueryParametersTable : IMutableTableDataSource {
		private readonly Query query;

		public QueryParametersTable(Query query) {
			this.query = query;
		}

		public void Dispose() {
		}

		public IEnumerator<long> GetEnumerator() {
			return GetRowCursor();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public int ColumnCount {
			get { return query.Parameters.Count; }
		}

		public long RowCount {
			get { return 1; }
		}

		public TableName TableName {
			get { return new TableName("@PARAMTABLE@");}
		}

		public Query Query {
			get { return query; }
		}

		public int GetColumnOffset(string columnName) {
			for (int i = 0; i < query.Parameters.Count; i++) {
				QueryParameter parameter = query.Parameters[i];
				if (parameter.Name == columnName)
					return i;
			}

			return -1;
		}

		public Variable GetColumnName(int offset) {
			QueryParameter parameter = query.Parameters[offset];
			return new Variable(TableName, parameter.Name);
		}

		public SqlType GetColumnType(int offset) {
			QueryParameter parameter = query.Parameters[offset];
			return parameter.Value.Type;
		}

		public SqlObject GetValue(int column, long row) {
			return query.Parameters[column].Value;
		}

		public void FetchValue(int column, long row) {
		}

		public IRowCursor GetRowCursor() {
			return new SimpleRowCursor(1);
		}

		public void SetValue(int column, long rowid, SqlObject value) {
			throw new NotImplementedException();
		}

		public void Finish(bool complete) {
		}

		public long BeginInsert() {
			return Int64.MinValue;
		}

		public long BeginUpdate(long rowid) {
			return rowid;
		}

		public void Remove(long rowid) {
		}

		public void Remove(IRowCursor rows) {
		}
	}
}