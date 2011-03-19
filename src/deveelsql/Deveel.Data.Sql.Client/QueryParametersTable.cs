using System;

using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql.Client {
	internal class QueryParametersTable : IMutableTable {
		private readonly Query query;
		private readonly ParameterColumnCollection columns;
		private TableRow currentRow;

		private static readonly TableName TableName = new TableName("@PARAMETERS@");

		public QueryParametersTable(Query query) {
			this.query = query;
			columns = new ParameterColumnCollection(this);
		}

		public void Dispose() {
		}

		public IColumnCollection Columns {
			get { return columns; }
		}

		public long RowCount {
			get { return 1; }
		}

		public TableName Name {
			get { return TableName; }
		}

		public Query Query {
			get { return query; }
		}

		public IRowCursor GetRowCursor() {
			return new SimpleRowCursor(1);
		}

		public TableRow GetRow(RowId rowid) {
			return currentRow;
		}

		public void PrefetchValue(int columnOffset, RowId rowid) {
		}

		public SqlObject GetValue(int columnOffset, RowId rowid) {
			return currentRow[columnOffset];
		}

		public bool RowExists(RowId rowid) {
			return rowid.ToInt64() == 0;
		}

		public TableRow NewRow() {
			currentRow = new ParameterRow(this, new RowId(0));
			return currentRow;
		}

		public void Insert(TableRow row) {
			currentRow = row;
		}

		public void Update(TableRow row) {
			currentRow = row;
		}

		public void Delete(RowId rowid) {
			currentRow = null;
		}

		public void Commit() {
		}

		public void Undo() {
		}

		#region ParameterColumnList

		private class ParameterColumnCollection : ColumnCollection {
			private readonly Query query;

			public ParameterColumnCollection(QueryParametersTable table)
				: base(table) {
				query = table.query;
			}

			public override int IndexOf(TableColumn item) {
				if (query == null)
					return -1;

				for (int i = 0; i < query.Parameters.Count; i++) {
					QueryParameter parameter = query.Parameters[i];
					if (parameter.Name == item.Name)
						return i;
				}

				return -1;
			}

			public override int Count {
				get { return query == null ? 0 :query.Parameters.Count; }
			}
		}

		#endregion

		#region PatameterRow

		private class ParameterRow : TableRow {
			private readonly Query query;

			public ParameterRow(QueryParametersTable table, RowId id)
				: base(table, id) {
				query = table.query;
			}

			public override SqlObject GetValue(int columnOffset) {
				if (query == null || columnOffset >= query.Parameters.Count)
					return SqlObject.Null;

				return query.Parameters[columnOffset].Value;
			}

			public override void SetValue(int columnOffset, SqlObject value) {
				if (query == null)
					throw new InvalidOperationException("The backed query is null.");

				if (columnOffset >= query.Parameters.Count) {
					for (int i = query.Parameters.Count - 1; i < columnOffset; i++) {
						query.Parameters.Add(SqlObject.Null);
					}
				}

				query.Parameters[columnOffset].Value = value;
			}
		}

		#endregion

	}
}