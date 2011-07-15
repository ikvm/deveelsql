using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Sql {
	public sealed class FunctionResultTable : ITable {
		private readonly SqlObject[] values;
		private TableRow row;
		private readonly ColumnCollection columns;

		private static readonly TableName FunctionTableName = new TableName("@FunctionResult@");

		public FunctionResultTable(SqlObject[] values) {
			this.values = values;

			columns = new ColumnCollection(this);
			for (int i = 0; i < values.Length; i++) {
				columns.Add(new TableColumn(this, "column" + i, values[i].Type));
			}
			columns.MakeReadOnly();
		}

		void IDisposable.Dispose() {
		}

		public IColumnCollection Columns {
			get { return columns; }
		}

		public long RowCount {
			get { return 1; }
		}

		public TableName Name {
			get { return FunctionTableName;}
		}

		public IRowCursor GetRowCursor() {
			return new SimpleRowCursor(1);
		}

		public TableRow GetRow(RowId rowid) {
			if (rowid.ToInt64() != 0)
				throw new ArgumentOutOfRangeException("rowid");

			if (row == null) {
				row = new TableRow(this, new RowId(0));
				for (int i = 0; i < columns.Count; i++)
					row.SetValue(i, values[i]);
			}

			return row;
		}

		void ITable.PrefetchValue(int columnOffset, RowId rowid) {
		}

		public SqlObject GetValue(int columnOffset, RowId rowid) {
			if (rowid.ToInt64() != 0)
				throw new ArgumentOutOfRangeException("rowid");

			return values[columnOffset];
		}

		public bool RowExists(RowId rowid) {
			return rowid.ToInt64() == 0;
		}
	}
}