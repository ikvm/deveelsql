using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public sealed class MockTable : ITable {
		private readonly TableName name;
		private readonly ITableSchema schema;
		private readonly Dictionary<long, TableRow> rows;
		private readonly List<long> rowIndex;

		private long rowIdSeq = -1;

		public MockTable(TableName name) {
			this.name = name;
			schema = new MockTableSchema(this);
			rows = new Dictionary<long, TableRow>();
			rowIndex = new List<long>();
		}

		public ITableSchema TableSchema {
			get { return schema; }
		}

		public long RowCount {
			get { return rows.Count; }
		}

		public TableName Name {
			get { return name; }
		}

		public IRowCursor GetRowCursor() {
			return new RowCursor(this);
		}

		public TableRow NewRow() {
			return new TableRow(this);
		}

		public TableRow GetRow(long rowid) {
			TableRow row;
			return !rows.TryGetValue(rowid, out row) ? null : row;
		}

		public void Insert(TableRow row) {
			long rowid = ++rowIdSeq;
			TableRow newRow = new TableRow(this, rowid);
			for (int i = 0; i < TableSchema.ColumnCount; i++) {
				newRow[i] = row[i];
			}
			row.ClearValues();
			rows[rowid] = newRow;
			rowIndex.Add(rowid);
		}

		public void Update(TableRow row) {
			if (rows.ContainsKey(row.Id)) {
				rows[row.Id] = row;
			}
		}

		public void Delete(long rowid) {
			if (rows.ContainsKey(rowid)) {
				rows.Remove(rowid);

				for (int i = rowIndex.Count - 1; i >= 0; i--) {
					if (rowIndex[i] == rowid)
						rowIndex.RemoveAt(i);
				}
			}
		}

		public void PrefetchValue(int columnOffset, long rowid) {
		}

		public SqlValue GetValue(int columnOffset, long rowid) {
			TableRow row;
			if (!rows.TryGetValue(rowid, out row))
				return SqlValue.Null;

			SqlObject obj = row.GetValue(columnOffset);
			return obj.Value;
		}

		public bool RowExists(long rowid) {
			return rows.ContainsKey(rowid);
		}

		private class RowCursor : IRowCursor {
			private readonly MockTable table;
			private int rowCount;
			private int index;

			public RowCursor(MockTable table) {
				this.table = table;
				rowCount = table.rows.Count;
				index = -1;
			}

			private void CheckCurrent() {
				if (rowCount != table.rows.Count)
					throw new InvalidOperationException();
			}

			public void Dispose() {
			}

			public bool MoveNext() {
				CheckCurrent();
				return ++index < rowCount;
			}

			public void Reset() {
				rowCount = table.rows.Count;
				index = -1;
			}

			public long Current {
				get {
					CheckCurrent();
					return table.rowIndex[index];
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			public object Clone() {
				return new RowCursor(table);
			}

			public long Position {
				get { return index; }
			}

			public long Count {
				get {
					CheckCurrent();
					return rowCount;
				}
			}

			public bool MoveBack() {
				CheckCurrent();
				return --index >= 0;
			}

			public void MoveBeforeStart() {
				CheckCurrent();
				index = -1;
			}

			public void MoveAfterEnd() {
				CheckCurrent();
				index = rowCount;
			}

			public long MoveTo(long position) {
				CheckCurrent();
				index = (int) position;
				return position;
			}
		}
	}
}