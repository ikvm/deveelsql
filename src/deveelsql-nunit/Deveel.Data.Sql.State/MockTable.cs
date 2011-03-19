using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql.State {
	public sealed class MockTable : IMutableTable {
		private readonly TableName name;
		private readonly ColumnCollection columns;
		private readonly Dictionary<RowId, TableRow> rows;
		private readonly List<RowId> rowIndex;

		private long rowIdSeq = -1;

		public MockTable(TableName name) {
			this.name = name;
			columns = new ColumnCollection(this);
			rows = new Dictionary<RowId, TableRow>();
			rowIndex = new List<RowId>();
		}

		public IColumnCollection Columns {
			get { return columns; }
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
			RowId rowid = new RowId(++rowIdSeq);
			return new TableRow(this, rowid);
		}

		public TableRow GetRow(RowId rowid) {
			TableRow row;
			return !rows.TryGetValue(rowid, out row) ? null : row;
		}

		public void Insert(TableRow row) {
			rows[row.Id] = row;
			rowIndex.Add(row.Id);
		}

		public void Update(TableRow row) {
			if (rows.ContainsKey(row.Id)) {
				rows[row.Id] = row;
			}
		}

		public void Delete(RowId rowid) {
			if (rows.ContainsKey(rowid)) {
				rows.Remove(rowid);

				for (int i = rowIndex.Count - 1; i >= 0; i--) {
					if (rowIndex[i] == rowid)
						rowIndex.RemoveAt(i);
				}
			}
		}

		public void Commit() {
		}

		public void Undo() {
		}

		public void PrefetchValue(int columnOffset, RowId rowid) {
		}

		public SqlObject GetValue(int columnOffset, RowId rowid) {
			TableRow row;
			if (!rows.TryGetValue(rowid, out row))
				return SqlObject.Null;

			return row.GetValue(columnOffset);
		}

		public bool RowExists(RowId rowid) {
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

			public RowId Current {
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

		public void Dispose() {
			rows.Clear();
			rowIndex.Clear();
		}
	}
}