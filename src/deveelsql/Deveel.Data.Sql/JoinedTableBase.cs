using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Sql {
	public abstract class JoinedTableBase : ITable {
		private readonly ITable[] tables;
		private readonly JoinedColumnCollection columns;
		private Expression orderComposite;

		protected JoinedTableBase(ITable[] tables) {
			if (tables.Length == 0)
				throw new ApplicationException("There must be at least 1 table to be joined.");

			this.tables = tables;
			columns = new JoinedColumnCollection(this);
		}

		protected JoinedTableBase(ITable left, ITable right)
			: this(new ITable[] { left, right }) {
		}

		protected ITable[] Tables {
			get { return tables; }
		}

		public Expression OrderComposite {
			get { return orderComposite; }
			set { orderComposite = value; }
		}

		public abstract long RowCount { get; }

		public TableName Name {
			get { return null; }
		}

		public IColumnCollection Columns {
			get { return columns; }
		}

		public IRowCursor GetRowCursor() {
			return new SimpleRowCursor(RowCount);
		}

		public TableRow GetRow(RowId rowid) {
			// Null value if the rowid is less than 0
			if (rowid == null || rowid.ToInt64() < 0)
				return null;

			int sz = Columns.Count;
			TableRow row = new TableRow(this, rowid);
			for (int i = 0; i < sz; i++) {
				int tableIndex = columns.IndexOfTable(i);
				// Adjust the column to the table the column is located
				int tableColumn = columns.AdjustColumn(i);
				// Adjust the row by the table
				RowId tableRow = AdjustRow(rowid.ToInt64(), tableIndex);

				// Fetch and return the data
				SqlObject value = tables[tableIndex].GetValue(tableColumn, tableRow);

				TableColumn column = Columns[i];
				row.SetValue(column.Offset, value);
			}

			return row;
		}

		public void PrefetchValue(int columnOffset, RowId rowid) {
			if (rowid == null || rowid.ToInt64() < 0)
				return;

			if (columnOffset == -1) {
				for (int i = 0; i < tables.Length; ++i) {
					tables[i].PrefetchValue(-1, AdjustRow(rowid.ToInt64(), i));
				}
			} else {
				int tableIndex = columns.IndexOfTable(columnOffset);
				// Adjust the column to the table the column is located
				int tableColumn = columns.AdjustColumn(columnOffset);
				// Adjust the row by the table
				RowId tableRow = AdjustRow(rowid.ToInt64(), tableIndex);

				// Delegate the hint to the parent table
				tables[tableIndex].PrefetchValue(tableColumn, tableRow);
			}
		}

		public SqlObject GetValue(int columnOffset, RowId rowid) {
			int tableIndex = columns.IndexOfTable(columnOffset);
			// Adjust the column to the table the column is located
			int tableColumn = columns.AdjustColumn(columnOffset);
			// Adjust the row by the table
			RowId tableRow = AdjustRow(rowid.ToInt64(), tableIndex);

			// Fetch and return the data
			return tables[tableIndex].GetValue(tableColumn, tableRow);
		}

		public bool RowExists(RowId rowid) {
			return rowid.ToInt64() > 0 && rowid.ToInt64() < RowCount;
		}

		internal void SetOrderCompositeIsChild() {
			// Get from the child
			ITable filter = tables[0];
			Expression childComposite = null;

			if (filter is FilteredTable) {
				childComposite = ((FilteredTable)filter).OrderComposite;
			} else if (filter is JoinedTableBase) {
				childComposite = ((JoinedTableBase)filter).OrderComposite;

			}
			orderComposite = childComposite;
		}

		protected abstract RowId AdjustRow(long row, int tableIndex);


		#region JoinedColumnList

		private class JoinedColumnCollection : ColumnCollection {
			private readonly JoinedTableBase joinedTable;
			private readonly int columnCount;
			private readonly int[] tableColumnLokup;
			private readonly int[] columnAdjust;

			public JoinedColumnCollection(JoinedTableBase joinedTable)
				: base(joinedTable) {
				this.joinedTable = joinedTable;

				ITable[] tables = joinedTable.Tables;

				int ccount = 0;
				for (int i = 0; i < tables.Length; ++i) {
					ccount += tables[i].Columns.Count;
				}

				tableColumnLokup = new int[ccount];
				columnAdjust = new int[ccount];
				int n = 0;
				int adjust = 0;
				for (int i = 0; i < tables.Length; ++i) {
					int end = n + tables[i].Columns.Count;
					while (n < end) {
						tableColumnLokup[n] = i;
						columnAdjust[n] = adjust;
						++n;
					}
					adjust = end;
				}

				columnCount = ccount;
			}

			public override int Count {
				get { return columnCount; }
			}

			public override bool IsReadOnly {
				get { return true; }
			}

			internal int IndexOfTable(int columnOffset) {
				return tableColumnLokup[columnOffset];
			}

			protected internal int AdjustColumn(int columnOffset) {
				return columnOffset - columnAdjust[columnOffset];
			}

			public override TableColumn GetColumn(int offset) {
				return joinedTable.Tables[IndexOfTable(offset)].Columns[AdjustColumn(offset)];
			}
		}

		#endregion

		void IDisposable.Dispose() {
		}
	}
}