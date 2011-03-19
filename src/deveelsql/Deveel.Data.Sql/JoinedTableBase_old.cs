using System;
using System.Collections;
using System.Collections.Generic;

using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	public abstract class JoinedTableBase : ITableDataSource {
		private readonly ITable[] tables;
		private int columnCount;
		private int[] tableColumn_Lokup;
		private int[] columnAdjust;
		private Expression orderComposite;

		protected JoinedTableBase(ITable[] tables) {
			if (tables.Length == 0)
				throw new ApplicationException("There must be at least 1 table to be joined.");

			int ccount = 0;
			for (int i = 0; i < tables.Length; ++i) {
				ccount += tables[i].TableSchema.ColumnCount;
			}

			tableColumn_Lokup = new int[ccount];
			columnAdjust = new int[ccount];
			int n = 0;
			int adjust = 0;
			for (int i = 0; i < tables.Length; ++i) {
				int end = n + tables[i].TableSchema.ColumnCount;
				while (n < end) {
					tableColumn_Lokup[n] = i;
					columnAdjust[n] = adjust;
					++n;
				}
				adjust = end;
			}

			this.tables = tables;
			this.columnCount = ccount;
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

		// Joined tables have a null table name
		public TableName TableName {
			get { return null; }
		}

		public int ColumnCount {
			get { return columnCount; }
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
		
		protected int IndexOfTable(int columnOffset) {
			return tableColumn_Lokup[columnOffset];
		}

		protected int AdjustColumn(int columnOffset) {
			return columnOffset - columnAdjust[columnOffset];
		}

		protected abstract long AdjustRow(long row, int tableIndex);


		public Variable GetColumnName(int column) {
			return tables[IndexOfTable(column)].TableSchema.GetColumn(AdjustColumn(column)).Name;
		}

		public int GetColumnOffset(string v) {
			int sz = ColumnCount;
			for (int i = 0; i < sz; ++i) {
				if (GetColumnName(i).Equals(v)) {
					return i;
				}
			}
			return -1;
		}

		public SqlType GetColumnType(int column) {
			return tables[IndexOfTable(column)].TableSchema.GetColumn(AdjustColumn(column)).Type;
		}


		public SqlObject GetValue(int column, long rowid) {
			// Null value if the rowid is less than 0
			if (rowid < 0)
				return SqlObject.MakeNull(GetColumnType(column));

			int tableIndex = IndexOfTable(column);
			// Adjust the column to the table the column is located
			int tableColumn = AdjustColumn(column);
			// Adjust the row by the table
			long tableRow = AdjustRow(rowid, tableIndex);

			// Fetch and return the data
			return tables[tableIndex].GetValue(tableColumn, tableRow);
		}

		public void FetchValue(int column, long rowid) {
			if (rowid < 0)
				return;

			if (column == -1) {
				for (int i = 0; i < tables.Length; ++i) {
					tables[i].PrefetchValue(-1, AdjustRow(rowid, i));
				}
			} else {
				int tableIndex = IndexOfTable(column);
				// Adjust the column to the table the column is located
				int tableColumn = AdjustColumn(column);
				// Adjust the row by the table
				long tableRow = AdjustRow(rowid, tableIndex);

				// Delegate the hint to the parent table
				tables[tableIndex].PrefetchValue(tableColumn, tableRow);
			}
		}


		public IRowCursor GetRowCursor() {
			// A simple interator between 0 and RowCount
			return new SimpleRowCursor(RowCount);
		}
		
		IEnumerator<long> IEnumerable<long>.GetEnumerator() {
			return GetRowCursor();
		}
		
		IEnumerator IEnumerable.GetEnumerator() {
			return GetRowCursor();
		}

		public void Dispose() {
			// Dispose all the parent tables,
			foreach (ITableDataSource table in tables) {
				table.Dispose();
			}
		}
	}
}