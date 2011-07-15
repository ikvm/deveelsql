using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Sql {
	public class NaturalJoinedTable : JoinedTableBase {

		private readonly IRowCursor[] cursors;
		private readonly long rowCount;

		public NaturalJoinedTable(ITable[] tables)
			: base(tables) {
			// Set up the row cursors,
			rowCount = 1;
			cursors = new IRowCursor[tables.Length];
			for (int i = 0; i < tables.Length; ++i) {
				cursors[i] = tables[i].GetRowCursor();
				rowCount = rowCount * tables[i].RowCount;
			}
		}

		public NaturalJoinedTable(ITable left, ITable right)
			: this(new ITable[] { left, right }) {
		}

		public override long RowCount {
			get { return rowCount; }
		}

		protected override RowId AdjustRow(long row, int tableIndex) {
			// NOTE, we know that row will be between 0 and RowCount,
			long divAmount = 1;
			for (int i = Tables.Length - 1; i > tableIndex; --i)
				divAmount *= Tables[i].RowCount;

			long absoluteRow = (row / divAmount) % Tables[tableIndex].RowCount;

			// Now translate to a row using the cursor,
			IRowCursor cursor = cursors[tableIndex];
			// Position is -1 so 'next' will return the current
			cursor.MoveTo(absoluteRow - 1);
			// And return
			if (!cursor.MoveNext())
				throw new SystemException();

			return cursor.Current;
		}
	}
}