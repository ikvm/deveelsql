using System;

using Deveel.Data.Base;
using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	public sealed class JoinedTable : JoinedTableBase {
		private readonly IIndexCursor<RowId>[] tableLists;
		private readonly long rowCount;

		public JoinedTable(ITable[] tables, IIndex<RowId>[] lists)
			: base(tables) {

			if (tables.Length != lists.Length)
				throw new ApplicationException("'tables' and 'lists' size do not match.");

			tableLists = new IIndexCursor<RowId>[lists.Length];

			// Work out the row count (all the lists should be the same size).
			long rcount = lists[0].Count;
			for (int i = 0; i < lists.Length; ++i) {
				if (rcount != lists[i].Count)
					throw new ApplicationException("List " + i + " is not equal size to rest in list.");

				tableLists[i] = lists[i].GetCursor();
			}

			rowCount = rcount;
		}

		public JoinedTable(ITable left, ITable right, IIndex<RowId> leftList, IIndex<RowId> rightList)
			: this(new ITable[] { left, right }, new IIndex<RowId>[] { leftList, rightList }) {
		}

		public override long RowCount {
			get { return rowCount; }
		}

		protected override RowId AdjustRow(long row, int tableIndex) {
			// Note that row will be between 0 and row count
			IIndexCursor<RowId> cursor = tableLists[tableIndex];
			cursor.Position = row - 1;
			if (!cursor.MoveNext())
				throw new SystemException();
			return cursor.Current;
		}
	}
}