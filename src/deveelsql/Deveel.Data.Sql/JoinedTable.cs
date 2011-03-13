using System;

using Deveel.Data.Base;

namespace Deveel.Data.Sql {
	public class JoinedTable : JoinedTableBase {
		private readonly IIndexCursor[] tableLists;
		private readonly long rowCount;

		public JoinedTable(ITableDataSource[] tables, IIndex[] lists)
			: base(tables) {

			if (tables.Length != lists.Length)
				throw new ApplicationException("'tables' and 'lists' size do not match.");

			tableLists = new IIndexCursor[lists.Length];

			// Work out the row count (all the lists should be the same size).
			long rcount = lists[0].Count;
			for (int i = 0; i < lists.Length; ++i) {
				if (rcount != lists[i].Count)
					throw new ApplicationException("List " + i + " is not equal size to rest in list.");

				tableLists[i] = lists[i].GetCursor();
			}

			rowCount = rcount;
		}

		public JoinedTable(ITableDataSource left, ITableDataSource right, IIndex left_list, IIndex right_list)
			: this(new ITableDataSource[] { left, right }, new IIndex[] { left_list, right_list }) {
		}

		public JoinedTable(ITableDataSource left, ITableDataSource right)
			: this(left, right, Indexes.Empty, Indexes.Empty) {
		}

		protected override long AdjustRow(long row, int tableIndex) {
			// Note that row will be between 0 and row count
			IIndexCursor iterator = tableLists[tableIndex];
			iterator.Position = row - 1;
			if (!iterator.MoveNext())
				throw new SystemException();
			return iterator.Current;
		}

		public override long RowCount {
			get { return rowCount; }
		}

	}

}