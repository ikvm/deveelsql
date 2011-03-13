using System;

namespace Deveel.Data.Sql {
	class CollationIndexResolver : IndexResolver {
		private ITableDataSource table;
		private int[] columns;

		public CollationIndexResolver(ITableDataSource table, IndexCollation collation)
			: base() {
			this.table = table;
			// Resolve the column names in the table into index references
			int sz = collation.Columns.Length;
			columns = new int[sz];

			TableName tname = table.TableName;

			for (int i = 0; i < sz; ++i) {
				string colName = collation.Columns[i].ColumnName;
				int colOffset = table.GetColumnOffset(new Variable(tname, colName));
				if (colOffset == -1)
					throw new ApplicationException("Column '" + colName + "' not found.");
				
				columns[i] = colOffset;
			}

			// TODO: handle function indexes
		}

		public override SqlObject[] GetValue(long rowid) {
			SqlObject[] values;
			if (columns.Length == 1) {
				// Single value
				values = new SqlObject[] { table.GetValue(columns[0], rowid) };
			} else {
				// Composite value so create a composite object as the key.
				int sz = columns.Length;
				values = new SqlObject[sz];
				for (int i = 0; i < sz; ++i) {
					values[i] = table.GetValue(columns[i], rowid);
				}
			}
			return values;
		}

	}
}