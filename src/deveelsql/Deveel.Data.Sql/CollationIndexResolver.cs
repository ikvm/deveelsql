using System;

using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	class CollationIndexResolver : IndexResolver {
		private readonly ITable table;
		private readonly int[] columns;

		public CollationIndexResolver(ITable table, IndexCollation collation) {
			this.table = table;
			// Resolve the column names in the table into index references
			int sz = collation.Columns.Length;
			columns = new int[sz];

			TableName tname = table.Name;

			for (int i = 0; i < sz; ++i) {
				string colName = collation.Columns[i].ColumnName;
				int colOffset = table.Columns.IndexOf(colName);
				if (colOffset == -1)
					throw new ApplicationException("Column '" + colName + "' not found.");
				
				columns[i] = colOffset;
			}

			// TODO: handle function indexes
		}

		public override SqlObject[] GetValue(RowId rowid) {
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