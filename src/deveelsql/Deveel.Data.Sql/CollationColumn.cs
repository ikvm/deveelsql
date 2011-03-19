using System;

namespace Deveel.Data.Sql {
	public sealed class CollationColumn {
		private readonly string columnName;
		private readonly bool ascending;

		public CollationColumn(string columnName, bool ascending) {
			this.columnName = columnName;
			this.ascending = ascending;
		}

		public CollationColumn(string columnName)
			: this(columnName, true) {
		}

		public bool Ascending {
			get { return ascending; }
		}

		public string ColumnName {
			get { return columnName; }
		}
	}
}