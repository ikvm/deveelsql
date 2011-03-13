using System;

namespace Deveel.Data.Sql {
	public sealed class IndexSortColumn {
		private readonly string columnName;
		private readonly SqlType type;
		private readonly bool ascending;

		public IndexSortColumn(string columnName, SqlType type, bool ascending) {
			this.columnName = columnName;
			this.ascending = ascending;
			this.type = type;
		}

		public IndexSortColumn(string columnName, SqlType type)
			: this(columnName, type, true) {
		}

		public bool Ascending {
			get { return ascending; }
		}

		public SqlType Type {
			get { return type; }
		}

		public string ColumnName {
			get { return columnName; }
		}
	}
}