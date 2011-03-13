using System;

namespace Deveel.Data.Sql {
	public sealed class IndexCollation {
		private readonly string function;
		private readonly IndexSortColumn[] columns;

		public IndexCollation(IndexSortColumn[] columns, string function) {
			this.function = function;
			this.columns = (IndexSortColumn[])columns.Clone();

		}

		public IndexCollation(string columnName, SqlType type)
			: this(new IndexSortColumn(columnName, type)) {
		}

		public IndexCollation(IndexSortColumn column)
			: this(new IndexSortColumn[] { column }, null) {
		}

		public IndexSortColumn[] Columns {
			get { return columns; }
		}

		public string Function {
			get { return function; }
		}

		public bool MatchesColumn(string columnName) {
			return (Function == null && Columns.Length == 1 && Columns[0].ColumnName.Equals(columnName));
		}

		public bool ContainsColumn(string columnName) {
			if (Function != null)
				return false;

			for (int i = 0; i < Columns.Length; i++) {
				if (Columns[i].ColumnName == columnName)
					return true;
			}

			return false;
		}
	}
}