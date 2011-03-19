using System;

namespace Deveel.Data.Sql {
	public sealed class IndexCollation {
		private readonly string function;
		private readonly SqlType type;
		private readonly CollationColumn[] columns;

		public IndexCollation(SqlType type, CollationColumn[] columns, string function) {
			if (columns.Length == 0)
				throw new ArgumentException("Cannot create an empty collation.", "columns");

			if (columns.Length > 1) {
				if (!(type is SqlCompositeType))
					throw new ArgumentException("Composite indexes must be represented by composite-type");

				SqlCompositeType ctype = (SqlCompositeType) type;
				if (ctype.PartCount != columns.Length)
					throw new ArgumentException("Composite type size different to the number of columns given", "columns");
			}

			// Can't contain identical columns
			for (int i = 0; i < columns.Length; ++i) {
				for (int n = 0; n < columns.Length; ++n) {
					if (i != n && columns[i].ColumnName.Equals(columns[n].ColumnName))
						throw new ArgumentException("Repeat column '" + columns[i] +"' name");
				}
			}

			this.type = type;
			this.function = function;
			this.columns = (CollationColumn[]) columns.Clone();
		}

		public IndexCollation(SqlType type, string columnName, bool ascending)
			: this(type, new CollationColumn(columnName, ascending)) {
		}

		public IndexCollation(SqlType type, string columnName)
			: this(type, new CollationColumn(columnName)) {
		}

		public IndexCollation(SqlType type, CollationColumn column)
			: this(type, new CollationColumn[] { column }, null) {
		}

		public IndexCollation(SqlType type, CollationColumn[] columns)
			: this(type, columns, null) {
		}

		public SqlType Type {
			get { return type; }
		}

		public int PartCount {
			get { return columns.Length; }
		}

		public CollationColumn[] Columns {
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

		public IndexCollation Reverse() {
			CollationColumn[] reverseColumns = new CollationColumn[columns.Length];
			for (int i = 0; i < columns.Length; i++)
				reverseColumns[i] = new CollationColumn(columns[i].ColumnName, !columns[i].Ascending);

			return new IndexCollation(type, reverseColumns, function);
		}
	}
}