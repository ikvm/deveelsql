using System;

namespace Deveel.Data.Sql {
	public class Constraint {
		private readonly string name;
		private readonly ConstraintType type;
		private readonly TableName tableName;
		private readonly string[] columns;
		private readonly bool deferrable;
		private readonly bool deferred;

		public Constraint(TableName tableName, string name, ConstraintType type, string[] columns, bool deferrable, bool deferred) {
			this.tableName = tableName;
			this.columns = (string[]) columns.Clone();
			this.type = type;
			this.name = name;
			this.deferrable = deferrable;
			this.deferred = deferred;
		}

		public Constraint(TableName tableName, ConstraintType type, string[] columns, bool deferrable, bool deferred)
			: this(tableName, null, type, columns, deferrable, deferred) {
		}

		public Constraint(TableName tableName, string name, ConstraintType type, string[] columns)
			: this(tableName, name, type, columns, true, false) {
		}

		public Constraint(TableName tableName, ConstraintType type, string[] columns)
			: this(tableName, null, type, columns) {
		}

		public bool Deferred {
			get { return deferred; }
		}

		public bool Deferrable {
			get { return deferrable; }
		}

		public string[] Columns {
			get { return columns; }
		}

		public TableName TableName {
			get { return tableName; }
		}

		public ConstraintType Type {
			get { return type; }
		}

		public string Name {
			get { return name; }
		}
	}
}