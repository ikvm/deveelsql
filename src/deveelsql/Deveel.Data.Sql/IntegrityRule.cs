using System;

namespace Deveel.Data.Sql {
	public sealed class IntegrityRule {
		private readonly TableName name;
		private readonly TableName tableName;
		private readonly IntegrityRuleKind kind;
		private readonly string[] columnNames;
		private readonly TableName refTableName;
		private readonly string[] refColumnNames;

		internal IntegrityRule(TableName name, TableName tableName, IntegrityRuleKind kind, string[] columnNames) {
			this.name = name;
			this.columnNames = columnNames;
			this.kind = kind;
			this.tableName = tableName;
		}

		internal IntegrityRule(TableName name, TableName tableName, IntegrityRuleKind kind, string[] columnNames, TableName refTableName, string [] refColumnNames)
			: this(name, tableName, kind, columnNames) {
			this.refTableName = refTableName;
			this.refColumnNames = refColumnNames;

		}

		public TableName Name {
			get { return name; }
		}

		public IntegrityRuleKind Kind {
			get { return kind; }
		}

		public bool IsForeignKey {
			get {
				return kind == IntegrityRuleKind.ExportedForeignKey ||
				       kind == IntegrityRuleKind.ImportedForeignKey;
			}
		}

		public bool IsUnique {
			get { return kind == IntegrityRuleKind.Unique; }
		}

		public bool IsPrimary {
			get { return kind == IntegrityRuleKind.Primary; }
		}

		public TableName TableName {
			get { return tableName; }
		}

		public string[] ColumnNames {
			get { return columnNames; }
		}

		public TableName ReferencedTableName {
			get { return refTableName; }
		}

		public string[] ReferencedColumnNames {
			get { return refColumnNames; }
		}
	}
}