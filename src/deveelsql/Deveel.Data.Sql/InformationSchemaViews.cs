using System;

namespace Deveel.Data.Sql {
	public static class InformationSchemaViews {
		public const string SchemaName = "INFORMATION_SCHEMA";

		public static readonly TableName OneRowTable = new TableName(SchemaName, "OneRowTable");
		public static readonly TableName EmptyTable = new TableName(SchemaName, "EmptyRowTable");

		public static readonly TableName Schemata = new TableName(SchemaName, "SCHEMATA");
		public static readonly TableName Tables = new TableName(SchemaName, "TABLES");
		public static readonly TableName Columns = new TableName(SchemaName, "COLUMNS");
		public static readonly TableName Indexes = new TableName(SchemaName, "INDEXES");
		public static readonly TableName IndexColumns = new TableName(SchemaName, "INDEX_COLUMNS");
		public static readonly TableName ColumnDefaultExpressions = new TableName(SchemaName, "COLUMN_DEFAULT_EXPRESSIONS");
		public static readonly TableName Constraints = new TableName(SchemaName, "CONSTRAINTS_UNIQUES");
		public static readonly TableName ConstraintColumns = new TableName(SchemaName, "CONSTRAINT_COLUMNS");
		public static readonly TableName Sequences = new TableName(SchemaName, "SEQUENCES");
	}
}