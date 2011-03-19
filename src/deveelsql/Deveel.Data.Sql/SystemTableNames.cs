using System;

namespace Deveel.Data.Sql {
	public static class SystemTableNames {
		public const string SystemSchema = "SYSTEM";

		public static readonly TableName OneRowTable = new TableName(SystemSchema, "OneRowTable");
		public static readonly TableName EmptyTable = new TableName(SystemSchema, "EmptyRowTable");

		public static readonly TableName Tables = new TableName(SystemSchema, "Tables");
		public static readonly TableName Index = new TableName(SystemSchema, "Index");
		public static readonly TableName ColumnSet = new TableName(SystemSchema, "ColumnSet");
		public static readonly TableName ConstraintsUnique = new TableName(SystemSchema, "ConstraintsUnique");
		public static readonly TableName ConstraintsForeign = new TableName(SystemSchema, "ConstraintsForeign");
		public static readonly TableName ConstraintsCheck = new TableName(SystemSchema, "ConstraintsCheck");
		public static readonly TableName DefaultColumnExpression = new TableName(SystemSchema, "DefaultColumnExpressions");
		public static readonly TableName Schema = new TableName(SystemSchema, "Schema");
	}
}