using System;

namespace Deveel.Data.Sql {
	public static class SystemTableNames {
		public const string InformationSchema = "INFORMATION_SCHEMA";
		
		public static readonly TableName OneRowTable = new TableName(InformationSchema, "OneRowTable");
	}
}