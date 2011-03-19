using System;

namespace Deveel.Data.Sql {
	public sealed class ForeignKey : Constraint {
		private readonly TableName refTableName;
		private readonly string[] destColumns;
		private readonly string updateAction;
		private readonly string deleteAction;

		public ForeignKey(TableName sourceTable, string[] sourceColumns, TableName refTableName, string[] destColumns, 
			string updateAction, string deleteAction, bool deferrable, bool deferred)
			: base(sourceTable, ConstraintType.ForeignKey, sourceColumns, deferrable, deferred) {
			this.deleteAction = deleteAction;
			this.updateAction = updateAction;
			this.destColumns = destColumns;
			this.refTableName = refTableName;
		}

		public string DeleteAction {
			get { return deleteAction; }
		}

		public string UpdateAction {
			get { return updateAction; }
		}

		public string[] ReferencedColumns {
			get { return destColumns; }
		}

		public TableName ReferencedTableName {
			get { return refTableName; }
		}
	}
}