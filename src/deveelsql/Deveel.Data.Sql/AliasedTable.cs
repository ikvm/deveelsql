using System;

using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	public class AliasedTable : FilteredTable {
		private readonly TableName alias;

		public AliasedTable(ITable filter, TableName alias)
			: base(filter) {
			this.alias = alias;
		}

		public override TableName Name {
			get { return alias; }
		}
	}
}