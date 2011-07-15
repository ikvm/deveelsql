using System;

using Deveel.Data.Base;
using Deveel.Data.Sql;

namespace Deveel.Data.Sql {
	public class IndexBasedIndexSetDataSource : IndexSetDataSourceBase {

		private readonly ITable table;
		private readonly IndexResolver resolver;

		public IndexBasedIndexSetDataSource(ITable table, IndexResolver resolver, IIndex<RowId> index)
			: base(table, index) {
			this.table = table;
			this.resolver = resolver;
		}

		public override IndexCollation Collation {
			get { throw new NotSupportedException(); }
		}

		public override IndexResolver IndexResolver {
			get { return resolver; }
		}

		public override TableName SourceTableName {
			get { return table.Name; }
		}

		public override string Name {
			get { return "Index"; }
		}
	}
}