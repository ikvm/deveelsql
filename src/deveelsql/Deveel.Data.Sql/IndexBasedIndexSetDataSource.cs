using System;

using Deveel.Data.Base;

namespace Deveel.Data.Sql {
	public class IndexBasedIndexSetDataSource : IndexSetDataSourceBase {

		private readonly ITableDataSource table;
		private readonly IndexResolver resolver;

		public IndexBasedIndexSetDataSource(ITableDataSource table, IndexResolver resolver, IIndex index)
			: base(index) {
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
			get { return table.TableName; }
		}

		public override TableName Name {
			get { return TableName.Resolve("Index"); }
		}
	}
}