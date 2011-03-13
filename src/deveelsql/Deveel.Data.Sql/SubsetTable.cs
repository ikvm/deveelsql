using System;

namespace Deveel.Data.Sql {
	public class SubsetTable : FilteredTable {

		private readonly IRowCursor subset;
		private bool indexRequestsFallthrough;


		public SubsetTable(ITableDataSource child, IRowCursor subset)
			: base(child) {
			this.subset = subset;
			indexRequestsFallthrough = false;
		}

		public SubsetTable(ITableDataSource parent)
			: this(parent, new SimpleRowCursor(0)) {
		}

		public bool IndexRequestFallthrough {
			set { indexRequestsFallthrough = value; }
			get { return indexRequestsFallthrough; }
		}

		public override long RowCount {
			get { return subset.Count; }
		}

		public override IRowCursor GetRowCursor() {
			return (IRowCursor) subset.Clone();
		}
	}
}