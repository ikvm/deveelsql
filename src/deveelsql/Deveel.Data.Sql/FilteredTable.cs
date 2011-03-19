using System;

using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	public class FilteredTable : ITable {
		private ITable filter;
		private Expression orderComposite;

		public FilteredTable(ITable filter) {
			this.filter = filter;
		}

		public virtual IColumnCollection Columns {
			get { return filter.Columns; }
		}

		public virtual long RowCount {
			get { return filter.RowCount; }
		}

		public virtual TableName Name {
			get { return filter.Name; }
		}

		public Expression OrderComposite {
			get { return orderComposite; }
			set { orderComposite = value; }
		}

		public ITable BaseTable {
			get { return filter; }
		}

		internal void SetOrderCompositeIsChild() {
			// Get from the child
			Expression child_composite = null;

			if (filter is FilteredTable) {
				child_composite = ((FilteredTable)filter).OrderComposite;
			} else if (filter is JoinedTable) {
				child_composite = ((JoinedTable)filter).OrderComposite;
			}
			orderComposite = child_composite;
		}

		public virtual IRowCursor GetRowCursor() {
			return filter.GetRowCursor();
		}

		public virtual TableRow GetRow(RowId rowid) {
			return filter.GetRow(rowid);
		}

		public virtual void PrefetchValue(int columnOffset, RowId rowid) {
			filter.PrefetchValue(columnOffset, rowid);
		}

		public virtual SqlObject GetValue(int columnOffset, RowId rowid) {
			return filter.GetValue(columnOffset, rowid);
		}

		public virtual bool RowExists(RowId rowid) {
			return filter.RowExists(rowid);
		}

		public virtual void Dispose() {
			if (filter != null)
				filter.Dispose();
			filter = null;
		}
	}
}