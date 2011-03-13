using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public class FilteredTable : ITableDataSource {
		private ITableDataSource filter;
		private Expression orderComposite;
		
		public FilteredTable(ITableDataSource filter) {
			this.filter = filter;
		}
		
		public virtual int ColumnCount {
			get { return filter.ColumnCount; }
		}
		
		public virtual long RowCount {
			get { return filter.RowCount; }
		}
		
		public virtual TableName TableName {
			get { return filter.TableName; }
		}
		
		public Expression OrderComposite {
			get { return orderComposite; }
			set { orderComposite = value; }
		}
		
		public ITableDataSource BaseTable {
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
			orderComposite =  child_composite;
		}
		
		public virtual int GetColumnOffset(Variable columnName) {
			return filter.GetColumnOffset(columnName);
		}
		
		public virtual Variable GetColumnName(int offset) {
			return filter.GetColumnName(offset);
		}
		
		public virtual SqlType GetColumnType(int offset) {
			return filter.GetColumnType(offset);
		}
		
		public virtual SqlObject GetValue(int column, long row) {
			return filter.GetValue(column, row);
		}
		
		public virtual void FetchValue(int column, long row) {
			filter.FetchValue(column, row);
		}
		
		public virtual IRowCursor GetRowCursor() {
			return filter.GetRowCursor();
		}
		
		public virtual void Dispose() {
			if (filter != null)
				filter.Dispose();
			filter = null;
		}
		
		IEnumerator<long> IEnumerable<long>.GetEnumerator() {
			return GetRowCursor();
		}
		
		IEnumerator IEnumerable.GetEnumerator() {
			return GetRowCursor();
		}
	}
}