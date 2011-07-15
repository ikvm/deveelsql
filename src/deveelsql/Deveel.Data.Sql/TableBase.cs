using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Sql {
	public abstract class TableBase : IMutableTable {
		private readonly ColumnCollection columns;
		private readonly TableName tableName;

		protected TableBase(TableName tableName) {
			this.tableName = tableName;
			columns = new ColumnCollection(this);

			DoSetupColumns();
		}

		public void Dispose() {
		}

		public IColumnCollection Columns {
			get { return columns; }
		}

		public abstract long RowCount { get; }

		public TableName Name {
			get { return tableName; }
		}

		private void DoSetupColumns() {
			SetupColumns();
		}

		protected abstract void SetupColumns();

		public virtual IRowCursor GetRowCursor() {
			return new SimpleRowCursor(RowCount);
		}

		public virtual TableRow GetRow(RowId rowid) {
			return new TableRow(this, rowid);
		}

		public virtual void PrefetchValue(int columnOffset, RowId rowid) {
		}

		public abstract SqlObject GetValue(int columnOffset, RowId rowid);

		public virtual bool RowExists(RowId rowid) {
			return false;
		}

		public virtual TableRow NewRow() {
			throw new NotSupportedException();
		}

		public void Insert(TableRow row) {
			throw new NotSupportedException();
		}

		public virtual void Update(TableRow row) {
			throw new NotSupportedException();
		}

		public virtual void Delete(RowId rowid) {
			throw new NotSupportedException();
		}

		public virtual void Commit() {
		}

		public virtual void Undo() {
		}
	}
}