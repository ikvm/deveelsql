using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Sql {
	internal class SystemColumnsTable : ITable {
		private readonly SystemTransaction transaction;
		private readonly long id;
		private readonly ColumnCollection columns;

		public SystemColumnsTable(SystemTransaction transaction, long id) {
			this.transaction = transaction;
			this.id = id;

			columns = new ColumnCollection(this);

			SetupColumns();
		}

		public void Dispose() {
		}

		public IColumnCollection Columns {
			get { return columns; }
		}

		public long RowCount {
			get { throw new NotImplementedException(); }
		}

		public TableName Name {
			get { throw new NotImplementedException(); }
		}

		private void SetupColumns() {
			
		}

		public IRowCursor GetRowCursor() {
			throw new NotImplementedException();
		}

		public TableRow GetRow(RowId rowid) {
			throw new NotImplementedException();
		}

		public void PrefetchValue(int columnOffset, RowId rowid) {
			throw new NotImplementedException();
		}

		public SqlObject GetValue(int columnOffset, RowId rowid) {
			throw new NotImplementedException();
		}

		public bool RowExists(RowId rowid) {
			throw new NotImplementedException();
		}
	}
}