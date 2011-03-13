using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public sealed class SystemTableDataSource : IMutableTableDataSource {
		private readonly TableName tableName;
		private readonly ITable table;
		private TableRow currentRow;
		private int opType = -1;

		private const int OperationInsert = 1;
		private const int OperationUpdate = 2;

		public SystemTableDataSource(TableName tableName, ITable table) {
			this.tableName = tableName;
			this.table = table;
		}

		public void Dispose() {
			//TODO:
		}

		public IEnumerator<long> GetEnumerator() {
			return GetRowCursor();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public int ColumnCount {
			get { return table.TableSchema.ColumnCount; }
		}

		public long RowCount {
			get { return table.RowCount; }
		}

		public TableName TableName {
			get { return tableName; }
		}

		public int GetColumnOffset(Variable columnName) {
			return table.TableSchema.GetColumnOffset(columnName.Name);
		}

		public Variable GetColumnName(int offset) {
			return new Variable(tableName, table.TableSchema.GetColumn(offset).Name);
		}

		public SqlType GetColumnType(int offset) {
			return table.TableSchema.GetColumn(offset).Type;
		}

		public SqlObject GetValue(int column, long row) {
			SqlValue value = table.GetValue(column, row);
			SqlType type = table.TableSchema.GetColumn(column).Type;
			return new SqlObject(type, value);
		}

		public void FetchValue(int column, long row) {
			table.PrefetchValue(column, row);
		}

		public IRowCursor GetRowCursor() {
			return table.GetRowCursor();
		}

		public void SetValue(int column, SqlObject value) {
			if (opType == -1)
				throw new InvalidOperationException("No operation was started.");

			currentRow.SetValue(column, value);
		}

		public void Finish(bool complete) {
			if (!complete) {
				opType = -1;
				currentRow = null;
			}

			if (opType == OperationInsert) {
				table.Insert(currentRow);
			} else if (opType == OperationUpdate) {
				table.Update(currentRow);
			}
		}

		public void BeginInsert() {
			if (opType != -1)
				throw new InvalidOperationException("Another operation was started and not completed.");

			opType = OperationInsert;
			currentRow = new TableRow(table);
		}

		public void BeginUpdate(long rowid) {
			if (opType != -1)
				throw new InvalidOperationException("Another operation was started and not completed.");

			opType = OperationUpdate;
			currentRow = new TableRow(table, rowid);
		}

		public void Remove(long rowid) {
			table.Delete(rowid);
		}

		public void Remove(IRowCursor rows) {
			throw new NotSupportedException();
		}
	}
}