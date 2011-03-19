using System;
using System.Collections;
using System.Collections.Generic;

using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	class SystemTableDataSource : IMutableTableDataSource {
		private readonly SystemTransaction transaction;
		private long version;
		private readonly ITable table;
		private TableRow currentRow;
		private int opType = -1;
		private readonly bool commitable;

		private readonly TransactionJournal journal;
		private readonly Dictionary<long, TableRow> insertRows;
		private readonly Dictionary<long, TableRow> updateInsertRows;
		private readonly List<long> updateRemoveRowList;
		
		private bool changed;
		private bool invalid;


		private const int OperationInsert = 1;
		private const int OperationUpdate = 2;

		public SystemTableDataSource(SystemTransaction transaction, ITable table) {
			this.transaction = transaction;
			version = 0;
			this.table = table;

			commitable = (table is ICommitableTable);

			journal = new TransactionJournal();
			insertRows = new Dictionary<long, TableRow>(10);
			updateInsertRows = new Dictionary<long, TableRow>(10);
			updateRemoveRowList = new List<long>(10);
		}

		public long Id {
			get { return table.Id; }
		}

		public long Version {
			get { return version; }
		}

		public TransactionJournal Journal {
			get { return journal; }
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
			get { return table.Name; }
		}

		private void OnChanged() {
			// Only notify if the 'table_changed' flag is false
			if (!changed) {
				changed = true;
				transaction.OnTableChanged(Id);
			}
		}

		private static void CopyRow(SystemTableDataSource source, long srcRowid, SystemTableDataSource dest, long dstRowid) {
			dest.BeginUpdate(dstRowid);
			int sz = source.ColumnCount;
			for (int i = 0; i < sz; ++i) {
				SqlType colType = source.table.TableSchema.GetColumn(i).Type;
				SqlValue srcValue = source.table.GetValue(i, srcRowid);
				SqlObject srcObj = new SqlObject(colType, srcValue);
				if (srcValue != null) {
					dest.SetValue(i, dstRowid, srcObj);
				}
			}
			dest.Finish(true);
		}

		private void OnStructureChanged() {
			transaction.OnTableStructureChanged(Id);
		}

		private void CheckValid() {
			if (invalid)
				throw new ApplicationException("The table has been invalidated.");
		}

		private bool IsValidMutableRow(long rowid) {
			if (updateInsertRows.ContainsKey(rowid))
				return true;

			if (insertRows.ContainsKey(rowid))
				return true;

			return false;
		}

		internal void Invalidate() {
			invalid = true;
		}

		internal void IncrementVersion() {
			version++;
		}

		internal void CopyRowIdFrom(SystemTableDataSource from, long row_id) {
			CopyRow(from, row_id, this, row_id);
			journal.AddEntry(JournalCommandCode.RowRemove, row_id);
			// Notfy the backed transaction that we changed the table
			OnChanged();
		}

		internal void RemoveRowId(long row_id) {
			CheckValid();

			TableRow row = table.GetRow(row_id);
			// If the rowid wasn't found,)
			if (row == null || !row.Exists)
				throw new ApplicationException("DELETE ROW on '" + table.Name + "' failed:" +
				                          " row was updated or deleted in a concurrent transaction");

			// Otherwise, remove the value at the position

			// Update the journal with this record removal
			journal.AddEntry(JournalCommandCode.RowRemove, row_id);
			// Notfy the backed transaction that we changed the table
			OnChanged();

		}

		internal void CopyEntirelyTo(SystemTransaction destination, long destTableId) {
			SystemTableDataSource destTable = destination.GetTable(destTableId);
			if (!commitable || !destTable.commitable)
				throw new NotSupportedException();

			((ICommitableTable)table).CopyTo(destTable.table);
		}

		internal void CopyEntirelyTo(SystemTransaction destination) {
			CopyEntirelyTo(transaction, Id);
		}

		public void AddColumn(string name, SqlType type, bool notNull) {
			CheckValid();

			TableColumn column;

			try {
				column = table.TableSchema.AddColumn(name, type, notNull);
				if (!column.Exists)
					throw new ApplicationException("The column '" + name + "' does not exists after creation.");

				journal.AddEntry(JournalCommandCode.ColumnAdd, column.Id);
				OnStructureChanged();
			} catch (Exception e) {
				throw new ApplicationException("Error while creating column '" + name + "' on table '" + table.Name + "': " + e.Message, e);
			}
		}

		public void RemoveColumn(int offset) {
			CheckValid();

			try {
				TableColumn column = table.TableSchema.GetColumn(offset);
				if (column == null)
					throw new ArgumentOutOfRangeException();
				if (!column.Exists)
					throw new ApplicationException("The column '" + column.Name + "' does not exists.");

				// TODO: Check that this column isn't referenced in any indexes. Fail if we are.

				table.TableSchema.RemoveColumn(offset);

				journal.AddEntry(JournalCommandCode.ColumnRemove, column.Id);
				OnStructureChanged();
			} catch (Exception e) {
				throw new ApplicationException("Error while removing column from table '" + table.Name + "' at index '" + offset + "': " + e.Message, e);
			}
		}

		public int GetColumnOffset(string columnName) {
			CheckValid();
			return table.TableSchema.GetColumnOffset(columnName);
		}

		public Variable GetColumnName(int offset) {
			CheckValid();
			return new Variable(table.Name, table.TableSchema.GetColumn(offset).Name);
		}

		public SqlType GetColumnType(int offset) {
			CheckValid();
			return table.TableSchema.GetColumn(offset).Type;
		}

		public SqlObject GetValue(int column, long rowid) {
			CheckValid();

			if (table.RowExists(rowid)) {
				SqlValue value = table.GetValue(column, rowid);
				SqlType type = table.TableSchema.GetColumn(column).Type;
				return new SqlObject(type, value);
			}

			TableRow row;
			if (commitable && insertRows.TryGetValue(rowid, out row))
				return row.GetValue(column);
			if (commitable && updateInsertRows.TryGetValue(rowid, out row))
				return row.GetValue(column);

			throw new ApplicationException("Invalid rowid");
		}

		public void FetchValue(int column, long row) {
			table.PrefetchValue(column, row);
		}

		public IRowCursor GetRowCursor() {
			return table.GetRowCursor();
		}

		public void SetValue(int column, long rowid, SqlObject value) {
			CheckValid();

			if (!commitable) {
				if (opType == -1)
					throw new InvalidOperationException("No operation was started.");

				currentRow.SetValue(column, value);
			} else {
				if (IsValidMutableRow(rowid)) {
					
				}
			}
		}

		public void Finish(bool complete) {
			CheckValid();

			if (!complete) {
				// Clear the lists
				insertRows.Clear();
				updateRemoveRowList.Clear();
				updateInsertRows.Clear();

				opType = -1;
				currentRow = null;
			}

			if (!commitable) {
				if (opType == OperationInsert) {
					table.Insert(currentRow);
				} else if (opType == OperationUpdate) {
					table.Update(currentRow);
				}
			} else {
				// This is a commitable table, so the cached rows are processed in a later moment
				ICommitableTable commitableTable = (ICommitableTable) table;

				commitableTable.BeginCommit();

				foreach (KeyValuePair<long, TableRow> pair in insertRows) {
					commitableTable.Insert(pair.Value);
					journal.AddEntry(JournalCommandCode.RowAdd, pair.Key);
				}
				foreach (long rowid in updateRemoveRowList) {
					commitableTable.Delete(rowid);
					journal.AddEntry(JournalCommandCode.RowRemove, rowid);
				}
				foreach (KeyValuePair<long, TableRow> pair in updateInsertRows) {
					commitableTable.Update(pair.Value);
					journal.AddEntry(JournalCommandCode.RowUpdate, pair.Key);
					journal.AddEntry(JournalCommandCode.RowAdd, pair.Key);
				}
				commitableTable.EndCommit();

				// Notify the transaction that something changed
				OnChanged();
				// Clear the lists
				insertRows.Clear();
				updateRemoveRowList.Clear();
				updateInsertRows.Clear();
			}
		}

		public long BeginInsert() {
			CheckValid();

			if (!commitable) {
				if (opType != -1)
					throw new InvalidOperationException("Another operation was started and not completed.");

				opType = OperationInsert;
				currentRow = table.NewRow();

				if (!currentRow.HasRowId)
					throw new SystemException("Unable to create a new row.");

				return currentRow.Id;
			}
			TableRow row = table.NewRow();
			if (!row.HasRowId)
				throw new SystemException("Unable to create a new row.");

			insertRows.Add(row.Id, row);
			return row.Id;
		}

		public long BeginUpdate(long rowid) {
			if (!commitable) {
				if (opType != -1)
					throw new InvalidOperationException("Another operation was started and not completed.");

				if (!table.RowExists(rowid))
					throw new ApplicationException("Invalid rowid");

				opType = OperationUpdate;
				currentRow = new TableRow(table, rowid);
			} else {
				// We can't update a record that isn't in the main index or is already
				// in the process of being updated.
				if (!table.RowExists(rowid) ||
				    updateRemoveRowList.Contains(rowid))
					throw new ApplicationException("Invalid rowid");

				// Create a unique record identifier for the updated row
				TableRow row = table.NewRow();
				if (!row.HasRowId)
					throw new SystemException();

				// Copy all the row data,
				CopyRow(this, rowid, this, row.Id);


				// Remember that this is a record we are updating
				updateInsertRows.Add(row.Id, row);
				updateRemoveRowList.Add(rowid);
			}
			return rowid;
		}

		public void Remove(long rowid) {
			CheckValid();

			if (commitable) {
				if (!table.RowExists(rowid)) {
					// If it's not in the main index, it may be in the insert or update
					// buffer, which we can remove the data instantly.
					updateInsertRows.Remove(rowid);
					insertRows.Remove(rowid);
				} else {
					table.Delete(rowid);
					journal.AddEntry(JournalCommandCode.RowRemove, rowid);
					// Notfy the backed transaction that we changed the table
					OnChanged();
				}
			} else {
				table.Delete(rowid);
			}
		}

		public void Remove(IRowCursor rows) {
			while (rows.MoveNext()) {
				Remove(rows.Current);
			}
		}
	}
}