using System;
using System.Collections;
using System.Collections.Generic;

using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	internal class SystemTable : IMutableTable {
		private readonly ITable backedTable;
		private readonly SystemColumnCollection columns;
		private readonly SystemTransaction transaction;
		private long version;
		private readonly long id;
		private readonly bool commitable;
		private readonly bool mutable;

		private readonly TransactionJournal journal;
		private readonly Dictionary<RowId, TableRow> insertRows;
		private readonly Dictionary<RowId, TableRow> updateInsertRows;
		private readonly List<RowId> updateRemoveRowList;
		private long sessionRowCount;

		private bool changed;
		private bool invalid;

		public static readonly SystemTable Empty;
		public static readonly SystemTable OneRow;

		public SystemTable(SystemTransaction transaction, ITable backedTable, long id) {
			this.transaction = transaction;
			this.backedTable = backedTable;
			this.id = id;

			commitable = (backedTable is ICommitableTable);
			mutable = (backedTable is IMutableTable);

			version = 0;

			if (commitable) {
				insertRows = new Dictionary<RowId, TableRow>();
				updateInsertRows = new Dictionary<RowId, TableRow>();
				updateRemoveRowList = new List<RowId>();
			}

			journal = new TransactionJournal();
			columns = new SystemColumnCollection(this);
		}

		static SystemTable() {
			Empty = new SystemTable(null, new NRowTable(new TableName("EmptyTable"), 0), -1);
			OneRow = new SystemTable(null, new NRowTable(new TableName("OneRowTable"), 1), -1);
		}

		public long Version {
			get { return version; }
		}

		public TransactionJournal Journal {
			get { return journal; }
		}

		public IColumnCollection Columns {
			get { return columns; }
		}

		internal bool IsCommitable {
			get { return commitable; }
		}

		internal bool IsMutable {
			get { return mutable; }
		}

		private IMutableTable Mutable {
			get { return !mutable ? null : (IMutableTable)backedTable; }
		}

		public long RowCount {
			get {
				long baseCount = backedTable.RowCount;
				if (commitable)
					baseCount += sessionRowCount;
				return baseCount;
			}
		}

		public TableName Name {
			get { return backedTable.Name; }
		}

		private void CheckValid() {
			if (invalid)
				throw new ApplicationException("The table has been invalidated.");
		}

		private void OnChanged() {
			// Only notify if the 'table_changed' flag is false
			if (!changed) {
				changed = true;
				transaction.OnTableChanged(id);
			}
		}

		private void OnStructureChanged() {
			transaction.OnTableStructureChanged(id);
		}

		private static void CopyRow(SystemTable source, RowId srcRowid, SystemTable dest, RowId dstRowid) {
			TableRow updateRow = dest.GetRow(dstRowid);
			TableRow sourceRow = source.GetRow(srcRowid);
			int sz = source.Columns.Count;
			for (int i = 0; i < sz; ++i) {
				SqlObject srcObj = sourceRow.GetValue(i);
				if (srcObj != null) {
					updateRow.SetValue(i, srcObj);
				}
			}
			dest.Update(updateRow);
			dest.Commit();
		}

		public IRowCursor GetRowCursor() {
			if (!IsCommitable)
				return backedTable.GetRowCursor();

			return new SystemRowCursor(this, backedTable.GetRowCursor(), updateInsertRows.Keys);
		}

		public TableRow NewRow() {
			CheckMutable();
			return Mutable.NewRow();
		}

		private void CheckMutable() {
			if (!IsMutable)
				throw new InvalidOperationException("The table is read-only.");
		}

		internal void Invalidate() {
			invalid = true;
		}

		internal void IncrementVersion() {
			version++;
		}

		internal void CopyRowIdFrom(SystemTable from, RowId rowId) {
			CopyRow(from, rowId, this, rowId);
			journal.AddEntry(JournalCommandCode.RowRemove, rowId.ToInt64());
			// Notfy the backed transaction that we changed the table
			OnChanged();
		}

		internal void RemoveRowId(RowId rowid) {
			CheckValid();

			TableRow row = backedTable.GetRow(rowid);
			// If the rowid wasn't found,)
			if (row == null || !row.Exists)
				throw new ApplicationException("DELETE ROW on '" + Name + "' failed:" +
										  " row was updated or deleted in a concurrent transaction");

			// Otherwise, remove the value at the position

			// Update the journal with this record removal
			journal.AddEntry(JournalCommandCode.RowRemove, rowid.ToInt64());
			// Notfy the backed transaction that we changed the table
			OnChanged();

		}

		public TableRow GetRow(RowId rowid) {
			TableRow row;
			if (IsCommitable && insertRows.TryGetValue(rowid, out row))
				return row;

			return backedTable.GetRow(rowid);
		}

		public void Insert(TableRow row) {
			CheckMutable();

			if (!IsCommitable) {
				Mutable.Insert(row);
			} else {
				// This is a commitable table, so the cached rows are processed in a later moment
				insertRows.Add(row.Id, row);
				sessionRowCount++;
			}
		}

		public void Update(TableRow row) {
			if (!IsCommitable) {
				Mutable.Update(row);
			} else {
				// This is a commitable table, so the cached rows are processed in a later moment
				updateInsertRows.Add(row.Id, row);
			}
		}

		public void Delete(RowId rowid) {
			if (!IsCommitable) {
				Mutable.Delete(rowid);
			} else {
				updateRemoveRowList.Add(rowid);
				sessionRowCount--;
			}
		}

		public void Undo() {
			if (IsCommitable) {
				// Clear the lists
				insertRows.Clear();
				updateRemoveRowList.Clear();
				updateInsertRows.Clear();
			}
		}

		public void PrefetchValue(int columnOffset, RowId rowid) {
			backedTable.PrefetchValue(columnOffset, rowid);
		}

		public SqlObject GetValue(int columnOffset, RowId rowid) {
			CheckValid();

			TableRow row;
			if (commitable && insertRows.TryGetValue(rowid, out row))
				return row.GetValue(columnOffset);
			if (commitable && updateInsertRows.TryGetValue(rowid, out row))
				return row.GetValue(columnOffset);

			if (backedTable.RowExists(rowid))
				return backedTable.GetValue(columnOffset, rowid);

			throw new ApplicationException("Invalid rowid");
		}

		public bool RowExists(RowId rowid) {
			if (IsCommitable && insertRows.ContainsKey(rowid))
				return true;

			return backedTable.RowExists(rowid);
		}

		public void Commit() {
			if (!IsMutable)
				return;

			if (IsCommitable) {
				ICommitableTable commitableTable = (ICommitableTable) backedTable;

				commitableTable.BeginCommit();

				foreach (KeyValuePair<RowId, TableRow> pair in insertRows) {
					Mutable.Insert(pair.Value);
					journal.AddEntry(JournalCommandCode.RowAdd, pair.Key.ToInt64());
				}

				foreach (RowId rowid in updateRemoveRowList) {
					Mutable.Delete(rowid);
					journal.AddEntry(JournalCommandCode.RowRemove, rowid.ToInt64());
				}

				foreach (KeyValuePair<RowId, TableRow> pair in updateInsertRows) {
					Mutable.Update(pair.Value);
					journal.AddEntry(JournalCommandCode.RowUpdate, pair.Key.ToInt64());
					journal.AddEntry(JournalCommandCode.RowAdd, pair.Key.ToInt64());
				}

				commitableTable.EndCommit();

				// Notify the transaction that something changed
				OnChanged();
				// Clear the lists
				insertRows.Clear();
				updateRemoveRowList.Clear();
				updateInsertRows.Clear();
			} else {
				Mutable.Commit();
			}
		}

		public void CopyTo(SystemTransaction destination, long destTableId) {
			SystemTable destTable = destination.GetTable(destTableId);
			if (!commitable || !destTable.IsCommitable)
				throw new NotSupportedException();

			((ICommitableTable)backedTable).CopyTo(destTable.backedTable);
		}

		public void CopyTo(SystemTransaction destination) {
			CopyTo(transaction, id);
		}

		#region TableColumList

		private class SystemColumnCollection : IColumnCollection {
			private readonly SystemTable table;

			public SystemColumnCollection(SystemTable table) {
				this.table = table;
			}

			public bool Remove(TableColumn item) {
				int index = IndexOf(item);
				if (index == -1)
					return false;
				RemoveAt(index);
				return true;
			}

			public int Count {
				get { return table.backedTable.Columns.Count; }
			}

			public bool IsReadOnly {
				get { return false; }
			}

			public void Add(TableColumn item) {
				Add(item.Name, item.Type, item.NotNull);
			}

			public void Clear() {
				table.CheckValid();

				int count = Count;
				for (int i = count - 1; i >= 0; i--) {
					RemoveAt(i);
				}
			}

			public bool Contains(TableColumn item) {
				return IndexOf(item) != -1;
			}

			public void CopyTo(TableColumn[] array, int arrayIndex) {
				throw new NotImplementedException();
			}

			public TableColumn this[string columnName] {
				get {
					table.CheckValid();
					return table.backedTable.Columns[columnName];
				}
			}

			public TableColumn Add(string columnName, SqlType columnType, bool notNull) {
				table.CheckValid();

				TableColumn column;

				try {
					column = table.backedTable.Columns.Add(columnName, columnType, notNull);
					if (!column.Exists)
						throw new ApplicationException("The column '" + columnName + "' does not exists after creation.");

					table.journal.AddEntry(JournalCommandCode.ColumnAdd, column.Id);
					table.OnStructureChanged();
				} catch (Exception e) {
					throw new ApplicationException("Error while creating column '" + columnName + "' on table '" + table.Name + "': " + e.Message, e);
				}

				return column;
			}

			public bool Contains(string columnName) {
				table.CheckValid();
				return table.backedTable.Columns.Contains(columnName);
			}

			public int IndexOf(string columnName) {
				table.CheckValid();
				return table.backedTable.Columns.IndexOf(columnName);
			}

			public bool Remove(string columnName) {
				table.CheckValid();

				try {
					TableColumn column = table.backedTable.Columns[columnName];
					if (column == null)
						throw new ArgumentOutOfRangeException();
					if (!column.Exists)
						throw new ApplicationException("The column '" + column.Name + "' does not exists.");

					// TODO: Check that this column isn't referenced in any indexes. Fail if we are.

					table.backedTable.Columns.Remove(columnName);

					table.journal.AddEntry(JournalCommandCode.ColumnRemove, column.Id);
					table.OnStructureChanged();

					return true;
				} catch (Exception e) {
					throw new ApplicationException("Error while removing column '" + columnName + "' from table '" + table.Name + "': " + e.Message, e);
				}
			}

			public int IndexOf(TableColumn item) {
				table.CheckValid();
				return table.backedTable.Columns.IndexOf(item);
			}

			public void Insert(int index, TableColumn item) {
				throw new NotSupportedException();
			}

			public void RemoveAt(int index) {
				table.CheckValid();

				try {
					TableColumn column = table.backedTable.Columns[index];
					if (column == null)
						throw new ArgumentOutOfRangeException();
					if (!column.Exists)
						throw new ApplicationException("The column '" + column.Name + "' does not exists.");

					// TODO: Check that this column isn't referenced in any indexes. Fail if we are.

					table.backedTable.Columns.RemoveAt(index);

					table.journal.AddEntry(JournalCommandCode.ColumnRemove, column.Id);
					table.OnStructureChanged();
				} catch (Exception e) {
					throw new ApplicationException("Error while removing column from table '" + table.Name + "' at index '" + index + "': " + e.Message, e);
				}
			}

			public TableColumn this[int index] {
				get {
					table.CheckValid();
					return table.backedTable.Columns[index];
				}
				set {
					table.CheckValid();
					//TODO:
				}
			}

			public IEnumerator<TableColumn> GetEnumerator() {
				return table.backedTable.Columns.GetEnumerator();
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}
		}

		#endregion

		public void Dispose() {
			//TODO:
		}

		#region NRowTable

		private class NRowTable : TableBase {
			private readonly int rows;

			public NRowTable(TableName tableName, int rows) 
				: base(tableName) {
				this.rows = rows;
			}

			public override long RowCount {
				get { return rows; }
			}

			protected override void SetupColumns() {
			}

			public override SqlObject GetValue(int columnOffset, RowId rowid) {
				return SqlObject.Null;
			}
		}

		#endregion

		#region SystemRowCursor

		private class SystemRowCursor : IRowCursor {
			private readonly SystemTable systemTable;
			private readonly IRowCursor originalCursor;
			private readonly ICollection<RowId> addedRows;
			private readonly IEnumerator<RowId> addedRowsEnum;
			private RowId currentRowId;
			private long pos = -1;

			public SystemRowCursor(SystemTable systemTable, IRowCursor originalCursor, ICollection<RowId> addedRows) {
				this.systemTable = systemTable;
				this.originalCursor = originalCursor;
				this.addedRows = addedRows;
				addedRowsEnum = addedRows.GetEnumerator();
			}

			public void Dispose() {
				originalCursor.Dispose();
				addedRowsEnum.Dispose();
			}

			public bool MoveNext() {
				bool hasNext = originalCursor.MoveNext();
				while (hasNext) {
					++pos;
					currentRowId = originalCursor.Current;
					if (!systemTable.updateRemoveRowList.Contains(currentRowId))
						break;

					hasNext = originalCursor.MoveNext();
				}

				if (!hasNext) {
					hasNext = addedRowsEnum.MoveNext();
					while (hasNext) {
						++pos;
						currentRowId = addedRowsEnum.Current;
						if (!systemTable.updateRemoveRowList.Contains(currentRowId))
							break;

						hasNext = addedRowsEnum.MoveNext();
					}
				}

				return hasNext;
			}

			public void Reset() {
				originalCursor.Reset();
				addedRowsEnum.Reset();
				pos = -1;
			}

			public RowId Current {
				get { return currentRowId; }
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			public object Clone() {
				return new SystemRowCursor(systemTable, (IRowCursor) originalCursor.Clone(), new List<RowId>(addedRows));
			}

			public long Position {
				get { return pos; }
			}

			public long Count {
				get { return originalCursor.Count + addedRows.Count - systemTable.updateRemoveRowList.Count; }
			}

			public bool MoveBack() {
				throw new NotImplementedException();
			}

			public void MoveBeforeStart() {
				Reset();
			}

			public void MoveAfterEnd() {
				originalCursor.MoveAfterEnd();
				while (addedRowsEnum.MoveNext())
					continue;

				pos = Count;
			}

			public long MoveTo(long position) {
				if (position < originalCursor.Count) {
					originalCursor.MoveTo(position);
				}
				pos = position;
				return pos;
			}
		}

		#endregion
	}
}