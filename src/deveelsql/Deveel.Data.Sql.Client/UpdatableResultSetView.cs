using System;

using Deveel.Data.Base;
using Deveel.Data.Sql;

namespace Deveel.Data.Sql.Client {
	class UpdatableResultSetView {
		private readonly SystemTransaction transaction;
		private readonly IMutableTable backedTable;
		private readonly Expression[] project;
		private readonly IRowCursor originalSelect;
		private IIndex<RowId> select;
		private IRowCursor currentSelect;
		private TableRow currentRow;
		private TableRow updatedRow;
		private long oldRowIndex;
		private char updateType = ' ';

		public UpdatableResultSetView(SystemTransaction transaction, IMutableTable backedTable, Expression[] project, IRowCursor select) {
			this.transaction = transaction;
			this.backedTable = backedTable;
			this.project = project;
			originalSelect = select;
			currentSelect = select;
			this.select = null;
		}

		private TableName BackedTableName {
			get { return backedTable is EmbeddedSessionContext.QueryContainerTable ? null : backedTable.Name; }
		}

		public IIndex<RowId> GetCurrentIndex() {
			// If there's no index defined,
			if (select == null) {
				// We have no idea how large the index will eventually grow to
				IIndex<RowId> newIndex = transaction.CreateTemporaryIndex<RowId>(Int64.MaxValue);
				// NOTE: This would be a really great opportunity to use the ghost
				//   copy on the source index
				// Populate this index
				originalSelect.MoveBeforeStart();
				while (originalSelect.MoveNext()) {
					// Read the value and put it in the temporary index
					RowId rowId = originalSelect.Current;
					if (rowId == null)
						throw new ApplicationException("Cannot determine the ROWID");

					newIndex.Add(rowId);
				}

				// Set the index
				select = newIndex;
			}
			return select;
		}

		public IRowCursor GetCurrentRowCursor() {
			if (currentSelect == null)
				currentSelect = new DefaultRowCursor(GetCurrentIndex().GetCursor());
			return currentSelect;
		}

		#region RowIdResolver

		private class RowIdResolver : IRowIdResolver {
			private readonly UpdatableResultSetView resultView;

			public RowIdResolver(UpdatableResultSetView resultView) {
				this.resultView = resultView;
			}

			public RowId ResolveRowId(long value) {
				return resultView.GetRowId(value);
			}
		}

		#endregion

		public RowId GetRowId(long rowNum) {
			// If we request the rowid of the currently updating record, we refer it
			// through to the rowid of the mutating copy.
			if (updateType == 'U' && rowNum == oldRowIndex)
				return currentRow.Id;
			if (updateType == 'I' && rowNum == oldRowIndex)
				return currentRow.Id;

			IRowCursor cursor = GetCurrentRowCursor();
			cursor.MoveTo(rowNum - 1);
			if (!cursor.MoveNext())
				throw new ArgumentException();

			return cursor.Current;

		}

		public long InsertRow() {
			if (currentRow != null)
				throw new InvalidOperationException("Already updating a row");

			TableRow insertedRow = backedTable.NewRow();
			// Set up the inserted row to its default values
			TableName backedTname = BackedTableName;
			if (backedTname != null)
				SqlInterpreter.SetInsertRowToDefault(transaction, backedTname, backedTable, insertedRow.Id);

			updateType = 'I';
			oldRowIndex = GetCurrentRowCursor().Count;
			currentRow = insertedRow;
			return oldRowIndex;
		}

		public void UpdateRow(long rowIndex) {
			if (currentRow != null)
				throw new InvalidOperationException("Already updating a row");

			RowId toUpdateRowid = GetRowId(rowIndex);
			TableRow toUpdateRow = backedTable.GetRow(toUpdateRowid);
			updateType = 'U';
			oldRowIndex = rowIndex;
			updatedRow = toUpdateRow;
			currentRow = toUpdateRow;
		}

		public void RemoveRow(long rowIndex) {
			if (currentRow != null) {
				throw new InvalidOperationException("Already updating a row");
			}

			RowId toRemoveRowid = GetRowId(rowIndex);
			TableRow toRemoveRow = backedTable.GetRow(toRemoveRowid);
			// Note the remove happens immediately, so we need to make a copy of the
			// index now so the 'completeOperation' will work correctly.
			// This is a little bit messy.  Perhaps we should change the contract of
			// 'removeRow' in MutableTableDataSource so the actual remove happens
			// when we call completeOperation?
			GetCurrentIndex();  // NOTE, we call this method for its side effect
			// Remove the row
			backedTable.Delete(toRemoveRowid);
			updateType = 'R';
			oldRowIndex = rowIndex;
			currentRow = toRemoveRow;
		}

		public void SetValue(int column, RowId rowid, object value) {
			if (updateType == 'R' || updateType == ' ')
				throw new InvalidOperationException("Incorrect state to update");

			if (!rowid.Equals(currentRow.Id))
				throw new InvalidOperationException("rowid does not reference a mutable row");

			int ncol = column;
			if (project != null) {
				// Map the given 'col' into a column in the native table
				// Get the projection op
				Expression col_projection = project[column];
				// Turn it into a var
				Variable var = QueryProcessor.GetAsVariableRef(col_projection);
				// If it's not a var,
				if (var == null)
					throw new InvalidOperationException("Column is not updatable.");

				// Look up the column in the source table
				ncol = backedTable.Columns.IndexOf(var.Name);
				// If not found
				if (ncol == -1)
					throw new InvalidOperationException("Column is not updatable.");
			}

			// Set the contents of the native table!
			currentRow.SetValue(ncol, new SqlObject(value));
		}

		public void Finish(bool commit) {
			if (updateType == ' ')
				throw new InvalidOperationException("No update operations");

			try {
				// We get the index before we complete the operation.  This ensures
				// the original iterator will not be corrupted by the complete
				// operation.

				// Get the index (this may need to copy the index from the view).
				IIndex<RowId> index = null;
				if (commit) {
					index = GetCurrentIndex();
				}

				// If we are to do the operation,
				if (commit) {
					// Get the native table name being updated,
					TableName backedTname = BackedTableName;

					// Perform the operation via the SQLIterpreter.  The interpreter
					// checks for immediate referential constraint violations and
					// updates any indexes on the table.
					// Note that these operations may change the state of
					// 'original_iterator'

					if (updateType == 'U') {
						// Update indexes, check integrity, etc
						// This may generate an exception and fail the operation.
						if (backedTname != null) {
							SqlInterpreter.CompleteRowUpdate(transaction, backedTable, backedTname, updatedRow.Id, currentRow.Id);
						}
						// Remove the value at the old position and insert a new value
						index.RemoveAt(oldRowIndex);
						index.Insert(currentRow.Id, oldRowIndex);
						backedTable.Update(currentRow);
					} else if (updateType == 'I') {
						// Update indexes, check integrity, etc
						// This may generate an exception and fail the operation.
						if (backedTname != null) {
							SqlInterpreter.CompleteRowInsert(transaction, backedTable, backedTname, currentRow.Id);
						}
						// Insert the row identifier on the end of the index
						index.Add(currentRow.Id);
						backedTable.Insert(currentRow);
					} else if (updateType == 'R') {
						// Update indexes, check integrity, etc
						// This may generate an exception and fail the operation.
						if (backedTname != null) {
							SqlInterpreter.CompleteRowRemove(transaction, backedTable, backedTname, currentRow.Id);
						}
						// Remove the entry from the index
						index.RemoveAt(oldRowIndex);
					} else {
						// This should never be able to happen
						throw new ApplicationException("Unexpected update type: " + updateType);
					}
					// Invalidate the current select iterator.
					currentSelect = null;
				} else {
					// Fail the operation
					backedTable.Undo();
				}
			} finally {
				// Ensure we invalidate all this state information
				updateType = ' ';
				oldRowIndex = -1;
				currentRow = null;
			}
		}
	}
}