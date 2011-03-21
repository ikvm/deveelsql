using System;
using System.Collections.Generic;

using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	internal sealed partial class SystemTransaction {
		private SystemTable InternalGetTable(TableName tableName) {
			// Is it in the cache?
			SystemTable table;
			if (!tableNameMap.TryGetValue(tableName, out table)) {
				// Directory table is special case,
				if (tableName.Equals(SystemTableNames.Tables)) {
					ITable t = state.GetTable(tableName);
					if (t == null)
						return null;
					
					table = new SystemTable(this, t, -1);
				}
					// Index tables are special cases
				else if (tableName.Equals(SystemTableNames.Index)) {
					ITable t = state.GetTable(tableName);
					if (t == null)
						return null;

					table = new SystemTable(this, t, -1);
				}
					// Not a special case table,
				else {
					// If the table is in the table id map
					long tableId;
					if (tableIdMap.TryGetValue(tableName, out tableId)) {
						// Simply fetch if it is,
						table = GetTable(tableId);
					} else {
						// Otherwise we need to resolve the table name to an id
						// Discover the id for this table name from a query on the directory
						// table.
						// This is probably going to be a very heavy operation for what is a
						// simple TableName to id lookup, and I think ultimately this
						// operation will be backed by a cache.
						
						// first check if we have the system TABLES table, that means
						// we have a full setup
						if (!TableExists(SystemTableNames.Tables)) {
							// we have not a full setup: try to get the table
							// directly from the state
							ITable t = state.GetTable(tableName);
							if (t == null)
								return null;

							tableId = state.CreateUniqueId(SystemTableNames.Tables);
							table = new SystemTable(this, t, tableId);
							tableIdMap[tableName] = tableId;
						} else {
							// Query the composite index
							SystemTable tables = GetTable(SystemTableNames.Tables);
							// Find the RowIndex that represents the set of all schema/name
							// items in the table
							IRowCursor i = GetNames(tables, tableName);
							// Fail conditions
							if (i.Count == 0)
								return null;

							if (i.Count > 1)
								throw new ApplicationException("Multiple '" + tableName + "' tables.");

							if (!i.MoveNext())
								throw new ArgumentException();

							// Read the result
							RowId rowid = i.Current;
							tableId = tables.GetValue(0, rowid);
							string tableType = tables.GetValue(3, rowid);

							// Fetch the table info
							if (tableType.Equals("TABLE")) {
								// Handle table_id overflow gracefully
								if (tableId > Int32.MaxValue)
									throw new ApplicationException("table_id overflow (" + tableId + ")");

								// Put the table id in the map
								tableIdMap[tableName] = tableId;
								// Fetch the table,
								table = GetTable(tableId);
							}

								// Sequences
							else if (tableType.Equals("SEQUENCE")) {
								// NOTE, this doesn't get put on the table cache!
								//TODO:
							}

								// Table primitives
							else if (tableType.StartsWith("PRIMITIVE:")) {
								// The name of the primitive
								string tableOb = tableType.Substring(10);
								if (tableOb.Equals("EmptyTable"))
									return SystemTable.Empty;
								if (tableOb.Equals("OneRowTable"))
									return SystemTable.OneRow;
							}

								// Dynamically created tables created via reflection
							else if (tableType.StartsWith("DYN:")) {
								// A dynamic table type
								//TODO:
							} else {
								throw new ApplicationException("Unknown table type: " + tableType);
							}
						}
					}
				}

				tableNameMap[tableName] = table;
			}
			return table;
		}

		internal int GetTableId(TableName tableName) {
			CheckNotDisposed();
			// Check the table name given is qualified
			CheckTableNameQualified(tableName);

			// Query the composite index
			ITable tables = GetTable(SystemTableNames.Tables);
			// Find the RowIndex that represents the set of all schema/name
			// items in the table
			IRowCursor rowCursor = GetNames(tables, tableName);
			// Fail conditions
			if (rowCursor.Count == 0)
				throw new ApplicationException("Table '" + tableName + "' not found.");
			if (rowCursor.Count > 1)
				throw new ApplicationException("Multiple tables '" + tableName + "'.");

			// Read the result
			if (!rowCursor.MoveNext())
				throw new ApplicationException();

			return tables.GetValue(0, rowCursor.Current).ToNumber().ToInt32();
		}

		internal long GetTableVersion(long tableId) {
			return GetTable(tableId).Version;
		}

		internal void IncrementTableVersion(long tableId) {
			GetTable(tableId).IncrementVersion();
		}

		internal void CopyTableFrom(SystemTransaction transaction, long tableId) {
			// We get the table as a TSTableDataSource
			SystemTable tableSource = transaction.GetTable(tableId);
			// Check if an object with this name exists in this transaction,
			TableName tableName = tableSource.Name;
			if (TableExists(tableName))
				// It does exist, so generate an error
				throw new ApplicationException("Table copy failed, table '" + tableName + "' already exists");

			// Copy the table to this transaction
			tableSource.CopyTo(this);

			// Update the directory id,
			AddObject(tableId, tableName, user.Name);
		}

		public SystemTable CreateTable(TableName tableName) {
			CheckNotDisposed();
			// Check the table name given is qualified
			CheckTableNameQualified(tableName);

			// Does an object with this name already exist in the directory?
			ITable tables = GetTable(SystemTableNames.Tables);
			IRowCursor ind = GetNames(tables, tableName);
			if (ind.Count > 0)
				throw new ApplicationException("Table '" + tableName + "' already exists.");

			ITable table = state.CreateTable(tableName);
			if (table == null)
				throw new ApplicationException("The table '" + tableName + "' was not created.");

			long tableId = state.CreateUniqueId(SystemTableNames.Tables);

			// Construct and create the table
			SystemTable sysTable = new SystemTable(this, table, tableId);

			// Add this table to the tables.
			AddObject(tableId, tableName, "TABLE");

			// Log the change in the journal
			journal.AddEntry(JournalCommandCode.TableCreate, tableId);
			OnChanged();

			// Put it in the cache
			tableNameMap[tableName] = sysTable;

			// And return it
			return sysTable;
		}

		public bool TableExists(TableName tableName) {
			CheckNotDisposed();
			CheckTableNameQualified(tableName);
			return InternalGetTable(tableName) != null;
		}

		public bool TableExists(long tableId) {
			return GetTable(tableId) != null;
		}

		public SystemTable GetTable(TableName tableName) {
			CheckNotDisposed();
			CheckTableNameQualified(tableName);

			SystemTable table = InternalGetTable(tableName);
			if (table == null)
				throw new ArgumentException("Table '" + tableName + "' not found.");

			return table;
		}

		internal void DropTable(long tableId) {
			CheckNotDisposed();

			// Get the object name of this id
			TableName table_name = GetObjectName(tableId);

			// Not allowed to drop the directory or index table,
			if (table_name.Equals(SystemTableNames.Tables) ||
				table_name.Equals(SystemTableNames.Index))
				throw new ApplicationException("Unable to drop " + table_name + " table.");

			// Remove the directory item for this table
			RemoveObject(tableId);

			// Drop the table
			state.DeleteTable(table_name);

			// Log the change in the journal
			journal.AddEntry(JournalCommandCode.TableDrop, tableId);
			OnChanged();

			// And remove it from the cache
			tableNameMap.Remove(table_name);
			tableIdMap.Remove(table_name);
			tableMap.Remove(tableId);
		}

		public void DropTable(TableName tableName) {
			CheckNotDisposed();
			// Check the table name given is qualified
			CheckTableNameQualified(tableName);

			// Get the id of the table
			long table_id = GetTableId(tableName);

			// Drop the table
			DropTable(table_id);
		}

		public IList<ForeignKey> QueryForeignReferencesFrom(TableName tableName) {
			CheckNotDisposed();
			// Check the table name given is qualified
			CheckTableNameQualified(tableName);

			ITable foreignTable = GetTable(SystemTableNames.ConstraintsForeign);

			// The index on the object_id column on the foreign table
			IIndexSetDataSource index = GetIndex(SystemTableNames.ConstraintsForeign, "composite_name_idx");
			SqlObject val = SqlObject.MakeComposite(new SqlObject[] { tableName.Schema, tableName.Name });
			// Query the index
			IRowCursor cursor = index.Select(SelectableRange.Is(val));

			//TODO: this is a total mess... must be refactored ...

			// Turn it into an arraylist
			List<ForeignKey> list = new List<ForeignKey>((int)cursor.Count);
			while (cursor.MoveNext()) {
				RowId rowid = cursor.Current;
				SqlObject srcColSetId = foreignTable.GetValue(2, rowid);
				SqlObject dstSchema = foreignTable.GetValue(3, rowid);
				SqlObject dstName = foreignTable.GetValue(4, rowid);
				SqlObject dstColSetId = foreignTable.GetValue(5, rowid);
				SqlObject updateAction = foreignTable.GetValue(6, rowid);
				SqlObject deleteAction = foreignTable.GetValue(7, rowid);
				SqlObject deferred = foreignTable.GetValue(8, rowid);
				SqlObject deferrable = foreignTable.GetValue(9, rowid);

				// Create the destination table name object
				TableName dstTableName = new TableName(dstSchema.ToString(), dstName.ToString());

				// Turn it into a ForeignKeyReference object
				string[] sourceColumns = QueryColumns(srcColSetId.ToNumber().ToInt64());
				string[] destColumns = QueryColumns(dstColSetId.ToNumber().ToInt64());
				
				// Add to the list
				list.Add(new ForeignKey(tableName, sourceColumns, dstTableName, destColumns, updateAction.ToString(),
				                        deleteAction.ToString(), deferrable.ToBoolean(),
				                        deferred.ToBoolean()));
			}

			// Return the list
			return list;
		}

		private string[] QueryColumns(long columnSetId) {
			CheckNotDisposed();

			// The table,
			ITable columnSetTable = GetTable(SystemTableNames.ColumnSet);
			// Get the index on the the id field
			IIndexSetDataSource index = GetIndex(SystemTableNames.ColumnSet, "id_idx");
			// Query the entries that match this id
			IRowCursor cursor = index.Select(SelectableRange.Is(new SqlObject(columnSetId)));
			List<string> columns = new List<string>((int)cursor.Count);
			List<int> seqs = new List<int>((int)cursor.Count);
			while (cursor.MoveNext()) {
				RowId rowid = cursor.Current;
				// Read the info
				SqlObject seqNo = columnSetTable.GetValue(1, rowid);
				SqlObject columnName = columnSetTable.GetValue(2, rowid);

				int jseq_no = seqNo.ToNumber().ToInt32();
				string jcolumn_name = columnName.ToString();

				// We use a binary search and insert sort.  This should be fine for
				// efficiency since these lists are likely to be small.
				// Find the position to insert
				int pos = seqs.BinarySearch(jseq_no);
				if (pos >= 0) {
					// Oops, duplicate sequence numbers for a column set id means there's
					// been some sort of corruption.
					throw new ApplicationException("Duplicate seq_no in system column set");
				}

				int addPos = -(pos + 1);
				// Insert in sorted position
				seqs.Insert(addPos, jseq_no);
				columns.Insert(addPos, jcolumn_name);

			}
			// Return the list
			return columns.ToArray();
		}

		public SystemTable GetTable(long tableId) {
			// Is it in the cache?
			SystemTable sysTable;
			if (!tableMap.TryGetValue(tableId, out sysTable))
				return null;

			// Return the table
			return sysTable;
		}
	}
}