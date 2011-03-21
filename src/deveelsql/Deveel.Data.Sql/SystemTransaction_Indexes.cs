using System;
using System.Collections.Generic;

using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	internal sealed partial class SystemTransaction {
		internal void AddIndex(long indexId, TableName onTable, string indexName, string type, IndexCollation collation) {
			IMutableTable indexTable = GetTable(SystemTableNames.Index);
			TableRow row = indexTable.NewRow();
			row.SetValue(0, indexId);
			row.SetValue(1, onTable.Schema);
			row.SetValue(2, onTable.Name);
			row.SetValue(3, indexName);
			row.SetValue(4, type);
			row.SetValue(5, collation.Function);
			row.SetValue(6, collation.Type.ToString());
			row.SetValue(7, user.Name);
			indexTable.Insert(row);
			indexTable.Commit();

			RowId insertRowId = row.Id;

			IMutableTable indexColumnTable = GetTable(SystemTableNames.ColumnSet);
			for (int i = 0; i < collation.Columns.Length; i++) {
				CollationColumn column = collation.Columns[i];

				row = indexColumnTable.NewRow();
				row.SetValue(0, indexId);
				row.SetValue(1, onTable.Schema);
				row.SetValue(2, onTable.Name);
				row.SetValue(3, indexName);
				row.SetValue(4, i);
				row.SetValue(5, column.ColumnName);
				row.SetValue(6, column.Ascending);
				indexColumnTable.Insert(row);
			}

			indexColumnTable.Commit();

			// If this isn't an index on the INDEXES or TABLES tables,););););
			if (!onTable.Equals(SystemTableNames.Tables) &&
				!onTable.Equals(SystemTableNames.Index)) {
				// Update the indexes
				IIndexSetDataSource[] idxs = GetTableIndexes(SystemTableNames.Index);
				foreach (IIndexSetDataSource i in idxs) {
					i.Insert(insertRowId);
				}
			}
		}

		internal void RemoveIndex(long index_id) {
			IIndexSetDataSource index = GetIndex(SystemTableNames.Index, "id_idx");
			SqlObject val = new SqlObject(index_id);
			// Query the index
			IRowCursor cursor = index.Select(SelectableRange.Is(val));
			// We assert this (we have already checked the index exists and this
			// iterator is valid).
			if (cursor.Count != 1)
				throw new ApplicationException();

			// The index table,
			IMutableTable indexTable = GetTable(SystemTableNames.Index);
			if (!cursor.MoveNext())
				throw new ApplicationException();

			RowId rowId = cursor.Current;
			indexTable.Delete(rowId);

			// Update indexes on index table
			IIndexSetDataSource[] idxs = GetTableIndexes(SystemTableNames.Index);
			foreach (IIndexSetDataSource idx in idxs) {
				idx.Remove(rowId);
			}
		}

		internal void RebuildIndex(TableName tableName, IIndexSetDataSource indexSet) {
			IMutableTable table = GetTable(tableName);
			indexSet.Clear();
			IRowCursor cursor = table.GetRowCursor();
			while (cursor.MoveNext()) {
				RowId rowid = cursor.Current;
				indexSet.Insert(rowid);
			}
		}

		internal void RebuildIndexes(TableName tableName) {
			if (tableName.Equals(SystemTableNames.Index) ||
				tableName.Equals(SystemTableNames.Tables)) {
				throw new ApplicationException("Cannot rebuild index on " + tableName);
			}

			// All indexes on the table,
			IIndexSetDataSource[] idxs = GetTableIndexes(tableName);
			// Rebuild each index
			foreach (IIndexSetDataSource idx in idxs) {
				RebuildIndex(tableName, idx);
			}
		}

		internal void RebuildSystemIndexes() {
			// Note that we can't use any functions that use indexes to perform this
			// operation.  We need to iterate over the tables to rebuild.
			// We iterate through our index table, any indexes that are on the
			// TABLES or INDEXES table we build
			ITable indexTable = GetTable(SystemTableNames.Index);
			IRowCursor i = indexTable.GetRowCursor();
			while (i.MoveNext()) {
				RowId rowid = i.Current;
				SqlObject id = indexTable.GetValue(0, rowid);
				SqlObject schema = indexTable.GetValue(1, rowid);
				SqlObject name = indexTable.GetValue(2, rowid);
				TableName tableName = TableName.Resolve(schema.ToString(), name.ToString());
				// If it's in the index or directory table,
				if (tableName.Equals(SystemTableNames.Index) ||
					tableName.Equals(SystemTableNames.Tables)) {
					// Fetch the index and rebuild it
					SystemIndexSetDataSource sysIndex = GetIndex(id);
					RebuildIndex(tableName, sysIndex);
				}
			}
		}

		internal IndexName GetIndexName(long indexId) {
			// Get the index on the index table
			SystemIndexSetDataSource index = GetIndex(SystemTableNames.Index, "id_idx");
			// If index is null, then we are in a state of setting up the
			// database, so we assume no indexes exist in this case.  A
			// hack for sure.
			if (index == null)
				return null;

			SqlObject val = indexId;

			// Query the index
			IRowCursor cursor = index.Select(SelectableRange.Is(val));
			// If more than 1 entry found, it's an internal formatting error
			long sz = cursor.Count;
			if (sz > 1 || sz < 0)
				throw new ApplicationException("Index table error");

			// If empty iterator, it doesn't exist
			if (cursor.Count == 0)
				return null;

			if (!cursor.MoveNext())
				throw new ApplicationException();

			// Otherwise it must be 1 so read the values

			RowId row_id = cursor.Current;
			SystemTable indexTable = GetTable(SystemTableNames.Index);
			SqlObject schema = indexTable.GetValue(1, row_id);
			SqlObject name = indexTable.GetValue(2, row_id);
			SqlObject idxName = indexTable.GetValue(3, row_id);
			TableName tname = new TableName(schema.ToString(), name.ToString());
			string idx_name = idxName.ToString();
			return new IndexName(tname, idx_name);

		}

		internal void CopyIndexFrom(SystemTransaction transaction, long indexId) {
			// Get the index name
			IndexName indexName = GetIndexName(indexId);
			// We get the index as a SystemTableIndex
			SystemIndexSetDataSource indexSource = transaction.GetIndex(indexId);
			// Check if an object with this name exists in this transaction,
			if (IndexExists(indexName.TableName, indexName.Name)) {
				// It does exist, so generate an error
				throw new ApplicationException("Index copy failed, index '" + indexName + "' already exists");
			}

			// Copy the index to this transaction
			indexSource.CopyEntirelyTo(this);
			// Update the index id,
			AddIndex(indexId, indexName.TableName, indexName.Name, user.Name, indexSource.Collation);
		}

		public SystemIndexSetDataSource CreateIndex(TableName onTable, string indexName, string indexType, IndexCollation collation) {
			CheckNotDisposed();
			// Check the table name given is qualified
			CheckTableNameQualified(onTable);

			// We can't create an index that already exists
			if (IndexExists(onTable, indexName)) {
				throw new ApplicationException("Index '" + indexName + "' already exists");
			}

			string[] columnNames = new string[collation.Columns.Length];
			bool[] columnOrders = new bool[columnNames.Length];
			for (int i = 0; i < columnNames.Length; i++) {
				columnNames[i] = collation.Columns[i].ColumnName;
				columnOrders[i] = collation.Columns[i].Ascending;
			}

			ITableIndex index = state.CreateIndex(new IndexName(onTable, indexName), indexType, columnNames, columnOrders);
			if (index == null)
				throw new ApplicationException("Unable to create index.");

			long indexId = state.CreateUniqueId(SystemTableNames.Index);

			// Create the index object
			SystemIndexSetDataSource indexSet = new SystemIndexSetDataSource(this, index);

			// Add this index item
			AddIndex(indexId, onTable, indexName, indexType, collation);

			// Log the change in the journal
			journal.AddEntry(JournalCommandCode.IndexAdd, indexId);
			OnChanged();

			// Put it in the local cache
			indexMap[indexId] = indexSet;

			// Return the index
			return indexSet;
		}

		public bool IndexExists(TableName onTable, string indexName) {
			CheckNotDisposed();

			if (indexName.Equals("index_composite_idx"))
				return true;

			// Get the index on the index table
			SystemIndexSetDataSource index = GetIndex(SystemTableNames.Index, "index_composite_idx");

			// If index is null, then we are in a state of setting up the
			// database, so we assume no indexes exist in this case.  A
			// hack for sure.
			if (index == null)
				return false;

			SqlObject val = SqlObject.CompositeString(new string[] { onTable.Schema, onTable.Name, indexName });

			// Query the index
			IRowCursor cursor = index.Select(SelectableRange.Is(val));

			// If more than 1 entry found, it's an internal formatting error
			long sz = cursor.Count;
			if (sz > 1 || sz < 0)
				throw new ApplicationException("Index table error");

			// If empty iterator, it doesn't exist
			return sz > 0;
		}

		public SystemIndexSetDataSource[] GetTableIndexes(TableName tableName) {
			SystemTable table = InternalGetTable(SystemTableNames.Index);
			if (table == null)
				return new SystemIndexSetDataSource[0];

			IRowCursor cursor = GetNames(table, tableName);

			if (cursor.Count == 0)
				return new SystemIndexSetDataSource[0];

			SystemIndexSetDataSource[] indexes = new SystemIndexSetDataSource[cursor.Count];
			int i = -1;
			while (cursor.MoveNext()) {
				SqlObject indexId = table.GetValue(0, cursor.Current);
				indexes[i++] = GetIndex(indexId);
			}

			return indexes;
		}

		internal SystemIndexSetDataSource GetIndex(long indexId) {
			CheckNotDisposed();

			// Get from the cache
			SystemIndexSetDataSource sysIndex;
			if (!indexMap.TryGetValue(indexId, out sysIndex)) {
				IndexName name = GetIndexName(indexId);
				if (name == null)
					return null;

				sysIndex = GetIndex(name.TableName, name.Name);
				if (sysIndex == null)
					return null;

				indexMap[indexId] = sysIndex;
			}

			return sysIndex;
		}

		public SystemIndexSetDataSource GetIndex(TableName onTable, string indexName) {
			CheckNotDisposed();
			// Check the table name given is qualified
			CheckTableNameQualified(onTable);

			// Fetch the index id
			long iid = GetIndexId(onTable, indexName);
			// Return null if the id is -1 (not found)
			if (iid == -1)
				return null;

			return GetIndex(iid);
		}

		internal long GetIndexId(TableName onTable, string indexName) {
			// Handle fetching the index definition entry specially,
			if (onTable.Equals(SystemTableNames.Index) &&
				indexName.Equals("index_composite_idx")) {
				// The index table,
				ITable indexTable = GetTable(SystemTableNames.Index);
				// Iterate until we find it (it should be found after only a few
				// iterations).
				IRowCursor cursor = indexTable.GetRowCursor();
				bool found = false;
				SqlObject indexId = null;
				while (cursor.MoveNext()) {
					RowId rowId = cursor.Current;
					indexId = indexTable.GetValue(0, rowId);
					SqlObject schem = indexTable.GetValue(1, rowId);
					SqlObject table = indexTable.GetValue(2, rowId);
					SqlObject iname = indexTable.GetValue(3, rowId);

					if (schem.ToString().Equals(onTable.Schema) &&
						table.ToString().Equals(onTable.Name) &&
						iname.ToString().Equals(indexName)) {
						found = true;
						break;
					}
				}
				// If found
				return found ? indexId.ToInt64() : -1;
			} else {
				// No, so fetch it
				IIndexSetDataSource index = GetIndex(SystemTableNames.Index, "index_composite_idx");
				SqlObject val = SqlObject.CompositeString(new string[] { onTable.Schema, onTable.Name, indexName });

				// Query the index
				IRowCursor cursor = index.Select(SelectableRange.Is(val));

				// Should only be 1 value or none,
				if (cursor.Count > 1)
					throw new ApplicationException("System index definition table invalid");

				// If no data in iterator, return null
				if (cursor.Count == 0)
					return -1;

				// The index table,
				SystemTable indexTable = GetTable(SystemTableNames.Index);
				if (!cursor.MoveNext())
					throw new ApplicationException();

				return indexTable.GetValue(0, cursor.Current);
			}
		}

		public void DropIndex(TableName onTable, string indexName) {
			CheckNotDisposed();
			// Check the table name given is qualified
			CheckTableNameQualified(onTable);

			// Get the index id
			long indexId = GetIndexId(onTable, indexName);

			// Found?
			if (indexId == -1)
				throw new ApplicationException("Index '" + indexName + "' not found on table '" + onTable + "'");

			DropIndex(indexId);
		}

		internal void DropIndex(long indexId) {
			CheckNotDisposed();

			// Get the IndexNameKey for this index id
			IndexName name_key = GetIndexName(indexId);
			if (name_key == null)
				throw new ApplicationException("Index id '" + indexId + "' not found.");

			// Check it's a protected index
			if (name_key.TableName.Equals(SystemTableNames.Index) ||
				name_key.TableName.Equals(SystemTableNames.Tables)) {
				throw new ApplicationException("Protected index " + name_key);
			}

			// Get the index set
			SystemIndexSetDataSource index_set = GetIndex(indexId);

			// Remove the index item from the index directory
			RemoveIndex(indexId);

			// Drop the index itself
			state.DeleteIndex(name_key);

			// Log the change in the journal
			journal.AddEntry(JournalCommandCode.IndexDelete, indexId);
			OnChanged();

			// Remove it from the local cache
			indexMap.Remove(indexId);
		}

		internal SystemIndexSetDataSource FindIndexOn(TableName tableName, IList<string> columns) {
			CheckNotDisposed();

			// The list of all indexes on the table
			SystemIndexSetDataSource[] table_indexes = GetTableIndexes(tableName);
			int sz = columns.Count;
			foreach (SystemIndexSetDataSource idx in table_indexes) {
				IndexCollation collation = idx.Collation;
				if (collation.Function != null) {
					int part_count = collation.PartCount;
					if (part_count == sz) {
						bool all_match = true;
						for (int i = 0; i < part_count; ++i) {
							CollationColumn column = collation.Columns[i];
							if (!column.Ascending ||
							    !column.ColumnName.Equals(columns[i])) {
								// If the column doesn't match, look at the next index,
								all_match = false;
								break;
							}
						}

						// If all_match (all parts are ascending and all column names match
						// the given sequence), return the index
						if (all_match)
							return idx;
					}
				}
			}
			// None of the indexes matches, so return null
			return null;
		}
	}
}