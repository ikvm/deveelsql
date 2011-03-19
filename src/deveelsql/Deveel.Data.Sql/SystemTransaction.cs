using System;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Base;
using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	internal sealed partial class SystemTransaction {
		private readonly DatabaseSession session;
		private readonly SystemFunctionManager functionManager;
		private readonly ITransactionState state;
		private readonly QueryProcessor queryProcessor;
		private readonly User user;
		private readonly long commitVersion;
		private bool disposed;

		private string currentSchema = "APP";

		private readonly Dictionary<TableName, SystemTable> tableNameMap;
		private readonly Dictionary<long, SystemTable> tableMap;
		private readonly Dictionary<TableName, long> tableIdMap;

		private readonly Dictionary<long, SystemIndexSetDataSource> indexMap;

		private Dictionary<Variable, ColumnStatistics> columnStats;
		private FactStatistics factStats;

		private readonly TransactionJournal journal;
		private bool changed;
		private readonly List<long> alteredTables;

		public SystemTransaction(DatabaseSession session, ITransactionState state, long commitVersion, User user) {
			this.session = session;
			this.state = state;
			this.user = user;
			this.commitVersion = commitVersion;
			user.StartTransaction(this);
			functionManager = new SystemFunctionManager();
			queryProcessor = new QueryProcessor(this);

			tableMap = new Dictionary<long, SystemTable>();
			tableIdMap = new Dictionary<TableName, long>();
			tableNameMap = new Dictionary<TableName, SystemTable>();

			indexMap = new Dictionary<long, SystemIndexSetDataSource>();

			journal = new TransactionJournal();
			alteredTables = new List<long>();
		}

		internal ITransactionState State {
			get { return state; }
		}

		public User User {
			get { return user; }
		}

		public QueryProcessor QueryProcessor {
			get { return queryProcessor; }
		}

		public void Dispose() {
			if (!disposed) {
				session.DisposeTransaction(this);
				user.EndTransaction();
				GC.SuppressFinalize(this);
			}
		}

		public string CurrentSchema {
			get {
				CheckNotDisposed();
				return currentSchema;
			}
		}

		public long CommitVersion {
			get { return commitVersion; }
		}

		internal SystemFunctionManager FunctionManager {
			get { return functionManager; }
		}

		public FactStatistics FactStatistics {
			get {
				if (factStats == null)
					factStats = new FactStatistics(this);

				// TODO: Implement a more complex persistant cache for fact statistics.
				return factStats;
			}
		}

		public bool HasChanges {
			get { return changed; }
		}

		internal List<long> AlteredTables {
			get { return alteredTables; }
		}

		internal TransactionJournal Journal {
			get { return journal; }
		}

		internal void MarkAsDisposed() {
			disposed = true;
		}

		private void OnChanged() {
			changed = true;
		}

		private void CheckNotDisposed() {
			if (disposed) {
				throw new ObjectDisposedException("Transaction");
			}
		}

		private static void CheckTableNameQualified(TableName tableName) {
			if (tableName == null ||
				tableName.Schema == null ||
				tableName.Name == null) {
				throw new ArgumentException("Table '" + tableName + "' is not qualified");
			}
		}

		internal IRowCursor GetNames(ITable table, TableName tableName) {
			// The name of the table being searched
			TableName searchTname = table.Name;
			// Get the index
			IIndexSetDataSource indexSet = GetIndex(searchTname, "composite_name_idx");
			// Make TObject 'val' which is the composite value we are searching
			// for.
			SqlObject val = SqlObject.CompositeString(new String[] { tableName.Schema, tableName.Name });
			// Query the index and return
			return indexSet.Select(SelectableRange.Is(val));
		}

		internal void OnTableChanged(long tableId) {
			alteredTables.Add(tableId);
			OnChanged();
		}

		internal void OnTableStructureChanged(long tableId) {
			journal.AddEntry(new JournalEntry(JournalCommandCode.TableAlter, tableId));
		}

		internal void AddObject(TableName tableName, string type) {
			SystemTable tables = GetTable(SystemTableNames.Tables);
			TableRow row = tables.NewRow();
			row.SetValue(1,tableName.Schema);
			row.SetValue(2, tableName.Name);
			row.SetValue(3, type);
			row.SetValue(4, user.Name);
			tables.Insert(row);
			tables.Commit();

			// If this isn't an index on the INDEX or DIRECTORY tables,
			if (!tableName.Equals(SystemTableNames.Tables) &&
			    !tableName.Equals(SystemTableNames.Index)) {
				// Update the indexes
				IIndexSetDataSource[] idxs = GetTableIndexes(SystemTableNames.Tables);
				foreach (IIndexSetDataSource i in idxs) {
					i.Insert(row.Id);
				}
			}
		}

		internal void RemoveObject(long objectId) {
			// Look up the table in the directory table,
			SystemTable tables = GetTable(SystemTableNames.Tables);
			IIndexSetDataSource index = GetIndex(SystemTableNames.Tables, "id_idx");

			// The name of the string
			SqlObject tid_ob = new SqlObject(objectId);
			IRowCursor i = index.Select(SelectableRange.Is(tid_ob));

			if (i.Count == 0)
				throw new ApplicationException("Object id '" + objectId + "' not found");
			if (i.Count > 1)
				throw new ApplicationException("Multiple of the same object id '" + objectId + "' found in directory");

			// Fetch the rowid of the entry,
			if (!i.MoveNext())
				throw new ApplicationException();

			RowId rowId = i.Current;
			// Make it into a TableName object
			SqlObject schema = tables.GetValue(1, rowId);
			SqlObject name = tables.GetValue(2, rowId);
			TableName tableName = new TableName(schema.ToString(), name.ToString());

			// We now have the row_id in the directory table of the record to remove
			// and the table_id of the given table name

			// Remove it from the directory table
			tables.Delete(rowId);
			// Complete the remove operation
			tables.Commit();
			// Update the indexes
			IIndexSetDataSource[] idxs = GetTableIndexes(SystemTableNames.Tables);
			foreach (IIndexSetDataSource idx in idxs) {
				idx.Remove(rowId);
			}

			// This is really just some security to make sure we don't hold on to the
			// cached object.
			tableNameMap.Remove(tableName);
			tableIdMap.Remove(tableName);
			tableMap.Remove(objectId);
		}

		public TableName GetObjectName(long objectId) {
			CheckNotDisposed();
			// Query the composite index
			ITable tables = GetTable(SystemTableNames.Tables);
			SystemIndexSetDataSource index = GetIndex(SystemTableNames.Tables, "id_idx");
			SqlObject val = objectId;

			// Query the index
			IRowCursor iterator = index.Select(SelectableRange.Is(val));
			// Assert there's no more than 1
			if (iterator.Count > 1)
				throw new ApplicationException("Duplicate ids in the system directory table");

			if (iterator.Count == 0)
				return null;

			if (!iterator.MoveNext())
				throw new ApplicationException();

			// Fetch the object name data
			RowId rowid = iterator.Current;
			SqlObject schema = tables.GetValue(1, rowid);
			SqlObject name = tables.GetValue(2, rowid);

			// Return it.
			return new TableName(schema.ToString(), name.ToString());
		}

		public void ChangeSchema(string newSchema) {
			CheckNotDisposed();

			//TODO: check the schema exists...
			currentSchema = newSchema;
		}

		public string CreateTempLargeObject() {
			throw new NotImplementedException();
		}

		public IBlobDataSource GetTempLargeObject(SqlType type, string key) {
			throw new NotImplementedException();
		}

		private IBinaryIndexResolver<T> GetResolver<T>() {
			Type type = typeof (T);
			int itemLength;
			if (type == typeof(int))
				itemLength = 4;
			else if (type == typeof(long))
				itemLength = 8;
			else if (type == typeof(RowId))
				return (IBinaryIndexResolver<T>) new RowIdIndex.Resolver(state.RowIdLength);
			else {
				throw new ArgumentException();
			}

			return new IntegerBinaryIndexResolver<T>(itemLength);
		}

		private class IntegerBinaryIndexResolver<T> : IBinaryIndexResolver<T> {
			private readonly int itemLength;

			public IntegerBinaryIndexResolver(int itemLength) {
				this.itemLength = itemLength;
			}

			public int ItemLength {
				get { return itemLength; }
			}

			public void Write(T value, Stream output) {
				BinaryWriter writer = new BinaryWriter(output);
				if (typeof(T) == typeof(int))
					writer.Write((int)Convert.ChangeType(value, typeof(int)));
				else if (typeof(T) == typeof(long))
					writer.Write((long)Convert.ChangeType(value, typeof(long)));
			}

			public T Read(Stream input) {
				BinaryReader reader = new BinaryReader(input);
				if (typeof(int) == typeof(T))
					return (T) Convert.ChangeType(reader.ReadInt32(), typeof(T));
				if (typeof(long) == typeof(T))
					return (T) Convert.ChangeType(reader.ReadInt64(), typeof (T));

				throw new InvalidOperationException();
			}
		}

		public IIndex<T> CreateTemporaryIndex<T>(long maxSize) where T : IComparable<T> {
			// Try to obtain a resolver fot the type
			IBinaryIndexResolver<T> resolver = GetResolver<T>();
			
			// This implementation uses the database model to store temporary index
			// data, however another implementation could use an index that's stored
			// on the heap or some of both.

			// If the max elements is small enough, we allocate the temporary index
			// from an array.  Note that we should put a limit on this because even
			// a small temporary index can consume a lot of memory if there are
			// hundreds of thousands of them.
			if (maxSize <= 8)
				return Indexes<T>.Array((int)maxSize, resolver);

			// Create a unique temporary data stream,
			Stream data = state.CreateStream();
			// Create and return the object (the data is deleted when finalized).
			return new BinarySortedIndex<T>(data, resolver);
		}

		public ColumnStatistics GetColumnStatistics(Variable columnName) {
			if (columnStats == null)
				columnStats = new Dictionary<Variable, ColumnStatistics>();

			// We store column stats in a local cache.
			// TODO: we need to implement a more complex persistant cache for column statistics.
			ColumnStatistics colStats;

			if (!columnStats.TryGetValue(columnName, out colStats)) {
				colStats = new ColumnStatistics(columnName);
				colStats.PerformSample(this);
				columnStats[columnName] = colStats;
			}

			return colStats;
		}

		public IList<object> QueryTableDefaults(TableName tableName) {
			CheckNotDisposed();
			// Check the table name given is qualified
			CheckTableNameQualified(tableName);


			// TODO: Profile performance of this and if it's bad put it behind a
			//    cache that flushes on system table change.

			// Get the id of the table name
			int table_id = GetTableId(tableName);

			// Get the default table
			ITable default_table = GetTable(SystemTableNames.DefaultColumnExpression);
			// Get the index on the table field of the default table
			IIndexSetDataSource index = GetIndex(SystemTableNames.DefaultColumnExpression, "object_id_idx");

			// Select all rows from the index that equal the table name
			IRowCursor i = index.Select(SelectableRange.Is(new SqlObject(table_id)));
			List<object> result = new List<object>((int)i.Count * 2);
			while (i.MoveNext()) {
				RowId rowid = i.Current;
				// Get the values from the table
				SqlObject columnName = default_table.GetValue(1, rowid);
				SqlObject defaultValue = default_table.GetValue(3, rowid);
				// Add the col name
				result.Add(columnName.ToString());
				// Deserialize the default operation
				result.Add(SqlValue.Deserialize(defaultValue.Value));
			}
			// Return the array
			return result;
		}

		public long CreateUniqueId(TableName tableName) {
			return state.CreateUniqueId(tableName);
		}
	}
}