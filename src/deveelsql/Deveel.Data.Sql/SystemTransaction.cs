using System;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Base;

namespace Deveel.Data.Sql {
	public sealed class SystemTransaction {
		private readonly SystemFunctionManager functionManager;
		private readonly ISystemState state;
		private ITransactionContext context;
		private readonly QueryProcessor queryProcessor;

		private string currentSchema = "APP";

		private Dictionary<Variable, ColumnStatistics> columnStats;
		private FactStatistics factStats;

		public SystemTransaction(ITransactionContext context, ISystemState state) {
			this.context = context;
			this.state = state;
			functionManager = new SystemFunctionManager();

			queryProcessor = new QueryProcessor(this);
		}

		public QueryProcessor QueryProcessor {
			get { return queryProcessor; }
		}

		public void Dispose() {
			state.Dispose();
			context.Dispose();
			context = null;
			GC.SuppressFinalize(this);
		}

		public string CurrentSchema {
			get { return currentSchema; }
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

		public void ChangeSchema(string newSchema) {
			if (!context.SchemaExists(newSchema))
				throw new ApplicationException("The schema '" + newSchema + "' was not found.");

			if (!context.CanChangeSchema(newSchema))
				throw new ApplicationException("Not authorized to change schema.");

			currentSchema = newSchema;
		}

		public string CreateTempLargeObject() {
			throw new NotImplementedException();
		}

		public IBlobDataSource GetTempLargeObject(SqlType type, string key) {
			throw new NotImplementedException();
		}

		public bool TableExists(TableName tableName) {
			return context.TableExists(tableName.ResolveSchema(currentSchema));
		}

		public IMutableTableDataSource GetTable(TableName tableName) {
			ITable table = context.GetTable(tableName.ResolveSchema(currentSchema));
			return new SystemTableDataSource(tableName, table);
		}

		public bool IndexExists(TableName indexName) {
			return context.IndexExists(indexName.ResolveSchema(currentSchema));
		}

		public IIndexSetDataSource[] GetTableIndexes(TableName tableName) {
			IMutableTableDataSource table = GetTable(tableName.ResolveSchema(currentSchema));

			ITableIndex[] indexNames = context.GetTableIndexes(tableName);
			IIndexSetDataSource[] indexSources = new IIndexSetDataSource[indexNames.Length];
			for (int i = 0; i < indexNames.Length; ++i) {
				indexSources[i] = new SystemIndex(table, indexNames[i].Name, indexNames[i]);
			}

			// Return the index sources,
			return indexSources;
		}

		public IIndexSetDataSource GetIndex(TableName indexName) {
			ITableIndex tdbIndex = context.GetIndex(indexName.ResolveSchema(currentSchema));
			IMutableTableDataSource table = GetTable(tdbIndex.TableName);
			return new SystemIndex(table, indexName, tdbIndex);
		}

		public IIndex CreateTemporaryIndex(long maxSize) {
			// This implementation uses the database model to store temporary index
			// data, however another implementation could use an index that's stored
			// on the heap or some of both.

			// If the max elements is small enough, we allocate the temporary index
			// from an array.  Note that we should put a limit on this because even
			// a small temporary index can consume a lot of memory if there are
			// hundreds of thousands of them.
			if (maxSize <= 8)
				return Indexes.Array((int)maxSize);

			// Create a unique temporary data stream,
			Stream data = state.CreateStream();
			// Create and return the object (the data is deleted when finalized).
			return new SortedIndex(data);
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
	}
}