using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public sealed class MockTransactionContext : ITransactionContext {
		private readonly Dictionary<TableName, ITable> tables;
		private readonly Dictionary<TableName, ITableIndex> indexes;
		private readonly List<string> schemata;

		public MockTransactionContext() {
			tables = new Dictionary<TableName, ITable>(new TableNameComparer());
			indexes = new Dictionary<TableName, ITableIndex>(new TableNameComparer());
			schemata = new List<string>();
		}

		public void DeleteTable(TableName tableName) {
			tables.Remove(tableName);
		}

		public ITableIndex CreateIndex(TableName indexName, string indexType, TableName tableName, string[] columnNames) {
			MockTableIndex index = new MockTableIndex(indexName, indexType, tableName, columnNames);
			indexes.Add(indexName, index);
			return index;
		}

		public ITableIndex GetIndex(TableName indexName) {
			ITableIndex index;
			if (!indexes.TryGetValue(indexName, out index))
				return null;
			return index;
		}

		public ITableIndex[] GetTableIndexes(TableName tableName) {
			List<ITableIndex> list = new List<ITableIndex>();
			foreach (KeyValuePair<TableName, ITableIndex> pair in indexes) {
				if (pair.Value.TableName.Equals(tableName))
					list.Add(pair.Value);
			}

			return list.ToArray();
		}

		public void DeleteIndex(TableName tableName) {
			indexes.Remove(tableName);
		}

		public bool IndexExists(TableName indexName) {
			return indexes.ContainsKey(indexName);
		}

		public void Commit() {
		}

		public void CreateSchema(string schema) {
			if (!schemata.Contains(schema))
				schemata.Add(schema);
		}

		public bool SchemaExists(string schema) {
			return schemata.Contains(schema);
		}

		public bool CanChangeSchema(string schema) {
			return true;
		}

		public void DeleteSchema(string schema) {
			schemata.Remove(schema);
		}

		public ITable CreateTable(TableName tableName) {
			MockTable table = new MockTable(tableName);
			tables.Add(tableName, table);
			return table;
		}

		public ITable GetTable(TableName tableName) {
			ITable table;
			if (!tables.TryGetValue(tableName, out table))
				return null;
			return table;
		}

		public bool TableExists(TableName tableName) {
			return tables.ContainsKey(tableName);
		}

		public void Dispose() {
		}

		private class TableNameComparer : IEqualityComparer<TableName> {
			public bool Equals(TableName x, TableName y) {
				return x.Equals(y);
			}

			public int GetHashCode(TableName obj) {
				return obj.GetHashCode();
			}
		}
	}
}