using System;

namespace Deveel.Data.Sql {
	public interface ITransactionContext : IDisposable {
		void CreateSchema(string schema);

		bool SchemaExists(string schema);

		bool CanChangeSchema(string schema);

		void DeleteSchema(string schema);

		ITable CreateTable(TableName tableName);

		ITable GetTable(TableName tableName);

		bool TableExists(TableName tableName);

		void DeleteTable(TableName tableName);

		ITableIndex CreateIndex(TableName indexName, string indexType, TableName tableName, string[] columnNames);

		ITableIndex GetIndex(TableName indexName);

		ITableIndex[] GetTableIndexes(TableName tableName);

		void DeleteIndex(TableName tableName);

		bool IndexExists(TableName indexName);

		void Commit();
	}
}