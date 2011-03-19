using System;
using System.IO;

namespace Deveel.Data.Sql.State {
	public interface ITransactionState : IDisposable {
		int RowIdLength { get; }


		IDatabaseState Database { get; }

		void CreateSchema(string schema);

		void DeleteSchema(string schema);

		ITable CreateTable(TableName tableName);

		ITable GetTable(TableName tableName);

		long CreateUniqueId(TableName tableName);

		void DeleteTable(TableName tableName);

		ITableIndex CreateIndex(IndexName indexName, string indexType, string[] columnNames, bool[] columnOrders);

		ITableIndex GetIndex(IndexName indexName);

		void DeleteIndex(IndexName indexName);

		Stream CreateStream();
	}
}