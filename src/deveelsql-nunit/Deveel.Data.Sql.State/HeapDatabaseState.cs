using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.State {
	public sealed class HeapDatabaseState : IDatabaseState {
		private readonly ISystemState system;
		private readonly string name;
		private readonly Dictionary<int, HeapTransactionState> transactions;
		private readonly Dictionary<TableName, long> tableUniqueIds;

		internal readonly Dictionary<TableName, ITable> tables;

		private int tranIdSeq = -1;

		public HeapDatabaseState(ISystemState system, string name) {
			this.system = system;
			this.name = name;
			transactions = new Dictionary<int, HeapTransactionState>(10);

			tableUniqueIds = new Dictionary<TableName, long>();
			tables = new Dictionary<TableName, ITable>();
		}

		public void Dispose() {
		}

		public string Name {
			get { return name; }
		}

		public ISystemState System {
			get { return system; }
		}

		public long CreateUniqueId(TableName tableName) {
			long lastId;
			if (!tableUniqueIds.TryGetValue(tableName, out lastId))
				lastId = -1;

			tableUniqueIds[tableName] = ++lastId;
			return lastId;
		}

		public ITransactionState CreateTransaction() {
			HeapTransactionState transaction = new HeapTransactionState(this, ++tranIdSeq);
			transactions.Add(transaction.Id, transaction);
			return transaction;
		}

		public void DisposeTransaction(ITransactionState transactionState) {
			HeapTransactionState heapTransactionState = (HeapTransactionState) transactionState;
			transactions.Remove(heapTransactionState.Id);
			heapTransactionState.tables.Clear();
		}

		public void CommitTransaction(ITransactionState transactionState) {
			HeapTransactionState heapTransactionState = (HeapTransactionState) transactionState;
			foreach (KeyValuePair<TableName, ITable> table in heapTransactionState.tables) {
				tables[table.Key] = table.Value;
			}
		}
	}
}