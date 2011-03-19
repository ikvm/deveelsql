using System;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Sql.State {
	public sealed class HeapTransactionState : ITransactionState {
		private readonly HeapDatabaseState dbState;
		private readonly Dictionary<long , Stream> allocated;
		internal readonly Dictionary<TableName, ITable> tables;
		private readonly Dictionary<IndexName, ITableIndex> indexes;
		private readonly List<string> schemata;
		private readonly int id;
		
		private long idSeq = -1;

		private readonly object SyncRoot = new object();

		public HeapTransactionState(HeapDatabaseState dbState, int id) {
			allocated = new Dictionary<long, Stream>();
			this.dbState = dbState;
			this.id = id;

			tables = new Dictionary<TableName, ITable>();
			indexes = new Dictionary<IndexName, ITableIndex>();
			schemata = new List<string>();

			foreach (KeyValuePair<TableName, ITable> table in dbState.tables) {
				tables[table.Key] = table.Value;
			}
		}

		public void Dispose() {
			List<Stream> list = new List<Stream>(allocated.Values);
			for (int i = 0; i < list.Count; i++) {
				list[i].Close();
			}
			GC.SuppressFinalize(this);
		}

		public int RowIdLength {
			get { return 8; }
		}

		public IDatabaseState Database {
			get { return dbState; }
		}

		public int Id {
			get { return id; }
		}

		public void CreateSchema(string schema) {
			if (!schemata.Contains(schema))
				schemata.Add(schema);
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

		public long CreateUniqueId(TableName tableName) {
			return dbState.CreateUniqueId(tableName);
		}

		public void DeleteTable(TableName tableName) {
			tables.Remove(tableName);
		}

		public ITableIndex CreateIndex(IndexName indexName, string indexType, string[] columnNames, bool[] columnOrders) {
			MockTable table = (MockTable) GetTable(indexName.TableName);
			MockTableIndex index = new MockTableIndex(table, indexName, indexType, columnNames, columnOrders);
			indexes.Add(indexName, index);
			return index;
		}

		public ITableIndex GetIndex(IndexName indexName) {
			ITableIndex index;
			if (!indexes.TryGetValue(indexName, out index))
				return null;
			return index;
		}

		public void DeleteIndex(IndexName indexName) {
			indexes.Remove(indexName);
		}

		public Stream CreateStream() {
			lock (SyncRoot) {
				long streamId = ++idSeq;
				HeapStream stream = new HeapStream(this, streamId);
				allocated.Add(streamId, stream);
				return stream;
			}
		}

		#region HeapStream

		private class HeapStream : MemoryStream {
			private readonly long id;
			private readonly HeapTransactionState state;

			public HeapStream(HeapTransactionState state, long id)
				: base (1024) {
				this.state = state;
				this.id = id;
			}

			public override void Close() {
				state.allocated.Remove(id);
				base.Close();
			}
		}

		#endregion
	}
}