using System;
using System.Data;

namespace Deveel.Data.Sql.Client {
	public sealed class DeveelSqlConnection : IDbConnection {
		private readonly ISessionContext sessionContext;
		private DeveelSqlTransaction currentTransaction;
		private ConnectionState state;
		
		private readonly object connLock = new object();

		internal DeveelSqlConnection(ISessionContext sessionContext) {
			this.sessionContext = sessionContext;
		}

		internal ISessionContext SessionContext {
			get { return sessionContext; }
		}

		public void Dispose() {
			if (state != ConnectionState.Closed) {
				Close();
				GC.SuppressFinalize(this);
			}
		}

		internal void OnStateChange(ConnectionState newState) {
			state = newState;
		}

		internal void ClearTransaction() {
			currentTransaction = null;
		}

		public DeveelSqlTransaction BeginTransaction() {
			if (currentTransaction != null)
				throw new InvalidOperationException("A transaction is already open.");

			currentTransaction = new DeveelSqlTransaction(this);
			return currentTransaction;
		}

		IDbTransaction IDbConnection.BeginTransaction() {
			return BeginTransaction();
		}

		IDbTransaction IDbConnection.BeginTransaction(IsolationLevel il) {
			if (il != IsolationLevel.Serializable)
				throw new NotSupportedException("The only supported isolation level is SERIALIZABLE.");

			return BeginTransaction();
		}

		public void Close() {
			lock (connLock) {
				if (state == ConnectionState.Closed)
					return;

				try {
					sessionContext.Close();
				} finally {
					OnStateChange(ConnectionState.Closed);
				}
			}
		}

		public void ChangeDatabase(string databaseName) {
			throw new NotImplementedException();
		}

		IDbCommand IDbConnection.CreateCommand() {
			return CreateCommand();
		}

		public DeveelSqlCommand CreateCommand(string text) {
			return new DeveelSqlCommand(this, text);
		}

		public DeveelSqlCommand CreateCommand() {
			return CreateCommand(null);
		}

		public void Open() {
			try {
				if (sessionContext == null) {
					//TODO:
				}

				OnStateChange(ConnectionState.Open);
			} catch (Exception) {
				OnStateChange(ConnectionState.Broken);
				throw;
			}
		}

		public string ConnectionString {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		public int ConnectionTimeout {
			get { throw new NotImplementedException(); }
		}

		public string Database {
			get { throw new NotImplementedException(); }
		}

		public ConnectionState State {
			get { return state; }
		}

		internal DeveelSqlTransaction CurrentTransaction {
			get { return currentTransaction; }
		}
	}
}