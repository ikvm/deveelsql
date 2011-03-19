using System;
using System.Data;

namespace Deveel.Data.Sql.Client {
	public sealed class DeveelSqlTransaction : IDbTransaction {
		private readonly DeveelSqlConnection connection;
		private bool committed;
		private bool rolledback;

		internal DeveelSqlTransaction(DeveelSqlConnection connection) {
			this.connection = connection;
		}

		public void Dispose() {
			if (!committed && !rolledback) {
				Rollback();
				GC.SuppressFinalize(this);
			}
		}

		public void Commit() {
			if (committed)
				throw new InvalidOperationException("The transaction was already committed.");

			try {
				IDbCommand command = connection.CreateCommand();
				command.CommandText = "COMMIT";
				command.ExecuteNonQuery();
			} catch (Exception e) {
				throw new InvalidOperationException("The transaction was not committed properly.", e);
			} finally {
				committed = true;
				connection.ClearTransaction();
			}
		}

		public void Rollback() {
			if (rolledback)
				throw new InvalidOperationException("The transaction was already rolleback.");

			try {
				IDbCommand command = connection.CreateCommand();
				command.CommandText = "ROLLBACK";
				command.ExecuteNonQuery();
			} catch (Exception e) {
				throw new InvalidOperationException("The transaction was not committed properly.", e);
			} finally {
				rolledback = true;
				connection.ClearTransaction();
			}
		}

		IDbConnection IDbTransaction.Connection {
			get { return Connection; }
		}

		public DeveelSqlConnection Connection {
			get { return connection; }
		}

		IsolationLevel IDbTransaction.IsolationLevel {
			get { return IsolationLevel.Serializable; }
		}
	}
}