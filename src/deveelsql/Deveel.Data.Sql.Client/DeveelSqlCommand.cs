using System;
using System.Data;

namespace Deveel.Data.Sql.Client {
	public sealed class DeveelSqlCommand : IDbCommand {
		private ISessionContext session;
		private DeveelSqlConnection connection;
		private readonly DeveelSqlParameterCollection parameters;
		private QueryResult[] results;
		private int multiResultsIndex;
		private string text;

		internal DeveelSqlCommand(ISessionContext session, string text)
			: this() {
			this.session = session;
			this.text = text;
		}

		public DeveelSqlCommand() {
			parameters = new DeveelSqlParameterCollection(this);
		}

		public DeveelSqlCommand(DeveelSqlConnection connection, string text)
			: this() {
			Connection = connection;
			CommandText = text;
		}

		public DeveelSqlCommand(string text)
			: this((DeveelSqlConnection)null, text) {
		}

		public DeveelSqlCommand(DeveelSqlConnection connection)
			: this(connection, null) {
		}

		private QueryResult[] InternalResultSetList(int count) {
			if (count <= 0)
				throw new ArgumentException("'count' must be > 0");

			if (results != null && results.Length != count) {
				// Dispose all the ResultSet objects currently open.
				for (int i = 0; i < results.Length; ++i) {
					results[i].Dispose();
				}
				results = null;
			}

			if (results == null) {
				results = new QueryResult[count];
				for (int i = 0; i < count; ++i) {
					results[i] = new QueryResult(this);
				}
			}

			return results;
		}

		internal QueryResult InternalResultSet() {
			return InternalResultSetList(1)[0];
		}

		private QueryResult ExecuteQuery(QueryResult result) {
			if (connection.State != ConnectionState.Open)
				throw new InvalidOperationException("The connection is in an invalid state.");

			connection.OnStateChange(ConnectionState.Executing);

			try {
				// Execute the query,
				// Make it into a regular query,
				IQueryContext context = session.CreateContext();
				QueryResult queryRs = new QueryResult(context);
				queryRs.BeginInsertRow();
				queryRs.Update(0, text);
				DeveelSqlParameterCollection vars = Parameters;
				for (int i = 0; i < vars.Count; ++i) {
					DeveelSqlParameter v = vars[i];
					if (v != null) {
						queryRs.SetCurrentRowCell(new ObjectValue(v.Value), i + 1);
					}
				}
				queryRs.InsertRow();

				// Execute the query,
				IQueryContext resps = session.Execute(context);
				queryRs.Close();
				result.ConnectionSetup(resps);
				return result;
			} finally {
				connection.OnStateChange(ConnectionState.Open);
			}
		}

		private QueryResult ExecuteSingleQuery() {
			// Allocate the result set for this batch
			QueryResult result = InternalResultSet();

			// Reset the result set index
			multiResultsIndex = 0;

			// Make sure the result set is closed
			results[0].CloseCurrentResult();

			// Execute each query
			return ExecuteQuery(result);
		}


		internal void DisposeContext(IQueryContext resp) {
			resp.Close();
		}

		internal bool HasMoreResults() {
			// If we are at the end then return false
			if (results == null || multiResultsIndex >= results.Length)
				return false;

			// Move to the next result set.
			++multiResultsIndex;

			// We successfully moved to the next result
			return true;
		}

		internal QueryResult GetCurrentContext() {
			if (results != null) {
				if (multiResultsIndex < results.Length) {
					return results[multiResultsIndex];
				}
			}
			return null;
		}

		public void Dispose() {
			// Behaviour of calls to command undefined after this method finishes.
			if (results != null) {
				for (int i = 0; i < results.Length; ++i) {
					results[i].Dispose();
				}
				results = null;
			}
			GC.SuppressFinalize(this);
		}

		public void Prepare() {
		}

		public void Cancel() {
			if (results != null) {
				for (int i = 0; i < results.Length; ++i) {
					DisposeContext(results[i].QueryContext);
				}
			}
		}

		IDbDataParameter IDbCommand.CreateParameter() {
			return CreateParameter();
		}

		public DeveelSqlParameter CreateParameter() {
			return new DeveelSqlParameter();
		}

		public int ExecuteNonQuery() {
			QueryResult result =  ExecuteSingleQuery();
			return !result.IsUpdate ? -1 : result.UpdateCount;
		}

		IDataReader IDbCommand.ExecuteReader() {
			return ExecuteReader();
		}

		public DeveelSqlDataReader ExecuteReader() {
			return ExecuteReader(CommandBehavior.Default);
		}

		IDataReader IDbCommand.ExecuteReader(CommandBehavior behavior) {
			return ExecuteReader(behavior);
		}

		public DeveelSqlDataReader ExecuteReader(CommandBehavior behavior) {
			if (connection.State == ConnectionState.Fetching)
				throw new InvalidOperationException("There is a data-reader already opened within the connection.");

			QueryResult result = InternalResultSet();
			result = ExecuteQuery(result);
			DeveelSqlDataReader reader = new DeveelSqlDataReader(this, result, behavior);
			connection.OnStateChange(ConnectionState.Fetching);
			return reader;
		}

		public object ExecuteScalar() {
			QueryResult result = ExecuteSingleQuery();
			if (result.IsUpdate)
				throw new InvalidOperationException("The query resulted into an update command.");

			if (result.RowCount == 0)
				return null;

			return result.GetRawColumn(0);
		}

		IDbConnection IDbCommand.Connection {
			get { return Connection; }
			set { Connection = (DeveelSqlConnection) value; }
		}

		public DeveelSqlConnection Connection {
			get { return connection; }
			set {
				if (value == null)
					throw new ArgumentNullException("value");

				connection = value;
				session = connection.SessionContext;
			}
		}

		IDbTransaction IDbCommand.Transaction {
			get { return connection.CurrentTransaction; }
			set { throw new NotSupportedException(); }
		}

		public string CommandText {
			get { return text; }
			set { text = value; }
		}

		public int CommandTimeout {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		CommandType IDbCommand.CommandType {
			get { return CommandType.Text; }
			set {
				if (value != CommandType.Text)
					throw new NotSupportedException();
			}
		}

		IDataParameterCollection IDbCommand.Parameters {
			get { return Parameters; }
		}

		public DeveelSqlParameterCollection Parameters {
			get { return parameters; }
		}

		public UpdateRowSource UpdatedRowSource {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}
	}
}