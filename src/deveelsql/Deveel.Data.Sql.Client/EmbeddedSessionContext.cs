using System;
using System.Collections.Generic;
using System.Data;

using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql.Client {
	public sealed class EmbeddedSessionContext : ISessionContext {
		private readonly DatabaseSession session;
		private SystemTransaction transaction;
		private readonly bool allowShutdown;

		private readonly string username;

		private readonly Object syncObject = new Object();

		internal EmbeddedSessionContext(DatabaseSession session, bool allowShutdown, string username) {
			if (String.IsNullOrEmpty(username))
				throw new ArgumentNullException("username");

			this.session = session;
			this.allowShutdown = allowShutdown;
			this.username = username;
		}

		public EmbeddedSessionContext(IDatabaseState dbState, bool allowShutdown, string username)
			: this(new DatabaseSession(dbState), allowShutdown, username) {
		}

		public EmbeddedSessionContext(ISystemState sysState, string database, bool allowShutdown, string username)
			: this(new DatabaseSession(sysState, database), allowShutdown, username) {
		}

		private IQueryContext ExecuteQuery(Query query) {
			lock (syncObject) {
				// Make the statement interpreter
				SqlInterpreter interpreter = new SqlInterpreter(this, transaction);
				// Execute the query
				ITable resultSet = interpreter.ExecQuery(query);

				// Was the last command a transactional operation (commit or rollback)?
				if (interpreter.HasSpecialCommand) {
					if (interpreter.SpecialCommand.Equals("set_schema")) {
						string schemaName = SqlInterpreter.GetStaticString(interpreter.SpecialCommandArgument);
						transaction.ChangeSchema(schemaName);
						resultSet = SqlInterpreter.DMLResult(1);
					} else {
						try {
							// Commit it (if it's a commit op) and dispose
							if (interpreter.SpecialCommand.Equals("commit"))
								session.CommitTransaction(transaction);
						} finally {
							// We must ensure that dispose is always called regardless
							SystemTransaction to_dispose = transaction;
							transaction = null;
							session.DisposeTransaction(to_dispose);
						}
						int result = (transaction == null) ? 0 : 1;
						resultSet = SqlInterpreter.DMLResult(result);
					}
				}

				// Map the result table into a form the JDBC driver wants
				return new EmbeddedQueryContext(resultSet, transaction, syncObject);
			}
		}

		public void Dispose() {
			Close();
		}

		public IDbConnection CreateConnection() {
			return new DeveelSqlConnection(this);
		}

		public IQueryContext CreateContext() {
			lock (syncObject) {
				// Create a new transaction if there isn't already one open.
				if (transaction == null) {
					transaction = session.CreateTransaction(username);
				}

				// The query container table
				QueryContainerTable queryContainer = new QueryContainerTable(transaction, null);
				// Return a query container interface
				return new EmbeddedQueryContext(queryContainer, transaction, syncObject);
			}
		}

		public IQueryContext Execute(IQueryContext context) {
			// Synchronize over this object to enforce the single thread per session
			// rule.  Note that this does not prevent other sessions from interacting
			// with the database concurrently.

			lock (syncObject) {
				// Create a new transaction if there isn't already one open.
				if (transaction == null)
					transaction = session.CreateTransaction(username);

				// Only row count of 1 currently supported,
				if (context.RowCount != 1)
					throw new ArgumentException("Incorrect query context");

				EmbeddedQueryContext result_i = (EmbeddedQueryContext)context;
				QueryContainerTable table = (QueryContainerTable) result_i.BackedTable;

				Query query = table.Query;

				// Execute the query and return the result
				return ExecuteQuery(query);
			}
		}

		public IQueryContext Execute(Query query) {
			// Synchronize over this object to enforce the single thread per session
			// rule.  Note that this does not prevent other sessions from interacting
			// with the database concurrently.

			lock (syncObject) {
				// Create a new transaction if there isn't already one open.
				if (transaction == null)
					transaction = session.CreateTransaction(username);

				// Execute the query and return the result
				return ExecuteQuery(query);
			}
		}

		public void Close() {
		}

		#region EmbeddedQueryContext

		internal class EmbeddedQueryContext : IQueryContext {
			private readonly ITable backedTable;
			private UpdatableResultSetView updatableView;
			private readonly string notNotUpdatableReason;
			private bool isClosed;
			private readonly object syncObject;

			public EmbeddedQueryContext(ITable backedTable, SystemTransaction transaction, object syncObject) {
				this.syncObject = syncObject;
				isClosed = false;
				this.backedTable = backedTable;

				// Determine the updatability of the result set

				notNotUpdatableReason = null;

				// If the result set is a mutable table data source object,
				if (backedTable is IMutableTable) {
					updatableView = new UpdatableResultSetView(transaction, (IMutableTable) backedTable, null, backedTable.GetRowCursor());
				} else {
					// Can we map this to a native table?
					TableName nativeTableName = QueryProcessor.GetNativeTableName(backedTable);
					// If we can,
					if (nativeTableName != null) {
						// The top table must be an operation table and must have all
						// FETCHVAR operations,
						if (backedTable is ExpressionTable) {
							ExpressionTable expressionTable = (ExpressionTable)backedTable;
							Expression[] projectionExps = expressionTable.Expressions;
							foreach (Expression op in projectionExps) {
								if (QueryProcessor.GetAsVariableRef(op) == null) {
									notNotUpdatableReason = "Not updatable, result set contains functional " +
														   "projections. Please simplify the select terms.";
									break;
								}
							}
							// Otherwise, if it all looks ok, set the updatable table
							if (notNotUpdatableReason == null) {
								SystemTable nativeTableSource = transaction.GetTable(nativeTableName);
								updatableView = new UpdatableResultSetView(transaction, nativeTableSource, projectionExps, backedTable.GetRowCursor());
							}
						} else {
							notNotUpdatableReason = "This result set is not updatable.";
						}
					} else {
						notNotUpdatableReason = "Not updatable, result set does not source " +
											   "to a native table.";
					}

					// If we didn't create an updatable view, we create one with null values
					// and use if for iterator caching only
					if (updatableView == null) {
						updatableView = new UpdatableResultSetView(null, null, null, backedTable.GetRowCursor());
					}
				}
			}

			private void CheckUpdatable() {
				if (notNotUpdatableReason != null)
					throw new ApplicationException(notNotUpdatableReason);
			}

			private IRowCursor GetRowCursor() {
				return updatableView.GetCurrentRowCursor();
			}

			public void Dispose() {
				Close();
			}

			public bool IsClosed {
				get {
					lock (syncObject) {
						return isClosed;
					}
				}
			}

			public bool IsUpdatable {
				get { return notNotUpdatableReason == null; }
			}

			public long RowCount {
				get {
					lock (syncObject) {
						return GetRowCursor().Count;
					}
				}
			}

			public int ColumnCount {
				get {
					lock (syncObject) {
						return backedTable.Columns.Count;
					}
				}
			}

			public ITable BackedTable {
				get { return backedTable; }
			}

			public ResultColumn GetColumn(int columnIndex) {
				TableColumn column = backedTable.Columns[columnIndex];
				return new ResultColumn(column.Name, column.Type);
			}

			public RowId GetRowId(long offset) {
				lock (syncObject) {
					return updatableView.GetRowId(offset);
				}
			}

			public bool IsNativelyConverted(int column, long rowOffset) {
				//TODO:
				return true;
			}

			public object GetValue(string columnName, long rowOffset) {
				int columnOffset = backedTable.Columns.IndexOf(columnName);
				return GetValue(columnOffset, rowOffset);
			}

			public object GetValue(int columnOffset, long rowOffset) {
				lock (syncObject) {
					IRowCursor i = GetRowCursor();
					i.MoveTo(rowOffset - 1);
					if (!i.MoveNext())
						throw new ApplicationException();

					RowId rowid = i.Current;
					// Is the cell small?
					// Get the TObject value
					return backedTable.GetValue(columnOffset, rowid).ToObject();
				}
			}

			public void SetValue(int columnOffset, RowId rowid, object value) {
				lock (syncObject) {
					CheckUpdatable();
					updatableView.SetValue(columnOffset, rowid, value);
				}
			}

			public void DeleteRow(long rowOffset) {
				lock (syncObject) {
					CheckUpdatable();
					updatableView.RemoveRow(rowOffset);
				}
			}

			public void UpdateRow(long rowOffset) {
				lock (syncObject) {
					CheckUpdatable();
					updatableView.UpdateRow(rowOffset);
				}
			}

			public long InsertRow() {
				lock (syncObject) {
					CheckUpdatable();
					return updatableView.InsertRow();
				}
			}

			public void Finish(bool commit) {
				lock (syncObject) {
					CheckUpdatable();
					updatableView.Finish(commit);
				}
			}

			public void Close() {
				lock (syncObject) {
					updatableView = null;
					isClosed = true;
				}
			}
		}

		#endregion

		#region QueryContainerTable

		internal class QueryContainerTable : IMutableTable {
			private SystemTransaction transaction;
			private string queryString;
			private readonly List<SqlObject> parameters;
			private TableRow currentRow;
			private long rowCount;
			private readonly ColumnCollection columns;

			public QueryContainerTable(SystemTransaction transaction, Query query) {
				this.transaction = transaction;

				parameters = new List<SqlObject>();
				if (query != null) {
					queryString = query.Text;
					foreach (QueryParameter parameter in query.Parameters)
						parameters.Add(parameter.Value);
				}

				columns = new ColumnCollection(this);
				columns.Add("#QUERY", SqlType.String, true);
			}

			public void Dispose() {
			}

			public long RowCount {
				get { return rowCount; }
			}

			public TableName Name {
				get { return null; }
			}

			public IColumnCollection Columns {
				get { return columns; }
			}

			public Query Query {
				get {
					Query query = new Query(queryString, ParameterStyle.Marker);
					int i = 0;
					foreach (SqlObject parameter in parameters) {
						query.Parameters.Add(new QueryParameter(i++, parameter));
					}

					return query;
				}
			}

			public void PrefetchValue(int columnOffset, RowId rowid) {
			}

			public SqlObject GetValue(int column, RowId row) {
				return column == 0 ? (SqlObject)queryString : parameters[column - 1];
			}

			public bool RowExists(RowId rowid) {
				return rowid.ToInt64() == 0;
			}

			public void FetchValue(int column, RowId row) {
			}

			public IRowCursor GetRowCursor() {
				return new SimpleRowCursor(rowCount);
			}

			public TableRow GetRow(RowId rowid) {
				if (rowid.ToInt64() != 0)
					return null;

				return currentRow;
			}

			public TableRow NewRow() {
				if (currentRow == null) {
					currentRow = new QueryTableRow(this, new RowId(0));
				}

				return currentRow;
			}

			public void Insert(TableRow row) {
				Update(row);
				rowCount++;
			}

			public void Update(TableRow row) {
				queryString = row[0];

				int sz = row.Table.Columns.Count;

				parameters.Clear();
				for (int i = 1; i < sz; i++) {
					parameters.Add(row[i]);
				}

				currentRow = row;
			}

			public void Delete(RowId rowid) {
				if (rowid.ToInt64() != 0)
					throw new ArgumentException();

				queryString = null;
				parameters.Clear();
				currentRow = null;
				rowCount--;
			}

			public void Commit() {
			}

			public void Undo() {
				currentRow = null;
			}

			#region QueryTableRow

			private class QueryTableRow : TableRow {
				private readonly QueryContainerTable table;
				private string queryString;
				private readonly List<SqlObject> values;

				public QueryTableRow(QueryContainerTable table, RowId id) 
					: base(table, id) {
					this.table = table;
					values = new List<SqlObject>();
				}

				public override SqlObject GetValue(int columnOffset) {
					if (columnOffset == 0)
						return queryString;

					columnOffset = columnOffset - 1;

					if (columnOffset < 0 || columnOffset >= values.Count)
						return SqlObject.Null;

					return values[columnOffset];
				}

				public override void SetValue(int columnOffset, SqlObject value) {
					if (columnOffset == 0) {
						queryString = value;
					} else {
						if (columnOffset >= table.Columns.Count) {
							int toAdd = columnOffset - table.Columns.Count - 1;
							for (int i = 0; i < toAdd; i++)
								table.Columns.Add("#PARAM" + (table.Columns.Count - 1), SqlType.Null, false);

							table.Columns.Add("#PARAM" + (table.Columns.Count - 1), value.Type, false);
						}

						columnOffset = columnOffset - 1;
						if (columnOffset >= values.Count) {
							int toAdd = columnOffset - values.Count;
							for (int i = 0; i < toAdd; i++)
								values.Add(SqlObject.Null);

							values.Add(SqlObject.Null);
						}

						values[columnOffset] = value;
					}
				}
			}

			#endregion
		}

		#endregion
	}
}