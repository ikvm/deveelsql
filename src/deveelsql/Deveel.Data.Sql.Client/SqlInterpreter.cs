using System;
using System.Collections.Generic;
using System.Data;
using System.IO;

using Deveel.Data.Sql.Parser;
using Deveel.Data.Sql;

namespace Deveel.Data.Sql.Client {
	internal sealed partial class SqlInterpreter {
		private readonly ISessionContext context;
		private readonly SystemTransaction transaction;
		private string lastSpecialCommand;
		private Expression specialCommandArgument;

		public SqlInterpreter(ISessionContext context, SystemTransaction transaction) {
			this.context = context;
			this.transaction = transaction;
		}

		public Expression SpecialCommandArgument {
			get { return specialCommandArgument; }
		}

		public bool HasSpecialCommand {
			get { return !String.IsNullOrEmpty(lastSpecialCommand); }
		}

		public string SpecialCommand {
			get { return lastSpecialCommand; }
		}

		private TableName QualifyTableName(TableName tableName) {
			return tableName.ResolveSchema(transaction.CurrentSchema);
		}

		internal static ITable DMLResult(long v) {
			return QueryProcessor.ResultTable(new SqlObject(v));
		}

		private TableName GetTableName(Expression exp, int n) {
			Expression tname_op = (Expression)exp.GetArgument("arg" + n);
			if (tname_op.Type == ExpressionType.FetchTable)
				throw new ApplicationException("No FETCHTABLE expression at param " + n);

			return (TableName)tname_op.GetArgument("name");
		}

		private static SqlObject CreateValue(ITable table, RowId rowid, IList<string> columns) {
			int sz = columns.Count;
			if (sz == 0)
				throw new ArgumentException();

			// If there's just 1 column reference, we fetch the TObject and return
			// it.
			if (sz == 1) {
				string columnName = columns[0];
				int columnOffset = table.Columns.IndexOf(columnName);
				return table.GetValue(columnOffset, rowid);
			}

			// Otherwise we make a composite object

			// Make the composite type
			SqlObject[] val = new SqlObject[sz];
			for (int i = 0; i < sz; ++i) {
				int columnOffset = table.Columns.IndexOf(columns[i]);
				val[i] = table.GetValue(columnOffset, rowid);
			}
			// Create the composite type and return the object
			return SqlObject.MakeComposite(val);
		}

		private static IRowCursor QueryAllMatches(SystemTransaction transaction, TableName tableName, SystemTable table,
			IList<string> columns, SqlObject val) {
			// Try and find an index on these columns
			SystemIndexSetDataSource indexSet = transaction.FindIndexOn(tableName, columns);

			// If index found
			if (indexSet != null)
				// Query the index and find all matches
				return indexSet.Select(SelectableRange.Is(val));

			// Otherwise no index, so scan the table for matches

			// Make an Expression for the operation;
			//  (column1, column2, ...columnn) = val
			Expression compExp;
			int sz = columns.Count;
			if (sz > 1) {
				FunctionExpression cfunExp = new FunctionExpression("composite_fetch");
				for (int i = 0; i < sz; ++i) {
					Expression varRef = new FetchVariableExpression(new Variable(tableName, columns[i]));
					cfunExp.Parameters.Add(varRef);
				}
				compExp = cfunExp;
			} else if (sz == 1) {
				compExp = new FetchVariableExpression(new Variable(tableName, columns[0]));
			} else {
				throw new ApplicationException("Invalid columns list size");
			}

			// Equality test
			FunctionExpression funExp = new FunctionExpression("@is_sql");
			funExp.Parameters.Add(compExp);
			funExp.Parameters.Add(new FetchStaticExpression(val));

			// Create a query processor and perform the scan operation
			QueryProcessor processor = new QueryProcessor(transaction);
			ITable result = processor.FilterByScan(table, funExp);
			// Return the row cursor
			return result.GetRowCursor();
		}

		internal static void SetInsertRowToDefault(SystemTransaction transaction, TableName table_name, IMutableTable table, RowId rowid) {
			// Get all column defaults on the table
			IList<object> table_defaults = transaction.QueryTableDefaults(table_name);
			int sz = table_defaults.Count / 2;
			// Exit quickly if there's no default values
			if (sz == 0)
				return;

			// Create a query processor
			QueryProcessor processor = new QueryProcessor(transaction);
			// For each default value,
			TableRow row = table.GetRow(rowid);
			for (int i = 0; i < sz; ++i) {
				string colName = (string)table_defaults[i * 2];
				Expression colDefault = (Expression)table_defaults[(i * 2) + 1];
				// Execute the default value expression
				ITable defaultResult = processor.Execute(colDefault);
				// Turn it into a TObject
				SqlObject val = defaultResult.GetValue(0, new RowId(0));
				// The col num of the column name
				int colIndex = table.Columns.IndexOf(colName);
				if (colIndex < 0)
					throw new ApplicationException("Column '" + colName + "' not found for DEFAULT value");

				// And insert it
				row.SetValue(colIndex, val);
			}
		}

		internal static void CompleteRowInsert(SystemTransaction transaction, IMutableTable table, TableName tableName, RowId rowid) {
			IIndexSetDataSource[] ids = transaction.GetTableIndexes(tableName);
			foreach (IIndexSetDataSource i in ids) {
				i.Insert(rowid);
			}

			// TODO: check for constraint violations

			table.Commit();
		}

		internal static void CompleteRowUpdate(SystemTransaction transaction, IMutableTable table, TableName tableName, RowId beforeRowid, RowId afterRowid) {
			IIndexSetDataSource[] ids = transaction.GetTableIndexes(tableName);
			foreach (IIndexSetDataSource i in ids) {
				i.Remove(beforeRowid);
				i.Insert(afterRowid);
			}

			// TODO: check for constraint violations

			table.Commit();
		}

		internal static void CompleteRowRemove(SystemTransaction transaction, IMutableTable table, TableName tableName, RowId rowid) {
			IIndexSetDataSource[] ids = transaction.GetTableIndexes(tableName);
			foreach (IIndexSetDataSource i in ids) {
				i.Remove(rowid);
			}

			// TODO: check for constraint violations

			table.Commit();
		}

		public ITable Execute(Query query, Expression expression) {
			// Create the QueryProcessor
			QueryProcessor processor = new QueryProcessor(transaction);

			// If it's a select,
			if (expression is SelectExpression) {
				QueryOptimizer optimizer = new QueryOptimizer(transaction);
				expression = optimizer.SubstituteParameters(expression, query);
				expression = optimizer.Qualify(expression);
				expression = optimizer.Optimize(expression);

				// Execute the query,
				return processor.Execute(expression);
			}

			// Set the parameter as the base table, and the base rowid (the
			// parameters table only has 1 row).
			processor.PushTable(new QueryParametersTable(query));
			processor.UpdateTableRow(new RowId(0));

			// Otherwise it must be an interpretable function

			if (expression is FunctionExpression) {
				string fun_name = (string)expression.GetArgument("name");

				if (fun_name.Equals("create_table"))
					return CreateTable(expression);
				/*
				TODO:
				if (fun_name.Equals("drop_table"))
					return DropTable(processor, expression);
				if (fun_name.Equals("create_index"))
					return CreateIndex(processor, expression);
				if (fun_name.Equals("drop_index"))
					return DropIndex(processor, expression);
				if (fun_name.Equals("explain_expression"))
					return ExplainExpression(expression);
				*/
			}

			throw new NotSupportedException();
		}

		public ITable ExecQuery(Query query) {
			// TODO: Look up the query in a cache to see if we parsed it
			//   before.

			// Parse it
			SqlParser parser = new SqlParser(new StringReader(query.Text));

			// Determine that type of expression and dispatch as necessary,

			// If it's a SELECT query,
			Expression expression;
			try {
				expression = parser.Statement();
			} catch (ParseException e) {
				// Parse error, report it back as an sql exception.
				throw new ApplicationException(e.Message);
			}

			// commit and rollback are special case operations,
			string fun_name = "";
			if (expression is FunctionExpression) {
				fun_name = expression.GetArgument("name").ToString();
			}
			if (fun_name.Equals("transaction_commit")) {
				lastSpecialCommand = "commit";
				return null;
			}
			if (fun_name.Equals("transaction_rollback")) {
				lastSpecialCommand = "rollback";
				return null;
			}
			if (fun_name.Equals("schema_assignment")) {
				lastSpecialCommand = "set_schema";
				specialCommandArgument = expression;
				return null;
			}

			// Put the operation through the interpreter
			return Execute(query, expression);
		}

		private QueryResult TableUpdatable(TableName tableName) {
			return TableUpdatable(transaction.GetTable(tableName));
		}

		private QueryResult TableUpdatable(IMutableTable table) {
			return new QueryResult(new EmbeddedSessionContext.EmbeddedQueryContext(table, transaction, new Object()));
		}

		private IDbCommand CreateDbCommand(string queryString) {
			return new DeveelSqlCommand(context, queryString);
		}

		public static string GetStaticString(Expression expression) {
			FetchStaticExpression staticExp = (FetchStaticExpression) expression.GetArgument("arg0");
			return staticExp.Value;
		}
	}
}