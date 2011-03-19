using System;
using System.Collections.Generic;

using Deveel.Data.Base;
using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	public sealed partial class QueryProcessor {
		private readonly SystemTransaction transaction;
		private readonly List<ITable> tableStack;
		private readonly List<RowId> rowIdStack;
		
		internal QueryProcessor(SystemTransaction transaction) {
			this.transaction = transaction;
			tableStack = new List<ITable>();
			rowIdStack = new List<RowId>();
		}
		
		internal QueryProcessor(QueryProcessor src) {
			transaction = src.transaction;
			tableStack = new List<ITable>(src.tableStack);
			rowIdStack = new List<RowId>(src.rowIdStack);
		}
		
		private ITable Function(FunctionExpression op) {
			string functionName = op.Name;
			int argCount = op.Parameters.Count;

			if (transaction.FunctionManager.FunctionExists(functionName)) {
				Expression[] expArgs = new Expression[argCount];
				for (int i = 0; i < expArgs.Length; ++i) {
					expArgs[i] = (Expression) op.Parameters[i];
				}
				// If it's an aggregate function,
				if (transaction.FunctionManager.IsAggregate(functionName)) {
					// (TODO: make this a 'GetGroupValue' processor function and
					//   support subset filtered stack).
					// The top table should be an aggregate table, or aggregate table
					// friendly (have a near child table that is an aggregate table).
					ITable topTable = tableStack[tableStack.Count - 1];
					RowId rowid = rowIdStack[tableStack.Count - 1];
					// Get the group of the current rowid
					AggregateTable aggregateTable = GetAggregateTable(topTable);
					ITable group = aggregateTable.GetGroupValue(rowid.ToInt64());
					// Execute the aggregate function
					bool distinct = op.IsDistinct;
					return transaction.FunctionManager.EvaluateAggregate(functionName, this, distinct, group, expArgs);
				}
					
				return transaction.FunctionManager.Evaluate(functionName, this, expArgs);
			} 
			
			if (functionName.Equals("range_set")) {
				// Resolve the variable
				SqlObject[] val = Result(DoExecute((Expression) op.Parameters[0]));
				SelectableRange range_set = (SelectableRange)op.Parameters[1];
				if (range_set.Intersector.ValueIntersects(val)) {
					// If the resolved value intersects, return true
					return ResultTable(new SqlObject(true));
				} else {
					// Otherwise return false
					return ResultTable(new SqlObject(false));
				}
			}
			
			// This function evaluates each parameter as a composite part and returns
			// an evaluation of the object as a composite object.
			if (functionName.Equals("composite_fetch")) {
				if (argCount == 0)
					throw new ArgumentException();
				
				// If there's only 1 arg,
				if (argCount == 1)
					return DoExecute((Expression)op.Parameters[0]);
				
				// Otherwise it's a true composite
				throw new NotImplementedException();
			}
			
			throw new ArgumentException("Unknown function " + functionName);
		}

		private ITable FetchVariable(Variable var) {
			return ResultTable(new SqlObject[] { GetValue(var.Name) });
		}

		private ITable FetchStatic(SqlObject[] ob) {
			return ResultTable(ob);
		}
		
		private ITable FetchTable(TableName tname) {
			return transaction.GetTable(tname);
		}

		private ITable AliasTable(ITable child, TableName tname) {
			return new AliasedTable(child, tname);
		}

		
		private ITable DoExecute(Expression expression) {
			ExpressionType type = expression.Type;
			
			switch(type) {
				case ExpressionType.Filter:
					// We are filtering on the child,
					ITable table = DoExecute(((FilterExpression)expression).Child);
					return Filter(table, (FilterExpression)expression);
				case ExpressionType.Join: {
					// The tables to join,
					JoinExpression joinExp = (JoinExpression)expression;
					ITable leftTable = DoExecute(joinExp.Left);
					ITable rightTable = DoExecute(joinExp.Right);
					return Join(leftTable, rightTable, joinExp);
				}
				case ExpressionType.Function:
					return Function((FunctionExpression) expression);
				case ExpressionType.FetchVariable:
					Variable var = ((FetchVariableExpression)expression).Variable;
					return FetchVariable(var);
				case ExpressionType.FetchStatic:
					SqlObject[] static_val = ((FetchStaticExpression)expression).Values;
					return FetchStatic(static_val);
				case ExpressionType.FetchTable:
					TableName tname = ((FetchTableExpression)expression).TableName;
					return FetchTable(tname);
				case ExpressionType.AliasTableName:
					AliasTableNameExpression aliasExp = (AliasTableNameExpression)expression;
					TableName alias = aliasExp.Alias;
					return AliasTable(DoExecute(aliasExp.Child), alias);
				default:
					throw new ApplicationException("Unknown expression");
			}
		}
		
		private SqlType[] CreateResolverType(ITable table, Expression[] expressions) {
			// Fetch the DbType object for the expression(s).  If it's not composite
			// we simply lookup the expression.
			SqlType[] indexType;
			int compositeParts = expressions.Length;
			if (compositeParts == 1) {
				// Single TType
				indexType = new SqlType[1];
				indexType[0] = GetExpressionType(table, expressions[0]);
			} else {
				// Composite DbType
				SqlType[] compositeType = new SqlType[compositeParts];
				for (int n = 0; n < compositeParts; ++n)
					compositeType[n] = GetExpressionType(table, expressions[n]);
				indexType = compositeType;
			}
			return indexType;
		}
		
		private static bool TrueResult(ITable table) {
			if (!IsFunctionResultTable(table))
				return false;
			
			SqlObject[] result = Result(table);
			// If the returned value is not null
			return result.Length == 1 && !result[0].IsNull && result[0].Value.ToBoolean().Value;
		}
		
		private static AggregateTable GetAggregateTable(ITable table) {
			if (table is AggregateTable)
				return (AggregateTable)table;
			if (table is FilteredTable)
				return GetAggregateTable(((FilteredTable)table).BaseTable);

			// This means the aggregate table we expected is not reachable from the
			// table we are querying from.
			throw new ApplicationException("AggregateTable not reachable");
		}

		private IIndexSetDataSource GetIndex(ITable graph, string indexName) {
			// Null check
			if (indexName == null)
				return null;
			
			// If graph is an instance of alias table name, then the index is valid
			if (graph is AliasedTable)
				return transaction.GetIndex(graph.Name, indexName);
			if (graph is IMutableTable)
				return transaction.GetIndex(graph.Name, indexName);
			
			// If it's a subset table, index requests may fallthrough
			if (graph is SubsetTable) {
				SubsetTable subsetTable = (SubsetTable)graph;
				if (subsetTable.IndexRequestFallthrough)
					return GetIndex(subsetTable.BaseTable, indexName);
			}
			
			// Otherwise index not valid for this graph
			return null;
		}

		internal static Variable GetAsVariableRef(Expression op) {
			if (op.Type == ExpressionType.FetchVariable) {
				return (Variable)op.GetArgument("var");
			}
			return null;
		}

		internal static TableName GetNativeTableName(ITable table) {
			if (table is AggregateTable)
				// Aggregate tables can't be sourced back to a native source because the
				// filter compacts several rows behind single aggregate sources.
				return null;

			if (table is FilteredTable)
				// Source back through filtered tables by default
				return GetNativeTableName(((FilteredTable) table).BaseTable);
			if (table is SystemTable) {
				// Reached a TSTableDataSource which is a native source
				// TODO: We should probably have a NativeTableSource interface that
				//   tables implement to represent they are a native source that can be
				//   changed, etc.
				return ((SystemTable) table).Name;
			}

			// All other table types can't be sourced back
			return null;

		}

		internal IndexResolver CreateResolver(ITable table, FunctionExpression sortExpression) {
			// Sort expressions
			string functionName = sortExpression.Name;
			
			if (!functionName.Equals("composite"))
				throw new ArgumentException("Invalid sort function expression.");
			
			int paramCount = sortExpression.Parameters.Count;

			// Extract the terms and the ordering information from the function
			int termCount = paramCount / 2;
			Expression[] sortExprs = new Expression[termCount];
			bool[] sortAscending = new bool[termCount];
			int n = 0;

			for (int i = 0; i < paramCount; i += 2) {
				sortExprs[n] = (Expression) sortExpression.Parameters[i];
				SqlObject asc = (SqlObject)sortExpression.Parameters[i + 1];
				sortAscending[n] = SqlObject.Equals(asc, new SqlObject(true));
				++n;
			}

			// Create a Type of the composite of the expressions
			SqlType[] indexType = CreateResolverType(table, sortExprs);
			// Create the resolver,
			return new ExpressionIndexResolver(this, table, indexType, sortAscending, sortExprs);
		}

		internal ITable DistinctSubset(ITable table, Expression[] exps) {
			// Trivial case of an empty table,
			if (table.RowCount == 0)
				return table;

			IndexResolver resolver = CreateResolver(table, exps);
			// The working set,
			IIndex<RowId> workingSet = transaction.CreateTemporaryIndex<RowId>(table.RowCount);
			// Iterate over the table
			IRowCursor cursor = table.GetRowCursor();

			// Wrap in a forward prefetch iterator
			cursor = new PrefetchRowCursor(cursor, table);

			while (cursor.MoveNext()) {
				// The rowid
				RowId rowid = cursor.Current;
				// Fetch the SqlObject
				SqlObject[] val = resolver.GetValue(rowid);
				// TODO: How should DISTINCT behave for multiple columns when one of
				//   the items may or may not be null?

				// Insert only if all the values are not null,
				bool nullFound = false;
				foreach (SqlObject v in val) {
					if (v.IsNull)
						nullFound = true;
				}

				if (!nullFound)
					// Index it
					workingSet.InsertUnique(val, rowid, resolver);
			}

			// Wrap it in an iterator and return, etc
			return new SubsetTable(table, new DefaultRowCursor(workingSet.GetCursor()));
		}

		private IndexResolver CreateResolver(ITable table, Expression[] expressions) {
			// Create a DbType of the composite of the operations
			SqlType[] indexType = CreateResolverType(table, expressions);
			// Create the resolver for the term(s) on the table
			return new ExpressionIndexResolver(this, table, indexType, new bool[] { true }, expressions);
		}
		
		public void PushTable(ITable table) {
			tableStack.Add(table);
			rowIdStack.Add(null);
		}

		public ITable PopTable() {
			rowIdStack.RemoveAt(rowIdStack.Count - 1);
			ITable table = tableStack[tableStack.Count - 1];
			tableStack.RemoveAt(tableStack.Count - 1);
			return table;
		}

		public void UpdateTableRow(RowId rowid) {
			rowIdStack[rowIdStack.Count - 1] = rowid;
		}
		
		public SqlObject GetValue(string columnName) {
			// We inspect the table stack from the top to the bottom, the first
			// valid reference is the value we return
			int sz = tableStack.Count;
			for (int i = sz - 1; i >= 0; --i) {
				ITable stackEntry = tableStack[i];
				int colOffset = stackEntry.Columns.IndexOf(columnName);
				if (colOffset != -1) {
					// Found it, so look it up
					// Fetch the row_id
					RowId rowid = rowIdStack[i];
					return stackEntry.GetValue(colOffset, rowid);
				}
			}
			
			throw new ApplicationException("Unable to dereference " + columnName);
		}
		
		public SqlType GetColumnType(string columnName) {
			// We inspect the table stack from the top to the bottom, the first
			// valid reference is the value we return
			int sz = tableStack.Count;
			for (int i = sz - 1; i >= 0; --i) {
				ITable stackEntry = tableStack[i];
				int colOffset = stackEntry.Columns.IndexOf(columnName);
				if (colOffset != -1)
					// Found it, so look it up
					return stackEntry.Columns[colOffset].Type;
			}
			
			throw new ApplicationException("Unable to dereference " + columnName);
		}

		
		public SqlType GetExpressionType(ITable table, Expression expression) {
			// If it's a fetch var expression
			if (expression.Type == ExpressionType.FetchVariable) {
				// The var
				Variable var = ((FetchVariableExpression) expression).Variable;
				ITable varTable = table;
				// If it's in the table
				int colOffset = varTable.Columns.IndexOf(var.Name);
				// Query the stack if we didn't find it yet
				int sz = tableStack.Count;
				for (int i = sz - 1; i >= 0 && colOffset == -1; --i) {
					varTable = tableStack[i];
					colOffset = varTable.Columns.IndexOf(var.Name);
				}
				// Exception if we didn't find it
				if (colOffset == -1)
					throw new ApplicationException("Unable to dereference " + var);
				
				// Get the ttype
				return varTable.Columns[colOffset].Type;
			}
				// If it's a function,
			if (expression.Type == ExpressionType.Function) {
				// Get the function name,
				FunctionExpression functionExp = (FunctionExpression)expression;
				string functionName = functionExp.Name;
				// CAST is special case
				if (functionName.Equals("@cast")) {
					// The parameter count
					int paramCount = functionExp.Parameters.Count;
					// It MUST be 2
					if (paramCount != 2)
						throw new ApplicationException("Incorrect argument count to CAST");
					
					// The return type is the DbType as specified in the second
					// argument
					// Get the Expression object representing param 1
					FetchStaticExpression expType = (FetchStaticExpression)functionExp.Parameters[1];
					// It will be a FETCH_STATIC expression, turn it into a string and
					// the string is the type encoding.
					SqlObject[] val = expType.Values;
					string castTypeString = val[0].Value.ToString();
					return SqlType.Parse(castTypeString);
				} else {
					// Get the list of function specifications for this function
					Function[] functions = transaction.FunctionManager.GetFunction(functionName);
					// No function, so generate an error
					if (functions == null || functions.Length == 0)
						throw new ApplicationException("Function not found: " + functionName);
					
					// The parameter count
					int paramCount = functionExp.Parameters.Count;
					// For each spec,
					foreach (Function function in functions) {
						// If the spec matches the parameter count
						if (function.MatchesParameterCount(paramCount)) {
							// Do we have a constant return type?
							if (function.Return.IsConstant)
								return function.Return.ConstantType;

							// The name of the return reference
							string returnReference = function.Return.Reference;
							// Get the list of parameters in this spec with a variable
							// reference the same as the return type,
							Expression[] paramExps = new Expression[paramCount];
							for (int i = 0; i < paramCount; ++i) {
								object param_ob = functionExp.Parameters[i];
								// If the paramater is an Expression, 
								if (param_ob is Expression) {
									// Build the array of expressions
									paramExps[i] = (Expression) param_ob;
								} else {
									// Exception if the argument isn't an expression
									throw new ApplicationException("Expected expression as argument for function " + functionName);
								}
							}
							// Matches the expression in the parameter list that are the same
							// as the return type.
							Expression[] matchedReturnParameters = function.MatchParameterExpressions(paramExps, returnReference);
							// Recurse on the matched expressions
							SqlType widestType = null;
							bool success = true;
							foreach (Expression tExp in matchedReturnParameters) {
								SqlType type = GetExpressionType(table, tExp);
								if (widestType == null) {
									widestType = type;
								} else {
									// If not comparable types, we fail matching
									if (!type.IsComparableTo(widestType)) {
										success = false;
										break;
									}
										// Comparable types, make sure we set the widest type
									else {
										widestType = type.Widest(widestType);
									}
								}
							}
							// Success?
							if (success)
								return widestType;

						}
					}
				}

				// If we get here, we didn't match
				throw new ApplicationException("Parameters for function " + functionName + " do not match the " +
				                               "specification of the function");

			} 
			if (expression.Type == ExpressionType.FetchStatic) {
				// Return the static value type
				SqlObject[] val = ((FetchStaticExpression)expression).Values;
				return val[0].Type;
			}
				
			throw new ApplicationException("Unexpected expression: " + expression.Type);
		}

		public ITable Execute(Expression expression) {
			return DoExecute(expression);
		}
		
		public static FunctionResultTable ResultTable(SqlObject[] val) {
			return new FunctionResultTable(val);
		}

		public static FunctionResultTable ResultTable(SqlObject val) {
			return new FunctionResultTable(new SqlObject[] { val });
		}

		public static bool IsFunctionResultTable(ITable source) {
			return (source is FunctionResultTable);
		}

		public static SqlObject[] Result(ITable table) {
			if (!(table is FunctionResultTable))
				throw new ArgumentException("The table must be the result of a function execution.", "table");
			
			int cols = table.Columns.Count;
			SqlObject[] result = new SqlObject[cols];
			for (int i = 0; i < cols; ++i) {
				result[i] = table.GetValue(i, new RowId(0));
			}
			return result;
		}

		#region ExpressionIndexResolver
		
		/// <summary>
		/// A <see cref="IndexResolver"/> implementation where the element 
		/// keys are based on the result of an <see cref="Expression"/> 
		/// on a table.
		/// </summary>
		class ExpressionIndexResolver : IndexResolver {
			private QueryProcessor processor;
			private ITable table;
			private Expression[] columnExps;
			private SqlType[] collationType;
			private bool[] ascending;

			public ExpressionIndexResolver(QueryProcessor processor, ITable table, 
			                               SqlType[] type, bool[] ascending_type, Expression[] column_ops) {
				this.processor = new QueryProcessor(processor);
				this.table = table;
				this.columnExps = column_ops;
				this.collationType = type;
				this.ascending = ascending_type;
				// Push the table onto the top of the processor
				this.processor.PushTable(this.table);
				// NOTE: we don't need to pop the table since we are using a processor
				//   that isn't shared with anything else.
			}

			public override int Compare(RowId rowid, SqlObject[] value) {
				SqlObject[] val1 = GetValue(rowid);
				SqlObject[] val2 = value;

				// Compare until we reach the end of the array.
				int min_compare = System.Math.Min(val1.Length, val2.Length);
				for (int i = 0; i < min_compare; ++i) {
					int c = SqlObject.Compare(val1[i], val2[i]);
					if (c != 0) {
						bool rev = ascending[i];
						return rev ? c : -c;
					}
				}

				// If the sizes are equal, compare equally,
				if (val1.Length == val2.Length)
					return 0;
				
				// If val1.length is greater, return +1, else return -1 (val1.length is
				// less)
				if (val1.Length > val2.Length) {
					return 1;
				} else {
					return -1;
				}
			}

			public override SqlObject[] GetValue(RowId rowid) {
				// Set the top of stack table row_id
				processor.UpdateTableRow(rowid);
				if (columnExps.Length == 1)
					// If a single part,
					return Result(processor.DoExecute(columnExps[0]));
					
				// TODO: We can clean this up a great deal! We should make
				//   'DoExecute' produce a table with multiple columns when a
				//   composite function is given.
				int compParts = columnExps.Length;
				// If composite,
				SqlObject[] arr = new SqlObject[compParts];
				for (int i = 0; i < compParts; ++i)
					arr[i] = Result(processor.DoExecute(columnExps[i]))[0];
				return arr;
			}
		}
		
		#endregion
	}
}