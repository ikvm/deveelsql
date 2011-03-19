using System;

using Deveel.Data.Base;
using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	public sealed partial class QueryProcessor {
		private ITable Filter(ITable table, FilterExpression op) {
			// The filter name
			string filter_name = op.Name;
			if (filter_name.Equals("single_filter"))
				return SingleFilter(table, op);
			if (filter_name.Equals("expression_table"))
				return ExpressionTableFilter(table, op);
			if (filter_name.Equals("sort"))
				return SortFilter(table, op);
			if (filter_name.Equals("static_filter"))
				return StaticFilter(table, op);
			if (filter_name.Equals("aggregate"))
				return AggregateFilter(table, op);
				
			throw new ApplicationException("Unknown filter type " + filter_name);
		}

		
		private static Expression GetTableOrderComposite(ITable table) {
			if (table is FilteredTable)
				return ((FilteredTable)table).OrderComposite;
			if (table is JoinedTableBase)
				return ((JoinedTableBase)table).OrderComposite;
				
			return null;
		}

		internal ITable FilterByScan(ITable table, Expression op) {
			// Default is to match table,
			ITable resultTable = table;

			long rowCount = table.RowCount;

			// If the table is not empty,
			if (rowCount > 0) {
				// Put the table on the stack,
				PushTable(table);

				// The working set,
				IIndex<RowId> workingSet = transaction.CreateTemporaryIndex<RowId>(rowCount);

				// TODO: Common expression scans should be optimized

				// Default scan (works for everything)
				// Fetch the table's row iterator
				IRowCursor cursor = table.GetRowCursor();

				// Wrap in a forward prefetch iterator
				cursor = new PrefetchRowCursor(cursor, table);

				// True if all matched
				bool allMatched = true;
				// For each value,
				while (cursor.MoveNext()) {
					// Fetch the next row_id from the iterator
					RowId rowid = cursor.Current;
					// Set the top of stack table row_id
					UpdateTableRow(rowid);
					
					// Execute the expression,
					ITable expResult = DoExecute(op);
					// If it's true, add the row_id to the working set
					
					if (TrueResult(expResult)) {
						// Note, we preserve the ordering of the table being filtered
						workingSet.Add(rowid);
					} else {
						// Wasn't a true result, so we didn't all match.
						allMatched = false;
					}
				}

				// If we matched nothing
				if (workingSet.Count == 0) {
					// Return a subset of the given table that is empty
					SubsetTable subsetTable = new SubsetTable(table);
					// We inherit the order composite description from the child.
					subsetTable.SetOrderCompositeIsChild();
					resultTable = subsetTable;
				}
					// If we matched something
				else {
					// If all matched we return the table
					if (allMatched) {
						// Clear the working set and set the result table
						workingSet.Clear();
						resultTable = table;
					} else {
						// Something in working set, and we didn't match everything,
						IRowCursor setCursor = new DefaultRowCursor(workingSet.GetCursor());
						SubsetTable subsetTable = new SubsetTable(table, setCursor);
						// We inherit the order composite description from the child.
						subsetTable.SetOrderCompositeIsChild();
						resultTable = subsetTable;
					}
				}

				// Pop the current table from the stack
				PopTable();
			}

			return resultTable;
		}
		
		private SubsetTable FilterByIndex(ITable table, IIndexSetDataSource index, Expression order, SelectableRange range) {
			// Select from the index and return the subset
			IRowCursor rows = index.Select(range);
			SubsetTable filteredTable = new SubsetTable(table, rows);

			filteredTable.OrderComposite = order;
			
			// If the number of rows selected from the index is the same as the
			// original table, then it is safe for index requests to fallthrough to the
			// parent.
			long selectCount = rows.Count;
			long originalCount = table.RowCount;
			if (selectCount == originalCount)
				filteredTable.IndexRequestFallthrough = true;
			
			// Assert we didn't select more than in the original context
			if (selectCount > originalCount)
				throw new ApplicationException(	"Index found more values than in parent table.");

			return filteredTable;
		}

		private ITable FilterByIndex(ITable table, IIndexSetDataSource index, Expression order, string compareFunction, SqlObject[] values) {
			// Make a selectable range set
			SelectableRange range = SelectableRange.Full;
			range = range.Intersect(SelectableRange.GetOperatorFromFunction(compareFunction), values);
			// And return the subset
			return FilterByIndex(table, index, order, range);
		}

		private ITable FilterByIndex(ITable table, IIndexSetDataSource index, Expression order, string compareFunction, SqlObject value) {
			return FilterByIndex(table, index, order, compareFunction, new SqlObject[] { value });
		}
		
		private ITable StaticFilter(ITable child, FilterExpression expression) {
			// The filter operation
			Expression filterExp = expression.Filter;
			// Execute the static expression
			SqlObject[] result = Result(DoExecute(filterExp));
			// If true,
			if (result.Length == 1 &&
				SqlObject.Compare(result[0], new SqlObject(true)) == 0)
				return child;
			
			return new SubsetTable(child);
		}
		
		private ITable AggregateFilter(ITable child, FilterExpression expression) {
			// The filter operation
			FunctionExpression filterExp = (FunctionExpression) expression.Filter;
			// The filter operation is the sort composite
			AggregateTable aggregate = new AggregateTable(child, filterExp);
			// Create an empty index for the aggregate table and initialize
			// Note: Time cost of this is a scan on 'child'
			IIndex<long> emptyIndexContainer = transaction.CreateTemporaryIndex<long>(System.Math.Max(2, child.RowCount * 2));
			aggregate.InitGroups(this, emptyIndexContainer);
			// Set the order composite
			aggregate.SetOrderCompositeIsChild();
			// Return the table
			return aggregate;
		}
		
		private ITable SortFilter(ITable table, FilterExpression expression) {
			// The filter operation which is a function that describes the sort terms
			Expression filterExp = expression.Filter;
			if (!(filterExp is FunctionExpression))
				throw new ArgumentException("Expected a function as argument to the filter.");

			ITable resultTable = table;

			// If there's something to sort,
			if (table.RowCount > 1) {
				// Get the composite function representing the sort collation,
				FunctionExpression compositeExp = (FunctionExpression) filterExp;
				if (!compositeExp.Name.Equals("composite"))
					throw new ArgumentException("Invalid composite function for sorting.");

				// The natural ordering of the child
				Expression naturalChildOrder = GetTableOrderComposite(table);
				if (naturalChildOrder != null) {
					if (naturalChildOrder.Equals(compositeExp))
						// No sort necessary, already sorted
						return table;
					
					// TODO: test for the reverse condition, which we can optimize
					//   with a reverse row iterator.
				}

				int paramCount = compositeExp.Parameters.Count;
				int termCount = paramCount / 2;
				IIndexSetDataSource rowIndex;
				bool naturalOrder = true;
				// If 1 sort term,
				if (termCount == 1) {
					Expression sortExp = (Expression) compositeExp.Parameters[0];
					naturalOrder = SqlObject.Equals((SqlObject)compositeExp.Parameters[1], SqlObject.True);
					// Get the index candidate
					string indexName = sortExp.IndexCandidate;
					TableName indexTableName = sortExp.IndexTableName;
					// Index available?
					rowIndex = GetIndex(table, indexName);
				} else {
					// Multiple terms,
					// Get the index candidate if there is one
					string indexName = compositeExp.IndexCandidate;
					TableName indexTableame = compositeExp.IndexTableName;
					// Index available?
					rowIndex = GetIndex(table, indexName);
				}

				// If we have an index,
				if (rowIndex != null) {
					IRowCursor sortedCursor = rowIndex.Select(SelectableRange.Full);
					if (!naturalOrder)
						// Reverse iterator,
						sortedCursor = new ReverseRowCursor(sortedCursor);
					
					SubsetTable sortedTable = new SubsetTable(table, sortedCursor);
					sortedTable.IndexRequestFallthrough = true;
					// Set the order composite function the describes how the subset is
					// naturally sorted.
					sortedTable.OrderComposite = (Expression) compositeExp.Clone();
					resultTable = sortedTable;
				} else {
					// NOTE: There's lots of directions we can take for optimizing this
					//  sort.  For example, if the number of values being sorted meets some
					//  criteria (such as all integers and less than 2 millions values)
					//  then the values being sorted could be read onto the heap and sorted
					//  in memory with a quick sort.

					// Scan sort,
					// The working set,
					IIndex<RowId> workingSet = transaction.CreateTemporaryIndex<RowId>(table.RowCount);
					// Create the resolver
					IndexResolver resolver = CreateResolver(table, compositeExp);
					// Iterator over the source table
					IRowCursor tableCursor = table.GetRowCursor();

					// Wrap in a forward prefetch iterator
					tableCursor = new PrefetchRowCursor(tableCursor, table);

					// Use a buffer,
					RowId[] rowIds = new RowId[128];
					while (tableCursor.MoveNext()) {
						int count = 0;
						while (tableCursor.MoveNext() && count < 128) {
							rowIds[count] = tableCursor.Current;
							++count;
						}
						for (int i = 0; i < count; ++i) {
							RowId rowid = rowIds[i];
							// Get the value,
							SqlObject[] values = resolver.GetValue(rowid);
							// Insert the record into sorted order in the working_set
							workingSet.Insert(values, rowid, resolver);
						}
					}

					// TODO: record 'workingSet' for future resolution.
					
					// The result,
					IRowCursor sortedCursor = new DefaultRowCursor(workingSet.GetCursor());
					SubsetTable sortedTable = new SubsetTable(table, sortedCursor);
					sortedTable.IndexRequestFallthrough = true;
					// Set the order composite function the describes how the subset is
					// naturally sorted.
					sortedTable.OrderComposite = (Expression) compositeExp.Clone();
					resultTable = sortedTable;
				}
			}

			return resultTable;
		}
		
		private ITable ExpressionTableFilter(ITable table, FilterExpression op) {
			// The filter operation which is a function that describes the output
			// columns
			Expression filterExp = op.Filter;
			if (filterExp.Type != ExpressionType.Function)
				throw new ApplicationException("Expected a function.");

			FunctionExpression functionExp = (FunctionExpression)filterExp;
			
			// Function name and parameter count,
			string funName = functionExp.Name;
			if (!funName.Equals("table_out"))
				throw new ArgumentException();
			
			int paramCount = functionExp.Parameters.Count;

			// Create the expression table data source
			ExpressionTable expressionTable = new ExpressionTable(table, this);

			// The number of parameters,
			for (int i = 0; i < paramCount; ++i) {
				Expression outExp = (Expression) functionExp.Parameters[i];
				// This will always be an aliasvarname with an operation child which is
				// the expression we perform for the column.
				if (!(outExp is AliasVariableNameExpression))
					throw new ApplicationException("Expected ALIASVARNAME.");
				
				// The label,
				Variable v = ((AliasVariableNameExpression)outExp).Alias;
				string label = v.Name;
				// The actual function
				Expression funExp = ((AliasVariableNameExpression) outExp).Child;
				// Work out the type,
				SqlType expType = GetExpressionType(table, funExp);
				// Add the column
				expressionTable.AddColumn(label, expType, funExp);
			}
			
			// Return the operation table
			return expressionTable;
		}
		
		private ITable SingleFilter(ITable table, FilterExpression expression) {
			// The filter operation
			Expression filterExp = expression.Filter;

			// A range set is eligible for indexes, etc
			if (filterExp.Type == ExpressionType.Function) {
				FunctionExpression functionExp = (FunctionExpression) filterExp;
				string funType = functionExp.Name;
				// Is this a range set?
				if (funType.Equals("range_set")) {
					// Get the var and the range set
					FetchVariableExpression varExp = (FetchVariableExpression) functionExp.Parameters[0];
					Variable var = varExp.Variable;
					SelectableRange rangeSet = (SelectableRange) functionExp.Parameters[1];
					// Is the var an index candidate?
					TableName indexTname = varExp.IndexTableName;
					string indexName = varExp.IndexCandidate;

					if (indexName != null) {
						// Try and get this index in the parent
						IIndexSetDataSource rowIndex = GetIndex(table, indexName);
						if (rowIndex != null) {
							// The order composite
							Expression orderComposite = expression.OrderRequired;
							return FilterByIndex(table, rowIndex, orderComposite, rangeSet);
						}
					}
					// Here if; no index found we can use, so we fall through to the scan

				} else {
					// Check to see if we have a simple expression we can use an index
					// for.  For example, 'a = ?'
					// Check the function is a simple comparison
					if (QueryPlanner.IsSimpleComparison(funType)) {
						// Do we have a var on one side and a simple static expression on the
						// other?
						Expression p0 = (Expression) functionExp.Parameters[0];
						Expression p1 = (Expression)functionExp.Parameters[1];

						Expression var_op = null;
						Expression static_op = null;
						String comparison = null;

						ExpressionType p0type = p0.Type;
						ExpressionType p1type = p1.Type;

						if (p0type == ExpressionType.FetchVariable &&
							p1type != ExpressionType.FetchVariable) {
							var_op = p0;
							static_op = p1;
							comparison = funType;
						} else if (p1type == ExpressionType.FetchVariable &&
								   p0type != ExpressionType.FetchVariable) {
							var_op = p1;
							static_op = p0;
							comparison = QueryPlanner.ReverseSimpleComparison(funType);
						}

						// Did we find an expression that is eligible?
						if (comparison != null) {
							// If the var_op is an index candidate.
							string indexName = var_op.IndexCandidate;
							TableName indexTableName = var_op.IndexTableName;
							if (indexName != null) {
								// We have an index,
								// Try and get this index in the parent
								IIndexSetDataSource rowIndex = GetIndex(table, indexName);
								if (rowIndex != null) {
									// Is the static locally static?
									if (IsLocallyStatic(table, static_op)) {
										// Resolve the static,
										ITable staticResult = DoExecute(static_op);
										// Assert we have 1 column and 1 row
										if (staticResult.RowCount != 1 &&
											staticResult.Columns.Count != 1)
											throw new ApplicationException("Static operation gave incorrectly formatted result");

										// Get the value
										IRowCursor rowCursor = staticResult.GetRowCursor();
										if (!rowCursor.MoveNext())
											throw new ApplicationException();

										RowId rowId = rowCursor.Current;
										SqlObject staticVal = staticResult.GetValue(0, rowId);
										// The order composite
										Expression orderComposite = expression.OrderRequired;
										// Perform the index lookup,
										return FilterByIndex(table, rowIndex, orderComposite, comparison, staticVal);

									} else {
										// Here if; the static is not locally static
									}
								} else {
									// Here if; the parent isn't base table with the index
									// definition
								}
							} else {
								// Here if; the var isn't an index candidate
							}
						} else {
							// Here if; there isn't a fetch var on a side or both sides are
							// fetch vars
						}

					} else {
						// TODO: Check if it's a logical operator, so we can check for
						//   constructs like 'a = 3 or a = ? or a = ?' which we can
						//   potentially convert into a pointer union between 3 index
						//   lookups.

						// Currently, fall through to a scan operation...
					}
				}
			} else {
				// Here if; the filter is not based on a function operation.
			}

			// We filter by a full scan on the table with the operation.
			return FilterByScan(table, filterExp);
		}

		private static bool IsLocallyStatic(ITable domain_table, Expression op) {
			LocalStaticGraphInspector local_static_test = new LocalStaticGraphInspector(domain_table);
			QueryOptimizer.WalkGraph(op, local_static_test);
			return local_static_test.Result;
		}

		#region LocalStaticGraphInspector

		private class LocalStaticGraphInspector : QueryOptimizer.IGraphInspector {
			private readonly ITable domainTable;
			private bool result;

			public LocalStaticGraphInspector(ITable domainTable) {
				this.domainTable = domainTable;
			}

			public bool Result {
				get { return result; }
			}

			public Expression OnBeforeWalk(Expression expression) {
				if (expression is FetchVariableExpression) {
					Variable v = ((FetchVariableExpression)expression).Variable;
					// If this variable references a table in the domain, we aren't
					// locally static,
					if (domainTable.Columns.IndexOf(v.Name) == -1)
						result = false;
				}
				return expression;
			}

			public Expression OnAfterWalk(Expression expression) {
				return expression;
			}
		}

		#endregion
	}
}