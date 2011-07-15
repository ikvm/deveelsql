using System;
using System.Collections.Generic;

using Deveel.Data.Sql;

namespace Deveel.Data.Sql {
	partial class QueryCostModel {
		private readonly SystemTransaction transaction;
		private readonly QueryProcessor processor;

		private const long BTreeLookupCost = 5;
		
		public QueryCostModel(SystemTransaction transaction) {
			this.transaction = transaction;
			processor = new QueryProcessor(transaction);
		}

		private ITable ExecuteExpression(Expression expression) {
			// Note that this does not preserve query stack information.
			return processor.Execute(expression);
		}

		private static Variable Dereference(Expression graph, Variable v) {
			if (graph is FilterExpression)
				// Recurse to the child
				return Dereference(((FilterExpression)graph).Child, v);

			if (graph is JoinExpression) {
				JoinExpression joinExp = (JoinExpression) graph;

				// Recurse the left and right nodes
				Variable v2 = Dereference(joinExp.Left, v);
				if (v2 == null || !v2.Equals(v))
					return v2;

				return Dereference(joinExp.Right, v);
			} 
			
			if (graph is AliasTableNameExpression) {
				AliasTableNameExpression aliasExp = (AliasTableNameExpression) graph;
				if (aliasExp.Alias.Equals(v.TableName)) {
					// Ok, match found, table name is in the child
					Expression child = aliasExp.Child;
					if (child is FetchTableExpression) {
						TableName newTableName = ((FetchTableExpression)child).TableName;
						return new Variable(newTableName, v.Name);
					}
					
					// Return null if the reference is to a nested query
					return null;
				}
					
				return null;
			}

			if (graph is FunctionExpression)
				return null;
				
			throw new ApplicationException("Unknown operation type in graph.");
		}

		private static bool IsCompletelySourcedIn(Expression op, IList<TableName> sources) {
			// Assume true
			bool[] completelySourced = new bool[1];
			completelySourced[0] = true;
			QueryOptimizer.WalkGraph(op, new SourceCompleteChecker(sources, completelySourced));
			return completelySourced[0];
		}

		private static bool LeftRightComparisonPairs(Expression expression, IList<String> functionType, 
			IList<Expression> left, IList<Expression> right, 
			IList<TableName> leftSources, IList<TableName> rightSources) {

			if (!(expression is FunctionExpression))
				throw new ArgumentException();

			FunctionExpression functionExp = (FunctionExpression) expression;
			string funType = functionExp.Name;
			Expression p0 = (Expression) functionExp.Parameters[0];
			Expression p1 = (Expression) functionExp.Parameters[1];

			// For simple comparison functions, we determine which is the left and
			// right expression and add the entries as appropriate.
			if (QueryPlanner.IsSimpleComparison(funType)) {
				// If the pairs completely source 
				if (IsCompletelySourcedIn(p0, leftSources)) {
					if (IsCompletelySourcedIn(p1, rightSources)) {
						left.Add(p0);
						right.Add(p1);
						functionType.Add(funType);
						return true;
					}
				} else if (IsCompletelySourcedIn(p1, leftSources)) {
					if (IsCompletelySourcedIn(p0, rightSources)) {
						left.Add(p1);
						right.Add(p0);
						// We add the reversed operation
						functionType.Add(QueryPlanner.ReverseSimpleComparison(funType));
						return true;
					}
				}
				return false;
			}
			
			// If this is a logical and, then we recurse looking for groups,
			if (funType.Equals("and")) {
				// Recurse
				return LeftRightComparisonPairs(p0, functionType, left, right, leftSources, rightSources) &&
				       LeftRightComparisonPairs(p1, functionType, left, right, leftSources, rightSources);
			}

			// Any other type of function we return false (not valid simple pair).	
			return false;
		}

		private IndexKey FindIndexOn(IList<Expression> expressions, Expression expression) {
			// expressions must all be simple fetch variable operations
			TableName tableName = null;
			List<Variable> vars = new List<Variable>(expressions.Count);
			for (int i = 0; i < expressions.Count; ++i) {
				Expression var_op = expressions[i];
				if (var_op is FetchVariableExpression) {
					Variable v = ((FetchVariableExpression)var_op).Variable;
					v = Dereference(expression, v);
					// If can't dereference, then return null
					if (v == null)
						return null;

					if (tableName == null) {
						tableName = v.TableName;
					} else if (!tableName.Equals(v.TableName)) {
						// If not a common table name, return null
						return null;
					} else if (vars.Contains(v)) {
						// If repeat vars, index not possible
						return null;
					}
					vars.Add(v);
				} else {
					return null;
				}
			}

			// Single case (easy)
			if (vars.Count == 1) {
				IndexKey indexVal = new IndexKey();
				indexVal.IndexTable = expressions[0].IndexTableName;
				indexVal.IndexName = expressions[0].IndexCandidate;
				return indexVal;
			}

			// ok, common table name and all the expressions are variables

			IIndexSetDataSource[] indexInfo = transaction.GetTableIndexes(tableName);
			for (int i = 0; i < indexInfo.Length; ++i) {
				// Get the collation
				IndexCollation collation = indexInfo[i].Collation;
				// Matches?
				if (collation.Columns.Length == vars.Count) {
					bool match = true;
					foreach (Variable v in vars) {
						if (!collation.ContainsColumn(v.Name)) {
							match = false;
							break;
						}
					}
					// Found a match, so return the index name
					if (match) {
						IndexKey indexVal = new IndexKey();
						indexVal.IndexTable = tableName;
						indexVal.IndexName = indexInfo[i].Name;
						return indexVal;
					}
				}
			}

			// No index discovered
			return null;
		}

		private static TableName FetchFirstIndexedTable(Expression graph) {
			if (graph is AliasTableNameExpression)
				// Recurse if alias table name
				return FetchFirstIndexedTable(((AliasTableNameExpression)graph).Child);

			if (graph is FetchTableExpression)
				// Index available, (assuming the index name is an index on this table).
				return ((FetchTableExpression)graph).TableName;				

			if (graph is FilterExpression) {
				FilterExpression filterExp = (FilterExpression) graph;

				// Certain filters do not destroy index information from their child,
				string filterName = filterExp.Name;
				// Static filter does not destroy index information, so recurse on this.
				return filterName.Equals("static_filter") ? FetchFirstIndexedTable(filterExp.Child) : null;
			}
				
			return null;
		}

		private static bool IsCostWorse(double time, Expression op) {
			return (op.CostTime > time);
		}

		private static bool GraphCollatedByComposite(Expression graph, Expression composite) {
			if (graph is JoinExpression) {
				JoinExpression joinExp = (JoinExpression) graph;

				// We only look at the left,
				// A join can not change the ordering from the left branch (it may add
				// new elements, but the order remains the same).
				Expression left = joinExp.Left;
				return GraphCollatedByComposite(left, composite);
			} 

			if (graph is FilterExpression) {
				FilterExpression filterExp = (FilterExpression) graph;

				// Filter operation, do we have an order required field on this filter?
				Expression orderRequired = filterExp.OrderRequired;
				// If not, we recurse to the child,
				if (orderRequired == null) {
					Expression child = filterExp.Child;
					return GraphCollatedByComposite(child, composite);
				}
					
				// Otherwise, compare composites,
				return composite.Equals(orderRequired);
			}

			// Otherwise, unknown collation
			return false;
		}
		
		public void ClearGraph(Expression expression) {
			// The graph should only have 'FETCHTABLE', 'JOIN', 'FILTER' and 'FUNCTION'
			// operations in the graph.  The FUNCTION expressions are themselves other
			// costed operation graphs.
			if (expression is FilterExpression) {
				// Recurse to the child
				ClearGraph(((FilterExpression)expression).Child);
			} else if (expression is JoinExpression) {
				JoinExpression joinExp = (JoinExpression) expression;
				// Recurse the left and right nodes
				ClearGraph(joinExp.Left);
				ClearGraph(joinExp.Right);
			} else if (expression is AliasTableNameExpression) {
			} else if (expression is FunctionExpression) {
				// This does not have costing information we should clear
				return;
			} else {
				throw new ApplicationException("Unknown operation type in graph.");
			}

			// Clear the costing information
			expression.UnsetCost();
		}
		
		public void Cost(Expression expression, double currentBestTime, int[] walkIteration) {
			// If this already has costing information, return
			if (expression.IsCostSet)
				return;

			++walkIteration[0];

			if (expression is FilterExpression) {
				// Cost the child
				Expression childExp = ((FilterExpression)expression).Child;
				Cost(childExp, currentBestTime, walkIteration);
				if (!childExp.IsCostSet ||
					IsCostWorse(currentBestTime, childExp)) {
					return;
				}

				// Cost the filter operation
				CostFilterExpression(childExp, expression);
			} else if (expression is JoinExpression) {
				JoinExpression joinExp = (JoinExpression) expression;

				// Cost the left and right operations
				Expression left = joinExp.Left;
				Expression right = joinExp.Right;

				Cost(left, currentBestTime, walkIteration);
				if (!left.IsCostSet || IsCostWorse(currentBestTime, left))
					return;

				Cost(right, currentBestTime, walkIteration);
				if (!right.IsCostSet || IsCostWorse(currentBestTime, right))
					return;

				// Cost the join operation
				CostJoinExpression(left, right, joinExp);
			} else if (expression is AliasTableNameExpression) {
				// Fetch the table, apply the alias, and update the cost information.
				// The cost in time is 0 for a fetch operation because no scan operations
				// are necessary.
				ITable table = ExecuteExpression(expression);
				expression.CostTime = 0;
				expression.CostRows = table.RowCount;
			} else if (expression is FunctionExpression) {
				// Function should already be costed
				return;
			} else {
				throw new ApplicationException("Unrecognized operation type");
			}
		}

		public static Expression DereferenceExpression(Expression graph, Expression expression) {
			if (expression is FetchVariableExpression) {
				Variable v = Dereference(graph, ((FetchVariableExpression)expression).Variable);
				return v == null ? null : new FetchVariableExpression(v);
			}

			if (expression is FunctionExpression) {
				FunctionExpression functionExp = (FunctionExpression) expression;
				string functionName = functionExp.Name;

				Expression p0 = DereferenceExpression(graph, (Expression)functionExp.Parameters[0]);
				if (p0 == null)
					return null;

				Expression p1 = DereferenceExpression(graph, (Expression)functionExp.Parameters[1]);
				if (p1 == null)
					return null;

				return new FunctionExpression(functionName, new Expression[] { p0, p1 });
			}
				
			throw new ApplicationException("Unexcepted operation");
		}

		#region SourceCompleteChecker

		private class SourceCompleteChecker : QueryOptimizer.IGraphInspector {
			private readonly bool[] completelySourced;
			private readonly IList<TableName> sources;

			public SourceCompleteChecker(IList<TableName> sources, bool[] completelySourced) {
				this.sources = sources;
				this.completelySourced = completelySourced;
			}

			public Expression OnBeforeWalk(Expression expression) {
				if (expression is FetchVariableExpression) {
					Variable v = ((FetchVariableExpression)expression).Variable;
					if (!sources.Contains(v.TableName)) 
						completelySourced[0] = false;
				}

				return expression;
			}

			public Expression OnAfterWalk(Expression expression) {
				return expression;
			}
		}

		#endregion

		#region IndexKey

		private sealed class IndexKey {
			public TableName IndexTable;
			public string IndexName;
		}

		#endregion
	}
}