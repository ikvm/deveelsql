using System;

namespace Deveel.Data.Sql {
	partial class QueryCostModel {
		private void CostFilterExpression(Expression child_op, Expression op) {
			FilterExpression filterExp = (FilterExpression)op;

			// The filter type,
			string filterType = filterExp.Name;
			if (filterType.Equals("single_filter")) {
				CostSingleFilterExpression(child_op, filterExp);
			} else if (filterType.Equals("sort")) {
				CostSortFilterExpression(child_op, filterExp);
			} else if (filterType.Equals("static_filter")) {
				CostStaticFilterExpression(child_op, filterExp);
			} else if (filterType.Equals("aggregate")) {
				CostAggregateFilterOp(child_op, filterExp);
			} else if (filterType.Equals("expression_table")) {
				CostNoCostFilterExpression(child_op, filterExp);
			} else {
				throw new ApplicationException("Unknown filter " + filterType);
			}
		}

		private static void CostNoCostFilterExpression(Expression child, FilterExpression expression) {
			// Set the costs
			expression.CostTime = child.CostTime;
			expression.CostRows = child.CostRows;
		}

		private static void CostSortFilterExpression(Expression child, FilterExpression expression) {
			// The child cost values
			double childRows = child.CostRows;
			double childTime = child.CostTime;

			// If child has an index we can use for the sort or is already
			// sorted by the filter terms, we don't need to incur the cost of the
			// sort.
			string indexName;
			TableName indexTableName;

			// The filter operation
			Expression filter = expression.Filter;
			FunctionExpression functionExp = filter as FunctionExpression;

			// Filter must be a composite function
			if (functionExp == null || !functionExp.Name.Equals("composite"))
				throw new ApplicationException("Expected composite function.");

			// Get the terms, etc
			int paramCount = functionExp.Parameters.Count;
			int termCount = paramCount / 2;
			// If 1 sort term,
			if (termCount == 1) {
				Expression sortExp = (Expression)functionExp.Parameters[0];

				// Get the index candidate
				indexName = sortExp.IndexCandidate;
				indexTableName = sortExp.IndexTableName;
			} else {
				// Multiple terms,
				// Get the index candidate if there is one
				indexName = filter.IndexCandidate;
				indexTableName = filter.IndexTableName;
			}

			bool indexLookup = false;

			// If we have an index candidate,
			if (indexName != null) {
				// Index found, 
				// Is the child operation a table where the index is available?
				TableName indexedTable = FetchFirstIndexedTable(child);
				indexLookup = indexedTable != null && indexedTable.Equals(indexTableName);
			}

			// If no index candidate or index not available, check if the child
			// is already ordered by this composite,
			if (!indexLookup)
				indexLookup = GraphCollatedByComposite(child, filter);

			// Cost of index lookup
			if (indexLookup) {
				expression.CostTime = childTime + (BTreeLookupCost * 2.0d);
			} else {
				// Cost of sort operation with no index involved in the operation
				expression.CostTime = childTime + (childRows * BTreeLookupCost);
			}

			// Set the costs
			expression.CostRows = childRows;
		}

		private void CostSingleFilterExpression(Expression child, FilterExpression expression) {
			// The child cost values
			double childRows = child.CostRows;
			double childTime = child.CostTime;

			// The filter operation
			Expression filter = expression.Filter;

			// If the filter is a range_set function, and the child is a table
			// alias then we check for index candidates.
			string funType = (string) filter.GetArgument("name");

			// We can work out an estimate of the time cost now,
			bool indexApplicable = false;
			string indexName = null;
			TableName tableName = null;
			Expression compositeIndexExp = null;

			// Fetch the first table to which index information is applicable
			TableName firstIndexedTable = FetchFirstIndexedTable(child);
			
			if (firstIndexedTable != null) {
				// Ok, child of filter is a fetch table, so look for clauses that we
				// can use an index for

				// Get the index candidate 
				if (filter.Type == ExpressionType.Function) {
					Expression param0 = (Expression) filter.GetArgument("arg0");
					// Check if we can use an index for a range set function
					Expression varExp = null;
					if (funType.Equals("range_set")) {
						varExp = param0;
					} else {
						// Index is still applicable for parameter queries.  The operator
						// must be sufficiently simple and contain 1 variable that is an
						// index candidate.
						if (QueryPlanner.IsSimpleComparison(funType)) {
							// Does is contain 1 variable that is an index candidate?
							Expression param1 = (Expression) filter.GetArgument("arg1");
							if (param0.Type == ExpressionType.FetchVariable &&
							    param1.Type != ExpressionType.FetchVariable) {
								varExp = param0;
							} else if (param0.Type != ExpressionType.FetchVariable &&
							           param1.Type == ExpressionType.FetchVariable) {
								varExp = param1;
							}
						}
					}
					if (varExp != null) {
						indexName = varExp.IndexCandidate;
						tableName = varExp.IndexTableName;
						if (indexName != null) {
							indexApplicable = true;
							// Set the indexed ops field, which is an array of operations
							// representing the term of the index
							compositeIndexExp = FunctionExpression.Composite(varExp, true);
						}
					}
				}
			}

			// We use the index to predict worst case cost in an accurate way
			if (indexApplicable && funType.Equals("range_set")) {
				// If we have an index, and the filter is a range set, we query the index
				// directly to get worst case probability.

				// Get the variable.
				SelectableRange rangeSet = (SelectableRange) filter.GetArgument("arg1");

				// The time to perform this operation is selectable range set
				// elements * (2 * LOOKUP_COST)
				long filterTimeCost;
				if (indexApplicable) {
					// Index time cost
					filterTimeCost = rangeSet.Count() * (BTreeLookupCost * 2);

					// Notify the graph that this filter must be ordered by the terms of
					// the expression regardless of how the processor decides to solve the
					// operation.
					expression.OrderRequired = compositeIndexExp;
				} else {
					// Scan time cost
					filterTimeCost = (long)((double)childRows * 1.1);
				}

				// Have we done a size estimate already on this filter?
				long? resultSize = (long?)filter.GetArgument("result_size_lookup");
				if (resultSize == null) {
					// Fetch the index on the table
					IIndexSetDataSource rowIndex = transaction.GetIndex(tableName, indexName);
					// Do the index lookup and cost appropriately
					IRowCursor result = rowIndex.Select(rangeSet);

					resultSize = result.Count;
					filter.SetArgument("result_size_lookup", resultSize.Value);
				}

				// Row count is the worst case, either the child rows or the number of
				// elements in the index, whichever is smaller.
				double newRowCount = System.Math.Min((double)resultSize, childRows);

				// Note, this information is a very precise worst case
				expression.CostRows = newRowCount;
				expression.CostTime = childTime + filterTimeCost;
				return;
			} else {
				// This is a parameter operation eg. 'a = ?', '? > b'
				// We know we if we have an index to resolve this which we use for
				// time costing, but we don't know anything specific about the value
				// being searched.  We always assume that something will be matched.

				// The time cost of this operation
				double filterTimeCost;
				double newRowCount;
				if (indexApplicable) {
					// Index lookup
					filterTimeCost = BTreeLookupCost * 2.0d;
					// Notify the graph that this filter must be ordered by the terms of
					// the expression regardless of how the processor decides to solve the
					// operation.
					expression.OrderRequired = compositeIndexExp;
				} else {
					// Complete scan of child
					filterTimeCost = childRows * 1.1d;
				}

				// If we are a simple function
				if (QueryPlanner.IsSimpleComparison(funType)) {
					// Fetch the first variable that is locally referencable from the
					// arguments
					Variable var = null;
					Expression varExp = (Expression) filter.GetArgument("arg0");
					if (varExp.Type == ExpressionType.FetchVariable) {
						var = Dereference(expression, (Variable)varExp.GetArgument("var"));
						if (var == null) {
							varExp = (Expression) filter.GetArgument("arg1");
							if (varExp.Type == ExpressionType.FetchVariable)
								var = Dereference(expression, (Variable)varExp.GetArgument("var"));
						}
					}

					// If we can't dereference it, assume worst case
					if (var == null) {
						newRowCount = childRows;
					} else {
						// No index, so defer to a probability estimate,
						double? cachedProbability = (double?)filter.GetArgument("result_probability");
						if (cachedProbability == null) {
							// Get the column statistics object for this
							ColumnStatistics col_stats = transaction.GetColumnStatistics(var);
							// Estimated probability of the given function truth over a sample
							// of the data.
							cachedProbability = col_stats.ProbabilityEstimate(funType);
							filter.SetArgument("result_probability", cachedProbability.Value);
						}

						double predictedRowCount = childRows * cachedProbability.Value;
						// Round up.
						newRowCount = predictedRowCount + 1;
					}
				} else if (funType.Equals("range_set")) {
					// If we are a range_set

					// Get the variable.
					Expression varExp = (Expression) filter.GetArgument("arg0");
					SelectableRange rangeSet = (SelectableRange)filter.GetArgument("arg1");

					// Get the var,
					Variable var = (Variable) varExp.GetArgument("var");

					// Dereference this variable
					var = Dereference(expression, var);

					// If we can't dereference it, assume worst case
					if (var == null) {
						newRowCount = childRows;
					} else {
						double probability;
						// If the var is an index candidate,
						indexName = varExp.IndexCandidate;
						tableName = varExp.IndexTableName;

						if (indexName != null) {
							// There's an index we can use!
							// Fetch the index on the table
							IIndexSetDataSource rowIndex = transaction.GetIndex(tableName, indexName);
							// Have we done a size estimate already on this filter?
							long? resultSize = (long?)filter.GetArgument("result_size_lookup");
							if (resultSize == null) {
								// Do the index lookup and cost appropriately
								IRowCursor result = rowIndex.Select(rangeSet);

								resultSize = result.Count;
								filter.SetArgument("result_size_lookup", resultSize.Value);
							}

							// Calculate the probability,
							long indexSize = rowIndex.Select(SelectableRange.Full).Count;
							if (indexSize > 0) {
								probability = (double)resultSize / indexSize;
							} else {
								probability = 0;
							}
						} else {
							// No index, so defer to a probability estimate,
							double? cached_probability = (double?)filter.GetArgument("result_probability");
							if (cached_probability == null) {
								// Get the column statistics object for this
								ColumnStatistics col_stats = transaction.GetColumnStatistics(var);

								// Estimated probability of the given function truth over a
								// sample of the data.
								cached_probability = col_stats.ProbabilityEstimate(rangeSet);
								filter.SetArgument("result_probability", cached_probability.Value);
							}
							probability = cached_probability.Value;
						}

						double predictedRowCount = childRows * probability;

						// Round up.
						newRowCount = predictedRowCount + 1;
					}
				} else {
					// Otherwise not a simple function, and can't really predict anything
					// about this.

					// Assume worst case,
					newRowCount = childRows;
				}

				// Set the costs
				expression.CostRows = newRowCount;
				expression.CostTime = childTime + filterTimeCost;
				return;
			}
		}

		private static void CostStaticFilterExpression(Expression child, FilterExpression expression) {
			// The child cost values
			double childRows = child.CostRows;
			double childTime = child.CostTime;

			// The filter operation
			Expression filter = expression.Filter;
			double estimatedChildRows = childRows;

			// If it's a fetch static,
			if (filter is FetchStaticExpression) {
				SqlObject[] val = ((FetchStaticExpression)filter).Values;
				// If not true, the filter will filter all,
				bool isTrue = false;
				if (val[0].Type.IsBoolean) {
					bool? b = val[0].Value.ToBoolean();
					if (b.HasValue && b.Value.Equals(true))
						isTrue = true;
				}
				if (!isTrue) {
					estimatedChildRows = 0.0d;
				}
			}

			// Set the time cost
			expression.CostRows = estimatedChildRows;
			expression.CostTime = childTime;
		}

		private static void CostAggregateFilterOp(Expression child, FilterExpression expression) {
			// The child cost values
			double childRows = child.CostRows;
			double childTime = child.CostTime;

			// TODO: We should check for full range aggregate, in which case we
			//   know there will only be 1 row result.

			// Set the costs
			expression.CostTime = childTime + (childRows * 1);
			expression.CostRows = childRows;
		}
	}
}