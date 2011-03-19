using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	partial class QueryCostModel {
		private void CostJoinExpression(Expression left, Expression right, JoinExpression joinExpression) {
			// Get the left and right row count
			double leftRows = left.CostRows;
			double rightRows = right.CostRows;

			// The time scan iteration cost up to this point
			double costTime = left.CostTime + right.CostTime;

			// join type
			JoinType joinType = joinExpression.JoinType;

			if (joinType == JoinType.Cartesian) {
				// The cost of a cartesian join is nothing in addition to the right and
				// left cost (it's a simple matter to map tables into a cartesian join),
				// however the number of rows multiply.

				double rowSize = (leftRows * rightRows);
				joinExpression.CostRows = rowSize;
				joinExpression.CostTime = costTime;
			} else if (joinType == JoinType.Inner || 
				joinType == JoinType.OuterLeft) {

				double rowResult;

				// Get the filter expression
				Expression filter = joinExpression.Filter;

				// Test if the filter expression is simple enough that we can use it in
				// a scan and search expression on the right table.  An expression is
				// sufficiently simple when the search side (right side) is nothing more
				// than a variable reference, and the function is a simple comparison.
				// Otherwise, the join will be the cost of a cartesian join plus a scan
				// on the result.

				// True if the expression is a simple comparison and the parameters
				// reference the left and right sources respectively.
				bool isSimpleRelation = false;
				List<Expression> leftVarExps = new List<Expression>(4);
				List<Expression> rightVarExps = new List<Expression>(4);
				List<String> functionTypes = new List<string>(4);

				if (filter is FunctionExpression) {
					// Filter is a function.  What this code does is make a list of 
					// expressions that source to the left and right branches respectively
					// in the filter expression.  For example, consider left/right branch
					// T1 and T2, given the expression 'T2.a=T1.a' this will put T1.a in
					// the left list, T2.a in the right list.  This also works with
					// equi groups, for example, (T1.a = T2.a AND T1.b = T2.b).

					List<TableName> leftSources = new List<TableName>();
					List<TableName> rightSources = new List<TableName>();

					QueryPlanner.PopulateSourceList(leftSources, new List<Expression>(), left);
					QueryPlanner.PopulateSourceList(rightSources, new List<Expression>(), right);

					// Groups all the operations that source to the left and right
					// respectively.
					bool valid = LeftRightComparisonPairs(filter, functionTypes, leftVarExps, rightVarExps, leftSources, rightSources);

					// NOTES:
					//  xxxVarExps can contain anything, including statics, nested
					//  queries, etc.  eg. consider (T1.A = T2.A AND T1.A + 2 = T2.B).
					if (valid) {
						// If there is more than one functionTypes, they must all be
						// of the same equivalence
						if (functionTypes.Count > 1) {
							if (!QueryPlanner.IsSimpleEquivalence(functionTypes[0]))
								valid = false;

							for (int i = 1; i < functionTypes.Count; ++i) {
								string tFunType = functionTypes[i];
								if (!tFunType.Equals(functionTypes[0]))
									valid = false;
							}
						}

						// If still valid
						if (valid) {
							// Passed the test for a simple relation expression. This means we are
							// either a single simple comparison expression, or we are the
							// interesection (logical AND) of a group of equivalence functions.
							// Also, we have a distinct group of left and right joining
							// conditions.
							isSimpleRelation = true;
						}
					}
				}

				// If it's not a simple relation expression,
				if (!isSimpleRelation) {
					// Mark the join up as a simple relation expression
					joinExpression.SetArgument("cartesian_scan", "true");

					// These joins are nasty - we'd need to find the cartesian product and
					// scan on the result.
					double complexJoinCost = (leftRows * rightRows) * 1.1d;

					costTime = costTime + complexJoinCost;

					// Work out the probability of the filter_op being true for this set.

					// Assume worse case,
					rowResult = leftRows * rightRows;

				} else {
					// If it's a simple relation expression,

					// Mark the join as a simple relation expression
					joinExpression.IsSimpleRelation = true;

					// Record state information with this item
					joinExpression.SetArgument("!left_var_exps", leftVarExps.AsReadOnly());
					joinExpression.SetArgument("!right_var_exps", rightVarExps.AsReadOnly());
					joinExpression.SetArgument("!function_types", functionTypes.AsReadOnly());

					bool addRightPrepareCost = true;

					// The cost is dependant on the indexes available and which we use.
					// If all rightVarExps are fetch vars and there's a multi-column
					// index, we cost for that.  If there's partial indexes we can use
					// then the cost must reflect that.

					// Is the right a fetch table op?
					if (right is AliasTableNameExpression) {
						// Yes, so this is a candidate for using an index if there is one
						// Look for the multi-column index
						IndexKey idxKey = FindIndexOn(rightVarExps, right);
						TableName rightIndexTableName = idxKey.IndexTable;
						string rightIndexName = idxKey.IndexName;

						// If there are none and this is multi-column, we try and pick the
						// first index (this is not really a very good heuristic).  The
						// processor will choose the best index.
						if (rightVarExps.Count > 1) {
							for (int i = 0; rightIndexName == null && i < rightVarExps.Count; ++i) {
								Expression varExp = rightVarExps[i];
								if (varExp is FetchVariableExpression) {
									rightIndexName = varExp.IndexCandidate;
									rightIndexTableName = varExp.IndexTableName;
								}
							}
						}
						// If we found an index, we don't prepare right
						if (rightIndexName != null) {
							addRightPrepareCost = false;
							// The index to use
							joinExpression.SetArgument("use_right_index", rightIndexName);
							joinExpression.SetArgument("use_right_index_table_name", rightIndexTableName);
						}

					} 
						
					// Is right sorted by 'rightVarExps'?
					// TODO:

					// A scan join will always scan the left table, lookup values in the
					// right table.  The factors that change the time cost of this expression
					// is when there is an applicable index that can be used by one or more
					// of the expressions, or when operations on the right table have left
					// an ordering that is useful for the join.
					// If there is no index or convenient ordering, then the cost of the
					// join is the cost of a sort or hash on the right table in addition to
					// the regular cost.

					// Cost time is a scan on the left table plus lookup cost for each left
					// element on the right table.
					double joinCost = (leftRows * 1.1) + (leftRows * BTreeLookupCost * 2.0d);
					double rightPrepareCost = rightRows * BTreeLookupCost;


					// The cost calculation
					costTime = costTime + joinCost;
					if (addRightPrepareCost) {
						costTime = costTime + rightPrepareCost;
					}

					// TODO:
					// Estimate the number of results returned by this expression. We
					// know that leftVarExp and rightVarExp are valid, so we can build
					// some statistical basis of a search provided the left and right
					// operations are sufficiently simple.

					// Is the filter expression sufficiently simple that we can make it
					// into a database fact?
					rowResult = -1d;

					// Does the filter have a fact id?
					string factId = (string)filter.GetArgument("fact_id");
					if (factId != null) {
						FactStatistics facts = transaction.FactStatistics;
						// Do we have any historical data?
						if (facts.FactSampleCount(factId) > 0) {
							// What is the truth probability of this fact?
							double prob = facts.ProbabilityEstimate(factId);
							// The probability of the cartesian product.  We set a min of 3
							// rows.
							rowResult = ((leftRows * rightRows) * prob);
							if (rowResult < 3.0d)
								rowResult = 3.0d;
						}
					}

					// If no results from fact statistics, we use a general heuristic to
					// find the result.
					if (rowResult < 0d) {
						// For the moment, we use a general heuristic to determine the
						// probability of the result, where equi joins results in less
						// results.
						// Of course, an equi join can be a cartesian join when all values
						// match, however in most useful systems an equivalence test, at
						// worse, will match as many values as the larger table.
						string relationType = functionTypes[0];
						if (QueryPlanner.IsSimpleEquivalence(relationType)) {
							// Somewhere between left rows and right rows
							double v = rightRows - leftRows;
							if (v < 0)
								v = -v;
							v = v / 3;
							rowResult = System.Math.Min(leftRows, rightRows) + v;
						} else {
							// If not equivalance, then we assume a result that's a little less
							// than cartesian.
							rowResult = (leftRows * rightRows) * 0.85d;
						}
					}
				}

				// None stat estimated cost
				joinExpression.CostRows = rowResult;
				joinExpression.CostTime = costTime;
			} else {
				throw new ApplicationException("Unknown join type " + joinType);
			}
		}
	}
}