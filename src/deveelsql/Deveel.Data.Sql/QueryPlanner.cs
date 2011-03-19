using System;
using System.Collections.Generic;

using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	class QueryPlanner {
		private readonly SystemTransaction transaction;
		
		private Expression joinGraph;
		private Expression filterGraph;
		private Expression sortComposite;
		private Random random;
		private readonly QueryProcessor simpleProcessor;

		private readonly List<TableName> sourceList = new List<TableName>();
		private readonly List<Expression> sourceListExps = new List<Expression>();

		private readonly static List<string> simpleFunctions = new List<string>();
		private readonly static Dictionary<String, String> simpleComparisonMap = new Dictionary<string, string>();
		private readonly static List<String> simpleEquivalence = new List<string>();
		private readonly static List<string> simpleLogical = new List<string>();
		private readonly static List<string> simpleArithmetic = new List<string>();
		private readonly static List<String> simpleSetComparison = new List<string>();

		public QueryPlanner(SystemTransaction transaction) {
			this.transaction = transaction;
			random = new Random();
			simpleProcessor = new QueryProcessor(transaction);
		}

		static QueryPlanner() {
			// All simple functions
			simpleFunctions.Add("@add_sql");
			simpleFunctions.Add("@sub_sql");
			simpleFunctions.Add("@mul_sql");
			simpleFunctions.Add("@div_sql");
			simpleFunctions.Add("@and_sql");
			simpleFunctions.Add("@or_sql");
			simpleFunctions.Add("@not_sql");
			simpleFunctions.Add("@gt_sql");
			simpleFunctions.Add("@lt_sql");
			simpleFunctions.Add("@gte_sql");
			simpleFunctions.Add("@lte_sql");
			simpleFunctions.Add("@eq_sql");
			simpleFunctions.Add("@neq_sql");
			simpleFunctions.Add("@is_sql");
			simpleFunctions.Add("@isn_sql");

			simpleFunctions.Add("@cast");

			// comparison operator map (to reverse)
			simpleComparisonMap.Add("@gt_sql", "@lt_sql");
			simpleComparisonMap.Add("@lt_sql", "@gt_sql");
			simpleComparisonMap.Add("@gte_sql", "@lte_sql");
			simpleComparisonMap.Add("@lte_sql", "@gte_sql");
			simpleComparisonMap.Add("@eq_sql", "@eq_sql");
			simpleComparisonMap.Add("@neq_sql", "@neq_sql");
			simpleComparisonMap.Add("@is_sql", "@is_sql");
			simpleComparisonMap.Add("@isn_sql", "@isn_sql");

			// equivalence operators
			simpleEquivalence.Add("@eq_sql");
			simpleEquivalence.Add("@is_sql");

			// logical operators
			simpleLogical.Add("@and_sql");
			simpleLogical.Add("@or_sql");

			// Arithmatic operators
			simpleArithmetic.Add("@add_sql");
			simpleArithmetic.Add("@sub_sql");
			simpleArithmetic.Add("@mul_sql");
			simpleArithmetic.Add("@div_sql");

			// Set operators
			simpleSetComparison.Add("@anygt_sql");
			simpleSetComparison.Add("@anylt_sql");
			simpleSetComparison.Add("@anygte_sql");
			simpleSetComparison.Add("@anylte_sql");
			simpleSetComparison.Add("@anyeq_sql");
			simpleSetComparison.Add("@anyneq_sql");
			simpleSetComparison.Add("@allgt_sql");
			simpleSetComparison.Add("@alllt_sql");
			simpleSetComparison.Add("@allgte_sql");
			simpleSetComparison.Add("@alllte_sql");
			simpleSetComparison.Add("@alleq_sql");
			simpleSetComparison.Add("@allneq_sql");
		}

		private static Variable GetSingleVariable(Expression expression) {
			if (expression is FunctionExpression &&
				((FunctionExpression)expression).Name.Equals("range_set")) {
				return ((FetchVariableExpression)((FunctionExpression)expression).Parameters[0]).Variable;
			}
			return null;
		}

		private static Variable AsVariable(Expression op) {
			if (op is FetchVariableExpression)
				return ((FetchVariableExpression)op).Variable;
			return null;
		}

		private static void SplitByFunction(IList<Expression> list, Expression graph, string fun_name) {
			if (graph == null)
				return;

			if (graph is FunctionExpression) {
				FunctionExpression functionExp = (FunctionExpression) graph;
				if (functionExp.Name.Equals(fun_name)) {
					SplitByFunction(list, (Expression) functionExp.Parameters[0], fun_name);
					SplitByFunction(list, (Expression) functionExp.Parameters[1], fun_name);
					return;
				}
			}
			list.Add(graph);
		}

		private ITable FetchTable(Expression op, TableName tname) {
			if (op is JoinExpression) {
				JoinExpression joinExp = (JoinExpression) op;
				ITable table = FetchTable(joinExp.Left, tname);
				if (table != null) 
					return table;
				return FetchTable(joinExp.Right, tname);
			}
			if (op is AliasTableNameExpression) {
				AliasTableNameExpression aliasExp = (AliasTableNameExpression) op;
				TableName n = aliasExp.Alias;
				if (n.Equals(tname)) {
					// Look at the child
					Expression child_op = aliasExp.Child;
					if (child_op is FetchTableExpression) {
						return transaction.GetTable(((FetchTableExpression)child_op).TableName);
					}
				}
				return null;
			}
			
			throw new ApplicationException("Unexpected operation");
		}

		private Expression SimplifyTerm(Expression expression) {
			if (expression is FunctionExpression) {
				FunctionExpression functionExp = (FunctionExpression) expression;

				// Get the function name
				string functionName = functionExp.Name;
				// If the function is an object comparator,
				if (IsSimpleComparison(functionName)) {
					// First try and simplify,
					Expression p1 = SimplifyTerm((Expression) functionExp.Parameters[0]);
					Expression p2 = SimplifyTerm((Expression) functionExp.Parameters[1]);
					// Is one of the terms is a fetchvar and the other is static, then
					// we can make a selectable range set function.
					Variable v1 = AsVariable(p1);
					Variable v2 = AsVariable(p2);
					SqlObject[] r1 = EvaluateExpression(p1);
					SqlObject[] r2 = EvaluateExpression(p2);

					Variable v;
					SqlObject[] ob;
					string rangeOp;
					if (v1 != null && r2 != null) {
						v = v1;
						ob = r2;
						rangeOp = functionName;
					} else if (v2 != null && r1 != null) {
						// We need to reverse the operator in this case
						v = v2;
						ob = r1;
						rangeOp = ReverseSimpleComparison(functionName);
					} else {
						// if both are static, evaluate and return the operation
						FunctionExpression new_fun = new FunctionExpression(functionName, new Expression[] { p1, p2 });
						if (r1 != null && r2 != null)
							return new FetchStaticExpression(EvaluateExpression(new_fun));
						
						// Can't simplify	
						return new_fun;
					}

					// Make the range set,
					SelectableRange rangeSet = SelectableRange.Full;
					rangeSet = rangeSet.Intersect(SelectableRange.GetOperatorFromFunction(rangeOp), ob);
					FunctionExpression funExp = new FunctionExpression("range_set");
					funExp.Parameters.Add(new FetchVariableExpression(v));
					funExp.Parameters.Add(rangeSet);
					funExp.SetArgument("full_range_object", ob);
					return funExp;
				}

				if (IsSimpleLogical(functionName)) {
					// either and/or
					// If it's a logical operation
					Expression p1 = SimplifyTerm((Expression) functionExp.Parameters[0]);
					Expression p2 = SimplifyTerm((Expression) functionExp.Parameters[1]);
					// Can we combine the terms?
					Variable v1 = GetSingleVariable(p1);
					Variable v2 = GetSingleVariable(p2);

					// If one of the terms is a range set
					bool success;
					Expression otherTerm = null;
					if (v1 != null) {
						Variable v = v1;
						FunctionExpression rangeSetTerm = (FunctionExpression) p1;
						otherTerm = p2;
						success = AttemptRangeSetMerge(v, rangeSetTerm, otherTerm, functionName);
					} else if (v2 != null) {
						Variable v = v2;
						FunctionExpression rangeSetTerm = (FunctionExpression) p2;
						otherTerm = p1;
						success = AttemptRangeSetMerge(v, rangeSetTerm, otherTerm, functionName);
					} else {
						// Neither left or right is a 'range_set'
						success = false;
					}

					// Try and merge the range set term with the other term
					// If it was a success, then we return the other term,
					if (success)
						return otherTerm;

					// Otherwise, look for a static term,

					// If one of the terms is a static,
					SqlObject[] r1 = EvaluateExpression(p1);
					SqlObject[] r2 = EvaluateExpression(p2);
					SqlObject[] staticVal = null;
					Expression nonStaticVal = null;
					if (r1 != null && r2 != null) {
						// Both are static, so evaluate and return
						FunctionExpression newFun = new FunctionExpression(functionName, new Expression[] {p1, p2});
						return new FetchStaticExpression(EvaluateExpression(newFun));
					}
					if (r1 != null) {
						staticVal = r1;
						nonStaticVal = p2;
					} else if (r2 != null) {
						staticVal = r2;
						nonStaticVal = p1;
					}
					// If one of the sides is static,
					if (staticVal != null) {
						// If it's a single value,
						if (staticVal.Length == 1) {
							SqlObject tv = staticVal[0];
							if (tv.Type.IsBoolean) {
								bool? valb = tv.Value.ToBoolean();
								if (valb != null) {
									// If it's true
									if (valb == true)
										return functionName.Equals("or") ? new FetchStaticExpression(staticVal) : nonStaticVal;
										
									// If it's false
									if (valb == false)
										return functionName.Equals("or") ? nonStaticVal : new FetchStaticExpression(staticVal);
								}
							}
						} else {
							// If it's not true of false
							return functionName.Equals("or") ? nonStaticVal : new FetchStaticExpression(SqlObject.MakeNull(SqlType.GetSqlType(typeof(bool))));
						}
					}

					// Ok, neither side static,

					// If one of the sides is a range_set, then we search the other side
					// of the tree for a potential term to merge with.

					return new FunctionExpression(functionName, new Expression[] {p1, p2});
				}

				// Perhaps we can simplify a static operation?
				// Simplify the terms of the functions arguments,
				int sz = functionExp.Parameters.Count;
				object[] newParams = new object[sz];
				bool allStaticParams = true;
				for (int i = 0; i < sz; ++i) {
					object ob = functionExp.Parameters[i];
					if (ob != null && ob is Expression) {
						Expression paramExp = (Expression)ob;
						paramExp = SimplifyTerm(paramExp);
						newParams[i] = paramExp;
						if (!(paramExp is FetchStaticExpression))
							allStaticParams = false;
					} else {
						newParams[i] = ob;
					}
				}

				// If the function graph is static, evaluate it and return the fetch
				// static operation
				FunctionExpression funcExp = new FunctionExpression(functionName);
				for (int i = 0; i < sz; ++i) {
					funcExp.Parameters.Add(newParams[i]);
				}
				if (allStaticParams &&
					IsSimpleEvaluable(functionName)) {
					return new FetchStaticExpression(EvaluateExpression(funcExp));
				}

				return funcExp;
			}
			return expression;
		}

		private static bool AttemptRangeSetMerge(Variable v, FunctionExpression rangeSetTerm, Expression toMergeWith, string logicalOpName) {
			if (toMergeWith is FunctionExpression) {
				FunctionExpression functionExp = (FunctionExpression) toMergeWith;

				// If the type is the same logical operation, we recurse
				string funType = functionExp.Name;
				if (funType.Equals(logicalOpName)) {
					// Recurse condition, we try left and right merge
					// We attempt left and right param
					if (AttemptRangeSetMerge(v, rangeSetTerm, (Expression)functionExp.Parameters[0], logicalOpName)) 
						return true;

					return AttemptRangeSetMerge(v, rangeSetTerm, (Expression) functionExp.Parameters[1], logicalOpName);
				}
				
				// If it's a range set,
				if (funType.Equals("range_set")) {
					// Get the var
					Variable targetVariable = ((FetchVariableExpression) functionExp.Parameters[0]).Variable;

					// If they match, we merge
					if (v.Equals(targetVariable)) {
						// Get the range sets
						SelectableRange rangeSet1 = (SelectableRange) functionExp.Parameters[1];
						SelectableRange rangeSet2 = (SelectableRange) rangeSetTerm.Parameters[1];
						// Make sure the range types are the same
						SqlObject[] ob1 = (SqlObject[])toMergeWith.GetArgument("full_range_object");
						SqlObject[] ob2 = (SqlObject[])rangeSetTerm.GetArgument("full_range_object");

						if (ob1.Length != 1 || ob2.Length != 1)
							// PENDING: Handle composite terms,
							return false;

						SqlType rs1Type = ob1[0].Type;
						SqlType rs2Type = ob2[0].Type;
						if (!rs1Type.Equals(rs2Type))
							// Types are not strictly comparable, therefore can't merge,
							return false;

						// Merge (note that range_set1 which is part of 'to_merge_with'
						// will be modified).
						if (logicalOpName.Equals("@and_sql")) {
							// intersect (and)
							rangeSet1 = rangeSet1.Intersect(rangeSet2);
						} else {
							// union (or)
							rangeSet1 = rangeSet1.Union(rangeSet2);
						}
						// Update the simplified term,
						functionExp.Parameters[1] = rangeSet1;
						return true;
					}
					// Not equal variables so return false
					return false;
				}
					
				// fun_type isn't named "range_set", "or" or "and" so we return false
				// indicating no merge is possible.
				return false;
			}
				
			return false;
		}

		private SqlObject[] EvaluateExpression(Expression expression) {
			if (expression is FetchStaticExpression)
				return ((FetchStaticExpression)expression).Values;

			if (expression is FunctionExpression) {
				FunctionExpression functionExp = (FunctionExpression) expression;

				// Check all the parameters are static
				int sz = functionExp.Parameters.Count;
				Expression[] functionArgs = new Expression[sz];
				for (int i = 0; i < sz; ++i) {
					object ob = functionExp.Parameters[i];
					if (ob != null) {
						if (ob is Expression) {
							// Try and evaluate it
							SqlObject[] result = EvaluateExpression((Expression)ob);
							if (result == null)
								return null;

							functionArgs[i] = new FetchStaticExpression(result);
						}
					}
				}

				// Go ahead an evaluate the function,
				string functionName = functionExp.Name;
				if (IsSimpleEvaluable(functionName)) {
					// Evaluate the simple function and return the result
					ITable result = transaction.FunctionManager.Evaluate(functionName, simpleProcessor, functionArgs);
					return QueryProcessor.Result(result);
				}
					
				// Not a simple evaluatable function, so return null
				return null;
			}
				
			return null;
		}

		private void MarkUpIndexCandidates(Expression expression) {
			// If it's a fetch variable operation
			if (expression is FetchVariableExpression) {
				Variable var = ((FetchVariableExpression)expression).Variable;
				// Get the table name
				TableName tname = var.TableName;
				// Get the TableDataSource for this table,
				ITable tableSource = FetchTable(joinGraph, tname);
				if (tableSource != null) {
					// Get the list of indexes defined on this table,
					TableName index_tname = tableSource.Name;
					IIndexSetDataSource[] indexes = transaction.GetTableIndexes(index_tname);
					foreach (IIndexSetDataSource ind in indexes) {
						IndexCollation collation = ind.Collation;
						// If the collation matches the var name then we have a match
						if (collation.Columns.Length == 1 &&
							collation.Columns[0].Equals(var.Name)) {
							// Match found so mark it up
							expression.IndexTableName = index_tname;
							expression.IndexCandidate = ind.Name;
							return;
						}
					}
				}
			}
				// If it's a function,
			else if (expression is FunctionExpression) {
				FunctionExpression functionExp = (FunctionExpression) expression;

				// If the exp is a range_set function then it's an index candidate,
				String fuunctionName = functionExp.Name;
				if (fuunctionName.Equals("range_set")) {
					// Mark the fetch variable exp as an index candidate if we can
					FetchVariableExpression fetch_var_op = (FetchVariableExpression)functionExp.Parameters[0];
					MarkUpIndexCandidates(fetch_var_op);
				}
					// If its a simple comparison
				else if (IsSimpleComparison(fuunctionName) ||
						 IsSimpleLogical(fuunctionName)) {
					// Something like 'a = ?' is an index possibility
					Expression p1 = (Expression) functionExp.Parameters[0];
					Expression p2 = (Expression) functionExp.Parameters[1];
					// Mark up them up if we can
					MarkUpIndexCandidates(p1);
					MarkUpIndexCandidates(p2);
				}
					// If it is a composite function
				else if (fuunctionName.Equals("composite")) {
					// A composite index
					int paramCount = functionExp.Parameters.Count;
					int termCount = paramCount / 2;
					// If 1 term,
					if (termCount == 1) {
						// Recurse to the term,
						MarkUpIndexCandidates((Expression) functionExp.Parameters[0]);
					}
						// Multiple terms,
					else {
						Variable[] vars = new Variable[termCount];
						TableName tname = null;
						bool valid = true;
						for (int i = 0; i < termCount; ++i) {
							// If the collation of the composite is descending, then we can't
							// represent it as a composite index,
							SqlObject tv = (SqlObject)functionExp.Parameters[(i * 2) + 1];
							if (!tv.Value.ToBoolean().GetValueOrDefault()) {
								valid = false;
								break;
							}
							// Get the composite part operation
							Expression compVar = (Expression)functionExp.Parameters[i * 2];
							// Is it a var?
							if (!(compVar is FetchVariableExpression)) {
								valid = false;
								break;
							}
							// Fetch the var,
							Variable v = ((FetchVariableExpression)compVar).Variable;
							// This var is in a different table,
							if (i > 0 && !v.TableName.Equals(tname)) {
								valid = false;
								break;
							}
							// Ok, we know the composite part is a fetch var, and that the vars
							// reference the same source, and it's ascending,
							tname = v.TableName;
							vars[i] = v;
						}
						// If the composite meets the indexable requirement look for an
						// index,
						if (valid) {
							ITable tableSource = FetchTable(joinGraph, tname);
							if (tableSource != null) {
								// Get the list of indexes defined on this table,
								TableName indexTableName = tableSource.Name;
								IIndexSetDataSource[] indexes = transaction.GetTableIndexes(indexTableName);
								foreach (IIndexSetDataSource ind in indexes) {
									IndexCollation collation = ind.Collation;
									// If the collation matches the number of terms,
									if (collation.Columns.Length == termCount) {
										bool matchFound = true;
										for (int i = 0; i < termCount; ++i) {
											if (!collation.Columns[i].Equals(vars[i].Name)) {
												matchFound = false;
												break;
											}
										}
										// If match found, mark up the function,
										if (matchFound) {
											expression.IndexTableName = indexTableName;
											expression.IndexCandidate = ind.Name;
											return;
										}
									}
								}
							}
						}
					}
				} else {
					// index would not make sense so stop descending
					return;
				}
			}
		}

		private void PopulateJoinDependantsList(IList<QueryPredicate> list, Expression joinGraph) {
			if (joinGraph is JoinExpression) {
				JoinExpression joinExp = (JoinExpression) joinGraph;

				// Populate parents first
				PopulateJoinDependantsList(list, joinExp.Left);
				PopulateJoinDependantsList(list, joinExp.Right);

				// Is this an outer join?
				JoinType joinType = joinExp.JoinType;
				if (joinType == JoinType.Outer ||
					joinType == JoinType.OuterLeft ||
					joinType == JoinType.OuterRight) {
					// The outer table
					if (joinType != JoinType.OuterLeft)
						throw new ApplicationException("Unsupported join type " + joinType);

					// Make a list of tables that are dependants on the right side
					// (the outer join).
					List<TableName> rightDependancy = new List<TableName>();
					List<Expression> rightDependancyExps = new List<Expression>();
					PopulateSourceList(rightDependancy, rightDependancyExps, joinExp.Right);

					// Yes, so get the filter term
					Expression filter = joinExp.Filter;

					// Make a list of dependants on the expression itself
					List<QueryPredicate> dependants = new List<QueryPredicate>();

					// If filter_op is not null
					if (filter != null) {
						// Simplify the filter expression and create a predicate list
						Expression simplifiedFilter = SimplifyTerm(filter);
						PopulateDependantsList(dependants, simplifiedFilter);

						// Any predicates that have no dependancy on the right dependency of
						// the outer join we move to the main filter clause
						for (int j = dependants.Count - 1; j >= 0; j--) {
							QueryPredicate exp = dependants[j];
							// If this predicate not dependant on any sources on the right
							// tree, we simply shift it to the main filter clause.
							if (!exp.IsDependantOnAny(rightDependancy)) {
								MergeWithDependantList(exp, list);
								dependants.RemoveAt(j);
							} else {
								// Set the right dependancy info in the predicate
								exp.right_dependancy = rightDependancy;
								exp.right_exclusive = true;
								exp.JoinType = joinType;
							}
						}
					} 

					// filter_op is null, so dependants size will be 0 so it'll just add
					// a default expression dependant to the list for this join

					// If there are no dependants, we create a default dependant term for
					// this outer join, so that we correctly markup this outer join
					// expression.
					if (dependants.Count == 0) {
						QueryPredicate exprDep = new QueryPredicate(new FetchStaticExpression(new SqlObject(true)), new List<TableName>());
						exprDep.right_dependancy = rightDependancy;
						exprDep.right_exclusive = true;
						exprDep.JoinType = joinType;
						list.Add(exprDep);
					} else {
						// The final QueryPredicate are expressions that reference the
						// outer table in the join filter

						// Merge them all together,
						QueryPredicate exprDep = dependants[0];
						for (int i = 1; i < dependants.Count; ++i) {
							QueryPredicate next_expr = dependants[i];
							exprDep.Merge(next_expr);
						}
						list.Add(exprDep);
					}
				}

			} else if (joinGraph is AliasTableNameExpression) {
				// Terminating condition
			} else {
				throw new ApplicationException("Unexpected operation");
			}
		}

		private void MergeWithDependantList(QueryPredicate exprDep, IList<QueryPredicate> list) {
			bool merged = false;
			// If we should even bother trying to merge this
			bool merge_attempt = exprDep.dependant_on.Count > 1 &&
			                     exprDep.expression is FunctionExpression &&
			                     IsSimpleEquivalence(((FunctionExpression) exprDep.expression).Name);
			if (merge_attempt) {
				// Merge this expression into the list
				int sz = list.Count;
				for (int i = 0; i < sz && !merged; ++i) {
					QueryPredicate targetExp = list[i];
					// If the set of dependants is equal and all the terms are equi
					// predicates, then we can simply combine the terms.
					// We do not combine single source terms letting the planner schedule
					// filter terms as it sees best fit.
					if (targetExp.right_dependancy == null &&
						exprDep.HasEqualDependants(targetExp)) {
						Expression expr = targetExp.expression;
						// Only merge if the expression is an equivalence function
						if (expr is FunctionExpression &&
							IsSimpleEquivalence(((FunctionExpression)expr).Name)) {
							// Yes, so merge and break
							targetExp.Merge(exprDep);
							// Simplify
							targetExp.expression = SimplifyTerm(targetExp.expression);

							merged = true;
							break;
						}
					}
				}
			}
			// If we didn't merge, add to list
			if (!merged) {
				list.Add(exprDep);
			}
		}

		private Expression ProduceRandomPlan(long planSeed, IList<QueryPredicate> expressions, IList<Expression> sourceListExps, 
			Expression joinGraph, Expression sortFunction, Expression staticExpression) {

			// The current list of dangling nodes - operations that have not yet been
			// joined by the query plan
			List<Expression> danglingExps = new List<Expression>();
			// Populate it with our source tables
			for (int i = 0; i < sourceListExps.Count; i++) {
				Expression sourceExp = sourceListExps[i];
				// Apply the static filter if necessary,
				if (staticExpression != null) {
					sourceExp = new FilterExpression("static_filter", sourceExp, staticExpression);
				}
				danglingExps.Add(sourceExp);
			}

			random = new Random((int)planSeed);
			RandomPlanSchedule(expressions, danglingExps, joinGraph);

			// Connect any remaining dangling operations to a cartesian product.
			while (danglingExps.Count > 1) {
				// We randomly connect the remaining elements together
				int p1 = 0;
				int p2 = 1;
				if (danglingExps.Count > 2) {
					p1 = random.Next(danglingExps.Count);
					p2 = p1;
					while (p2 == p1) {
						p2 = random.Next(danglingExps.Count);
					}
				}

				Expression left = danglingExps[p1];
				Expression right = danglingExps[p2];
				if (p1 > p2) {
					danglingExps.RemoveAt(p1);
					danglingExps.RemoveAt(p2);
				} else {
					danglingExps.RemoveAt(p2);
					danglingExps.RemoveAt(p1);
				}

				danglingExps.Add(new JoinExpression(left, right, JoinType.Cartesian, null));
			}

			// The remaining operation is the query plan,
			Expression plan = danglingExps[0];

			// Put the final ordering filter on the plan if the ordering is important
			if (sortFunction != null)
				plan = new FilterExpression("sort", plan, sortFunction);

			return plan;
		}

		private static IList<TableName> GetExpressionSources(Expression expression) {
			List<TableName> sources = new List<TableName>();
			PopulateSourceList(sources, new List<Expression>(), expression);
			return sources;
		}

		private static int RandomPredicate(IList<QueryPredicate> predicates, IList<Expression> layout) {
			// If no predicates
			if (predicates.Count == 0)
				return -1;

			// Pick a random predicate
			int ri = predicates.Count - 1;
			int ci = ri;
			while (true) {
				QueryPredicate picked = predicates[ci];
				List<TableName> pickedDep = new List<TableName>(picked.dependant_on);
				// Merge this with any existing joins in the layout,
				foreach(Expression op in layout) {
					IList<TableName> expressionSources = GetExpressionSources(op);
					foreach(TableName src in pickedDep) {
						// If any elements in the source are in the picked_dep, we add
						// all the elements in the source to the picked_dep list
						if (expressionSources.Contains(src)) {
							// Copy all the elements from 'op_sources' to 'picked_dep' that
							// aren't already in picked_dep
							foreach(TableName cpy in expressionSources) {
								if (!pickedDep.Contains(cpy)) {
									pickedDep.Add(cpy);
								}
							}
							break;
						}
					}
				}
				// 'pickedDep' now contains the set of all sources that would result
				// from a join in the current state,
				// We fail if there are any other predicates that have full right
				// dependancy on elements from this.
				bool fail = false;
				foreach(QueryPredicate p in predicates) {
					if (p != picked && p.right_exclusive && !fail) {
						// Fail if this predicate has a rightdependacy on a table in the
						// tables expressed by 'picked_dep'
						foreach(TableName src in pickedDep) {
							if (p.right_dependancy.Contains(src)) {
								fail = true;
								break;
							}
						}
					}
				}

				// Returning conditions
				if (!fail)
					return ci;

				ci = ci - 1;
				if (ci < 0) {
					ci = predicates.Count - 1;
				}
				if (ci == ri) {
					return -1;
				}
			}
		}

		private Expression CreateRandomQueryPlan(IList<QueryPredicate> expressions, IList<TableName> sourceList, IList<Expression> sourceListExps, Expression joinGraph) {
			Expression static_expression = null;
			// For each input expression
			foreach (QueryPredicate expr in expressions) {
				// Find all static expressions (not dependant on terms in the current
				// query context) and make a single static expression to resolve it.
				int depend_on_count = expr.dependant_on.Count;
				// No dependants
				if (depend_on_count == 0) {
					// Create the static expression if there is one,
					static_expression = static_expression == null
					                    	? expr.expression
					                    	: new FunctionExpression("@and_sql", new Expression[] {static_expression, expr.expression});
				}

				// Mark up index information
				MarkUpIndexCandidates(expr.expression);
				// Mark up fact information
				if (FactStatistics.CanBeFact(expr.expression)) {
					Expression derefExp = QueryCostModel.DereferenceExpression(joinGraph, expr.expression);
					if (derefExp != null) {
						expr.expression.SetArgument("fact_id", FactStatistics.ToFactId(derefExp));
					}
				}
			}

			// Create the sort function
			Expression sortFunction = null;
			if (sortComposite != null) {
				sortFunction = (Expression) sortComposite.Clone();
				// Mark up any index information on the composite
				MarkUpIndexCandidates(sortFunction);
			}

			// Create the cost model
			QueryCostModel costModel = new QueryCostModel(transaction);

			// The list of the best plans in the current iteration
			List<PlanState> plans1 = new List<PlanState>();

			int[] walkIteration = new int[1];

			long plan_seed = DateTime.Now.Ticks; // Arbitary starting seed value
			int cost_iterations = 0;

			// Randomly schedule
			for (int i = 0; i < 64; ++i) {
				// Produce a random plan
				IList<QueryPredicate> predOrder = ShufflePredicateOrder(expressions, 1.0d);
				Expression result = ProduceRandomPlan(plan_seed, predOrder, sourceListExps, joinGraph, sortFunction,
				                                      static_expression);
				// Cost it out
				costModel.ClearGraph(result);
				costModel.Cost(result, Double.PositiveInfinity, walkIteration);
				++cost_iterations;
				double currentCostTime = result.CostTime;

				if (plans1.Count < 8 ||
					plans1[plans1.Count - 1].cost > currentCostTime) {
					// If it's a good plan, add it to the plan list
					PlanState state1 = new PlanState(plan_seed, predOrder, currentCostTime);
					int pos = plans1.BinarySearch(state1);
					if (pos < 0) {
						pos = -(pos + 1);
					}
					plans1.Insert(pos, state1);
				}

				// Ensure the list of good plans isn't more than 48
				if (plans1.Count > 48) {
					plans1.RemoveAt(plans1.Count - 1);
				}

				// Increment the plan seed
				plan_seed += 500000;
			}

			// Now go through the list from the end to the start and shuffle the
			// predicates.  If a better plan is found we insert it back into the list.
			for (int i = plans1.Count - 1; i >= 0; --i) {
				int tryCount;
				int graphMessChance;
				if (i <= 2) {
					tryCount = 32;
					graphMessChance = 120;
				} else if (i <= 3) {
					tryCount = 32;
					graphMessChance = 120;
				} else if (i <= 5) {
					tryCount = 24;
					graphMessChance = 60;
				} else if (i <= 8) {
					tryCount = 18;
					graphMessChance = 30;
				} else if (i <= 16) {
					tryCount = 18;
					graphMessChance = 10;
				} else {
					tryCount = 12;
					graphMessChance = 1;
				}
				int worse_plans_count = 0;
				for (int n = 0; n < tryCount; ++n) {
					PlanState curState = plans1[i];
					plan_seed = curState.seed;
					int bestI = System.Math.Min(plans1.Count - 1, 4);
					double costToBeat = plans1[bestI].cost;
					// Shuffle the predicate order of this plan
					IList<QueryPredicate> predOrder = ShufflePredicateOrder(curState.predicate_order, 0.012d);
					// 10% chance that we change the seed also,
					if (i > 14 ||
						new Random().Next(graphMessChance) == 0) {
						plan_seed = plan_seed + 1;
					}

					Expression result = ProduceRandomPlan(plan_seed, predOrder, sourceListExps, joinGraph, sortFunction,
					                                      static_expression);
					// Cost it out
					costModel.ClearGraph(result);
					costModel.Cost(result, costToBeat, walkIteration);
					++cost_iterations;
					if (result.IsCostSet) {
						double currentCostTime = result.CostTime;
						// If it's a better plan, feed it back into the list
						if (currentCostTime < curState.cost &&
							currentCostTime < costToBeat) {
							// If it's a good plan, add it to the plan list
							PlanState state1 = new PlanState(plan_seed, predOrder, currentCostTime);
							// NOTE; this doesn't add the entry to the list if there exists
							//   an entry that's the same cost and seed.
							int pos = plans1.BinarySearch(state1);
							if (pos < 0) {
								pos = -(pos + 1);
								plans1.Insert(pos, state1);
								++i;
							}
						} else {
							if (currentCostTime > costToBeat) {
								++worse_plans_count;
							}
						}
					}
				}

				// Remove all plans down to i
				for (int n = plans1.Count - 1; n >= System.Math.Max(i, 8); --n) {
					plans1.RemoveAt(n);
				}
			}

			// Make up the best plan from the seed,
			PlanState state = plans1[0];
			Expression bestExp = ProduceRandomPlan(state.seed, state.predicate_order, sourceListExps, joinGraph, sortFunction,
			                                       static_expression);

			// Make sure the cost information is correct for this graph before printing
			// it out
			costModel.ClearGraph(bestExp);
			costModel.Cost(bestExp, Double.PositiveInfinity, new int[1]);

			return bestExp;
		}

		private static IList<QueryPredicate> ShufflePredicateOrder(IList<QueryPredicate> expressions, double shuffleFactor) {
			List<QueryPredicate> plan_exprs = new List<QueryPredicate>(expressions);
			ShuffleByFactor(plan_exprs, new Random(), shuffleFactor);
			return plan_exprs;
		}

		private static void ShuffleByFactor(IList<QueryPredicate> list, Random random, double factor) {
			int sz = list.Count;
			int shuffle_count = (int)(sz * factor);
			shuffle_count = System.Math.Min(sz, shuffle_count);
			shuffle_count = System.Math.Max(1, shuffle_count);
			for (int i = 0; i < shuffle_count; ++i) {
				int exch_point1 = random.Next(sz);
				int exch_point2 = random.Next(sz);
				QueryPredicate a = list[exch_point1];
				list[exch_point1] = list[exch_point2];
				list[exch_point2] = a;
			}
		}

		private List<TableName> RandomPlanSchedule(IList<QueryPredicate> expressions, IList<Expression> danglingExps, Expression joinGraph) {
			// Collect the set of table sources around the left branch,
			List<Expression> expList = new List<Expression>();
			if (joinGraph is JoinExpression) {
				LeftDeepTableSources(expList, joinGraph);
			} else {
				expList.Add(joinGraph);
			}

			// Recurse on any right branches first (outer joins) and create a list of
			// sources
			List<TableName> sources = new List<TableName>();
			int sz = expList.Count;
			for (int i = 0; i < sz; ++i) {
				Expression expression = expList[i];
				if (expression is JoinExpression) {
					sources.AddRange(RandomPlanSchedule(expressions, danglingExps, expression));
				} else {
					TableName tn = ((AliasTableNameExpression) expression).Alias;
					if (tn == null)
						throw new SystemException();
					sources.Add(tn);
				}
			}

			// Now 'sources' is our domain of tables to work with
			// By the time this method returns, the domain 'sources' must be entirely
			// joined together in the danglingExps list.

			// The list of predicates that are entirely dependant on tables in this
			// source.
			List<QueryPredicate> predicates = new List<QueryPredicate>();
			foreach(QueryPredicate expr in expressions) {
				int dependOnCount = expr.dependant_on.Count;
				// Some dependants
				if (dependOnCount > 0) {
					bool touchAll = true;
					foreach(TableName src in expr.dependant_on) {
						if (!sources.Contains(src)) {
							touchAll = false;
							break;
						}
					}
					// Add if the dependants of this expression are all contained within
					// the source domain.
					if (touchAll) {
						predicates.Add(expr);
					}
				}
			}

			while (true) {
				// Find a random predicate that can be scheduled in this domain.
				// Returns -1 if no predicate can be found in the set.  If this is the
				// case and the source domain isn't entirely joined, then we perform a
				// cartesian join on any dangling tables.
				int ri = RandomPredicate(predicates, danglingExps);

				// If no predicates found,
				if (ri == -1) {
					// Break the main loop and return
					break;
				}

				// Remove the predicate from the list
				QueryPredicate predicate = predicates[ri];
				predicates.RemoveAt(ri);
				// Pick the operations from the source this query is dependant on
				List<Expression> srcExps = new List<Expression>();
				List<int> srcIds = new List<int>();
				foreach(TableName tname in predicate.dependant_on) {
					int id = 0;
					foreach(Expression op in danglingExps) {
						if (IsASourceOf(tname, op)) {
							if (!srcIds.Contains(id)) {
								srcIds.Add(id);
								srcExps.Add(op);
							}
							break;
						}
						++id;
					}
				}

				// Error condition
				if (srcExps.Count == 0)
					throw new ApplicationException("Unable to schedule predicate: " + predicate);

				// If we only found 1 predicate, we simply merge the predicate
				// expression as a scan operation.
				if (srcExps.Count <= 1) {
					int expPos = srcIds[0];
					Expression oldExp = danglingExps[expPos];
					// Make a filter with 'old_op' as the child and predicate.expression
					// as the filter
					danglingExps[expPos] = new FilterExpression("single_filter", oldExp, predicate.expression);
				} else {
					// If 2 or more

					// If more than 2, we randomly pick sources to merge as a cartesian
					// product while still maintaining the right requirement of the join
					// if there is one.
					if (srcExps.Count > 2) {
						// Randomize the list
						Util.CollectionsUtil.Shuffle(srcIds);
						// If the predicate has a right dependancy, put it on the end
						if (predicate.right_dependancy != null) {
							TableName farRight = predicate.right_dependancy[0];
							int i = 0;
							foreach(int expId in srcIds) {
								Expression op = danglingExps[expId];
								if (IsASourceOf(farRight, op)) {
									// swap with last element
									int lastI = srcIds.Count - 1;
									int lid = srcIds[lastI];
									srcIds[lastI] = srcIds[i];
									srcIds[i] = lid;
								}
								++i;
							}
						}

						// Cartesian join the terms, left to right until we get to the last
						// element.
						Expression procExp = danglingExps[srcIds[0]];
						for (int i = 1; i < srcIds.Count - 1; ++i) {
							procExp = new JoinExpression(procExp, danglingExps[srcIds[i]], JoinType.Cartesian, null);
						}

						// Remove the terms from the current layout list
						// Remember the expression on the right
						Expression leftExp1 = procExp;
						Expression rightExp1 = danglingExps[srcIds[srcIds.Count - 1]];

						// Sort the id list
						int[] idSet = srcIds.ToArray();
						Array.Sort(idSet);
						// Remove the values
						for (int i = idSet.Length - 1; i >= 0; --i) {
							danglingExps.RemoveAt(idSet[i]);
						}

						// Reset the src_ids and src_ops list
						srcIds.Clear();
						srcExps.Clear();

						// Add the left and right expression
						danglingExps.Add(leftExp1);
						danglingExps.Add(rightExp1);
						srcIds.Add(danglingExps.Count - 2);
						srcIds.Add(danglingExps.Count - 1);
						srcExps.Add(leftExp1);
						srcExps.Add(rightExp1);
					}

					// Ok, down to 2 to merge,
					int li;
					// Do we have a right requirement?
					if (predicate.right_dependancy != null) {
						// Yes, so either one src is part of the right dependancy or they
						// are both part of the right dependancy.
						Expression exp1 = srcExps[0];
						Expression exp2 = srcExps[1];
						int op1_c = 0;
						int op2_c = 0;
						foreach(TableName tname in predicate.right_dependancy) {
							if (IsASourceOf(tname, exp1)) {
								++op1_c;
							}
							if (IsASourceOf(tname, exp2)) {
								++op2_c;
							}
						}

						// If they are both part of the right dependancy, we cartesian join
						if (op1_c > 0 && op2_c > 0) {
							// TODO:
							throw new NotImplementedException();
						}

						// If op1 is part of the right dependancy,
						if (op1_c > 0) {
							li = 1;
						} else {
							// If exp2 is part of the right dependancy,
							li = 0;
						}
					} else {
						// No right dependancy,
						// Heuristic - If one of the sources is not a fetch table command
						// then we have a greater chance to pick that as our left.  This
						// encourages left deep scan graphs which are the sorts of graphs
						// we are interested in.
						ExpressionType type0 = srcExps[0].Type;
						ExpressionType type1 = srcExps[1].Type;

						if (type0 != ExpressionType.AliasTableName &&
						    type1 == ExpressionType.AliasTableName) {
							li = (random.Next(10) >= 2) ? 0 : 1;
						} else if (type1 != ExpressionType.AliasTableName &&
						           type0 == ExpressionType.AliasTableName) {
							li = (random.Next(10) >= 2) ? 1 : 0;
						} else {
							// Randomly pick if both are fetch table operations
							li = random.Next(2);
						}
					}

					Expression leftExp = srcExps[li];
					int leftId = srcIds[li];
					Expression rightExp = srcExps[(li + 1)%2];
					int rightId = srcIds[(li + 1)%2];

					// Schedule the join operation,
					// For 'join_inner', 'join_outer', etc
					// FIXME: check this ...
					JoinType jtype = !predicate.joinTypeSet ? JoinType.Inner : predicate.JoinType;
					// string join_type = "scan-" + jtype;
					JoinExpression join_op = new JoinExpression(leftExp, rightExp, jtype, predicate.expression);
					// Remove the left and right id from the list
					if (leftId > rightId) {
						danglingExps.RemoveAt(leftId);
						danglingExps.RemoveAt(rightId);
					} else {
						danglingExps.RemoveAt(rightId);
						danglingExps.RemoveAt(leftId);
					}
					// Add the new join
					danglingExps.Add(join_op);
				}
			}

			return sources;
		}

		private static void LeftDeepTableSources(IList<Expression> expressions, Expression graph) {
			if (graph == null)
				return;

			if (graph is JoinExpression) {
				JoinExpression joinExp = (JoinExpression) graph;
				Expression left = joinExp.Left;
				// If it's a join, recurse
				if (left is JoinExpression) {
					LeftDeepTableSources(expressions, left);
				} else {
					// Otherwise add it
					expressions.Add(left);
				}
				// Add the right expression
				expressions.Add(joinExp.Right);
			}
		}

		private void PopulateDependantsList(IList<QueryPredicate> list, Expression filter) {
			// Split by AND
			List<Expression> unionTerms = new List<Expression>();
			SplitByFunction(unionTerms, filter, "@and_sql");
			// Discover the tables each term is dependant on
			foreach (Expression expression in unionTerms) {
				List<TableName> tables = new List<TableName>();
				PopulateDependantTablesForExpression(tables, expression, sourceList);

				// Create an QueryPredicate object
				QueryPredicate exprDep = new QueryPredicate(expression, tables);

				// Merge it with the list or add to the end if we can't merge it with
				// an existing entry
				MergeWithDependantList(exprDep, list);
			}
		}

		public static void PopulateSourceList(IList<TableName> list, IList<Expression> listExps, Expression exp) {
			if (exp is AliasTableNameExpression) {
				TableName tname = ((AliasTableNameExpression)exp).Alias;
				list.Add(tname);
				listExps.Add(exp);
			} else if (exp is FetchTableExpression) {
				TableName tname = ((FetchTableExpression)exp).TableName;
				list.Add(tname);
				listExps.Add(exp);
			} else if (exp is JoinExpression) {
				JoinExpression joinExp = (JoinExpression) exp;
				// Walk the left and right nodes
				PopulateSourceList(list, listExps, joinExp.Left);
				PopulateSourceList(list, listExps, joinExp.Right);
			} else if (exp is FilterExpression) {
				// We walk to the child of scan functions
				PopulateSourceList(list, listExps, ((FilterExpression)exp).Child);
			} else {
				// We should only have table aliases and joins in the join graph at
				// this stage.
				throw new ApplicationException("Unexpected operation type");
			}
		}

		public static bool IsASourceOf(TableName tn, Expression expression) {
			if (expression is AliasTableNameExpression)
				return ((AliasTableNameExpression) expression).Alias.Equals(tn);

			if (expression is FetchTableExpression)
				return ((FetchTableExpression) expression).TableName.Equals(tn);

			if (expression is JoinExpression) {
				JoinExpression joinExp = (JoinExpression) expression;

				// Walk the left and right nodes
				if (IsASourceOf(tn, joinExp.Left))
					return true;

				return IsASourceOf(tn, joinExp.Right);
			}
			if (expression is FilterExpression) {
				// We walk to the child of scan functions
				return IsASourceOf(tn, ((FilterExpression)expression).Child);
			}

			// We should only have table aliases and joins in the join graph at
			// this stage.
			throw new ApplicationException("Unexpected operation type");
		}

		public void SetJoinGraph(Expression graph) {
			joinGraph = (Expression) graph.Clone();
		}

		public void SetFilterGraph(Expression graph) {
			// If set to null, set to static TRUE
			filterGraph = graph == null ? new FetchStaticExpression(new SqlObject(true)) : graph;
		}

		public void SetResultSortComposite(Expression sort) {
			sortComposite = sort;
		}

		public Expression FindCheapResolution() {
			// Find the list of data source names (as TableName)
			sourceList.Clear();
			sourceListExps.Clear();
			PopulateSourceList(sourceList, sourceListExps, joinGraph);

			// Simplify the filter graph
			Expression simplified_filter = SimplifyTerm(filterGraph);

			// We now have a simplified list of expressions that represent the terms
			// to intersect the sources in our join graph.

			// Create a list of QueryPredicate items for all the expressions in
			// our simplified join graph.
			List<QueryPredicate> dependants = new List<QueryPredicate>();
			PopulateDependantsList(dependants, simplified_filter);

			// Now fill in expression dependants for outer joins
			PopulateJoinDependantsList(dependants, joinGraph);

			// We now have a list the describes how all the expressions are dependant
			// on each other.  We can now schedule the joins.
			return CreateRandomQueryPlan(dependants, sourceList, sourceListExps, joinGraph);
		}
		
		public static bool IsSimpleComparison(string op) {
			return simpleComparisonMap.ContainsKey(op);
		}

		public static string ReverseSimpleComparison(string op) {
			String rev;
			if (!simpleComparisonMap.TryGetValue(op, out rev))
				throw new ApplicationException("Can not inverse operator");
			return rev;
		}
		
		public static bool IsSimpleArithmetic(string op) {
			return simpleArithmetic.Contains(op);
		}

		public static bool IsSimpleEquivalence(string op) {
			return simpleEquivalence.Contains(op);
		}

		public static bool IsSimpleLogical(string op) {
			return simpleLogical.Contains(op);
		}

		public static bool IsSimpleEvaluable(string op) {
			return simpleFunctions.Contains(op);
		}

		public static void PopulateDependantTablesForExpression(IList<TableName> list, Expression expr, IList<TableName> domainList) {
			QueryOptimizer.WalkGraph(expr, new DependantTablesPopulator(list, domainList));
		}

		#region DependantTablesPopulator

		private class DependantTablesPopulator : QueryOptimizer.IGraphInspector {
			private readonly IList<TableName> domainList;
			private readonly IList<TableName> list;

			public DependantTablesPopulator(IList<TableName> list, IList<TableName> domainList) {
				this.list = list;
				this.domainList = domainList;
			}

			public Expression OnBeforeWalk(Expression expression) {
				if (expression is FetchVariableExpression) {
					// If it's a fetch var, get the variable,
					Variable v = ((FetchVariableExpression)expression).Variable;
					// Is this in the domain we are interested in?
					TableName tableRef = v.TableName;
					if (domainList.Contains(tableRef)) {
						// Yes, so add it to the list
						if (!list.Contains(tableRef)) {
							list.Add(tableRef);
						}
					}
				}

				return expression;
			}

			public Expression OnAfterWalk(Expression expression) {
				return expression;
			}
		}

		#endregion

		#region QueryPredicate

		private sealed class QueryPredicate {
			public Expression expression;
			public readonly IList<TableName> dependant_on;
			public List<TableName> right_dependancy;
			public bool right_exclusive;
			private JoinType join_type;
			internal bool joinTypeSet;

			public QueryPredicate(Expression expression, IList<TableName> dependants) {
				this.expression = expression;
				this.dependant_on = dependants;
			}

			public JoinType JoinType {
				get { return join_type; }
				set {
					join_type = value;
					joinTypeSet = true;
				}
			}

			public bool HasEqualDependants(QueryPredicate dep) {
				// False if the sizes different
				int sz = dependant_on.Count;
				if (sz != dep.dependant_on.Count)
					return false;

				// Equal sizes
				for (int i = 0; i < sz; ++i) {
					if (!dep.IsDependantOn(dependant_on[i])) {
						// False if the given list doesn't contain this item.
						return false;
					}
				}
				// Match
				return true;
			}

			public bool IsDependantOn(TableName table) {
				return dependant_on.Contains(table);
			}

			public bool IsDependantOnAny(IList<TableName> list) {
				foreach (TableName tname in list) {
					if (IsDependantOn(tname))
						return true;
				}
				return false;
			}

			public void Merge(QueryPredicate dep) {
				// Assert the join types are equal
				if (joinTypeSet && dep.joinTypeSet &&
					!join_type.Equals(dep.join_type))
					throw new ApplicationException("Incorrect join type for merge.");

				if ((!joinTypeSet && dep.joinTypeSet) ||
					(joinTypeSet && !dep.joinTypeSet))
					throw new ApplicationException("Incorrect join type for merge.");

				// We simply wrap the expressions around an 'and'
				FunctionExpression andFun = new FunctionExpression("@and_sql");
				andFun.Parameters.Add(expression);
				andFun.Parameters.Add(dep.expression);
				expression = andFun;
				// Merge the dependants set
				foreach (TableName tname in dep.dependant_on) {
					if (!dependant_on.Contains(tname)) {
						dependant_on.Add(tname);
					}
				}
			}

			public override String ToString() {
				return dependant_on.ToString();
			}
		}

		#endregion

		#region PlanState

		class PlanState : IComparable {
			public readonly long seed;
			public readonly IList<QueryPredicate> predicate_order;
			public readonly double cost;

			public PlanState(long seed, IList<QueryPredicate> predicate_order, double cost) {
				this.seed = seed;
				this.predicate_order = predicate_order;
				this.cost = cost;
			}

			public int CompareTo(object ob) {
				PlanState dst = (PlanState) ob;
				if (cost > dst.cost)
					return 1;
				if (cost < dst.cost)
					return -1;
				if (seed > dst.seed)
					return 1;
				if (seed < dst.seed)
					return -1;
				
				return 0;
			}

			public override String ToString() {
				return "S:" + seed + " C:" + cost;
			}
		}

		#endregion
	}
}