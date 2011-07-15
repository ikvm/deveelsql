using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Client;
using Deveel.Data.Sql;

namespace Deveel.Data.Sql {
	internal class QueryOptimizer {
		private readonly SystemTransaction transaction;
		private static readonly List<string> NonQualifiedFunctions;

		public QueryOptimizer(SystemTransaction transaction) {
			this.transaction = transaction;
		}

		static QueryOptimizer() {
			NonQualifiedFunctions = new List<string>();
			NonQualifiedFunctions.Add("star");
			NonQualifiedFunctions.Add("range_set");

			NonQualifiedFunctions.Add("basic_var_list");
			NonQualifiedFunctions.Add("insert_into_table");
			NonQualifiedFunctions.Add("update_table");
			NonQualifiedFunctions.Add("delete_from");
			NonQualifiedFunctions.Add("from_values");
			NonQualifiedFunctions.Add("from_select");
			NonQualifiedFunctions.Add("from_set");
			NonQualifiedFunctions.Add("insert_expression_list");
			NonQualifiedFunctions.Add("assignments");
			NonQualifiedFunctions.Add("assignment");
			NonQualifiedFunctions.Add("order_function");

			NonQualifiedFunctions.Add("create_table");
			NonQualifiedFunctions.Add("drop_table");
			NonQualifiedFunctions.Add("alter_table");
		}

		private static bool StringsMatch(string str1, string str2) {
			// TODO: case insensitive qualification
			return str1 == null || str1.Equals(str2);
		}

		private TableName QualifyTableName(TableName tableName) {
			// TODO: case insensitive qualification

			// New table with the current transaction schema if applicable
			tableName = tableName.ResolveSchema(transaction.CurrentSchema);
			// Return it if it exists, otherwise return null
			return transaction.TableExists(tableName) ? tableName : null;
		}
		
		private Expression NormalizeReferences(Expression op) {
			// Assert the operator is qualified
			if (op.GetArgument("qualified") == null)
				throw new ApplicationException("Operator is not qualified.");

			// Perform the normalization

			op = WalkGraph(op, new ReferenceQualifier(this));

			// Mark up that we have successfully normalized the all
			// definitions/references
			op.SetArgument("normalized_def", true);
			return op;
		}


		private static IList<FetchVariableExpression> QualifyVariables(Variable v, IList<FetchVariableExpression> varList) {
			List<FetchVariableExpression> outList = new List<FetchVariableExpression>();
			int sz = varList.Count;
			for (int i = 0; i < sz; ++i) {
				FetchVariableExpression varExp = varList[i];
				Variable vin = varExp.Variable;
				// Assume match unless shown otherwise
				if (StringsMatch(v.Name, vin.Name)) {
					TableName vtname = v.TableName;
					TableName vintname = vin.TableName;
					if (vtname == null ||
						(StringsMatch(vtname.Name, vintname.Name) &&
						 StringsMatch(vtname.Schema, vintname.Schema))) {
						outList.Add(varExp);
					}
				}
			}

			// Return the qualifying vars
			return outList;
		}

		private static void InspectParamList(IGraphInspector inspector, Expression op, string size_arg, string pre_arg) {
			int sz = (int)op.GetArgument(size_arg);
			for (int i = 0; i < sz; ++i) {
				InspectParam(inspector, op, pre_arg + i);
			}
		}

		private static void InspectParam(IGraphInspector inspector, Expression expression, string paramArg) {
			object paramVal = expression.GetArgument(paramArg);
			if (paramVal != null && paramVal is Expression) {
				Expression inspected = paramVal as Expression;
				expression.SetArgument(paramArg, WalkGraph(inspected, inspector));
			}
		}

		public static Expression WalkGraph(Expression op, IGraphInspector inspector) {
			// The pre walk call
			op = inspector.OnBeforeWalk(op);

			ExpressionType type = op.Type;
			switch (type) {
				case ExpressionType.Function:
					InspectParamList(inspector, op, "param_count", "arg");
					break;

				case ExpressionType.Select:
					InspectParam(inspector, op, "join");
					InspectParam(inspector, op, "filter");
					InspectParam(inspector, op, "havingfilter");
					InspectParamList(inspector, op, "out_count", "out");
					InspectParamList(inspector, op, "groupby_count", "groupby");
					InspectParamList(inspector, op, "orderby_count", "orderby");
					break;

				case ExpressionType.Join:
					InspectParam(inspector, op, "left");
					InspectParam(inspector, op, "right");
					InspectParam(inspector, op, "filter");
					break;

				// Single passthrough
				case ExpressionType.AliasTableName:
				case ExpressionType.AliasVariableName:
					InspectParam(inspector, op, "child");
					break;
				case ExpressionType.Filter:
					InspectParam(inspector, op, "child");
					InspectParam(inspector, op, "filter");
					break;

				// Terminators
				case ExpressionType.FetchVariable:
				case ExpressionType.FetchStatic:
				case ExpressionType.FetchParameter:
				case ExpressionType.FetchGlob:
				case ExpressionType.FetchTable:
					break;

				default:
					throw new ArgumentException("Unknown operation " + op.Type);
			}

			// The post expression call
			op = inspector.OnAfterWalk(op);

			// Return the operation
			return op;
		}

		private static Exception InvalidTypes(SqlType t1, SqlType t2, Expression expression) {
			throw new SqlParseException("Expression types are not compatible (" + t1 + " with " + t2 + ")", expression);
		}

		private static void AutoCastSystemOperator(FunctionExpression exp, IList<Expression> parameters, List<SqlType> paramTypes) {
			SqlType t1 = paramTypes[0];
			SqlType t2 = paramTypes[1];

			// If both the types are identical, we good to go,
			if (!t1.IsComparableTo(t2))
				throw InvalidTypes(t1, t2, exp);

			// Types are compatible,
			// If they are numeric,

			if (t1.IsNumeric) {
				// The encoding is different, so now we do a static check
				if (!t1.Equals(t2)) {
					// TODO: We should do a check on each parameter by walking the tree
					//   to determine if it's static or not.

					Expression exp1 = parameters[0];
					Expression exp2 = parameters[1];

					int staticop;
					int varop;

					// If the left or right is FETCHSTATIC,
					if (exp1 is FetchStaticExpression) {
						staticop = 0;
						varop = 1;
					} else if (exp2 is FetchStaticExpression) {
						staticop = 1;
						varop = 0;
					} else {
						// Neither static, so report error,
						throw InvalidTypes(t1, t2, exp);
					}

					// The type of the variable and static sides,
					SqlType varType = paramTypes[varop];

					SqlObject castType = new SqlObject(varType.ToString());
					FetchStaticExpression castExp = new FetchStaticExpression(castType);
					castExp.ReturnType = SqlType.GetSqlType(typeof(string));

					// Cast the static type to the variable type,
					FunctionExpression newStaticExp = new FunctionExpression("@cast", new Expression[] {parameters[staticop], castExp});
					newStaticExp.ReturnType = varType;
					exp.Parameters[staticop] = newStaticExp;

				}
			}
		}
		
		private static Expression QualifyExpression(Expression expression, IList<FetchVariableExpression> varList, bool errorOnNotQualifiable) {
			return WalkGraph(expression, new QualifyGraphInspector(varList, errorOnNotQualifiable));
		}
		
		private bool CheckAndMarkupExpressionAggregated(Expression originalExpression) {
			// HACK: allows us to pass a returned value back from the inner class
			AggregateExpressionInspector i = new AggregateExpressionInspector(this);
			WalkGraph(originalExpression, i);
			return i.Result;
		}
		
		private static void WriteExpressionsAtReferences(Expression expression, IDictionary<Variable, Expression> remap) {
			WalkGraph(expression, new RemapVariablesAsExpressionsInspector(remap));
		}
		
		private static void RemapAllVariables(Expression expression, IDictionary<Variable, Variable> remap) {
			WalkGraph(expression, new ReampVariablesInspector(remap));
		}

		private bool GlobMatch(string globString, Variable v) {
			// TODO: case insensitive qualification (globString may be variable case
			//   however table_name and col_name is information directly from the
			//   current metadata)

			if (globString.Equals("*"))
				return true;
			
			// Must be something.*
			string tableName = globString.Substring(0, globString.Length - 2);
			// The table name to resolve against
			TableName searchTableName = TableName.Resolve(tableName);
			searchTableName = searchTableName.ResolveSchema(transaction.CurrentSchema);
			return searchTableName.Equals(v.TableName);
		}
		
		private void PopulateVariables(IList<FetchVariableExpression> varList, Expression joinGraph) {
			// Exit early if join graph is null
			if (joinGraph == null)
				return;

			if (joinGraph is JoinExpression) {
				// Search the left and right for matching terms
				JoinExpression joinExp = (JoinExpression)joinGraph;
				PopulateVariables(varList, joinExp.Left);
				PopulateVariables(varList, joinExp.Right);
			} else if (joinGraph is FetchTableExpression) {
				// This must be fully qualified
				TableName table_name = ((FetchTableExpression)joinGraph).TableName;
				PopulateVariables(varList, table_name);
			} else if (joinGraph is AliasTableNameExpression) {
				AliasTableNameExpression aliasExp = (AliasTableNameExpression)joinGraph;
				TableName alias_name = aliasExp.Alias;
				// We find any that match in the child,
				List<FetchVariableExpression> newList = new List<FetchVariableExpression>();
				PopulateVariables(newList, aliasExp.Child);
				// And rewrite them with this table name alias
				int sz = newList.Count;
				for (int i = 0; i < sz; ++i) {
					FetchVariableExpression varExp = newList[i];
					Variable v = varExp.Variable;
					FetchVariableExpression newVarExp = (FetchVariableExpression) varExp.Clone();
					newVarExp.Variable = new Variable(alias_name, v.Name);
					newVarExp.ReturnType = varExp.ReturnType;
					varList.Add(newVarExp);
				}
			} else if (joinGraph is SelectExpression) {
				// For nested selects, we resolve against the select output only
				SelectExpression selectExp = (SelectExpression)joinGraph;
				int sz = selectExp.Output.Count;
				for (int i = 0; i < sz; ++i) {
					Expression op = selectExp.Output[i].Expression;
					Variable v;
					SqlType varType;
					if (op is FetchVariableExpression) {
						FetchVariableExpression varExp = (FetchVariableExpression)op;
						v = varExp.Variable;
						varType = varExp.ReturnType;
					} else if (op is AliasVariableNameExpression) {
						AliasVariableNameExpression aliasExp = (AliasVariableNameExpression)op;
						v = aliasExp.Alias;
						varType = aliasExp.ReturnType;
					} else {
						throw new ApplicationException("Unknown output object in SELECT");
					}
					if (v != null) {
						FetchVariableExpression varExp = new FetchVariableExpression(v);
						varExp.ReturnType = varType;
						varList.Add(varExp);
					}
				}
			}
		}
		
		public void PopulateVariables(IList<FetchVariableExpression> varList, TableName tableName) {
			ITable tsource = transaction.GetTable(tableName);
			int sz = tsource.Columns.Count;
			for (int i = 0; i < sz; ++i) {
				TableColumn column = tsource.Columns[i];
				string columnName = column.Name;
				
				// The variable name,
				FetchVariableExpression varExp = new FetchVariableExpression(new Variable(tableName, columnName));
				// The type,
				SqlType returnType = column.Type;
				varExp.ReturnType = returnType;
				varList.Add(varExp);
			}
		}

		private static void MarkSourceTables(Expression joinGraph) {
			// Exit early if join graph is null
			if (joinGraph == null)
				return;

			if (joinGraph is JoinExpression) {
				// Search the left and right for matching terms
				JoinExpression joinExp = (JoinExpression)joinGraph;
				MarkSourceTables(joinExp.Left);
				MarkSourceTables(joinExp.Right);
			} else if (joinGraph is FetchTableExpression) {
			} else if (joinGraph is AliasTableNameExpression) {
				// Recurse to the child
				AliasTableNameExpression aliasExp = (AliasTableNameExpression)joinGraph;
				MarkSourceTables(aliasExp.Child);
			} else if (joinGraph is SelectExpression) {
				// We mark this
				((SelectExpression)joinGraph).IsSourceSelect = true;
			}
		}

		private Expression RemapTables(Expression joinGraph, int[] uniqueId, IDictionary<Variable, Variable> varRemap) {
			// Exit early if join graph is null
			if (joinGraph == null)
				return joinGraph;

			bool makeAlias = false;
			TableName newName;
			
			if (joinGraph is JoinExpression) {
				JoinExpression joinExp = (JoinExpression)joinGraph;
				// Search the left and right for matching terms
				joinExp.Left = RemapTables(joinExp.Left, uniqueId, varRemap);
				joinExp.Right = RemapTables(joinExp.Right, uniqueId, varRemap);
				return joinGraph;
			} 
			if (joinGraph is FetchTableExpression) {
				// The new name of this
				int un_id = uniqueId[0];
				newName = new TableName("#TT" + un_id);
				makeAlias = true;
			} else if (joinGraph is AliasTableNameExpression) {
				// The new name of this
				int un_id = uniqueId[0];
				newName = new TableName("#TT" + un_id);
			} else if (joinGraph is SelectExpression) {
				newName = ((SelectExpression)joinGraph).UniqueName;
				makeAlias = true;
			} else {
				return joinGraph;
			}

			// If this is an alias table name then we need to promote the aliased
			// information to the child if it's a nested query
			if (joinGraph is AliasTableNameExpression) {
				AliasTableNameExpression aliasExp = (AliasTableNameExpression)joinGraph;
				
				// The child
				Expression child = aliasExp.Child;
				// If the child is a nested query
				if (child is SelectExpression) {
					SelectExpression selectExp = (SelectExpression)child;
					
					// Promote the old aliased information
					// The current alias name
					TableName curAliasName = aliasExp.Alias;
					// Get the output from the select and update the info
					int sz = selectExp.Output.Count;
					for (int i = 0; i < sz; ++i) {
						SelectOutput outExp = selectExp.Output[i];
						if (outExp.Expression is AliasVariableNameExpression) {
							AliasVariableNameExpression varAliasExp = (AliasVariableNameExpression)outExp.Expression;
							Variable oldVar = varAliasExp.Alias;
							Variable newVar = new Variable(curAliasName, oldVar.Name);
							varAliasExp.Alias = newVar;
						} else {
							throw new ApplicationException("Expected alias.");
						}
					}
					// The new name of this alias is the unique name of the table
					newName = selectExp.UniqueName;
				}
			}

			// The list of all vars referencable from this operation
			List<FetchVariableExpression> allRefs = new List<FetchVariableExpression>();
			PopulateVariables(allRefs, joinGraph);
			
			// Map them to new values,
			int colr = 0;
			foreach (FetchVariableExpression varExp in allRefs) {
				Variable v = varExp.Variable;
				varRemap[v] = new Variable(newName, v.Name);
				++colr;
			}
			
			// Rename the alias,
			++uniqueId[0];
			if (makeAlias) {
				joinGraph = new AliasTableNameExpression(joinGraph, newName);
			} else {
				((AliasTableNameExpression)joinGraph).Alias = newName;
			}
			
			return joinGraph;
		}
		
		private static Variable GetVariable(Expression op) {
			if (op is FetchVariableExpression)
				return ((FetchVariableExpression) op).Variable;
			if (op is AliasVariableNameExpression)
				return ((AliasVariableNameExpression)op).Alias;
				
			// We shouldn't get here, if we do it's a corrupt graph.
			throw new ApplicationException("Bad operation graph.");
		}

		private static void AddToSelectFilter(SelectExpression selectExpression, Expression toAdd) {
			Expression selectFilter = selectExpression.Filter;
			if (selectFilter != null) {
				if (toAdd != null) {
					FunctionExpression newFilter = new FunctionExpression("@and_sql");
					newFilter.Parameters.Add(toAdd);
					newFilter.Parameters.Add(selectFilter);
					toAdd = newFilter;
				} else {
					toAdd = selectFilter;
				}
			}
			
			selectExpression.Filter = toAdd;
		}
		
		private void AddToJoinFilter(JoinExpression joinExpression, Expression toAdd) {
			Expression joinFilter = joinExpression.Filter;
			if (joinFilter != null) {
				if (toAdd != null) {
					FunctionExpression newFilter = new FunctionExpression("@and_sql");
					newFilter.Parameters.Add(toAdd);
					newFilter.Parameters.Add(joinFilter);
					toAdd = newFilter;
				} else {
					toAdd = joinFilter;
				}
			}
			joinExpression.Filter = toAdd;
		}
		
		private static Expression ComposeSetLogicalGraph(string logicalFunction, string comparisonFunction, Expression lhsExp, FunctionExpression rhsExp) {
			int listCount = rhsExp.Parameters.Count;
			Expression logTree = null;
			for (int i = 0; i < listCount; ++i) {
				Expression val = (Expression) rhsExp.Parameters[i];
				Expression itemFun = new FunctionExpression(comparisonFunction, new Expression[] { (Expression) lhsExp.Clone(), val });
				logTree = logTree == null ? itemFun : new FunctionExpression(logicalFunction, new Expression[] {logTree, itemFun});
			}
			return logTree;

		}

		private Expression CostAnalysisAndTransform(SelectExpression selectExpression) {
			Expression joinGraph = selectExpression.Join;
			Expression filterGraph = selectExpression.Filter;

			// The required final ordering of the select expression if necessary.
			// This is either the 'group by' ordering for an aggregate statement,
			// or 'order by' if it's not an aggregate.

			// Are we an aggregate?
			Expression[] resultOrderExps;
			bool[] resultOrderAsc;
			Expression sortComposite = null;
			bool aggregateExpression = false;

			if (selectExpression.IsAggregated) {
				// Yes, do we have group by clause?
				int groupbyCount = selectExpression.GroupBy.Count;
				resultOrderExps = new Expression[groupbyCount];
				resultOrderAsc = new bool[groupbyCount];
				for (int i = 0; i < groupbyCount; ++i) {
					resultOrderExps[i] = selectExpression.GroupBy[i];
					resultOrderAsc[i] = true;      // All group by ordering is ascending
				}
				// Note the aggregate,
				aggregateExpression = true;
			} else {
				// Not an aggregate statement, do we have a order by clause?
				int orderbyCount = selectExpression.OrderBy.Count;
				resultOrderExps = new Expression[orderbyCount];
				resultOrderAsc = new bool[orderbyCount];
				for (int i = 0; i < orderbyCount; ++i) {
					resultOrderExps[i] = selectExpression.OrderBy[i].Expression;
					resultOrderAsc[i] = selectExpression.OrderBy[i].IsAscending;
				}
			}
			// The sort composite
			if (resultOrderExps.Length > 0) {
				sortComposite = FunctionExpression.Composite(resultOrderExps, resultOrderAsc);
			}

			// Create a new query transform object
			QueryPlanner planner = new QueryPlanner(transaction);
			planner.SetFilterGraph(filterGraph);
			planner.SetJoinGraph(joinGraph);
			planner.SetResultSortComposite(sortComposite);
			Expression cheapResolution = planner.FindCheapResolution();

			// If this is an aggregate query, we apply the aggregate filter to the
			// query plan.
			if (aggregateExpression) {
				FilterExpression aggregateFilter =
					  new FilterExpression("aggregate", cheapResolution, sortComposite);
				cheapResolution = aggregateFilter;
				// Is there a having clause?
				Expression havingExp = selectExpression.Having;
				if (havingExp != null) {
					FilterExpression havingFilter =
						new FilterExpression("single_filter", cheapResolution, havingExp);
					cheapResolution = havingFilter;
				}
				// Is there an order by clause?
				int orderbyCount = selectExpression.OrderBy.Count;
				if (orderbyCount > 0) {
					Expression[] orderExps = new Expression[orderbyCount];
					bool[] order_asc = new bool[orderbyCount];
					for (int i = 0; i < orderbyCount; ++i) {
						orderExps[i] = selectExpression.OrderBy[i].Expression;
						order_asc[i] = selectExpression.OrderBy[i].IsAscending;
					}
					
					Expression aggrSortComposite = FunctionExpression.Composite(orderExps, order_asc);
					FilterExpression sortFilter = new FilterExpression("sort", cheapResolution, aggrSortComposite);
					cheapResolution = sortFilter;
				}
			}

			// cheap_resolution is the best plan found, now add decoration such as
			// filter terms, etc
			int outCount = selectExpression.Output.Count;
			FunctionExpression outFunction = new FunctionExpression("table_out");
			for (int i = 0; i < outCount; ++i) {
				outFunction.Parameters.Add(selectExpression.Output[i].Expression);
			}
			// Set the filter,
			Expression outFilter = new FilterExpression("expression_table", cheapResolution, outFunction);

			QueryCostModel costModel = new QueryCostModel(transaction);
			costModel.ClearGraph(outFilter);
			costModel.Cost(outFilter, Double.PositiveInfinity, new int[1]);

			return outFilter;
		}

		public Expression QualifyAgainstTable(Expression expression, TableName tableName) {
			List<TableName> tlist = new List<TableName>(1);
			tlist.Add(tableName);
			return QualifyAgainstTableList(expression, tlist);
		}

		
		public Expression QualifyAgainstTableList(Expression expression, IList<TableName> tableNames) {
			// First pass qualifies the table names and expands out the output
			// (globs, etc) on the exit.
			expression = WalkGraph(expression, new TableQualifyInspector(this));

			// Second pass qualifies all remaining references in the query, including
			// forward and backward references in nested queries.

			ExpressionQualifier qualifier = new ExpressionQualifier(this);
			// If there's a base set of tables to qualify the expression against,
			if (tableNames.Count > 0) {
				// Make a var list for all the tables that represent the base
				// qualifications
				List<FetchVariableExpression> varList = new List<FetchVariableExpression>(4);
				foreach (TableName t in tableNames) {
					PopulateVariables(varList, t);
				}
				// Add the reference set to the qualifier
				qualifier.AddReferences(varList);
			}
			
			// And qualify
			expression = WalkGraph(expression, qualifier);

			// Third pass, we perform type completion on the SELECT output and
			// functions and verify the functions are correct.
			expression = WalkGraph(expression, new SelectOutputTypeCompletion(this));

			// Fourth pass, check functions,
			expression = WalkGraph(expression, new AllFunctionTypeCompletion(this));


			// Mark up that the expression has successfully qualified,
			expression.SetArgument("qualified", true);

			return expression;
		}

		public Expression Qualify(Expression expression) {
			return QualifyAgainstTableList(expression, new List<TableName>(0));
		}
		
		public Expression Optimize(Expression expression) {
			// Normalize the operation graph so we can safely flatten terms without
			// worrying about clashes.
			expression = NormalizeReferences(expression);

			// Any source nested queries that aren't aggregate/composite (plus some
			// other criteria) can be safely moved to the parent.  This is really nice
			// for collapsing views.
			// In addition, this moves inner join filters into the general table
			// filter operation.

			expression = WalkGraph(expression, new ExpressionOptimizer());

			// Transform all select operations in the query to primitive database
			// scan or index lookup operations choosing the best option by cost
			// analysis.
			expression = WalkGraph(expression, new CostAnalyzer(this));

			return expression;
		}

		public Expression SubstituteParameters(Expression expression, Query query) {
			return WalkGraph(expression, new ParameterSubstitutor(query));
		}


		#region TableQualifyInspector
		
		class TableQualifyInspector : IGraphInspector {
			private readonly QueryOptimizer optimizer;
			private int selectTableId;

			public TableQualifyInspector(QueryOptimizer optimizer) {
				this.optimizer = optimizer;
			}
			
			public Expression OnBeforeWalk(Expression expression) {
				if (expression is FetchTableExpression) {
					// Straight object fetch - fully qualify it
					TableName tableName = ((FetchTableExpression)expression).TableName;
					TableName qualifiedName = optimizer.QualifyTableName(tableName);
					if (qualifiedName == null)
						throw new SqlParseException("Unable to resolve '" + tableName + "'", expression);
					
					((FetchTableExpression)expression).TableName = qualifiedName;
				} else if (expression is SelectExpression) {
					SelectExpression selectExp = (SelectExpression)expression;
					// Assign a unique table name to this select
					TableName unique_name = new TableName("#QT" + selectTableId);
					selectExp.UniqueName = unique_name;
					++selectTableId;
					// Tables with an empty join graph are given a join graph with the
					// system one-row-table
					if (selectExp.Join == null)
						selectExp.Join = new FetchTableExpression(SystemTableNames.OneRowTable);
				}
				return expression;
			}
			
			public Expression OnAfterWalk(Expression expression) {
				if (expression is SelectExpression) {
					SelectExpression selectExp = (SelectExpression)expression;
					
					// Exiting select, we expand the select output list to a complete
					// form.

					int sz = selectExp.Output.Count;

					// Create a list of all forward referencable variables in the join
					// graph
					List<FetchVariableExpression> varList = new List<FetchVariableExpression>();
					optimizer.PopulateVariables(varList, selectExp.Join);

					// The unique table name of this select
					TableName uniqueTableName = selectExp.UniqueName;
					if (uniqueTableName == null)
						throw new SystemException("Select must have a unique name.");

					List<Expression> newOutList = new List<Expression>();
					for (int i = 0; i < sz; ++i) {
						// The Expression
						SelectOutput selectOut = selectExp.Output[i];
						
						// Is this a glob type?
						if (selectOut.Expression is FetchGlobExpression) {
							// Get the glob string
							string str = ((FetchGlobExpression)selectOut.Expression).GlobString;
							// Search the referencable list for matches,
							bool matchFound = false;
							foreach(FetchVariableExpression varExp in varList) {
								Variable v = varExp.Variable;
								// Found a match, add it to the list
								if (optimizer.GlobMatch(str, v)) {
									SqlType returnType = varExp.ReturnType;
									Expression globExp = new FetchVariableExpression(v);
									globExp = new AliasVariableNameExpression(globExp, v);
									globExp.ReturnType = returnType;
									newOutList.Add(globExp);
									matchFound = true;
								}
							}
							// If we searched the whole referencable list and no matches,
							// we report the error.  This doesn't apply to the general
							// glob "*" which is valid for results with no columns.
							if (!str.Equals("*") && !matchFound)
								throw new SqlParseException("'" + str + "' does not match anything", selectOut.Expression);
						} else {
							// The makes sure all other types of output from a select are
							// named variable references.

							// If this is an alias, we need to make sure we qualify it with
							// this table name
							if (selectOut.Expression is AliasVariableNameExpression) {
								// Rewrite this alias name with the new unique table id
								Variable v = selectOut.Alias;
								if (v.TableName != null)
									throw new SqlParseException("Incorrect alias format", selectOut.Expression);
									
								v = new Variable(uniqueTableName, v.Name);
								selectOut.Alias = v;
							}
								// If it's not a fetchvar operation, we need to assign a
								// unique alias name for this value
							else if (!(selectOut.Expression is FetchVariableExpression)) {
								string label = (string)selectOut.Expression.GetArgument("label");
								if (label == null)
									label = "nolabel";
								
								selectOut = new SelectOutput(
									new AliasVariableNameExpression(selectOut.Expression, new Variable(uniqueTableName, "#" + newOutList.Count + "#" + label)),
									     selectOut.Alias);
							}
								// If it's a regular fetch variable operation, we need to
								// forward qualify it and label it.
							else {
								selectOut.Expression = QualifyExpression(selectOut.Expression, varList, true);
								Variable var = ((FetchVariableExpression)selectOut.Expression).Variable;
								SqlType returnType = selectOut.Expression.ReturnType;
								selectOut.Expression = new AliasVariableNameExpression(selectOut.Expression, var);
								selectOut.Expression.ReturnType = returnType;
							}

							// Add it to the new select output list
							newOutList.Add(selectOut.Expression);
						}
					}
					// new_out_list is now the new expanded select output list
					sz = newOutList.Count;
					
					selectExp.Output.Clear();
					
					for (int i = 0; i < sz; ++i) {
						selectExp.Output.Add(new SelectOutput(newOutList[i]));
					}

					// Set the qualified flag for each select statement,
					selectExp.IsQualified = true;

				}
				return expression;
			}
		}
		
		#endregion

		#region AggregateExpressionInspector
		
		class AggregateExpressionInspector : IGraphInspector {
			private int nestLevel;
			private int aggregateLevel;
			private bool result;
			private readonly QueryOptimizer optimizer;

			public AggregateExpressionInspector(QueryOptimizer optimizer) {
				this.optimizer = optimizer;
			}
			
			public bool Result {
				get { return result; }
			}
			
			public Expression OnBeforeWalk(Expression expression) {
				if (expression is SelectExpression) {
					++nestLevel;
					if (aggregateLevel > 0)
						throw new SqlParseException("Invalid nested query", expression);
				} else if (expression is FunctionExpression) {
					if (nestLevel == 0) {
						FunctionExpression functionExp = (FunctionExpression)expression;
						string fun_name = functionExp.Name;
						if (optimizer.transaction.FunctionManager.IsAggregate(fun_name)) {
							functionExp.IsAggregate = true;
							++aggregateLevel;
							result = true;
						} else {
							// If it's not an aggregate, then check we don't have aggregate
							// specifications on the function (for example, DISTINCT or STAR)
							if (functionExp.IsDistinct)
								throw new SqlParseException("DISTINCT on none aggregate function " + fun_name, expression);
							if (functionExp.IsGlob)
								throw new SqlParseException("'*' on none aggregate function " + fun_name, expression);
							
							functionExp.IsAggregate = false;
						}
					}
				}
				return expression;
			}
			
			public Expression OnAfterWalk(Expression expression) {
				if (expression is SelectExpression) {
					--nestLevel;
				} else if (expression is FunctionExpression) {
					if (nestLevel == 0) {
						if (((FunctionExpression)expression).IsAggregate)
							--aggregateLevel;
					}
				}
				return expression;
			}
		}
		
		#endregion
		
		#region RemapVariablesInspector
		
		class ReampVariablesInspector : IGraphInspector {
			private readonly IDictionary<Variable, Variable> remap;
			
			public ReampVariablesInspector(IDictionary<Variable, Variable> remap) {
				this.remap = remap;
			}
			
			public Expression OnBeforeWalk(Expression expression) {
				if (expression is FetchVariableExpression) {
					FetchVariableExpression varExp = (FetchVariableExpression)expression;
					Variable var = varExp.Variable;
					Variable newVar;
					if (remap.TryGetValue(var, out newVar))
						varExp.Variable =newVar;
				}
				return expression;
			}
			
			public Expression OnAfterWalk(Expression expression) {
				return expression;
			}
		}
		
		#endregion
		
		#region RemapVariablesAsExpressionsInspector
		
		class RemapVariablesAsExpressionsInspector : IGraphInspector {
			private readonly IDictionary<Variable, Expression> remap;
			
			public RemapVariablesAsExpressionsInspector(IDictionary<Variable, Expression> remap) {
				this.remap = remap;
			}
			
			public Expression OnBeforeWalk(Expression expression) {
				return expression;
			}
			
			public Expression OnAfterWalk(Expression expression) {
				if (expression is FetchVariableExpression) {
					Variable var = ((FetchVariableExpression)expression).Variable;
					Expression newExp;
					if (remap.TryGetValue(var, out newExp))
						expression = newExp;
				}
				return expression;
			}
		}
		
		#endregion

		#region QualifyGraphInspector
		
		private class QualifyGraphInspector : IGraphInspector {
			private readonly bool errorOnNotQualifiable;
			private readonly IList<FetchVariableExpression> varList;

			public QualifyGraphInspector(IList<FetchVariableExpression> varList, bool errorOnNotQualifiable) {
				this.errorOnNotQualifiable = errorOnNotQualifiable;
				this.varList = varList;
			}

			public Expression OnBeforeWalk(Expression expression) {
				if (expression is FetchVariableExpression) {
					FetchVariableExpression varExp = (FetchVariableExpression) expression;
					Variable v = ((FetchVariableExpression)expression).Variable;
					int sz = varList.Count;
					for (int i = 0; i < sz; ++i) {
						IList<FetchVariableExpression> matches = QualifyVariables(v, varList);
						if (errorOnNotQualifiable && matches.Count == 0)
							throw new SqlParseException("Unable to resolve '" + v + "'", expression);
						if (matches.Count > 1)
							throw new SqlParseException("Ambiguous reference '" + v + "'", expression);
						
						if (matches.Count == 1) {
							FetchVariableExpression matched_var_op = matches[0];
							varExp.Variable = matched_var_op.Variable;
							varExp.ReturnType = matched_var_op.ReturnType;
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
		
		#region ExpressionQualifier
		
		private class ExpressionQualifier : IGraphInspector {
			private readonly QueryOptimizer optimizer;
			private readonly IList<IList<FetchVariableExpression>> refStack;

			public ExpressionQualifier(QueryOptimizer optimizer) {
				refStack = new List<IList<FetchVariableExpression>>(4);
				this.optimizer = optimizer;
			}

			public void AddReferences(IList<FetchVariableExpression> refs) {
				refStack.Add(refs);
			}

			public Expression OnBeforeWalk(Expression expression) {
				if (expression is SelectExpression) {
					SelectExpression selectExp = (SelectExpression)expression;
					// Entering select so populate the forward reference list and push
					// it on the stack.
					List<FetchVariableExpression> varList = new List<FetchVariableExpression>();
					Expression join_op = selectExp.Join;
					optimizer.PopulateVariables(varList, join_op);
					AddReferences(varList);
				} else if (expression is FetchVariableExpression) {
					FetchVariableExpression varExp = (FetchVariableExpression)expression;
					Variable v = varExp.Variable;
					int sz = refStack.Count;
					FetchVariableExpression foundVar = null;
					int i = sz - 1;
					for (; i >= 0 && foundVar == null; --i) {
						// Find qualifying variables,
						IList<FetchVariableExpression> resultSample = QualifyVariables(v, refStack[i]);
						// Ambiguous reference check
						if (resultSample.Count > 1)
							throw new SqlParseException("Ambiguous reference '" + v + "'", expression);
						
						// Single reference found, so we found reference!
						if (resultSample.Count == 1) {
							foundVar = resultSample[0];
							break;
						}
					}
					// If no reference found, report error
					if (foundVar == null)
						throw new SqlParseException("Unable to resolve '" + v + "'", expression);
					
					varExp.Variable = foundVar.Variable;
					varExp.ReturnType = foundVar.ReturnType;
				}

				return expression;
			}

			public Expression OnAfterWalk(Expression expression) {
				// -- Perform mark ups --

				// Test for aggregation and mark up the select operation if so
				if (expression is SelectExpression) {
					SelectExpression selectExp = (SelectExpression)expression;
					
					bool aggregated = false;
					// If there are group by elements, we are aggregated
					if (selectExp.GroupBy.Count > 0)
						aggregated = true;
					
					int sz = selectExp.Output.Count;
					// For each output of the select, check the aggregrated functions
					for (int i = 0; i < sz; ++i) {
						SelectOutput selectOut = selectExp.Output[i];
						if (optimizer.CheckAndMarkupExpressionAggregated(selectOut.Expression))
							aggregated = true;
					}
					
					// If aggregated select, mark it up
					selectExp.IsAggregated = aggregated;
					// Source check
 					MarkSourceTables(selectExp.Join);
				}

				// -- Make all outer joins left outer --
				else if (expression is JoinExpression) {
					JoinExpression joinExp = (JoinExpression)expression;
					JoinType joinType = joinExp.JoinType;
					// If this is a right outer join, we turn it into a left outer join
					// by swapping the left/right operations.
					if (joinType == JoinType.OuterRight) {
						Expression oldLeft = joinExp.Left;
						Expression oldRight = joinExp.Right;
						JoinExpression newJoinExp = 
							new JoinExpression(oldRight, oldLeft, JoinType.OuterLeft, joinExp.Filter);
						expression = newJoinExp;
					}
				}

				// -- Qualify all function operations --
				else if (expression is FunctionExpression) {
					FunctionExpression functionExp = (FunctionExpression)expression;
					// Translate all parsed functions into system functions,
					// for example, '+' turns into 'add_sql'
					string origFunctionName = functionExp.Name;
					// Exempt function names,
					if (!NonQualifiedFunctions.Contains(origFunctionName)) {
						if (!origFunctionName.StartsWith("@")) {
							string fname = optimizer.transaction.FunctionManager.QualifyName(origFunctionName);
							if (fname == null) {
								throw new SqlParseException("Unable to translate system function " + origFunctionName, expression);
							}
							functionExp.Name = fname;
							origFunctionName = fname;
						}
					}

					// Look for set functions that have nested queries, eg.
					// '@anyeq_sql' and work out if we can collapse the function into a
					// join or a logical function tree.
					string functionName = functionExp.Name;

					if (QueryPlanner.IsSimpleComparison(functionName)) {
						// Is the RHS a nested list?
						Expression rhsExp = (Expression) functionExp.Parameters[1];
						if (rhsExp is FunctionExpression) {
							string rhsFunctionName = ((FunctionExpression)rhsExp).Name;

							// Is the right hand side a nested list?
							if (rhsFunctionName.Equals("@nested_list")) {
								// Transform the function into a set of logical operations,
								// The type of expression (eg. @eq_sql),
								String setComparison = "@" + functionName.Substring(4);
								Expression lhsExp = (Expression) functionExp.Parameters[0];

								// This is a sub-query expression to process,
								if (functionName.StartsWith("@any")) {
									// ANY function is turned into an OR tree
									expression = ComposeSetLogicalGraph("@or_sql", setComparison, lhsExp, (FunctionExpression) rhsExp);
								} else if (functionName.StartsWith("@all")) {
									// ALL function is turned into a group of AND
									expression = ComposeSetLogicalGraph("@and_sql", setComparison, lhsExp, (FunctionExpression) rhsExp);
								}
							}
						}
					}

				}

				if (expression is SelectExpression) {
					// Leaving SELECT so pop the forward reference list from the stack
					IList<FetchVariableExpression> varList = refStack[refStack.Count - 1];
					refStack.RemoveAt(refStack.Count - 1);
				}

				return expression;
			}

		}

		private void MarkUpOutput(Expression outExp, IList<IList<FetchVariableExpression>> varList) {
			SqlType functionType = outExp.ReturnType;
			// Return if already marked up,
			if (functionType != null)
				return;

			// If it's alias,
			if (outExp is AliasVariableNameExpression) {
				AliasVariableNameExpression varAliasExp = (AliasVariableNameExpression)outExp;
				// Recurse on the child,
				Expression childExp = varAliasExp.Child;
				MarkUpOutput(childExp, varList);
				functionType = outExp.ReturnType;
			}
				// If it's a static,
			else if (outExp is FetchStaticExpression) {
				// Static reference,
				SqlObject[] val = ((FetchStaticExpression)outExp).Values;
				if (val.Length == 1)
					functionType = val[0].Type;
			} else if (outExp is FetchVariableExpression) {
				// Variable reference
				Variable v = ((FetchVariableExpression)outExp).Variable;
				// Look up the var reference in the select_op graph. If it's not found
				// then report the error,
				// Find the qualifying variables

				Expression resolvedExp = null;
				for (int i = varList.Count - 1; i >= 0; --i) {
					IList<FetchVariableExpression> resultSample = QualifyVariables(v, varList[i]);
					if (resultSample.Count >= 1) {
						resolvedExp = resultSample[0];
						break;
					}
				}

				if (resolvedExp == null)
					// Oops, this problem should have been caught already!
					throw new SqlParseException("Unable to resolve '" + v + "'", outExp);
					
				functionType = resolvedExp.ReturnType;
				if (functionType == null)
					throw new SqlParseException("Unable to resolve type of object '" + v + "'", outExp);

			} else if (outExp is FunctionExpression) {
				FunctionExpression functionExp = (FunctionExpression)outExp;
					
				// Resolve the function return type by looking at the function
				// specification and parameter types.
				// Note that this assumes the types of all vars in the graph have been
				// resolved.

				// Recurse on each arg,
				int argCount = functionExp.Parameters.Count;
				List<Expression> parameters = new List<Expression>(argCount);
				List<SqlType> paramTypes = new List<SqlType>(argCount);
				for (int i = 0; i < argCount; ++i) {
					Expression paramExp = (Expression) functionExp.Parameters[i];
					parameters.Add(paramExp);
					MarkUpOutput(paramExp, varList);
					paramTypes.Add(paramExp.ReturnType);
				}

				// The function name,
				string functionName = functionExp.Name;

				// CAST is a special case,
				if (functionName.Equals("@cast")) {
					Expression exp = parameters[1];
					SqlObject[] val = ((FetchStaticExpression)exp).Values;
					string castTypeString = val[0].Value.ToString();
					SqlType castType = SqlType.Parse(castTypeString);
					functionExp.ReturnType = castType;
					return;
				}
					
				// Ignore except functions,
				if (NonQualifiedFunctions.Contains(functionName))
					return;

				// There's additional logic for system operator functions
				if (QueryPlanner.IsSimpleArithmetic(functionName) ||
					QueryPlanner.IsSimpleComparison(functionName)) {
					// Simple evaluatable functions always have 2 parameters, and we
					// auto-magically insert cast functions for static numeric terms if
					// necessary.
					AutoCastSystemOperator(functionExp, parameters, paramTypes);
				}

				// Get the function specification
				Function[] functions = transaction.FunctionManager.GetFunction(functionName);

				// The function return type,
				foreach (Function function in functions) {
					// If the parameter count matches,
					if (function.MatchesParameterCount(argCount)) {
						SqlType type = function.Return.ResolveType(paramTypes);
						if (type != null) {
							// Match found,
							functionExp.ReturnType = type;
							return;
						}
					}
				}

				// No match found,
				throw new SqlParseException(
					 "Function parameters for '" + functionName + "' are invalid (of wrong type or count)", outExp);

			} else if (outExp is SelectExpression) {
				// Leave this one alone...
			} else {
				throw new ApplicationException("Expecting FETCHSTATIC, FETCHVAR, FUNCTION, SELECT");
			}

			outExp.ReturnType = functionType;
		}

		
		#endregion
		
		#region SelectOutputTypeCompletion
		
		private class SelectOutputTypeCompletion : IGraphInspector {
			private readonly QueryOptimizer optimizer;

			public SelectOutputTypeCompletion(QueryOptimizer optimizer) {
				this.optimizer = optimizer;
			}

			public Expression OnBeforeWalk(Expression expression) {
				return expression;
			}

			public Expression OnAfterWalk(Expression expression) {
				if (expression is SelectExpression) {
					SelectExpression selectExp = (SelectExpression)expression;
					// Make a list of variables referencable by the output,
					Expression joinExp = selectExp.Join;
					IList<FetchVariableExpression> varList = new List<FetchVariableExpression>();
					optimizer.PopulateVariables(varList, joinExp);

					IList<IList<FetchVariableExpression>> varStack = new List<IList<FetchVariableExpression>>(1);
					varStack.Add(varList);

					// Look at the output vars of this select,
					int sz = selectExp.Output.Count;
					for (int i = 0; i < sz; ++i) {
						Expression outExp = selectExp.Output[i].Expression;
						if (outExp.ReturnType == null)
							optimizer.MarkUpOutput(outExp, varStack);
					}

				}

				return expression;
			}

		}
		
		#endregion

		#region AllFunctionTypeCompletion

		private class AllFunctionTypeCompletion : IGraphInspector {
			private readonly QueryOptimizer optimizer;
			private readonly List<IList<FetchVariableExpression>> refStack;

			public AllFunctionTypeCompletion(QueryOptimizer optimizer) {
				refStack = new List<IList<FetchVariableExpression>>(4);
				this.optimizer = optimizer;
			}

			private void AddReferences(IList<FetchVariableExpression> refs) {
				refStack.Add(refs);
			}

			public Expression OnBeforeWalk(Expression expression) {
				if (expression is SelectExpression) {
					// Entering select so populate the forward reference list and push
					// it on the stack.
					List<FetchVariableExpression> varList = new List<FetchVariableExpression>();
					Expression joinExp = ((SelectExpression) expression).Join;
					optimizer.PopulateVariables(varList, joinExp);
					AddReferences(varList);
				}
				return expression;
			}

			public Expression OnAfterWalk(Expression expression) {
				if (expression is FunctionExpression)
					optimizer.MarkUpOutput(expression, refStack);

				if (expression is SelectExpression) {
					// Leaving SELECT so pop the forward reference list from the stack
					IList<FetchVariableExpression> varList = refStack[refStack.Count - 1];
					refStack.RemoveAt(refStack.Count - 1);
				}

				return expression;
			}
		}

		
		#endregion
		
		#region ReferenceQualifier
		
		private class ReferenceQualifier : IGraphInspector {
			private readonly QueryOptimizer optimizer;
			private readonly int[] uniqueId = new int[] { 0 };
			private readonly List<Dictionary<Variable, Variable>> remapStack = new List<Dictionary<Variable, Variable>>();

			public ReferenceQualifier(QueryOptimizer optimizer) {
				this.optimizer = optimizer;
			}

			public Expression OnBeforeWalk(Expression expression) {
				if (expression.Type == ExpressionType.Select) {
					SelectExpression selectExp = (SelectExpression)expression;
					Dictionary<Variable, Variable> varRemaps = new Dictionary<Variable, Variable>();
					// The join graph
					Expression joinGraph = selectExp.Join;
					joinGraph = optimizer.RemapTables(joinGraph, uniqueId, varRemaps);
					selectExp.Join = joinGraph;
					// Don't do the variable remappings yet, put them on the stack to
					// do later
					remapStack.Add(varRemaps);
				}
				return expression;
			}
			
			public Expression OnAfterWalk(Expression expression) {
				if (expression.Type == ExpressionType.Select) {
					SelectExpression selectExp = (SelectExpression)expression;
					
					// The current table name
					TableName uniqueName = selectExp.UniqueName;

					// Exiting a select, so first update the output vars
					// The top remap stack for this select
					Dictionary<Variable, Variable> curRemap = remapStack[remapStack.Count - 1];
					remapStack.RemoveAt(remapStack.Count - 1);

					List<Variable> outList = new List<Variable>();

					int sz = selectExp.Output.Count;
					for (int i = 0; i < sz; ++i) {
						Expression outExp = selectExp.Output[i].Expression;
						// What the output is currently called
						Variable currentName = GetVariable(outExp);
						// The new reference we are assigning it,
						Variable newName = new Variable(uniqueName, "#C" + i);
						// This is how we are mapping the output
						outList.Add(currentName);
						outList.Add(newName);
					}

					RemapAllVariables(selectExp, curRemap);
					
					if (remapStack.Count > 0) {
						if (selectExp.IsSourceSelect) {
							for (int i = 0; i < sz; ++i) {
								Expression outExp = selectExp.Output[i].Expression;
								Variable curName = outList[i * 2];
								Variable newName = outList[(i * 2) + 1];

								if (outExp is AliasVariableNameExpression) {
									((AliasVariableNameExpression) outExp).Alias = newName;
								} else {
									throw new ApplicationException("Unexpected operation.");
								}

								// Tell the parent map of these changes, if applicable,
								Dictionary<Variable, Variable> parentRemap = remapStack[remapStack.Count - 1];
								// Get any existing remap for this name
								Variable existingMapTo = parentRemap[curName];

								if (existingMapTo != null) {
									// We just remap the name
									parentRemap[curName] = new Variable(existingMapTo.TableName, newName.Name);
								} else {
									parentRemap[curName] = newName;
								}
							}
						}
					}
				}
				
				return expression;
			}
		}
		
		#endregion
		
		#region CostAnalyzer
		
		private class CostAnalyzer : IGraphInspector {
			private readonly QueryOptimizer optimizer;

			public CostAnalyzer(QueryOptimizer optimizer) {
				this.optimizer = optimizer;
			}

			public Expression OnBeforeWalk(Expression expression) {
				return expression;
			}
			
			public Expression OnAfterWalk(Expression expression) {
				if (expression is SelectExpression)
					expression = optimizer.CostAnalysisAndTransform((SelectExpression) expression);
				return expression;
			}
		}

		
		#endregion
		
		#region ExpressionOptimizer
		
		private class ExpressionOptimizer : IGraphInspector {
			private readonly List<SelectExpression> selectStack = new List<SelectExpression>();

			public Expression OnBeforeWalk(Expression expression) {
				if (expression is SelectExpression) {
					// Add the operation to the stack.
					selectStack.Add((SelectExpression) expression);
				} else if (expression is JoinExpression) {
					JoinExpression joinExp = (JoinExpression)expression;
					// If this is an inner join, move the filter terms into the
					// general filter for the select.
					JoinType joinType = joinExp.JoinType;
					if (joinType == JoinType.Inner) {
						Expression filter = joinExp.Filter;
						// Get the select on the top of the stack,
						SelectExpression selectExp = selectStack[selectStack.Count - 1];
						AddToSelectFilter(selectExp, filter);
					}
				}
				return expression;
			}
			
			public Expression OnAfterWalk(Expression expression) {
				if (expression is AliasTableNameExpression) {
					AliasTableNameExpression aliasExp = (AliasTableNameExpression)expression;
					
					// If we have linked aliases or alias linked to a join (an artifact
					// that is created by collapsing a node), we remove the alias
					Expression child = aliasExp.Child;
					if (child == null ||
						child is AliasTableNameExpression ||
						child is JoinExpression) {
						expression = child;
					}
				} else if (expression is SelectExpression) {
					// Pop from the stack
					SelectExpression topExp = selectStack[selectStack.Count - 1];
					selectStack.RemoveAt(selectStack.Count - 1);
					// Is this a collapsable nested query?
					// We can't collapse distinct or aggregated queries
					if (topExp.IsSourceSelect && !topExp.IsDistinct &&
						!topExp.IsAggregated) {

						// Yes, we can collapse this
						// Work out how we remap variables in the parent
						Dictionary<Variable, Expression> varMap = new Dictionary<Variable, Expression>();

						// Get the query output
						int sz = topExp.Output.Count;
						for (int i = 0; i < sz; ++i) {
							SelectOutput selectOut = topExp.Output[i];
							// Assert this is an alias
							if (!(selectOut.Expression is AliasVariableNameExpression))
								throw new SystemException();
							
							AliasVariableNameExpression aliasVarExp = (AliasVariableNameExpression)selectOut.Expression;
							
							// The var reference in the parent
							Variable substName = aliasVarExp.Alias;
							// The Operation.Operation to substitute
							Expression substExp = aliasVarExp.Child;
							// Put it in the map
							varMap[substName] = substExp;
						}

						// Write operations at the mapped references on the parent
						SelectExpression parentExp = selectStack[selectStack.Count - 1];
						WriteExpressionsAtReferences(parentExp, varMap);
						// Move the filter into the parent
						Expression filterExp = ((SelectExpression)  expression).Filter;
						AddToSelectFilter(parentExp, filterExp);

						// Remove the query,
						expression = ((SelectExpression) expression).Join;
					}
				}
				
				return expression;
			}
		}

		
		#endregion

		#region ParameterSubstitutor

		private class ParameterSubstitutor : IGraphInspector {
			private readonly Query query;

			public ParameterSubstitutor(Query query) {
				this.query = query;
			}

			public Expression OnBeforeWalk(Expression expression) {
				if (expression is FetchParameterExpression) {
					FetchParameterExpression paramExp = (FetchParameterExpression) expression;

					QueryParameter parameter;
					if (query.ParameterStyle == ParameterStyle.Marker) {
						int paramId = paramExp.ParameterId;
						parameter = query.Parameters[paramId];
					} else {
						string paramName = paramExp.ParameterName;
						parameter = query.Parameters[paramName];
					}

					if (parameter == null)
						throw new ApplicationException("Parameter substitution not set.");

					SqlObject val = parameter.Value;
					return new FetchStaticExpression(val);
				}

				return expression;
			}

			public Expression OnAfterWalk(Expression expression) {
				return expression;
			}
		}

		#endregion

		#region IGraphInspector

		public interface IGraphInspector {
			Expression OnBeforeWalk(Expression expression);

			Expression OnAfterWalk(Expression expression);
		}

		#endregion
	}
}