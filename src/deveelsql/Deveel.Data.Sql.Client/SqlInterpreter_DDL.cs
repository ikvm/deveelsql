using System;
using System.Collections.Generic;
using System.Data;

using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql.Client {
	internal sealed partial class SqlInterpreter {
		private static bool IsDeferred(Expression expression) {
			if (expression is FetchStaticExpression) {
				SqlObject val = (SqlObject)expression.GetArgument("static");
				string str = val.ToString();
				if (str.Equals("initially deferred"))
					return true;
				if (str.Equals("initially immediate"))
					return false;

				throw new ApplicationException("Unexpected initial check type '" + str + "'");
			}

			throw new ApplicationException("Expecting static expression");
		}

		private static bool IsDeferrable(Expression op) {
			if (op is FetchStaticExpression) {
				SqlObject val = (SqlObject)op.GetArgument("static");
				string str = val.ToString();
				if (str.Equals("deferrable"))
					return true;
				if (str.Equals("not deferrable"))
					return false;
					
				throw new ApplicationException("Unexpected deferrability type '" + str + "'");
			}

			throw new ApplicationException("Expecting static expression");
		}

		private static string GetActionString(Expression expression) {
			if (expression is FetchStaticExpression) {
				SqlObject val = (SqlObject)expression.GetArgument("static");
				return val.ToString();
			}
			throw new ApplicationException("Expecting static expression");
		}

		private long AddToColumnsSystemTable(IList<string> columnNames) {
			// Create a unique identifier for this column set
			long colSetUniqueId = transaction.CreateUniqueId(SystemTableNames.ColumnSet);

			QueryResult uTable = TableUpdatable(SystemTableNames.ColumnSet);
			int i = 1;
			foreach (string colName in columnNames) {
				// Insert the column into the column set table
				uTable.BeginInsertRow();
				uTable.Update("id", colSetUniqueId);
				uTable.Update("seq_no", i);
				uTable.Update("column_name", colName);
				uTable.InsertRow();
				++i;
			}

			// Return the id
			return colSetUniqueId;
		}

		private long AddToColumnsSystemTable(Expression expression) {
			// Convert the list to an array of strings for each column name
			int varListCount = (int)expression.GetArgument("param_count");
			List<string> colNames = new List<string>(varListCount);
			for (int i = 0; i < varListCount; ++i) {
				// Get the var
				Expression var = (Expression)expression.GetArgument("arg" + i);
				Variable v = (Variable)var.GetArgument("var");
				// Add to the list
				colNames.Add(v.Name);
			}

			// Add the column set and return the id
			return AddToColumnsSystemTable(colNames.ToArray());
		}

		private void AddToConstraintColumns(TableName tableName, string constraintName, Expression expression, string columnType) {
			// Convert the list to an array of strings for each column name
			int varListCount = (int)expression.GetArgument("param_count");
			List<string> colNames = new List<string>(varListCount);
			for (int i = 0; i < varListCount; ++i) {
				// Get the var
				Expression var = (Expression)expression.GetArgument("arg" + i);
				Variable v = (Variable)var.GetArgument("var");
				// Add to the list
				colNames.Add(v.Name);
			}

			AddToConstraintColumns(tableName, constraintName, colNames, columnType);
		}

		private void AddToConstraintColumns(TableName tableName, string constraintName, IList<string> columnNames, string columnType) {
			QueryResult uTable = TableUpdatable(SystemTableNames.ColumnSet);
			int i = 1;
			foreach (string colName in columnNames) {
				// Insert the column into the column set table
				uTable.BeginInsertRow();
				uTable.Update("table_schema", tableName.Schema);
				uTable.Update("table_name", tableName.Name);
				uTable.Update("constraint_name", constraintName);
				uTable.Update("seq_no", i);
				uTable.Update("column_name", colName);
				uTable.Update("", columnType);
				uTable.InsertRow();
				++i;
			}

		}

		private void CheckNoCircularDependancy(IList<TableName> visitedList, TableName toVisit) {
			if (visitedList.Contains(toVisit))
				throw new ApplicationException("Circular foreign key reference dependancy");

			// Query all foreign references from this table
			IList<ForeignKey> refsFrom = transaction.QueryForeignReferencesFrom(toVisit);
			// Make a distinct list of destination tables
			List<TableName> destTables = new List<TableName>();
			foreach (ForeignKey fRef in refsFrom) {
				TableName destTname = fRef.ReferencedTableName;
				if (!destTables.Contains(destTname))
					destTables.Add(destTname);
			}

			// Now, recurse for each destination table
			foreach (TableName destTname in destTables) {
				// Make a copy of the list and add the table we visited
				List<TableName> newVisitedList = new List<TableName>(visitedList);
				newVisitedList.Add(toVisit);
				// Recurse
				CheckNoCircularDependancy(newVisitedList, destTname);
			}
		}

		private ITable CreateTable(Expression expression) {
			// create_table has the following arguments,
			//   ( table_name, declarations (columns and constraints),
			//     [ check_expression ] )

			// The table name
			TableName tableName = (TableName)((Expression)expression.GetArgument("arg0")).GetArgument("name");
			tableName = QualifyTableName(tableName);

			// Does the table exist already?
			if (transaction.TableExists(tableName)) {
				// If the 'if_not_exists' argument is present, we don't generate an
				// error, simply returning '0'.  Otherwise we generate an error.
				if (expression.GetArgument("if_not_exists") != null)
					return DMLResult(0);

				// Otherwise generate an exception
				throw new ApplicationException("Table '" + tableName + "' already exists");
			}

			// We aren't allowed to create tables in the system schema
			if (tableName.Schema.Equals(SystemTableNames.SystemSchema)) {
				throw new ApplicationException("Unable to create table in the " + SystemTableNames.SystemSchema + " schema");
			}

			// The declarations op
			Expression declarations = (Expression)expression.GetArgument("arg1");

			// An optimizer for qualifying operations
			QueryOptimizer optimizer = new QueryOptimizer(transaction);

			// Split out the column declarations
			List<Expression> columnDeclares = new List<Expression>();
			List<Expression> constraintDeclares = new List<Expression>();
			int arg_count = (int)declarations.GetArgument("param_count");
			for (int i = 0; i < arg_count; ++i) {
				Expression decl = (Expression)declarations.GetArgument("arg" + i);
				// Is this a column declaration?
				if (decl.GetArgument("name").Equals("column_declaration")) {
					columnDeclares.Add(decl);
				} else {
					constraintDeclares.Add(decl);
				}
			}

			// Create the table
			SystemTable table = transaction.CreateTable(tableName);
			try {
				// For each column declare
				foreach (Expression col in columnDeclares) {
					// Column name
					Variable colName = (Variable)((Expression)col.GetArgument("arg0")).GetArgument("var");
					if (colName.TableName != null &&
						!colName.TableName.Equals(tableName)) {
						throw new ApplicationException("Invalid column name " + colName);
					}

					// Declared type
					SqlType colType = (SqlType)col.GetArgument("arg1");
					// Does the column have a default expression?
					Expression defaultExpr = null;
					int n = 2;
					if (col.GetArgument("has_default_exp") != null) {
						defaultExpr = (Expression)col.GetArgument("arg2");
						++n;
					}
					// Any remaining arguments will be constraints
					bool notNull = false;
					bool unique = false;
					while (true) {
						Expression cons = (Expression)col.GetArgument("arg" + n);
						if (cons == null)
							break;

						SqlObject constraint = (SqlObject)cons.GetArgument("static");
						string constraint_str = constraint.ToString();
						if (constraint_str.Equals("NOT NULL")) {
							notNull = true;
						} else if (constraint_str.Equals("NULL")) {
							notNull = false;
						} else if (constraint_str.Equals("UNIQUE")) {
							unique = true;
						} else {
							throw new ApplicationException("Unknown column constraint '" + constraint_str + "'");
						}
						++n;
					}

					// If we have a unique constraint, add it to the constraint list
					if (unique) {
						FunctionExpression uniqueConstraint = new FunctionExpression("constraint_unique");
						FunctionExpression basic_ref = new FunctionExpression("basic_var_list");
						basic_ref.Parameters.Add(new FetchVariableExpression(colName));
						uniqueConstraint.Parameters.Add(basic_ref);
						uniqueConstraint.Parameters.Add(new FetchStaticExpression("deferrable"));
						uniqueConstraint.Parameters.Add(new FetchStaticExpression("initially immediate"));
						constraintDeclares.Add(uniqueConstraint);
					}

					// Add the column to the table
					table.Columns.Add(colName.Name, colType, notNull);

					// Insert the default expression into the system table for defaults
					if (defaultExpr != null) {
						// Qualify the default expression.  This will generate an error if
						// the expression can not be qualified.
						defaultExpr = optimizer.Qualify(defaultExpr);

						// The source string representation of the expression
						string sourceStr = (string)defaultExpr.GetArgument("source");
						if (sourceStr == null)
							throw new NullReferenceException();

						// Serialize the expression graph to a byte object
						SqlValue serializedExpression = SqlValue.Serialize(defaultExpr);

						// Update the default expression table
						QueryResult defaultTable = TableUpdatable(SystemTableNames.DefaultColumnExpression);
						defaultTable.BeginInsertRow();
						defaultTable.Update("table_schema", table.Name.Schema);
						defaultTable.Update("table_name", table.Name.Name);
						defaultTable.Update("column", colName.Name);
						defaultTable.Update("default_source", sourceStr);
						defaultTable.Update("default_bin", serializedExpression.ToBinary());
						defaultTable.InsertRow();
					}

				}

				// For each constraint declaration,
				foreach (Expression constraint in constraintDeclares) {
					// Add the table constraint
					AddTableConstraint(optimizer, tableName, constraint);
				}

			} catch (Exception) {
				// Drop the table and cleanup if an sql exception was generated.
				transaction.DropTable(tableName);
				throw;
			}

			// Success,
			return DMLResult(1);
		}

		private void AddTableConstraint(QueryOptimizer optimizer, TableName tableName, Expression constraint) {
			// The id of the table we are adding this constraint for
			long table_id = transaction.GetTableId(tableName);

			// The name of the constraint or null if no label defined for the
			// constraint.
			string constraint_name = (string)constraint.GetArgument("constraint_name");
			// The constraint function type
			string constraint_fun_type = (string)constraint.GetArgument("name");

			// If it's a primary key constraint,
			if (constraint_fun_type.Equals("constraint_primary_key")) {
				// The basic var list
				Expression var_list = (Expression) constraint.GetArgument("arg0");
				// Constraint check control
				Expression deferrability = (Expression) constraint.GetArgument("arg1");
				Expression init_check = (Expression) constraint.GetArgument("arg2");

				// Make sure the var list qualifies
				var_list = optimizer.QualifyAgainstTable(var_list, tableName);

				// Check a primary key isn't already defined
				IDbCommand command = CreateDbCommand(
					 " SELECT * \n " +
					 "   FROM " + SystemTableNames.ConstraintsUnique + " \n" +
					 "  WHERE object_id = ? \n" +
					 "    AND primary_key = true \n");

				command.Parameters.Add(tableName.Schema);
				command.Parameters.Add(tableName.Name);

				IDataReader result = command.ExecuteReader();
				// Already a primary key defined on the object
				if (result.Read())
					throw new ApplicationException("PRIMARY KEY constraint already defined on " + tableName);

				// Update the table
				QueryResult uTable = TableUpdatable(SystemTableNames.ConstraintsUnique);
				uTable.BeginInsertRow();
				uTable.Update("table_schema", tableName.Schema);
				uTable.Update("table_name", tableName.Name);
				uTable.Update("name", constraint_name);
				uTable.Update("deferred", IsDeferred(init_check));
				uTable.Update("deferrable", IsDeferrable(deferrability));
				uTable.Update("primary_key", true);
				uTable.InsertRow();

				// Add the var list to the column set
				AddToConstraintColumns(tableName, constraint_name, var_list, null);
			}
				// If it's a foreign key constraint,
			else if (constraint_fun_type.Equals("constraint_foreign_key")) {
				// The var list
				Expression var_list = (Expression) constraint.GetArgument("arg0");
				// The table the foreign key references
				Expression ref_table = (Expression) constraint.GetArgument("arg1");
				// Either 6 or 7 parameters
				int param_count = (int)constraint.GetArgument("param_count");
				// The var list on the referenced table
				Expression foreign_var_list;
				int n;
				if (param_count == 6) {
					n = 2;
					foreign_var_list = null;
				} else if (param_count == 7) {
					n = 3;
					foreign_var_list = (Expression) constraint.GetArgument("arg2");
				} else {
					throw new ApplicationException("Unexpected parameter count");
				}

				// Trigger actions
				Expression updateAction = (Expression) constraint.GetArgument("arg" + n + 0);
				Expression deleteAction = (Expression) constraint.GetArgument("arg" + n + 1);
				// Constraint check control
				Expression deferrability = (Expression) constraint.GetArgument("arg" + n + 2);
				Expression init_check = (Expression) constraint.GetArgument("arg" + n + 3);

				// Make sure the var list qualifies
				var_list = optimizer.QualifyAgainstTable(var_list, tableName);
				// Qualify the reference table
				ref_table = optimizer.Qualify(ref_table);
				TableName refTableName = (TableName) ref_table.GetArgument("name");

				// Walk the table reference graph and ensure no table is visited more
				// than once (there are no circular dependancies created).
				List<TableName> table_set = new List<TableName>();
				table_set.Add(tableName);
				CheckNoCircularDependancy(table_set, refTableName);

				// The number of parameters in the foreign key
				int var_param_count = (int)var_list.GetArgument("param_count");
				// If there's a foreign variable list
				if (foreign_var_list != null) {
					// Qualify it against the reference table
					foreign_var_list = optimizer.QualifyAgainstTable(foreign_var_list, refTableName);
					// Check the number of keys are equal
					if (((int)foreign_var_list.GetArgument("param_count")) != var_param_count) {
						throw new ApplicationException("Key element count mismatch in FOREIGN KEY constraint");
					}

					AddToConstraintColumns(refTableName, constraint_name, foreign_var_list, "REFERENCE");
				} else {
					// Foreign list is null, so we need to look up the primary key
					IDbCommand command = CreateDbCommand(
						" SELECT s.column_name \n " +
						"   FROM " + SystemTableNames.ConstraintsForeign + " c, \n" +
						"        " + SystemTableNames.ColumnSet + " s \n" +
						"  WHERE c.table_schema = ? \n" +
						"    AND c.table_name = ? \n" +
						"    AND c.primary_key = true \n" +
						"    AND c.table_schema = s.table_schema \n " +
						"    AND c.table_name = s.table_name \n" +
						"    AND c.name = s.constraint_name \n" +
						"    AND c.name = ? \n" +
						"ORDER BY s.seq_no"
						);
					command.Parameters.Add(refTableName.Schema);
					command.Parameters.Add(refTableName.Name);
					command.Parameters.Add(constraint_name);
					IDataReader result = command.ExecuteReader();
					if (!result.Read()) {
						throw new ApplicationException("Referenced table '" + refTableName + "' does not have a primary key defined");
					}
					List<string> ref_col_names = new List<string>();
					ref_col_names.Add(result.GetString(0));
					while (result.Read()) {
						ref_col_names.Add(result.GetString(1));
					}
					// Check the number of keys are equal
					if (ref_col_names.Count != var_param_count) {
						throw new ApplicationException("Key element count mismatch in FOREIGN KEY constraint");
					}

					// Add the referenced column names to the column set
					AddToConstraintColumns(refTableName, constraint_name, ref_col_names, "REFERENCE");
				}

				// Add the var list to the column set
				long col_set_id = AddToColumnsSystemTable(var_list);
				// Update the table
				QueryResult uTable = TableUpdatable(SystemTableNames.ConstraintsForeign);
				uTable.BeginInsertRow();
				uTable.Update("table_schema", tableName.Schema);
				uTable.Update("table_name", tableName.Name);
				uTable.Update("name", constraint_name);
				uTable.Update("column_set_id", col_set_id);
				uTable.Update("ref_schema", refTableName.Schema);
				uTable.Update("ref_table_name", refTableName.Name);
				uTable.Update("update_action", GetActionString(updateAction));
				uTable.Update("delete_action", GetActionString(deleteAction));
				uTable.Update("deferred", IsDeferred(init_check));
				uTable.Update("deferrable", IsDeferrable(deferrability));
				uTable.InsertRow();
			}
				// If it's a unique constraint
			else if (constraint_fun_type.Equals("constraint_unique")) {
				// The basic var list
				Expression var_list = (Expression)constraint.GetArgument("arg0");
				// Constraint check control
				Expression deferrability = (Expression)constraint.GetArgument("arg1");
				Expression init_check = (Expression)constraint.GetArgument("arg2");

				// Make sure the var list qualifies
				var_list = optimizer.QualifyAgainstTable(var_list, tableName);

				// Add the var list to the column set
				long col_set_id = AddToColumnsSystemTable(var_list);
				// Update the table
				QueryResult uTable = TableUpdatable(SystemTableNames.ConstraintsUnique);
				uTable.BeginInsertRow();
				uTable.Update("object_id", table_id);
				uTable.Update("name", constraint_name);
				uTable.Update("column_set_id", col_set_id);
				uTable.Update("deferred", IsDeferred(init_check));
				uTable.Update("deferrable", IsDeferrable(deferrability));
				uTable.Update("primary_key", false);
				uTable.InsertRow();


			}
				// If it's a generic check
			else if (constraint_fun_type.Equals("constraint_check")) {
				// The check expression,
				Expression check_expr = (Expression)constraint.GetArgument("arg0");
				// Constraint check control
				Expression deferrability = (Expression)constraint.GetArgument("arg1");
				Expression init_check = (Expression)constraint.GetArgument("arg2");

				// Qualify the check expression
				check_expr = optimizer.QualifyAgainstTable(check_expr, tableName);

				// The source string representation of the expression
				String source_str = (String)constraint.GetArgument("source");
				if (source_str == null)
					throw new NullReferenceException();

				// Serialize the expression graph to a binary object
				SqlValue serializedExpression = SqlValue.Serialize(check_expr);

				// Update the check constraint table
				QueryResult uTable = TableUpdatable(SystemTableNames.ConstraintsCheck);
				uTable.BeginInsertRow();
				uTable.Update("name", constraint_name);
				uTable.Update("check_source", source_str);
				uTable.Update("check_bin", serializedExpression.ToBinary());
				uTable.Update("deferred", IsDeferred(init_check));
				uTable.Update("deferrable", IsDeferrable(deferrability));
				uTable.InsertRow();
			} else {
				throw new ApplicationException("Unknown constraint type: " + constraint_fun_type);
			}
		}
	}
}