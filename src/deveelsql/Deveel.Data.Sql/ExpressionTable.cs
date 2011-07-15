using System;
using System.Collections.Generic;

using Deveel.Data.Sql;

namespace Deveel.Data.Sql {
	public sealed class ExpressionTable : FilteredTable {
		private readonly QueryProcessor processor;
		private readonly List<OutputExpression> outputExps;
		private ExpressionColumnCollection columns;
		
		public ExpressionTable(ITable child, QueryProcessor processor)
			: base(child) {
			// Make a copy of the processor
			this.processor = new QueryProcessor(processor);
			// Push the parent table onto the processor stack
			this.processor.PushTable(child);

			outputExps = new List<OutputExpression>();
			columns = new ExpressionColumnCollection(this);
		}
		
		public Expression[] Expressions {
			get {
				Expression[] resultExps = new Expression[outputExps.Count];
				for (int i = 0; i < outputExps.Count; i++) {
					resultExps[i] = outputExps[i].expression;
				}
				return resultExps;
			}
		}

		public override TableName Name {
			get { return new TableName("@FUNCTIONTABLE@"); }
		}
				
		public void AddColumn(string label, SqlType type, Expression expr) {
			OutputExpression outExpr = new OutputExpression();
			outExpr.label = label;
			outExpr.type = type;
			outExpr.expression = expr;
			// If the expression is a 'FETCHVAR' type then we pass it through the
			// blob accessor methods in hopes of not having to materialize large
			// objects.
			if (expr is FetchVariableExpression)
				outExpr.var = ((FetchVariableExpression)expr).Variable;

			outputExps.Add(outExpr);
		}
		
		public override SqlObject GetValue(int column, RowId row) {
			OutputExpression outExpr = outputExps[column];
			Expression expression = outExpr.expression;
			// Set the processor stack as necessary
			processor.UpdateTableRow(row);
			// Execute the expression and return
			return QueryProcessor.Result(processor.Execute(expression))[0];
		}


		#region OutputExpression
		
		private sealed class OutputExpression {
			public string label;
			public SqlType type;
			public Expression expression;
			public Variable var;
		}

		
		#endregion

		#region ExpressionColumnList

		private class ExpressionColumnCollection : ColumnCollection {
			private readonly ExpressionTable table;

			public ExpressionColumnCollection(ExpressionTable table) 
				: base(table) {
				this.table = table;
			}

			public override bool IsReadOnly {
				get { return true; }
			}

			public override int Count {
				get { return table.outputExps.Count; }
			}

			public override int IndexOf(string columnName) {
				int sz = Count;
				for (int i = 0; i < sz; i++) {
					OutputExpression exp = table.outputExps[i];
					if (String.Compare(exp.label, columnName, IgnoreCase) == 0)
						return i;
				}

				return -1;
			}

			public override TableColumn GetColumn(int index) {
				OutputExpression exp = table.outputExps[index];
				return new TableColumn(table, exp.label, exp.type);
			}
		}

		#endregion
	}
}