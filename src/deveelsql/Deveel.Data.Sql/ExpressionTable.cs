using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public sealed class ExpressionTable : FilteredTable {
		private readonly QueryProcessor processor;
		private readonly List<OutputExpression> outputExps;
		private readonly List<Column> columns;
		
		public ExpressionTable(ITableDataSource child, QueryProcessor processor)
			: base(child) {
			// Make a copy of the processor
			this.processor = new QueryProcessor(processor);
			// Push the parent table onto the processor stack
			this.processor.PushTable(child);

			outputExps = new List<OutputExpression>();
			columns = new List<Column>();
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

		public override TableName TableName {
			get { return new TableName("@FUNCTIONTABLE@"); }
		}
		
		public override int ColumnCount {
			get { return outputExps.Count; }
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

			// Add it to the table_meta
			Column col = new Column();
			col.name = new Variable(TableName, label);
			col.type = type;
			columns.Add(col);
		}
		
		public override int GetColumnOffset(Variable columnName) {
			if (!TableName.Equals(columnName.TableName))
				return -1;
			
			for (int i = 0; i < columns.Count; i++) {
				Column column = columns[i];
				if (column.name.Equals(columnName))
					return i;
			}
			
			return -1;
		}
		
		public override Variable GetColumnName(int offset) {
			return columns[offset].name;
		}
		
		public override SqlType GetColumnType(int offset) {
			return columns[offset].type;
		}
		
		public override SqlObject GetValue(int column, long row) {
			OutputExpression outExpr = outputExps[column];
			Expression expression = outExpr.expression;
			// Set the processor stack as necessary
			processor.UpdateTableRow(row);
			// Execute the expression and return
			return QueryProcessor.Result(processor.Execute(expression))[0];
		}


		#region Column
		
		class Column {
			public Variable name;
			public SqlType type;
		}
		
		#endregion

		#region OutputExpression
		
		private sealed class OutputExpression {
			public string label;
			public SqlType type;
			public Expression expression;
			public Variable var;
		}

		
		#endregion
	}
}