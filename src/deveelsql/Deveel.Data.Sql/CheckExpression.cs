using System;

namespace Deveel.Data.Sql {
	public class CheckExpression : Constraint {
		private readonly Expression expression;

		public CheckExpression(TableName tableName, string name, string[] columns, Expression expression, bool deferrable, bool deferred)
			: base(tableName, name, ConstraintType.Check, columns, deferrable, deferred) {
			this.expression = expression;
		}

		public CheckExpression(TableName tableName, string name, string[] columns, Expression expression)
			: this(tableName, name, columns, expression, true, false) {
		}

		public Expression Expression {
			get { return expression; }
		}
	}
}