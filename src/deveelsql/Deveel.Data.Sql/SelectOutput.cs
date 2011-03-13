using System;

namespace Deveel.Data.Sql {
	public sealed class SelectOutput {
		private Expression expression;
		private Variable alias;

		public SelectOutput(Expression expression, Variable alias) {
			this.expression = expression;
			this.alias = alias;
		}

		public SelectOutput(Expression expression)
			: this(expression, null) {
		}

		public Expression Expression {
			get { return expression; }
			internal set { expression = value; }
		}

		public Variable Alias {
			get { return alias; }
			set {alias = value; }
		}
	}
}