using System;

namespace Deveel.Data.Sql {
	public sealed class OrderBy {
		private readonly Expression expression;
		private readonly bool ascending;

		public OrderBy(Expression expression, bool ascending) {
			this.expression = expression;
			this.ascending = ascending;
		}

		public OrderBy(Expression expression)
			: this(expression, true) {
		}

		public Expression Expression {
			get { return expression; }
		}

		public bool IsAscending {
			get { return ascending; }
		}
	}
}