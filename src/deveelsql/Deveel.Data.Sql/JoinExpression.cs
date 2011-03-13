using System;
using System.Text;

namespace Deveel.Data.Sql {
	public sealed class JoinExpression : Expression {
		public JoinExpression(Expression left, Expression right, JoinType type, Expression filter)
			: this() {
			SetArgument("left", left);
			SetArgument("right", right);
			SetArgument("type", type);
			if (filter != null)
				SetArgument("filter", filter);
		}

		public JoinExpression(Expression left, Expression right, JoinType type)
			: this(left, right, type, null) {
		}

		private JoinExpression()
			: base(ExpressionType.Join) {
		}

		public Expression Left {
			get { return (Expression)GetArgument("left"); }
			internal set { SetArgument("left", value); }
		}

		public Expression Right {
			get { return (Expression)GetArgument("right"); }
			internal set { SetArgument("right", value); }
		}

		public JoinType JoinType {
			get { return (JoinType)GetArgument("type"); }
		}

		public Expression Filter {
			get { return (Expression)GetArgument("filter"); }
			internal set { SetArgument("filter", value); }
		}
		
		internal bool IsSimpleRelation {
			get { return (bool) GetArgument("simple_relation"); }
			set { SetArgument("simple_relation", value); }
		}

		protected override void Explain(StringBuilder sb, int indentFactor) {
			sb.Append(" ");
			sb.Append(Type);
			sb.AppendLine();

			indentFactor += 2;

			Indent(sb, indentFactor);
			sb.AppendLine("left:");
			Left.ExplainTo(sb, indentFactor + 2);

			Indent(sb, indentFactor);
			sb.AppendLine("right:");
			Right.ExplainTo(sb, indentFactor + 2);

			Expression filter = Filter;
			if (filter != null) {
				Indent(sb, indentFactor);

				sb.AppendLine("filter:");
				filter.ExplainTo(sb, indentFactor + 2);
			}

			indentFactor -= 2;
			Indent(sb, indentFactor);
		}

		public override object Clone() {
			JoinExpression exp = new JoinExpression();
			CloneThis(exp);
			return exp;
		}
	}
}