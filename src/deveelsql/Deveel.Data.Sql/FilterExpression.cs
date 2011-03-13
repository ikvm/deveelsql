using System;
using System.Text;

namespace Deveel.Data.Sql {
	[Serializable]
	public class FilterExpression : Expression {
		public FilterExpression(string name, Expression child, Expression filter)
			: base(ExpressionType.Filter) {
			SetArgument("name", name);
			SetArgument("child", child);
			SetArgument("filter", filter);
		}

		public string Name {
			get { return (string)GetArgument("name"); }
		}

		public Expression Child {
			get { return (Expression)GetArgument("child"); }
		}

		public Expression Filter {
			get { return (Expression)GetArgument("filter"); }
		}

		 internal Expression OrderRequired {
			get { return (Expression) GetArgument("order_required"); }
			set { SetArgument("order_required", value); }
		}

		protected override void Explain(StringBuilder sb, int indentFactor) {
			sb.Append(" ");
			sb.Append(Name);
			sb.AppendLine();
			Indent(sb, indentFactor);

			sb.AppendLine("child:");
			Child.ExplainTo(sb, indentFactor + 2);
			Indent(sb, indentFactor);

			sb.AppendLine("filter:");
			Filter.ExplainTo(sb, indentFactor + 2);
			Indent(sb, indentFactor);
		}
	}
}