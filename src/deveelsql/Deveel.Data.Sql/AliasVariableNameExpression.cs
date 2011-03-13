using System;
using System.Text;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class AliasVariableNameExpression : Expression {
		public AliasVariableNameExpression(Expression child, Variable alias, SqlType returnType)
			: base(ExpressionType.AliasVariableName) {
			SetArgument("alias", alias);
			SetArgument("child", child);
			if (returnType != null)
				SetArgument("return_type", returnType);
		}

		public AliasVariableNameExpression(Expression child, Variable alias)
			: this(child, alias, null) {
		}
		
		public Expression Child {
			get { return (Expression)GetArgument("child"); }
		}

		public Variable Alias {
			get { return (Variable)GetArgument("alias"); }
			internal set { SetArgument("alias", value); }
		}

		protected override void Explain(StringBuilder sb, int indentFactor) {
			SqlType returnType = ReturnType;
			if (returnType != null) {
				sb.Append(returnType);
				sb.Append(" ");
			}

			sb.Append(Alias);
			sb.AppendLine();
			Child.ExplainTo(sb, indentFactor + 2);

			Indent(sb, indentFactor);
		}
	}
}