using System;
using System.Text;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class AliasTableNameExpression : Expression {
		public AliasTableNameExpression(Expression child, TableName alias, SqlType returnType)
			: base(ExpressionType.AliasTableName) {
			SetArgument("child", child);
			SetArgument("alias", alias);
			if (returnType != null)
				SetArgument("return_type", returnType);
		}

		public AliasTableNameExpression(Expression child, TableName alias)
			: this(child, alias, null) {
		}

		public Expression Child {
			get { return (Expression)GetArgument("child"); }
		}

		public TableName Alias {
			get { return (TableName)GetArgument("alias"); }
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