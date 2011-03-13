using System;
using System.Text;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class FetchGlobExpression : Expression {
		public FetchGlobExpression(string globString)
			: base(ExpressionType.FetchGlob) {
			SetArgument("glob_str", globString);
		}

		public string GlobString {
			get { return (string)GetArgument("glob_str"); }
		}

		protected override void Explain(StringBuilder sb, int indentFactor) {
			sb.Append(GlobString);
		}
	}
}