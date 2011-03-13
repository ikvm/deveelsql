using System;
using System.Text;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class FetchTableExpression : Expression {
		public FetchTableExpression(TableName name)
			: base(ExpressionType.FetchTable) {
			SetArgument("name", name);
		}

		public TableName TableName {
			get { return (TableName) GetArgument("name"); }
			internal set { SetArgument("name", value); }
		}

		protected override void Explain(StringBuilder sb, int indentFactor) {
			sb.Append(TableName);
		}
	}
}