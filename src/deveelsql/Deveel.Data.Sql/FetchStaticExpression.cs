using System;
using System.Text;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class FetchStaticExpression : Expression {
		public FetchStaticExpression(SqlObject[] values)
			: base(ExpressionType.FetchStatic) {
			SetArgument("static", values);
		}
		
		public FetchStaticExpression(SqlObject value)
			: this(new SqlObject[] { value } ) {
		}

		public SqlObject[] Values {
			get { return (SqlObject[]) GetArgument("static"); }
		}
		
		public bool HasSingleValue {
			get {
				SqlObject[] values = Values;
				return (values != null && values.Length == 1);
			}
		}
		
		public SqlObject Value {
			get {
				SqlObject[] values = Values;
				return (values == null || values.Length != 1 ? null : values[0]);
			}
		}
		
		protected override void Explain(StringBuilder sb, int indentFactor) {
			AppendReturnType(sb, this);
			Explain(Values, sb);
		}
	}
}