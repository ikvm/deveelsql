using System;
using System.Text;

namespace Deveel.Data.Sql {
	public sealed class FetchParameterExpression : Expression {
		private FetchParameterExpression(object parameter, ParameterStyle style)
			: base(ExpressionType.FetchParameter) {
			SetArgument("param", parameter);
			SetArgument("param_style", style);
		}

		public FetchParameterExpression(string parameterName)
			: this(parameterName, ParameterStyle.Named) {
		}

		public FetchParameterExpression(int id)
			: this(id, ParameterStyle.Marker) {
		}

		public ParameterStyle ParameterStyle {
			get { return (ParameterStyle) GetArgument("param_style"); }
		}

		public string ParameterName {
			get {
				if (ParameterStyle != ParameterStyle.Named)
					throw new InvalidOperationException();

				return (string) GetArgument("param");
			}
		}

		public int ParameterId {
			get {
				if (ParameterStyle != ParameterStyle.Marker)
					throw new InvalidOperationException();

				return (int) GetArgument("param");
			}
		}

		protected override void Explain(StringBuilder sb, int indentFactor) {
			AppendReturnType(sb, this);
			sb.Append(GetArgument("param"));
		}
	}
}