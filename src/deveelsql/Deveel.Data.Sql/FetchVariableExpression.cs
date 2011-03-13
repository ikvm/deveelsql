using System;
using System.Text;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class FetchVariableExpression : Expression {
		public FetchVariableExpression(Variable var) 
			: base(ExpressionType.FetchVariable) {
			SetArgument("var", var);
		}

		public Variable Variable {
			get { return (Variable) GetArgument("var"); }
			internal set { SetArgument("var", value); }
		}
		
		public override bool Equals(object obj) {
			FetchVariableExpression fetchVarExp = (FetchVariableExpression) obj;
			return Variable.Equals(fetchVarExp.Variable);
		}

		public override int GetHashCode() {
			return base.GetHashCode();
		}

		protected override void Explain(StringBuilder sb, int indentFactor) {
			AppendReturnType(sb, this);
			sb.Append(Variable);
			object index_candidate = IndexCandidate;
			if (index_candidate != null)
				sb.Append(" [index_candidate:" + index_candidate + "] ");
		}
	}
}