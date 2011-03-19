using System;
using System.Collections.Generic;
using System.Text;

namespace Deveel.Data.Sql {
	[Serializable]
	public abstract class Expression : ICloneable, ILineInfo {
		private readonly ExpressionType type;
		private readonly Dictionary<string, object> args;
		
		private double costTime;
		private double costRows;
		private bool costSet;
		
		private int line = -1, column = -1;
		
		private static readonly string[] PreparedArgNames;
		
		internal Expression(ExpressionType type) {
			this.type = type;
			args = new Dictionary<string, object>();
		}
		
		static Expression() {
			PreparedArgNames = new string[32];
			for(int i = 0; i < 32; i++)
				PreparedArgNames[i] = String.Intern("arg" + i);
		}
		
		int ILineInfo.Column {
			get { return column; }
			set { column = value; }
		}
		
		int ILineInfo.Line {
			get { return line; }
			set { line = value; }
		}

		internal int Column {
			get { return column; }
			set { column = value; }
		}

		internal int Line {
			get { return line; }
			set { line = value; }
		}
		
		internal string IndexCandidate {
			get { return (string) GetArgument("index_candidate"); }
			set { SetArgument("index_candidate", value); }
		}
		
		internal TableName IndexTableName {
			get { return (TableName)GetArgument("index_table_name"); }
			set { SetArgument("index_table_name", value); }
		}

		
		internal ExpressionType Type {
			get { return type; }
		}
		
		public SqlType ReturnType {
			get { return (SqlType)GetArgument("return_type"); }
			set { SetArgument("return_type", value); }
		}

		
		internal double CostRows {
			get { 
				if (!costSet)
					throw new InvalidOperationException();
				return costRows;
			}
			set {
				costRows = value;
				costSet = true;
			}
		}
		
		internal double CostTime {
			get {
				if (!costSet)
					throw new InvalidOperationException();
				return costTime;
			}
			set {
				costTime = value;
				costSet = true;
			}
		}

		internal bool IsCostSet {
			get { return costSet; }
		}
		
		private static string TypedValueToString(SqlObject value) {
			StringBuilder sb = new StringBuilder();
			sb.Append(value.Type.ToString());
			sb.Append(": ");
			sb.Append(value.Value.ToObject());
			return sb.ToString();
		}

		internal object GetArgument(string argName) {
			return GetArgument(argName, null);
		}

		internal object GetArgument(string argName, object defaultValue) {
			object value;
			if (!args.TryGetValue(argName, out value))
				return defaultValue;
			return value;
		}
		
		internal void SetArgument(string argName, object value) {
			if (value == null) {
				args.Remove(argName);
			} else {
				args[argName] = value;
			}
		}

		
		internal void UnsetCost() {
			costSet = false;
		}
				
		internal void ExplainTo(StringBuilder sb, int indentFactor) {
			sb.Append(type.ToString());
			sb.Append('(');
			Explain(sb, indentFactor);
			sb.Append(')');
		}
		
		internal static void Indent(StringBuilder sb, int factor) {
			// The indent
			for (int i = 0; i < factor; ++i) {
				sb.Append(' ');
			}
		}
		
		internal static void Explain(Expression exp, StringBuilder buf, int indentFactor) {
			// The indent
			Indent(buf, indentFactor);
			if (exp == null) {
				buf.AppendLine("NULL");
				return;
			}
			
			exp.Explain(buf, indentFactor);			
		}
		
		internal static void AppendReturnType(StringBuilder sb, Expression exp) {
			object returnType = exp.GetArgument("return_type");
			if (returnType != null) {
				sb.Append(returnType.ToString());
				sb.Append(" ");
			}
		}
		
		internal static void Explain(SqlObject[] values, StringBuilder sb) {
			sb.Append("[");
			
			int sz = values.Length;
			for (int i = 0; i < sz; ++i) {
				sb.Append(TypedValueToString(values[i]));
				if (i < sz - 1)
					sb.Append(",");
			}
		}

		internal static string ArgName(int i) {
			if (i < PreparedArgNames.Length)
				return PreparedArgNames[i];
			return "arg" + i;
		}
		
		protected abstract void Explain(StringBuilder sb, int indentFactor);

		internal void CloneThis(Expression exp) {
			exp.line = line;
			exp.column = column;
			exp.costTime = costTime;
			exp.costRows = costRows;
			Dictionary<string, object> copy = new Dictionary<string, object>(args);
			foreach (KeyValuePair<string, object> arg in copy) {
				object value = arg.Value;
				if (value is ICloneable) {
					value = ((ICloneable) value).Clone();
				}

				exp.args[arg.Key] = value;
			}
		}

		public virtual object Clone() {
			Expression exp = (Expression)MemberwiseClone();
			CloneThis(exp);
			return exp;
		}
		
		public override bool Equals(object obj) {
			Expression exp = obj as Expression;
			if (exp == null)
				return false;
			
			ExpressionType type1 = type;
			ExpressionType type2 = exp.type;
			if (type1 != type2)
				return false;
						
			// True if we get here
			return true;
		}
		
		public override int GetHashCode() {
			return base.GetHashCode();
		}

		public string Explain() {
			StringBuilder sb = new StringBuilder();
			ExplainTo(sb, 0);
			return sb.ToString();
		}

		private void GetCostModelTo(StringBuilder buf, int t, List<Expression> exprs) {
			switch (Type) {
				case (ExpressionType.Join):
					// The indent
					Indent(buf, t);
					buf.Append(GetArgument("type"));
					if (GetArgument("simple_relation") != null)
						buf.Append(" simple_relation");
					if (GetArgument("cartesian_scan") != null)
						buf.Append(" cartesian_scan");

					string rightIndex = (string)GetArgument("use_right_index");
					if (rightIndex != null)
						buf.Append(" rindex:" + rightIndex);

					Expression filter = (Expression) GetArgument("filter");
					if (filter != null) {
						buf.Append(" [EXPR:" + exprs.Count + "]");
						exprs.Add(filter);
					}
					buf.Append(" (Costs t:");
					buf.Append(CostTime);
					buf.Append(" r:");
					buf.Append(CostRows);
					buf.Append(")\n");
					((Expression)GetArgument("left")).GetCostModelTo(buf, t + 2, exprs);
					((Expression)GetArgument("right")).GetCostModelTo(buf, t + 2, exprs);

					break;

				case (ExpressionType.AliasTableName):
					// We jump to the child for this
					((Expression) GetArgument("child")).GetCostModelTo(buf, t, exprs);
					buf.Append(" (Costs t:");
					buf.Append(CostTime);
					buf.Append(" r:");
					buf.Append(CostRows);
					buf.Append(")\n");

					break;

				case (ExpressionType.FetchTable):
					// The table fetch
					Indent(buf, t);
					buf.Append("Fetch: ");
					buf.Append(GetArgument("name"));
					break;

				case (ExpressionType.Filter):
					Indent(buf, t);
					buf.Append("FILTER ");
					buf.Append(GetArgument("name"));
					buf.Append(" [EXPR:" + exprs.Count + "]");
					exprs.Add((Expression)GetArgument("filter"));
					Expression order_required = (Expression)GetArgument("order_required");
					if (order_required != null) {
						buf.Append(" [ORDER REQUIRED]");
					}
					buf.Append(" (Costs t:");
					buf.Append(CostTime);
					buf.Append(" r:");
					buf.Append(CostRows);
					buf.Append(")\n");
					((Expression)GetArgument("child")).GetCostModelTo(buf, t + 2, exprs);

					break;

				default:
					throw new ApplicationException(String.Format("Unrecognized operation {0}", Type));
			}
		}

		public string GetCostModel() {
			List<Expression> exprs = new List<Expression>();
			StringBuilder sb = new StringBuilder();
			GetCostModelTo(sb, 0, exprs);

			// Output the expressions
			sb.Append("\nExpressions:\n");
			for (int i = 0; i < exprs.Count; ++i) {
				sb.Append("EXPRS:" + i + "\n");
				exprs[i].ExplainTo(sb, 2);
			}

			return sb.ToString();
		}
	}
}