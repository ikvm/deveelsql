using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Deveel.Data.Sql {
	public sealed class FunctionExpression : Expression {
		private readonly ParameterList parameters;

		public FunctionExpression(string name, object[] args)
			: base(ExpressionType.Function) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			parameters = new ParameterList(this);
			SetArgument("name", name);

			int paramCount = args == null ? 0 : args.Length;
			for (int i = 0; i < paramCount; i++) {
				parameters.Add(args[i]);
			}
		}

		public FunctionExpression(string name)
			: this(name, new object[0]) {
		}

		public string Name {
			get { return (string) GetArgument("name"); }
			internal set { SetArgument("name", value); }
		}

		public IList<object> Parameters {
			get { return parameters; }
		}

		public bool IsAggregate {
			get { return (bool) GetArgument("is_aggregated"); }
			set { SetArgument("is_aggregated", value); }
		}

		public bool IsDistinct {
			get { return (bool) GetArgument("is_distinct"); }
			set { SetArgument("is_distinct", value); }
		}
		
		internal bool IsGlob {
			get { return (bool)GetArgument("glob_use"); }
			set { SetArgument("glob_use", value); }
		}

		public override bool Equals(object obj) {
			FunctionExpression exp = (FunctionExpression) obj;

			string op1_fname = Name;
			string op2_fname = exp.Name;
			if (!op1_fname.Equals(op2_fname))
				return false;

			int param_count1 = Parameters.Count;
			int param_count2 = exp.Parameters.Count;
			if (param_count1 != param_count2)
				return false;

			for (int i = 0; i < param_count1; ++i) {
				object ob1 = GetArgument(ArgName(i));
				object ob2 = exp.GetArgument(ArgName(i));
				if (ob1 is Expression &&
					ob2 is Expression) {
					if (!ob1.Equals(ob2))
						return false;
				} else if (!FunctionParamsEqual(ob1, ob2)) {
					return false;
				}
			}

			return true;
		}

		protected override void Explain(StringBuilder sb, int indentFactor) {
			AppendReturnType(sb, this);

			sb.Append(" ");
			sb.Append(Name);

			object fun_index_c = IndexCandidate;
			if (fun_index_c != null)
				sb.Append(" [index_candidate:" + fun_index_c + "] ");

			sb.Append(" ");

			object is_aggr = GetArgument("is_aggregated");
			if (is_aggr != null)
				sb.Append("[aggregate:" + is_aggr + "] ");

			if (IsDistinct)
				sb.Append("[distinct] ");

			sb.AppendLine();

			int sz = Parameters.Count;
			for (int i = 0; i < sz; ++i) {
				object fun_param = Parameters[i];
				if (fun_param is Expression) {
					Explain((Expression)fun_param, sb, indentFactor + 2);
				} else {
					Indent(sb, indentFactor + 2);

					if (fun_param is SqlType) {
						sb.Append("Type: " + fun_param);
					} else if (fun_param is SqlObject) {
						sb.Append("SqlObject: " + fun_param);
					} else if (fun_param is SelectableRange) {
						sb.Append("RangeSet: " + fun_param);
					} else {
						throw new ApplicationException(String.Format("Unknown function paramater type: {0}", fun_param.GetType()));
					}

					sb.AppendLine();
				}
			}

			Indent(sb, indentFactor);
		}

		public override int GetHashCode() {
			return base.GetHashCode();
		}

		private static bool FunctionParamsEqual(object ob1, object ob2) {
			if (ob1 is SqlObject) {
				if (ob2 is SqlObject) {
					SqlObject o1 = (SqlObject)ob1;
					SqlObject o2 = (SqlObject)ob2;
					return o1.CompareTo(o2) == 0;
				}

				return false;
			}

			return ob1.Equals(ob2);
		}

		public static FunctionExpression Composite(Expression exp, bool order) {
			FunctionExpression composite = new FunctionExpression("composite");
			composite.Parameters.Add(exp);
			composite.Parameters.Add(order ? new SqlObject(true) : new SqlObject(false));
			return composite;
		}

		public static FunctionExpression Composite(Expression[] expressions, bool[] order) {
			if (expressions.Length != order.Length)
				throw new ArgumentException();

			FunctionExpression composite = new FunctionExpression("composite");
			for (int i = 0; i < expressions.Length; ++i) {
				composite.Parameters.Add(expressions[i]);
				composite.Parameters.Add(order[i] ? new SqlObject(true) : new SqlObject(false));
			}
			return composite;
		}

		#region ParameterList

		private class ParameterList : IList<object> {
			private readonly FunctionExpression expression;
			private bool dirty;

			public ParameterList(FunctionExpression expression) {
				this.expression = expression;
			}

			public IEnumerator<object> GetEnumerator() {
				return new Enumerator(this, Count);
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			public void Add(object item) {
				if (!(item is SqlType || item is SqlObject ||
					item is Expression || item is SelectableRange))
					throw new ArgumentException("The given parameter type '" + item.GetType() + "' is not permitted.");

				int p_count = (int)expression.GetArgument("param_count", 0);
				expression.SetArgument(ArgName(p_count), item);
				expression.SetArgument("param_count", p_count + 1);
				dirty = true;
			}

			public void Clear() {
				int count = (int)expression.GetArgument("param_count");
				for (int i = count - 1; i >= 0; i--) {
					expression.SetArgument(ArgName(i), null);
				}
				expression.SetArgument("param_count", 0);
				dirty = true;
			}

			public bool Contains(object item) {
				return IndexOf(item) != -1;
			}

			public void CopyTo(object[] array, int arrayIndex) {
				throw new NotImplementedException();
			}

			public bool Remove(object item) {
				int index = IndexOf(item);
				if (index == -1)
					return false;
				RemoveAt(index);
				return true;
			}

			public int Count {
				get { return (int)expression.GetArgument("param_count", 0); }
			}

			public bool IsReadOnly {
				get { return false; }
			}

			public int IndexOf(object item) {
				int p_count = (int)expression.GetArgument("param_count");
				for (int i = 0; i < p_count; i++) {
					if (expression.GetArgument(ArgName(i)).Equals(item))
						return i;
				}

				return -1;
			}

			public void Insert(int index, object item) {
				throw new NotImplementedException();
			}

			public void RemoveAt(int index) {
				int p_count = (int)expression.GetArgument("param_count");
				for (int i = index + 1; i < p_count; ++i) {
					expression.SetArgument(ArgName(i - 1), expression.GetArgument(ArgName(i)));
				}
				expression.SetArgument(ArgName(p_count), null);
				expression.SetArgument("param_count", p_count - 1);
				dirty = true;
			}

			public object this[int index] {
				get { return expression.GetArgument(ArgName(index)); }
				set { expression.SetArgument(ArgName(index), value); }
			}

			#region Enumerator

			private class Enumerator : IEnumerator<object> {
				private readonly ParameterList list;
				private int index = -1;
				private int count;

				public Enumerator(ParameterList list, int count) {
					this.list = list;
					this.count = count;
					list.dirty = false;
				}

				private void CheckDirty() {
					if (list.dirty)
						throw new InvalidOperationException();
				}

				public void Dispose() {
				}

				public bool MoveNext() {
					CheckDirty();
					return ++index < count;
				}

				public void Reset() {
					count = list.Count;
					index = -1;
					list.dirty = false;
				}

				public object Current {
					get {
						CheckDirty();
						return list[index];
					}
				}

				object IEnumerator.Current {
					get { return Current; }
				}
			}

			#endregion
		}
		#endregion
	}
}