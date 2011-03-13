using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Deveel.Data.Sql {
	public sealed class SelectExpression : Expression {
		private readonly OrderByList orderBy;
		private readonly GroupByList groupBy;
		private readonly OutputList outList;

		public SelectExpression()
			: base(ExpressionType.Select) {
			orderBy = new OrderByList(this);
			groupBy = new GroupByList(this);
			outList = new OutputList(this);
		}

		internal bool IsAggregated {
			get { return (bool)GetArgument("aggregated", false); }
			set { SetArgument("aggregated", value); }
		}

		public bool IsDistinct {
			get { return (bool)GetArgument("distinct", false); }
			set { SetArgument("distinct", value); }
		}

		internal TableName UniqueName {
			get { return (TableName)GetArgument("unique_name"); }
			set { SetArgument("unique_name", value); }
		}

		internal bool IsSourceSelect {
			get { return (bool)GetArgument("source_select", false); }
			set { SetArgument("source_select", value); }
		}
		
		internal bool IsQualified {
			get { return (bool)GetArgument("qualified", false); }
			set { SetArgument("qualified", value); }
		}

		public Expression Join {
			get { return (Expression)GetArgument("join"); }
			set { SetArgument("join", value); }
		}

		public Expression Filter {
			get { return (Expression)GetArgument("filter"); }
			set { SetArgument("filter", value); }
		}

		public Expression Having {
			get { return (Expression)GetArgument("having"); }
			set { SetArgument("having", value); }
		}

		public IList<OrderBy> OrderBy {
			get { return orderBy; }
		}

		public IList<Expression> GroupBy {
			get { return groupBy; }
		}

		public IList<SelectOutput> Output {
			get { return outList; }
		}

		protected override void Explain(StringBuilder sb, int indentFactor) {
			sb.Append(' ');

			TableName unique_name = UniqueName;
			if (unique_name != null)
				sb.Append("[uniqueid:" + unique_name.ToString() + "] ");

			object aggregated = GetArgument("aggregated");
			if (aggregated != null)
				sb.Append("[aggregate:" + aggregated + "] ");

			if (IsDistinct)
				sb.Append("[distinct] ");

			// Is a nested select in the join graph
			if (IsSourceSelect)
				sb.Append("[source] ");

			sb.AppendLine();
			indentFactor += 2;

			Expression join = Join;
			if (join != null) {
				Indent(sb, indentFactor);
				sb.AppendLine("source:");
				join.ExplainTo(sb, indentFactor + 2);
			}

			Expression filter = Filter;
			if (filter != null) {
				Indent(sb, indentFactor);
				sb.AppendLine("filter:");
				filter.ExplainTo(sb, indentFactor + 2);
			}

			Indent(sb, indentFactor);
			sb.AppendLine("group by:");

			int sz = groupBy.Count;
			for (int i = 0; i < sz; ++i) {
				groupBy[i].ExplainTo(sb, indentFactor + 2);
			}

			Indent(sb, indentFactor);

			sb.AppendLine("order by:");
			sz = orderBy.Count;
			for (int i = 0; i < sz; ++i) {
				OrderBy by = orderBy[i];
				by.Expression.ExplainTo(sb, indentFactor + 2);

				Indent(sb, indentFactor + 2);
				if (by.IsAscending) {
					sb.AppendLine("ASC");
				} else {
					sb.AppendLine("DESC");
				}
			}

			Expression having = Having;
			if (having != null) {
				Indent(sb, indentFactor);
				sb.AppendLine("having:");
				having.ExplainTo(sb, indentFactor + 2);
			}

			Indent(sb, indentFactor);

			sb.AppendLine("output:");
			sz = outList.Count;

			for (int i = 0; i < sz; ++i) {
				SelectOutput output = outList[i];
				output.Expression.ExplainTo(sb, indentFactor + 2);
				if (output.Alias != null) {
					Indent(sb, indentFactor + 2);

					sb.Append("alias: " + output.Alias);
					sb.AppendLine();
				}
			}

			indentFactor -= 2;
			Indent(sb, indentFactor);
		}

		#region GroupByList

		class GroupByList : IList<Expression> {
			private readonly SelectExpression expression;

			public GroupByList(SelectExpression expression) {
				this.expression = expression;
				expression.SetArgument("groupby_count", 0);
			}

			public Expression this[int index] {
				get { return (Expression)expression.GetArgument("groupby" + index); }
				set { expression.SetArgument("groupby" + index, value); }
			}

			public int Count {
				get { return (int)expression.GetArgument("groupby_count"); }
			}

			public bool IsReadOnly {
				get { return false; }
			}

			public int IndexOf(Expression item) {
				int count = Count;
				for (int i = 0; i < count; i++) {
					Expression exp = (Expression)this[i];
					if (exp.Equals(item))
						return i;
				}
				
				return -1;
			}

			public void Insert(int index, Expression item) {
				throw new NotImplementedException();
			}

			public void RemoveAt(int index) {
				int p_count = (int)expression.GetArgument("groupby_count");
				for (int i = index + 1; i < p_count; ++i) {
					expression.SetArgument("groupby" + (i - 1), expression.GetArgument("groupby" + i));
				}
				expression.SetArgument("groupby" + p_count, null);
				expression.SetArgument("groupby_count", p_count - 1);
			}

			public void Add(Expression item) {
				int count = (int)expression.GetArgument("groupby_count");
				expression.SetArgument("groupby" + count, item);
				expression.SetArgument("groupby_count", count + 1);
			}

			public void Clear() {
				int count = Count;
				for (int i = count - 1; i > 0; i--) {
					expression.SetArgument("groupby" + i, null);
				}
				expression.SetArgument("groupby_count", 0);
			}

			public bool Contains(Expression item) {
				return IndexOf(item) != -1;
			}

			public void CopyTo(Expression[] array, int arrayIndex) {
				throw new NotImplementedException();
			}

			public bool Remove(Expression item) {
				int index = IndexOf(item);
				if (index == -1)
					return false;
				
				RemoveAt(index);
				return true;
			}

			public IEnumerator<Expression> GetEnumerator() {
				return new Enumerator(this);
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}
			
			#region Enumerator
			
			private class Enumerator : IEnumerator<Expression> {
				private readonly GroupByList list;
				private int index;
				private int count;
				
				public Enumerator(GroupByList list) {
					this.list = list;
					count = list.Count;
					index = -1;
				}
				
				public Expression Current {
					get { return list[index]; }
				}
				
				object IEnumerator.Current {
					get { return Current; }
				}
				
				public void Dispose() {
				}
				
				public bool MoveNext() {
					return ++index < count;
				}
				
				public void Reset() {
					index = -1;
					count = list.Count;
				}
			}
			
			#endregion
		}

		#endregion

		#region OrderByList

		class OrderByList : IList<OrderBy> {
			private readonly SelectExpression expression;

			public OrderByList(SelectExpression expression) {
				this.expression = expression;
				expression.SetArgument("orderby_count", 0);
			}

			public OrderBy this[int index] {
				get {
					Expression exp = (Expression)expression.GetArgument("orderby" + index);
					if (exp == null)
						return null;

					object dir = expression.GetArgument("orderbydir" + index);
					return new OrderBy(exp, dir != null ? (bool)dir : true);
				}
				set {
					if (value == null)
						throw new ArgumentNullException("value");

					expression.SetArgument("orderby" + index, value.Expression);
					expression.SetArgument("orderbydir" + index, value.IsAscending);
				}
			}

			public int Count {
				get { return (int)expression.GetArgument("orderby_count"); }
			}

			public bool IsReadOnly {
				get { return false; }
			}

			public int IndexOf(OrderBy item) {
				int count = Count;
				for (int i = 0; i < count; i++) {
					OrderBy orderBy = this[i];
					if (orderBy.Expression.Equals(item.Expression))
						return i;
				}
				
				return -1;
			}

			public void Insert(int index, OrderBy item) {
				throw new NotImplementedException();
			}

			public void RemoveAt(int index) {
				int count = Count;
				for (int i = index + 1; i < count; ++i) {
					expression.SetArgument("orderby" + (i - 1), expression.GetArgument("orderby" + i));
					expression.SetArgument("orderbydir" + (i - 1), expression.GetArgument("orderbydir" + i));
				}
				expression.SetArgument("orderby" + count, null);
				expression.SetArgument("orderbydir" + count, null);
				expression.SetArgument("ordeby_count", count - 1);
			}

			public void Add(OrderBy item) {
				int count = Count;
				expression.SetArgument("orderby" + count, item.Expression);
				expression.SetArgument("orderbydir" + count, item.IsAscending);
				expression.SetArgument("orderby_count", count + 1);
			}

			public void Clear() {
				int count = Count;
				for (int i = count - 1; i >= 0; i--) {
					expression.SetArgument("orderby" + i, null);
					expression.SetArgument("orderbydir" + i, null);
				}

				expression.SetArgument("orderby_count", 0);
			}

			public bool Contains(OrderBy item) {
				return IndexOf(item) != -1;
			}

			public void CopyTo(OrderBy[] array, int arrayIndex) {
				throw new NotImplementedException();
			}

			public bool Remove(OrderBy item) {
				int index = IndexOf(item);
				if (index == -1)
					return false;
				
				RemoveAt(index);
				return true;
			}

			public IEnumerator<OrderBy> GetEnumerator() {
				return new Enumerator(this);
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			#region Enumearor

			private class Enumerator : IEnumerator<OrderBy> {
				private int index = -1;
				private int count;
				private readonly OrderByList list;

				public Enumerator(OrderByList list) {
					this.list = list;
					count = list.Count;
				}

				public void Dispose() {
				}

				public bool MoveNext() {
					return ++index < count;
				}

				public void Reset() {
					index = -1;
					count = list.Count;
				}

				public OrderBy Current {
					get { return list[index]; }
				}

				object IEnumerator.Current {
					get { return Current; }
				}
			}

			#endregion
		}

		#endregion

		#region OutList

		class OutputList : IList<SelectOutput> {
			private readonly SelectExpression expression;

			public OutputList(SelectExpression expression) {
				this.expression = expression;
				expression.SetArgument("out_count", 0);
			}

			public SelectOutput this[int index] {
				get {
					Expression outExpr = (Expression)expression.GetArgument("out" + index);
					Variable alias = (Variable)expression.GetArgument("outalias" + index);
					return new SelectOutput(outExpr, alias);
				}
				set {
					if (value == null)
						throw new ArgumentNullException("value");
					
					Expression outExpr = value.Expression;
					Variable alias = value.Alias;
					expression.SetArgument("out" + index, outExpr);
					if (alias != null)
						expression.SetArgument("outalias" + index, alias);
				}
			}

			public int Count {
				get { return (int)expression.GetArgument("out_count"); }
			}

			public bool IsReadOnly {
				get { return false; }
			}

			public int IndexOf(SelectOutput item) {
				int count = Count;
				for (int i = 0; i < count; i++) {
					SelectOutput sout = this[i];
					if (sout.Expression.Equals(item.Expression)) {
						if ((sout.Alias == null && item.Alias == null) ||
						     sout.Alias.Equals(item.Alias))
							return i;
					}
				}
				
				return -1;
			}

			public void Insert(int index, SelectOutput item) {
				throw new NotImplementedException();
			}

			public void RemoveAt(int index) {
								int count = Count;
				for (int i = index + 1; i < count; ++i) {
					expression.SetArgument("out" + (i - 1), expression.GetArgument("out" + i));
					expression.SetArgument("outalias" + (i - 1), expression.GetArgument("outalias" + i));
				}
				expression.SetArgument("out" + count, null);
				expression.SetArgument("outalias" + count, null);
				expression.SetArgument("out_count", count - 1);
			}

			public void Add(SelectOutput item) {
				int count = Count;
				expression.SetArgument("out" + count, item.Expression);
				if (item.Alias != null)
					expression.SetArgument("outalias" + count, item.Alias);
				expression.SetArgument("out_count", count + 1);
			}

			public void Clear() {
				int count = Count;
				for (int i = count - 1; i > 0; i--) {
					expression.SetArgument("out" + i, null);
					expression.SetArgument("outalias" + i, null);
				}
				expression.SetArgument("out_count", 0);
			}

			public bool Contains(SelectOutput item) {
				return IndexOf(item) != -1;
			}

			public void CopyTo(SelectOutput[] array, int arrayIndex) {
				throw new NotImplementedException();
			}

			public bool Remove(SelectOutput item) {
				int index = IndexOf(item);
				if (index == -1)
					return false;
				
				RemoveAt(index);
				return true;
			}

			public IEnumerator<SelectOutput> GetEnumerator() {
				return new Enumerator(this);
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			#region Enumerator

			private class Enumerator : IEnumerator<SelectOutput> {
				private int index = -1;
				private int count;
				private readonly OutputList list;

				public Enumerator(OutputList list) {
					this.list = list;
					count = list.Count;
				}

				public void Dispose() {
				}

				public bool MoveNext() {
					return ++index < count;
				}

				public void Reset() {
					index = -1;
					count = list.Count;
				}

				public SelectOutput Current {
					get { return list[index]; }
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