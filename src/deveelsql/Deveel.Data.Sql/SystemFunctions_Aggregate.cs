using System;
using System.Text;

using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	internal partial class SystemFunctions {
		private static ITable ProcessAggregate(QueryProcessor processor, bool distinct, ITable group, Expression[] args, IAggregateInspector aggregator) {
			// Qualify the return type of the parameter
			SqlType type = processor.GetExpressionType(group, args[0]);

			// If an empty group
			if (group.RowCount == 0) {
				// Null return type if group is empty,
				return QueryProcessor.ResultTable(SqlObject.MakeNull(type));
			}

			// Find the distinct subset of group
			if (distinct)
				group = processor.DistinctSubset(group, args);

			// Push the group table onto the processor stack
			processor.PushTable(group);

			// Scan the group table, returning null on a null value
			IRowCursor i = group.GetRowCursor();

			while (i.MoveNext()) {
				RowId rowid = i.Current;
				processor.UpdateTableRow(rowid);
				ITable val = processor.Execute(args[0]);
				SqlObject ob = QueryProcessor.Result(val)[0];
				// If we hit a null value, we ignore it.  SQL-92 apparently says we
				// should generate a warning for nulls that are eliminated by set
				// functions.
				if (!ob.IsNull) {
					aggregator.Accumulate(ob);
				}
			}

			// Pop the table and return the result
			processor.PopTable();
			SqlObject result = aggregator.Result();
			return QueryProcessor.ResultTable(result ?? SqlObject.MakeNull(type));
		}


		public static ITable Count(QueryProcessor processor, bool distinct, ITable group, Expression[] args) {
			// Only 1 argument allowed
			if (args.Length > 1)
				throw new ArgumentException("Only one argument permitted for COUNT function.");

			// If the parameter is a function operation with name "star" then this is
			// a simple group size result
			Expression arg = args[0];
			if (arg.Type == ExpressionType.Function &&
				arg.GetArgument("name").Equals("star")) {
				return QueryProcessor.ResultTable(SqlObject.CastTo(group.RowCount, SqlType.Numeric));
			}

			// Otherwise, if this is a distinct,
			if (distinct) {
				group = processor.DistinctSubset(group, args);
				// The above process removes null values so we return the count,
				return QueryProcessor.ResultTable(SqlObject.CastTo(group.RowCount, SqlType.Numeric));
			}

			// Otherwise, we need to iterate through a count all none null values,
			return ProcessAggregate(processor, false, group, args, new CountAggregateInspector());
		}

		public static ITable Max(QueryProcessor processor, bool distinct, ITable group, Expression[] args) {
			// Aggregate function only can have 1 argument
			if (args.Length > 1)
				throw new ArgumentException("Only one argument permitted for MAX function.");

			return ProcessAggregate(processor, distinct, group, args, new MaxAggregateInspector());
		}

		public static ITable Min(QueryProcessor processor, bool distinct, ITable group, Expression[] args) {
			// Aggregate function only can have 1 argument
			if (args.Length > 1)
				throw new ArgumentException("Only one argument permitted for MIN function.");

			return ProcessAggregate(processor, distinct, group, args, new MinAggregateInspector());
		}

		public static ITable Sum(QueryProcessor processor, bool distinct, ITable group, Expression[] args) {
			// Aggregate function only can have 1 argument
			if (args.Length > 1)
				throw new ArgumentException("Only one argument permitted for SUM function.");

			return ProcessAggregate(processor, distinct, group, args, new SumAggregateInspector());
		}

		public static ITable Avg(QueryProcessor processor, bool distinct, ITable group, Expression[] args) {
			// Aggregate function only can have 1 argument
			if (args.Length > 1)
				throw new ArgumentException("Only one argument permitted for SUM function.");

			return ProcessAggregate(processor, distinct, group, args, new AvgAggregateInspector());
		}

		public static ITable GroupConcat(QueryProcessor processor, bool distinct, ITable group, Expression[] args) {
			// The output string
			StringBuilder return_string = new StringBuilder();

			// Find the distinct subset of group
			if (distinct)
				group = processor.DistinctSubset(group, args);

			// Push the group table onto the processor stack
			processor.PushTable(group);

			// Iterator over the group
			IRowCursor i = group.GetRowCursor();
			bool first = true;
			while (i.MoveNext()) {
				RowId rowid = i.Current;
				processor.UpdateTableRow(rowid);
				foreach (Expression op in args) {
					ITable val = processor.Execute(op);
					SqlObject ob = QueryProcessor.Result(val)[0];
					if (!ob.IsNull) {
						if (!first) {
							return_string.Append(", ");
						}
						return_string.Append(SqlValue.FromObject(ob.Value).ToString());
						first = false;
					}
				}
			}

			// Pop the table and return the result
			processor.PopTable();
			return QueryProcessor.ResultTable(new SqlObject(return_string.ToString()));
		}

		#region CountAggregateInspector

		private class CountAggregateInspector : IAggregateInspector {
			private long count;

			public void Accumulate(SqlObject arg) {
				count++;
			}

			public SqlObject Result() {
				return SqlObject.CastTo(count, SqlType.MakeType(SqlTypeCode.Numeric));
			}
		}

		#endregion

		#region MaxAggregateInspector

		private class MaxAggregateInspector : IAggregateInspector {
			private SqlObject max;

			public void Accumulate(SqlObject arg) {
				if (max == null || SqlObject.Compare(arg, max) > 0)
					max = arg;
			}

			public SqlObject Result() {
				return max;
			}
		}

		#endregion

		#region MinAggregateInspector

		private class MinAggregateInspector : IAggregateInspector {
			private SqlObject min;

			public void Accumulate(SqlObject arg) {
				if (min == null || SqlObject.Compare(arg, min) < 0)
					min = arg;
			}

			public SqlObject Result() {
				return min;
			}
		}

		#endregion

		#region SumAggregateInspector

		private class SumAggregateInspector : IAggregateInspector {
			private SqlObject sum = null;

			public void Accumulate(SqlObject arg) {
				if (sum == null) {
					sum = arg;
				} else {
					sum = Add(new SqlObject[] { sum, arg });
				}
			}

			public SqlObject Result() {
				return sum;
			}
		}

		#endregion

		#region AvgAggregateInspector

		private class AvgAggregateInspector : IAggregateInspector {
			private SqlObject sum = null;
			private long count = 0;

			public void Accumulate(SqlObject arg) {
				if (sum == null) {
					sum = arg;
				} else {
					sum = Add(new SqlObject[] { sum, arg });
				}
				++count;
			}

			public SqlObject Result() {
				if (sum == null) {
					return null;
				} else {
					// Cast to numeric,
					SqlObject sum_val = SqlObject.CastTo(sum, SqlType.Numeric);
					SqlObject count_val = SqlObject.CastTo(count, SqlType.Numeric);

					return Divide(new SqlObject[] { sum_val, count_val });
				}
			}
		}

		#endregion

		#region IAggregateInspector

		private interface IAggregateInspector {
			void Accumulate(SqlObject arg);

			SqlObject Result();
		}

		#endregion
	}
}