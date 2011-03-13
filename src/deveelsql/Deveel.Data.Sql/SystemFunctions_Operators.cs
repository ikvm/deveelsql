using System;
using System.Reflection;

namespace Deveel.Data.Sql {
	internal static partial class SystemFunctions {
		private static SqlObject ArithmaticProcess(SqlObject[] args, ArithProc proc) {
			SqlType t1 = args[0].Type;
			SqlType t2 = args[1].Type;
			BigNumber arg1 = args[0].Value.ToNumber();
			BigNumber arg2 = args[1].Value.ToNumber();

			SqlType returnType = t1.Widest(t2);

			if (arg1 == null || arg2 == null)
				return SqlObject.MakeNull(returnType);

			return new SqlObject(returnType.Cast(proc(arg1, arg2)));
		}

		public static SqlObject Equal(SqlObject[] args) {
			if (IsComparableTypes(args[0], args[1])) {
				int c = SqlObject.Compare(args[0], args[1]);
				return AsBooleanValue(c == 0);
			}

			// Return null boolean,
			return SqlObject.MakeNull(SqlType.Boolean);
		}

		public static SqlObject NotEqual(SqlObject[] args) {
			if (IsComparableTypes(args[0], args[1])) {
				int c = SqlObject.Compare(args[0], args[1]);
				return AsBooleanValue(c != 0);
			}

			// Return null boolean,
			return SqlObject.MakeNull(SqlType.Boolean);
		}

		public static SqlObject GreaterThan(SqlObject[] args) {
			if (IsComparableTypes(args[0], args[1])) {
				int c = SqlObject.Compare(args[0], args[1]);
				return AsBooleanValue(c > 0);
			}

			// Return null boolean,
			return SqlObject.MakeNull(SqlType.Boolean);
		}

		public static SqlObject GreaterOrEqualThan(SqlObject[] args) {
			if (IsComparableTypes(args[0], args[1])) {
				int c = SqlObject.Compare(args[0], args[1]);
				return AsBooleanValue(c >= 0);
			}

			// Return null boolean,
			return SqlObject.MakeNull(SqlType.Boolean);
		}

		public static SqlObject LesserThan(SqlObject[] args) {
			if (IsComparableTypes(args[0], args[1])) {
				int c = SqlObject.Compare(args[0], args[1]);
				return AsBooleanValue(c < 0);
			}

			// Return null boolean,
			return SqlObject.MakeNull(SqlType.Boolean);
		}

		public static SqlObject LesserOrEqualThan(SqlObject[] args) {
			if (IsComparableTypes(args[0], args[1])) {
				int c = SqlObject.Compare(args[0], args[1]);
				return AsBooleanValue(c <= 0);
			}

			// Return null boolean,
			return SqlObject.MakeNull(SqlType.Boolean);
		}

		public static SqlObject Is(SqlObject[] args) {
			if (IsComparableTypes(args[0], args[1])) {
				int c = SqlObject.Compare(args[0], args[1]);
				return AsBooleanValue(c == 0);
			}

			// Return null boolean,
			return SqlObject.MakeNull(SqlType.Boolean);
		}

		public static SqlObject IsNot(SqlObject[] args) {
			if (IsComparableTypes(args[0], args[1])) {
				int c = SqlObject.Compare(args[0], args[1]);
				return AsBooleanValue(c != 0);
			}

			// Return null boolean,
			return SqlObject.MakeNull(SqlType.Boolean);
		}

		public static SqlObject And(SqlObject[] args) {
			if (IsBooleanType(args[0]) && IsBooleanType(args[1])) {
				bool? b1 = args[0].Value.ToBoolean();
				bool? b2 = args[1].Value.ToBoolean();
				return b1 == null || b2 == null ? SqlObject.MakeNull(SqlType.Boolean) : AsBooleanValue(b1.Value && b2.Value);
			}
			// Return null boolean,
			return SqlObject.MakeNull(SqlType.Boolean);
		}

		public static SqlObject Or(SqlObject[] args) {
			if (IsBooleanType(args[0]) && IsBooleanType(args[1])) {
				bool? b1 = args[0].Value.ToBoolean();
				bool? b2 = args[1].Value.ToBoolean();
				if (b1 == null)
					return AsBooleanValue(b2.Value);
				if (b2 == null)
					return AsBooleanValue(b1.Value);
				
				return AsBooleanValue(b1.Value || b2.Value);
			}

			// Return null boolean,
			return SqlObject.MakeNull(SqlType.Boolean);
		}

		public static SqlObject Not(SqlObject[] args) {
			if (IsBooleanType(args[0])) {
				bool? b = args[0].Value.ToBoolean();
				return b == null ? SqlObject.MakeNull(SqlType.Boolean) : AsBooleanValue(!b.Value);
			}
			// Return null boolean,
			return SqlObject.MakeNull(SqlType.Boolean);
		}

		public static SqlObject Add(SqlObject[] args) {
			return ArithmaticProcess(args, delegate(BigNumber arg1, BigNumber arg2) {
				return arg1.Add(arg2);
			});
		}

		public static SqlObject Subtract(SqlObject[] args) {
			return ArithmaticProcess(args, delegate(BigNumber arg1, BigNumber arg2) {
				return arg1.Subtract(arg2);
			});
		}

		public static SqlObject Multiply(SqlObject[] args) {
			return ArithmaticProcess(args, delegate(BigNumber arg1, BigNumber arg2) {
				return arg1.Multiply(arg2);
			});
		}

		public static SqlObject Divide(SqlObject[] args) {
			return ArithmaticProcess(args, delegate(BigNumber arg1, BigNumber arg2) {
				return arg1.Divide(arg2);
			});
		}

		private delegate BigNumber ArithProc(BigNumber arg1, BigNumber arg2);

		public static SqlObject Like(SqlObject[] args) {
			// If either the pattern or the str are null, return null
			if (args[0].IsNull || args[1].IsNull)
				return SqlObject.Null;

			// Otherwise pass to the PatternSearch method
			string str = args[0].ToString();
			string pattern = args[1].ToString();
			return new SqlObject(PatternSearch.Match(pattern, str, '\\'));
		}

		public static SqlObject NotLike(SqlObject[] args) {
			return Not(new SqlObject[] { Like(args) });
		}

		private delegate SqlObject NestedEvaluate(SqlObject[] args);

		private static ITableDataSource NestedAnyScan(QueryProcessor processor, Expression leftExp, Expression rightExp, NestedEvaluate evaluate) {
			// Evaluate the left and right side of the operation
			ITableDataSource left = processor.Execute(leftExp);
			ITableDataSource right = processor.Execute(rightExp);
			// Turn left into a SqlObject
			SqlObject leftOb = QueryProcessor.Result(left)[0];
			// Scan right, return true on the first that's equal
			IRowCursor i = right.GetRowCursor();
			try {
				while (i.MoveNext()) {
					long rowid = i.Current;
					SqlObject scanVal = right.GetValue(0, rowid);
					SqlObject[] args = new SqlObject[] { leftOb, scanVal };
					SqlObject r = evaluate(args);
					if (Equals(r, new SqlObject(true))) {
						return QueryProcessor.ResultTable(new SqlObject(true));
					}
				}
			} catch (AccessViolationException e) {
				throw new ApplicationException(e.Message, e);
			} catch (TargetInvocationException e) {
				throw new ApplicationException(e.InnerException.Message, e.InnerException);
			}

			return QueryProcessor.ResultTable(new SqlObject(false));
		}

		private static ITableDataSource NestedAllScan(QueryProcessor processor, Expression leftExp, Expression rightExp, NestedEvaluate evaluate) {
			// Evaluate the left and right side of the operation
			ITableDataSource left = processor.Execute(leftExp);
			ITableDataSource right = processor.Execute(rightExp);
			// Turn left into a TObject
			SqlObject leftOb = QueryProcessor.Result(left)[0];
			// Scan right, return true on the first that's equal
			IRowCursor i = right.GetRowCursor();
			try {
				while (i.MoveNext()) {
					long rowid = i.Current;
					SqlObject scanVal = right.GetValue(0, rowid);
					SqlObject[] args = new SqlObject[] { leftOb, scanVal };
					SqlObject r = evaluate(args);
					if (Equals(r, new SqlObject(false))) {
						return QueryProcessor.ResultTable(new SqlObject(false));
					}
				}
			} catch (AccessViolationException e) {
				throw new ApplicationException(e.Message, e);
			} catch (TargetInvocationException e) {
				throw new ApplicationException(e.InnerException.Message, e.InnerException);
			}

			return QueryProcessor.ResultTable(new SqlObject(true));
		}

		// ANY functions
		public static ITableDataSource AnyEqual(QueryProcessor processor, Expression[] args) {
			return NestedAnyScan(processor, args[0], args[1], Equal);
		}

		public static ITableDataSource AnyNotEqual(QueryProcessor processor, Expression[] args) {
			return NestedAnyScan(processor, args[0], args[1], NotEqual);
		}

		public static ITableDataSource AnyGreaterThan(QueryProcessor processor, Expression[] args) {
			return NestedAnyScan(processor, args[0], args[1], GreaterThan);
		}

		public static ITableDataSource AnyLesserThan(QueryProcessor processor, Expression[] args) {
			return NestedAnyScan(processor, args[0], args[1], LesserThan);
		}

		public static ITableDataSource AnyGreaterOrEqualThan(QueryProcessor processor, Expression[] args) {
			return NestedAnyScan(processor, args[0], args[1], GreaterOrEqualThan);
		}

		public static ITableDataSource AnyLesserOrEqualThan(QueryProcessor processor, Expression[] args) {
			return NestedAnyScan(processor, args[0], args[1], LesserOrEqualThan);
		}

		// ALL functions
		public static ITableDataSource AllEqual(QueryProcessor processor, Expression[] args) {
			return NestedAllScan(processor, args[0], args[1], Equal);
		}
		public static ITableDataSource AllNotEqual(QueryProcessor processor, Expression[] args) {
			return NestedAllScan(processor, args[0], args[1], NotEqual);
		}

		public static ITableDataSource AllGreaterThan(QueryProcessor processor, Expression[] args) {
			return NestedAllScan(processor, args[0], args[1], GreaterThan);
		}

		public static ITableDataSource AllLesserThan(QueryProcessor processor, Expression[] args) {
			return NestedAllScan(processor, args[0], args[1], LesserThan);
		}

		public static ITableDataSource AllGreaterOrEqualThan(QueryProcessor processor, Expression[] args) {
			return NestedAllScan(processor, args[0], args[1], GreaterOrEqualThan);
		}

		public static ITableDataSource AllLesserOrEqualThan(QueryProcessor processor, Expression[] args) {
			return NestedAllScan(processor, args[0], args[1], LesserOrEqualThan);
		}

		public static ITableDataSource Exists(QueryProcessor processor, Expression[] args) {
			// The nested expression to evaluate
			Expression nested_op = args[0];
			// Execute the nested operation,
			ITableDataSource result = processor.Execute(nested_op);
			// If there are elements, then 'exists' is true
			return QueryProcessor.ResultTable(result.RowCount > 0 ? SqlObject.True : SqlObject.False);
		}
	}
}