using System;
using System.Globalization;
using System.Text;

using Deveel.Math;

namespace Deveel.Data.Sql {
	internal static partial class SystemFunctions {
		private static bool IsComparableTypes(SqlObject v1, SqlObject v2) {
			SqlType t1 = v1.Type;
			SqlType t2 = v2.Type;
			return t1.IsComparableTo(t2);
		}

		private static SqlObject AsBooleanValue(bool b) {
			return new SqlObject(b);
		}

		private static bool IsBooleanType(SqlObject v) {
			return v.Type.IsBoolean;
		}

		public static SqlObject Abs(SqlObject[] args) {
			SqlObject obj = args[0];
			return obj.IsNull ? obj : new SqlObject(obj.Value.ToNumber().Abs());
		}

		public static SqlObject Sign(SqlObject[] args) {
			SqlObject obj = args[0];
			return obj.IsNull ? obj : new SqlObject(obj.Value.ToNumber().Signum());
		}

		public static SqlObject Modulo(SqlObject[] args) {
			SqlObject ob1 = args[0];
			SqlObject ob2 = args[1];
			if (ob1.IsNull)
				return ob1;
			if (ob2.IsNull)
				return ob2;
			
			BigNumber v = ob1.Value.ToNumber();
			BigNumber m = ob2.Value.ToNumber();
			return new SqlObject(v.Modulus(m));
		}

		public static SqlObject Round(SqlObject[] args) {
			SqlObject ob1 = args[0];
			if (ob1.IsNull)
				return ob1;

			BigNumber v = ob1.Value.ToNumber();
			int d = 0;
			if (args.Length == 2) {
				SqlObject ob2 = args[1];
				if (ob2.IsNull) {
					d = 0;
				} else {
					d = ob2.Value.ToNumber().ToInt32();
				}
			}
			return new SqlObject(v.SetScale(d, RoundingMode.HalfUp));
		}

		public static SqlObject Pow(SqlObject[] args) {
			SqlObject ob1 = args[0];
			SqlObject ob2 = args[1];
			if (ob1.IsNull)
				return ob1;
			if (ob2.IsNull)
				return ob2;

			BigNumber v = ob1.Value.ToNumber();
			BigNumber w = ob2.Value.ToNumber();
			return new SqlObject(v.Pow(w.ToInt32()));
		}

		public static SqlObject Sqrt(SqlObject[] args) {
			SqlObject ob = args[0];
			return ob.IsNull ? ob : new SqlObject(ob.Value.ToNumber().Sqrt());
		}

		public static ITableDataSource Least(QueryProcessor processor, Expression[] args) {
			SqlObject least = null;
			for (int i = 0; i < args.Length; ++i) {
				SqlObject ob = QueryProcessor.Result(processor.Execute(args[i]))[0];
				if (ob.IsNull)
					return QueryProcessor.ResultTable(ob);

				if (least == null || SqlObject.Compare(ob, least) < 0)
					least = ob;
			}

			return QueryProcessor.ResultTable(least);
		}

		public static ITableDataSource Greatest(QueryProcessor processor, Expression[] args) {
			SqlObject most = null;
			for (int i = 0; i < args.Length; ++i) {
				SqlObject ob = QueryProcessor.Result(processor.Execute(args[i]))[0];
				if (ob.IsNull)
					return QueryProcessor.ResultTable(ob);

				if (most == null || SqlObject.Compare(ob, most) > 0)
					most = ob;
			}

			return QueryProcessor.ResultTable(most);
		}

		public static SqlObject Lower(SqlObject[] args) {
			SqlObject str = args[0];
			if (str.IsNull)
				return str;

			// Get the locale of the string
			SqlType type = str.Type;
			if (!type.IsString)
				// Not a string type, so return null
				return SqlObject.MakeNull(type);

			CultureInfo locale = type.Locale;

			// If null locale then default locale is invariant
			if (locale == null)
				locale = CultureInfo.InvariantCulture;

			return new SqlObject(type, SqlValue.FromString(str.Value.ToString().ToLower(locale)));
		}

		public static SqlObject Upper(SqlObject[] args) {
			SqlObject str = args[0];
			if (str.IsNull)
				return str;

			// Get the locale of the string
			SqlType type = str.Type;
			if (!type.IsString)
				// Not a string type, so return null
				return SqlObject.MakeNull(type);

			CultureInfo locale = type.Locale;

			// If null locale then default locale is invariant
			if (locale == null)
				locale = CultureInfo.InvariantCulture;

			return new SqlObject(type, SqlValue.FromString(str.Value.ToString().ToUpper(locale)));

		}

		public static SqlObject Concat(SqlObject[] args) {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < args.Length; ++i) {
				sb.Append(args[i].Value.ToString());
			}
			return new SqlObject(sb.ToString());
		}


		public static ITableDataSource Length(QueryProcessor processor, Expression[] args) {
			if (args.Length != 1)
				throw new ArgumentException("The function LENGTH accepts only 1 argument.");

			Expression arg = args[0];

			SqlObject resultLength;
			SqlObject obj = QueryProcessor.Result(processor.Execute(arg))[0];
			if (obj.IsNull) {
				resultLength = SqlObject.MakeNull(SqlType.Numeric);
			} else {
				int length;
				SqlType obType = obj.Type;
				SqlValue obValue = obj.Value;
				// If it's a string,
				if (obType.IsString) {
					length = obValue.ToString().Length;
				}
					// If it's a binary,
				else if (obType.IsBinary) {
					length = obValue.Length - 1;
				}
					// Otherwise, return null,
				else {
					length = -1;
				}

				resultLength = length == -1 ? SqlObject.MakeNull(SqlType.Numeric) : new SqlObject((long) length);
			}

			return QueryProcessor.ResultTable(resultLength);
		}

		public static ITableDataSource CharLength(QueryProcessor processor, Expression[] args) {
			return Length(processor, args);
		}

		public static ITableDataSource BitLength(QueryProcessor processor, Expression[] args) {
			SqlObject ob = QueryProcessor.Result(Length(processor, args))[0];
			SqlObject eight = new SqlObject(8L);
			return QueryProcessor.ResultTable(Add(new SqlObject[] { ob, eight }));
		}

		public static SqlObject Trim(SqlObject[] args) {
			// The type of trim (leading, both, trailing)
			SqlObject ttype = args[0];
			// Characters to trim
			SqlObject cob = args[1];
			if (cob.IsNull)
				return cob;
			if (ttype.IsNull)
				return SqlObject.MakeNull(SqlType.String);

			string characters = cob.Value.ToString();
			string ttypeStr = ttype.Value.ToString();

			// The content to trim.
			SqlObject ob = args[2];
			if (ob.IsNull)
				return ob;

			string str = ob.Value.ToString();

			int skip = characters.Length;
			// Do the trim,
			if (ttypeStr.Equals("leading") || ttypeStr.Equals("both")) {
				// Trim from the start.
				int scan = 0;
				while (scan < str.Length && 
					str.IndexOf(characters, scan) == scan) {
					scan += skip;
				}
				str = str.Substring(System.Math.Min(scan, str.Length));
			}
			if (ttypeStr.Equals("trailing") || ttypeStr.Equals("both")) {
				// Trim from the end.
				int scan = str.Length - 1;
				int i = str.LastIndexOf(characters, scan);
				while (scan >= 0 && i != -1 && i == scan - skip + 1) {
					scan -= skip;
					i = str.LastIndexOf(characters, scan);
				}
				str = str.Substring(0, System.Math.Max(0, scan + 1));
			}

			return new SqlObject(str);
		}

		public static SqlObject LTrim(SqlObject[] args) {
			SqlObject ob = args[0];
			if (ob.IsNull)
				return ob;

			string str = ob.Value.ToString();

			// Do the trim,
			// Trim from the start.
			int scan = 0;
			while (scan < str.Length && str.IndexOf(' ', scan) == scan) {
				scan += 1;
			}

			return new SqlObject(str.Substring(System.Math.Min(scan, str.Length)));
		}

		public static SqlObject RTrim(SqlObject[] args) {
			SqlObject ob = args[0];
			if (ob.IsNull)
				return ob;

			string str = ob.Value.ToString();

			// Do the trim,
			// Trim from the end.
			int scan = str.Length - 1;
			int i = str.LastIndexOf(" ", scan);
			while (scan >= 0 && i != -1 && i == scan - 2) {
				scan -= 1;
				i = str.LastIndexOf(" ", scan);
			}

			return new SqlObject(str.Substring(0, System.Math.Max(0, scan + 1)));
		}

		public static SqlObject Substring(SqlObject[] args) {
			SqlObject ob = args[0];
			if (ob.IsNull)
				return ob;

			string str = ob.Value.ToString();
			int pcount = args.Length;
			int strLength = str.Length;
			int arg1 = 1;
			int arg2 = strLength;
			if (pcount >= 2) {
				if (args[1].IsNull)
					return SqlObject.MakeNull(SqlType.String);

				arg1 = args[1].Value.ToNumber().ToInt32();
			}

			if (pcount >= 3) {
				if (args[2].IsNull)
					return SqlObject.MakeNull(SqlType.String);

				arg2 = args[2].Value.ToNumber().ToInt32();
			}

			// Make sure this call is safe for all lengths of string.
			if (arg1 < 1)
				arg1 = 1;

			if (arg1 > strLength)
				return new SqlObject(String.Empty);

			if (arg2 + arg1 > strLength)
				arg2 = (strLength - arg1) + 1;
			
			if (arg2 < 1)
				return new SqlObject(String.Empty);

			return new SqlObject(str.Substring(arg1 - 1, ((arg1 + arg2) - 1) - (arg1 - 1)));
		}

		public static ITableDataSource If(QueryProcessor processor, Expression[] args) {
			SqlObject[] conditional = QueryProcessor.Result(processor.Execute(args[0]));
			// If it evaluated to true,
			bool? b = conditional[0].Value.ToBoolean();
			return b != null && b == true ? processor.Execute(args[1]) : processor.Execute(args[2]);
		}
	}
}