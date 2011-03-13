using System;

namespace Deveel.Data.Sql {
	public sealed partial class SqlObject {
		// String Functions

		public SqlObject ToLower() {
			return SystemFunctions.Lower(new SqlObject[] {this});
		}

		public SqlObject ToUpper() {
			return SystemFunctions.Upper(new SqlObject[] {this});
		}

		public SqlObject Concat(SqlObject other) {
			return Concat(new SqlObject[] { other});
		}

		public SqlObject Concat(SqlObject[] others) {
			SqlObject[] args = new SqlObject[others.Length + 1];
			args[0] = this;
			Array.Copy(others, 0, args, 1, others.Length);
			return SystemFunctions.Concat(args);
		}

		public SqlObject Trim(StringTrimType trimType, string content) {
			return SystemFunctions.Trim(new SqlObject[] {new SqlObject(trimType.ToString().ToLower()), this, new SqlObject(content)});
		}

		public SqlObject TrimBoth(string content) {
			return Trim(StringTrimType.Both, content);
		}

		public SqlObject TrimTrailing(string content) {
			return Trim(StringTrimType.Trailing, content);
		}

		public SqlObject TrimLeading(string content) {
			return Trim(StringTrimType.Leading, content);
		}

		public SqlObject LeftTrim() {
			return SystemFunctions.LTrim(new SqlObject[] {this});
		}

		public SqlObject RightTrim() {
			return SystemFunctions.RTrim(new SqlObject[] {this});
		}

		public SqlObject Substring(int offset, int count) {
			return SystemFunctions.Substring(new SqlObject[] {this, new SqlObject(offset), new SqlObject(count)});
		}

		public SqlObject Substring(int offset) {
			return SystemFunctions.Substring(new SqlObject[] {this, new SqlObject(offset)});
		}

		// Math Functions

		public SqlObject Abs() {
			return SystemFunctions.Abs(new SqlObject[] {this});
		}

		public SqlObject Pow(int n) {
			return SystemFunctions.Pow(new SqlObject[] {this, new SqlObject(n)});
		}

		public SqlObject Sqrt() {
			return SystemFunctions.Sqrt(new SqlObject[] {this});
		}

		public SqlObject Sign() {
			return SystemFunctions.Sign(new SqlObject[] {this});
		}

		public SqlObject Round() {
			return SystemFunctions.Round(new SqlObject[] {this});
		}

		public SqlObject Modulo(BigNumber n) {
			return SystemFunctions.Modulo(new SqlObject[] {this, new SqlObject(n) });
		}

		public SqlObject Add(BigNumber n) {
			return SystemFunctions.Add(new SqlObject[] {this, new SqlObject(n) });
		}

		public SqlObject Subtract(BigNumber n) {
			return SystemFunctions.Subtract(new SqlObject[] {this, new SqlObject(n)});
		}

		public SqlObject Multiply(BigNumber n) {
			return SystemFunctions.Multiply(new SqlObject[] {this, new SqlObject(n) });
		}

		public SqlObject Divide(BigNumber n) {
			return SystemFunctions.Divide(new SqlObject[] {this, new SqlObject(n)});
		}

		// Logical Functions

		public SqlObject Not() {
			return SystemFunctions.Not(new SqlObject[] {this});
		}

		public SqlObject Or(SqlObject other) {
			return SystemFunctions.Or(new SqlObject[]{this, other});
		}

		public SqlObject And(SqlObject other) {
			return SystemFunctions.And(new SqlObject[] {this, other});
		}

		public SqlObject Is(SqlObject other) {
			return SystemFunctions.Is(new SqlObject[] {this, other});
		}

		public SqlObject IsNot(SqlObject other) {
			return SystemFunctions.IsNot(new SqlObject[] {this, other});
		}

		public SqlObject LesserThan(SqlObject other) {
			return SystemFunctions.LesserThan(new SqlObject[] {this, other});
		}

		public SqlObject LesserOrEqualThan(SqlObject other) {
			return SystemFunctions.LesserOrEqualThan(new SqlObject[] { this, other });
		}

		public SqlObject GreaterThan(SqlObject other) {
			return SystemFunctions.GreaterThan(new SqlObject[] { this, other });
		}

		public SqlObject GreaterOrEqualThan(SqlObject other) {
			return SystemFunctions.GreaterOrEqualThan(new SqlObject[] { this, other });
		}

		// Operators

		public static SqlObject operator +(SqlObject a, SqlObject b) {
			return a.Add(b.Value.ToNumber());
		}

		public static SqlObject operator +(SqlObject a, BigNumber b) {
			return a.Add(b);
		}

		public static SqlObject operator -(SqlObject a, SqlObject b) {
			return a.Subtract(b.Value.ToNumber());
		}

		public static SqlObject operator -(SqlObject a, BigNumber b) {
			return a.Subtract(b);
		}

		public static SqlObject operator *(SqlObject a, SqlObject b) {
			return a.Multiply(b.Value.ToNumber());
		}

		public static SqlObject operator *(SqlObject a, BigNumber b) {
			return a.Multiply(b);
		}

		public static SqlObject operator /(SqlObject a, SqlObject b) {
			return a.Divide(b.Value.ToNumber());
		}

		public static SqlObject operator /(SqlObject a, BigNumber b) {
			return a.Divide(b);
		}

		public static SqlObject operator %(SqlObject a, SqlObject b) {
			return a.Modulo(b.Value.ToNumber());
		}

		public static SqlObject operator %(SqlObject a, BigNumber b) {
			return a.Modulo(b);
		}

		public static SqlObject operator ^(SqlObject a, SqlObject b) {
			return a.Pow(b.Value.ToNumber().ToInt32());
		}

		public static SqlObject operator ^(SqlObject a, int b) {
			return a.Pow(b);
		}

		public static SqlObject operator ++(SqlObject a) {
			return new SqlObject(a.Value.ToNumber().Add(BigNumber.One));
		}

		public static SqlObject operator --(SqlObject a) {
			return new SqlObject(a.Value.ToNumber().Subtract(BigNumber.One));
		}

		// Logical Operators

		public static SqlObject operator !(SqlObject a) {
			return a.Not();
		}

		public static SqlObject operator >=(SqlObject a, SqlObject b) {
			return a.GreaterOrEqualThan(b);
		}

		public static SqlObject operator <=(SqlObject a, SqlObject b) {
			return a.LesserOrEqualThan(b);
		}

		public static SqlObject operator >(SqlObject a, SqlObject b) {
			return a.GreaterThan(b);
		}

		public static SqlObject operator <(SqlObject a, SqlObject b) {
			return a.LesserThan(b);
		}

		//TODO: == and !=

		public static SqlObject operator |(SqlObject a, SqlObject b) {
			return a.Or(b);
		}

		public static SqlObject operator &(SqlObject a, SqlObject b) {
			return a.And(b);
		}

		public static bool operator true(SqlObject a) {
			if (!a.Type.IsBoolean)
				throw new InvalidOperationException("The object is not a BOOLEAN.");

			bool? value = a.Value.ToBoolean();
			return value.HasValue && value.Value;
		}

		public static bool operator false(SqlObject a) {
			if (!a.Type.IsBoolean)
				throw new InvalidOperationException("The object is not a BOOLEAN.");

			bool? value = a.Value.ToBoolean();
			return value.HasValue && !value.Value;
		}
	}
}