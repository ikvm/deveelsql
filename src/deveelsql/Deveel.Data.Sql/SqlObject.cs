using System;
using System.Diagnostics;
using System.Globalization;

namespace Deveel.Data.Sql {
	[DebuggerDisplay("{ToObject():Type}")]
	public sealed partial class SqlObject : IComparable, IComparable<SqlObject>, IEquatable<SqlObject> {
		private readonly SqlType type;
		private readonly SqlValue value;
		private readonly SqlValue[] values;

		public static readonly SqlObject Null = new SqlObject(SqlType.Null, SqlValue.Null);

		public static readonly SqlObject True = new SqlObject(SqlType.Boolean, SqlValue.FromBoolean(true));
		public static readonly SqlObject False = new SqlObject(SqlType.Boolean, SqlValue.FromBoolean(false));

		internal SqlObject(SqlType type, SqlValue value) {
			this.type = type;
			this.value = value;
		}

		public SqlObject(object value)
			: this(SqlType.TypeOf(value), SqlValue.FromObject(value)) {
		}

		internal SqlObject(SqlCompositeType type, SqlValue[] values) {
			this.type = type;
			this.values = values;
		}

		public SqlType Type {
			get { return type; }
		}

		internal SqlValue Value {
			get { return value; }
		}

		internal SqlValue[] Values {
			get { return values; }
		}

		public bool IsComposite {
			get { return type is SqlCompositeType; }
		}

		public bool IsNull {
			get { return type.TypeCode == SqlTypeCode.Null || value.IsNull; }
		}

		private static int CompareNull(object i1, object i2) {
			return i1 == null ? (i2 == null ? 0 : -1) : 1;
		}

		private static int CompareBoolean(SqlValue v1, SqlValue v2) {
			bool? b1 = v1.ToBoolean();
			bool? b2 = v2.ToBoolean();
			return b1 == null || b2 == null ? CompareNull(b1, b2) : (b1.Equals(b2) ? 0 : (b1.Equals(true) ? 1 : -1));
		}

		private static int CompareNumber(BigNumber n1, BigNumber n2) {
			return n1 == null || n2 == null ? CompareNull(n1, n2) : n1.CompareTo(n2);
		}

		private static int CompareString(CompareInfo collator, SqlValue v1, SqlValue v2) {
			String s1 = v1.ToString();
			String s2 = v2.ToString();
			return s1 == null || s2 == null
					? CompareNull(s1, s2)
					: (collator == null ? s1.CompareTo(s2) : collator.Compare(s1, s2));
		}

		public int CompareTo(SqlObject other) {
			if (other == null)
				return -1;

			SqlType t1 = Type;
			SqlType t2 = other.Type;
			bool null1 = t1.IsNull;
			bool null2 = t2.IsNull;

			// If both null types,
			if (null1 && null2)
				// Both are null, therefore compare equally,
				return 0;

			// If val1 is null, choose the type of val2
			SqlType t = null1 ? t2 : t1;

			if (t.IsNumeric) {
				BigNumber v1 = ToNumber();
				BigNumber v2 = other.ToNumber();
				return CompareNumber(v1, v2);
			}

			if (t.IsString) {
				CompareInfo c = null;
				if (t.HasLocale)
					c = t.Locale.CompareInfo;
				return CompareString(c, Value, other.Value);
			}
			if (t.IsBoolean)
				return CompareBoolean(Value, other.Value);

			if (!type.IsComparableTo(other.type))
				throw new ArgumentException("The type '" + type + "' is incompatible with '" + other.type + "'.");

			object value1 = value.ToObject();
			object value2 = other.value.ToObject();

			return type.Compare(value1, value2);
		}

		public static SqlObject MakeNull(SqlType type) {
			return new SqlObject(type, SqlValue.GetNull(type));
		}

		public static SqlObject MakeComposite(Array values) {
			if (values == null)
				return new SqlObject(new SqlCompositeType(new SqlType[] {SqlType.Null}), new SqlValue[0]);

			int sz = values.Length;
			SqlValue[] sqlValues = new SqlValue[sz];
			SqlType[] sqlTypes = new SqlType[sz];
			for (int i = 0; i < sz; i++) {
				object value = values.GetValue(i);

				sqlValues[i] = SqlValue.FromObject(value);
				sqlTypes[i] = SqlType.TypeOf(value);
			}

			return new SqlObject(new SqlCompositeType(sqlTypes), sqlValues);
		}

		public static SqlObject MakeComposite(SqlObject[] values) {
			if (values == null)
				return new SqlObject(new SqlCompositeType(new SqlType[] { SqlType.Null }), new SqlValue[0]);

			int sz = values.Length;
			SqlValue[] sqlValues = new SqlValue[sz];
			SqlType[] sqlTypes = new SqlType[sz];
			for (int i = 0; i < sz; i++) {
				SqlObject value = values[i];

				sqlValues[i] = value.Value;
				sqlTypes[i] = value.Type;
			}

			return new SqlObject(new SqlCompositeType(sqlTypes), sqlValues);
		}

		public SqlObject CastTo(SqlType destType) {
			if (type.Equals(destType))
				return this;

			if (value == null || destType.IsNull)
				return MakeNull(type);

			SqlObject destValue;

			try {
				destValue = destType.Cast(this);
			} catch (Exception e) {
				throw new ArgumentException("Unable to cast value type " + type + " to " + destType, e);
			}

			return destValue;
		}

		public static int Compare(SqlObject[] val1, SqlObject[] val2) {
			// Compare until we reach the end of the array.
			int min_compare = System.Math.Min(val1.Length, val2.Length);
			for (int i = 0; i < min_compare; ++i) {
				int c = Compare(val1[i], val2[i]);
				if (c != 0)
					return c;
			}

			// If the sizes are equal, compare equally,
			if (val1.Length == val2.Length)
				return 0;

			// If val1.Length is greater, return +1, else return -1 (val1.Length if
			// less)
			return (val1.Length > val2.Length) ? 1 : -1;
		}

		public static int Compare(SqlObject val1, SqlObject val2) {
			// Null compares,
			if (val1.IsNull)
				return val2.IsNull ? 0 : -1;
			if (val2.IsNull)
				return 1;


			// Check the types are equal,
			SqlType type = val1.Type;
			if (!type.IsComparableTo(val2.Type))
				throw new ApplicationException("Unable to compare " + type + " with " + val2.Type);

			// If it's a string, we need to consider its collation rules,
			if (type.IsString) {
				SqlValue v1 = val1.Value;
				SqlValue v2 = val2.Value;

				// Turn the objects into string values,
				string strv1 = v1.ToString();
				string strv2 = v2.ToString();

				// If the collation is lexicographical,
				if (!type.HasLocale)
					return strv1.CompareTo(strv2);

				return type.Locale.CompareInfo.Compare(strv1, strv2);
			}

			// Otherwise, it should be a straightforward object type,

			object obv1 = val1.Value.ToObject();
			object obv2 = val2.Value.ToObject();

			// Both values are none null,
			IComparable obc1 = (IComparable)obv1;
			IComparable obc2 = (IComparable)obv2;

			return obc1.CompareTo(obc2);

		}

		int IComparable.CompareTo(object obj) {
			SqlObject other = obj as SqlObject;
			if (other == null)
				return -1;

			return CompareTo(other);
		}

		public bool Equals(SqlObject other) {
			if (other == null)
				return false;

			return type.Equals(other.type) && value.Equals(other.value);
		}

		public override bool Equals(object obj) {
			SqlObject other = obj as SqlObject;
			if (other == null)
				return false;

			return Equals(other);
		}

		public override int GetHashCode() {
			return type.GetHashCode() ^ value.GetHashCode();
		}

		public static SqlObject CastTo(SqlObject value, SqlType destType) {
			if (destType == null)
				throw new ArgumentNullException("destType");

			return destType.Cast(value);
		}

		internal static SqlObject CompositeString(string[] array) {
			int len = array.Length;
			SqlType[] compArray = new SqlType[len];
			SqlValue[] values = new SqlValue[len];
			for (int i = 0; i < len; ++i) {
				compArray[i] = SqlType.String;
				values[i] = SqlValue.FromString(array[i]);
			}

			return new SqlObject(new SqlCompositeType(compArray), values);
		}
	}
}