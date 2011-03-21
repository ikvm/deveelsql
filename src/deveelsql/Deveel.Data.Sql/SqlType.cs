using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;

namespace Deveel.Data.Sql {
	[DebuggerDisplay("{ToString(),nq}")]
	public class SqlType : IEquatable<SqlType> {
		private readonly SqlTypeCode code;
		private object[] args;
		private CultureInfo cachedLocale;

		public static readonly SqlType Null = new SqlType(SqlTypeCode.Null);

		public static readonly SqlType Boolean = MakeType(SqlTypeCode.Boolean);
		public static readonly SqlType Numeric = MakeType(SqlTypeCode.Numeric);
		public static readonly SqlType String = MakeType(SqlTypeCode.String);
		public static readonly SqlType Binary = MakeType(SqlTypeCode.Binary);
		public static readonly SqlType DateTime = MakeType(SqlTypeCode.DateTime);

		private static readonly string[] SqlDateTimeFormats = new string[] {
		                                                                   	"yyyy-MM-dd HH:mm:ss.z",
		                                                                   	"yyyy-MM-dd HH:mm:ss",
		                                                                   	"yyyy-MM-dd",
		                                                                   	"HH:mm:ss"
		                                                                   };

		private const string SqlDateToStringFormat = "yyyy-MM-dd HH:mm:ss.z";

		internal SqlType(SqlTypeCode code, params object[] args) {
			this.code = code;
			if (args != null)
				this.args = (object[]) args.Clone();
		}

		internal SqlType(SqlTypeCode code)
			: this(code, null) {
		}

		public SqlTypeCode TypeCode {
			get { return code; }
		}

		public bool IsNumeric {
			get { return code == SqlTypeCode.Numeric; }
		}

		public bool IsBoolean {
			get { return code == SqlTypeCode.Boolean; }
		}

		public bool IsString {
			get { return code == SqlTypeCode.String; }
		}

		public bool IsNull {
			get { return code == SqlTypeCode.Null; }
		}

		public bool IsBinary {
			get { return code == SqlTypeCode.Binary; }
		}

		public bool IsUserType {
			get { return code == SqlTypeCode.UserType; }
		}

		public bool IsDateTime {
			get { return code == SqlTypeCode.DateTime; }
		}

		public bool IsSizeable {
			get {
				return code == SqlTypeCode.Numeric ||
				       code == SqlTypeCode.String ||
				       code == SqlTypeCode.Binary;
			}
		}

		public virtual bool IsComparable {
			get { return !IsBinary; }
		}

		public bool HasSize {
			get {
				if (!IsSizeable)
					return false;
				if (args == null || args.Length == 0)
					return false;

				object size = args[0];
				if (!(size is int))
					return false;

				return ((int) size) >= 0;
			}
		}

		public int Size {
			get { return !HasSize ? -1 : (int) args[0]; }
		}

		public bool HasScale {
			get {
				if (code != SqlTypeCode.Numeric)
					return false;
				if (args == null || args.Length < 2)
					return false;

				object scale = args[1];
				return (scale is int);
			}
		}

		public int Scale {
			get { return !HasScale ? -1 : (int) args[1]; }
		}

		public int Precision {
			get { return IsNumeric ? Size : -1; }
		}

		public bool HasLocale {
			get {
				if (!IsString)
					return false;

				if (args == null || args.Length < 2)
					return false;

				return args[1] != null;
			}
		}

		public CultureInfo Locale {
			get {
				if (!HasLocale)
					return null;

				return cachedLocale ?? (cachedLocale = new CultureInfo((string) args[1]));
			}
		}

		public virtual string Name {
			get { return code.ToString().ToUpperInvariant(); }
		}

		private object CastToBoolean(object value) {
			if (value == null || IsNull)
				return null;

			if (value is bool)
				return (bool) value;
			if (value is bool?)
				return ((bool?) value).Value;

			if (value is BigNumber) {
				BigNumber number = (BigNumber) value;
				if (number == BigNumber.Zero)
					return false;
				if (number == BigNumber.One)
					return true;
				return null;
			}
			if (value is string) {
				bool b;
				if (!System.Boolean.TryParse((string)value, out b))
					return null;
				return b;
			}

			throw new InvalidCastException("Cannot cast " + value + " to " + this);
		}


		private object CastToNumeric(object value) {
			if (value == null || IsNull)
				return null;

			if (value is bool)
				// Can't cast number from boolean
				return (bool) value ? BigNumber.One : BigNumber.Zero;
			if (value is BigNumber)
				return value;
			if (value is string)
				return BigNumber.Parse(value.ToString());

			throw new ArgumentException("Cannot cast to " + this);
		}

		private object CastToString(object value) {
			if (value == null || IsNull)
				return null;

			if (value is bool)
				return ((bool) value) ? "true" : "false";

			if (value is BigNumber) {
				string s = value.ToString();
				if (HasSize && s.Length > Size)
					return null;
				return s;
			}

			if (value is string) {
				string s = (string) value;
				if (HasSize && s.Length > Size) {
					s = s.Substring(0, Size);
				}

				return s;
			}

			if (value is DateTime) {
				DateTime d = (DateTime) value;
				string s = d.ToString(SqlDateToStringFormat, CultureInfo.InvariantCulture);
				if (HasSize && s.Length > Size)
					return null;
				return s;
			}

			throw new ArgumentException("Cast failed to " + this);
		}

		private object CastToDateTime(object value) {
			if (value == null || IsNull)
				return null;

			if (value is string) {
				string s = (string) value;
				DateTime d;
				if (!System.DateTime.TryParseExact(s, SqlDateTimeFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out d))
					return null;
				return d;
			}

			throw new InvalidCastException("Cannot cast " + value + " to DATETIME");
		}

		internal virtual void WriteTo(Stream output) {
			BinaryWriter writer = new BinaryWriter(output, Encoding.Unicode);
			if (code == SqlTypeCode.String) {
				writer.Write(Size);
				writer.Write(Locale.Name);
			} else if (code == SqlTypeCode.Binary) {
				writer.Write(Size);
			} else if (code == SqlTypeCode.Numeric) {
				writer.Write(Size);
				writer.Write(Scale);
			}
		}

		internal virtual void ReadFrom(Stream input) {
			BinaryReader reader = new BinaryReader(input, Encoding.Unicode);
			if (code == SqlTypeCode.String) {
				args = new object[2];
				args[0] = reader.ReadInt32();
				args[1] = reader.ReadString();
			} else if (code == SqlTypeCode.Binary) {
				args = new object[1];
				args[0] = reader.ReadInt32();
			} else if (code == SqlTypeCode.Numeric) {
				args = new object[2];
				args[0] = reader.ReadInt32();
				args[1] = reader.ReadInt32();
			}
		}


		public virtual bool IsComparableTo(SqlType type) {
			if (IsString && type.IsString)
				return Equals(type);
			if (IsNumeric && type.IsNumeric)
				return true;
			if (IsBoolean && type.IsBoolean)
				return true;

			// Null types can be compared with anything (the result is null though).
			if (IsNull || type.IsNull)
				return true;

			// Otherwise not comparable,
			return false;
		}

		public virtual int Compare(object x, object y) {
			if (x == null && y == null)
				return 0;
			if (x == null)
				return 1;

			if (IsString && HasLocale)
				return Locale.CompareInfo.Compare((string)x, (string)y);

			IComparable c = x as IComparable;
			if (c == null)
				throw new ArgumentException("Impossible to compare.");

			return c.CompareTo(y);
		}

		public virtual SqlType Widest(SqlType type) {
			// Nulls come first,
			if (IsNull)
				return this;
			if (type.IsNull)
				return type;

			// For numerics,
			if (IsNumeric)
				return this;

			// For strings and booleans,
			if (IsString || IsBoolean)
				// There is no 'wideness' criteria (they will be identical).
				return this;

			throw new ArgumentException("Unknown widest type error");
		}

		private object Cast(object value) {
			if (code == SqlTypeCode.Null)
				return null;

			if (code == SqlTypeCode.Boolean)
				return CastToBoolean(value);
			if (code == SqlTypeCode.Numeric)
				return CastToNumeric(value);
			if (code == SqlTypeCode.String)
				return CastToString(value);
			if (code == SqlTypeCode.DateTime)
				return CastToDateTime(value);

			// not supported by default ...
			throw new InvalidCastException("Cannot cast " + value + " to " + ToString());
		}

		public SqlObject Cast(SqlObject value) {
			if (value.IsNull)
				return SqlObject.Null;

			object obj = value.ToObject();
			obj = Cast(obj);
			return new SqlObject(this, SqlValue.FromObject(obj));
		}

		public byte[] ToBinary() {
			MemoryStream stream = new MemoryStream(256);
			WriteTo(stream);
			stream.Seek(0, SeekOrigin.Begin);
			byte[] buffer = new byte[stream.Length];
			stream.Read(buffer, 0, buffer.Length);
			byte[] finalBuffer = new byte[buffer.Length + 1];
			Array.Copy(buffer, 0, finalBuffer, 1, buffer.Length);
			finalBuffer[0] = (byte)code;
			return finalBuffer;
		}

		public override string ToString() {
			StringBuilder sb = new StringBuilder(code.ToString().ToUpper(CultureInfo.InvariantCulture));
			if (IsNumeric && HasSize) {
				sb.Append("(");
				sb.Append(Size);
				if (HasScale) {
					sb.Append(",");
					sb.Append(Scale);
				}
				sb.Append(")");
			} else if (IsString) {
				bool hasSize = HasSize;
				bool hasLocale = HasLocale;

				if (hasSize || hasLocale) {
					sb.Append("(");
					if (hasSize && hasLocale) {
						sb.Append(Size);
						sb.Append(",");
						sb.Append(Locale.Name);
					} else if (hasSize) {
						sb.Append(Size);
					} else {
						sb.Append(Locale.Name);
					}
					sb.Append(")");
				}
			}

			return sb.ToString();
		}

		public override int GetHashCode() {
			int hashCode = code.GetHashCode();
			if (args != null) {
				for (int i = 0; i < args.Length; i++) {
					object arg = args[i];
					if (arg != null)
						hashCode ^= arg.GetHashCode();
				}
			}
			return hashCode;
		}

		public override bool Equals(object obj) {
			SqlType other = obj as SqlType;

			if (other == null)
				return false;

			return Equals(other);
		}

		public virtual bool Equals(SqlType other) {
			if (other == null)
				return false;

			if (code != other.code)
				return false;

			if (args == null && other.args == null)
				return true;

			if (args == null || other.args == null)
				return false;

			if (args.Length != other.args.Length)
				return false;

			for (int i = 0; i < args.Length; i++) {
				object thisArg = args[i];
				object otherArg = other.args[i];

				if (thisArg == null && otherArg == null)
					continue;
				if (thisArg == null)
					return false;
				if (!thisArg.Equals(otherArg))
					return false;
			}

			return true;
		}

		public static SqlType TypeOf(object obj) {
			if (obj == null || obj == DBNull.Value)
				return Null;

			if (obj is string) {
				string s = (string) obj;
				return GetSqlType(typeof (string), s.Length);
			}
			if (obj is byte[]) {
				byte[] buffer = (byte[]) obj;
				return GetSqlType(typeof (byte[]), buffer.Length);
			}
			if (obj is Stream) {
				Stream stream = (Stream) obj;
				return GetSqlType(typeof (Stream), (int) stream.Length);
			}
			if (obj is BigNumber) {
				BigNumber number = (BigNumber) obj;
				return GetSqlType(typeof (BigNumber), number.Precision, number.Scale);
			}

			return GetSqlType(obj.GetType());
		}

		public static SqlType GetSqlType(Type type) {
			return GetSqlType(type, null);
		}

		private static SqlType GetSqlType(Type type, params object[] args) {
			if (type == typeof (DBNull))
				return Null;

			if (type == typeof (int) ||
			    type == typeof (short) ||
			    type == typeof (byte) ||
			    type == typeof (long) ||
			    type == typeof (float) ||
			    type == typeof (double) ||
			    type == typeof (BigNumber))
				return new SqlType(SqlTypeCode.Numeric, args);
			if (type == typeof (bool))
				return new SqlType(SqlTypeCode.Boolean);

			if (type == typeof (string))
				return new SqlType(SqlTypeCode.String, args);
			if (type == typeof (byte[]) || typeof (Stream).IsAssignableFrom(type))
				return new SqlType(SqlTypeCode.Binary, args);
			if (type == typeof (BigNumber))
				return new SqlType(SqlTypeCode.Numeric, args);

			if (type == typeof (DateTime))
				return new SqlType(SqlTypeCode.DateTime);

			//TODO: support user-defined types ...

			return null;
		}

		public static SqlType MakeType(SqlTypeCode typeCode) {
			if (typeCode == SqlTypeCode.UserType)
				throw new ArgumentException("A user-defined type cannot be constructed from here.");

			return new SqlType(typeCode);
		}

		public static SqlType MakeNumeric(int size, int scale) {
			return new SqlType(SqlTypeCode.Numeric, size, scale);
		}

		public static SqlType MakeNumeric(int size) {
			return new SqlType(SqlTypeCode.Numeric, size);
		}

		public static SqlType MakeString(int size, string locale) {
			return new SqlType(SqlTypeCode.String, size, locale);
		}

		public static SqlType MakeString(int size) {
			return new SqlType(SqlTypeCode.String, size);
		}

		public static SqlType MakeString(string locale) {
			return new SqlType(SqlTypeCode.String, -1, locale);
		}

		public static SqlType MakeBinary(int size) {
			return new SqlType(SqlTypeCode.Binary, size);
		}

		public static SqlType Parse(string s) {
			if (System.String.IsNullOrEmpty(s))
				throw new ArgumentNullException("s");

			int index = s.IndexOf('(');
			string[] args = null;

			if (index > 0) {
				int endIndex = s.IndexOf(')', index);
				if (endIndex == -1)
					throw new FormatException("No closing parenthesis found.");

				string argsStr = s.Substring(index + 1, endIndex - (index + 1));
				if (argsStr.Length > 0) {
					args = argsStr.Split(',');
					for (int i = 0; i < args.Length; i++) {
						args[i] = args[i].Trim();
					}
				}

				s = s.Substring(0, index);
			}

			SqlTypeCode code;

			try {
				code = (SqlTypeCode)Enum.Parse(typeof(SqlTypeCode), s, true);
			} catch (Exception) {
				throw new FormatException("The string '" + s + "' is not a recognized type.");
			}

			if (code == SqlTypeCode.Numeric) {
				if (args == null || args.Length == 0)
					return new SqlType(SqlTypeCode.Numeric);

				if (args.Length > 2)
					throw new FormatException("Invalid argument number.");

				int size;
				byte scale = 0;
				bool hasScale = false;

				if (!Int32.TryParse(args[0], out size))
					throw new FormatException("Invalid size number.");

				if (args.Length > 1) {
					if (!Byte.TryParse(args[1], out scale))
						throw new FormatException("Invalid scale number.");
					hasScale = true;
				}

				return hasScale
				       	? new SqlType(SqlTypeCode.Numeric, new object[] {size, scale})
				       	: new SqlType(SqlTypeCode.Numeric, new object[] {size});
			}
			if (code == SqlTypeCode.String) {
				if (args == null || args.Length == 0)
					return new SqlType(SqlTypeCode.String);

				if (args.Length > 2)
					throw new FormatException("Invalid argument number.");

				int size;
				string locale = null;

				if (!Int32.TryParse(args[0], out size))
					throw new FormatException("Invalid size number.");

				if (args.Length > 1)
					locale = args[1];

				return new SqlType(SqlTypeCode.String, new object[] {size, locale});
			}

			if (code == SqlTypeCode.Binary) {
				if (args == null || args.Length == 0)
					return new SqlType(SqlTypeCode.String);

				if (args.Length > 2)
					throw new FormatException("Invalid argument number.");

				int size;
				if (!Int32.TryParse(args[0], out size))
					throw new FormatException("Invalid size number.");

				return new SqlType(SqlTypeCode.Binary, new object[] {size});
			}

			return new SqlType(code);
		}

		public static SqlType FromBinary(byte[] bytes) {
			SqlTypeCode code = (SqlTypeCode)bytes[0];
			SqlType type = code == SqlTypeCode.UserType ? new SqlUserType() : new SqlType(code);
			MemoryStream stream = new MemoryStream(bytes, 1, bytes.Length - 1);
			type.ReadFrom(stream);
			return type;
		}

	}
}