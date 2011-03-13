using System;
using System.Collections;
using System.IO;
using System.Text;

namespace Deveel.Data.Sql {
	public abstract class SqlValue : IComparable<SqlValue> {
		public static readonly SqlValue Null = new SqlBinaryValue(new byte[] { NullType });

		private static readonly SqlValue StringNull = new SqlBinaryValue(new byte[] { StringNullType });
		private static readonly SqlValue NumericNull = new SqlBinaryValue(new byte[] { NumericNullType });
		private static readonly SqlValue BooleanNull = new SqlBinaryValue(new byte[] { BooleanNullType });
		private static readonly SqlValue DateTimeNull = new SqlBinaryValue(new byte[] { DateTimeNullType });

		private static readonly SqlValue BooleanTrue = new SqlBinaryValue(new byte[] { BooleanTrueType });
		private static readonly SqlValue BooleanFalse = new SqlBinaryValue(new byte[] { BooleanFalseType });

		private const byte NullType = 1;
		private const byte StringNullType = 2;
		private const byte NumericNullType = 3;
		private const byte BooleanNullType = 6;
		private const byte DateTimeNullType = 9;

		private const byte StringType = 64;
		private const byte NumericType = 65;
		private const byte LongType = 66;
		private const byte IntegerType = 67;
		private const byte BooleanTrueType = 68;
		private const byte BooleanFalseType = 69;
		private const byte DoubleType = 70;
		private const byte FloatType = 71;
		private const byte DateTimeType = 72;

		public abstract int Length { get; }

		public bool IsNull {
			get { 
				byte type = PeekByte(0);
				return type < 64;
			}
		}

		public abstract byte PeekByte(int offset);

		public int CompareTo(SqlValue other, IComparer comparer) {
			if (other == null)
				return -1;
			if (IsNull && other.IsNull)
				return 0;

			object value1 = ToObject();
			object value2 = other.ToObject();

			if (value1 == null && value2 == null)
				return 0;
			if (value1 == null)
				return 1;
			if (value2 == null)
				return -1;

			if (comparer != null)
				return comparer.Compare(value1, value2);

			if (value1 is IComparable)
				return ((IComparable)value1).CompareTo(value2);

			throw new NotSupportedException();
		}

		public int CompareTo(SqlValue other) {
			return CompareTo(other, null);
		}

		public long? ToLong() {
			// The null case,
			byte b = PeekByte(0);
			if (b < 64)
				return null;

			if (b != LongType)
				throw new InvalidOperationException("Not a Long type");

			try {
				BinaryReader din = new BinaryReader(new SqlValueInputStream(this, 1));
				return din.ReadInt64();
			} catch (IOException e) {
				throw new FormatException(e.Message, e);
			}
		}

		public int? ToInteger() {
			// The null case,
			byte b = PeekByte(0);
			if (b < 64)
				return null;

			if (b != IntegerType)
				throw new InvalidOperationException("Not an Integer type");

			try {
				BinaryReader din = new BinaryReader(new SqlValueInputStream(this, 1));
				return din.ReadInt32();
			} catch (IOException e) {
				throw new FormatException(e.Message, e);
			}
		}
		
		public override string ToString() {
			// The null case,
			byte b = PeekByte(0);
			if (b < 64)
				return null;

			if (b != StringType)
				throw new InvalidOperationException("Not a String type");

			StreamReader in_reader = new StreamReader(new SqlValueInputStream(this, 1), Encoding.Unicode);
			StringBuilder buf = new StringBuilder(Length + 16);

			try {
				int c;
				while ((c = in_reader.Read()) != -1) {
					buf.Append((char)c);
				}

				return buf.ToString();
			} catch (IOException e) {
				throw new FormatException(e.Message, e);
			}
		}

		public double? ToDouble() {
			// The null case,
			byte b = PeekByte(0);
			if (b < 64)
				return null;

			if (b != DoubleType)
				throw new InvalidOperationException("Not a Double type");

			try {
				BinaryReader din = new BinaryReader(new SqlValueInputStream(this, 1));
				return BitConverter.Int64BitsToDouble(din.ReadInt64());
			} catch (IOException e) {
				throw new FormatException(e.Message, e);
			}
		}

		public float? ToFloat() {
			// The null case,
			byte b = PeekByte(0);
			if (b < 64)
				return null;

			if (b != FloatType)
				throw new InvalidOperationException("Not a Float type");

			try {
				BinaryReader din = new BinaryReader(new SqlValueInputStream(this, 1));
				int v = din.ReadInt32();
				return BitConverter.ToSingle(BitConverter.GetBytes(v), 0);
			} catch (IOException e) {
				throw new FormatException(e.Message, e);
			}
		}

		public BigNumber ToNumber() {
			// The null case,
			byte b = PeekByte(0);
			if (b < 64)
				return null;

			if (b != NumericType)
				throw new InvalidOperationException("Not a Numeric type");

			try {
				BinaryReader din = new BinaryReader(new SqlValueInputStream(this, 1));
				byte state = din.ReadByte();
				int scale = din.ReadInt32();
				int data_size = Length - (4 + 1 + 1);
				byte[] num_data = new byte[data_size];
				for (int i = 0; i < data_size; i++) {
					num_data[i] = din.ReadByte();
				}

				return BigNumber.Create(num_data, scale, (NumberState)state);
			} catch (IOException e) {
				throw new FormatException(e.Message, e);
			}
		}

		public DateTime? ToDateTime() {
			byte b = PeekByte(0);
			if (b < 64)
				return null;

			if (b != DateTimeType)
				throw new InvalidOperationException("Not a DateTime type");

			try {
				BinaryReader din = new BinaryReader(new SqlValueInputStream(this, 1));
				long ticks = din.ReadInt64();
				return DateTime.FromBinary(ticks);
			} catch (Exception e) {
				throw new FormatException(e.Message, e);
			}
		}

		public bool? ToBoolean() {
			// The null case,
			byte b = PeekByte(0);
			if (b < 64)
				return null;

			if (b == BooleanFalseType)
				return false;
			if (b == BooleanTrueType)
				return true;

			throw new InvalidOperationException("Not a Boolean type");
		}


		public object ToObject() {
			byte b = PeekByte(0);
			if (b < 64)
				return null;

			if (b == StringType)
				return ToString();
			if (b == NumericType)
				return ToNumber();
			if (b == LongType)
				return ToLong();
			if (b == IntegerType)
				return ToInteger();
			if (b == BooleanTrueType)
				return true;
			if (b == BooleanFalseType)
				return false;
			if (b == DoubleType)
				return ToDouble();
			if (b == FloatType)
				return ToFloat();

			throw new ArgumentException("Unknown value format");
		}

		public static SqlValue GetNull(SqlType type) {
			SqlTypeCode code = type.TypeCode;
			switch (code) {
				case SqlTypeCode.Null:
					return Null;
				case SqlTypeCode.String:
					return StringNull;
				case SqlTypeCode.Numeric:
					return NumericNull;
				case SqlTypeCode.Boolean:
					return BooleanNull;
				case SqlTypeCode.DateTime:
					return DateTimeNull;
				default:
					throw new ArgumentException("Unknown type.");
			}
		}

		public static SqlValue FromString(string value) {
			if (value == null)
				return StringNull;

			byte[] stringBytes = Encoding.Unicode.GetBytes(value);
			byte[] buf = new byte[stringBytes.Length + 1];
			buf[0] = StringType;
			Array.Copy(stringBytes, 0, buf, 1, stringBytes.Length);
			return new SqlBinaryValue(buf);
		}

		public static SqlValue FromNumber(BigNumber value) {
			if (value == null)
				return NumericNull;

			byte state = (byte)value.State;
			int scale = value.Scale;
			byte[] numdata = value.ToByteArray();
			byte[] buf = new byte[1 + 1 + 4 + numdata.Length];
			buf[0] = NumericType;
			buf[1] = state;
			byte[] scaleBytes = BitConverter.GetBytes(scale);
			Array.Copy(scaleBytes, 0, buf, 2, 4);
			Array.Copy(numdata, 0, buf, 6, numdata.Length);
			return new SqlBinaryValue(buf);
		}

		public static SqlValue FromBoolean(bool? value) {
			if (value == null)
				return BooleanNull;

			return (value.Value) ? BooleanTrue : BooleanFalse;
		}

		public static SqlValue FromBoolean(bool value) {
			return FromBoolean(new bool?(value));
		}

		public static SqlValue FromObject(object value) {
			if (value == null || value == DBNull.Value)
				return Null;

			if (value is bool)
				return FromBoolean((bool) value);

			if (value is string)
				return FromString((string)value);
			if (value is short)
				return FromNumber(BigNumber.FromInt32((int) value));
			if (value is int)
				return FromNumber(BigNumber.FromInt32((int)value));
			if (value is long)
				return FromNumber(BigNumber.FromInt64((long)value));
			if (value is float)
				return FromNumber(BigNumber.FromSingle((float)value));
			if (value is double)
				return FromNumber(BigNumber.FromDouble((double)value));
			if (value is BigNumber)
				return FromNumber((BigNumber) value);

			if (value is DateTime)
				return FromDateTime((DateTime) value);

			throw new ArgumentException("Cannot construct from value.");
		}

		public static SqlValue FromDateTime(DateTime? value) {
			if (value == null)
				return DateTimeNull;

			//TODO: Convert it to UTC millis ...
			long v = value.Value.ToUniversalTime().ToBinary();
			byte[] buf = new byte[9];
			buf[0] = DateTimeType;
			byte[] intBytes = BitConverter.GetBytes(v);
			Array.Copy(intBytes, 0, buf, 1, 8);
			return new SqlBinaryValue(buf);
		}

		public static SqlValue FromDateTime(DateTime value) {
			return FromDateTime(new DateTime?(value));
		}
	}
}