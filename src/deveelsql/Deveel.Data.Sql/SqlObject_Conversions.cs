using System;

namespace Deveel.Data.Sql {
	public sealed partial class SqlObject : IConvertible {
		TypeCode IConvertible.GetTypeCode() {
			if (type.TypeCode == SqlTypeCode.Boolean)
				return TypeCode.Boolean;

			if (type.TypeCode == SqlTypeCode.Numeric) {
				BigNumber number = ToNumber();
				if (number.CanBeInt)
					return TypeCode.Int32;
				if (number.CanBeLong)
					return TypeCode.Int64;
				return TypeCode.Object;
			}

			if (type.TypeCode == SqlTypeCode.DateTime)
				return TypeCode.DateTime;

			if (type.TypeCode == SqlTypeCode.String)
				return TypeCode.String;

			if (type.TypeCode == SqlTypeCode.Null)
				return TypeCode.DBNull;

			return TypeCode.Object;
		}

		bool IConvertible.ToBoolean(IFormatProvider provider) {
			return ToBoolean();
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			throw new NotSupportedException();
		}

		sbyte IConvertible.ToSByte(IFormatProvider provider) {
			throw new NotSupportedException();
		}

		byte IConvertible.ToByte(IFormatProvider provider) {
			return ToByte();
		}

		short IConvertible.ToInt16(IFormatProvider provider) {
			return ToInt16();
		}

		ushort IConvertible.ToUInt16(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		int IConvertible.ToInt32(IFormatProvider provider) {
			return ToInt32();
		}

		uint IConvertible.ToUInt32(IFormatProvider provider) {
			throw new InvalidCastException();
		}

		long IConvertible.ToInt64(IFormatProvider provider) {
			return ToInt64();
		}

		ulong IConvertible.ToUInt64(IFormatProvider provider) {
			throw new NotSupportedException();
		}

		float IConvertible.ToSingle(IFormatProvider provider) {
			return ToSingle();
		}

		double IConvertible.ToDouble(IFormatProvider provider) {
			return ToDouble();
		}

		decimal IConvertible.ToDecimal(IFormatProvider provider) {
			throw new NotSupportedException();
		}

		DateTime IConvertible.ToDateTime(IFormatProvider provider) {
			return ToDateTime();
		}

		string IConvertible.ToString(IFormatProvider provider) {
			return ToString();
		}

		object IConvertible.ToType(Type conversionType, IFormatProvider provider) {
			if (conversionType == typeof(bool))
				return value.ToBoolean();

			if (conversionType == typeof(byte))
				return ToByte();
			if (conversionType == typeof(short))
				return ToInt16();
			if (conversionType == typeof(int))
				return ToInt32();
			if (conversionType == typeof(long))
				return ToInt64();
			if (conversionType == typeof(float))
				return ToSingle();
			if (conversionType == typeof(double))
				return ToDouble();
			if (conversionType == typeof(BigNumber))
				return ToNumber();

			if (conversionType == typeof(string))
				return ToString();
			if (conversionType == typeof(DateTime))
				return ToDateTime();

			throw new NotSupportedException("The value cannot be converted to '" + conversionType + "'.");
		}

		public bool ToBoolean() {
			return value.IsNull ? false : value.ToBoolean();
		}

		public bool? ToBooleanNullable() {
			return value.IsNull ? null : new bool?(value.ToBoolean());
		}

		public byte ToByte() {
			BigNumber num = ToNumber();
			return num == null ? (byte)0 : num.ToByte();
		}

		public byte? ToByteNullable() {
			BigNumber num = ToNumber();
			return num == null ? null : new byte?(num.ToByte());
		}

		public short ToInt16() {
			BigNumber num = ToNumber();
			return num == null ? (short)0 : num.ToInt16();
		}

		public short? ToInt16Nullable() {
			BigNumber num = ToNumber();
			return num == null ? null : new short?(num.ToInt16());
		}

		public int? ToInt32Nullable() {
			BigNumber num = ToNumber();
			return num == null ? null : new int?(num.ToInt32());
		}

		public int ToInt32() {
			BigNumber num = ToNumber();
			return num == null ? 0 : num.ToInt32();
		}

		public long? ToInt64Nullable() {
			BigNumber num = ToNumber();
			return num == null ? null : new long?(num.ToInt64());
		}

		public long ToInt64() {
			BigNumber num = ToNumber();
			return num == null ? 0L : num.ToInt64();
		}

		public float? ToSingleNullable() {
			BigNumber num = ToNumber();
			return num == null ? null : new float?(num.ToSingle());
		}

		public float ToSingle() {
			BigNumber num = ToNumber();
			return num == null ? 0.0f :num.ToSingle();
		}

		public double? ToDoubleNullable() {
			BigNumber num = ToNumber();
			return num == null ? null : new double?(num.ToDouble());
		}

		public double ToDouble() {
			BigNumber num = ToNumber();
			return num == null ? 0.0 : num.ToDouble();
		}

		public BigNumber ToNumber() {
			return value.ToNumber();
		}

		public override string ToString() {
			return value.ToString();
		}

		public DateTime? ToDateTimeNullable() {
			return value.ToDateTime();
		}

		public DateTime ToDateTime() {
			return value.IsNull ? DateTime.MinValue : value.ToDateTime().GetValueOrDefault();
		}

		public object ToObject() {
			return value.ToObject();
		}

		public static implicit operator bool(SqlObject obj) {
			return obj.ToBoolean();
		}

		public static implicit operator byte(SqlObject obj) {
			return obj.ToByte();
		}

		public static implicit operator short(SqlObject obj) {
			return obj.ToInt16();
		}

		public static implicit operator int (SqlObject obj) {
			return obj.ToInt32();
		}

		public static implicit operator long (SqlObject obj) {
			return obj.ToInt64();
		}

		public static implicit operator float (SqlObject obj) {
			return obj.ToSingle();
		}

		public static implicit operator double (SqlObject obj) {
			return obj.ToDouble();
		}

		public static implicit operator BigNumber(SqlObject obj) {
			return obj.ToNumber();
		}

		public static implicit operator string(SqlObject obj) {
			return obj.ToString();
		}

		public static implicit operator DateTime(SqlObject obj) {
			return obj.ToDateTime();
		}

		public static implicit operator SqlObject(bool value) {
			return new SqlObject(value);
		}

		public static implicit operator SqlObject(byte value) {
			return new SqlObject((int)value);
		}

		public static implicit operator SqlObject(short value) {
			return new SqlObject((int)value);
		}

		public static implicit operator SqlObject(int value) {
			return new SqlObject(value);
		}

		public static implicit operator SqlObject(long value) {
			return new SqlObject(value);
		}

		public static implicit operator SqlObject(float value) {
			return new SqlObject(value);
		}

		public static implicit operator SqlObject(double value) {
			return new SqlObject(value);
		}

		public static implicit operator SqlObject(BigNumber value) {
			return new SqlObject(value);
		}

		public static implicit operator SqlObject(string value) {
			return new SqlObject(value);
		}

		public static implicit operator SqlObject(DateTime value) {
			return new SqlObject(value);
		}
	}
}