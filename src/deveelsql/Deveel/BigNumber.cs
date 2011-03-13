// 
//  Copyright 2010  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Globalization;

using Deveel.Data.Util;
using Deveel.Math;

namespace Deveel {
	///<summary>
	/// Extends <see cref="BigDecimal"/> to allow a number to be positive 
	/// infinity, negative infinity and not-a-number.
	///</summary>
	/// <remarks>
	/// This provides compatibility with float and double types.
	/// </remarks>
	[Serializable]
	public sealed class BigNumber : IConvertible, IComparable {
		private static readonly BigDecimal BdZero = new BigDecimal(0);

		///<summary>
		/// Represets a <see cref="BigNumber"/> for the 1 number.
		///</summary>
		public static readonly BigNumber One = FromInt64(1L);

		///<summary>
		/// Represents a <see cref="BigNumber"/> for the 0 number.
		///</summary>
		public static readonly BigNumber Zero = FromInt64(0L);

		// Statics for negative infinity, positive infinity and NaN.
		///<summary>
		///</summary>
		public static readonly BigNumber NegativeInfinity = new BigNumber(NumberState.NegativeInfinity, null);
		///<summary>
		///</summary>
		public static readonly BigNumber PositiveInfinity = new BigNumber(NumberState.PositiveInfinity, null);
		///<summary>
		///</summary>
		public static readonly BigNumber NaN = new BigNumber(NumberState.NotANumber, null);

		/// <summary>
		/// The BigDecimal representation.
		/// </summary>
		private BigDecimal bigDecimal;

		/// <summary>
		/// If this can be represented as an int or long, this contains the number
		/// of bytes needed to represent the number.
		/// </summary>
		private byte byteCount = 120;
		/// <summary>
		/// A 'long' representation of this number.
		/// </summary>
		private long long_representation;
		private readonly NumberState numberState;

		/// <summary>
		///  Constructs the number.
		/// </summary>
		/// <param name="numberState"></param>
		/// <param name="bigDecimal"></param>
		internal BigNumber(NumberState numberState, BigDecimal bigDecimal) {
			this.numberState = numberState;
			if (numberState == NumberState.None)
				SetBigDecimal(bigDecimal);
		}

		private BigNumber(byte[] buf, int scale, NumberState state) {
			numberState = state;
			if (numberState == NumberState.None) {
				BigInteger bigint = new BigInteger(buf);
				SetBigDecimal(new BigDecimal(bigint, scale));
			}
		}


		// Only call this from a constructor!
		private void SetBigDecimal(BigDecimal value) {
			bigDecimal = value;
			if (bigDecimal.Scale == 0) {
				BigInteger bint = value.ToBigInteger();
				int bit_count = bint.BitLength;
				if (bit_count < 30) {
					long_representation = bint.ToInt64();
					byteCount = 4;
				} else if (bit_count < 60) {
					long_representation = bint.ToInt64();
					byteCount = 8;
				}
			}
		}

		///<summary>
		/// Returns true if this BigNumber can be represented by a 64-bit long (has
		/// no scale).
		///</summary>
		public bool CanBeLong {
			get { return byteCount <= 8; }
		}

		///<summary>
		/// Returns true if this BigNumber can be represented by a 32-bit int (has
		/// no scale).
		///</summary>
		public bool CanBeInt {
			get { return byteCount <= 4; }
		}

		///<summary>
		/// Returns the scale of this number, or -1 if the number has no scale (if
		/// it -inf, +inf or NaN).
		///</summary>
		public int Scale {
			get { return numberState == NumberState.None ? bigDecimal.Scale : -1; }
		}

		public int Precision {
			get { return numberState == NumberState.None ? bigDecimal.Precision : -1; }
		}

		///<summary>
		/// Returns the state of this number.
		///</summary>
		public NumberState State {
			get { return numberState; }
		}

		/// <summary>
		/// Returns the inverse of the state.
		/// </summary>
		private NumberState InverseState {
			get {
				if (numberState == NumberState.NegativeInfinity)
					return NumberState.PositiveInfinity;
				if (numberState == NumberState.PositiveInfinity)
					return NumberState.NegativeInfinity;
				return numberState;
			}
		}

		///<summary>
		/// Returns this number as a byte array (unscaled).
		///</summary>
		///<returns></returns>
		public byte[] ToByteArray() {
			return numberState == 0 ? bigDecimal.MovePointRight(bigDecimal.Scale).ToBigInteger().ToByteArray() : new byte[0];
		}

		/// <inheritdoc/>
		public override string ToString() {
			switch (numberState) {
				case (NumberState.None):
					return bigDecimal.ToString();
				case (NumberState.NegativeInfinity):
					return "-Infinity";
				case (NumberState.PositiveInfinity):
					return "Infinity";
				case (NumberState.NotANumber):
					return "NaN";
				default:
					throw new ApplicationException("Unknown number state");
			}
		}

		public double ToDouble() {
			switch (numberState) {
				case (NumberState.None):
					return bigDecimal.ToDouble();
				case (NumberState.NegativeInfinity):
					return Double.NegativeInfinity;
				case (NumberState.PositiveInfinity):
					return Double.PositiveInfinity;
				case (NumberState.NotANumber):
					return Double.NaN;
				default:
					throw new ApplicationException("Unknown number state");
			}
		}

		public float ToSingle() {
			switch (numberState) {
				case (NumberState.None):
					return bigDecimal.ToSingle();
				case (NumberState.NegativeInfinity):
					return Single.NegativeInfinity;
				case (NumberState.PositiveInfinity):
					return Single.PositiveInfinity;
				case (NumberState.NotANumber):
					return Single.NaN;
				default:
					throw new ApplicationException("Unknown number state");
			}
		}

		public long ToInt64() {
			if (CanBeLong)
				return long_representation;
			switch (numberState) {
				case (NumberState.None):
					return bigDecimal.ToInt64();
				default:
					return (long)ToDouble();
			}
		}

		public int ToInt32() {
			if (CanBeLong)
				return (int)long_representation;
			switch (numberState) {
				case (NumberState.None):
					return bigDecimal.ToInt32();
				default:
					return (int)ToDouble();
			}
		}

		public short ToInt16() {
			return (short)ToInt32();
		}

		public byte ToByte() {
			return (byte)ToInt32();
		}

		///<summary>
		/// Returns the big number as a <see cref="BigDecimal"/> object.
		///</summary>
		///<returns></returns>
		///<exception cref="ArithmeticException">
		/// If this number represents NaN, +Inf or -Inf.
		/// </exception>
		public BigDecimal ToBigDecimal() {
			if (numberState != NumberState.None)
				throw new ArithmeticException("NaN, +Infinity or -Infinity can't be translated to a BigDecimal");

			return bigDecimal;
		}

		public int CompareTo(object obj) {
			return CompareTo((BigNumber)obj);
		}

		/**
		 * Compares this BigNumber with the given BigNumber.  Returns 0 if the values
		 * are equal, >0 if this is greater than the given value, and &lt; 0 if this
		 * is less than the given value.
		 */

		public int CompareTo(BigNumber number) {
			if (this == number) {
				return 0;
			}

			// If this is a non-infinity number
			if (numberState == 0) {
				// If both values can be represented by a long value
				if (CanBeLong && number.CanBeLong) {
					// Perform a long comparison check,
					if (long_representation > number.long_representation)
						return 1;
					if (long_representation < number.long_representation)
						return -1;
					return 0;
				}

				// And the compared number is non-infinity then use the BigDecimal
				// compareTo method.
				if (number.numberState == 0)
					return bigDecimal.CompareTo(number.bigDecimal);

				// Comparing a regular number with a NaN number.
				// If positive infinity or if NaN
				if (number.numberState == NumberState.PositiveInfinity ||
				    number.numberState == NumberState.NotANumber) {
					return -1;
				}
				// If negative infinity
				if (number.numberState == NumberState.NegativeInfinity)
					return 1;

				throw new ApplicationException("Unknown number state.");

			}

			// This number is a NaN number.
			// Are we comparing with a regular number?
			if (number.numberState == NumberState.None) {
				// Yes, negative infinity
				if (numberState == NumberState.NegativeInfinity)
					return -1;

				// positive infinity or NaN
				if (numberState == NumberState.PositiveInfinity ||
				    numberState == NumberState.NotANumber) {
					return 1;
				}

				throw new ApplicationException("Unknown number state.");
			}
			// Comparing NaN number with a NaN number.
			// This compares -Inf less than Inf and NaN and NaN greater than
			// Inf and -Inf.  -Inf < Inf < NaN
			return (numberState - number.numberState);
		}

		/**
		 * The equals comparison uses the BigDecimal 'equals' method to compare
		 * values.  This means that '0' is NOT equal to '0.0' and '10.0' is NOT equal
		 * to '10.00'.  Care should be taken when using this method.
		 */

		public override bool Equals(Object ob) {
			BigNumber bnum = (BigNumber)ob;
			return numberState != NumberState.None ? numberState == bnum.numberState : bigDecimal.Equals(bnum.bigDecimal);
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return base.GetHashCode();
		}


		// ---- Mathematical functions ----

		///<summary>
		///</summary>
		///<param name="number"></param>
		///<returns></returns>
		public BigNumber BitWiseOr(BigNumber number) {
			if (numberState == NumberState.None && Scale == 0 &&
			    number.numberState == 0 && number.Scale == 0) {
				BigInteger bi1 = bigDecimal.ToBigInteger();
				BigInteger bi2 = number.bigDecimal.ToBigInteger();
				return new BigNumber(NumberState.None, new BigDecimal(bi1.Or(bi2)));
			}
			return null;
		}

		///<summary>
		///</summary>
		///<param name="number"></param>
		///<returns></returns>
		public BigNumber Add (BigNumber number) {
			if (numberState == NumberState.None) {
				if (number.numberState == NumberState.None)
					return new BigNumber(NumberState.None, bigDecimal.Add(number.bigDecimal));
				return new BigNumber(number.numberState, null);
			}
			return new BigNumber(numberState, null);
		}

		///<summary>
		///</summary>
		///<param name="number"></param>
		///<returns></returns>
		public BigNumber Subtract (BigNumber number) {
			if (numberState == NumberState.None) {
				if (number.numberState == NumberState.None)
					return new BigNumber(NumberState.None, bigDecimal.Subtract(number.bigDecimal));
				return new BigNumber(number.InverseState, null);
			}
			
			return new BigNumber(numberState, null);
		}

		///<summary>
		///</summary>
		///<param name="number"></param>
		///<returns></returns>
		public BigNumber Multiply(BigNumber number) {
			if (numberState == NumberState.None) {
				if (number.numberState == 0)
					return new BigNumber(NumberState.None, bigDecimal.Multiply(number.bigDecimal));
				return new BigNumber(number.numberState, null);
			}
			return new BigNumber(numberState, null);
		}

		///<summary>
		///</summary>
		///<param name="number"></param>
		///<returns></returns>
		public BigNumber Divide (BigNumber number) {
			if (numberState == 0) {
				if (number.numberState == 0) {
					BigDecimal div_by = number.bigDecimal;
					if (div_by.CompareTo (BdZero) != 0) {
						return new BigNumber(NumberState.None, bigDecimal.Divide(div_by, 10, RoundingMode.HalfUp));
					}
				}
			}
			// Return NaN if we can't divide
			return new BigNumber(NumberState.NotANumber, null);
		}

		public BigNumber Modulus(BigNumber number) {
			if (numberState == 0) {
				if (number.numberState == 0) {
					BigDecimal div_by = number.bigDecimal;
					if (div_by.CompareTo(BdZero) != 0) {
						BigDecimal remainder = bigDecimal.Remainder(div_by);
						return new BigNumber(NumberState.None, remainder);
					}
				}
			}

			return new BigNumber(NumberState.NotANumber, null);
		}

		///<summary>
		///</summary>
		///<returns></returns>
		public BigNumber Abs () {
			if (numberState == 0) {
				return new BigNumber(NumberState.None, bigDecimal.Abs());
			} else if (numberState == NumberState.NegativeInfinity) {
				return new BigNumber(NumberState.PositiveInfinity, null);
			} else {
				return new BigNumber(numberState, null);
			}
		}

		///<summary>
		///</summary>
		///<returns></returns>
		public int Signum() {
			if (numberState == 0) {
				return bigDecimal.Signum();
			} else if (numberState == NumberState.NegativeInfinity) {
				return -1;
			} else {
				return 1;
			}
		}

		///<summary>
		///</summary>
		///<param name="d"></param>
		///<param name="round_enum"></param>
		///<returns></returns>
		public BigNumber SetScale (int d, RoundingMode round_enum) {
			if (numberState == 0) {
				return new BigNumber(NumberState.None, bigDecimal.SetScale(d, round_enum));
			}
			// Can't round -inf, +inf and NaN
			return this;
		}

		///<summary>
		///</summary>
		///<returns></returns>
		public BigNumber Sqrt() {
			return FromDouble(System.Math.Sqrt(ToDouble()));
		}

		public BigNumber Pow(int n) {
			return new BigNumber(NumberState.None, bigDecimal.Pow(n));
		}


		// ---------- Casting from types ----------

		public static BigNumber FromDouble(double value) {
			if (Double.IsNegativeInfinity(value))
				return NegativeInfinity;
			if (Double.IsPositiveInfinity(value))
				return PositiveInfinity;
			if (Double.IsNaN(value))
				return NaN;

			return new BigNumber(NumberState.None, new BigDecimal(Convert.ToString(value, CultureInfo.InvariantCulture)));
		}

		public static BigNumber FromSingle(float value) {
			if (Single.IsNegativeInfinity(value))
				return NegativeInfinity;
			if (Single.IsPositiveInfinity(value))
				return PositiveInfinity;
			if (Single.IsNaN(value))
				return NaN;

			return new BigNumber(NumberState.None, new BigDecimal(Convert.ToString(value, CultureInfo.InvariantCulture)));
		}

		public static BigNumber FromInt64(long value) {
			return new BigNumber(NumberState.None, BigDecimal.ValueOf(value));
		}

		public static BigNumber FromInt32(int value) {
			return new BigNumber(NumberState.None, BigDecimal.ValueOf(value));
		}

		public static BigNumber FromBigDecimal(BigDecimal value) {
			return new BigNumber(NumberState.None, value);
		}

		//TODO:
		public static BigNumber FromDecimal(decimal value) {
			int[] bits = Decimal.GetBits(value);
			byte[] buffer = new byte[(3 * 4) + 1];
			buffer[0] = (byte)(bits[0] == 0 ? 0 : 1);
			for (int i = bits.Length - 1; i >= 0; i--) {
				ByteBuffer.WriteInt4(bits[i], buffer, i + 4);
			}
			return new BigNumber(NumberState.None, new BigDecimal(new BigInteger(buffer)));
		}

		/**
		 * Creates a BigNumber from a string.
		 */

		public static BigNumber Parse (String str) {
			if (str.Equals ("Infinity"))
				return PositiveInfinity;
			if (str.Equals ("-Infinity"))
				return NegativeInfinity;
			if (str.Equals ("NaN"))
				return NaN;
			
			return new BigNumber(NumberState.None, new BigDecimal(str));
		}

		/**
		 * Creates a BigNumber from the given data.
		 */

		public static BigNumber Create(byte[] buf, int scale, NumberState state) {
			if (state == NumberState.None) {
				// This inlines common numbers to save a bit of memory.
				if (scale == 0 && buf.Length == 1) {
					if (buf[0] == 0) {
						return Zero;
					} else if (buf[0] == 1) {
						return One;
					}
				}
				return new BigNumber(buf, scale, state);
			} else if (state == NumberState.NegativeInfinity) {
				return NegativeInfinity;
			} else if (state == NumberState.PositiveInfinity) {
				return PositiveInfinity;
			} else if (state == NumberState.NotANumber) {
				return NaN;
			} else {
				throw new ApplicationException("Unknown number state.");
			}
		}

		#region Implementation of IConvertible

		TypeCode IConvertible.GetTypeCode() {
			if (CanBeInt)
				return TypeCode.Int32;
			if (CanBeLong)
				return TypeCode.Int64;
			return TypeCode.Object;
		}

		bool IConvertible.ToBoolean(IFormatProvider provider) {
			int value = ToInt16();
			if (value == 0)
				return false;
			if (value == 1)
				return true;
			throw new InvalidCastException();
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			short value = ToInt16();
			if (value > Char.MaxValue || value < Char.MinValue)
				throw new InvalidCastException();
			return (char) value;
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
			throw new NotSupportedException();
		}

		int IConvertible.ToInt32(IFormatProvider provider) {
			return ToInt32();
		}

		uint IConvertible.ToUInt32(IFormatProvider provider) {
			throw new NotSupportedException();
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
			throw new NotSupportedException();
		}

		string IConvertible.ToString(IFormatProvider provider) {
			return ToString();
		}

		object IConvertible.ToType(Type conversionType, IFormatProvider provider) {
			if (conversionType == typeof(bool))
				return (this as IConvertible).ToBoolean(provider);
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

			if (conversionType == typeof(BigDecimal))
				return ToBigDecimal();
			if (conversionType == typeof(byte[]))
				return ToByteArray();

			throw new NotSupportedException();
		}

		#endregion
	}
}