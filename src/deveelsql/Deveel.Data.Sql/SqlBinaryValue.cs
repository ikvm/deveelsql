using System;
using System.Diagnostics;

namespace Deveel.Data.Sql {
	[DebuggerDisplay("{ToObject()}")]
	internal sealed class SqlBinaryValue : SqlValue {
		private readonly byte[] buffer;
		private readonly int index;
		private readonly int length;

		public SqlBinaryValue(byte[] buffer, int index, int length) {
			this.buffer = buffer;
			this.length = length;
			this.index = index;
		}

		public SqlBinaryValue(byte [] buffer)
			: this(buffer, 0, buffer.Length) {
		}

		public override int Length {
			get { return length; }
		}

		public override byte PeekByte(int offset) {
			if ((offset < 0) || (offset >= length))
				throw new ArgumentOutOfRangeException();

			return buffer[index + offset];
		}
	}
}