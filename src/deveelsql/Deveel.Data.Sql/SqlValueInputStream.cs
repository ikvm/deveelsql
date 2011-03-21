using System;
using System.IO;

namespace Deveel.Data.Sql {
	sealed class SqlValueInputStream : Stream {
		private int position;
		private readonly int length;
		private readonly SqlValue value;

		public SqlValueInputStream(SqlValue value, int startOffset) {
			if (value == null)
				throw new ArgumentNullException("value");

			this.value = value;
			position = startOffset;
			length = value.Length;
		}

		public SqlValueInputStream(SqlValue value)
			: this(value, 0) {
		}

		public override void Flush() {
		}

		public override long Seek(long offset, SeekOrigin origin) {
			if (origin == SeekOrigin.Begin) {
				if (offset > length)
					throw new ArgumentException("Cannot seek over the maximum length of the stream.");
				position = (int) offset;
			} else if (origin == SeekOrigin.Current) {
				if (offset + position > length)
					throw new ArgumentException("Cannot seek over the maximum length of the stream.");

				position += (int) offset;
			} else {
				if (position - offset < 0)
					throw new ArgumentException("Cannot seek before the begin of the stream.");

				position -= (int) offset;
			}

			return position;
		}

		public override void SetLength(long value) {
			throw new NotSupportedException();
		}

		public override int Read(byte[] buffer, int offset, int count) {
			if (count <= 0)
				return 0;

			int toRead = System.Math.Min(count, length - position);

			if (toRead == 0)
				return 0;

			int readCount = 0;
			while (readCount < toRead) {
				if (position == length)
					break;

				buffer[offset] = value.PeekByte(position);
				position++;
				readCount++;
				offset++;
			}

			return readCount;
		}

		public override void Write(byte[] buffer, int offset, int count) {
			throw new NotSupportedException();
		}

		public override bool CanRead {
			get { return true; }
		}

		public override bool CanSeek {
			get { return true; }
		}

		public override bool CanWrite {
			get { return false; }
		}

		public override long Length {
			get { return length; }
		}

		public override long Position {
			get { return position; }
			set { Seek(value, SeekOrigin.Begin); }
		}
	}
}