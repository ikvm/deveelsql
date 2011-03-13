using System;
using System.IO;

namespace Deveel.Data.Base {
	public static class Indexes {
		private static readonly SortedIndex EmptyIndex;
		
		static Indexes() {
			EmptyIndex = new SortedIndex (Stream.Null, true);
		}
		
		public static IIndex Empty {
			get { return EmptyIndex; }
		}
		
		public static IIndex ReadOnly(long value) {
			return ReadOnly(new long[] { value });
		}
		
		public static IIndex ReadOnly(long[] values) {
			return new SortedIndex(new Int64Stream(values), true);
		}

		public static IIndex Array(int length) {
			return new SortedIndex(new MemoryStream(length), false);
		}
		
		#region EmptyDataFile
		
		private class EmptyStream : Stream {
			private static ApplicationException EmptyStreamException () {
				return new ApplicationException ("EmptyStream");
			}

			public override bool CanWrite {
				get { return false; }
			}

			public override long Length {
				get { return 0; }
			}
			
			public override long Position  {
				get { return 0; }
				set {
					if (value != 0)
						throw new ArgumentOutOfRangeException("value", value, "The position is out of range.");
				}
			}
			
			public override int Read (byte[] buffer, int offset, int count) {
				throw EmptyStreamException ();
			}

			public override void Flush() {
				throw EmptyStreamException();
			}

			public override long Seek(long offset, SeekOrigin origin) {
				throw EmptyStreamException();
			}

			public override void SetLength (long value) {
				throw EmptyStreamException ();
			}
			
			public override void Write (byte[] buffer, int offset, int count) {
				throw EmptyStreamException ();
			}

			public override bool CanRead {
				get { return false; }
			}

			public override bool CanSeek {
				get { return false; }
			}
		}
		
		#endregion
		
		#region Int64DataFile
		
		private sealed class Int64Stream : Stream {
			private long[] arr;
			private int count;
			private int pos;

			public Int64Stream (long[] array) {
				this.arr = array;
				count = array.Length;
				pos = 0;
			}
			public Int64Stream (int length) {
				this.arr = new long[length];
				count = 0;
				pos = 0;
			}

			public override bool CanRead {
				get { return true; }
			}

			public override bool CanWrite {
				get { return true; }
			}

			public override bool CanSeek {
				get { return true; }
			}

			public override long Length {
				get { return count * 8; }
			}
			
			public override long Position {
				get { return pos; }
				set { pos = (int)(value / 8); }
			}
			
			//TODO: this considers only one at a time ... should handle more ...
			public override int Read(byte[] buffer, int offset, int count) {
				if (count > (this.count - pos) * 8)
					return 0;

				long value = arr[pos++];
				Deveel.Data.Util.ByteBuffer.WriteInt8(value, buffer, offset);
				return 8;
			}
			
			//TODO: this considers only one at a time ... should handle more ...
			public override void Write(byte[] buffer, int offset, int count) {
				if (pos == count) {
					if (pos >= arr.Length)
						throw new ApplicationException ("New size exceeds backed array size.");
				}
				
				long value = Deveel.Data.Util.ByteBuffer.ReadInt8(buffer, offset);
				arr[++pos] = value;
				++this.count;
			}
			
			public override void SetLength(long size) {
				int sz = (int)(size / 8);
				if (sz < 0)
					throw new ArgumentOutOfRangeException("size");
				
				if (sz > arr.Length)
					throw new ArgumentOutOfRangeException("size", "New size exceeds backed array size.");
				
				count = sz;
			}

			public override void Flush() {
			}

			public override long Seek(long offset, SeekOrigin origin) {
				if (origin == SeekOrigin.Begin) {
					pos = (int)offset;
				}
				if (origin == SeekOrigin.Current) {
					pos += (int)offset;
				}
				if (origin == SeekOrigin.End) {
					pos -= (int) offset;
				}
				return pos;
			}
		}

		
		#endregion
	}
}