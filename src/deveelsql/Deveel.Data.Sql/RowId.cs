using System;
using System.Text;

namespace Deveel.Data.Sql {
	[Serializable]
	public class RowId : IEquatable<RowId>, IComparable<RowId>, IComparable {
		private readonly byte[] buffer;
		[NonSerialized]
		private volatile int hashCode;

		public RowId(byte[] buffer) {
			if (buffer == null)
				throw new ArgumentNullException("buffer");

			this.buffer = (byte[]) buffer.Clone();
		}

		public RowId(long value)
			: this(BitConverter.GetBytes(value)) {
		}

		public RowId(string value)
			: this(Encoding.ASCII.GetBytes(value)) {
		}

		// has in http://bretm.home.comcast.net/~bretm/hash/6.html
		public static int ComputeHash(params byte[] data) {
			unchecked {
				const int p = 16777619;
				int hash = (int)2166136261;

				for (int i = 0; i < data.Length; i++)
					hash = (hash ^ data[i]) * p;

				hash += hash << 13;
				hash ^= hash >> 7;
				hash += hash << 3;
				hash ^= hash >> 17;
				hash += hash << 5;
				return hash;
			}
		}

		public long ToInt64() {
			return BitConverter.ToInt64(buffer, 0);
		}

		public byte[] ToBinary() {
			return (byte[]) buffer.Clone();
		}

		public virtual int CompareTo(RowId other) {
			if (other == null)
				return -1;

			if (Equals(other))
				return 0;

			//TODO: this takes in consideration just the case 
			//      of an INTEGER ROWID
			long value = ToInt64();
			long otherValue = other.ToInt64();
			return value.CompareTo(otherValue);
		}

		public override string ToString() {
			return Encoding.ASCII.GetString(buffer);
		}

		public bool Equals(RowId other) {
			if (other == null)
				return false;

			int sz = buffer.Length;
			if (sz != other.buffer.Length)
				return false;

			for (int i = 0; i < sz; i++) {
				byte b = buffer[i];
				if (!b.Equals(other.buffer[i]))
					return false;
			}

			return true;
		}

		public override bool Equals(object obj) {
			RowId other = obj as RowId;
			if (other == null)
				return false;

			return Equals(other);
		}

		public override int GetHashCode() {
			if (hashCode == 0)
				hashCode = ComputeHash(buffer);
			return hashCode;
		}

		public int CompareTo(object obj) {
			if (!(obj is RowId))
				throw new ArgumentException();

			return CompareTo((RowId) obj);
		}
	}
}