using System;
using System.Text;

namespace Deveel.Data.Sql {
	public sealed class SqlCompositeType : SqlType {
		private readonly SqlType[] parts;
		private readonly bool[] partsOrder;

		internal SqlCompositeType(SqlType[] parts, bool[] partsOrder) 
			: base(SqlTypeCode.Array) {
			if (parts == null)
				throw new ArgumentNullException("parts");

			this.parts = (SqlType[]) parts.Clone();
			if (partsOrder != null) {
				if (partsOrder.Length != parts.Length)
					throw new ArgumentException();

				this.partsOrder = partsOrder;
			}
		}

		internal SqlCompositeType(SqlType[] parts) 
			: this(parts, null) {
		}

		public int PartCount {
			get { return parts.Length; }
		}

		public SqlType this[int index] {
			get { return parts[index]; }
		}

		public override string ToString() {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < parts.Length; i++) {
				SqlType type = parts[i];
				sb.Append(type.ToBinary());
				if (partsOrder != null)
					sb.Append("(" + (partsOrder[i] ? "ASC" : "DESC") + ")");
				if (i < parts.Length - 1)
					sb.Append(",");
			}

			return sb.ToString();
		}

		public bool IsAscending(int index) {
			return partsOrder == null || partsOrder[index];
		}

		public override bool IsComparableTo(SqlType type) {
			if (type.IsNull)
				return true;

			if (!(type is SqlCompositeType))
				return false;

			SqlCompositeType other = (SqlCompositeType) type;
			int sz = PartCount;
			if (sz != other.PartCount)
				return false;

			for (int i = 0; i < sz; ++i) {
				// If part i is not comparable to part i of the dest composite
				// type then return false.
				if (!this[i].IsComparableTo(other[i]))
					return false;
				// Compare collation part types
				if (IsAscending(i) != other.IsAscending(i))
					return false;
			}
			// All compare correctly so return true.
			return true;
		}

		public override SqlType Widest(SqlType type) {
			if (type.IsNull)
				return type;

			return this;
		}

		public override int Compare(object x, object y) {
			// Cast objects to object arrays
			Array array1 = (Array) x;
			Array array2 = (Array) y;

			int sz = PartCount;

			// Go from left to right, if the first compares greater or less then
			// return the comparison, otherwise if it compares equal then move right.
			// If all compare to 0 then return 0 (values are equal).

			// Note this sorts null parts of the key high,  Nulls are sorted first.
			// We shouldn't be handling all NULL parts here but it is possible and it
			// is handled.

			for (int i = 0; i < sz; ++i) {
				SqlType type = this[i];
				bool b = IsAscending(i);

				object o1 = array1.GetValue(i);
				object o2 = array2.GetValue(i);

				if (o1 == null) {
					if (o2 != null)
						// o1 is null and o2 is not null
						return b ? -1 : 1;
				} else {
					if (o2 == null)
						// o1 is not null and o2 is null
						return b ? 1 : -1;

					// Neither values are null,
					int v = type.Compare(o1, o2);
					if (v != 0)
						return b ? v : -v;

				}
			}

			// All compared equally
			return 0;
		}
	}
}