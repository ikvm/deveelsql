using System;
using System.Text;

namespace Deveel.Data.Sql {
	public struct RangePair {
		private readonly SqlObject[] value1;
		private readonly SqlObject[] value2;

		public static readonly RangePair Null = new RangePair(null, null);

		internal RangePair(SqlObject[] value1, SqlObject[] value2) {
			this.value1 = value1;
			this.value2 = value2;
		}

		public SqlObject[] Value1 {
			get { return value1; }
		}

		public SqlObject[] Value2 {
			get { return value2; }
		}

		public override bool Equals(object obj) {
			RangePair pair = (RangePair)obj;
			if ((value1 == null && pair.value1 == null) &&
				(value2 == null && pair.value2 == null))
				return true;
			if (value1 != null && pair.value1 == null)
				return false;
			if (value2 != null && pair.value2 == null)
				return false;
			if (value1 == null && pair.value1 != null)
				return false;
			if (value2 == null && pair.value2 != null)
				return false;
			if (value1.Length != pair.value1.Length)
				return false;
			if (value2.Length != pair.value2.Length)
				return false;

			for (int i = 0; i < value1.Length; i++) {
				SqlObject v1a = value1[i];
				SqlObject v1b = value1[i];
				if (v1a == null && v1b == null)
					continue;
				if (v1a == null)
					return false;
				if (!v1a.Equals(v1b))
					return false;
			}

			for (int i = 0; i < value2.Length; i++) {
				SqlObject v2a = value2[i];
				SqlObject v2b = value2[i];
				if (v2a == null && v2b == null)
					continue;
				if (v2a == null)
					return false;
				if (!v2a.Equals(v2b))
					return false;
			}

			return true;
		}

		public override int GetHashCode() {
			return base.GetHashCode();
		}

		public override string ToString() {
			StringBuilder b = new StringBuilder();
			b.Append("[");
			foreach (SqlObject v in value1) {
				b.Append(v.ToString());
				b.Append(" ");
			}
			b.Append("], [");
			foreach (SqlObject v in value2) {
				b.Append(v.ToString());
				b.Append(" ");
			}
			b.Append("]");
			return b.ToString();
		}
	}
}