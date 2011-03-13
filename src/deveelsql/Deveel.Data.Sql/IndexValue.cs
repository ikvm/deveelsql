using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public sealed class IndexValue : IEnumerable<SqlObject> {
		private readonly SqlObject[] values;

		public IndexValue(SqlObject[] values) {
			this.values = values;
		}

		public IndexValue(SqlObject value)
			: this(new SqlObject[] { value }) {
		}

		public SqlObject this[int index] {
			get { return values[index]; }
		}

		public int Length {
			get { return values.Length; }
		}

		public IEnumerator<SqlObject> GetEnumerator() {
			return new Enumerator(this);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		#region Enumerator

		private class Enumerator : IEnumerator<SqlObject> {
			private int index = -1;
			private readonly IndexValue value;

			public Enumerator(IndexValue value) {
				this.value = value;
			}

			public void Dispose() {
			}

			public bool MoveNext() {
				return ++index < value.values.Length;
			}

			public void Reset() {
				index = -1;
			}

			public SqlObject Current {
				get { return value.values[index]; }
			}

			object IEnumerator.Current {
				get { return Current; }
			}
		}

		#endregion
	}
}