using System;
using System.Collections;

using Deveel.Data.Base;

namespace Deveel.Data.Sql {
	sealed class DefaultRowCursor : IRowCursor {
		private readonly IIndexCursor<RowId> cursor;

		public DefaultRowCursor(IIndexCursor<RowId> cursor) {
			this.cursor = cursor;
		}

		public void Dispose() {
			cursor.Dispose();
		}

		public bool MoveNext() {
			return cursor.MoveNext();
		}

		public void Reset() {
			cursor.Reset();
		}

		public RowId Current {
			get { return cursor.Current; }
		}

		object IEnumerator.Current {
			get { return Current; }
		}

		public object Clone() {
			return new DefaultRowCursor((IIndexCursor<RowId>) cursor.Clone());
		}

		public long Position {
			get { return cursor.Position; }
		}

		public long Count {
			get { return cursor.Count; }
		}

		public bool MoveBack() {
			return cursor.MoveBack();
		}

		public void MoveBeforeStart() {
			cursor.Position = -1;
		}

		public void MoveAfterEnd() {
			cursor.Position = cursor.Count;
		}

		public long MoveTo(long position) {
			return cursor.Position = position;
		}
	}
}