using System;
using System.Collections;

namespace Deveel.Data.Sql {
	public class SubsetRowCursor : IRowCursor {

		private readonly IRowCursor cursor;
		private readonly long start;
		private readonly long end;

		public SubsetRowCursor(IRowCursor cursor, long start, long size) {
			this.cursor = cursor;
			this.start = start;
			end = start + (size - 1);
			MoveBeforeStart();
		}

		public long Count {
			get { return (end - start) + 1; }
		}

		public void MoveBeforeStart() {
			cursor.MoveTo(start - 1);
		}

		public void MoveAfterEnd() {
			cursor.MoveTo(end + 1);
		}

		public long MoveTo(long position) {
			return cursor.MoveTo(start + position);
		}

		public long Position {
			get { return cursor.Position - start; }
		}

		public bool MoveNext() {
			return cursor.MoveNext() && cursor.Position < end;
		}

		public void Reset() {
			MoveBeforeStart();
		}

		object IEnumerator.Current {
			get { return Current; }
		}

		public long Current {
			get { return cursor.Current; }
		}

		public bool MoveBack() {
			return cursor.MoveBack() && cursor.Position > start;
		}

		public object Clone() {
			return new SubsetRowCursor((IRowCursor) cursor.Clone(), start, Count);
		}

		public void Dispose() {
			cursor.Dispose();
		}
	}
}