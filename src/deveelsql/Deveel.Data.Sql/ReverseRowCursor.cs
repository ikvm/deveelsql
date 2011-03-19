using System;
using System.Collections;

namespace Deveel.Data.Sql {
	public sealed class ReverseRowCursor : IRowCursor {
		private IRowCursor cursor;
		private readonly long size;
		
		public ReverseRowCursor(IRowCursor cursor, long size) {
			this.cursor = cursor;
			this.size = size;
			MoveBeforeStart();
		}
		
		public ReverseRowCursor(IRowCursor cursor)
			: this(cursor, cursor.Count) {
		}
		
		public long Position {
			get { return size - (cursor.Position + 1); }
		}
		
		public long Count {
			get { return size; }
		}
		
		public RowId Current {
			get { return cursor.Current; }
		}
		
		object IEnumerator.Current {
			get { return Current; }
		}
		
		public bool MoveBack() {
			return cursor.MoveNext();
		}
		
		public void MoveBeforeStart() {
			cursor.MoveAfterEnd();
		}
		
		public void MoveAfterEnd() {
			cursor.MoveBeforeStart();
		}
		
		public long MoveTo(long position) {
			return cursor.MoveTo(size - (position + 1));
		}
		
		public void Dispose() {
			cursor.Dispose();
			cursor = null;
		}
		
		public bool MoveNext() {
			return cursor.MoveBack();
		}
		
		public void Reset() {
			cursor.Reset();
		}
		
		public object Clone() {
			return new ReverseRowCursor((IRowCursor)cursor.Clone(), size);
		}
	}
}