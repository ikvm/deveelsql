using System;
using System.Collections;

namespace Deveel.Data.Sql {
	public abstract class SystemRowCursor : IRowCursor {
		private readonly IRowCursor rowCursor;

		internal SystemRowCursor(IRowCursor rowCursor) {
			this.rowCursor = rowCursor;
		}

		public IRowCursor BaseCursor {
			get { return rowCursor; }
		}

		public void Dispose() {
			rowCursor.Dispose();
		}

		public bool MoveNext() {
			return rowCursor.MoveNext();
		}

		public void Reset() {
			rowCursor.Reset();
		}

		public RowId Current {
			get { return rowCursor.Current; }
		}

		object IEnumerator.Current {
			get { return Current; }
		}

		object ICloneable.Clone() {
			return Clone();
		}

		public abstract SystemRowCursor Clone();

		public long Position {
			get { return rowCursor.Position; }
		}

		public long Count {
			get { return rowCursor.Count; }
		}

		public bool MoveBack() {
			return rowCursor.MoveBack();
		}

		public void MoveBeforeStart() {
			rowCursor.MoveTo(-1);
		}

		public void MoveAfterEnd() {
			rowCursor.MoveTo(rowCursor.Count);
		}

		public long MoveTo(long position) {
			rowCursor.MoveTo(position);
			return rowCursor.Position;
		}
	}
}