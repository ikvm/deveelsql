using System;
using System.Collections;

namespace Deveel.Data.Sql {
	public class PrefetchRowCursor : IRowCursor {
		private readonly IRowCursor cursor;
		private readonly ITableDataSource table;

		private readonly long[] pageRead = new long[16];
		private int readp = 0;

		private const long PageSize = 80;


		public PrefetchRowCursor(IRowCursor cursor, ITableDataSource table) {
			this.cursor = cursor;
			this.table = table;

			for (int i = 0; i < pageRead.Length; i++) {
				pageRead[i] = -1;
			}
		}

		private void PageHint() {
			// Hint at future fetches,
			long current_p = cursor.Position;

			// Did we fetch the page?
			// Search the 'page_read' array to see
			long page_no = (current_p / PageSize);
			bool found = false;
			for (int i = 0; i < pageRead.Length; ++i) {
				if (pageRead[i] == page_no) {
					found = true;
					break;
				}
			}

			// If not found,
			if (!found) {
				pageRead[readp] = page_no;
				++readp;
				if (readp >= pageRead.Length) {
					readp = 0;
				}
				// TODO: Remember the row_id's read from cursor.MoveNext() so these values
				//   can be passed back the next time 'next' or 'previous' is called
				//   instead of having to fetch the values again.

				// Prefetch the page,
				long pagep = (page_no * PageSize);
				cursor.MoveTo(pagep - 1);
				for (int i = 0; i < PageSize && cursor.MoveNext(); ++i) {
					try {
						long row_id = cursor.Current;
						table.FetchValue(-1, row_id);
					} catch (ApplicationException e) {
						//TODO: log ...
						throw;
					}
				}
				// Revert to the position,
				MoveTo(current_p);
			}
		}


		public void MoveAfterEnd() {
			cursor.MoveAfterEnd();
		}

		public void MoveBeforeStart() {
			cursor.MoveBeforeStart();
		}

		public object Clone() {
			return new PrefetchRowCursor((IRowCursor) cursor.Clone(), table);
		}

		public bool MoveNext() {
			return cursor.MoveNext();
		}

		public void Reset() {
			for (int i = 0; i < pageRead.Length; i++)
				pageRead[i] = -1;

			cursor.Reset();
		}

		object IEnumerator.Current {
			get { return Current; }
		}

		public bool MoveBack() {
			PageHint();
			return cursor.MoveBack();
		}

		public long Current {
			get { return cursor.Current; }
		}

		public long MoveTo(long position) {
			return cursor.MoveTo(position);
		}

		public long Position {
			get { return cursor.Position; }
		}

		public long Count {
			get { return cursor.Count; }
		}

		public void Dispose() {
		}
	}
}