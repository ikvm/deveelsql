using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public sealed class SystemIndex : IIndexSetDataSource {
		private readonly IMutableTableDataSource table;
		private readonly TableName indexName;
		private readonly ITableIndex index;

		public SystemIndex(IMutableTableDataSource table, TableName indexName, ITableIndex index) {
			this.table = table;
			this.index = index;
			this.indexName = indexName;
		}

		private IndexValue ToIndexValue(SqlObject[] val) {
			return new IndexValue(val);
		}

		private IndexCollation CreateIndexCollation() {
			// Get the description,
			string[] columns = index.ColumnNames;
			// If it's a single index description,
			if (columns.Length == 0) {
				string columnName = columns[0];
				int columnOffset = table.GetColumnOffset(new Variable(table.TableName, columnName));
				SqlType columnType = table.GetColumnType(columnOffset);
				return new IndexCollation(new IndexSortColumn(columnName, columnType, true));
			}

			// TODO: support multi-column indexes, etc
			throw new NotSupportedException();
		}

		private IRowCursor RangeCursor(SqlObject[] lower, SqlObject[] upper, bool lowerAtFirst, bool upperAtLast) {
			IndexValue from;
			IndexValue to;

			// Set up 'from'
			bool fromInclusive = lowerAtFirst;
			if (lower.Length == 0) {
				// First value in set, therefore the query is a 'head' type query,
				from = null;
			} else {
				from = ToIndexValue(lower);
			}

			// Set up 'to'
			bool toInclusive = upperAtLast;
			if (upper.Length == 0) {
				// Last value in set, therefore the query is a 'tail' type query,
				to = null;
			} else {
				to = ToIndexValue(upper);
			}

			// full range query,
			if (from == null && to == null)
				return index.GetCursor();

			// 'head' query,
			if (from == null)
				return index.Head(to, toInclusive).GetCursor();

			// 'tail' query,
			if (to == null)
				return index.Tail(from, fromInclusive).GetCursor();

			// 'sub' query,
			return index.Sub(from, fromInclusive, to, toInclusive).GetCursor();
		}

		public void Dispose() {
			//TODO:
		}

		public TableName SourceTableName {
			get { return table.TableName; }
		}

		public TableName Name {
			get { return indexName; }
		}

		public IndexCollation Collation {
			get { return CreateIndexCollation(); }
		}

		public IRowCursor Select(SelectableRange range) {
			// If it's an empty range,
			if (range.IsEmpty)
				return new SimpleRowCursor(0);

			List<IRowCursor> cursors = new List<IRowCursor>();
			// For each range pair,
			ISelectableRangeEnumerator en = range.GetEnumerator();

			while (en.MoveNext()) {
				// The range pair information,
				SqlObject[] lower = en.LowerBound;
				SqlObject[] upper = en.UpperBound;
				bool lowerAtFirst = en.IsLowerBoundAtFirst;
				bool upperAtLast = en.IsUpperBoundAtLast;

				IRowCursor cursor = RangeCursor(lower, upper, lowerAtFirst, upperAtLast);
				// If the pair resolves to something, add it to the iterator set.
				if (cursor != null && cursor.Count > 0)
					cursors.Add(cursor);
			}

			return new SelectedRowCursor(new GroupedRowCursor(cursors));
		}

		public void Clear() {
			throw new NotSupportedException();
		}

		public void Insert(long rowid) {
			index.Add(rowid);
		}

		public void Remove(long rowid) {
			index.Remove(rowid);
		}

		#region SelectedRowCursor

		private class SelectedRowCursor : SqlRowCursor {
			public SelectedRowCursor(IRowCursor rowCursor) 
				: base(rowCursor) {
			}

			public override SqlRowCursor Clone() {
				return new SelectedRowCursor((IRowCursor) BaseCursor.Clone());
			}
		}
		#endregion

		#region GroupedRowCursor

		private class GroupedRowCursor : IRowCursor {
			private readonly List<IRowCursor> cursors;
			private long position;
			private int cursorIndex;
			private long cursorOffset;
			private IRowCursor currentCursor;

			public GroupedRowCursor(List<IRowCursor> cursors) {
				this.cursors = cursors;
				MoveTo(-1);
			}

			public void Dispose() {
			}

			public bool MoveNext() {
				currentCursor = cursors[cursorIndex];
				if (!currentCursor.MoveNext()) {
					// Set to before the first of the next cursor in the set
					currentCursor = cursors[++cursorIndex];
					cursorOffset = -1;
					currentCursor.MoveTo(cursorOffset);
				}
				return ++position < Count;
			}

			public void Reset() {
				MoveTo(-1);
			}

			public long Current {
				get { return currentCursor.Current; }
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			public object Clone() {
				throw new NotSupportedException();
			}

			public long Position {
				get { return position; }
			}

			public long Count {
				get {
					// Calculate the size of all the cursor in the set.
					long totalSize = 0;
					for (int i = 0; i < cursors.Count; ++i) {
						totalSize += cursors[i].Count;
					}
					return totalSize;
				}
			}

			public bool MoveBack() {
				currentCursor = cursors[cursorIndex];
				if (!currentCursor.MoveBack()) {
					// Set to after the end of the previous cursor in the set
					currentCursor = cursors[--cursorIndex];
					cursorOffset = currentCursor.Count;
					currentCursor.MoveTo(cursorOffset);
				}
				return --position > 0;
			}

			public void MoveBeforeStart() {
				MoveTo(-1);
			}

			public void MoveAfterEnd() {
				MoveTo(Count);
			}

			public long MoveTo(long p) {
				// If p is -1 then setup for first value
				if (p == -1) {
					position = -1;
					cursorIndex = 0;
					cursorOffset = -1;
				} else {
					// Is this point in the first cursor?
					cursorIndex = 0;

					IRowCursor cursor = cursors[cursorIndex];
					long leftP = 0;
					long len = cursor.Count;

					// Search through each cursor from left to right
					for (int i = 1; i < cursors.Count; ++i) {
						if (p >= leftP && p < leftP + len) {
							// Break out of p is within the range of this cursor
							break;
						}

						leftP += len;
						++cursorIndex;
						cursor = cursors[cursorIndex];
						len = cursor.Count;
					}

					cursorOffset = p - leftP;
					// If found within first cursor then add the first_offset to the
					// cursorOffset.
					if (cursorIndex == 0) {
						cursorOffset += 0;
					}
					position = p;
				}
				// Set the position of the cursor
				if (cursors.Count > 0) {
					cursors[cursorIndex].MoveTo(cursorOffset);
				}

				return Position;
			}
		}

		#endregion
	}
}