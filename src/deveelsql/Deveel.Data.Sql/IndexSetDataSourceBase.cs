using System;
using System.Collections;
using System.Collections.Generic;

using Deveel.Data.Base;
using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	public abstract class IndexSetDataSourceBase : IIndexSetDataSource {
		private IIndex<RowId> index;
		private readonly ITable table;

		private const byte FirstValue = 1;
		private const byte LastValue = 2;
		private const byte BeforeFirstValue = 3;
		private const byte AfterLastValue = 4;

		protected IndexSetDataSourceBase(ITable table, IIndex<RowId> index) {
			this.table = table;
			this.index = index;
		}

		private long Count {
			get { return index.Count; }
		}

		public abstract IndexResolver IndexResolver { get; }

		public abstract TableName SourceTableName { get; }

		public abstract string Name { get; }

		public abstract IndexCollation Collation { get; }

		private long SearchFirst(IndexResolver resolver, SqlObject[] val) {
			return index.SearchFirst(val, resolver);
		}

		private long SearchLast(IndexResolver resolver, SqlObject[] val) {
			return index.SearchLast(val, resolver);
		}

		private IIndexCursor<RowId> CreateRangeCursor(long p1, long p2) {
			return index.GetCursor(p1, p2);
		}

		private IRowCursor GetCursor() {
			// If size is 0 then return an empty cursor
			if (Count == 0)
				return new SimpleRowCursor(0);

			return new DefaultRowCursor(index.GetCursor());
		}

		private SqlObject[] FirstInCollationOrder(IndexResolver resolver) {
			IRowCursor it = GetCursor();
			if (!it.MoveNext())
				throw new SystemException();

			return resolver.GetValue(it.Current);
		}

		private SqlObject[] LastInCollationOrder(IndexResolver resolver) {
			IRowCursor it = GetCursor();
			it.MoveAfterEnd();
			if (!it.MoveBack())
				throw new SystemException();

			return resolver.GetValue(it.Current);
		}

		private long PositionOfRangePoint(IndexResolver resolver, byte flag, SqlObject[] val) {
			long p;
			SqlObject[] cell;

			switch (flag) {

				// LOWER val
				case (FirstValue):
					// Represents the first value
					if (val.Length == 0)
						return 0;

					p = SearchFirst(resolver, val);

					// (If value not found)
					if (p < 0)
						return -(p + 1);
					return p;

				// UPPER val
				case (LastValue):
					// Represents the last value
					if (val.Length == 0)
						return Count - 1;

					p = SearchLast(resolver, val);

					// (If value not found)
					if (p < 0)
						return -(p + 1) - 1;
					return p;

				// UPPER val
				case (BeforeFirstValue):
					if (val.Length == 0) {
						// Get the last value and search for the first instance of it.
						cell = LastInCollationOrder(resolver);
					} else {
						cell = val;
					}

					p = SearchFirst(resolver, cell);

					// (If value not found)
					if (p < 0)
						return -(p + 1) - 1;
					return p - 1;

				// LOWER val
				case (AfterLastValue):
					if (val.Length == 0) {
						// Get the first value.
						cell = FirstInCollationOrder(resolver);
					} else {
						cell = val;
					}

					p = SearchLast(resolver, cell);

					// (If value not found)
					if (p < 0)
						return -(p + 1);
					return p + 1;

				default:
					throw new SystemException("Unrecognised flag.");
			}
		}

		private IIndexCursor<RowId> RangeCursor(IndexResolver resolver, SqlObject[] lower, SqlObject[] upper, bool lowerAtFirst, bool upperAtLast) {
			long p1 = PositionOfRangePoint(resolver, lowerAtFirst ? FirstValue : AfterLastValue, lower);
			long p2 = PositionOfRangePoint(resolver, upperAtLast ? LastValue : BeforeFirstValue, upper);

			if (p2 < p1)
				return null;

			// Add the range to the set
			return CreateRangeCursor(p1, p2);

		}

		public void Dispose() {
			index = null;
		}

		public IRowCursor Select(SelectableRange range) {
			// If we are selecting the full range, return the full cursor
			if (range.IsFull)
				return GetCursor();

			// The set of cursors
			List<IIndexCursor<RowId>> cursors = new List<IIndexCursor<RowId>>();
			// A resolver for values in this index
			IndexResolver resolver = IndexResolver;
			// For each range pair,
			ISelectableRangeEnumerator en = range.GetEnumerator();
			while (en.MoveNext()) {
				// The range pair information,
				SqlObject[] lower = en.LowerBound;
				SqlObject[] upper = en.UpperBound;
				bool lowerAtFirst = en.IsLowerBoundAtFirst;
				bool upperAtLast = en.IsUpperBoundAtLast;

				// Generate an cursor for the range,
				IIndexCursor<RowId> cursor = RangeCursor(resolver, lower, upper, lowerAtFirst, upperAtLast);
				if (cursor != null)
					// Select the range
					cursors.Add(cursor);
			}

			// And return
			return new DefaultRowCursor(new SubsetIndexCursor(cursors));
		}

		public void Clear() {
			index.Clear();
		}

		public void Insert(RowId rowid) {
			// A resolver for values in this index
			IndexResolver resolver = IndexResolver;
			// Insert the value into sorted position in the index
			index.Insert(resolver.GetValue(rowid), rowid, resolver);
		}

		public void Remove(RowId rowid) {
			// A resolver for values in this index
			IndexResolver resolver = IndexResolver;
			// Remove the value from sorted position in the index
			index.Remove(resolver.GetValue(rowid), rowid, resolver);
		}

		#region SubsetIndexCursor

		private sealed class SubsetIndexCursor : IIndexCursor<RowId> {
			private readonly List<IIndexCursor<RowId>> cursors;
			private long position;
			private int cursorIndex;
			private long cursorOffset;

			public SubsetIndexCursor(List<IIndexCursor<RowId>> cursors) {
				this.cursors = cursors;
				Position = -1;
			}

			public long Count {
				get {
					// Calculate the size of all the cursors in the set.
					long total_size = 0;
					for (int i = 0; i < cursors.Count; ++i) {
						total_size += cursors[i].Count;
					}
					return total_size;
				}
			}

			public long Position {
				get { return position; }
				set {
					// If p is -1 then setup for first value
					if (value == -1) {
						position = -1;
						cursorIndex = 0;
						cursorOffset = -1;
					} else {
						// Is this point in the first cursor?
						cursorIndex = 0;

						IIndexCursor<RowId> iterator = cursors[cursorIndex];
						long left_p = 0;
						long len = iterator.Count;

						// Search through each cursor from left to right
						for (int i = 1; i < cursors.Count; ++i) {
							if (value >= left_p && value < left_p + len) {
								// Break out of p is within the range of this cursor
								break;
							} else {
								left_p += len;
								++cursorIndex;
								iterator = cursors[cursorIndex];
								len = iterator.Count;
							}
						}

						cursorOffset = value - left_p;
						// If found within first cursor then add the first_offset to the
						// cursorOffset.
						if (cursorIndex == 0) {
							cursorOffset += 0;
						}
						position = value;
					}
					// Set the position of the cursor
					if (cursors.Count > 0) {
						cursors[cursorIndex].Position = cursorOffset;
					}
				}
			}

			public bool MoveNext() {
				bool hasNext = ++position < Count;

				if (hasNext) {
					IIndexCursor<RowId> cursor = cursors[cursorIndex];
					if (!cursor.MoveNext()) {
						// Set to before the first of the next cursor in the set
						++cursorIndex;
						cursor = cursors[cursorIndex];
						cursorOffset = -1;
						cursor.Position = cursorOffset;
					}
				}

				return hasNext;
			}

			public void Reset() {
				Position = -1;
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			public RowId Current {
				get { return cursors[cursorIndex].Current; }
			}

			public bool MoveBack() {
				bool hasPrev = --position > 0;
				if (hasPrev) {
					IIndexCursor<RowId> cursor = cursors[cursorIndex];
					if (!cursor.MoveBack()) {
						// Set to after the end of the previous cursor in the set
						--cursorIndex;
						cursor = cursors[cursorIndex];
						cursorOffset = cursor.Count;
						cursor.Position = cursorOffset;
					}
				}
				return hasPrev;
			}

			public RowId Remove() {
				throw new NotSupportedException();
			}

			public object Clone() {
				List<IIndexCursor<RowId>> set_copy = new List<IIndexCursor<RowId>>(cursors.Count);
				foreach (IIndexCursor<RowId> i in cursors) {
					set_copy.Add((IIndexCursor<RowId>) i.Clone());
				}
				return new SubsetIndexCursor(set_copy);
			}

			public void Dispose() {
			}
		}

		#endregion
	}
}