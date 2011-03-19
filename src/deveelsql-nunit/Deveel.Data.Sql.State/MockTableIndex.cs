using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Base;

namespace Deveel.Data.Sql.State {
	public sealed class MockTableIndex : ITableIndex {
		private readonly MockTable table;
		private readonly IndexName name;
		private readonly string type;
		private readonly string[] columns;
		private readonly bool[] orders;
		private readonly BinarySortedIndex<RowId> index;
		private IIndexedObjectComparer<RowId, IndexValue> comparer;
		private readonly IIndexedObjectComparer<RowId, RowId> keyComparer;
		private readonly MockTableIndex baseIndex;

		public MockTableIndex(MockTable table, IndexName name, string type, string[] columns, bool[] orders) {
			this.orders = orders;
			this.type = type;
			this.columns = columns;
			this.name = name;
			this.table = table;
			index = new BinarySortedIndex<RowId>(new MemoryStream(1024), new RowIdIndex.Resolver(8));
			keyComparer = new KeyComparer();
		}

		public IEnumerator<RowId> GetEnumerator() {
			return index.GetCursor();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public long Count {
			get { return index.Count; }
		}

		public RowId First {
			get { return index[0]; }
		}

		public RowId Last {
			get { return index[Count - 1]; }
		}

		public string[] ColumnNames {
			get { return columns; }
		}

		public bool[] ColumnOrder {
			get { return orders; }
		}

		public IndexName Name {
			get { return name; }
		}

		public string Type {
			get { return type; }
		}

		private IIndexedObjectComparer<RowId, IndexValue> IndexComparer {
			get {
				if (comparer == null) {
					if (baseIndex != this) {
						comparer = baseIndex.IndexComparer;
					} else {
						comparer = new IndexValueComparer(table);
					}
				}
				return comparer;
			}
		}

		private class IndexValueComparer : IIndexedObjectComparer<RowId, IndexValue> {
			private readonly ITable table;

			public IndexValueComparer(ITable table) {
				this.table = table;
			}

			public int Compare(RowId reference, IndexValue value) {
				throw new NotImplementedException();
			}
		}

		public void Add(RowId rowId) {
			index.Insert(rowId, rowId, keyComparer);
		}

		public void Remove(RowId rowId) {
			index.Remove(rowId, rowId, keyComparer);
		}

		public int CompareTo(RowId rowid, IndexValue value) {
			throw new NotImplementedException();
		}

		public bool Contains(IndexValue value) {
			throw new NotImplementedException();
		}

		public IRowCursor GetCursor() {
			throw new NotImplementedException();
		}

		public ITableIndex Head(IndexValue toElement, bool inclusive) {
			throw new NotImplementedException();
		}

		public ITableIndex Tail(IndexValue fromElement, bool inclusive) {
			throw new NotImplementedException();
		}

		public ITableIndex Sub(IndexValue fromElement, bool fromInclusive, IndexValue toElement, bool toInclusive) {
			throw new NotImplementedException();
		}

		public ITableIndex Head(IndexValue toElement) {
			throw new NotImplementedException();
		}

		public ITableIndex Tail(IndexValue fromElement) {
			throw new NotImplementedException();
		}

		public ITableIndex Sub(IndexValue fromElement, IndexValue toElement) {
			throw new NotImplementedException();
		}

		public void Clear() {
			index.Clear();
		}

		#region KeyComparer

		private class KeyComparer : IIndexedObjectComparer<RowId, RowId> {
			public int Compare(RowId indexed, RowId value) {
				throw new NotImplementedException();
			}
		}

		#endregion
	}
}