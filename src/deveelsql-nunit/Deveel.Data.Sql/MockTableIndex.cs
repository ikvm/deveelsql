using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Base;

namespace Deveel.Data.Sql {
	public sealed class MockTableIndex : ITableIndex {
		private readonly TableName tableName;
		private readonly TableName name;
		private readonly string type;
		private readonly string[] columns;
		private readonly SortedIndex index; 

		public MockTableIndex(TableName name, string type, TableName tableName, string[] columns) {
			this.tableName = tableName;
			this.type = type;
			this.columns = columns;
			this.name = name;
			index = new SortedIndex(new MemoryStream(1024));
		}

		public IEnumerator<long> GetEnumerator() {
			return index.GetCursor();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public long Count {
			get { return index.Count; }
		}

		public long First {
			get { return index[0]; }
		}

		public long Last {
			get { return index[Count - 1]; }
		}

		public string[] ColumnNames {
			get { return columns; }
		}

		public TableName TableName {
			get { return tableName; }
		}

		public TableName Name {
			get { return name; }
		}

		public string Type {
			get { return type; }
		}

		public void Add(long rowId) {
			index.InsertSortKey(rowId);
		}

		public void Remove(long rowId) {
			index.RemoveSortKey(rowId);
		}

		public int CompareTo(long rowid, IndexValue value) {
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
	}
}