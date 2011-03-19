using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.State {
	public interface ITableIndex : IEnumerable<RowId> {
		long Count { get; }

		RowId First { get; }

		RowId Last { get; }

		string[] ColumnNames { get; }

		bool[] ColumnOrder { get; }

		IndexName Name { get; }

		string Type { get; } 


		void Add(RowId rowid);

		void Remove(RowId rowid);


		int CompareTo(RowId rowid, IndexValue value);


		bool Contains(IndexValue value);

		IRowCursor GetCursor();

		ITableIndex Head(IndexValue toElement, bool inclusive);

		ITableIndex Tail(IndexValue fromElement, bool inclusive);

		ITableIndex Sub(IndexValue fromElement, bool fromInclusive, IndexValue toElement, bool toInclusive);

		ITableIndex Head(IndexValue toElement);

		ITableIndex Tail(IndexValue fromElement);

		ITableIndex Sub(IndexValue fromElement, IndexValue toElement);

		void Clear();
	}
}