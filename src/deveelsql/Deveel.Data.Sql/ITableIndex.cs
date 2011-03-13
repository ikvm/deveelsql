using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public interface ITableIndex : IEnumerable<long> {
		long Count { get; }

		long First { get; }

		long Last { get; }

		string[] ColumnNames { get; }

		TableName TableName { get; }

		TableName Name { get; }

		string Type { get; }



		void Add(long rowId);

		void Remove(long rowId);


		int CompareTo(long rowid, IndexValue value);


		bool Contains(IndexValue value);

		IRowCursor GetCursor();

		ITableIndex Head(IndexValue toElement, bool inclusive);

		ITableIndex Tail(IndexValue fromElement, bool inclusive);

		ITableIndex Sub(IndexValue fromElement, bool fromInclusive, IndexValue toElement, bool toInclusive);

		ITableIndex Head(IndexValue toElement);

		ITableIndex Tail(IndexValue fromElement);

		ITableIndex Sub(IndexValue fromElement, IndexValue toElement);
	}
}