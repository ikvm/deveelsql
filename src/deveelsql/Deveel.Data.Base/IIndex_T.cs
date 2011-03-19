using System;
using System.Collections.Generic;

namespace Deveel.Data.Base {
	public interface IIndex<T> : IEnumerable<T> {
		long Count { get; }

		T this[long offset] { get; }


		void Clear();

		void Clear(long offset, long size);

		long SearchFirst<S>(S value, IIndexedObjectComparer<T, S> c);

		long SearchLast<S>(S value, IIndexedObjectComparer<T, S> c);

		IIndexCursor<T> GetCursor(long start, long end);

		IIndexCursor<T> GetCursor();

		void Insert<S>(S value, T index, IIndexedObjectComparer<T, S> c);

		bool InsertUnique<S>(S value, T index, IIndexedObjectComparer<T, S> c);

		void Remove<S>(S value, T index, IIndexedObjectComparer<T, S> c);

		void Add(T value);

		void Insert(T value, long offset);

		T RemoveAt(long offset);

		void InsertSortKey(T value);

		void RemoveSortKey(T value);

		bool ContainsSortKey(T value);
	}

}