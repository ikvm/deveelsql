using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Base {
	public class BinarySortedIndex<T> : IIndex<T> where T : IComparable<T> {
		private readonly Stream stream;
		private readonly bool readOnly;
		private readonly IBinaryIndexResolver<T> resolver;

		private readonly IndexComparer keyComparer = new IndexComparer();

		public BinarySortedIndex(Stream stream, IBinaryIndexResolver<T> resolver, bool readOnly) {
			if (stream == null)
				throw new ArgumentNullException("stream");
			if (resolver == null)
				throw new ArgumentNullException("resolver");
			if (!stream.CanRead)
				throw new ArgumentException("Cannot read from the stream.");
			if (!readOnly && !stream.CanWrite)
				throw new ArgumentException("The stream is not writeable.");

			this.resolver = resolver;
			this.stream = stream;
			this.readOnly = readOnly;
		}

		public BinarySortedIndex(Stream stream, IBinaryIndexResolver<T> resolver)
			: this(stream, resolver, false) {
		}

		IEnumerator<T> IEnumerable<T>.GetEnumerator() {
			return GetCursor();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetCursor();
		}

		public long Count {
			get { return stream.Length / resolver.ItemLength; }
		}

		public bool IsReadOnly {
			get { return readOnly; }
		}

		public IndexComparer KeyComparer {
			get { return keyComparer; }
		}

		private long SearchFirst<S>(S value, IIndexedObjectComparer<T, S> c, long low, long high) {
			if (low > high)
				return -1;

			while (true) {
				// If low is the same as high, we are either at the first value or at
				// the position to insert the value,
				if ((high - low) <= 4) {
					for (long i = low; i <= high; ++i) {
						stream.Position = (i * resolver.ItemLength);
						T val = resolver.Read(stream);
						int res = c.Compare(val, value);

						if (res == 0)
							return i;
						if (res > 0)
							return -(i + 1);
					}
					return -(high + 2);
				}

				// The index half way between the low and high point
				long mid = (low + high) >> 1;
				// Reaf the middle value from the data file,
				stream.Position = (mid * resolver.ItemLength);
				T mid_val = resolver.Read(stream);

				// Compare it with the value
				int res1 = c.Compare(mid_val, value);
				if (res1 < 0) {
					low = mid + 1;
				} else if (res1 > 0) {
					high = mid - 1;
				} else {  // if (res == 0)
					high = mid;
				}
			}
		}

		private long SearchLast<S>(S value, IIndexedObjectComparer<T, S> c, long low, long high) {
			if (low > high)
				return -1;

			while (true) {
				// If low is the same as high, we are either at the last value or at
				// the position to insert the value,
				if ((high - low) <= 4) {
					for (long i = high; i >= low; --i) {
						stream.Position = (i * resolver.ItemLength);
						T val = resolver.Read(stream);
						int res = c.Compare(val, value);
						if (res == 0)
							return i;
						if (res < 0)
							return -(i + 2);
					}
					return -(low + 1);
				}

				// The index half way between the low and high point
				long mid = (low + high) >> 1;
				// Reaf the middle value from the data file,
				stream.Position = (mid * resolver.ItemLength);
				T mid_val = resolver.Read(stream);

				// Compare it with the value
				int res1 = c.Compare(mid_val, value);
				if (res1 < 0) {
					low = mid + 1;
				} else if (res1 > 0) {
					high = mid - 1;
				} else {  // if (res == 0)
					low = mid;
				}
			}

		}

		private void SearchFirstAndLast<S>(S value, IIndexedObjectComparer<T, S> c, out long first, out long last) {
			long low = 0;
			long high = Count - 1;

			if (low > high) {
				first = -1;
				last = -1;
				return;
			}

			while (true) {
				// If low is the same as high, we are either at the first value or at
				// the position to insert the value,
				if ((high - low) <= 4) {
					first = SearchFirst(value, c, low, high);
					last = SearchLast(value, c, low, high);
					return;
				}

				// The index half way between the low and high point
				long mid = (low + high) >> 1;
				// Reaf the middle value from the data file,
				stream.Position = (mid * resolver.ItemLength);
				T mid_val = resolver.Read(stream);

				// Compare it with the value
				int res = c.Compare(mid_val, value);
				if (res < 0) {
					low = mid + 1;
				} else if (res > 0) {
					high = mid - 1;
				} else {  // if (res == 0)
					first = SearchFirst(value, c, low, high);
					last = SearchLast(value, c, low, high);
					return;
				}
			}
		}

		private void CheckReadOnly() {
			if (readOnly)
				throw new ApplicationException("The source is read-only.");
		}

		public void Clear() {
			CheckReadOnly();
			stream.SetLength(0);
		}

		public void Clear(long offset, long size) {
			CheckReadOnly();

			if (offset < 0 || offset + size > Count)
				throw new ArgumentOutOfRangeException();

			stream.Position = (offset + size) * resolver.ItemLength;
			stream.SetLength(stream.Length - (size * resolver.ItemLength));
		}

		public long SearchFirst<S>(S value, IIndexedObjectComparer<T, S> c) {
			long low = 0;
			long high = Count - 1;

			return SearchFirst(value, c, low, high);
		}

		public long SearchLast<S>(S value, IIndexedObjectComparer<T, S> c) {
			long low = 0;
			long high = Count - 1;

			return SearchLast(value, c, low, high);
		}

		public T this[long offset] {
			get {
				stream.Position = (offset * resolver.ItemLength);
				return resolver.Read(stream);
			}
		}

		public IIndexCursor<T> GetCursor(long start, long end) {
			// Make sure start and end aren't out of bounds
			if (start < 0 || end > Count || start - 1 > end)
				throw new ArgumentOutOfRangeException();

			return new Cursor(this, start, end);
		}

		public IIndexCursor<T> GetCursor() {
			return GetCursor(0, Count - 1);
		}

		public void Insert<S>(S value, T index, IIndexedObjectComparer<T, S> c) {
			CheckReadOnly();

			// Search for the position of the last value in the set, 
			long pos = SearchLast(value, c);
			// If pos < 0 then the value was not found,
			if (pos < 0) {
				// Correct it to the point where the value must be inserted
				pos = -(pos + 1);
			} else {
				// If the value was found in the set, insert after the last value,
				++pos;
			}

			// Insert the value by moving to the position, shifting the data n bytes
			// and writing the index value.
			stream.Position = (pos * resolver.ItemLength);
			stream.SetLength(stream.Length + resolver.ItemLength);
			resolver.Write(index, stream);
		}

		public bool InsertUnique<S>(S value, T index, IIndexedObjectComparer<T, S> c) {
			CheckReadOnly();

			// Search for the position of the last value in the set, 
			long pos = SearchLast(value, c);
			// If pos < 0 then the value was not found,
			if (pos < 0) {
				// Correct it to the point where the value must be inserted
				pos = -(pos + 1);
			} else {
				// If the value was found in the set, return false and don't change the
				// list.
				return false;
			}

			// Insert the value by moving to the position, shifting the data n bytes
			// and writing the index value.
			stream.Position = (pos * resolver.ItemLength);
			stream.SetLength(stream.Length + resolver.ItemLength);
			resolver.Write(index, stream);
			// Return true because we changed the list,
			return true;
		}

		public void Remove<S>(S value, T index, IIndexedObjectComparer<T, S> c) {
			CheckReadOnly();

			// Search for the position of the last value in the set, 
			long p1, p2;
			SearchFirstAndLast(value, c, out p1, out p2);

			// If the value isn't found report the error,
			if (p1 < 0)
				throw new ApplicationException("Value '" + value + "' was not found in the set.");

			IIndexCursor<T> cursor = GetCursor(p1, p2);
			while (cursor.MoveNext()) {
				if (cursor.Current.Equals(index)) {
					// Remove the value and return
					cursor.Remove();
					return;
				}
			}
		}

		public void Add(T index) {
			CheckReadOnly();

			stream.Position = stream.Length;
			resolver.Write(index, stream);
		}

		public void Insert(T index, long offset) {
			CheckReadOnly();

			if (offset < 0)
				throw new ArgumentOutOfRangeException("offset");

			long sz = Count;
			// Shift and insert
			if (offset < sz) {
				stream.SetLength(stream.Length + resolver.ItemLength);
				stream.Position = (offset * resolver.ItemLength);
				resolver.Write(index, stream);
			}
				// Insert at end
			else if (offset == sz) {
				stream.Position = (sz * resolver.ItemLength);
				resolver.Write(index, stream);
			}
		}

		public T RemoveAt(long offset) {
			CheckReadOnly();

			if (offset < 0 || offset > Count)
				throw new ArgumentOutOfRangeException("offset");

			stream.Position = (offset * resolver.ItemLength);
			// Read the value then remove it
			T val = resolver.Read(stream);
			stream.SetLength(stream.Length - resolver.ItemLength);
			// Return the value
			return val;
		}

		public void InsertSortKey(T value) {
			Insert(value, value, keyComparer);
		}

		public void RemoveSortKey(T value) {
			Remove(value, value, keyComparer);
		}

		public bool ContainsSortKey(T value) {
			return SearchFirst(value, keyComparer) >= 0;
		}

		#region Cursor

		private class Cursor : IIndexCursor<T> {
			private readonly BinarySortedIndex<T> index;
			private readonly long start;
			private long end;
			private long p = -1;
			private int lastDir;

			public Cursor(BinarySortedIndex<T> index, long start, long end) {
				this.index = index;
				this.end = end;
				this.start = start;
			}

			public void Dispose() {
			}

			public bool MoveNext() {
				if (++p + start <= end) {
					lastDir = 1;
					return true;
				}

				return false;
			}

			public void Reset() {
				p = -1;
				lastDir = 0;
				index.stream.Position = start * index.resolver.ItemLength;
			}

			public T Current {
				get {
					// Check the point is within the bounds of the iterator,
					if (p < 0 || start + p > end)
						throw new IndexOutOfRangeException();

					// Change the position and fetch the data,
					index.stream.Position = ((start + p) * index.resolver.ItemLength);
					return index.resolver.Read(index.stream);
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			public object Clone() {
				return new Cursor(index, start, end);
			}

			public long Count {
				get { return (end - start) + 1; }
			}

			public long Position {
				get { return p; }
				set {
					p = value;
					lastDir = 0;
				}
			}

			public bool MoveBack() {
				if (--p > 0) {
					lastDir = 2;
					return true;
				}

				return false;
			}

			public T Remove() {
				index.stream.Position = ((start + p) * index.resolver.ItemLength);
				T v = index.resolver.Read(index.stream);
				index.stream.SetLength(index.stream.Length - index.resolver.ItemLength);

				if (lastDir == 1)
					--p;

				--end;

				// Returns the value we removed,
				return v;
			}
		}

		#endregion

		#region IndexComparer

		public sealed class IndexComparer : IIndexedObjectComparer<T, T> {
			public int Compare(T indexed, T value) {
				return indexed.CompareTo(value);
			}
		}

		#endregion
	}
}