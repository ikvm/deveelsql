using System;

namespace Deveel.Data.Base {
	public interface IIndexedObjectComparer<T, S> {
		int Compare(T indexed, S value);
	}
}