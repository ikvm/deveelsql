using System;
using System.Collections.Generic;

namespace Deveel.Data.Base  {
	public interface IIndexCursor<T> : IEnumerator<T>, ICloneable {
		long Count { get; }

		long Position { get; set; }

		bool MoveBack();

		T Remove();
	}
}