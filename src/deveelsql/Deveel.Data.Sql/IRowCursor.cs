using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public interface IRowCursor : IEnumerator<long>, ICloneable {
		long Position { get; }
		
		long Count { get; }
		
		
		bool MoveBack();
		
		void MoveBeforeStart();
		
		void MoveAfterEnd();
		
		long MoveTo(long position);
	}
}