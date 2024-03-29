﻿using System;
using System.Collections;

namespace Deveel.Data.Sql {
	sealed class SimpleRowCursor : IRowCursor {
		private long pos;
		private readonly long size;
		
		public SimpleRowCursor(long size) {
			this.size = size;
			pos = -1;
		}
		
		public long Position {
			get { return pos; }
		}
		
		public long Count {
			get { return size; }
		}
		
		public RowId Current {
			get { return new RowId(pos); }
		}
		
		object IEnumerator.Current {
			get { return Current; }
		}
		
		public bool MoveBack() {
			return --pos >= 0;
		}
		
		public void MoveBeforeStart() {
			MoveTo(-1);
		}
		
		public void MoveAfterEnd() {
			MoveTo(size);
		}
		
		public long MoveTo(long position) {
			return pos = position;
		}
		
		public void Dispose() {
		}
		
		public bool MoveNext() {
			return ++pos < size;
		}
		
		public void Reset() {
			pos = -1;
		}
		
		public object Clone() {
			return new SimpleRowCursor(size);
		}
	}
}