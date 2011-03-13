using System;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Sql {
	public sealed class HeapSystemState : ISystemState {
		private readonly Dictionary<int, Stream> allocated;
		private int idSeq = -1;

		public HeapSystemState() {
			allocated = new Dictionary<int, Stream>();
		}

		public void Dispose() {
			List<Stream> list = new List<Stream>(allocated.Values);
			for (int i = 0; i < list.Count; i++) {
				list[i].Close();
			}
			GC.SuppressFinalize(this);
		}

		public object SyncRoot {
			get { return this; }
		}

		public Stream CreateStream() {
			lock (SyncRoot) {
				int id = ++idSeq;
				HeapStream stream = new HeapStream(this, id);
				allocated.Add(id, stream);
				return stream;
			}
		}

		#region HeapStream

		private class HeapStream : MemoryStream {
			private readonly int id;
			private readonly HeapSystemState state;

			public HeapStream(HeapSystemState state, int id)
				: base (1024) {
				this.state = state;
				this.id = id;
			}

			public override void Close() {
				state.allocated.Remove(id);
				base.Close();
			}
		}

		#endregion
	}
}