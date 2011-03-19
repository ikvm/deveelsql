using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.State {
	public sealed class HeapSystemState : ISystemState {
		private readonly Dictionary<string, HeapDatabaseState> databases;

		public HeapSystemState() {
			databases = new Dictionary<string, HeapDatabaseState>();
		}

		public IDatabaseState CreateDatabase(string name) {
			if (databases.ContainsKey(name))
				throw new InvalidOperationException("Database " + name + " already exists.");

			HeapDatabaseState state = new HeapDatabaseState(this, name);
			databases[name] = state;
			return state;
		}

		public IDatabaseState GetDatabase(string name) {
			HeapDatabaseState state;
			if (databases.TryGetValue(name, out state))
				return null;
			return state;
		}

		public void DeleteDatabase(string name) {
			databases.Remove(name);
		}
	}
}