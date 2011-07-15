using System;

namespace Deveel.Data.Sql {
	public interface IDatabaseSystem {
		IDatabase CreateDatabase(string name);

		IDatabase GetDatabase(string name);

		void DeleteDatabase(string name);
	}
}