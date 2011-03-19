using System;

namespace Deveel.Data.Sql.State {
	public interface ISystemState {
		IDatabaseState CreateDatabase(string name);

		IDatabaseState GetDatabase(string name);

		void DeleteDatabase(string name);
	}
}