using System;

namespace Deveel.Data.Sql.State {
	public interface ICommitableTable : ITable {
		void BeginCommit();

		void EndCommit();

		void CopyTo(ITable table);
	}
}