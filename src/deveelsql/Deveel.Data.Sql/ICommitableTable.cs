using System;

namespace Deveel.Data.Sql {
	public interface ICommitableTable : ITable {
		void BeginCommit();

		void EndCommit();

		void CopyTo(ITable table);
	}
}