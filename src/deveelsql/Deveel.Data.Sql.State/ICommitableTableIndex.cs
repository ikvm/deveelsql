using System;

namespace Deveel.Data.Sql.State {
	public interface ICommitableTableIndex : ITableIndex {
		void CopyTo(ITableIndex index);
	}
}