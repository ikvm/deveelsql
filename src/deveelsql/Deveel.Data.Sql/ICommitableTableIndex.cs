using System;

namespace Deveel.Data.Sql {
	public interface ICommitableTableIndex : ITableIndex {
		void CopyTo(ITableIndex index);
	}
}