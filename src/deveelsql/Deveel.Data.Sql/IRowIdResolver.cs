using System;

namespace Deveel.Data.Sql {
	public interface IRowIdResolver {
		RowId ResolveRowId(long value);
	}
}