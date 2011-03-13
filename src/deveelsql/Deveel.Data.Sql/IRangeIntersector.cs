using System;

namespace Deveel.Data.Sql {
	public interface IRangeIntersector {
		bool ValueIntersects(SqlObject[] values);
	}
}