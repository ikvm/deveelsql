using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public interface ISelectableRangeEnumerator : IEnumerator<RangePair> {
		SqlObject[] LowerBound { get; }

		bool IsLowerBoundAtFirst { get; }

		SqlObject[] UpperBound { get; }

		bool IsUpperBoundAtLast { get; }
	}
}