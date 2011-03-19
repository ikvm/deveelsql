using System;

namespace Deveel.Data.Sql {
	public enum SqlTypeCode {
		Null = 0,
		Boolean = 1,
		Numeric = 6,
		String = 7,
		Binary = 8,
		DateTime = 9,
		UserType = 20,
		Array = 100
	}
}