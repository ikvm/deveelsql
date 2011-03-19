using System;

namespace Deveel.Data.Sql {
	public enum ConstraintType {
		Unique,
		PrimaryKey,
		Check,
		ForeignKey
	}
}