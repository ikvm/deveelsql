using System;

namespace Deveel.Data.Sql {
	public enum JoinType {
		Inner,
		Outer,
		OuterLeft,
		OuterRight,
		Cartesian
	}
}