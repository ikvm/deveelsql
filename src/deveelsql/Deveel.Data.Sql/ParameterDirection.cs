using System;

namespace Deveel.Data.Sql {
	public enum ParameterDirection {
		Input = 1,
		InputOutput = 3,
		Output = 2,
		ReturnValue = 4
	}
}