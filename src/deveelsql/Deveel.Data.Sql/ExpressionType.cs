using System;

namespace Deveel.Data.Sql {
	enum ExpressionType {
		Function = 2,
		FetchVariable = 3,
		FetchStatic = 4,
		FetchParameter = 5,
		FetchGlob = 6,
		Select = 7,
		FetchTable = 8,
		Join = 9,
		AliasTableName = 10,
		AliasVariableName = 11,
		Filter = 12
	}
}