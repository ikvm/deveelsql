using System;

namespace Deveel.Data.Sql {
	public enum FunctionParameterMatch {
		// 1
		Exact,
		
		// ?
		ZeroOrOne,
		
		// +
		OneOrMore,
		
		// *
		ZeroOrMore
	}
}