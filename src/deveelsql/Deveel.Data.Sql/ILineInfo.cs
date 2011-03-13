using System;

namespace Deveel.Data.Sql {
	interface ILineInfo {
		int Column { get; set; }
		
		int Line { get; set; }
	}
}