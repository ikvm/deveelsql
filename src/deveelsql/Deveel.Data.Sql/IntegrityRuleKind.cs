using System;

namespace Deveel.Data.Sql {
	public enum IntegrityRuleKind {
		Primary,
		Unique,
		ImportedForeignKey,
		ExportedForeignKey,
	}
}