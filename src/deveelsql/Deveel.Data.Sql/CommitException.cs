using System;

namespace Deveel.Data.Sql {
	public sealed class CommitException : Exception {
		internal CommitException(string message, Exception innerException)
			: base(message, innerException) {
		}

		internal  CommitException(string message)
			: base(message) {
		}
	}
}