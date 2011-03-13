using System;
using System.IO;

namespace Deveel.Data.Sql {
	public interface ISystemState : IDisposable {
		object SyncRoot { get; }

		Stream CreateStream();
	}
}