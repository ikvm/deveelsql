using System;

namespace Deveel.Data.Sql.State {
	public interface IDatabaseState : IDisposable {
		string Name { get; }

		ISystemState System { get; }


		ITransactionState CreateTransaction();

		void DisposeTransaction(ITransactionState transactionState);

		void CommitTransaction(ITransactionState transactionState);
	}
}