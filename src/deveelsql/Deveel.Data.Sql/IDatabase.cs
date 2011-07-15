using System;

namespace Deveel.Data.Sql {
	public interface IDatabase : IDisposable {
		string Name { get; }

		IDatabaseSystem System { get; }


		ITransactionState CreateTransaction();

		void DisposeTransaction(ITransactionState transactionState);

		void CommitTransaction(ITransactionState transactionState);
	}
}