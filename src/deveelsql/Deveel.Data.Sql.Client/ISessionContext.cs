using System;

namespace Deveel.Data.Sql.Client {
	public interface ISessionContext : IDisposable {
		IQueryContext CreateContext();

		IQueryContext Execute(IQueryContext context);

		IQueryContext Execute(Query query);

		void Close();
	}
}