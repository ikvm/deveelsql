using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.State {
	public interface IColumnCollection : IList<TableColumn> {
		TableColumn this[string columnName] { get; }


		TableColumn Add(string columnName, SqlType columnType, bool notNull);

		bool Contains(string columnName);

		int IndexOf(string columnName);

		bool Remove(string columnName);
	}
}