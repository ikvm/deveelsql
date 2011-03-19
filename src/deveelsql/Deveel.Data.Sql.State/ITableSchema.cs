using System;

namespace Deveel.Data.Sql.State {
	public interface ITableSchema {
		ITable Table { get; }

		int ColumnCount { get; }


		TableColumn AddColumn(string name, SqlType type, bool notNull);

		long GetColumnId(int offset);

		int GetColumnOffset(string name);

		TableColumn GetColumn(int offset);

		void RemoveColumn(int offset);
	}
}