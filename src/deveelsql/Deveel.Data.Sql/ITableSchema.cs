using System;

namespace Deveel.Data.Sql {
	public interface ITableSchema {
		ITable Table { get; }

		int ColumnCount { get; }


		TableColumn AddColumn(string name, SqlType type);

		int GetColumnOffset(string name);

		TableColumn GetColumn(int offset);

		TableColumn[] GetColumns();

		void RemoveColumn(int offset);
	}
}