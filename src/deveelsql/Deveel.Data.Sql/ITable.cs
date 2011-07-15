using System;

namespace Deveel.Data.Sql {
	public interface ITable : IDisposable {
		IColumnCollection Columns { get; }

		long RowCount { get; }

		TableName Name { get; }


		IRowCursor GetRowCursor();

		TableRow GetRow(RowId rowid);

		void PrefetchValue(int columnOffset, RowId rowid);

		SqlObject GetValue(int columnOffset, RowId rowid);

		bool RowExists(RowId rowid);
	}
}