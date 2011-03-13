using System;

namespace Deveel.Data.Sql {
	public interface IMutableTableDataSource : ITableDataSource {
		void SetValue(int column, SqlObject value);

		void Finish(bool complete);

		void BeginInsert();

		void BeginUpdate(long rowid);

		void Remove(long rowid);

		void Remove(IRowCursor rows);
	}
}