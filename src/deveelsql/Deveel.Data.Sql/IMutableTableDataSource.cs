using System;

namespace Deveel.Data.Sql {
	public interface IMutableTableDataSource : ITableDataSource {
		void SetValue(int column, long rowid, SqlObject value);

		void Finish(bool complete);

		long BeginInsert();

		long BeginUpdate(long rowid);

		void Remove(long rowid);

		void Remove(IRowCursor rows);
	}
}