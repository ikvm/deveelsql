using System;

namespace Deveel.Data.Sql.State {
	public interface IMutableTable : ITable {
		TableRow NewRow();

		void Insert(TableRow row);

		void Update(TableRow row);

		void Delete(RowId rowid);

		void Commit();

		void Undo();
	}
}