using System;

namespace Deveel.Data.Sql {
	public interface ITable {
		ITableSchema TableSchema { get; }

		long RowCount { get; }

		TableName Name { get; }


		IRowCursor GetRowCursor();

		TableRow NewRow();

		TableRow GetRow(long rowid);

		void Insert(TableRow row);

		void Update(TableRow row);

		void Delete(long rowid);

		void PrefetchValue(int columnOffset, long rowid);

		SqlValue GetValue(int columnOffset, long rowid);

		bool RowExists(long rowid);
	}
}