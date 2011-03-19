using System;

namespace Deveel.Data.Sql.Client {
	public interface IQueryContext : IDisposable {
		bool IsClosed { get; }

		bool IsUpdatable { get; }

		long RowCount { get; }

		int ColumnCount { get; }


		ResultColumn GetColumn(int column);

		RowId GetRowId(long offset);

		bool IsNativelyConverted(int column, long rowOffset);

		object GetValue(int columnOffset, long rowOffset);

		void SetValue(int columnOffset, RowId rowid, object value);

		void DeleteRow(long rowOffset);

		void UpdateRow(long rowOffset);

		long InsertRow();

		void Finish(bool commit);

		void Close();
	}
}