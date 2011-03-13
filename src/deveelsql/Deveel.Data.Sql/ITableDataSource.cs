using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public interface ITableDataSource : IDisposable, IEnumerable<long> {
		int ColumnCount { get; }
		
		long RowCount { get; }
		
		TableName TableName { get; }
		
		
		int GetColumnOffset(Variable columnName);
		
		Variable GetColumnName(int offset);
		
		SqlType GetColumnType(int offset);
		
		
		SqlObject GetValue(int column, long row);
		
		void FetchValue(int column, long row);
		
		
		IRowCursor GetRowCursor();
	}
}