using System;

namespace Deveel.Data.Sql {
	public interface IIndexSetDataSource : IDisposable {
		TableName SourceTableName { get; }
		
		string Name { get; }
		
		IndexCollation Collation { get; }
		
		
		 IRowCursor Select(SelectableRange range);
		 
		 void Clear();
		 
		 void Insert(RowId rowid);
		 
		 void Remove(RowId rowid);
	}
}