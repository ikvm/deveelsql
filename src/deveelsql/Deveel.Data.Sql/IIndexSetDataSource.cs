using System;

namespace Deveel.Data.Sql {
	public interface IIndexSetDataSource : IDisposable {
		TableName SourceTableName { get; }
		
		TableName Name { get; }
		
		IndexCollation Collation { get; }
		
		
		 IRowCursor Select(SelectableRange range);
		 
		 void Clear();
		 
		 void Insert(long rowid);
		 
		 void Remove(long rowid);
	}
}