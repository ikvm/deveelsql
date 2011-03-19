using System;

using Deveel.Data.Base;

namespace Deveel.Data.Sql {
	public abstract class IndexResolver : IIndexedObjectComparer<RowId, SqlObject[]> {
		public abstract SqlObject[] GetValue(RowId rowId);

		public virtual int Compare(RowId rowId, SqlObject[] value) {
			SqlObject[] ob = GetValue(rowId);
			return SqlObject.Compare(ob, value);
		}
	}
}