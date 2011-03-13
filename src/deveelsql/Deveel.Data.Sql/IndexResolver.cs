using System;

using Deveel.Data.Base;

namespace Deveel.Data.Sql {
	public abstract class IndexResolver : IIndexedObjectComparer<SqlObject[]> {
		public abstract SqlObject[] GetValue(long rowid);

		public virtual int Compare(long index, SqlObject[] value) {
			SqlObject[] ob = GetValue(index);
			return SqlObject.Compare(ob, value);
		}
	}
}