using System;
using System.Diagnostics;

namespace Deveel.Data.Sql {
	[Serializable]
	[DebuggerDisplay("{ToString(),nq}")]
	public sealed class IndexName : IComparable<IndexName>, IComparable {
		private readonly string name;
		private readonly TableName tableName;

		public IndexName(TableName tableName, string name) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			this.tableName = tableName;
			this.name = name;
		}

		public IndexName(string schema, string tableName, string name)
			: this(TableName.Resolve(schema, tableName), name) {
		}

		public IndexName(string tableName, string name)
			: this(TableName.Resolve(tableName), name) {
		}

		public TableName TableName {
			get { return tableName; }
		}

		public string Name {
			get { return name; }
		}

		public string FullName {
			get { return ToString(); }
		}

		public int CompareTo(IndexName other) {
			if (other == null)
				throw new ArgumentNullException("other");

			int c = tableName.CompareTo(other.TableName);
			if (c != 0)
				return c;

			return name.CompareTo(other.name);
		}

		int IComparable.CompareTo(object obj) {
			if (obj == null)
				throw new ArgumentNullException("obj");

			IndexName other = obj as IndexName;
			if (other == null)
				throw new ArgumentException("The object is not an index name.");

			return CompareTo(other);
		}

		public override string ToString() {
			return TableName + "." + Name;
		}

		public override bool Equals(object obj) {
			IndexName other = obj as IndexName;
			if (other == null)
				return false;

			return CompareTo(other) == 0;
		}

		public override int GetHashCode() {
			return tableName.GetHashCode() + name.GetHashCode();
		}
	}
}