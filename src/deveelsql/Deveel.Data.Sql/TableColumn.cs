using System;

namespace Deveel.Data.Sql {
	public class TableColumn {
		private readonly ITable table;
		private readonly string name;
		private readonly SqlType type;
		private readonly bool notNull;
		private readonly long id;

		public TableColumn(ITable table, long id, string name, SqlType type, bool notNull) {
			if (table == null)
				throw new ArgumentNullException("table");
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");
			if (type == null)
				throw new ArgumentNullException("type");

			this.table = table;
			this.id = id;
			this.type = type;
			this.name = name;
			this.notNull = notNull;
		}


		public TableColumn(ITable table, long id, string name, SqlType type)
			 : this(table, id, name, type, false) {
		}

		public TableColumn(ITable table, string name, SqlType type, bool notNull)
			: this(table, -1, name, type, notNull) {
		}

		public TableColumn(ITable table, string name, SqlType type)
			: this(table, name, type, false) {
		}

		public bool NotNull {
			get { return notNull; }
		}

		public long Id {
			get { return id; }
		}

		public bool Exists {
			get { return Id != -1; }
		}

		public ITable Table {
			get { return table; }
		}

		public SqlType Type {
			get { return type; }
		}

		public string Name {
			get { return name; }
		}

		public Variable QualifiedName {
			get { return new Variable(Table.Name, name); }
		}

		public int Offset {
			get { return table.Columns.IndexOf(name); }
		}
	}
}