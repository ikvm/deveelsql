using System;

namespace Deveel.Data.Sql {
	public class TableColumn {
		private readonly ITableSchema schema;
		private readonly string name;
		private readonly SqlType type;

		public TableColumn(ITableSchema schema, string name, SqlType type) {
			this.schema = schema;
			this.type = type;
			this.name = name;
		}

		public ITableSchema Schema {
			get { return schema; }
		}

		public SqlType Type {
			get { return type; }
		}

		public string Name {
			get { return name; }
		}

		public int Offset {
			get { return schema.GetColumnOffset(name); }
		}
	}
}