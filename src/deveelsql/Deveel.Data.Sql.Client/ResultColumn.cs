using System;

namespace Deveel.Data.Sql.Client {
	public sealed class ResultColumn {
		private readonly string name;
		private readonly SqlType type;
		private readonly bool nullable;

		public ResultColumn(string name, SqlType type, bool nullable) {
			this.name = name;
			this.nullable = nullable;
			this.type = type;
		}

		public ResultColumn(string name, SqlType type)
			: this(name, type, true) {
		}

		public ResultColumn(string name, string type, bool nullable)
			: this(name, SqlType.Parse(type), nullable) {
		}

		public ResultColumn(string name, string type)
			: this(name, type, true) {
		}

		public bool IsNullable {
			get { return nullable; }
		}

		public string Name {
			get { return name; }
		}

		public string TypeName {
			get { return type.ToString(); }
		}
	}
}