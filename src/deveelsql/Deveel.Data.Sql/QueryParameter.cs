using System;
using System.Text;

namespace Deveel.Data.Sql {
	public sealed class QueryParameter : ICloneable {
		private readonly ParameterStyle style;
		private readonly string name;
		private readonly int id;
		private ParameterDirection direction;
		private readonly SqlObject value;

		public QueryParameter(string name, SqlObject value, ParameterDirection direction)
			: this(-1, name, ParameterStyle.Named, value, direction) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");
		}

		public QueryParameter(string name, SqlObject value)
			: this(name, value, ParameterDirection.Input) {
		}

		public QueryParameter(int id, SqlObject value, ParameterDirection direction)
			: this(id, null, ParameterStyle.Marker, value, direction) {
			if (id < 0)
				throw new ArgumentOutOfRangeException("id");
		}

		public QueryParameter(int id, SqlObject value)
			: this(id, value, ParameterDirection.Input) {
		}

		private QueryParameter(int id, string name, ParameterStyle style, SqlObject value, ParameterDirection direction) {
			this.id = id;
			this.name = name;
			this.style = style;
			this.value = value;
			this.direction = direction;
		}

		public SqlObject Value {
			get { return value; }
		}

		public int Id {
			get { return id; }
		}

		public string Name {
			get { return name; }
		}

		public ParameterStyle Style {
			get { return style; }
		}

		public ParameterDirection Direction {
			get { return direction; }
			set { direction = value; }
		}

		public object Clone() {
			return new QueryParameter(id, name, style, value, direction);
		}

		public override bool Equals(object obj) {
			QueryParameter parameter = obj as QueryParameter;
			if (parameter == null)
				return false;

			if (style != parameter.style)
				return false;

			if (style == ParameterStyle.Marker &&
				id != parameter.id)
				return false;
			if (style == ParameterStyle.Named &&
				name != parameter.name)
				return false;

			return true;
		}

		public override int GetHashCode() {
			return style == ParameterStyle.Marker ? id.GetHashCode() : name.GetHashCode();
		}

		public override string ToString() {
			StringBuilder sb = new StringBuilder();
			if (style == ParameterStyle.Named) {
				sb.Append(name);
			} else {
				sb.Append("?");
			}

			if (value != null) {
				sb.Append(" = ");
				sb.Append(value.Value.ToString());
				sb.Append(" (");
				sb.Append(value.Type.ToString());
				sb.Append(")");
			}
			return sb.ToString();
		}
	}
}