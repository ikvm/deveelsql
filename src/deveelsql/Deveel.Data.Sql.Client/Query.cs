using System;

namespace Deveel.Data.Sql.Client {
	[Serializable]
	public sealed class Query : ICloneable {
		private readonly string text;
		private readonly QueryParameterList parameters;
		private bool readOnly;
		private readonly ParameterStyle style;

		internal int ParameterId = -1;

		public Query(string text, ParameterStyle style) {
			this.text = text;
			this.style = style;
			parameters = new QueryParameterList(this);
		}

		public Query(string text)
			: this(text, ParameterStyle.Default) {
		}

		public string Text {
			get { return text; }
		}

		public bool IsReadOnly {
			get { return readOnly; }
		}

		public ParameterStyle ParameterStyle {
			get { return style; }
		}

		public QueryParameterList Parameters {
			get { return parameters; }
		}

		internal void MakeReadOnly() {
			readOnly = true;
		}

		public object Clone() {
			Query query = new Query((string) text.Clone(), style);
			foreach(QueryParameter parameter in Parameters) {
				query.Parameters.Add((QueryParameter) parameter.Clone());
			}
			query.readOnly = readOnly;
			return query;
		}
	}
}