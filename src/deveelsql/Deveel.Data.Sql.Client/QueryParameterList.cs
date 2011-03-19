using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Client {
	public sealed class QueryParameterList : IList<QueryParameter> {
		private readonly Query query;
		private readonly List<QueryParameter> parameters;

		internal QueryParameterList(Query query) {
			this.query = query;
			parameters = new List<QueryParameter>();
		}

		private void CheckStyle(QueryParameter parameter) {
			CheckStyle(parameter.Style);
		}

		private void CheckStyle(ParameterStyle parameterStyle) {
			if (query.ParameterStyle != parameterStyle)
				throw new InvalidOperationException();
		}

		private void CheckReadOnly() {
			if (query.IsReadOnly)
				throw new InvalidOperationException("The parameter collection is read-only.");
		}

		public IEnumerator<QueryParameter> GetEnumerator() {
			return parameters.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public void Add(QueryParameter item) {
			CheckStyle(item);
			CheckReadOnly();

			if (query.ParameterStyle == ParameterStyle.Named &&
			    Contains(item.Name))
				throw new ArgumentException();

			parameters.Add(item);
		}

		public QueryParameter Add(string name, SqlObject value, ParameterDirection direction) {
			QueryParameter parameter = new QueryParameter(name, value, direction);
			Add(parameter);
			return parameter;
		}

		public QueryParameter Add(string name, SqlObject value) {
			return Add(name, value, ParameterDirection.Input);
		}

		public QueryParameter Add(SqlObject value, ParameterDirection direction) {
			int id = query.ParameterId++;
			QueryParameter parameter = new QueryParameter(id, value, direction);
			Add(parameter);
			return parameter;
		}

		public QueryParameter Add(SqlObject value) {
			return Add(value, ParameterDirection.Input);
		}

		public void Clear() {
			CheckReadOnly();
			parameters.Clear();
		}

		public bool Contains(QueryParameter item) {
			return IndexOf(item) != -1;
		}

		public bool Contains(string parameterName) {
			return IndexOf(parameterName) != -1;
		}

		public void CopyTo(QueryParameter[] array, int arrayIndex) {
			parameters.CopyTo(array, arrayIndex);
		}

		public bool Remove(QueryParameter item) {
			CheckReadOnly();
			CheckStyle(item);

			int index = IndexOf(item);
			if (index == -1)
				return false;

			RemoveAt(index);
			return true;
		}

		public bool Remove(string parameterName) {
			CheckReadOnly();
			CheckStyle(ParameterStyle.Named);

			int index = IndexOf(parameterName);
			if (index == -1)
				return false;

			parameters.RemoveAt(index);
			return true;
		}

		public int Count {
			get { return parameters.Count; }
		}

		public bool IsReadOnly {
			get { return query.IsReadOnly; }
		}

		public int IndexOf(QueryParameter item) {
			CheckStyle(item);
			if (item.Style == ParameterStyle.Named)
				return IndexOf(item.Name);
				
			return item.Id;
		}

		public int IndexOf(string parameterName) {
			CheckStyle(ParameterStyle.Named);
			for (int i = 0; i < parameters.Count; i++) {
				QueryParameter parameter = parameters[i];
				if (parameter.Name == parameterName)
					return i;
			}

			return -1;
		}

		public void Insert(int index, QueryParameter item) {
			CheckReadOnly();
			CheckStyle(item);
			parameters.Insert(index, item);
		}

		public void RemoveAt(int index) {
			CheckReadOnly();
			parameters.RemoveAt(index);
		}

		public QueryParameter this[int index] {
			get { return parameters[index]; }
			set {
				CheckReadOnly();
				if (value == null) {
					parameters.RemoveAt(index);
				} else {
					CheckStyle(value);
					if (value.Style == ParameterStyle.Named) {
						int prevIndex = IndexOf(value.Name);
						if (prevIndex != -1 && prevIndex != index)
							throw new ArgumentException();
					}

					parameters[index] = value;
				}
			}
		}

		public QueryParameter this[string parameterName] {
			get {
				CheckStyle(ParameterStyle.Named);
				int index = IndexOf(parameterName);
				return index == -1 ? null : parameters[index];
			}
			set { 
				CheckReadOnly();
				CheckStyle(ParameterStyle.Named);
				int index = IndexOf(parameterName);
				if (index == -1) {
					if (value == null)
						throw new ArgumentNullException("value");
					Add(parameterName, value.Value);
				} else {
					this[index] = value;
				}
			}
		}
	}
}