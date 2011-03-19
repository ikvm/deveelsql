using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace Deveel.Data.Sql.Client {
	public sealed class DeveelSqlParameterCollection : IDataParameterCollection {
		private readonly DeveelSqlCommand command;
		private readonly List<DeveelSqlParameter> parameters;

		internal DeveelSqlParameterCollection(DeveelSqlCommand command) {
			this.command = command;
			parameters = new List<DeveelSqlParameter>();
		}

		public DeveelSqlCommand Command {
			get { return command; }
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public IEnumerator<DeveelSqlParameter> GetEnumerator() {
			return parameters.GetEnumerator();
		}

		void ICollection.CopyTo(Array array, int index) {
			CopyTo((DeveelSqlParameter[])array, index);
		}

		public void CopyTo(DeveelSqlParameter[] array, int index) {
			parameters.CopyTo(array, index);
		}

		public int Count {
			get { return parameters.Count; }
		}

		public object SyncRoot {
			get { return this; }
		}

		public bool IsSynchronized {
			get { return false; }
		}

		int IList.Add(object value) {
			Add(value);
			return parameters.Count - 1;
		}

		public DeveelSqlParameter Add(object value) {
			if (!(value is DeveelSqlParameter)) {
				value = new DeveelSqlParameter(value);
			}

			return Add((DeveelSqlParameter)value);
		}


		public DeveelSqlParameter Add(DeveelSqlParameter parameter) {
			if (parameter == null)
				throw new ArgumentNullException("parameter");

			parameters.Add(parameter);
			return parameter;
		}

		public DeveelSqlParameter Add(SqlType type, object value) {
			DeveelSqlParameter parameter = new DeveelSqlParameter(type, value);
			Add(parameter);
			return parameter;
		}

		bool IList.Contains(object value) {
			throw new NotSupportedException();
		}

		public void Clear() {
			parameters.Clear();
		}

		int IList.IndexOf(object value) {
			throw new NotSupportedException();
		}

		public void Insert(int index, object value) {
			if (!(value is DeveelSqlParameter))
				value = new DeveelSqlParameter(value);
			Insert(index, (DeveelSqlParameter)value);
		}

		public void Insert(int index, DeveelSqlParameter parameter) {
			parameters.Insert(index, parameter);
		}

		void IList.Remove(object value) {
			throw new NotSupportedException();
		}

		public void RemoveAt(int index) {
			parameters.RemoveAt(index);
		}

		object IList.this[int index] {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		public DeveelSqlParameter this[int index] {
			get { return parameters[index]; }
			set { parameters[index] = value; }
		}

		bool IList.IsReadOnly {
			get { return false; }
		}

		bool IList.IsFixedSize {
			get { return false; }
		}

		bool IDataParameterCollection.Contains(string parameterName) {
			throw new NotSupportedException();
		}

		int IDataParameterCollection.IndexOf(string parameterName) {
			throw new NotSupportedException();
		}

		void IDataParameterCollection.RemoveAt(string parameterName) {
			throw new NotSupportedException();
		}

		object IDataParameterCollection.this[string parameterName] {
			get { throw new NotSupportedException(); }
			set { throw new NotSupportedException(); }
		}
	}
}