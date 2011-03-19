using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql.State {
	public class ColumnCollection : IColumnCollection {
		private readonly ITable table;
		private bool readOnly;
		private readonly List<TableColumn> columns;
		private bool ignoreCase;

		public ColumnCollection(ITable table, bool ignoreCase) {
			this.table = table;
			this.ignoreCase = ignoreCase;

			columns = new List<TableColumn>();
		}

		public ColumnCollection(ITable table)
			: this(table, false) {
		}

		private void CheckNotReadOnly() {
			if (IsReadOnly)
				throw new InvalidOperationException("This list is read-only.");
		}

		public IEnumerator<TableColumn> GetEnumerator() {
			return columns.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public virtual void Add(TableColumn item) {
			CheckNotReadOnly();
			columns.Add(item);
		}

		public virtual TableColumn Add(string columnName, SqlType columnType, bool notNull) {
			CheckNotReadOnly();
			TableColumn column = new TableColumn(table, -1, columnName, columnType, notNull);
			columns.Add(column);
			return column;
		}

		public virtual void Clear() {
			CheckNotReadOnly();
			columns.Clear();
		}

		public bool Contains(TableColumn item) {
			return IndexOf(item) != -1;
		}

		public void CopyTo(TableColumn[] array, int arrayIndex) {
			columns.CopyTo(array, arrayIndex);
		}

		public bool Remove(TableColumn item) {
			CheckNotReadOnly();
			int index = IndexOf(item);
			if (index == -1)
				return false;

			RemoveAt(index);
			return true;
		}

		public virtual int Count {
			get { return columns.Count; }
		}

		public virtual bool IgnoreCase {
			get { return ignoreCase; }
			set { ignoreCase = value; }
		}

		public virtual bool IsReadOnly {
			get { return readOnly; }
		}

		public virtual int IndexOf(TableColumn item) {
			return columns.IndexOf(item);
		}

		public virtual void Insert(int index, TableColumn item) {
			CheckNotReadOnly();
			columns.Insert(index, item);
		}

		public virtual void RemoveAt(int index) {
			CheckNotReadOnly();
			columns.RemoveAt(index);
		}

		public TableColumn this[int index] {
			get { return GetColumn(index); }
			set { SetColumn(index, value); }
		}

		public virtual TableColumn GetColumn(int index) {
			return columns[index];
		}

		public virtual void SetColumn(int index, TableColumn column) {
			CheckNotReadOnly();
			columns[index] = column;			
		}

		public TableColumn this[string columnName] {
			get { return this[IndexOf(columnName)]; }
		}

		public bool Contains(string columnName) {
			return IndexOf(columnName) != -1;
		}

		public virtual int IndexOf(string columnName) {
			for (int i = 0; i < Count; i++) {
				if (String.Compare(this[i].Name, columnName, ignoreCase) == 0)
					return i;
			}

			return -1;
		}

		public bool Remove(string columnName) {
			int index = IndexOf(columnName);
			if (index == -1)
				return false;

			columns.RemoveAt(index);
			return true;
		}

		public void MakeReadOnly() {
			readOnly = true;
		}
	}
}