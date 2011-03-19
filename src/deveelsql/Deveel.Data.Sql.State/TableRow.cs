using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.State {
	public class TableRow {
		private readonly RowId id;
		private readonly ITable table;
		private bool dirty;
		private readonly bool mutable;

		private readonly Dictionary<int, SqlObject> cachedValues;

		public TableRow(ITable table, RowId id) {
			if (table == null)
				throw new ArgumentNullException("table");
			if (id == null)
				throw new ArgumentNullException("id");

			this.table = table;
			this.id = id;

			mutable = (table is IMutableTable);
			cachedValues = new Dictionary<int, SqlObject>(16);
		}

		public ITable Table {
			get { return table; }
		}

		public bool IsReadOnly {
			get { return !mutable; }
		}

		public bool Exists {
			get { return table.RowExists(id); }
		}

		private IMutableTable Mutable {
			get { return (IMutableTable) table; }
		}

		public RowId Id {
			get { return id; }
		}

		public SqlObject this[int columnOffset] {
			get { return GetValue(columnOffset); }
			set { SetValue(columnOffset, value); }
		}

		public SqlObject this[string columnName] {
			get { return GetValue(columnName); }
			set { SetValue(columnName, value); }
		}

		private void CheckMutable() {
			if (!mutable)
				throw new InvalidOperationException("The row is read-only.");
		}

		private int GetColumnOffset(string columnName) {
			int offset = table.Columns.IndexOf(columnName);
			if (offset == -1)
				throw new ArgumentException("Column '" + columnName + "' not found in table " + table.Name);

			return offset;
		}

		public virtual SqlObject GetValue(int columnOffset) {
			SqlObject obj;
			if (dirty) {
				cachedValues.TryGetValue(columnOffset, out obj);
			} else if (!cachedValues.TryGetValue(columnOffset, out obj)) {
				obj = table.GetValue(columnOffset, id);
				cachedValues[columnOffset] = obj;
			}

			return obj;
		}

		public SqlObject GetValue(string columnName) {
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException("columnName");

			return GetValue(GetColumnOffset(columnName));
		}

		public virtual void SetValue(int columnOffset, SqlObject value) {
			CheckMutable();

			if (columnOffset < 0 || columnOffset >= table.Columns.Count)
				throw new ArgumentOutOfRangeException("columnOffset");

			TableColumn column = table.Columns[columnOffset];

			if (value == null) {
				value = SqlObject.MakeNull(column.Type);
			} else if (!column.Type.Equals(value.Type)) {
				value = value.CastTo(column.Type);
			}

			cachedValues[columnOffset] = value;
			dirty = true;
		}

		public void SetValue(string columnName, SqlObject value) {
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException("columnName");

			SetValue(GetColumnOffset(columnName), value);
		}

		public virtual void ClearValues() {
			CheckMutable();
			cachedValues.Clear();
			dirty = false;
		}

		public void Insert() {
			CheckMutable();

			Mutable.Insert(this);
		}

		public void Update() {
			CheckMutable();

			Mutable.Update(this);
		}
	}
}