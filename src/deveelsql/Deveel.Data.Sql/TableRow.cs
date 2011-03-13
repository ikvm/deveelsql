using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public sealed class TableRow {
		private readonly long id;
		private readonly ITable table;
		private bool dirty;

		private readonly Dictionary<int, SqlObject> cachedValues;

		public TableRow(ITable table, long id) {
			if (table == null)
				throw new ArgumentNullException("table");

			this.table = table;
			this.id = id;

			cachedValues = new Dictionary<int, SqlObject>(16);
		}

		public TableRow(ITable table)
			: this(table, -1) {
		}

		public ITable Table {
			get { return table; }
		}

		public bool HasRowId {
			get { return id != -1; }
		}

		public bool Exists {
			get { return HasRowId && table.RowExists(id); }
		}

		public long Id {
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

		private int GetColumnOffset(string columnName) {
			int offset = table.TableSchema.GetColumnOffset(columnName);
			if (offset == -1)
				throw new ArgumentException("Column '" + columnName + "' not found in table " + table.Name);

			return offset;
		}

		public SqlObject GetValue(int columnOffset) {
			SqlObject obj;
			if (dirty) {
				cachedValues.TryGetValue(columnOffset, out obj);
			} else if (!cachedValues.TryGetValue(columnOffset, out obj) && HasRowId) {
				SqlValue value = table.GetValue(columnOffset, id);
				SqlType type = table.TableSchema.GetColumn(columnOffset).Type;
				obj = new SqlObject(type, value);
				cachedValues[columnOffset] = obj;
			}

			return obj;
		}

		public SqlObject GetValue(string columnName) {
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException("columnName");

			return GetValue(GetColumnOffset(columnName));
		}

		public void SetValue(int columnOffset, SqlObject value) {
			cachedValues[columnOffset] = value;
			dirty = true;
		}

		public void SetValue(string columnName, SqlObject value) {
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException("columnName");

			SetValue(GetColumnOffset(columnName), value);
		}

		public void ClearValues() {
			cachedValues.Clear();
			dirty = false;
		}
	}
}