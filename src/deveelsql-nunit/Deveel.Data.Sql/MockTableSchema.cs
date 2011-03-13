using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public sealed class MockTableSchema  : ITableSchema {
		private readonly MockTable table;
		private readonly List<TableColumn> columns;

		public MockTableSchema(MockTable table) {
			this.table = table;
			columns = new List<TableColumn>();
		}

		public ITable Table {
			get { return table; }
		}

		public int ColumnCount {
			get { return columns.Count; }
		}

		public TableColumn AddColumn(string name, SqlType type) {
			TableColumn column = new TableColumn(this, name, type);
			columns.Add(column);
			return column;
		}

		public int GetColumnOffset(string name) {
			for (int i = 0; i < columns.Count; i++) {
				if (columns[i].Name == name)
					return i;
			}

			return -1;
		}

		public TableColumn GetColumn(int offset) {
			return columns[offset];
		}

		public TableColumn[] GetColumns() {
			return columns.ToArray();
		}

		public void RemoveColumn(int offset) {
			columns.RemoveAt(offset);
		}
	}
}