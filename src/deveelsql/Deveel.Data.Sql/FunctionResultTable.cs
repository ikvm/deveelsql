using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public sealed class FunctionResultTable : ITableDataSource {
		private readonly SqlObject[] values;
		private readonly Column[] columns;
		
		private static readonly TableName FunctionTableName = new TableName("@FunctionResult@");

		public FunctionResultTable(SqlObject[] values) {
			this.values = values;
			
			columns = new Column[values.Length];
			for(int i = 0; i < values.Length; i++) {
				columns[i] = new Column(new Variable(FunctionTableName, "column" + i), values[i].Type);
			}
		}

		void IDisposable.Dispose() {
		}
		
		public TableName TableName {
			get { return FunctionTableName; }
		}

		public long RowCount {
			get { return 1; }
		}
		
		public int ColumnCount {
			get { return columns.Length; }
		}

		public IRowCursor GetRowCursor() {
			return new SimpleRowCursor(RowCount);
		}
		
		public int GetColumnOffset(Variable name) {
			for (int i = 0; i < columns.Length; i++) {
				if (columns[i].Name.Equals(name))
					return i;
			}
			
			return -1;
		}
		
		public SqlType GetColumnType(int offset) {
			return columns[offset].Type;
		}
		
		public Variable GetColumnName(int offset) {
			return columns[offset].Name;
		}

		public SqlObject GetValue(int column, long row) {
			if (row > 0)
				throw new ArgumentOutOfRangeException("row");

			return values[column];
		}

		void ITableDataSource.FetchValue(int column, long row) {
		}

		IEnumerator<long> IEnumerable<long>.GetEnumerator() {
			return GetRowCursor();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetRowCursor();
		}

		#region Column
		
		class Column {
			private readonly SqlType type;
			private readonly Variable name;
			
			public Column(Variable name, SqlType type) {
				this.name = name;
				this.type = type;
			}
			
			public Variable Name {
				get { return name; }
			}
			
			public SqlType Type {
				get { return type; }
			}
		}
		
		#endregion
	}
}