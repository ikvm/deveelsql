using System;

namespace Deveel.Data.Sql {
	internal class ColumnsSystemTable : TableBase {
		private readonly SystemTransaction transaction;
		private readonly long tableId;
		private int rowCount;
		private TableName[] tableNames;
		private IColumnCollection[] tableColumns;
		private int[] tableIndex;

		public ColumnsSystemTable(SystemTransaction transaction, long tableId)
			: base(new TableName(SystemTableNames.SystemSchema, "Columns")) {
			this.transaction = transaction;
			this.tableId = tableId;
		}

		public override long RowCount {
			get {
				Init();
				return rowCount;
			}
		}

		private int TableIndex(int index, int low, int high) {
			if (low > high)
				throw new ApplicationException("Index error");
			if (low == high)
				return low;

			if ((high - low) < 3) {
				for (int i = low; i < high; ++i) {
					if (index < tableIndex[i])
						return i;
				}

				throw new ApplicationException("Index error");
			}

			int mid = (low + high)/2;
			int mid_ref = tableIndex[mid];
			if (mid_ref > index)
				return TableIndex(index, low, mid + 1);
			if (mid_ref < index)
				return TableIndex(index, mid + 1, high);

			return mid + 1;
		}

		private void Init() {
			if (tableColumns == null) {
				rowCount = 0;

				// Get the list of all table names in the database,
				TableName[] tables = transaction.GetTableNames();
				tableNames = new TableName[tables.Length];
				tableColumns = new IColumnCollection[tables.Length];
				tableIndex = new int[tables.Length];

				// For each table
				for (int i = 0; i < tables.Length; ++i) {
					TableName tname = tables[i];

					// Get the table source
					SystemTable table_source = transaction.GetTable(tname);
					// Get and record the table information
					tableNames[i] = table_source.Name;
					tableColumns[i] = table_source.Columns;

					int cc = tableColumns[i].Count;
					if (cc == 0)
						cc = 1;

					rowCount += cc;
					tableIndex[i] = rowCount;
				}
			}
		}

		protected override void SetupColumns() {
		}

		public override SqlObject GetValue(int columnOffset, RowId rowid) {
			Init();

			int i = TableIndex(rowid, 0, tableIndex.Length);
			IColumnCollection columns = tableColumns[i];
			int col_count = columns.Count;
			TableName tableName = tableNames[i];
			// If no column, return null
			if (col_count == 0) {
				// We output the schema and name but the rest of the info is null
				// for tables with 0 columns
				if (columnOffset == 0)
					return tableName.Schema;
				if (columnOffset == 1)
					return tableName.Name;
					
				return SqlObject.Null;
			}

			int seq_no = col_count - (def_index[i] - (int)row_id);
			TableColumn col_def = columns[seq_no];

			switch (columnOffset) {
				case 0:  // schema
					return columns.getSchema();
				case 1:  // table
					return TObject.stringVal(columns.getName());
				case 2:  // column
					return TObject.stringVal(col_def.getName());
				case 3:  // sql_type
					return TObject.longVal(col_def.getSQLType());
				case 4:  // type_desc
					return TObject.stringVal(col_def.getSQLTypeString());
				case 5:  // size
					return TObject.longVal(col_def.getSize());
				case 6:  // scale
					return TObject.longVal(col_def.getScale());
				case 7:  // not_null
					return TObject.booleanVal(col_def.isNotNull());
				case 8:  // default
					return TObject.stringVal(col_def.getDefaultExpressionString());
				case 9:  // store hint
					return TObject.stringVal(col_def.getStoreHint());
				case 10:  // seq_no
					return TObject.longVal(seq_no);
				default:
					throw new Error("Column out of bounds.");
			}
		}
	}
}