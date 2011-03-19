using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Client {
	internal class QueryResult : IDisposable {
		private DeveelSqlCommand command;
		private IQueryContext queryContext;

		private int maxRowCount = Int32.MaxValue;
		private long realIndexL = Int64.MaxValue;

		private Dictionary<string, int> columnHash;
		private Dictionary<int, ResultColumn> columnDescMap;

		private long insertStateRow = -1;
		private long updateStateRow = -1;

		private long prevRealIndexL = -1;

		public QueryResult(DeveelSqlCommand command) {
			this.command = command;
		}

		public QueryResult(IQueryContext context) {
			command = null;
			ConnectionSetup(context);
		}

		internal IQueryContext QueryContext {
			get { return queryContext; }
		}

		internal bool IsUpdate {
			get {
				// Must have 1 col and 1 row and the title of the column must be
				// 'result' aliased.
				return (ColumnCount == 1 && RowCount == 1 && GetColumn(0).Name.Equals("@aresult"));
			}
		}

		internal int UpdateCount {
			get {
				if (!IsUpdate)
					return -1;

				object ob = queryContext.GetValue(0, 0);
				return ob is BigNumber ? ((BigNumber)ob).ToInt32() : 0;
			}
		}

		internal long RowCount {
			get {
				// The row count is whatever is the least between max_row_count (the
				// maximum the user has set) and result_row_count (the actual number of
				// rows in the result.
				return System.Math.Min(queryContext.RowCount, maxRowCount);
			}
		}

		internal int ColumnCount {
			get { return queryContext.ColumnCount; }
		}

		private RowId CurrentRowId {
			get {
				// If it's the insert row
				if (insertStateRow != -1 && realIndexL == insertStateRow)
					return queryContext.GetRowId(realIndexL);
				if (realIndexL >= 0 && realIndexL < queryContext.RowCount)
					return queryContext.GetRowId(realIndexL);

				throw new InvalidOperationException("Cursor out of bounds.");
			}
		}

		private void RealIndexUpdate() {
			// If we were in an insert or update state, we complete operation but don't
			// commit.
			if ((insertStateRow >= 0 && insertStateRow != realIndexL) ||
				(updateStateRow >= 0 && updateStateRow != realIndexL)) {
				// Cancel any pending updates
				CancelUpdates();
			}
		}

		private bool IsUpdatable {
			get { return queryContext.IsUpdatable; }
		}

		private void CheckUpdatable() {
			if (!IsUpdatable) {
				throw new InvalidOperationException();
			}
		}

		private void CancelUpdates() {
			if (insertStateRow >= 0 || updateStateRow >= 0) {
				// Clean state whether we succeed or not
				insertStateRow = -1;
				updateStateRow = -1;
				queryContext.Finish(false);
			}
		}

		internal void ConnectionSetup(IQueryContext context) {
			queryContext = context;
			realIndexL = -1;
			insertStateRow = -1;
			updateStateRow = -1;
			prevRealIndexL = -1;
		}

		internal ResultColumn GetColumn(int column) {
			// Allocate the map if it doesn't exist
			if (columnDescMap == null) {
				columnDescMap = new Dictionary<int, ResultColumn>();
			}
			// Is it in the cache?
			ResultColumn cd;
			if (!columnDescMap.TryGetValue(column, out cd)) {
				cd = queryContext.GetColumn(column);
				columnDescMap[column] = cd;
			}
			return cd;
		}

		internal void CloseCurrentResult() {
			if (queryContext != null && !queryContext.IsClosed) {
				// Request to close the current result set
				queryContext.Close();
			}

			realIndexL = Int64.MaxValue;
			// Clear the column name -> index mapping,
			if (columnHash != null) {
				columnHash.Clear();
			}
			if (columnDescMap != null) {
				columnDescMap.Clear();
			}
			insertStateRow = -1;
			updateStateRow = -1;
		}

		internal RowId GetRowIdForUpdate(RowId rowid) {
			CheckUpdatable();

			// If we aren't in an update state, we go to update state now
			if (insertStateRow == -1 && updateStateRow == -1) {
				// Check the given row_id points to the record of the current cursor
				RowId current_rowid = CurrentRowId;
				if (current_rowid.Equals(rowid)) {
					// It is on the current cursor, so update the record
					queryContext.UpdateRow(realIndexL);
					updateStateRow = realIndexL;
					// And return the rowid of the update
					return CurrentRowId;
				}
			}

			// Otherwise, we must already be in a mutation state so return the row
			// id given.
			return rowid;
		}

		internal void SetCurrentRowCell(IValue value, int col) {
			CheckUpdatable();

			// If we aren't in an update state, we go to update state now
			if (insertStateRow == -1 && updateStateRow == -1) {
				// Check the cursor is valid before we update
				if (realIndexL < 0 || realIndexL >= RowCount) {
					throw new InvalidOperationException();
				}
				queryContext.UpdateRow(realIndexL);
				updateStateRow = realIndexL;
			}

			// Get the rowid for the current row we are on
			RowId rowid = CurrentRowId;
			// Determine if we upload the value as a materialized value or in parts as
			// a binary object.

			// Is the value materialized?
			if (value.IsConverted) {
				// The materialized object
				object valueObj = value.Value;
				// true if the object should be streamed
				bool should_stream_ob = false;
				bool is_string = false;
				// If it's not null
				if (valueObj != null) {
					// Is it a very large materialized object?
					long binarySize = 0;
					if (value.Type.IsString) {
						binarySize = valueObj.ToString().Length * 2;
						is_string = true;
					} else if (value.Type.IsBinary) {
						binarySize = ((byte[])valueObj).Length;
					}
					// If the size of the object is greater than 16k, we stream it
					if (binarySize >= 16 * 1024) {
						should_stream_ob = true;
					}
				}
				// If we aren't streaming it, use the set materialized value and
				// return.
				if (!should_stream_ob) {
					queryContext.SetValue(col, rowid, valueObj);
				}
					// Otherwise the materialized object needs to be streamed,
				else {
					//TODO: support BLOBs
				}
			}
		}

		internal int FindColumnIndex(string name) {
			// For speed, we keep column name -> column index mapping in the hashtable.
			// This makes column reference by string faster.
			if (columnHash == null)
				columnHash = new Dictionary<string, int>();

			name = name.ToUpper();

			int index;
			if (!columnHash.TryGetValue(name, out index)) {
				int colCount = ColumnCount;
				// First construct an unquoted list of all column names
				String[] cols = new String[colCount];
				for (int i = 0; i < colCount; ++i) {
					string colName = queryContext.GetColumn(i).Name;
					if (colName.StartsWith("\"")) {
						colName = colName.Substring(1, colName.Length - 2);
					}
					// Strip any codes from the name
					if (colName.StartsWith("@")) {
						colName = colName.Substring(2);
					}
					
					colName = colName.ToUpper();
					cols[i] = colName;
				}

				for (int i = 0; i < colCount; ++i) {
					string colName = cols[i];
					if (colName.Equals(name)) {
						columnHash[name] = i;
						return i;
					}
				}

				// If not found then search for column name ending,
				string pointName = "." + name;
				for (int i = 0; i < colCount; ++i) {
					string colName = cols[i];
					if (colName.EndsWith(pointName)) {
						columnHash[name] = i;
						return i;
					}
				}

				throw new ArgumentException("Couldn't find column with name: " + name);
			}

			return index;
		}

		internal object GetRawColumn(int column) {
			// ASSERTION -
			// Is the given column in bounds?
			if (column < 0 || column > ColumnCount)
				throw new ArgumentOutOfRangeException("column", column, "Column index out of bounds: 1 > " + column + " > " + ColumnCount);

			// Get the rowid (this also ensures the current row is within bounds).
			RowId rowid = CurrentRowId;

			Object val = null;
			// Is the cell object naturally materialized?
			if (queryContext.IsNativelyConverted(column, realIndexL)) {
				// Yes, so fetch the object, etc
				val = queryContext.GetValue(column, realIndexL);
			}
				// Otherwise we need to use the blob interface to access the value,
			else {
				//TODO: support BLOBs
			}

			return val;
		}

		public void Dispose() {
			try {
				Close();
			} catch (Exception) {
				// Ignore
				// We ignore exceptions because handling cases where the server
				// connection has broken for many ResultSets would be annoying.
			}

			command = null;
		}

		public void Close() {
			CloseCurrentResult();
		}

		public bool Next() {
			long rowCount = RowCount;
			if (realIndexL < rowCount) {
				++realIndexL;
				RealIndexUpdate();
			}
			return (realIndexL < rowCount);
		}

		public void MoveToCurrentRow() {
			if (insertStateRow >= 0 && realIndexL == insertStateRow) {
				realIndexL = prevRealIndexL;
				RealIndexUpdate();
			}
		}

		public void BeginInsertRow() {
			CheckUpdatable();
			// If we are already positioned on the insert row generate
			// an error
			if (insertStateRow >= 0 && realIndexL == insertStateRow)
				throw new InvalidOperationException("Already on insert row");

			// If the result set isn't in an insert state
			if (insertStateRow < 0) {
				// Make a new row in the backed table,
				long row_num = queryContext.InsertRow();
				insertStateRow = row_num;
			}

			// Move pointer to the insert_state row,
			prevRealIndexL = realIndexL;
			realIndexL = insertStateRow;
			RealIndexUpdate();
		}

		public void InsertRow() {
			if (insertStateRow < 0)
				throw new InvalidOperationException("No data to insert");

			insertStateRow = -1;
			queryContext.Finish(true);
		}

		public void UpdateRow() {
			if (updateStateRow < 0)
				throw new InvalidOperationException("No data to update");

			updateStateRow = -1;
			queryContext.Finish(true);
		}

		public void DeleteRow() {
			CheckUpdatable();
			// If we are updating, cancel the update first
			if (updateStateRow >= 0) {
				updateStateRow = -1;
				queryContext.Finish(false);
			}
			// Fail if we are inserting
			if (insertStateRow >= 0) {
				throw new InvalidOperationException("deleteRow on insert row");
			}
			// Check bounds
			if (realIndexL >= 0 && realIndexL < RowCount) {
				queryContext.DeleteRow(realIndexL);
				// Move the cursor to the previous row if we aren't at the end
				// (this is in the JDBC 3.0 spec)
				if (realIndexL < RowCount) {
					--realIndexL;
				}
				queryContext.Finish(true);
			} else {
				throw new InvalidOperationException("Cursor out of bounds");
			}
		}

		public void Update(int column, object value) {
			SetCurrentRowCell(new ObjectValue(value), column);
		}

		public void Update(string columnName, object value) {
			Update(FindColumnIndex(columnName), value);
		}
	}
}