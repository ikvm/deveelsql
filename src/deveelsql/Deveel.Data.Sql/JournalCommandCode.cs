using System;

namespace Deveel.Data.Sql {
	internal enum JournalCommandCode : byte {
		// (params: row_index)
		RowAdd = 1,

		// (params: row_index)
		RowRemove = 2,

		// (params: row_index)
		RowUpdate = 3,


		IndexAdd = 17,
		IndexDelete = 18,
		IndexUpdate = 19,

		// (params: column_id)
		ColumnAdd = 21,

		// (params: column_id)
		ColumnRemove = 22,

		// (params: table_id)
		TableCreate = 25,

		// (params: table_id)
		TableDrop = 26,

		// (params: table_id)
		TableAlter = 27
	}
}