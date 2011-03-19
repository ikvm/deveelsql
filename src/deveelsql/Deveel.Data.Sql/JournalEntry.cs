using System;

namespace Deveel.Data.Sql {
	internal struct JournalEntry {
		private readonly JournalCommandCode code;
		private readonly long id;
		private readonly RowId rowid;
		private readonly bool forTable;
		private readonly bool forRow;

		public JournalEntry(JournalCommandCode code, long id) : this() {
			this.code = code;
			this.id = id;
			rowid = null;
			forTable = true;
			forRow = false;
		}

		public JournalEntry(JournalCommandCode code, RowId rowid) : this() {
			this.code = code;
			this.rowid = rowid;
			id = -1;
			forTable = false;
			forRow = true;
		}

		public bool ForRow {
			get { return forRow; }
		}

		public bool ForTable {
			get { return forTable; }
		}

		public long TableId {
			get { return id; }
		}

		public RowId RowId {
			get { return rowid; }
		}

		public JournalCommandCode Code {
			get { return code; }
		}

		public bool IsIndexModification {
			get {
				return (code == JournalCommandCode.IndexAdd ||
				        code == JournalCommandCode.IndexDelete ||
				        code == JournalCommandCode.IndexUpdate);
			}
		}

		public bool IsAddCommand {
			get { return (((byte)code & 0x010) == 0) && (((byte)code & 0x03) == (byte)JournalCommandCode.RowAdd); }
		}

		public bool IsRemoveCommand {
			get { return (((byte) code & 0x010) == 0) && (((byte) code & 0x03) == (byte) JournalCommandCode.RowRemove); }
		}

		public bool IsUpdateCommand {
			get { return (((byte) code & 0x010) == 0) && (((byte) code & 0x03) == (byte) JournalCommandCode.RowUpdate); }
		}
	}
}