using System;

namespace Deveel.Data.Sql {
	internal struct JournalEntry {
		private readonly JournalCommandCode code;
		private readonly long id;

		public JournalEntry(JournalCommandCode code, long id) : this() {
			this.code = code;
			this.id = id;
		}

		public long Id {
			get { return id; }
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