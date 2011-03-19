using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	internal class TransactionJournal : IEnumerable<JournalEntry> {
		private int entriesCount;
		private byte[] buffer = new byte[80];
		private int bufLength;
		private int lastEntryOffset = -1;
		private bool indexModified;

		public int Count {
			get { return entriesCount; }
		}

		public bool IndexChanged {
			get { return indexModified; }
		}

		private int IndexOfEntry(int entry) {
			if (entry == 0)
				return 0;
			if (entry == (Count - 1))
				return lastEntryOffset;

			// Create an iterator and walk through to the given entry number
			JournalEnumerator i = new JournalEnumerator(buffer, 0, lastEntryOffset);
			while (i.MoveNext() && entry > 0) {
				--entry;
			}
			// Return the position we're at
			return i.Offset;

		}

		private void EnsureSpace(int size) {
			if (bufLength + size > buffer.Length) {
				// Resize command array.
				int growSize = buffer.Length * 2;
				byte[] newBuffer = new byte[buffer.Length + growSize];
				Array.Copy(buffer, 0, newBuffer, 0, buffer.Length);
				buffer = newBuffer;
			}
		}

		public void AddEntry(JournalCommandCode code, long id) {
			AddEntry(new JournalEntry(code, id));
		}

		public void AddEntry(JournalEntry entry) {
			if (entry.ForTable && entry.TableId < 0)
				throw new ArgumentException();
			if (entry.ForRow && entry.RowId == null)
				throw new ArgumentException();

			// If index modification command then mark indexes as changed,
			if (entry.IsIndexModification)
				indexModified = true;

			if (entry.ForTable) {
				// Work out the size of this entry,
				byte[] b = new byte[8];
				for (int i = 0; i < 8; ++i) {
					b[i] = (byte) (entry.TableId >> (i*8));
				}

				int level = 0;
				for (int i = 7; i > 0; --i) {
					if (b[i] != 0) {
						level = i;
						break;
					}
				}

				// We now can predict the size,
				int size = level + 2;
				// Ensure there's enough room to store this command,
				EnsureSpace(size);
				buffer[bufLength] = (byte) entry.TableId;
				Array.Copy(b, 0, buffer, bufLength + 1, level + 1);

				lastEntryOffset = bufLength;
				bufLength += size;
			} else {
				RowId rowid = entry.RowId;
				byte[] b = rowid.ToBinary();
				int sz = b.Length;

				EnsureSpace(sz);
				Array.Copy(b, 0, buffer, bufLength, sz);

				lastEntryOffset = bufLength;
				bufLength += sz;
			}

			++entriesCount;
		}

		public IEnumerator<JournalEntry> GetEnumerator(int startEntry, int endEntry) {
			int start_off = IndexOfEntry(startEntry);
			int end_off = IndexOfEntry(endEntry);
			return new JournalEnumerator(buffer, start_off, end_off);
		}

		public IEnumerator<JournalEntry> GetEnumerator(int startEntry) {
			return GetEnumerator(startEntry, Count - 1);
		}

		public IEnumerator<JournalEntry> GetEnumerator() {
			return GetEnumerator(0);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public IEnumerator<JournalEntry> GetAddedRows() {
			// The iterator looks for RowAdd or RowUpdate commands in the
			// journal, and then scans forward through the journal to determine if the
			// row was not also removed.
			return new NormAddedEnumerator(buffer, 0, lastEntryOffset);
		}

		public IEnumerator<JournalEntry> GetRemovedRows() {
			return new CommandFilteredEnumerator(buffer, 0, lastEntryOffset,
												 delegate(JournalEntry entry) { return entry.IsRemoveCommand; });
		}

		public IEnumerator<JournalEntry> GetUpdatedRows() {
			return new CommandFilteredEnumerator(buffer, 0, lastEntryOffset,
												 delegate(JournalEntry entry) { return entry.IsUpdateCommand; });
		}

		public IEnumerator<JournalEntry> GetIndexCommands() {
			return new CommandFilteredEnumerator(buffer, 0, lastEntryOffset,
												 delegate(JournalEntry entry) { return entry.IsIndexModification; });
		}

		public IEnumerator<JournalEntry> GetIndexAddCommands() {
			return new CommandFilteredEnumerator(buffer, 0, lastEntryOffset,
												 delegate(JournalEntry entry) { return entry.Code == JournalCommandCode.IndexAdd; });
		}

		public IEnumerator<JournalEntry> GetIndexDeleteCommands() {
			return new CommandFilteredEnumerator(buffer, 0, lastEntryOffset,
												 delegate(JournalEntry entry) { return entry.Code == JournalCommandCode.IndexDelete; });
		}

		public IEnumerator<JournalEntry> GetIndexUpdateCommands() {
			return new CommandFilteredEnumerator(buffer, 0, lastEntryOffset,
												 delegate(JournalEntry entry) { return entry.Code == JournalCommandCode.IndexUpdate; });
		}

		#region JournalEnumerator

		private class JournalEnumerator : IEnumerator<JournalEntry> {
			private readonly byte[] buffer;
			private int offset;
			private readonly int startOffset;
			private readonly int endPos;
			private int identBytes;
			private int commandLength;
			private JournalEntry currentEntry;
			private bool advanced;

			public JournalEnumerator(byte[] buffer, int offset, int endPos) {
				this.buffer = (byte[])buffer.Clone();
				this.endPos = endPos;
				this.offset = startOffset = offset;
			}

			public int Offset {
				get { return offset; }
			}

			private void SetupForOffset() {
				byte c = buffer[offset];
				identBytes = ((c >> 5) & 0x07) + 1;
				commandLength = identBytes + 1;
			}

			public void Dispose() {
			}

			public bool MoveNext() {
				offset += commandLength;
				bool hasNext = offset <= endPos;
				if (hasNext) {
					// Calculate the command size,
					SetupForOffset();
				}
				advanced = hasNext;
				return hasNext;
			}

			public void Reset() {
				Reset(startOffset);
			}

			public void Reset(int toOffset) {
				offset = toOffset;
				SetupForOffset();
			}

			public JournalEntry Current {
				get {
					if (advanced) {
						JournalCommandCode code = (JournalCommandCode)(buffer[offset] & 0x01F);
						object id = GetCommandId(code);
						if (id is RowId)
							currentEntry = new JournalEntry(code, (RowId)id);
						else
							currentEntry = new JournalEntry(code, (long)id);
					}

					return currentEntry;
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			private object GetCommandId(JournalCommandCode code) {
				int sz = identBytes;

				if (code == JournalCommandCode.RowAdd ||
					code == JournalCommandCode.RowRemove ||
					code == JournalCommandCode.RowUpdate) {
					byte[] b = new byte[sz];
					Array.Copy(buffer, offset + 1, b, 0, sz);
					return new RowId(b);
				}

				
				long v = 0;
				for (int i = 0; i < sz; ++i) {
					v = v | (((long)buffer[offset + i + 1]) & 0x0FF) << (i * 8);
				}
				return v;
			}
		}

		#endregion

		#region NormAddedEnumerator

		private class NormAddedEnumerator : IEnumerator<JournalEntry> {
			private readonly JournalEnumerator scanEnumerator;

			public NormAddedEnumerator(byte[] buffer, int offset, int endOffset) {
				scanEnumerator = new JournalEnumerator(buffer, offset, endOffset);
			}

			public void Dispose() {
			}

			public bool MoveNext() {
				// Scan forward until we find an add command,
				while (scanEnumerator.MoveNext()) {
					JournalEntry entry = scanEnumerator.Current;
					if (entry.IsAddCommand) {
						// Okay, this is an add, so record the position and scan for the
						// remove command,
						RowId scanForRow = entry.RowId;

						int recOffset = scanEnumerator.Offset;
						bool isRemoved = false;

						while (scanEnumerator.MoveNext() && !isRemoved) {
							entry = scanEnumerator.Current;

							// If this is a remove command and the row identifier matches,
							if (entry.IsRemoveCommand && entry.RowId.Equals(scanForRow)) {
								// Remove command found with this identifier, so this row is
								// removed so it shouldn't be returned.
								isRemoved = true;
							}
						}

						// Reset the position
						scanEnumerator.Reset(recOffset);

						// If we have found the record wasn't removed, we return true
						// indicating this row is a normalized, added row
						if (!isRemoved) {
							return true;
						}
					}
				}
				// End reached so no next value,
				return false;
			}

			public void Reset() {
				scanEnumerator.Reset();
			}

			public JournalEntry Current {
				get { return scanEnumerator.Current; }
			}

			object IEnumerator.Current {
				get { return Current; }
			}
		}

		#endregion

		private delegate bool EnumerationFilter(JournalEntry entry);

		#region CommandFilteredEnumerator

		private class CommandFilteredEnumerator : IEnumerator<JournalEntry> {
			private readonly JournalEnumerator scanEnumerator;
			private readonly EnumerationFilter filter;

			public CommandFilteredEnumerator(byte[] buffer, int offset, int endOffset, EnumerationFilter filter) {
				scanEnumerator = new JournalEnumerator(buffer, offset, endOffset);
				this.filter = filter;
			}

			public void Dispose() {
			}

			public bool MoveNext() {
				// Scan forward until we find a remove command,
				while (scanEnumerator.MoveNext()) {
					if (filter(scanEnumerator.Current))
						return true;
				}
				// End reached so no next value,
				return false;
			}

			public void Reset() {
				scanEnumerator.Reset();
			}

			public JournalEntry Current {
				get { return scanEnumerator.Current; }
			}

			object IEnumerator.Current {
				get { return Current; }
			}
		}

		#endregion
	}
}