using System;
using System.Collections.Generic;

using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	internal class DatabaseSession {
		private readonly ISystemState sysState;
		private IDatabaseState dbState;
		private readonly object commitSyncRoot = new object();
		private long currentCommitVersion = 100;

		public DatabaseSession(IDatabaseState dbState) {
			if (dbState == null)
				throw new ArgumentNullException("dbState");

			this.dbState = dbState;
			sysState = dbState.System;
		}

		public DatabaseSession(ISystemState sysState, string database) {
			if (sysState == null)
				throw new ArgumentNullException("sysState");

			this.sysState = sysState;
			if (!String.IsNullOrEmpty(database))
				dbState = sysState.GetDatabase(database);
		}

		public static void AddForeignConstraint(SystemTransaction transaction, TableName sourceTable, string[] srcColSet, TableName destTable, string[] dstColSet) {
			string anonName = "#ANON:" + Guid.NewGuid().ToString("D");

			// The constraint table
			SystemTable fcTable = transaction.GetTable(SystemTableNames.ColumnSet);
			// Insert the data,
			TableRow rowid = fcTable.NewRow();
			rowid.SetValue(0, null);				// catalog
			rowid.SetValue(1, sourceTable.Schema);	// table_schema
			rowid.SetValue(2, sourceTable.Name);	// table_name
			rowid.SetValue(3, anonName);			// name
			rowid.SetValue(4, destTable.Schema);	// ref_table_schema
			rowid.SetValue(5, destTable.Name);		// ref_table_name
			rowid.SetValue(6, "FOREIGN_KEY");		// type
			rowid.SetValue(7, null);				// check_source
			rowid.SetValue(8, null);				// check_expression
			rowid.SetValue(6, "NO ACTION");			// update_action
			rowid.SetValue(7, "NO ACTION");			// delete_action
			rowid.SetValue(8, false);				// deferred
			rowid.SetValue(9, true);				// deferrable
			fcTable.Insert(rowid);
			fcTable.Commit();

			SystemTable fccTable = transaction.GetTable(SystemTableNames.ColumnSet);
			for (int i = 0; i < srcColSet.Length; i++) {
				TableRow row = fccTable.NewRow();
				row.SetValue(0, null);		// catalog
				row.SetValue(1, sourceTable.Schema);
				row.SetValue(2, sourceTable.Name);
				row.SetValue(3, anonName);
				row.SetValue(4, i);
				row.SetValue(5, srcColSet[i]);
				row.SetValue(6, "SOURCE");
				row.Insert();
			}
			for (int i = 0; i < dstColSet.Length; i++) {
				TableRow row = fccTable.NewRow();
				row.SetValue(0, null);		// catalog
				row.SetValue(1, sourceTable.Schema);
				row.SetValue(2, sourceTable.Name);
				row.SetValue(3, anonName);
				row.SetValue(4, i);
				row.SetValue(5, srcColSet[i]);
				row.SetValue(6, "REFERENCE");
				row.Insert();
			}
			fccTable.Commit();
		}

		public IDatabaseState CreateDatabase(string name, string adminUser, string adminPass, bool changeInSession) {
			if (sysState == null)
				throw new InvalidOperationException("Cannot create databases in this context.");

			if (sysState.GetDatabase(name) != null)
				throw new InvalidOperationException("Database '" + name + "' already exists.");

			IDatabaseState state = sysState.CreateDatabase(name);

			SystemTransaction transaction = CreateTransaction(User.System.Name);

			try {
				// First of all, the INFORMATION_SCHEMA ....
				transaction.State.CreateSchema(SystemTableNames.SystemSchema);

				//TODO:

				// Commit the transaction and finish
				CommitTransaction(transaction);
			} catch (Exception e) {
				throw new Exception("Unable to create the database '" + name + "': " + e.Message, e);
			} finally {
				DisposeTransaction(transaction);
			}

			if (changeInSession)
				dbState = state;

			return state;
		}

		private void Add3ColumnIndex(SystemTransaction transaction, SystemTable table, string indexName, string column1, string column2, string column3) {
			throw new NotImplementedException();
		}

		private void Add2ColumnIndex(SystemTransaction transaction, SystemTable seqTable, string indexName, string column1, string column2) {
			throw new NotImplementedException();
		}

		private void AddColumnIndex(SystemTransaction transaction, SystemTable schemaTable, string indexName, string column) {
			throw new NotImplementedException();
		}

		public IDatabaseState CreateDatabase(string name, string adminUser, string adminPass) {
			return CreateDatabase(name, adminUser, adminPass, true);
		}

		public SystemTransaction CreateTransaction(string username) {
			User user = new User(username);
			if (user.IsSystem)
				throw new ArgumentException("System user cannot create a transaction explicitely.");

			return CreateTransaction(dbState, user);
		}

		private SystemTransaction CreateTransaction(IDatabaseState databaseState, User user) {
			if (databaseState == null)
				throw new InvalidOperationException("No database selected");

			// The commit version number of this transaction
			long transactionVersionNumber;
			lock (commitSyncRoot) {
				transactionVersionNumber = currentCommitVersion;
			}

			return new SystemTransaction(this, databaseState.CreateTransaction(), transactionVersionNumber, user);
		}

		public void DisposeTransaction(SystemTransaction transaction) {
			ITransactionState transactionState = transaction.State;
			transaction.MarkAsDisposed();
			dbState.DisposeTransaction(transactionState);
		}

		public void CommitTransaction(SystemTransaction transaction) {
			// If the transaction has had no changes, we return immediately
			if (!transaction.HasChanges)
				return;

			// The transaction we are working on through this commit process,
			SystemTransaction workingTransaction;

			// If the given transaction is of the same commit version as the current,
			// then the working transaction is the given transaction
			lock (commitSyncRoot) {
				// The list table ids we updated
				List<long> modifiedTables = new List<long>();

				// If the given transaction is of the same commit version, then the
				// working version is the given.  If there hasn't been a commit since
				// the given transaction was created then no transaction merge is
				// necessary.
				if (transaction.CommitVersion == currentCommitVersion) {
					workingTransaction = transaction;

					// We make sure we update the 'modified_tables' list with the set of
					// unique tables that were changed
					List<long> alteredTableIds = transaction.AlteredTables;
					alteredTableIds.Sort();
					int sz = alteredTableIds.Count;
					long lastTableId = -1;
					// This goes through the altered table list provided by the
					// transaction and creates a unique set of tables that were
					// altered.
					for (int i = 0; i < sz; ++i) {
						long table_id = alteredTableIds[i];
						if (table_id != lastTableId) {
							modifiedTables.Add(table_id);
							lastTableId = table_id;
						}
					}

				}
					// Otherwise we must create a new working transaction
				else {

					// -----------------
					// Transaction MERGE
					// -----------------
					// This condition occurs when a transaction has been committed after
					// the given transaction was created, and we need to move any updates
					// from the committing transaction to the latest update.

					workingTransaction = new SystemTransaction(this, dbState.CreateTransaction(), currentCommitVersion, transaction.User.Verified());

					// SystemTable merges,

					// First of all replay the general database updates as described in
					// the journal (table and index creation/drops etc).
					TransactionJournal tranJournal = transaction.Journal;
					IEnumerator<JournalEntry> i = tranJournal.GetEnumerator();

					// The list of tables created in this transaction
					List<long> tablesCreated = new List<long>();

					while (i.MoveNext()) {
						JournalEntry entry = i.Current;
						JournalCommandCode command = entry.Code;
						long ident = entry.TableId;
						// Table commands,
						if (command == JournalCommandCode.TableCreate) {
							// Copy the created table from the original transaction to the
							// working transaction.

							// We check the table exists in the transaction.  If it doesn't
							// it means the table was dropped.
							if (transaction.TableExists(ident)) {
								// Get the name of the object
								TableName tname = transaction.GetObjectName(ident);
								if (workingTransaction.TableExists(tname)) {
									// Create fails, the table name exists in the current
									// transaction
									throw new CommitException("CREATE TABLE '" + tname + "' failed: table with name exists in " +
									                          "current transaction");
								}
								// Otherwise copy the table
								workingTransaction.CopyTableFrom(transaction, ident);
								tablesCreated.Add(ident);
							}
						}
							// When the table is structurally changed (columns, added/removed,
							// etc)
						else if (command == JournalCommandCode.TableAlter) {
							long tableId = ident;
							// If this table was created by this transaction, then we don't
							// need to worry, we have the most recent version of the table
							// structure.

							// Otherwise, the table was altered during the transaction, so
							// we need to copy the most recent version if it hasn't changed.
							if (!tablesCreated.Contains(tableId)) {
								// Check it exists in the current
								// If it doesn't it means it was dropped
								if (transaction.TableExists(tableId)) {
									// Check the table exists
									if (!workingTransaction.TableExists(tableId)) {
										throw new CommitException("ALTER TABLE '" + transaction.GetObjectName(tableId) +
										                          "' failed: table does not exist in the " + "current transaction");
									}

									// Check the version of the table we are dropping is the same
									// version and exists.
									long ver1 = transaction.GetTableVersion(tableId);
									long ver2 = workingTransaction.GetTableVersion(tableId);
									// If the versions are different, some modification has happened
									// to the table so we generate an error
									if (ver1 != ver2)
										throw new CommitException("ALTER TABLE '" + transaction.GetObjectName(tableId) +
										                          "' failed: Table was modified by a concurrent transaction");
									
									// Okay, we can now copy the table.  We drop the existing table
									// and copy the new one over.
									workingTransaction.DropTable(tableId);
									workingTransaction.CopyTableFrom(transaction, tableId);
								}
							}

						} else if (command == JournalCommandCode.TableDrop) {
							long table_id = ident;
							// Check the table exists
							if (!workingTransaction.TableExists(table_id)) {
								throw new CommitException("DROP TABLE '" + transaction.GetObjectName(table_id) +
								                          "' failed: Table does not exist in the current transaction");
							}
							// Check the version of the table we are dropping is the same
							// version and exists.
							long ver1 = transaction.GetTableVersion(table_id);
							long ver2 = workingTransaction.GetTableVersion(table_id);
							// If the versions are different, some modification has happened
							// to the table so we generate an error
							if (ver1 != ver2) {
								throw new CommitException("DROP TABLE '" + transaction.GetObjectName(table_id) +
								                          "' failed: Table was modified by a concurrent transaction");
							}
							// Drop the table
							workingTransaction.DropTable(table_id);
						}

							// Index commands,
						else if (command == JournalCommandCode.IndexAdd) {
							long indexId = ident;
							// The name of the table of the index
							TableName indexTname = transaction.GetIndexName(indexId).TableName;
							// If the above returns null, it means the index no longer
							// exists so we skip
							if (indexTname != null) {
								// If the table doesn't exist in the working transaction,
								if (!workingTransaction.TableExists(indexTname))
									throw new CommitException("CREATE INDEX on '" + indexTname +
									                          "' failed: table does not exist in the current transaction");

								// Get the table id
								long tableId = workingTransaction.GetTableId(indexTname);

								// NOTE: This check ensures the index we copy from the transaction
								//   we are committing is correct and up to date.  We should,
								//   perhaps, rewrite this to rebuild the index if there were
								//   concurrent modifications to the table.
								// If the versions of the table we are creating the index on is
								// different then we fail,
								long ver1 = transaction.GetTableVersion(tableId);
								long ver2 = workingTransaction.GetTableVersion(tableId);
								// If the versions are different, some modification has happened
								// to the table so we generate an error
								if (ver1 != ver2) {
									throw new CommitException("CREATE INDEX on '" + indexTname + "' failed: Table was modified by " +
									                          "a concurrent transaction");
								}

								// Copy the created index,
								workingTransaction.CopyIndexFrom(transaction, ident);
							}
						} else if (command == JournalCommandCode.IndexDelete) {
							long indexId = ident;
							// Drop the index.  This fails if the index doesn't exist in the
							// current version.  Note that this will succeed if there were
							// modifications to the table by a concurrent transaction.
							workingTransaction.DropIndex(indexId);
						}
							// Otherwise unrecognized
						else {
							throw new ApplicationException("Unknown journal entry: " + command);
						}
					}

					// Now replay the table level operations,

					// The list of all tables changed during the lifespan of the
					// transaction,
					List<long> alteredTableIds = transaction.AlteredTables;
					alteredTableIds.Sort();

					long lastTableId = -1;
					int sz = alteredTableIds.Count;
					// For each table id that was altered,
					for (int n = 0; n < sz; ++n) {
						long tableId = alteredTableIds[n];
						if (tableId != lastTableId) {
							// The SystemTable object
							SystemTable currentTable = workingTransaction.GetTable(tableId);
							// If the table no longer exists we ignore the journal and go to
							// the next table.  We assume the table drop has not broken
							// database integrity if we have reached this part.
							if (currentTable != null) {
								// Add this to the list of tables we updated
								modifiedTables.Add(tableId);

								// The table name
								TableName tableName = currentTable.Name;
								// The indexes on this table
								SystemIndexSetDataSource[] indexes = workingTransaction.GetTableIndexes(tableName);

								// The table from which we are merging in entries,
								SystemTable toMerge = transaction.GetTable(tableId);
								// Get the journal for this table id,
								TransactionJournal journal = toMerge.Journal;
								// Replay the operations in the journal,
								IEnumerator<JournalEntry> ti = journal.GetEnumerator();
								while (ti.MoveNext()) {
									JournalEntry entry = ti.Current;
									JournalCommandCode command = entry.Code;
									RowId ident = entry.RowId;
									if (command == JournalCommandCode.RowAdd) {
										currentTable.CopyRowIdFrom(toMerge, ident);
										// Update the indexes
										foreach (IIndexSetDataSource idx in indexes) {
											idx.Insert(ident);
										}
									} else if (command == JournalCommandCode.RowRemove) {
										// This will commit exception if the row no longer exists.
										currentTable.RemoveRowId(ident);
										// Update the indexes
										foreach (IIndexSetDataSource idx in indexes) {
											idx.Remove(ident);
										}
									} else if (command == JournalCommandCode.ColumnAdd) {
										// Can be ignored, this is handled by a structural change
										// entry in the transaction log, and copying the table over
										// from the lower version transaction.
									} else if (command == JournalCommandCode.ColumnRemove) {
										// Can be ignored, this is handled by a structural change
										// entry in the transaction log, and copying the table over
										// from the lower version transaction.
									}
								}
							}

							lastTableId = tableId;
						}
					}

					// We have now successfully replayed all the operations on the working
					// transaction.  The following artifacts may now be in the database;
					//   1) Unique constraints may be violated
					//   2) Referential integrity may be violated
					//   3) Check constraints need to be checked again if they contain
					//        nested query functions.

				}


				// Go through the list of updated tables and increment the commit
				// version on each of them
				foreach (long modifiedTableId in modifiedTables) {
					workingTransaction.IncrementTableVersion(modifiedTableId);
				}

				// Check any deferred constraints on the working_transaction


				// TODO: all the table/index merge functionality here...

				try {
					// And commit the working transaction
					dbState.CommitTransaction(workingTransaction.State);

					// Increment the commit version number
					++currentCommitVersion;
				} catch (Exception e) {
					throw new CommitException("The database state failed to commit the transaction changes: " + e.Message, e);
				}
			}
		}
	}
}