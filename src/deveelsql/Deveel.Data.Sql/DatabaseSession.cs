using System;
using System.Collections.Generic;

using Deveel.Data.Sql;

namespace Deveel.Data.Sql {
	internal class DatabaseSession {
		private readonly IDatabaseSystem sysState;
		private IDatabase db;
		private readonly object commitSyncRoot = new object();
		private long currentCommitVersion = 100;

		public DatabaseSession(IDatabase db) {
			if (db == null)
				throw new ArgumentNullException("db");

			this.db = db;
			sysState = db.System;
		}

		public DatabaseSession(IDatabaseSystem sysState, string database) {
			if (sysState == null)
				throw new ArgumentNullException("sysState");

			this.sysState = sysState;
			if (!String.IsNullOrEmpty(database))
				db = sysState.GetDatabase(database);
		}

		public IDatabase CreateDatabase(string name, string adminUser, string adminPass, bool changeInSession) {
			if (sysState == null)
				throw new InvalidOperationException("Cannot create databases in this context.");

			if (sysState.GetDatabase(name) != null)
				throw new InvalidOperationException("Database '" + name + "' already exists.");

			IDatabase state = sysState.CreateDatabase(name);

			SystemTransaction transaction = CreateTransaction(User.System.Name);

			try {
				// First of all, the INFORMATION_SCHEMA ....
				transaction.State.CreateSchema(SystemTableNames.SystemSchema);

				// Create the system directory table (table id 1),
				SystemTable tables = new SystemTable(transaction, transaction.State.CreateTable(SystemTableNames.Tables), 1);
				tables.Columns.Add("id", SqlType.Numeric, true);
				tables.Columns.Add("schema", SqlType.String, true);
				tables.Columns.Add("name", SqlType.String, true);
				tables.Columns.Add("type", SqlType.String, true);


				// Create the system index tables (table id 2),
				SystemTable indext = new SystemTable(transaction, transaction.State.CreateTable(SystemTableNames.Index), 2);
				indext.Columns.Add("id", SqlType.Numeric, true);
				indext.Columns.Add("schema", SqlType.String, true);
				indext.Columns.Add("name", SqlType.String, true);
				indext.Columns.Add("index_name", SqlType.String, true);
				indext.Columns.Add("type", SqlType.String, true);

				// Create an index over the system tables
				Add2ColumnIndex(transaction, indext, "composite_name_idx", "schema", "name");
				Add3ColumnIndex(transaction, indext, "index_composite_idx",
													  "schema", "name", "index_name");
				AddColumnIndex(transaction, indext, "id_idx", "id");
				AddColumnIndex(transaction, tables, "id_idx", "id");
				AddColumnIndex(transaction, tables, "schema_idx", "schema");
				AddColumnIndex(transaction, tables, "name_idx", "name");
				Add2ColumnIndex(transaction, tables, "composite_name_idx", "schema", "name");
				AddColumnIndex(transaction, tables, "source_idx", "source");

				// Dispose the table objects
				tables.Dispose();
				indext.Dispose();

				// Add the directory item for this table itself
				transaction.AddObject(1, SystemTableNames.Tables, "TABLE");
				transaction.AddObject(2, SystemTableNames.Index, "TABLE");
				transaction.RebuildSystemIndexes();

				// We have now configured enough for all directory operations.

				// ----- The schema table -----

				// All valid schema defined by the database,
				SystemTable schemaTable = transaction.CreateTable(SystemTableNames.Schema);
				schemaTable.Columns.Add("name", SqlType.String, true);
				AddColumnIndex(transaction, schemaTable, "name_idx", "name");

				// Add an entry for the system SYS_INFO schema.
				TableRow nrow = schemaTable.NewRow();
				nrow.SetValue(0, SystemTableNames.SystemSchema);
				nrow.Insert();

				// ----- Table constraints -----

				// A set of columns tied with a unique id used by the referential
				// integrity tables.
				SystemTable columnSetTable = transaction.CreateTable(SystemTableNames.ColumnSet);
				columnSetTable.Columns.Add("id", SqlType.Numeric, true);
				columnSetTable.Columns.Add("seq_no", SqlType.Numeric, true);
				columnSetTable.Columns.Add("column_name", SqlType.String, true);
				AddColumnIndex(transaction, columnSetTable, "id_idx", "id");

				// Unique/Primary key constraints
				SystemTable constraintsUnique = transaction.CreateTable(SystemTableNames.ConstraintsUnique);
				constraintsUnique.Columns.Add("object_id", SqlType.Numeric, true);
				constraintsUnique.Columns.Add("name", SqlType.String, false);
				constraintsUnique.Columns.Add("column_set_id", SqlType.Numeric, true);
				constraintsUnique.Columns.Add("deferred", SqlType.Boolean, false);
				constraintsUnique.Columns.Add("deferrable", SqlType.Boolean, false);
				constraintsUnique.Columns.Add("primary_key", SqlType.Boolean, false);
				AddColumnIndex(transaction, constraintsUnique, "object_id_idx", "object_id");
				AddColumnIndex(transaction, constraintsUnique, "name_idx", "name");

				// Foreign key reference constraints
				SystemTable constraintsForeign = transaction.CreateTable(SystemTableNames.ConstraintsForeign);
				constraintsForeign.Columns.Add("object_id", SqlType.Numeric, true);
				constraintsForeign.Columns.Add("name", SqlType.String, false);
				constraintsForeign.Columns.Add("column_set_id", SqlType.Numeric, true);
				constraintsForeign.Columns.Add("ref_schema", SqlType.String, true);
				constraintsForeign.Columns.Add("ref_object", SqlType.Numeric, true);
				constraintsForeign.Columns.Add("ref_column_set_id", SqlType.Numeric, true);
				constraintsForeign.Columns.Add("update_action", SqlType.String, false);
				constraintsForeign.Columns.Add("delete_action", SqlType.String, false);
				constraintsForeign.Columns.Add("deferred", SqlType.Boolean, true);
				constraintsForeign.Columns.Add("deferrable", SqlType.Boolean, false);
				AddColumnIndex(transaction, constraintsForeign, "object_id_idx", "object_id");
				AddColumnIndex(transaction, constraintsForeign, "name_idx", "name");
				Add2ColumnIndex(transaction, constraintsForeign, "composite_name_idx", "ref_schema", "ref_object");

				// Expression check constraints
				SystemTable constraintsCheck = transaction.CreateTable(SystemTableNames.ConstraintsCheck);
				constraintsCheck.Columns.Add("object_id", SqlType.Numeric, true);
				constraintsCheck.Columns.Add("name", SqlType.String, false);
				constraintsCheck.Columns.Add("check_source", SqlType.String, true);
				constraintsCheck.Columns.Add("check_bin", SqlType.Binary, true);
				constraintsCheck.Columns.Add("deferred", SqlType.Boolean, false);
				constraintsCheck.Columns.Add("deferrable", SqlType.Boolean, false);
				AddColumnIndex(transaction, constraintsCheck, "object_id_idx", "object_id");
				AddColumnIndex(transaction, constraintsCheck, "name_idx", "name");

				// Default column expressions
				SystemTable default_column_expr_ts = transaction.CreateTable(SystemTableNames.DefaultColumnExpression);
				default_column_expr_ts.Columns.Add("object_id", SqlType.Numeric, true);
				default_column_expr_ts.Columns.Add("column", SqlType.String, true);
				default_column_expr_ts.Columns.Add("default_source", SqlType.String, true);
				default_column_expr_ts.Columns.Add("default_bin", SqlType.Binary, true);
				AddColumnIndex(transaction, default_column_expr_ts, "object_id_idx", "object_id");


				// Insert referential constraints on the system tables so they cascade
				// delete.
				long src_col_set, dst_col_set;

				src_col_set = AddColumnSet(transaction, new String[] { "object_id" });
				dst_col_set = AddColumnSet(transaction, new String[] { "id" });
				AddForeignConstraint(transaction, SystemTableNames.ConstraintsUnique, src_col_set,
				                     SystemTableNames.Tables, dst_col_set);
				src_col_set = AddColumnSet(transaction, new String[] { "object_id" });
				dst_col_set = AddColumnSet(transaction, new String[] { "id" });
				AddForeignConstraint(transaction, SystemTableNames.ConstraintsForeign, src_col_set,
				                     SystemTableNames.Tables, dst_col_set);
				src_col_set = AddColumnSet(transaction, new String[] { "object_id" });
				dst_col_set = AddColumnSet(transaction, new String[] { "id" });
				AddForeignConstraint(transaction, SystemTableNames.ConstraintsCheck, src_col_set,
				                     SystemTableNames.Tables, dst_col_set);
				src_col_set = AddColumnSet(transaction, new String[] { "object_id" });
				dst_col_set = AddColumnSet(transaction, new String[] { "id" });
				AddForeignConstraint(transaction, SystemTableNames.DefaultColumnExpression, src_col_set,
				                     SystemTableNames.Tables, dst_col_set);
				src_col_set = AddColumnSet(transaction, new String[] { "id" });
				dst_col_set = AddColumnSet(transaction, new String[] { "column_set_id" });
				AddForeignConstraint(transaction, SystemTableNames.ColumnSet, src_col_set,
				                     SystemTableNames.ConstraintsForeign, dst_col_set);
				src_col_set = AddColumnSet(transaction, new String[] { "id" });
				dst_col_set = AddColumnSet(transaction, new String[] { "ref_column_set_id" });
				AddForeignConstraint(transaction, SystemTableNames.ColumnSet, src_col_set,
				                     SystemTableNames.ConstraintsForeign, dst_col_set);
				src_col_set = AddColumnSet(transaction, new String[] { "id" });
				dst_col_set = AddColumnSet(transaction, new String[] { "column_set_id" });
				AddForeignConstraint(transaction, SystemTableNames.ColumnSet, src_col_set,
				                     SystemTableNames.ConstraintsUnique, dst_col_set);

				// Rebuild the indexes on the tables we added information to.
				transaction.RebuildIndexes(SystemTableNames.ColumnSet);
				transaction.RebuildIndexes(SystemTableNames.ConstraintsForeign);



				// Add a directory item for the system columns table
				transaction.AddObject(4, SystemTableNames.TableColumns, "DYN:Deveel.Data.Sql.SystemColumnsTable");

				// The empty and single row zero column item (should this be in
				// TSTransaction?)
				transaction.AddObject(5, SystemTableNames.EmptyTable, "PRIMITIVE:EmptyTable");
				transaction.AddObject(6, SystemTableNames.OneRowTable, "PRIMITIVE:OneRowTable");

				// Commit the transaction and finish
				CommitTransaction(transaction);
			} catch (Exception e) {
				throw new Exception("Unable to create the database '" + name + "': " + e.Message, e);
			} finally {
				DisposeTransaction(transaction);
			}

			if (changeInSession)
				db = state;

			return state;
		}

		private void AddForeignConstraint(SystemTransaction transaction, TableName srcTableName, long srcColSet, TableName refTableName, long refColSet) {
			throw new NotImplementedException();
		}

		private long AddColumnSet(SystemTransaction transaction, string[] columns) {
			throw new NotImplementedException();
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

		public IDatabase CreateDatabase(string name, string adminUser, string adminPass) {
			return CreateDatabase(name, adminUser, adminPass, true);
		}

		public SystemTransaction CreateTransaction(string username) {
			User user = new User(username);
			if (user.IsSystem)
				throw new ArgumentException("System user cannot create a transaction explicitely.");

			return CreateTransaction(db, user);
		}

		private SystemTransaction CreateTransaction(IDatabase database, User user) {
			if (database == null)
				throw new InvalidOperationException("No database selected");

			// The commit version number of this transaction
			long transactionVersionNumber;
			lock (commitSyncRoot) {
				transactionVersionNumber = currentCommitVersion;
			}

			return new SystemTransaction(this, database.CreateTransaction(), transactionVersionNumber, user);
		}

		public void DisposeTransaction(SystemTransaction transaction) {
			ITransactionState transactionState = transaction.State;
			transaction.MarkAsDisposed();
			db.DisposeTransaction(transactionState);
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

					workingTransaction = new SystemTransaction(this, db.CreateTransaction(), currentCommitVersion, transaction.User.Verified());

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
					db.CommitTransaction(workingTransaction.State);

					// Increment the commit version number
					++currentCommitVersion;
				} catch (Exception e) {
					throw new CommitException("The database state failed to commit the transaction changes: " + e.Message, e);
				}
			}
		}
	}
}