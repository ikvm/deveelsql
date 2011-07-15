using System;
using System.Data;

using Deveel.Data.Sql.Client;
using Deveel.Data.Sql;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public sealed class SelectTest {
		private EmbeddedSessionContext sessionContext;
		private IDbConnection connection;

		[TestFixtureSetUp]
		public void TestSetUp() {
			HeapDatabase testdb = new HeapDatabase(null, "testdb");
			ITransactionState transaction = testdb.CreateTransaction();
			transaction.CreateSchema("test");

			IMutableTable table = (IMutableTable) transaction.CreateTable(new TableName("test", "names"));
			table.Columns.Add("id", SqlType.Numeric, true);
			table.Columns.Add("first_name", SqlType.String, true);
			table.Columns.Add("last_name", SqlType.String, true);

			TableRow row = table.NewRow();
			row.SetValue("id", 1);
			row.SetValue("first_name", "Antonello");
			row.SetValue("last_name", "Provenzano");
			row.Insert();
			table.Commit();

			table = (IMutableTable) transaction.CreateTable(new TableName("test", "books"));
			table.Columns.Add("id", SqlType.Numeric, true);
			table.Columns.Add("title", SqlType.String, true);
			table.Columns.Add("author", SqlType.String, false);

			row = table.NewRow();
			row["id"] = 1;
			row["title"] = "The Lord Of The Rings";
			row["author"] = "J. R. R. Tolkien";
			row.Insert();

			row = table.NewRow();
			row["id"] = 2;
			row["title"] = "Buddenbrooks";
			row["author"] = "Thomas Mann";
			row.Insert();

			table.Commit();

			table = (IMutableTable) transaction.CreateTable(new TableName("test", "book_read"));
			table.Columns.Add("id", SqlType.Numeric, true);
			table.Columns.Add("book_id", SqlType.Numeric, true);
			table.Columns.Add("name_id", SqlType.Numeric, true);
			table.Columns.Add("read_date", SqlType.DateTime, true);

			row = table.NewRow();
			row["id"] = 1;
			row["book_id"] = 1;
			row["name_id"] = 1;
			row["read_date"] = "2001-12-22";
			row.Insert();

			row = table.NewRow();
			row["id"] = 2;
			row["book_id"] = 2;
			row["name_id"] = 1;
			row["read_date"] = "2009-05-10";
			row.Insert();

			table.Commit();

			testdb.CommitTransaction(transaction);

			sessionContext = new EmbeddedSessionContext(testdb, true, "antonello");
			connection = sessionContext.CreateConnection();
			connection.Open();

			try {
				IDbCommand command = connection.CreateCommand();
				command.CommandText = "SET SCHEMA test;";
				command.ExecuteNonQuery();
			} catch (Exception e) {
				Console.Error.WriteLine("Error: {0}", e.Message);
				Console.Error.WriteLine(e.StackTrace);
			}
		}

		[TearDown]
		public void TestTearDown() {
			connection.Close();
		}

		[Test]
		public void SimpleSelect() {
			IDbCommand command = connection.CreateCommand();
			command.CommandText = "SELECT first_name, last_name FROM names WHERE id = ?";
			command.Parameters.Add(1);
			IDataReader reader = command.ExecuteReader();

			Assert.IsTrue(reader.Read());
			Assert.AreEqual("Antonello", reader.GetString(0));
			Assert.AreEqual("Provenzano", reader.GetString(1));
		}

		[Test]
		public void InnerJoinSelect() {
			IDbCommand command = connection.CreateCommand();
			command.CommandText = "SELECT b.title, n.first_name, br.read_date " +
			                      "FROM book_read br, names n, books b " +
			                      "WHERE br.book_id = b.id AND br.name_id = n.id;";
			IDataReader reader = command.ExecuteReader();

			while (reader.Read()) {
				Console.Out.WriteLine("{0} has read {1} on {2}", reader.GetString(1), reader.GetString(0), reader.GetDateTime(2));
			}
		}

		[Test]
		public void GroupBySelect() {
			IDbCommand command = connection.CreateCommand();
			command.CommandText = "SELECT n.id, n.first_name, max(br.read_date) " +
			                      "FROM names n, books b, book_read br " +
			                      "WHERE n.id = br.name_id and b.id = br.book_id " +
			                      "GROUP by n.id, n.last_name ORDER BY upper(n.last_name);";
			IDataReader reader = command.ExecuteReader();

			while (reader.Read()) {
				
			}
		}
	}
}