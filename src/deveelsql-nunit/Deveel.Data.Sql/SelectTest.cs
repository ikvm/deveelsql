using System;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public sealed class SelectTest {
		private SystemTransaction transaction;

		[SetUp]
		public void SetUp() {
			MockTransactionContext context = new MockTransactionContext();
			context.CreateSchema("test");
			ITable table = context.CreateTable(new TableName("test", "names"));
			table.TableSchema.AddColumn("id", SqlType.Numeric);
			table.TableSchema.AddColumn("first_name", SqlType.String);
			table.TableSchema.AddColumn("last_name", SqlType.String);

			TableRow row = table.NewRow();
			row.SetValue("id", 1);
			row.SetValue("first_name", "Antonello");
			row.SetValue("last_name", "Provenzano");
			table.Insert(row);

			table = context.CreateTable(new TableName("test", "books"));
			table.TableSchema.AddColumn("id", SqlType.Numeric);
			table.TableSchema.AddColumn("title", SqlType.String);
			table.TableSchema.AddColumn("pages", SqlType.Numeric);
			table.TableSchema.AddColumn("added", SqlType.DateTime);

			row = table.NewRow();
			row.SetValue("id", 1);
			row.SetValue("title", "The Lord of The Rings");
			row.SetValue("pages", 712);
			row.SetValue("added", "20/12/1998");
			table.Insert(row);

			table = context.CreateTable(new TableName("test", "book_read"));
			table.TableSchema.AddColumn("id", SqlType.Numeric);
			table.TableSchema.AddColumn("name_id", SqlType.Numeric);
			table.TableSchema.AddColumn("book_id", SqlType.Numeric);
			table.TableSchema.AddColumn("read_date", SqlType.DateTime);

			row = table.NewRow();
			row.SetValue("id", 1);
			row.SetValue("name_id", 1);
			row.SetValue("book_id", 1);
			row.SetValue("read_date", new DateTime(2000, 12, 4));
			table.Insert(row);

			transaction = new SystemTransaction(context, new HeapSystemState());
			transaction.ChangeSchema("test");
		}

		[Test]
		public void SimpleSelect() {
			Query query = new Query("SELECT first_name, last_name FROM names WHERE id = 1");
			SqlSelect sqlSelect = new SqlSelect(query);
			ITableDataSource result = sqlSelect.Execute(transaction);

			Assert.AreEqual(1, result.RowCount);

			IRowCursor cursor = result.GetRowCursor();
			while (cursor.MoveNext()) {
				string firstName = result.GetValue(0, cursor.Current);
				string lastName = result.GetValue(1, cursor.Current);

				Assert.AreEqual("Antonello", firstName);
				Assert.AreEqual("Provenzano", lastName);
			}
		}

		[Test]
		public void InnerJoinSelect() {
			Query query = new Query("SELECT b.title, n.first_name, br.read_date FROM book_read br, names n, books b WHERE br.book_id = b.id AND br.name_id = n.id;");
			SqlSelect sqlSelect = new SqlSelect(query);
			ITableDataSource result = sqlSelect.Execute(transaction);

			Assert.AreEqual(1, result.RowCount);

			IRowCursor cursor = result.GetRowCursor();
			while (cursor.MoveNext()) {
				string bookTitle = result.GetValue(0, cursor.Current);
				string personName = result.GetValue(1, cursor.Current);
				DateTime date = result.GetValue(2, cursor.Current);

				Console.Out.WriteLine("{0} read the book {1} the day {2}", personName, bookTitle, date);
			}
		}
	}
}