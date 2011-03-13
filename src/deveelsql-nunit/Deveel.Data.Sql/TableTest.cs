using System;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public sealed class TableTest {
		[SetUp]
		public void SetUp() {
		}

		[Test]
		public void Create() {
			MockTransactionContext context = new MockTransactionContext();
			context.CreateSchema("test");
			ITable table = context.CreateTable(new TableName("test.test_table"));
			table.TableSchema.AddColumn("id", SqlType.Numeric);
			table.TableSchema.AddColumn("first_name", SqlType.String);
			table.TableSchema.AddColumn("last_name", SqlType.String);
		}

		[Test]
		public void Insert() {
			MockTransactionContext context = new MockTransactionContext();
			context.CreateSchema("test");
			ITable table = context.CreateTable(new TableName("test.test_table"));
			table.TableSchema.AddColumn("id", SqlType.Numeric);
			table.TableSchema.AddColumn("first_name", SqlType.String);
			table.TableSchema.AddColumn("last_name", SqlType.String);

			TableRow row = table.NewRow();
			row.SetValue("id", 1);
			row.SetValue("first_name", "Antonello");
			row.SetValue("last_name", "Provenzano");
			table.Insert(row);
		}
	}
}