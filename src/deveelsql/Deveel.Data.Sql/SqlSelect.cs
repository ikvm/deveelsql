using System;
using System.IO;
using System.Text;

using Deveel.Data.Sql.Parser;

namespace Deveel.Data.Sql {
	public sealed class SqlSelect {
		private readonly Query query;

		public SqlSelect(Query query) {
			this.query = query;
		}

		public string Explain(SystemTransaction transaction) {
			SqlParser sql = new SqlParser(new StringReader(query.Text));
			Expression expression;
			try {
				expression = sql.Statement();
			} catch (ParseException e) {
				throw new SqlParseException(e.Message, e);
			}

			QueryOptimizer optimizer = new QueryOptimizer(transaction);
			expression = optimizer.SubstituteParameters(expression, query);
			expression = optimizer.Qualify(expression);
			expression = optimizer.Optimize(expression);

			StringBuilder b = new StringBuilder();
			b.Append("1. Cost Model\n\n");
			b.Append(expression.GetCostModel());
			b.Append("\n\n");
			b.Append("2. Query Function\n\n");
			b.Append(expression.Explain());

			return b.ToString();
		}


		public ITableDataSource Execute(SystemTransaction transaction) {
			SqlParser sql = new SqlParser(new StringReader(query.Text));
			Expression expression;
			try {
				expression = sql.Statement();
			} catch (ParseException e) {
				throw new SqlParseException(e.Message, e);
			}

			QueryOptimizer optimizer = new QueryOptimizer(transaction);
			expression = optimizer.SubstituteParameters(expression, query);
			expression = optimizer.Qualify(expression);
			expression = optimizer.Optimize(expression);

			QueryProcessor processor = new QueryProcessor(transaction);
			return processor.Execute(expression);
		}
	}
}