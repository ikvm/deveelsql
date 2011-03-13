using System;

namespace Deveel.Data.Sql {
	public interface IFunctionEvaluationContext {
		ITableDataSource EvaluateAggregate(QueryProcessor processor, bool distinct, ITableDataSource group, Expression[] args);
		
		ITableDataSource Evaluate(QueryProcessor processor, Expression[] args);
	}
}