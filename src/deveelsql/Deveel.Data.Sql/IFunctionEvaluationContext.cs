using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Sql {
	public interface IFunctionEvaluationContext {
		ITable EvaluateAggregate(QueryProcessor processor, bool distinct, ITable group, Expression[] args);
		
		ITable Evaluate(QueryProcessor processor, Expression[] args);
	}
}