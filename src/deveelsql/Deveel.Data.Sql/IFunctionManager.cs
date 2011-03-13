using System;

namespace Deveel.Data.Sql {
	public interface IFunctionManager {
		bool FunctionExists(string name);

		bool IsAggregate(string name);
		
		string QualifyName(string name);

		Function[] GetFunction(string name);

		Function AddFunction(string functionDef, Type definingType, string methodName);

		void AddFunction(Function function);
		
		ITableDataSource Evaluate(string functionName, QueryProcessor processor, Expression[] args);

		ITableDataSource EvaluateAggregate(string functionName, QueryProcessor processor, bool distinct,
		                                   ITableDataSource group, Expression[] args);

	}
}