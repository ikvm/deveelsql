using System;
using System.Reflection;

namespace Deveel.Data.Sql {
	class ReflectionFunctionEvaluationContext : IFunctionEvaluationContext {
		private readonly Function function;
		private readonly int invokeType;
		private readonly MethodInfo method;

		internal ReflectionFunctionEvaluationContext(Function function, MethodInfo method, int invokeType) {
			this.function = function;
			this.invokeType = invokeType;
			this.method = method;
		}

		public ITableDataSource EvaluateAggregate(QueryProcessor processor, bool distinct, ITableDataSource group, Expression[] args) {
			if (!function.IsAggregate)
				throw new InvalidOperationException("The function is not an aggregate.");

			try {
				// Execute it
				object[] funArgs;
				if (invokeType == 6) {
					funArgs = new object[] { function.Name, processor, distinct, group, args };
				}
					// The QueryProcessor, Expression[] construct
				else if (invokeType == 1) {
					funArgs = new object[] { processor, distinct, group, args };
				} else {
					throw new ApplicationException("Unknown invoke type");
				}

				return (ITableDataSource)method.Invoke(null, funArgs);
			} catch (MethodAccessException e) {
				throw new ApplicationException(e.Message, e);
			} catch (TargetInvocationException e) {
				throw new ApplicationException(e.InnerException.Message, e.InnerException);
			}
		}

		public ITableDataSource Evaluate(QueryProcessor processor, Expression[] args) {
			// 'CAST' is a special case,
			if (function.Name.Equals("@cast")) {
				// Get the value to cast, and the type to cast it to,
				SqlObject val = QueryProcessor.Result(processor.Execute(args[0]))[0];
				SqlObject castType = QueryProcessor.Result(processor.Execute(args[1]))[0];

				string castTypeString = castType.Value.ToString();
				SqlType type = SqlType.Parse(castTypeString);

				// Do the cast,
				SqlObject result = val.CastTo(type);

				// And return the result,
				return QueryProcessor.ResultTable(result);
			}

			if (function.IsAggregate)
				throw new InvalidOperationException("The function is aggregate.");

			try {
				// Execute it
				if (invokeType == 6) {
					object[] funArgs = { function.Name, processor, args };
					return (ITableDataSource)method.Invoke(null, funArgs);
				}
					// The QueryProcessor, Expression[] construct
				if (invokeType == 1) {
					object[] funArgs = { processor, args };
					return (ITableDataSource)method.Invoke(null, funArgs);
				}
					// The SqlObject construct
				if (invokeType == 2) {
					int sz = args.Length;
					// Resolve the arguments into TypedValues
					SqlObject[] obs = new SqlObject[sz];
					for (int i = 0; i < sz; ++i) {
						obs[i] = QueryProcessor.Result(processor.Execute(args[i]))[0];
					}
					// Set up the arguments and invoke the method
					object[] funArgs = { obs };
					SqlObject result = (SqlObject)method.Invoke(null, funArgs);
					// Wrap on a FunctionTable and return
					return QueryProcessor.ResultTable(result);
				}
					
				throw new ApplicationException("Unknown invoke type");
			} catch (MethodAccessException e) {
				throw new ApplicationException(e.Message, e);
			} catch (TargetInvocationException e) {
				throw new ApplicationException(e.InnerException.Message, e.InnerException);
			}
		}
	}
}