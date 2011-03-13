using System;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Sql.Parser;

namespace Deveel.Data.Sql {
	public sealed class SystemFunctionManager : IFunctionManager {
		private readonly Dictionary<string, List<Function>> globalFunctions;
		private readonly Dictionary<String, String> sysFunctionMap;

		internal SystemFunctionManager() {
			globalFunctions = new Dictionary<string, List<Function>>();
			sysFunctionMap = new Dictionary<string, string>();
			Initialize();
		}

		private void Initialize() {
			FunctionDiscoverContext context = new FunctionDiscoverContext(typeof (SystemFunctions));

			// Functions of SQL operators
			context.Discover("DETERMINISTIC T       add_sql   (NUMERIC T, NUMERIC T)", "Add");
			context.Discover("DETERMINISTIC T       sub_sql   (NUMERIC T, NUMERIC T)", "Subtract");
			context.Discover("DETERMINISTIC T       mul_sql   (NUMERIC T, NUMERIC T)", "Multiply");
			context.Discover("DETERMINISTIC T       div_sql   (NUMERIC T, NUMERIC T)", "Divide");

			context.Discover("DETERMINISTIC BOOLEAN and_sql   (BOOLEAN, BOOLEAN)", "And");
			context.Discover("DETERMINISTIC BOOLEAN or_sql    (BOOLEAN, BOOLEAN)", "Or");
			context.Discover("DETERMINISTIC BOOLEAN not_sql   (BOOLEAN)", "Not");

			context.Discover("DETERMINISTIC BOOLEAN gt_sql    (COMPARABLE, COMPARABLE)", "GreaterThan");
			context.Discover("DETERMINISTIC BOOLEAN lt_sql    (COMPARABLE, COMPARABLE)", "LesserThan");
			context.Discover("DETERMINISTIC BOOLEAN gte_sql   (COMPARABLE, COMPARABLE)", "GreaterOrEqualThan");
			context.Discover("DETERMINISTIC BOOLEAN lte_sql   (COMPARABLE, COMPARABLE)", "LesserOrEqualThan");
			context.Discover("DETERMINISTIC BOOLEAN eq_sql    (COMPARABLE, COMPARABLE)", "Equal");
			context.Discover("DETERMINISTIC BOOLEAN neq_sql   (COMPARABLE, COMPARABLE)", "NotEqual");
			context.Discover("DETERMINISTIC BOOLEAN is_sql    (COMPARABLE, COMPARABLE)", "Is");
			context.Discover("DETERMINISTIC BOOLEAN isn_sql   (COMPARABLE, COMPARABLE)", "IsNot");

			context.Discover("DETERMINISTIC BOOLEAN like_sql  (STRING, STRING)", "Like");
			context.Discover("DETERMINISTIC BOOLEAN nlike_sql (STRING, STRING)", "NotLike");

			// Nested query boolean functions
			context.Discover("DETERMINISTIC BOOLEAN anyeq_sql  (COMPARABLE, TABLE)", "AnyEqual");
			context.Discover("DETERMINISTIC BOOLEAN anyneq_sql (COMPARABLE, TABLE)", "AnyNotEqual");
			context.Discover("DETERMINISTIC BOOLEAN anygt_sql  (COMPARABLE, TABLE)", "AnyGreaterThan");
			context.Discover("DETERMINISTIC BOOLEAN anylt_sql  (COMPARABLE, TABLE)", "AnyLesserThan");
			context.Discover("DETERMINISTIC BOOLEAN anygte_sql (COMPARABLE, TABLE)", "AnyGreaterOrEqualThan");
			context.Discover("DETERMINISTIC BOOLEAN anylte_sql (COMPARABLE, TABLE)", "AnyLesserOrEqualThan");

			context.Discover("DETERMINISTIC BOOLEAN alleq_sql  (COMPARABLE, TABLE)", "AllEqual");
			context.Discover("DETERMINISTIC BOOLEAN allneq_sql (COMPARABLE, TABLE)", "AllNotEqual");
			context.Discover("DETERMINISTIC BOOLEAN allgt_sql  (COMPARABLE, TABLE)", "AllGreaterThan");
			context.Discover("DETERMINISTIC BOOLEAN alllt_sql  (COMPARABLE, TABLE)", "AllLesserThan");
			context.Discover("DETERMINISTIC BOOLEAN allgte_sql (COMPARABLE, TABLE)", "AllGreaterOrEqualThan");
			context.Discover("DETERMINISTIC BOOLEAN alllte_sql (COMPARABLE, TABLE)", "AllLesserOrEqualThan");

			context.Discover("DETERMINISTIC BOOLEAN exists_sql (TABLE)", "Exists");

			context.Discover("DETERMINISTIC T       abs (NUMERIC T)", "Abs");
			context.Discover("DETERMINISTIC NUMERIC sign (NUMERIC)", "Sign");
			context.Discover("DETERMINISTIC T       mod (NUMERIC T, NUMERIC)", "Modulo");
			context.Discover("DETERMINISTIC T       round (NUMERIC T, NUMERIC?)", "Round");
			context.Discover("DETERMINISTIC T       pow (NUMERIC T, NUMERIC)", "Pow");
			context.Discover("DETERMINISTIC T       sqrt (NUMERIC T)", "Sqrt");
			context.Discover("DETERMINISTIC T       least (COMPARABLE T+)", "Least");
			context.Discover("DETERMINISTIC T       greatest (COMPARABLE T+)", "Greatest");
			context.Discover("DETERMINISTIC T       lower (STRING T)", "Lower");
			context.Discover("DETERMINISTIC T       upper (STRING T)", "Upper");
			context.Discover("DETERMINISTIC T       concat (STRING T+)", "Concat");
			context.Discover("DETERMINISTIC STRING  concat (ANY+)", "Concat");
			context.Discover("DETERMINISTIC NUMERIC length (STRING)", "Length");
			context.Discover("DETERMINISTIC NUMERIC length (BINARY)", "Length");
			context.Discover("DETERMINISTIC NUMERIC char_length (STRING)", "CharLength");
			context.Discover("DETERMINISTIC NUMERIC bit_length (STRING)", "BitLength");
			context.Discover("DETERMINISTIC T       trim (STRING, STRING, STRING T)", "Trim");
			context.Discover("DETERMINISTIC T       ltrim (STRING T)", "LTrim");
			context.Discover("DETERMINISTIC T       rtrim (STRING T)", "RTrim");
			context.Discover("DETERMINISTIC T       substring (STRING T, NUMERIC, NUMERIC?)", "Substring");

			// Aggregates
			context.Discover("DETERMINISTIC NUMERIC count (*)", "Count", true);
			context.Discover("DETERMINISTIC NUMERIC count (ANY+)", "Count", true);
			context.Discover("DETERMINISTIC T       min (COMPARABLE T)", "Min", true);
			context.Discover("DETERMINISTIC T       max (COMPARABLE T)", "Max", true);
			context.Discover("DETERMINISTIC T       sum (NUMERIC T)", "Sum", true);
			context.Discover("DETERMINISTIC T       avg (NUMERIC T)", "Avg", true);
			context.Discover("DETERMINISTIC STRING  group_concat (ANY T+)", "GroupConcat", true);

			context.Flush(this);

			MapFunction("+", "@add_sql");
			MapFunction("-", "@sub_sql");
			MapFunction("*", "@mul_sql");
			MapFunction("/", "@div_sql");
			MapFunction("and", "@and_sql");
			MapFunction("or", "@or_sql");
			MapFunction("not", "@not_sql");
			MapFunction(">", "@gt_sql");
			MapFunction("<", "@lt_sql");
			MapFunction(">=", "@gte_sql");
			MapFunction("<=", "@lte_sql");
			MapFunction("=", "@eq_sql");
			MapFunction("==", "@eq_sql");
			MapFunction("<>", "@neq_sql");
			MapFunction("is", "@is_sql");
			MapFunction("isnot", "@isn_sql");

			MapFunction("any=", "@anyeq_sql");
			MapFunction("any<>", "@anyneq_sql");
			MapFunction("any>", "@anygt_sql");
			MapFunction("any<", "@anylt_sql");
			MapFunction("any>=", "@anygte_sql");
			MapFunction("any<=", "@anylte_sql");

			MapFunction("all=", "@alleq_sql");
			MapFunction("all<>", "@allneq_sql");
			MapFunction("all>", "@allgt_sql");
			MapFunction("all<", "@alllt_sql");
			MapFunction("all>=", "@allgte_sql");
			MapFunction("all<=", "@alllte_sql");

			MapFunction("exists", "@exists_sql");

			MapFunction("if", "@if_sql");
		}

		private Function PickFunction(string name, Expression[] args) {
			List<Function> funs;
			if (globalFunctions.TryGetValue(name, out  funs)) {
				if (funs.Count > 1) {
					foreach (Function sys_fun in funs) {
						if (sys_fun.MatchesParameterCount(args.Length))
							return sys_fun;
					}
				} else {
					return funs[0];
				}
			}

			return null;
		}

		public bool FunctionExists(string name) {
			return name.Equals("@cast") || globalFunctions.ContainsKey(name);
		}

		public bool IsAggregate(string name) {
			if (name.Equals("@cast"))
				return false;

			List<Function> funs;
			if (!globalFunctions.TryGetValue(name, out funs))
				return false;
				
			return funs[0].IsAggregate;
		}

		public void MapFunction(string alias, string functionName) {
			sysFunctionMap[alias] = functionName;
		}

		public string QualifyName(string name) {
			string qname;
			if (!sysFunctionMap.TryGetValue(name, out qname))
				qname = name;
			return qname;
		}

		public Function[] GetFunction(string name) {
			List<Function> funs;
			if (!globalFunctions.TryGetValue(name, out funs))
				throw new ApplicationException("Unable to resolve function: " + name);

			return funs.ToArray();
		}

		public Function AddFunction(string functionDef, Type definingType, string methodName) {
			FunctionDiscoverContext context = new FunctionDiscoverContext(definingType);
			Function f = context.Discover(functionDef, methodName);
			context.Flush(this);
			return f;
		}

		public void AddFunction(Function function) {
			string functionKey = String.Intern("@" + function.Name);

			List<Function> functions;
			if (!globalFunctions.TryGetValue(functionKey, out functions)) {
				functions = new List<Function>();
				globalFunctions[functionKey] = functions;
			}

			functions.Add(function);
		}

		public ITableDataSource Evaluate(string functionName, QueryProcessor processor, Expression[] args) {
			// 'CAST' is a special case,
			if (functionName.Equals("@cast")) {
				// Get the value to cast, and the type to cast it to,
				SqlObject val = QueryProcessor.Result(processor.Execute(args[0]))[0];
				SqlObject cast_type = QueryProcessor.Result(processor.Execute(args[1]))[0];

				SqlType type = SqlType.Parse(cast_type.ToString());

				// Do the cast operation and return the result,
				return QueryProcessor.ResultTable(val.CastTo(type));
			}

			Function sysFun = PickFunction(functionName, args);
			if (sysFun == null)
				throw new ApplicationException("Unable to resolve function " + functionName);

			return sysFun.Evaluate(processor, args);
		}

		public ITableDataSource EvaluateAggregate(string functionName, QueryProcessor processor, bool distinct, ITableDataSource group, Expression[] args) {
			Function sysFun = PickFunction(functionName, args);
			if (sysFun == null)
				throw new ApplicationException("Unable to resolve function: " + functionName);

			return sysFun.EvaluateAggregate(processor, distinct, group, args);
		}

		#region FunctionDiscoverContext

		private class FunctionDiscoverContext {
			private readonly Type declaringType;
			private readonly Dictionary<string, List<Function>> discovered;

			public FunctionDiscoverContext(Type declaringType) {
				this.declaringType = declaringType;
				discovered = new Dictionary<string, List<Function>>();
			}

			public Function Discover(string function, string methodName, bool aggregate) {
				try {
					SqlParser parser = new SqlParser(new StringReader(function));
					Function f = parser.Function();

					if (String.IsNullOrEmpty(methodName))
						methodName = f.Name;

					f.IsAggregate = aggregate;
					f.SetEvaluationContext(declaringType, methodName);

					// The function name
					string functionKey = String.Intern("@" + f.Name);
					// Does an entry already exist?
					List<Function> functionList;
					if (!discovered.TryGetValue(functionKey, out functionList)) {
						// No so create a new containing class for the list
						functionList = new List<Function>();
					}
					// Add the function to the list and put it in the map
					functionList.Add(f);
					discovered[functionKey] = functionList;
					return f;
				} catch (ParseException e) {
					throw new ApplicationException(e.Message);
				}
			}

			public Function Discover(string function, string methodName) {
				return Discover(function, methodName, false);
			}

			public void Flush(SystemFunctionManager manager) {
				foreach (KeyValuePair<string, List<Function>> systemFunction in discovered) {
					manager.globalFunctions.Add(systemFunction.Key, systemFunction.Value);
				}
			}
		}

		#endregion
	}
}