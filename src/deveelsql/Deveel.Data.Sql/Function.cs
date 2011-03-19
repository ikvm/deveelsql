using System;
using System.Collections.Generic;
using System.Reflection;

using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	public sealed class Function : ILineInfo {
		private string name;
		private bool aggregate;
		private readonly List<FunctionParameter> parameters;
		private readonly FunctionReturn returnType;
		private FunctionState state;
		private IFunctionEvaluationContext evalContext;
		
		private int line = -1;
		private int column = -1;
		
		public Function(string name, bool aggregate)
			: this() {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");
			
			this.name = String.Intern(name);
			this.aggregate = aggregate;
		}

		public Function(string name)
			: this(name, false) {
		}
		
		internal Function() {
			parameters = new List<FunctionParameter>();
			returnType = new FunctionReturn(this);
		}
		
		public string Name {
			get { return name; }
			internal set { name = String.Intern(value); }
		}
		
		public FunctionState State {
			get { return state; }
			set { state = value; }
		}
		
		public IList<FunctionParameter> Parameters {
			get { return parameters; }
		}
		
		public FunctionReturn Return {
			get { return returnType; }
		}
		
		int ILineInfo.Column {
			get { return line;}
			set { line = value; }
		}
		
		int ILineInfo.Line {
			get { return column; }
			set { column = value; }
		}

		internal int Column {
			get { return column; }
			set { column = value; }
		}

		internal int Line {
			get { return line; }
			set { line = value; }
		}

		public bool IsAggregate {
			get { return aggregate; }
			internal set { aggregate = value; }
		}

		public bool MatchesParameterCount(int paramCount) {
			int sz = parameters.Count;
			if (sz == 1 && parameters[0].IsStar)  // Special case for star aggregates
				return paramCount == 1;

			int minCount = 0;
			int maxCount = 0;
			for (int i = 0; i < sz; i++) {
				FunctionParameterMatch t = parameters[i].Match;
				if (t == FunctionParameterMatch.Exact) {
					++minCount;
					if (maxCount < Int32.MaxValue) {
						++maxCount;
					}
				} else if (t == FunctionParameterMatch.OneOrMore) {
					++minCount;
					maxCount = Int32.MaxValue;
				} else if (t == FunctionParameterMatch.ZeroOrMore) {
					maxCount = Int32.MaxValue;
				} else if (t == FunctionParameterMatch.ZeroOrOne) {
					if (maxCount < Int32.MaxValue) {
						++maxCount;
					}
				}
			}
			// If param count is within the bounds,
			return (paramCount >= minCount && paramCount <= maxCount);
		}
		
		public Expression[] MatchParameterExpressions(Expression[] expressions, string reference) {
			// Note that the return reference must be ancored from single elements at
			// the start or end, or must represent the entire param list.
			List<Expression> returnExps = new List<Expression>(expressions.Length);
			// If single element,
			int sz = parameters.Count;
			if (sz == 1) {
				FunctionParameter param = parameters[0];
				string elem_ref = param.Reference;
				// Return all the expressions if refs match
				if (elem_ref.Equals(reference)) {
					foreach (Expression op in expressions) {
						returnExps.Add(op);
					}
				}
			} else {
				// Reference must anchor from the start and/or the end
				// Scan forward all the single elements, if they match the reference we add the
				// expression.
				int i = 0;
				for (; i < sz; i++) {
					FunctionParameter param = parameters[i];
					string elem_ref = param.Reference;
					FunctionParameterMatch t = param.Match;
					if (t != FunctionParameterMatch.Exact) {
						break;
					}
					if (elem_ref.Equals(reference)) {
						returnExps.Add(expressions[i]);
					}
				}
				// If we didn't reach the end on the previous scan, do the same but
				// backwards from the end.
				if (i < sz) {
					i = sz - 1;
					for (; i >= 0; i--) {
						FunctionParameter param = parameters[i];
						string elem_ref = param.Reference;
						FunctionParameterMatch t = param.Match;
						if (t != FunctionParameterMatch.Exact) {
							break;
						}
						if (elem_ref.Equals(reference)) {
							returnExps.Add(expressions[i]);
						}
					}
				}
			}

			return returnExps.ToArray();
		}

		private static readonly Type[] AggregateMethodParams = {
		                                                    	typeof(QueryProcessor), 
																typeof(bool), 
																typeof(ITable),
		                                                    	typeof(Expression[])
		                                                    };

		private static readonly Type[] AggregateStandardParams = {
		                                                      	typeof(string), 
																typeof(QueryProcessor), 
																typeof(bool),
		                                                      	typeof(ITable),
		                                                      	typeof(Expression[])
		                                                      };

		private readonly Type[] FunctionMethodParams = {typeof(QueryProcessor), typeof(Expression[])};
		private readonly Type[] FunctionMethodParams2 = {typeof(SqlObject[])};
		private readonly Type[] FunctionStandardParams = {typeof(string), typeof(QueryProcessor), typeof(Expression[])};

		public void SetEvaluationContext(Type declaringType, string methodName) {
			List<MethodInfo> methods = new List<MethodInfo>(2);
			int invokeType = 0;

			if (aggregate) {
				// Get the spec if it's there,
				MethodInfo m = declaringType.GetMethod(methodName, AggregateMethodParams);
				if (m != null) {
					if (!m.ReturnType.IsAssignableFrom(typeof(ITable))) {
						throw new ApplicationException("Method " + methodName + " needs return type of ITable");
					}
					methods.Add(m);
					invokeType = 1;
				}

				// If we didn't find the method, we go to the standard method declaration
				if (methods.Count == 0) {
					m = declaringType.GetMethod("AggregateStandard", AggregateStandardParams);
					if (m != null) {
						if (!m.ReturnType.IsAssignableFrom(typeof(ITable))) {
							throw new ApplicationException("Method " + methodName + " needs return type of ITable");
						}
						methods.Add(m);
						invokeType = 6;
					}
				}
			} else {
				// Get the expression spec if it's available first,
				MethodInfo m = declaringType.GetMethod(methodName,
				                                       BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static, null,
				                                       FunctionMethodParams, null);
				if (m != null) {
					if (!typeof(ITable).IsAssignableFrom(m.ReturnType))
						throw new ApplicationException("Method " + methodName + " needs return type of ITable");

					methods.Add(m);
					invokeType = 1;
				}

				m = declaringType.GetMethod(methodName, FunctionMethodParams2);
				if (m != null) {
					if (!typeof(SqlObject).IsAssignableFrom(m.ReturnType)) {
						throw new ApplicationException("Method " + methodName + " needs return type of SqlObject");
					}
					methods.Add(m);
					invokeType = 2;
				}

				// If we didn't find the method, we go to the standard method declaration
				if (methods.Count == 0) {
					m = declaringType.GetMethod("FunctionStandard", FunctionStandardParams);
					if (m != null) {
						if (!typeof(ITable).IsAssignableFrom(m.ReturnType)) {
							throw new ApplicationException("Method " + methodName +" needs return type of ITable");
						}
						methods.Add(m);
						invokeType = 6;
					}
				}
			}

			// Check the methods
			foreach (MethodInfo m in methods) {
				if (!m.IsStatic)
					throw new ApplicationException("Method " + methodName + " is not static.");
				if (!m.IsPublic)
					throw new ApplicationException("Method " + methodName + " is not public.");
			}

			// If method resolution is ambiguous
			if (methods.Count > 1) {
				throw new AmbiguousMatchException("Ambiguous function reflection.");
			}
			// If no method resolution discovered
			if (methods.Count == 0)
				throw new ApplicationException("Could not find reflection method for  '" + methodName + "' in Type " + declaringType);

			evalContext = new ReflectionFunctionEvaluationContext(this, methods[0], invokeType);
		}

		public void SetEvaluationContext(Type declaringType) {
			SetEvaluationContext(declaringType, name);
		}
		
		public ITable EvaluateAggregate(QueryProcessor processor, bool distinct, ITable group, Expression[] args) {
			if (evalContext == null)
				throw new InvalidOperationException("Evaluation context was not set");
			
			return evalContext.EvaluateAggregate(processor, distinct, group, args);
		}
		
		public ITable Evaluate(QueryProcessor processor, Expression[] args) {
			if (evalContext == null)
				throw new InvalidOperationException("Evaluation context was not set");
			
			return evalContext.Evaluate(processor, args);
		}
	}
}