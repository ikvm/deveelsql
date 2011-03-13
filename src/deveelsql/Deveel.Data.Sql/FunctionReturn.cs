using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public sealed class FunctionReturn {
		private readonly Function function;
		private FunctionType type;
		
		internal FunctionReturn(Function function) {
			this.function = function;
		}
		
		public FunctionType Type {
			get { return type; }
			set { type = value; }
		}
		
		public bool IsConstant {
			get { return !type.IsReference; }
		}
		
		public SqlType ConstantType {
			get { 
				if (!IsConstant)
					throw new InvalidOperationException("The return type is not constant.");
				return type.Type;
			}
		}
		
		public string Reference {
			get {
				if (IsConstant)
					throw new InvalidOperationException("The return type is constant.");
				return type.Reference;
			}
		}
		
		private static bool TypeMatches(SqlType type, FunctionType[] types) {
			foreach (FunctionType funcType in types) {
				if (funcType.IsComparable) {
					//TODO: also check for user-defined types that implement 'IComparable'
					if (!type.IsBinary) {
						return true;
					}
				} else if (funcType.Type.IsBoolean) {
					if (type.IsBoolean) {
						return true;
					}
				} else if (funcType.IsAny) {
					return true;
				} else if (funcType.Type.IsNumeric) {
					if (type.IsNumeric) {
						return true;
					}
				} else if (funcType.Type.IsString) {
					if (type.IsString) {
						return true;
					}
				} else if (funcType.IsTable) {
					// TODO?
					return true;
				} else {
					throw new ApplicationException("Unknown type specification.");
				}
			}
			return false;
		}
		
		private FunctionType TypesConsume(int paramIndex, int param_consume, IList<SqlType> args, int start, int end, FunctionType guessed_t) {
			// If size mismatch on consumed amount
			if (end - start != param_consume)
				return null;

			for (int i = start; i < end; ++i) {
				SqlType t = args[i];
				FunctionParameter param = function.Parameters[paramIndex];
				FunctionType[] types = param.Types;

				if (!TypeMatches(t, types))
					return null;
				
				if (guessed_t.IsReference) {
					string reference = param.Reference;
					if (guessed_t.Reference.Equals(reference)) {
						guessed_t = new FunctionType(t);
					}
				}
				// Go to the next param,
				++paramIndex;
			}

			// All matches,
			return guessed_t;

		}

		private FunctionType RegexConsume(int paramIndex, IList<SqlType> args, int start, int end, FunctionType guessedT) {
			FunctionParameter param = function.Parameters[paramIndex];
			FunctionParameterMatch regexType = param.Match;
			string reference = param.Reference;
			FunctionType[] types = param.Types;

			if (end - start == 0) {
				if (regexType == FunctionParameterMatch.OneOrMore) {
					return null;
				}
			}
			if (end - start > 1) {
				if (regexType == FunctionParameterMatch.ZeroOrOne) {
					return null;
				}
			}

			for (int i = start; i < end; ++i) {
				SqlType t = args[i];

				if (!TypeMatches(t, types)) {
					return null;
				}
				if (guessedT.IsReference) {
					if (guessedT.Reference.Equals(reference)) {
						guessedT = new FunctionType(t);
					}
				}
			}

			return guessedT;
		}
		
		public SqlType ResolveType(IList<SqlType> args) {
			// We assume that the specification;
			//   Will only ever have 1 term with a regex of '?', '*', '+' which will be
			//   at the start or end of the specification.
			//   All other terms will be '1'.

			int pcount = function.Parameters.Count;

			// If no parameters,
			if (pcount == 0)
				return args.Count == 0 ? type.Type : null;

			// Single parameter,
			if (pcount == 1) {
				// Is it a regex?
				FunctionParameterMatch matchType = function.Parameters[0].Match;
				FunctionType foundT;
				if (matchType == FunctionParameterMatch.Exact) {
					// No, so straight match,
					foundT = TypesConsume(0, 1, args, 0, args.Count, type);
				} else {
					// Yes, so regex match,
					foundT = RegexConsume(0, args, 0, args.Count, type);
				}
				if (foundT == null || foundT.IsReference) {
					return null;
				}
				return foundT.Type;

			}
				// More than 1 parameter
			else {
				FunctionType foundT = type;

				// If the first is a regex
				FunctionParameterMatch firstMatch = function.Parameters[0].Match;
				FunctionParameterMatch lastMatch = function.Parameters[function.Parameters.Count - 1].Match;
				if (firstMatch != FunctionParameterMatch.Exact) {
					// Yes, so anchor against the last terms,
					int anchor_size = pcount - 1;

					foundT = RegexConsume(0, args, 0, args.Count - anchor_size, foundT);
					if (foundT == null) {
						return null;
					}
					foundT = TypesConsume(1, pcount - 1, args, args.Count - anchor_size, args.Count, foundT);
					if (foundT == null || foundT.IsReference) {
						return null;
					}
					return foundT.Type;

				}
				if (lastMatch != FunctionParameterMatch.Exact) {
					// Yes, so anchor against first term,
					int anchor_size = pcount - 1;

					foundT = TypesConsume(0, pcount - 1, args, 0, anchor_size, foundT);
					if (foundT == null) {
						return null;
					}
					foundT = RegexConsume(anchor_size, args, anchor_size, args.Count, foundT);
					if (foundT == null || foundT.IsReference) {
						return null;
					}
					return foundT.Type;

				}

				// First and last are '1', so it's a straight match,

				foundT = TypesConsume(0, pcount, args, 0, args.Count, foundT);
				if (foundT == null || foundT.IsReference) {
					return null;
				}
				return foundT.Type;
			}
		}
	}
}