using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Deveel.Data.Sql.Parser {
	internal class Util {
		public static String AsNonQuotedRef(Token token) {
			if (token.kind == SqlParserConstants.QUOTED_VARIABLE)
				// Strip " from start and end if a quoted variable
				return token.image.Substring(1, token.image.Length - 2);

			if (token.kind == SqlParserConstants.QUOTED_DELIMINATED_REF ||
				token.kind == SqlParserConstants.QUOTEDGLOBVARIABLE) {
				// Remove all " from the string
				string image = token.image;
				StringBuilder b = new StringBuilder();
				int sz = image.Length;
				for (int i = 0; i < sz; ++i) {
					char c = image[i];
					if (c != '\"') {
						b.Append(c);
					}
				}
				return b.ToString();
			}

			return token.image;
		}

		public static object ToParamObject(Token token, bool upperIdentifiers) {
			if (token.kind == SqlParserConstants.STRING_LITERAL) {
				string raw_string = token.image.Substring(1, token.image.Length - 2);
				return new SqlObject(EscapeTranslated(raw_string));
			}

			if (token.kind == SqlParserConstants.BOOLEAN_LITERAL)
				return new SqlObject(token.image.ToLower(CultureInfo.InvariantCulture).Equals("true"));

			if (token.kind == SqlParserConstants.NULL_LITERAL)
				return SqlObject.Null;

			if (token.kind == SqlParserConstants.REGEX_LITERAL) {
				// Horrible hack,
				// Get rid of the 'regex' string at the start,
				String str = token.image.Substring(5).Trim();
				return new SqlObject(str);
			}
			if (token.kind == SqlParserConstants.QUOTED_VARIABLE ||
					   token.kind == SqlParserConstants.GLOBVARIABLE ||  // eg. Part.*
					   token.kind == SqlParserConstants.IDENTIFIER ||
					   token.kind == SqlParserConstants.DOT_DELIMINATED_REF ||
					   token.kind == SqlParserConstants.QUOTED_DELIMINATED_REF) {
				string name = AsNonQuotedRef(token);
				if (upperIdentifiers)
					name = name.ToUpper(CultureInfo.InvariantCulture);

				Variable v;
				int div = name.LastIndexOf('.');
				if (div != -1) {
					// Column represents '[something].[name]'
					// Check if the column name is an alias.
					String column_name = name.Substring(div + 1);
					// Make the '[something]' into a TableName
					TableName table_name = TableName.Resolve(name.Substring(0, div));

					// Set the variable name
					v = new Variable(table_name, column_name);
				} else {
					// Column represents '[something]'
					v = new Variable(name);
				}
				return v;
			} else {  // Otherwise it must be a reserved word, so just return the image
				// as a variable.
				string name = token.image;
				if (upperIdentifiers) {
					name = name.ToUpper(CultureInfo.InvariantCulture);
				}
				return new Variable(name);
			}
		}

		public static SqlObject ParseNumberToken(Token token, bool negative) {
			if (negative)
				return new SqlObject(BigNumber.Parse("-" + token.image));
			return new SqlObject(BigNumber.Parse(token.image));
		}

		public static TableName ParseTableName(Token token) {
			if (token.kind == SqlParserConstants.QUOTED_VARIABLE ||
				token.kind == SqlParserConstants.GLOBVARIABLE ||  // eg. Part.*
				token.kind == SqlParserConstants.IDENTIFIER ||
				token.kind == SqlParserConstants.DOT_DELIMINATED_REF ||
				token.kind == SqlParserConstants.QUOTED_DELIMINATED_REF) {

				return TableName.Resolve(AsNonQuotedRef(token));
			}
				
			return TableName.Resolve(token.image);
		}

		public static String MakeSourceString(Token start, Token end) {
			StringBuilder buf = new StringBuilder();
			Token t = start.next;
			while (t != end) {
				buf.Append(t.image);
				buf.Append(' ');
				t = t.next;
			}
			buf.Append(t.image);
			return buf.ToString();
		}

		public static String MakeLabel(Token start, Token end) {
			StringBuilder buf = new StringBuilder();
			Token t = start.next;
			bool last_letter = false;
			while (t != end) {
				bool is_letter_token = Char.IsLetter(t.image[0]);
				if (is_letter_token) {
					if (last_letter) {
						buf.Append(' ');
					}
					last_letter = true;
				} else {
					last_letter = false;
				}
				buf.Append(t.image);
				t = t.next;
			}
			buf.Append(t.image);
			return buf.ToString();
		}


		private static string EscapeTranslated(string input) {
			StringBuilder result = new StringBuilder();
			int size = input.Length;
			bool lastCharEscape = false;
			bool lastCharQuote = false;
			for (int i = 0; i < size; ++i) {
				char c = input[i];
				if (lastCharQuote) {
					lastCharQuote = false;
					if (c != '\'') {
						result.Append(c);
					}
				} else if (lastCharEscape) {
					if (c == '\\') {
						result.Append('\\');
					} else if (c == '\'') {
						result.Append('\'');
					} else if (c == 't') {
						result.Append('\t');
					} else if (c == 'n') {
						result.Append('\n');
					} else if (c == 'r') {
						result.Append('\r');
					} else {
						result.Append('\\');
						result.Append(c);
					}
					lastCharEscape = false;
				} else if (c == '\\') {
					lastCharEscape = true;
				} else if (c == '\'') {
					lastCharQuote = true;
					result.Append(c);
				} else {
					result.Append(c);
				}
			}
			return result.ToString();
		}

		public static FunctionParameter CreateFunctionParameter(IList<string> list, string reference, string matchCount) {
			FunctionType[] types = new FunctionType[list.Count];
			for (int i = 0; i < list.Count; i++) {
				string s = list[i];
				FunctionType funType;
				if (String.Compare(s, "any", true) == 0) {
					funType = FunctionType.Any;
				} else if (String.Compare(s, "comparable", true) == 0) {
					funType = FunctionType.Comparable;
				} else if (String.Compare(s, "table", true) == 0) {
					funType = FunctionType.Table;
				} else {
					funType = new FunctionType(SqlType.Parse(s));
				}

				types[i] = funType;
			}

			FunctionParameterMatch match;
			if (matchCount == "1") {
				match = FunctionParameterMatch.Exact;
			} else if (matchCount == "+") {
				match = FunctionParameterMatch.OneOrMore;
			} else if (matchCount == "*") {
				match = FunctionParameterMatch.ZeroOrMore;
			} else {
				match = FunctionParameterMatch.ZeroOrOne;
			}

			return new FunctionParameter(types, reference, match);
		}

		public static void SetFunctionReturnType(Function function, string s) {
			function.Return.Type = new FunctionType(SqlType.Parse(s));
		}

		public static void SetFunctionVariableReturnType(Function function, string s) {
			function.Return.Type = new FunctionType(s);
		}
	}
}