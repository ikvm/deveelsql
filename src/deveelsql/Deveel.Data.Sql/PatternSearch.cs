using System;
using System.Text;

namespace Deveel.Data.Sql {
	public sealed class PatternSearch {
		private const char ZERO_OR_MORE_CHARS = '%';
		private const char ONE_CHAR = '_';


		#region PatternTokenizer

		private class PatternTokenizer {
			private int p;
			private bool last_was_wildcard;
			private readonly string pattern;
			private readonly char escape_char;

			public PatternTokenizer(String pattern, char escape_char) {
				this.pattern = pattern;
				p = 0;
				this.escape_char = escape_char;
			}

			public int Position {
				get { return p; }
				set { p = value; }
			}

			public bool LastWasWildcard {
				get { return last_was_wildcard; }
			}

			public bool HasNext() {
				return p < pattern.Length;
			}

			// Returns either a wildcard or none wildcard part of the string, or null
			// if the end of the pattern string has been reached
			public String NextToken() {
				int len = pattern.Length;
				int cur_token = p;
				int next_token = -1;
				if (p >= len)
					return null;

				bool containsEscapeChars = false;

				// Are we iterating over a wildcard or a string?

				// If it's not a wild card,
				if (!IsWildCard(pattern[p])) {
					last_was_wildcard = false;
					bool last_escape_char = false;
					for (; p < len && next_token == -1; ++p) {
						char c = pattern[p];
						if (last_escape_char) {
							last_escape_char = false;
						} else if (c == escape_char) {
							containsEscapeChars = true;
							last_escape_char = true;
						} else if (IsWildCard(c)) {
							next_token = p;
							break;
						}
					}
				}
					// If it is a wild card,
				else {
					last_was_wildcard = true;
					// Look for the next char that isn't a wild card
					for (; p < len && next_token == -1; ++p) {
						char c = pattern[p];
						if (!IsWildCard(c)) {
							next_token = p;
							break;
						}
					}
				}
				// If no next token, we have reached the end
				string tokenStr = next_token == -1 ? pattern.Substring(cur_token) : pattern.Substring(cur_token, p);

				// If the token string contains escape characters,
				if (containsEscapeChars) {
					// Remove them,
					StringBuilder sb = new StringBuilder();
					for (int i = 0; i < tokenStr.Length; ++i) {
						char c = tokenStr[i];
						if (c != escape_char) {
							sb.Append(c);
						}
					}
					tokenStr = sb.ToString();
				}
				// Return the token string
				return tokenStr;
			}
		}

		#endregion

		private static bool IsWildCard(char ch) {
			return (ch == ONE_CHAR || ch == ZERO_OR_MORE_CHARS);
		}

		private static bool NewPatternMatch(PatternTokenizer tokenizer, String str) {
			// If no more tokens and str is empty, we matched
			if (!tokenizer.HasNext())
				return (str.Length == 0);

			// Get the next token from the tokenizer
			string token = tokenizer.NextToken();

			// Is it a wild card token?
			if (tokenizer.LastWasWildcard) {
				// Yes, what are the minimum and maximum extent of characters to match
				// by this wildcard string?
				int strLen = str.Length;
				int min = 0;
				int max = 0;
				for (int i = 0; i < token.Length; ++i) {
					if (token[i] == ONE_CHAR) {
						++min;
						++max;
					} else if (token[i] == ZERO_OR_MORE_CHARS) {
						max = strLen;
					} else {
						throw new ApplicationException("Tokenizer error");
					}
				}
				// If it's not possible to match this size string,
				if (min > strLen) {
					return false;
				}
				// If there are no more tokens to match,
				if (!tokenizer.HasNext()) {
					// If str_len falls within the size of the pattern we can match
					// then return true, otherwise false
					return strLen >= min && strLen <= max;
				}

				// Search for the index of the next token. It's not possible for this to
				// be a wildcard.
				string next_tok = tokenizer.NextToken();
				int p = min;
				while (true) {
					p = str.IndexOf(next_tok, p);
					if (p < 0 || p > max) {
						// Not found, so fail this
						return false;
					}
					// Recurse at the point we found
					int state = tokenizer.Position;
					if (NewPatternMatch(tokenizer, str.Substring(p + next_tok.Length))) {
						return true;
					}
					// Reverse state if the search failed and try again
					tokenizer.Position = state;
					++p;
				}
			}

			// Not a wild card, so match

			// If the string doesn't match the token, we return false
			if (!str.StartsWith(token))
				return false;

			// Otherwise recurse	
			return NewPatternMatch(tokenizer, str.Substring(token.Length));
		}

		public static bool Match(string pattern, string str, char escapeChar) {
			// Create the tokenizer
			PatternTokenizer tokenizer = new PatternTokenizer(pattern, escapeChar);
			return NewPatternMatch(tokenizer, str);
		}
	}
}