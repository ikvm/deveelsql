using System;

namespace Deveel.Data.Sql {
	public sealed class SqlParseException : ApplicationException {
		private readonly int line;
		private readonly int column;
		
		internal SqlParseException(string message, ILineInfo lineInfo)
			: this(message, lineInfo.Line, lineInfo.Column) {
		}
		
		internal SqlParseException(string message, int line, int column)
			: base(message) {
			this.line = line;
			this.column = column;
		}

		internal SqlParseException(string message, Exception innerException)
			: base(message, innerException) {
		}
		
		public int Line {
			get { return line; }
		}
		
		public int Column {
			get { return column; }
		}
	}
}