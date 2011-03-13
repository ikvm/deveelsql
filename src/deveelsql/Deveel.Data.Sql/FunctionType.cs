using System;

namespace Deveel.Data.Sql {
	public sealed class FunctionType {
		private readonly SqlType type;
		private readonly bool isAny;
		private readonly bool isComparable;
		private readonly bool isTable;
		private readonly string reference;
		
		public static readonly FunctionType Any = new FunctionType(true, false, false);
		
		public static readonly FunctionType Comparable = new FunctionType(false, true, false);
		
		public static readonly FunctionType Table = new FunctionType(false, false, true);
		
		private FunctionType(bool isAny, bool isComparable, bool isTable) {
			this.isAny = isAny;
			this.isComparable = isComparable;
			this.isTable = isTable;
		}
		
		public FunctionType(SqlType type)
			: this(false, false, false) {
			this.type = type;
		}
		
		public FunctionType(string reference)
			: this(false, false, false) {
			this.reference = reference;
		}
		
		public bool IsAny {
			get { return isAny; }
		}
		
		public bool IsComparable {
			get { return isComparable; }
		}
		
		public bool IsTable {
			get { return isTable; }
		}
		
		public SqlType Type {
			get { return type; }
		}
		
		public bool IsReference {
			get { return !String.IsNullOrEmpty(reference); }
		}
		
		public string Reference {
			get { return reference; }
		}
	}
}