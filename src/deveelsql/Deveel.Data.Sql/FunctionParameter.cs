using System;

namespace Deveel.Data.Sql {
	public sealed class FunctionParameter {
		private readonly FunctionType[] types;
		private readonly FunctionParameterMatch match;
		private readonly string reference;
		private readonly bool isStar;
		
		private const string NoRefernece = "noref";
		
		public static readonly FunctionParameter Star = new FunctionParameter(true);
		
		private FunctionParameter(bool isStar) {
			this.isStar = isStar;
		}
		
		public FunctionParameter(FunctionType[] types, string reference, FunctionParameterMatch match)
			: this(false) {
			this.types = types;
			if (String.IsNullOrEmpty(reference))
				reference = NoRefernece;
			this.reference = String.Intern(reference);
			this.match = match;
		}
		
		public FunctionParameter(FunctionType[] types, FunctionParameterMatch match)
			: this(types, NoRefernece, match) {
		}
		
		public FunctionParameter(FunctionType[] types)
			: this(types, FunctionParameterMatch.Exact) {
		}
				
		public FunctionType[] Types {
			get { return types; }
		}
		
		public FunctionParameterMatch Match {
			get { return match; }
		}
		
		public string Reference {
			get { return reference; }
		}
		
		public bool IsStar {
			get { return isStar; }
		}
		
		internal bool HasReference {
			get { return !String.IsNullOrEmpty(reference) &&
					reference != NoRefernece; }
		}
	}
}