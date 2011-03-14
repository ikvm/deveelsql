using System;
using System.Collections;
using System.IO;
using System.Text;

namespace Deveel.Data.Sql {
	public class SqlUserType : SqlType {
		private string typeName;
		private IComparer comparer;
		private bool comparable;

		internal SqlUserType(string typeName) 
			: base(SqlTypeCode.UserType, null) {
			this.typeName = typeName;
		}

		internal SqlUserType()
			: this(null) {
		}

		public string TypeName {
			get { return typeName; }
		}

		public override bool IsComparable {
			get { return comparable; }
		}

		internal IComparer Comparer {
			get { return comparer; }
			set {
				comparable = (value != null);
				comparer = value;
			}
		}

		public override int Compare(object x, object y) {
			if (!comparable)
				throw new InvalidOperationException("Type '" + typeName + "' is not comparable.");

			if (comparer == null)
				throw new SystemException("The comparer was not set.");

			return comparer.Compare(x, y);
		}

		public override bool IsComparableTo(SqlType type) {
			SqlUserType userType = (SqlUserType) type;
			if (userType == null)
				return false;

			return comparable && typeName.Equals(userType.typeName);
		}

		internal override void ReadFrom(Stream input) {
			BinaryReader reader = new BinaryReader(input, Encoding.Unicode);
			typeName = reader.ReadString();

			//TODO: read the members
		}

		internal override void WriteTo(System.IO.Stream output) {
			BinaryWriter writer = new BinaryWriter(output, Encoding.Unicode);
			writer.Write(TypeName);
		}
	}
}