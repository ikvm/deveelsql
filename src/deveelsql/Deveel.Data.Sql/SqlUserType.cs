using System;
using System.IO;
using System.Text;

namespace Deveel.Data.Sql {
	public class SqlUserType : SqlType {
		private string typeName;

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