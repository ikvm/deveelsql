using System;

namespace Deveel.Data.Sql.Client {
	internal class ObjectValue : IValue {
		private readonly SqlType type;
		private readonly object value;

		public ObjectValue(SqlType type, object value) {
			this.type = type;
			this.value = value;
		}

		public ObjectValue(object value)
			: this(SqlType.TypeOf(value), value) {
		}

		public SqlType Type {
			get { return type; }
		}

		public bool IsNull {
			get { return value == null || type.IsNull; }
		}

		public bool IsConverted {
			get { return true; }
		}

		public bool IsReadOnly {
			get { return true; }
		}

		public object Value {
			get { return value; }
			set { throw new NotSupportedException(); }
		}

		public Type ValueType {
			get { return value == null ? typeof (DBNull) : value.GetType(); }
		}

		public long EstimateSize() {
			throw new NotImplementedException();
		}
	}
}