using System;

namespace Deveel.Data.Sql.Client {
	internal interface IValue {
		SqlType Type { get; }

		bool IsNull { get; }

		bool IsConverted { get; }

		bool IsReadOnly { get; }

		object Value { get; set; }

		Type ValueType { get; }

		long EstimateSize();

	}
}