using System;
using System.IO;

namespace Deveel.Data.Base {
	public interface IBinaryIndexResolver<T> {
		int ItemLength { get; }


		void Write(T value, Stream output);

		T Read(Stream input);
	}
}