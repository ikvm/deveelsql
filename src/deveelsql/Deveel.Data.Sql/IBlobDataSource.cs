using System;

namespace Deveel.Data.Sql {
	public interface IBlobDataSource {
		SqlType Type { get; }

		long Length { get; }


		void SetLength(long value);

		int Read(long position, byte[] buffer, int offset, int length);

		void Write(long position, byte[] buffer, int offset, int length);

		void Shift(long position, long count);
	}
}