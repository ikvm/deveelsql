using System;
using System.IO;

using Deveel.Data.Base;

namespace Deveel.Data.Sql {
	public sealed class RowIdIndex : BinarySortedIndex<RowId> {
		public RowIdIndex(Stream stream, int itemLength)
			: base(stream, new Resolver(itemLength)) {
		}

		#region RowIdIndexResolver

		public class Resolver : IBinaryIndexResolver<RowId> {
			private readonly int itemLength;

			public Resolver(int itemLength) {
				this.itemLength = itemLength;
			}

			public int ItemLength {
				get { return itemLength; }
			}

			public void Write(RowId value, Stream output) {
				BinaryWriter writer = new BinaryWriter(output);
				byte[] buffer = value.ToBinary();
				writer.Write(buffer);
			}

			public RowId Read(Stream input) {
				BinaryReader reader = new BinaryReader(input);
				byte[] buffer = new byte[itemLength];
				reader.Read(buffer, 0, itemLength);
				return new RowId(buffer);
			}
		}

		#endregion
	}
}