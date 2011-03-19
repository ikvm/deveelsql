using System;
using System.IO;

namespace Deveel.Data.Base {
	public static class Indexes<T> where T : IComparable<T> {
		public static IIndex<T> Array(int length, IBinaryIndexResolver<T> resolver) {
			return new BinarySortedIndex<T>(new MemoryStream(length), resolver, false);
		}
	}
}