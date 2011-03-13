using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public sealed class SelectableRange : IEnumerable<RangePair> {
		private readonly List<SqlObject> encoding;
		private int pp;

		public static readonly SqlObject First = new SqlObject(SqlType.Null, SqlValue.FromString("[FIRST]"));
		public static readonly SqlObject Last = new SqlObject(SqlType.Null, SqlValue.FromString("[LAST]"));

		public static readonly SqlObject BeforeFirst = new SqlObject(SqlType.Null, SqlValue.FromString("[BEFORE_FIRST]"));
		public static readonly SqlObject AfterLast = new SqlObject(SqlType.Null, SqlValue.FromString("[AFTER_LAST]"));

		public static readonly SelectableRange Full;
		public static readonly SelectableRange Empty;

		private SelectableRange() {
			encoding = new List<SqlObject>(8);
		}

		static SelectableRange() {
			Full = new SelectableRange();
			Full.encoding.Clear();
			Full.encoding.Add(First);
			Full.encoding.Add(Last);
			Empty = new SelectableRange();
		}

		public bool IsFull {
			get {
				return encoding.Count == 2 &&
					   (encoding[0] == First &&
						encoding[1] == Last);
			}
		}

		public bool IsEmpty {
			get { return encoding.Count == 0; }
		}

		public IRangeIntersector Intersector {
			get { return new SelectableRangeIntersector(this); }
		}

		private static bool IsSpecialValue(SqlObject v) {
			return v == First ||
				   v == Last ||
				   v == BeforeFirst ||
				   v == AfterLast;
		}

		private static int GetSpecialValue(SqlObject v) {
			if (v == BeforeFirst)
				return 1;
			if (v == First)
				return 2;
			if (v == Last)
				return 3;
			if (v == AfterLast)
				return 4;

			throw new ArgumentException("Unknown special value");
		}

		private int NextValue(int p) {
			while (true) {
				SqlObject v = encoding[p];
				// If it's a special value,
				if (IsSpecialValue(v))
					return p + 1;
				++p;
			}
		}

		private static int Compare(SqlObject[] val1, SqlObject[] val2) {
			int p1 = 0;
			int p2 = 0;
			while (true) {
				SqlObject v1 = val1[p1];
				SqlObject v2 = val2[p2];
				if (IsSpecialValue(v1)) {
					if (IsSpecialValue(v2)) {
						// Both are special values,
						// If the values are the same then return 0
						if (v1 == v2)
							return 0;
						// The rest,
						int m1 = GetSpecialValue(v1);
						int m2 = GetSpecialValue(v2);
						return m1 - m2;
					}

					// v1 is special, v2 is normal
					if (v1 == BeforeFirst || v1 == First)
						return -1;

					// (LAST and AFTER_LAST)
					return 1;
				}
				if (IsSpecialValue(v2)) {
					// v1 is normal, v2 is special
					if (v2 == BeforeFirst || v2 == First)
						return 1;

					// (LAST and AFTER_LAST)
					return -1;
				}

				// Neither are special values,
				int c = v1.CompareTo(v2);
				// Only continue if the values are equal,
				if (c != 0)
					return c;
				++p1;
				++p2;
			}
		}

		private void MoveToFirstPair() {
			pp = 0;
		}

		private bool HasNextPair() {
			return (pp < encoding.Count);
		}

		private RangePair GetNextPair() {
			// Find the pair parts and move pp to the next part.
			int first_part = pp;
			int second_part = NextValue(pp);
			pp = NextValue(second_part);

			// Extract the pair parts from the encoding array.
			int sz1 = second_part - first_part;
			int sz2 = pp - second_part;

			SqlObject[] v1 = new SqlObject[sz1];
			SqlObject[] v2 = new SqlObject[sz2];

			for (int i = 0; i < sz1; ++i) {
				v1[i] = encoding[first_part + i];
			}
			for (int i = 0; i < sz2; ++i) {
				v2[i] = encoding[second_part + i];
			}

			// Return it as an object
			return new RangePair(v1, v2);
		}

		private void AddPair(RangePair pair) {
			SqlObject[] v1 = pair.Value1;
			SqlObject[] v2 = pair.Value2;
			foreach (SqlObject v in v1) {
				encoding.Add(v);
			}
			foreach (SqlObject v in v2) {
				encoding.Add(v);
			}
		}

		private void InsertPair(int p, RangePair pair) {
			SqlObject[] v1 = pair.Value1;
			SqlObject[] v2 = pair.Value2;
			foreach (SqlObject v in v1) {
				encoding.Insert(p, v);
				++p;
			}
			foreach (SqlObject v in v2) {
				encoding.Insert(p, v);
				++p;
			}
		}

		private void RemoveNextPair() {
			int second_part = NextValue(pp);
			int end = NextValue(second_part);
			int remove_count = end - pp;
			// This will not be very efficient, we are doing an arraycopy for each
			// remove and shifting the whole list left.
			for (int i = 0; i < remove_count; ++i) {
				encoding.RemoveAt(pp);
			}
		}

		private void AddPair(SqlObject[] val1, SqlObject code1,
							 SqlObject[] val2, SqlObject code2) {

			if (val1 == null)
				val1 = new SqlObject[0];
			if (val2 == null)
				val2 = new SqlObject[0];

			for (int i = 0; i < val1.Length; ++i) {
				encoding.Add(val1[i]);
			}
			encoding.Add(code1);

			for (int i = 0; i < val2.Length; ++i) {
				encoding.Add(val2[i]);
			}
			encoding.Add(code2);
		}

		private void CopyFrom(SelectableRange source) {
			encoding.Clear();
			encoding.AddRange(source.encoding);
		}

		private static RangePair PairIntersection(RangePair p1, RangePair p2) {
			SqlObject[] lower;
			int lc = Compare(p1.Value1, p2.Value1);
			if (lc > 0) {
				lower = p1.Value1;
			} else {
				lower = p2.Value1;
			}
			SqlObject[] upper;
			int uc = Compare(p1.Value2, p2.Value2);
			if (uc > 0) {
				upper = p2.Value2;
			} else {
				upper = p1.Value2;
			}

			// If lower is greater or equal to upper, return null (no intersection)
			int fc = Compare(lower, upper);
			if (fc >= 0) {
				return RangePair.Null;
			}
			// Otherwise return the intersected pair,
			return new RangePair(lower, upper);
		}

		private static RangePair PairUnion(RangePair p1, RangePair p2) {
			SqlObject[] lower;
			SqlObject[] upper;

			if (Compare(p1.Value1, p2.Value1) < 0) {
				lower = p1.Value1;
			} else {
				lower = p2.Value1;
			}
			if (Compare(p1.Value2, p2.Value2) > 0) {
				upper = p1.Value2;
			} else {
				upper = p2.Value2;
			}

			return new RangePair(lower, upper);
		}

		private static bool IntersectCheck(RangePair p1, RangePair p2) {
			if (Compare(p1.Value1, p2.Value2) <= 0 &&
				Compare(p1.Value2, p2.Value1) >= 0) {
				return true;
			}
			return false;
		}

		private void UnionPair(RangePair in_pair) {
			// We go through each pair in this encoding. If the 'pair' intersects with
			// in_pair, we modify 'pair' to be a union with in_pair. If a modify
			// happens, we check all later pairs to see if a union happens and modify
			// as appropriate.

			MoveToFirstPair();
			// For each pair,
			while (HasNextPair()) {
				int previous_pp = pp;
				RangePair pair = GetNextPair();

				if (IntersectCheck(pair, in_pair)) {
					// Intersection; modify range to be a union of this pair,

					RangePair p = in_pair;
					do {
						p = PairUnion(pair, p);
						pp = previous_pp;
						RemoveNextPair();
						if (!HasNextPair()) {
							break;
						}
						pair = GetNextPair();
					}
					while (IntersectCheck(pair, p));

					InsertPair(previous_pp, p);

					return;
				}
			}

			// If we get here then this pair does not intersect, so find the position
			// to insert this pair into the encoding,

			MoveToFirstPair();
			// For each pair,
			while (HasNextPair()) {
				int insert = pp;
				RangePair pair = GetNextPair();
				if (Compare(in_pair.Value1, pair.Value1) < 0) {
					InsertPair(insert, in_pair);
					return;
				}
			}
			// Otherwise add to the end,
			AddPair(in_pair);
		}

		public SelectableRange Intersect(SelectableRange in_range) {
			// We go through each pair in this range and find the result of
			// intersecting it in each part of 'in_range'.

			SelectableRange result = new SelectableRange();

			MoveToFirstPair();
			while (HasNextPair()) {
				RangePair pair_src = GetNextPair();
				in_range.MoveToFirstPair();
				while (in_range.HasNextPair()) {
					RangePair pair_dst = in_range.GetNextPair();
					RangePair intersect = PairIntersection(pair_src, pair_dst);
					if (!intersect.Equals(RangePair.Null)) {
						result.AddPair(intersect);
					}
				}
			}

			return result;
		}

		public SelectableRange Intersect(RangeOperator op, SqlObject[] values) {
			return Intersect(RangeConstruction(op, values));

		}

		public SelectableRange Union(RangeOperator op, SqlObject[] values) {
			return Union(RangeConstruction(op, values));
		}

		public SelectableRange Union(SelectableRange in_range) {
			// Set result to a copy of in_range.
			SelectableRange result = new SelectableRange();
			result.CopyFrom(in_range);

			MoveToFirstPair();
			while (HasNextPair()) {
				RangePair pair_src = GetNextPair();
				result.UnionPair(pair_src);
			}

			return result;
		}

		private static SelectableRange RangeConstruction(RangeOperator op, SqlObject[] values) {
			// The input range pair, or set,
			SelectableRange in_range = new SelectableRange();
			if (op == RangeOperator.NotEquals ||
				op == RangeOperator.IsNot) {
				in_range.AddPair(null, First, values, BeforeFirst);
				in_range.AddPair(values, AfterLast, null, Last);
			} else {
				if (op == RangeOperator.Greater) {
					in_range.AddPair(values, AfterLast, null, Last);
				} else if (op == RangeOperator.GreaterOrEquals) {
					in_range.AddPair(values, First, null, Last);
				} else if (op == RangeOperator.Lesser) {
					in_range.AddPair(null, First, values, BeforeFirst);
				} else if (op == RangeOperator.LesserOrEquals) {
					in_range.AddPair(null, First, values, Last);
				} else if (op == RangeOperator.Is) {
					// null allowed
					in_range.AddPair(values, First, values, Last);
				} else if (op == RangeOperator.Equals) {
					// null not allowed
					in_range.AddPair(values, First, values, Last);
				} else {
					throw new ArgumentException();
				}
			}
			return in_range;
		}
		
		internal static RangeOperator GetOperatorFromFunction(string functionName) {
			switch(functionName) {
				case "@is_sql":
					return RangeOperator.Is;
				case "@isn_sql":
					return RangeOperator.IsNot;
				case "@eq_sql":
					return RangeOperator.Equals;
				case "@neq_sql":
					return RangeOperator.NotEquals;
				case "@gt_sql":
					return RangeOperator.Greater;
				case "@lt_sql":
					return RangeOperator.Lesser;
				case "@gte_sql":
					return RangeOperator.GreaterOrEquals;
				case "@lte_sql":
					return RangeOperator.LesserOrEquals;
				default:
					throw new ArgumentException("Invalid function for a range.");
			}
		}

		public int Count() {
			ISelectableRangeEnumerator i = GetEnumerator();
			int count = 0;
			while (i.MoveNext()) {
				++count;
			}
			return count;
		}

		#region SelectableRangeIntersector

		private class SelectableRangeIntersector : IRangeIntersector {
			private readonly SelectableRange range;

			public SelectableRangeIntersector(SelectableRange range) {
				this.range = range;
			}

			public bool ValueIntersects(SqlObject[] values) {
				SqlObject[] testValue = new SqlObject[values.Length + 1];
				Array.Copy(values, 0, testValue, 0, values.Length);
				testValue[values.Length] = First;

				range.MoveToFirstPair();
				while (range.HasNextPair()) {
					RangePair pair = range.GetNextPair();
					// Check if the value intersects the pair
					// NOTE: The intersection check is on the value (val, FIRST).
					// ISSUE: Should we generate an error if 'val' is a smaller composite
					//   than the pairs being checked? This situation seems a little
					//   illogical.

					if (Compare(pair.Value1, testValue) <= 0 &&
						Compare(pair.Value2, testValue) >= 0) {
						// Yes, intersection so return true,
						return true;
					}

				}
				// No intersection,
				return false;
			}
		}

		#endregion

		#region SelectableRangeEnumerator

		private class SelectableRangeEnumerator : ISelectableRangeEnumerator {
			private readonly SelectableRange range;
			private int p1;
			private int p2;
			private int next;
			private bool first;

			public SelectableRangeEnumerator(SelectableRange range) {
				this.range = range;
				Reset();
			}

			public bool MoveNext() {
				if (!first) {
					if (next == range.encoding.Count) {
						next = Int32.MaxValue;
					} else {
						p1 = next;
						p2 = range.NextValue(p1);
						next = range.NextValue(p2);
					}
				}

				first = false;
				return (next <= range.encoding.Count);
			}

			public void Reset() {
				if (range.encoding.Count > 0) {
					p1 = 0;
					p2 = range.NextValue(p1);
					next = range.NextValue(p2);
				} else {
					next = 0;
				}

				first = true;
			}

			object IEnumerator.Current {
				get { return Current; }
			}

			public RangePair Current {
				get { return new RangePair(LowerBound, UpperBound); }
			}

			public SqlObject[] LowerBound {
				get {
					int sz = (p2 - p1) - 1;
					SqlObject[] result = new SqlObject[sz];
					for (int i = 0; i < sz; ++i) {
						result[i] = range.encoding[p1 + i];
					}
					return result;
				}
			}

			public SqlObject[] UpperBound {
				get {
					int sz = (next - p2) - 1;
					SqlObject[] result = new SqlObject[sz];
					for (int i = 0; i < sz; ++i) {
						result[i] = range.encoding[p2 + i];
					}
					return result;
				}
			}

			public bool IsLowerBoundAtFirst {
				get {
					SqlObject v = range.encoding[p2 - 1];
					return (v == First);
				}
			}

			public bool IsUpperBoundAtLast {
				get {
					SqlObject v = range.encoding[next - 1];
					return (v == Last);
				}
			}

			public void Dispose() {
			}
		}

		#endregion

		public ISelectableRangeEnumerator GetEnumerator() {
			return new SelectableRangeEnumerator(this);
		}

		IEnumerator<RangePair> IEnumerable<RangePair>.GetEnumerator() {
			return GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}
	}
}