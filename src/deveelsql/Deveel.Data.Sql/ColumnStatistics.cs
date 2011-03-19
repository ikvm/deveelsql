using System;

using Deveel.Data.Base;
using Deveel.Data.Sql.State;

namespace Deveel.Data.Sql {
	class ColumnStatistics {
		private const int DivisionPointCount = 32;
		private const int MaxSampleCount = 1024;

		private readonly Variable var;
		private readonly SqlObject[] divisionPoints;
		private long totalSize;
		private int sampleCount;

		internal ColumnStatistics(Variable var) {
			if (var == null)
				throw new ArgumentNullException("var");

			this.var = var;
			divisionPoints = new SqlObject[DivisionPointCount];
		}

		public double ProbabilityEstimate(SelectableRange rangeSet) {
			// If we don't have any information, return the worst case of 1.0.  This
			// will happen if the data set is very small.
			if (totalSize == 0) {
				return 1.0d;
			}

			// How many samples intersect the range set?
			IRangeIntersector comparator = rangeSet.Intersector;
			int intersect_count = 0;
			for (int i = 0; i < divisionPoints.Length; ++i) {
				if (comparator.ValueIntersects(new SqlObject[] { divisionPoints[i] })) {
					++intersect_count;
				}
			}

			// The worst case is ((1 + intersect_count) * row_to_samples) for single
			// value range sets.  This is the general heuristic we will use even
			// though the worst case could be higher for multi valued ranges.

			double iCount = intersect_count + 1;

			// Probability
			double probability = iCount / DivisionPointCount;
			return System.Math.Min(probability, 1.0d);
		}

		public double ProbabilityEstimate(string functionType) {
			// If no samples, assume worse
			if (sampleCount <= 0)
				return 1.0d;

			// equality functions,
			if (functionType.Equals("=") ||
			    functionType.Equals("==") ||
			    functionType.Equals("is")) {

				// We ignore nulls in our estimate if the function is '='
				bool includeNulls = functionType.Equals("is");

				// For equi functions, we find how many division points have equal
				// values and the worst string of equal values becomes our worst case.
				int curCount = 0;
				int worstCount = 1;
				for (int i = 1; i < divisionPoints.Length; ++i) {
					if (divisionPoints[i].Equals(divisionPoints[i - 1])) {
						if (!includeNulls && divisionPoints[i].IsNull) {
							curCount = 0;
						} else {
							++curCount;
						}
					} else {
						worstCount = System.Math.Max(worstCount, curCount);
						curCount = 0;
					}
				}
				worstCount = System.Math.Max(worstCount, curCount);

				// The worst case
				double probability = worstCount/(double) DivisionPointCount;
				return System.Math.Min(probability, 1.0d);
			}

			// Any other function that don't allow nulls, our worst case is the number
			// of non null values
			if (!functionType.Equals("isnot")) {
				int nullCount = 0;
				for (int i = 1; i < divisionPoints.Length; ++i) {
					if (!divisionPoints[i].IsNull) {
						break;
					}
					++nullCount;
				}

				int nonNullValues = DivisionPointCount - nullCount;

				double probability = nonNullValues/DivisionPointCount;
				return System.Math.Min(probability, 1.0d);
			}

			// Otherwise

			// This is 'isnot' some unknown value, we can't really determine anything
			// about this in a general sense so we return 1.0d
			return 1.0d;

		}

		public void PerformSample(SystemTransaction transaction) {
			// Translate into tables and column names
			ITable tableSource = transaction.GetTable(var.TableName);
			// DOn't bother unless the table has 64 or more values
			if (tableSource.RowCount < (DivisionPointCount * 2)) {
				sampleCount = 0;
				totalSize = 0;
				return;
			}
			// The number of elements in total
			totalSize = tableSource.RowCount;
			// The actual number of samples,
			sampleCount = (int)System.Math.Min(tableSource.RowCount / 2, MaxSampleCount);

			String col_name = var.Name;
			int colId = tableSource.Columns.IndexOf(var.Name);
			// Work out the size
			long size = tableSource.RowCount;
			// The sample point difference
			double sampleDiff = (double)size / sampleCount;
			// The index of the tables used in sampling
			IIndex<RowId> sampleIndex = transaction.CreateTemporaryIndex<RowId>(sampleCount);
			// Create a RowIndexCollation for this
			SqlType type;
			type = tableSource.Columns[colId].Type;
			IndexCollation collation = new IndexCollation(type, col_name);
			// Create the collation object,
			CollationIndexResolver resolver = new CollationIndexResolver(tableSource, collation);

			// The row cursor
			IRowCursor rowCursor = tableSource.GetRowCursor();

			RowId[] sampleRowset = new RowId[sampleCount];

			// First read in the row_ids we are sampling,
			{
				// The current sample point
				double p = 0;
				// The number read,
				int samplesRead = 0;
				// Make a sorted sample index of the dataset
				while (samplesRead < sampleCount) {
					long pos = ((long)p) - 1;
					pos = System.Math.Min(pos, tableSource.RowCount - 2);
					rowCursor.MoveTo(pos);
					if (!rowCursor.MoveNext())
						throw new SystemException();

					RowId rowId = rowCursor.Current;
					sampleRowset[samplesRead] = rowId;

					// Should this be Math.random(sample_diff * 2) for random distribution
					// of the samples?
					p += sampleDiff;
					++samplesRead;
				}
			}

			// Now read the samples,
			{

				int samplePoint = 0;

				foreach (RowId rowId in sampleRowset) {
					// Hint ahead the samples we are picking,
					if ((samplePoint % 24) == 0) {
						for (int i = samplePoint;
							 i < samplePoint + 24 && i < sampleRowset.Length;
							 ++i) {
							tableSource.PrefetchValue(-1, sampleRowset[i]);
						}
					}

					// Pick the sample and sort it,
					SqlObject[] sample = new SqlObject[] { tableSource.GetValue(colId, rowId) };
					sampleIndex.Insert(sample, rowId, resolver);

					++samplePoint;
				}
			}

			// Now extract the interesting sample points from the sorted set
			IIndexCursor<RowId> samplesCursor = sampleIndex.GetCursor();
			long sampleIndexSize = sampleIndex.Count;
			double divisionDiff = sampleIndexSize / (DivisionPointCount - 1);
			for (int i = 0; i < DivisionPointCount; ++i) {
				long samplePoint = (long)(divisionDiff * i);
				if (samplePoint >= sampleIndexSize) {
					samplePoint = sampleIndexSize - 1;
				}

				samplesCursor.Position = samplePoint - 1;
				if (!samplesCursor.MoveNext())
					throw new SystemException();

				RowId rowId = samplesCursor.Current;
				divisionPoints[i] = tableSource.GetValue(colId, rowId);
			}

			// Clear the temporary index
			sampleIndex.Clear();
		}
	}
}