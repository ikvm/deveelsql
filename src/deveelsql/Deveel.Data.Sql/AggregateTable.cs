using System;
using System.Diagnostics;

using Deveel.Data.Base;

namespace Deveel.Data.Sql {
	public class AggregateTable : FilteredTable {
		private FunctionExpression aggregateComposite;
		private IIndex childGroupsIndex;
		private IRowCursor lookupCursor;

		public AggregateTable(ITableDataSource child, FunctionExpression aggregateComposite)
			: base(child) {
			this.aggregateComposite = aggregateComposite;
		}

		public void InitGroups(QueryProcessor processor, IIndex emptyIndexContainer) {
			Debug.Assert(emptyIndexContainer.Count == 0);
			ITableDataSource child = BaseTable;
			// No groups, so make the entire child table the group,
			if (aggregateComposite == null || child.RowCount <= 1) {
				emptyIndexContainer.Add(0);
				emptyIndexContainer.Add(child.RowCount);
			}
				// Populate the index by the aggregate composite,
			else {
				// Create a resolver for the composite function
				IndexResolver resolver = processor.CreateResolver(child, aggregateComposite);
				
				// The groups state
				long groupPos = 0;
				long groupSize = 0;
				SqlObject[] lastComposite = null;
				// Scan over the child
				IRowCursor cursor = child.GetRowCursor();
				while (cursor.MoveNext()) {
					long rowid = cursor.Current;
					// Get the group term
					SqlObject[] groupValue = resolver.GetValue(rowid);
					if (lastComposite == null) {
						lastComposite = groupValue;
					} else {
						int c = SqlObject.Compare(groupValue, lastComposite);
						// If group_val > the last composite, we are on a new group
						if (c > 0) {
							// New group,
							emptyIndexContainer.Add(groupPos);
							emptyIndexContainer.Add(groupSize);
							lastComposite = groupValue;
							groupPos = groupPos + groupSize;
							groupSize = 0;
						} else if (c < 0) {
							// This will happen if the child table is not sorted by the
							// composite expression.
							throw new ApplicationException("Aggregate child is not sorted correctly.");
						}
					}
					++groupSize;
				}
				// Final group
				// (the below check probably not necessary since we already check for the
				//  empty child so group size will always be >1 at this point).
				if (groupSize > 0) {
					emptyIndexContainer.Add(groupPos);
					emptyIndexContainer.Add(groupSize);
				}
			}
			// Set the group index
			this.childGroupsIndex = emptyIndexContainer;
			this.lookupCursor = BaseTable.GetRowCursor();
		}

		public ITableDataSource GetGroupValue(long rowid) {
			long n = (rowid * 2);
			// Get the group position and size in the index
			long groupPos = childGroupsIndex[n];
			long groupSize = childGroupsIndex[n + 1];
			// Create an interator over this subset of the filter index
			SubsetRowCursor subset = new SubsetRowCursor(BaseTable.GetRowCursor(), groupPos, groupSize);
			// Return the table subset
			SubsetTable groupTable = new SubsetTable(BaseTable, subset);
			// The subset retains the order of the child
			groupTable.SetOrderCompositeIsChild();
			// Return the group
			return groupTable;
		}
	}
}