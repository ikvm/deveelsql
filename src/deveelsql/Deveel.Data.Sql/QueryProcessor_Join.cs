using System;
using System.Collections.Generic;

using Deveel.Data.Base;

namespace Deveel.Data.Sql {
	public sealed partial class QueryProcessor {
		private ITableDataSource Join(ITableDataSource left, ITableDataSource right, JoinExpression op) {
			// Get the type of join,
			JoinType joinType = op.JoinType;
			// The filter expression
			Expression filterExp = op.Filter;

			// If it's a simple relation
			bool simpleRelation = op.IsSimpleRelation;

			// If the join is not a simple relation, then we need to naturally join
			// and scan
			if (!simpleRelation) {
				JoinedTableBase result = new NaturalJoinedTable(left, right);
				result.SetOrderCompositeIsChild();
				if (filterExp != null)
					// return the scan over the cartesian product
					return FilterByScan(result, filterExp);
					
				return result;
			}
			
			// This is a simple relation so we may not need to scan over the
			// cartesian join.  A simple relation is of the type '[something1]
			// [comparison] [something2]' where something1 and 2 reference terms
			// in the right and left tables exclusively, or a multi variable
			// equivalence comparison such as 't1.a = t2.a and t1.b = t2.b'.

			// A join of this type should always be a scan on the left and lookup
			// on the right.

			// The process cost (roughly)
			long processCost = 0;

			// NOTE, these are marked up by the QueryCostModel (perhaps should move
			//   this markup functionality in the planner.
			IList<Expression> leftVarExps = (IList<Expression>)op.GetArgument("!left_var_exps");
			IList<Expression> rightVarExps = (IList<Expression>)op.GetArgument("!right_var_exps");
			IList<string> functionTypes = (IList<string>)op.GetArgument("!function_types");
				
			// Right index, if applicable
			TableName rIndexStr = (TableName)op.GetArgument("use_right_index");
			TableName rIndexTableName = (TableName)op.GetArgument("use_right_index_table_name");

			// If the right index is defined, then we know the cost model has
			// determined the right table has a single index we can use.
			IIndexSetDataSource rightIndex;
			IndexResolver rightResolver;
				
			if (rIndexStr != null) {
				// Fetch the index
				rightIndex = GetIndex(right, rIndexStr);
				
				// If no index, we screwed up somewhere.  Error in cost model most
				// likely.
				if (rightIndex == null)
					throw new ApplicationException("Right index '" + rIndexStr + "' not found.");
				
				// Create a resolver for the right table
				IndexCollation rcollation = rightIndex.Collation;
				rightResolver = new CollationIndexResolver(right, rcollation);
			} else {
				// No right index, so we need to prepare a temporary index
				// We index on the right var ops (note that 'right_var_ops' will not
				// necessarily be a variable reference, it may be a complex expression).

				// Create the resolver for the term(s) on the right table
				Expression[] rops = new Expression[rightVarExps.Count];
				rightVarExps.CopyTo(rops, 0);
				rightResolver = CreateResolver(right, rops);
					
				// The working set,
				IIndex workingSet = transaction.CreateTemporaryIndex(right.RowCount);
				
				// Iterate over the right table
				IRowCursor rightCursor = right.GetRowCursor();
				// Wrap in a forward prefetch cursor
				rightCursor = new PrefetchRowCursor(rightCursor, right);
				
				while (rightCursor.MoveNext()) {
					// The rowid
					long rowid = rightCursor.Current;
					// Fetch the SqlObject
					SqlObject[] value = rightResolver.GetValue(rowid);
					// Index it
					workingSet.Insert(value, rowid, rightResolver);
				}
				
				// Map this into a RowIndex object,
				rightIndex = new IndexBasedIndexSetDataSource(right, rightResolver, workingSet);

				// Rough cost estimate of a sort on the right elements
				processCost += rightCursor.Count * 5;
			}

			// Now we have a rightIndex and rightResolver that describes the keys
			// we are searching for. Scan the left table and lookup values in the
			// right.
			
			// The join function
			string joinFunctionName = functionTypes[0];
				
			// Work out the maximum number of elements needed to perform this join
			long maxSize;
			long leftSize = left.RowCount;
			long rightSize = right.RowCount;
				
			// Make sure to account for the possibility of overflow
			if (leftSize < Int32.MaxValue && rightSize < Int32.MaxValue) {
				maxSize = leftSize * rightSize;
			} else {
				// This is a poor estimate, but it meets the requirements of the
				// contract of 'createTemporaryIndex'.  Idea: use a BigDecimal here?
				maxSize = Int64.MaxValue;
			}
				
			// Allocate the indexes
			IIndex leftSet = transaction.CreateTemporaryIndex(maxSize);
			IIndex rightSet = transaction.CreateTemporaryIndex(maxSize);
				
			// Create a resolver for the left terms
			Expression[] lops = new Expression[leftVarExps.Count];
			leftVarExps.CopyTo(lops, 0);
			IndexResolver leftResolver = CreateResolver(left, lops);
			
			// Cursor over the left table
			IRowCursor leftCursor = left.GetRowCursor();

			// Wrap in a forward prefetch cursor
			leftCursor = new PrefetchRowCursor(leftCursor, left);

			while (leftCursor.MoveNext()) {
				// The left rowid
				long leftRowid = leftCursor.Current;
					
				// TODO: Need to change this to support multi-column join
				//   conditions,
					
				// Fetch it into a SqlObject
				SqlObject[] value = leftResolver.GetValue(leftRowid);
				
				// lookup in the right
				SelectableRange joinRange = SelectableRange.Full;
				joinRange = joinRange.Intersect(SelectableRange.GetOperatorFromFunction(joinFunctionName), value);
				IRowCursor matchedResult = rightIndex.Select(joinRange);
				
				// If there are elements
				if (matchedResult.Count > 0) {
					// For each matched element, add a left rowid and right rowid
					while (matchedResult.MoveNext()) {
						long rightRowid = matchedResult.Current;
						leftSet.Add(leftRowid);
						rightSet.Add(rightRowid);
					}
				} else {
					// If there are no elements, is this an outer join?
					if (joinType == JoinType.OuterLeft) {
						// Yes, so add left with a null entry,
						leftSet.Add(leftRowid);
						rightSet.Add(-1);
					}
				}
			}

			// Rough cost estimate on the scan/lookup
			processCost += (left.RowCount + (left.RowCount * 5));

			// Return the joined table.
			JoinedTableBase joinTable = new JoinedTable(left, right, leftSet, rightSet);
			joinTable.SetOrderCompositeIsChild();
			return joinTable;
		}
	}
}