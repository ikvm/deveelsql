using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public class AliasedTable : FilteredTable {
		private readonly TableName alias;
		private readonly Variable[] columnNames;
		private readonly Dictionary<Variable, int> columnsLookup;

		public AliasedTable(ITableDataSource filter, TableName alias_name)
			: base(filter) {
			alias = alias_name;
			columnNames = new Variable[filter.ColumnCount];
			columnsLookup = new Dictionary<Variable, int>();
		}

		private Variable GetAliasedColumnName(int column) {
			return new Variable(alias, base.GetColumnName(column).Name);
		}

		public override TableName TableName {
			get { return alias; }
		}

		public override Variable GetColumnName(int column) {
			if (columnNames[column] == null)
				columnNames[column] = GetAliasedColumnName(column);
			return columnNames[column];
		}

		public override int GetColumnOffset(Variable v) {
			int ind;
			if (!columnsLookup.TryGetValue(v, out ind)) {
				ind = -1;
				int sz = ColumnCount;
				for (int i = 0; i < sz; ++i) {
					if (GetColumnName(i).Equals(v)) {
						ind = i;
						break;
					}
				}
				columnsLookup[v] = ind;
			}
			return ind;
		}

	}
}