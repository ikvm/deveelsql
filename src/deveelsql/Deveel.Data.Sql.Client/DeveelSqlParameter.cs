using System;
using System.Data;

namespace Deveel.Data.Sql.Client {
	public sealed class DeveelSqlParameter : IDbDataParameter {
		private SqlType type;
		private object value;
		private DbType dbType;

		public DeveelSqlParameter(SqlType type, object value) {
			Type = type;
			Value = value;
		}

		public DeveelSqlParameter(object value)
			: this(SqlType.TypeOf(value), value) {
		}

		public DeveelSqlParameter() {
		}

		public SqlType Type {
			get { return type; }
			set {
				if (value == null)
					throw new ArgumentNullException("value");

				type = value;

				switch (value.TypeCode) {
					case SqlTypeCode.Binary:
						dbType = DbType.Binary;
						break;
					case SqlTypeCode.Boolean:
						dbType = DbType.Boolean;
						break;
					case SqlTypeCode.DateTime:
						dbType = DbType.DateTime;
						break;
					case SqlTypeCode.Numeric:
						dbType = DbType.VarNumeric;
						break;
					case SqlTypeCode.String:
						dbType = DbType.String;
						break;
					default:
						throw new ArgumentException("SQL Type '" + value + "' not supported in this context.");
				}
			}
		}

		DbType IDataParameter.DbType {
			get { return dbType; }
			set { 
				dbType = value;
				switch (value) {
					case DbType.AnsiString:
					case DbType.AnsiStringFixedLength:
					case DbType.StringFixedLength:
					case DbType.String:
						type = SqlType.String;
						break;
					case DbType.Binary:
						type = SqlType.Binary;
						break;
					case DbType.VarNumeric:
					case DbType.Byte:
					case DbType.Double:
					case DbType.Decimal:
					case DbType.Int16:
					case DbType.Int32:
					case DbType.Int64:
					case DbType.Single:
						type = SqlType.Numeric;
						break;
					case DbType.DateTime:
					case DbType.DateTime2:
					case DbType.DateTimeOffset:
					case DbType.Time:
						type = SqlType.DateTime;
						break;
					case DbType.Boolean:
						type = SqlType.Boolean;
						break;
					default:
						throw new ArgumentException("DbType '" + value + "' is not supported.");
				}
			}
		}

		System.Data.ParameterDirection IDataParameter.Direction {
			get { return System.Data.ParameterDirection.Input; }
			set {
				if (value != System.Data.ParameterDirection.Input)
					throw new ArgumentException();
			}
		}

		//TODO: map from the column connected
		public bool IsNullable {
			get { return true; }
		}

		string IDataParameter.ParameterName {
			get { return "?"; }
			set {
				if (value != "?")
					throw new ArgumentException();
			}
		}

		string IDataParameter.SourceColumn {
			get { return null; }
			set { throw new NotSupportedException(); }
		}

		DataRowVersion IDataParameter.SourceVersion {
			get { return DataRowVersion.Current; }
			set { throw new NotSupportedException(); }
		}

		public object Value {
			get { return value; }
			set { 
				SqlType sqlType = SqlType.TypeOf(value);
				if (!type.TypeCode.Equals(sqlType.TypeCode))
					Type = sqlType;
				if (type.IsSizeable) {
					if (type.IsString) {
						string s = (string) value;
						s = s.Substring(0, System.Math.Min(s.Length, Size));
						value = s;
					} else if (type.IsBinary) {
						//TODO:
					}
				}

				this.value = value;
			}
		}

		public byte Precision {
			get { return (byte) type.Precision; }
			set {
				if (!type.IsNumeric)
					throw new ArgumentException("The type is not a NUMERIC.");

				type = SqlType.MakeNumeric(value, Scale);
			}
		}

		public byte Scale {
			get { return (byte) type.Scale; }
			set {
				if (!type.IsNumeric)
					throw new ArgumentException("The type is not a NUMERIC.");

				type = SqlType.MakeNumeric(Precision, value);
			}
		}

		public int Size {
			get { return type.Size; }
			set {
				if (!type.IsSizeable)
					throw new ArgumentException("The type " + type + " does not support size.");

				if (type.IsBinary)
					type = SqlType.MakeBinary(value);
				else if (type.IsString)
					type = SqlType.MakeString(value);
			}
		}
	}
}