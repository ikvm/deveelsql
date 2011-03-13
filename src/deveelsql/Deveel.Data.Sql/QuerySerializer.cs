using System;
using System.IO;
using System.Text;

namespace Deveel.Data.Sql {
	public sealed class QuerySerializer {
		private Encoding encoding;

		public QuerySerializer(Encoding encoding) {
			this.encoding = encoding;
		}

		public QuerySerializer()
			: this(Encoding.Unicode) {
		}

		public Encoding Encoding {
			get { return encoding; }
			set { encoding = value; }
		}

		public void Serialize(Query query, Stream output) {
			if (query == null)
				throw new ArgumentNullException("query");
			if (output == null)
				throw new ArgumentNullException("output");

			if (!output.CanWrite)
				throw new ArgumentException();

			BinaryWriter writer = new BinaryWriter(output, encoding);

			writer.Write((byte)1);
			writer.Write(query.Text);
			writer.Write((byte)query.ParameterStyle);

			int paramCount = query.Parameters.Count;
			writer.Write(paramCount);

			for (int i = 0; i < paramCount; i++) {
				QueryParameter parameter = query.Parameters[i];
				writer.Write((byte)parameter.Direction);

				if (query.ParameterStyle == ParameterStyle.Marker) {
					writer.Write(parameter.Id);
				} else {
					writer.Write(parameter.Name);
				}

				SqlObject value = parameter.Value;
				if (value == null) {
					writer.Write((byte)0);
				} else {
					writer.Write((byte)1);

					SqlType type = value.Type;
					byte[] typeBuffer = type.ToBinary();

					writer.Write(typeBuffer.Length);
					writer.Write(typeBuffer);

					SqlValueInputStream valueInput = new SqlValueInputStream(value.Value);
					byte[] valueBuffer = new byte[valueInput.Length];
					valueInput.Read(valueBuffer, 0, valueBuffer.Length);
					writer.Write(valueBuffer);
				}
			}
		}

		public Query Deserialize(Stream input) {
			if (input == null)
				throw new ArgumentNullException("input");
			if (!input.CanRead)
				throw new ArgumentException("The input stream cannot be read.");

			BinaryReader reader = new BinaryReader(input);

			byte version = reader.ReadByte();
			if (version != 1)
				throw new FormatException("Invalid version.");

			string text = reader.ReadString();
			ParameterStyle style = (ParameterStyle) reader.ReadByte();

			Query query = new Query(text, style);

			int paramCount = reader.ReadInt32();

			for (int i = 0; i < paramCount; i++) {
				ParameterDirection direction = (ParameterDirection) reader.ReadByte();

				int id = -1;
				string name = null;

				if (style == ParameterStyle.Named) {
					name = reader.ReadString();
				} else {
					id = reader.ReadInt32();
				}

				byte valueType = reader.ReadByte();
				QueryParameter parameter;

				if (valueType == 0) {
					parameter = style == ParameterStyle.Marker
					            	? new QueryParameter(id, null, direction)
					            	: new QueryParameter(name, null, direction);
				} else {
					int typeBufferLength = reader.ReadInt32();
					byte[] typeBuffer = new byte[typeBufferLength];
					reader.Read(typeBuffer, 0, typeBufferLength);

					SqlType type = SqlType.FromBinary(typeBuffer);

					int valueLength = reader.ReadInt32();
					byte[] valueBuffer = new byte[valueLength];
					reader.Read(valueBuffer, 0, valueLength);

					SqlValue value = new SqlBinaryValue(valueBuffer);

					parameter = style == ParameterStyle.Marker
					            	? new QueryParameter(id, new SqlObject(type, value))
					            	: new QueryParameter(name, new SqlObject(type, value));
				}

				parameter.Direction = direction;
				query.Parameters.Add(parameter);
			}

			return query;
		}
	}
}