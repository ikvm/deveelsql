using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class User {
		private readonly string name;
		private readonly bool toCreate;
		private DateTime createDate;
		private DateTime modifiedDate;
		private UserStatus status;
		private DateTime? expireDate;
		private DateTime sessionStarted;
		private List<Query> sessionCommands;
		private long sessionId;

		internal User(string name, bool toCreate) {
			this.name = name;
			this.toCreate = toCreate;

			if (toCreate)
				createDate = DateTime.Now;
		}

		public string Name {
			get { return name; }
		}

		public long SessionId {
			get { return sessionId; }
		}

		public bool IsInSession {
			get { return sessionId != -1; }
		}

		public DateTime SessionStarted {
			get { return sessionStarted; }
		}

		public DateTime Created {
			get { return createDate; }
			internal set { createDate = value; }
		}

		public DateTime Modified {
			get { return modifiedDate; }
			internal set { modifiedDate = value; }
		}

		public UserStatus Status {
			get { return status; }
			internal set { status = value; }
		}

		public DateTime? Expires {
			get { return expireDate; }
			internal set { expireDate = value; }
		}

		public bool IsSetToExpire {
			get { return expireDate != null; }
		}

		internal bool IsExpired {
			get { return IsSetToExpire && expireDate >= DateTime.Now; }
		}

		internal bool IsToCreate {
			get { return toCreate; }
		}

		internal void StartSession(long id) {
			sessionStarted = DateTime.Now;
			sessionCommands = new List<Query>();
			sessionId = id;
		}

		internal void OnSessionCommand(Query query) {
			if (sessionCommands == null)
				throw new SystemException();

			sessionCommands.Add(query);
		}

		internal void EndSession() {
			sessionId = -1;
			sessionCommands.Clear();
			sessionCommands = null;
			sessionStarted = DateTime.MinValue;
		}
	}
}