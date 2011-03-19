using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Client;

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
		private SystemTransaction transaction;
		private bool verified;

		private const string SystemUserName = "@SYSTEM";

		public static User System = new User(SystemUserName, false);

		internal User(string name, bool toCreate) {
			this.name = name;
			this.toCreate = toCreate;

			if (toCreate)
				createDate = DateTime.Now;
		}

		internal User(string name)
			: this(name, false) {
		}

		public string Name {
			get { return name; }
		}

		public bool IsSystem {
			get { return name == SystemUserName; }
		}

		internal SystemTransaction Transaction {
			get { return transaction; }
		}

		public bool HasOpenTransaction {
			get { return transaction != null; }
		}

		public DateTime TransactionStarted {
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

		internal void StartTransaction(SystemTransaction tnx) {
			if (!verified) {
				tnx.VerifyUser(this);
				verified = true;
			}

			verified = true;
			sessionStarted = DateTime.Now;
			sessionCommands = new List<Query>();
			transaction = tnx;
		}

		internal void OnTransactionCommand(Query query) {
			if (sessionCommands == null)
				throw new SystemException();

			sessionCommands.Add(query);
		}

		internal void EndTransaction() {
			transaction = null;
			sessionCommands.Clear();
			sessionCommands = null;
			sessionStarted = DateTime.MinValue;
			verified = false;
		}

		internal User Verified() {
			User user = new User(name);
			user.verified = true;
			return user;
		}
	}
}