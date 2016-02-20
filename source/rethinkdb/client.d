module rethinkdb.client;

import rethinkdb.connection;

final class RethinkClient
{
	RethinkConnection.Builder connection() pure @safe
	{
		return new RethinkConnection.Builder(this);
	}

	@property void db(string name) { m_opts_db = name; }
	@property string db() { return m_opts_db; }

private:
	string m_opts_db;
}