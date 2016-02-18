module rethinkdb.client;

import rethinkdb.connection;

final class RethinkClient
{
	RethinkConnection.Builder connection() pure @safe
	{
		return new RethinkConnection.Builder();
	}
}