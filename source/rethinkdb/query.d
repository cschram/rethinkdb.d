module rethinkdb.query;

import vibe.data.json;
import rethinkdb.exception;
import rethinkdb.datum;
import rethinkdb.term;
import ql = rethinkdb.ql2;
import rethinkdb.util;

alias QueryType = ql.Query.QueryType;
alias ResponseType = ql.Response.ResponseType;

final class Query
{
	static struct State
	{
		PROCESSING = 0,
		STREAMING = 1,
		DONE = 2
	}

	this(ulong token, Json tree, Json args)
	{
		m_token = token;
		m_tree = tree;
		m_args = optArgs;
	}

	ubyte[] serialize()
	{
		auto query = Json([Json(cast(double)m_type), m_tree, m_args]).toString();
		auto len = cast(uint)query.length;
		ubyte[] message = new ubyte[12 + len];
		message[0 .. 8] = toBytes!ulong(m_token);
		message[8 .. 12] = toBytes!uint(len);
		message[12 .. $] = cast(ubyte[])query;
		return message;
	}

	void onResponse(ResponseType type, Json data)
	{
		import vibe.core.log;
		logInfo(data.toString());
		m_state = State.DONE;
	}

	@property ulong token() { return m_token; }
	@property QueryType type() { return m_type; }
	@property State state() { return m_state; }

private:
	ulong m_token;
	QueryType m_type = QueryType.START;
	Json m_tree;
	Json m_args;
	State m_state;

	invariant
	{
		assert(m_optArgs.type == Json.Type.object);
	}
}