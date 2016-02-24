module rethinkdb.query;

import vibe.data.json;
import rethinkdb.exception;
import rethinkdb.datum;
import rethinkdb.term;
import ql = rethinkdb.ql2;
import rethinkdb.util;

alias QueryType = ql.Query.QueryType;

class Query
{
	this(ulong token, QueryType type=QueryType.START)
	{
		m_token = token;
		m_type = type;
	}

	this(ulong token, Term term, QueryType type=QueryType.START)
	{
		m_token = token;
		m_type = type;
		m_term = term;
	}

	ubyte[] serialize(Datum[string] args)
	{
		auto query = Json([Json(cast(double)m_type), m_term.toJSON(), args.toJSON()]);
		auto len = cast(uint)query.length;
		ubyte[] message = new ubyte[12 + len];
		message[0 .. 8] = toBytes!ulong(m_token);
		message[8 .. 12] = toBytes!uint(len);
		message[12 .. $] = cast(ubyte[])query.toString();
		return message;
	}

private:
	ulong m_token;
	QueryType m_type;
	Term m_term;
}