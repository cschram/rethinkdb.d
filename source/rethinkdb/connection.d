module rethinkdb.connection;

import std.algorithm;
import std.bitmanip;
import std.conv;
import std.datetime;
import std.traits;

import vibe.core.connectionpool;
import vibe.core.core;
import vibe.core.log;
import vibe.core.net;
import vibe.data.json;
import vibe.stream.operations;

import rethinkdb.client;
import rethinkdb.exception;
import ql = rethinkdb.ql2;
import rethinkdb.query;
import rethinkdb.term;
import rethinkdb.util;

final class Connection
{
	static struct Settings
	{
		string host = "localhost";
		ushort port = 28015;
		string db = "test";
		// Authentication key
		string key = "";
		// Timeout period in seconds for the connection to be opened
		Duration timeout = 20.seconds;
	}

	final static class Builder
	{
		Builder hostname(string host) nothrow @safe
		{
			m_settings.host = host;
			return this;
		}

		Builder port(ushort port) nothrow @safe
		{
			m_settings.port = port;
			return this;
		}

		Builder database(string name) nothrow @safe
		{
			m_settings.db = name;
			return this;
		}

		Builder authKey(string key) nothrow @safe
		{
			m_settings.key = key;
			return this;
		}

		Builder timeout(Duration timeout) nothrow @safe
		{
			m_settings.timeout = timeout;
			return this;
		}

		ConnectionPool!Connection connect() { return new ConnectionPool!Connection(&createConn); }

	private:
		Settings m_settings;

		Connection createConn() { return new Connection(m_settings); }
	}

	this(in Settings settings)
	{
		m_settings = settings;

		// Connect to server
		m_transport = connectTCP(settings.host, settings.port);
		scope(failure) m_transport.close();
		m_transport.readTimeout = settings.timeout;
		assert(m_transport.connected, "Connection failed");
		m_stream = m_transport; // When SSL support is added this will be a SSL stream when applicable

		// Initiate handshake
		uint size = cast(uint)settings.key.length;
		ubyte[] handshake = new ubyte[12 + size];
		handshake[0 .. 4] = toBytes!uint(ql.VersionDummy.Version.V0_4);
		handshake[4 .. 8] = toBytes!uint(size);
		handshake[8 .. size + 8] = cast(ubyte[])settings.key;
		handshake[8 + size .. $] = toBytes!uint(ql.VersionDummy.Protocol.JSON);
		m_stream.write(handshake);

		// Receive response from the server
		auto resp = m_stream.readUntil([0]);
		if (resp != "SUCCESS") {
			throw new RethinkDBConnectionExcpetion((cast(char[])resp).to!string);
		}
		logInfo("Connected to RethinkDB server %s:%s", settings.host, settings.port);
		m_loop = runTask(&readLoop);
	}

	~this()
	{
		disconnect();
	}

	void disconnect()
	{
		m_stream.finalize();
		m_transport.close();
	}

	@property bool connected() const { return m_transport.connected; }

	Query query(Json tree, Json globalOptArgs)
	{
		assert(connected, "Attempted to query without connection");
		auto token = ++m_counter;
		auto query = new Query(token, tree, globalOptArgs);
		m_queries[token] = query;
		m_stream.write(query.serialize(globalOptArgs));
	}

private:
	const(Settings) m_settings;
	TCPConnection m_transport;
	Stream m_stream;
	Task m_loop;
	ulong m_counter;
	Query[ulong] m_queries;

	void readLoop()
	{
		yield(); // There isn't going to be any incoming data immediately
		while(connected) {
			if (m_stream.dataAvailableForRead()) {
				ubyte[12] header;
				m_stream.read(header);
				auto token = fromBytes!ulong(header[0 .. 8]);
				auto size = fromBytes!uint(header[8 .. $]);
				assert(token in m_queries, "Unexpected message");
				ubyte[] buf = new ubyte[size];
				m_stream.read(buf);

				auto resp = parseJsonString((cast(char[])buf).to!string);
				// Should check for errors here

				m_queries[token].onResponse(cast(ResponseType)resp.t.get!int, resp.r);
				if (m_queries[token].state == Query.State.DONE) {
					m_queries.remove(token);
				}
			}
			yield();
		}
	}
}

Connection.Builder connection() pure @safe
{
    return new Connection.Builder();
}