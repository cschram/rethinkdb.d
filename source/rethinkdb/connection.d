module rethinkdb.connection;

import core.time;
import std.algorithm;
import std.bitmanip;
import std.conv;
import std.traits;

import vibe.core.connectionpool;
import vibe.core.core;
import vibe.core.log;
import vibe.core.net;
import vibe.data.json;
import vibe.stream.operations;

import ql = rethinkdb.ql2;
import rethinkdb.client;
import rethinkdb.exception;

private union ByteSwapper(T)
{
	Unqual!T value;
	ubyte[T.sizeof] array;
}

private auto toBytes(T)(T val)
{
	ByteSwapper!T bs = void;
	bs.value = val;
	return bs.array;
}

private auto fromBytes(T)(ubyte[] bytes)
{
	ByteSwapper!T bs = void;
	bs.array = bytes;
	return bs.value;
}

class RethinkConnectionException : RethinkException
{
	this(string message, string file = __FILE__, int line = __LINE__, Throwable next = null)
	{
		super(message, file, line, next);
	}
}

alias QueryResp = void delegate(Json resp);

final class RethinkConnection
{
	static struct ConnectionSettings
	{
		string host = "localhost";
		ushort port = 28015;
		string db = "test";
		string key = ""; // Authentication key
		Duration timeout = dur!"seconds"(20); // Timeout period in seconds for the connection to be opened
	}

	final static class Builder
	{
		this(RethinkClient client) pure @safe { m_client = client; }

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

		RethinkConnection createConn()
		{
			return new RethinkConnection(m_client, m_settings);
		}

		ConnectionPool!RethinkConnection connect()
		{
			m_client.db = m_settings.db;
			return new ConnectionPool!RethinkConnection(&createConn);
		}

	private:
		RethinkClient m_client;
		ConnectionSettings m_settings;
	}

	this(RethinkClient client, in ConnectionSettings settings)
	{
		m_client = client;
		m_settings = settings;

		// Connect to server
		m_transport = connectTCP(settings.host, settings.port);
		scope(failure) m_transport.close();
		m_transport.readTimeout = settings.timeout;
		assert(m_transport.connected);
		m_stream = m_transport; // When SSL support is added this will be a SSL stream when applicable

		// Initiate handshake
		uint size = settings.key.length;
		ubyte[] handshake = new ubyte[12 + size];
		handshake[0 .. 4] = toBytes!uint(ql.VersionDummy.Version.V0_4);
		handshake[4 .. 8] = toBytes!uint(size);
		handshake[8 .. size + 8] = cast(ubyte[])settings.key;
		handshake[8 + size .. $] = toBytes!uint(ql.VersionDummy.Protocol.JSON);
		m_stream.write(handshake);

		// Receive response from the server
		auto resp = m_stream.readUntil([0]);
		if (resp != "SUCCESS") {
			throw new RethinkConnectionException((cast(char[])resp).to!string);
		}
		logInfo("Connected to RethinkDB server %s:%s", settings.host, settings.port);
		readLoop();
	}

	~this()
	{
		disconnect();
	}

	void disconnect()
	{
		if (m_stream) {
			m_stream.finalize();
			m_stream = null;
		}

		if (m_transport) {
			m_transport.close();
			m_transport = null;
		}
	}

	@property bool connected() const { return m_transport && m_transport.connected; }

	void runQuery(Json query, QueryResp onResp)
	{
		assert(connected);
		auto id = ++m_counter;
		auto queryStr = query.toString();
		uint queryLen = queryStr.length;

		m_handlers[id] = onResp;

		ubyte[] message = new ubyte[12 + queryLen];
		message[0 .. 8] = toBytes!ulong(id);
		message[8 .. 12] = toBytes!uint(queryLen);
		message[12 .. $] = cast(ubyte[])queryStr;
		m_stream.write(message);
	}

private:
	RethinkClient m_client;
	const(ConnectionSettings) m_settings;
	TCPConnection m_transport;
	Stream m_stream;
	ulong m_counter;
	QueryResp[ulong] m_handlers;

	void readLoop()
	{
		logInfo("Entering connection loop");
		while(connected) {
			ubyte[12] header;
			m_stream.read(header);
			auto id = fromBytes!ulong(header[0 .. 8]);
			auto size = fromBytes!uint(header[8 .. $]);
			assert(id in m_handlers);
			ubyte[] buf = new ubyte[size];
			m_stream.read(buf);
			m_handlers[id](parseJsonString((cast(char[])buf).to!string));
			m_handlers.remove(id);
			yield();
		}
		logInfo("Exiting connection loop");
	}
}