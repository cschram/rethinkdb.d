module rethinkdb.connection;

import core.time;
import std.algorithm;
import std.bitmanip;
import std.conv;
import std.traits;

import vibe.core.connectionpool;
import vibe.core.log;
import vibe.core.net;
import vibe.stream.operations;

import rethinkdb.ql2;
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

class RethinkConnectionException : RethinkException
{
	this(string message, string file = __FILE__, int line = __LINE__, Throwable next = null)
	{
		super(message, file, line, next);
	}
}

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
			return new RethinkConnection(m_settings);
		}

		ConnectionPool!RethinkConnection connect()
		{
			return new ConnectionPool!RethinkConnection(&createConn);
		}

	private:
		ConnectionSettings m_settings;
	}

	this(in ConnectionSettings settings)
	{
		m_settings = settings;

		// Connect to server
		m_transport = connectTCP(settings.host, settings.port);
		scope(failure) m_transport.close();
		m_transport.readTimeout = settings.timeout;
		assert(m_transport.connected);

		// Initiate handshake
		uint size = settings.key.length;
		ubyte[] handshake = new ubyte[12 + size];
		handshake[0 .. 4] = toBytes!uint(VersionDummy.Version.V0_4);
		handshake[4 .. 8] = toBytes!uint(size);
		handshake[8 .. size + 8] = cast(ubyte[])settings.key;
		handshake[8 + size .. handshake.length] = toBytes!uint(VersionDummy.Protocol.JSON);
		m_transport.write(handshake);

		// Receive response from the server
		auto resp = m_transport.readUntil([0]);
		if (resp != "SUCCESS") {
			throw new RethinkConnectionException((cast(char[])resp).to!string);
		}
		logInfo("Connected to RethinkDB server %s:%s", settings.host, settings.port);
	}

	~this()
	{
		disconnect();
	}

	void disconnect()
	{
		if (m_transport) {
			m_transport.close();
			m_transport = null;
		}
	}

	@property bool connected() const { return m_transport && m_transport.connected; }

private:
	const(ConnectionSettings) m_settings;
	TCPConnection m_transport;
}