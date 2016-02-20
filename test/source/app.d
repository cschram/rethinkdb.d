import std.stdio;

import vibe.d;
import rethinkdb;

void runTest()
{
	auto r = new RethinkClient();
	auto pool = r.connection().connect();
	auto conn = pool.lockConnection();
	scope (exit) conn.disconnect();
	assert(conn.connected);
	logInfo("Test");
}

void main()
{
	runTask({
		try runTest();
		finally exitEventLoop(true);
	});
	runEventLoop();
}