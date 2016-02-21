module rethinkdb;

public import rethinkdb.client;
public import rethinkdb.connection;

unittest
{
    import vibe.d;
    import vibe.core.connectionpool;
    import etc.linux.memoryerror;
    static if (is(typeof(registerMemoryErrorHandler)))
        registerMemoryErrorHandler();

    alias Conn = LockedConnection!RethinkConnection;

    void testQuery(RethinkClient r, ref Conn conn)
    {
        auto query = Json([Json(1), Json("foo"), Json.emptyObject]);
        bool done = false;
        conn.runQuery(query, (Json resp) {
            assert(resp["r"][0].to!string == "foo", "Expected raw query to return \"foo\".");
            done = true;
        });

        while(!done) vibe.core.core.yield();
    }

    void runTest()
    {
        auto r = new RethinkClient();
        auto pool = r.connection().connect();
        auto conn = pool.lockConnection();
        scope (exit) conn.disconnect();

        testQuery(r, conn);
    }

    runTask({
        try {
            runTest();
        } finally {
            processEvents();
            exitEventLoop(true);
        }
    });
    runEventLoop();
}