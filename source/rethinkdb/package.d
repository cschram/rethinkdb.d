module rethinkdb;

public import rethinkdb.client;
public import rethinkdb.connection;

unittest
{
    import vibe.d;
    import etc.linux.memoryerror;
    static if (is(typeof(registerMemoryErrorHandler)))
        registerMemoryErrorHandler();

    void runTest()
    {
        auto r = new RethinkClient();
        auto pool = r.connection().connect();
        auto conn = pool.lockConnection();
        scope (exit) conn.disconnect();
        logInfo("Test");
    }

    runTask({
        try runTest();
        finally exitEventLoop(true);
    });
    runEventLoop();
}