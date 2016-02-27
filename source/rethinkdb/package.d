module rethinkdb;

public import rethinkdb.connection;
public import rethinkdb.datum;
public import rethinkdb.exception;
public import rethinkdb.query;
public import rethinkdb.term;

Connection.Builder connection() pure @safe
{
    return new Connection.Builder();
}

unittest
{
    import vibe.d;
    import vibe.core.connectionpool;
    import etc.linux.memoryerror;
    static if (is(typeof(registerMemoryErrorHandler)))
        registerMemoryErrorHandler();

    void testQuery(ref auto conn)
    {
        auto term = Term(Datum("foo"));
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
        auto pool = connection().connect();
        auto conn = pool.lockConnection();
        testQuery(conn);
    }

    runTask({
        try {
            runTest();
        } catch(Throwable e) {
            logError(e.toString());
        } finally {
            processEvents();
            exitEventLoop(true);
        }
    });
    runEventLoop();
}