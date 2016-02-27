module rethinkdb.exception;

import std.exception;

class RethinkDBException : Exception
{
	this(string message, string file = __FILE__, int line = __LINE__, Throwable next = null)
	{
		super(message, file, line, next);
	}
}

class RethinkDBConnectionException : RethinkException
{
    this(string message, string file = __FILE__, int line = __LINE__, Throwable next = null)
    {
        super(message, file, line, next);
    }
}