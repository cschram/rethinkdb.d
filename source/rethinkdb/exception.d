module rethinkdb.exception;

import std.exception;

class RethinkException : Exception
{
	this(string message, string file = __FILE__, int line = __LINE__, Throwable next = null)
	{
		super(message, file, line, next);
	}
}