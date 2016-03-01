//
// Datum type definition
// Currently this is just a wrapper around Vibe.d's Json type, as a stopgap for
// the future when time and binary psuedotypes are properly supported.
//

module rethinkdb.datum;

import std.variant;
import vibe.data.json;

alias Datum = Algebraic!(Json);

Json toJson(Datum value)
{
	return value.get!Json();
}

// In the future this should be generalized to support SysTime, ubyte[], etc.
Datum toDatum(Json value)
{
	return Datum(value);
}