module rethinkdb.util;

import std.traits;

union ByteSwapper(T)
{
	Unqual!T value;
	ubyte[T.sizeof] array;
}

auto toBytes(T)(T val)
{
	ByteSwapper!T bs = void;
	bs.value = val;
	return bs.array;
}

auto fromBytes(T)(ubyte[] bytes)
{
	ByteSwapper!T bs = void;
	bs.array = bytes;
	return bs.value;
}