module rethinkdb.query;

import std.typecons;
import std.variant;

import vibe.data.json;

import rethinkdb.exception;
import ql = rethinkdb.ql2;

alias QueryType = ql.Query.QueryType;
alias TermType = ql.Term.TermType;
alias DatumType = ql.Datum.DatumType;

alias Datum = Algebraic!(typeof(null), bool, double, string, This[], This[string]);

Json serialize(Datum datum)
{
	if (datum.peek!(typeof(null)))
		return Json(null);
	else if (datum.peek!bool)
		return Json(datum.get!bool);
	else if (datum.peek!double)
		return Json(datum.get!double);
	else if (datum.peek!string)
		return Json(datum.get!string);
	else if (datum.peek!(Datum[])) {
		Json r = Json.emptyArray;
		foreach (Datum d; datum.get!(Datum[])) {
			r.appendArrayElement(d.serialize);
		}
		return r;
	} else if (datum.peek!(Datum[string])) {
		Json r = Json.emptyObject;
		foreach (string name, Datum d; datum.get!(Datum[string])) {
			r[name] = d;
		}
		return r;
	}
}

struct QueryTerm
{
	TermType type;
	Nullable!Datum datum;
	QueryTerm[] args;
	QueryTerm[string] optArgs;

	Json toJSON()
	{
		if (type == TermType.DATUM) {
			return datum.serialize;
		}
		// the rest...
	}
}

class Query
{
	static struct OptArgs
	{
		enum ReadMode : string
		{
			SINGLE = "single",
			MAJORITY = "majority",
			OUTDATED = "outdated"
		}
		enum TimeFormat : string
		{
			NATIVE = "native",
			RAW = "raw"
		}
		enum Durability : string
		{
			HARD = "hard",
			SOFT = "soft"
		}
		enum GroupFormat : string
		{
			NATIVE = "native",
			RAW = "raw"
		}
		enum BinaryFormat : string
		{
			NATIVE = "native",
			RAW = "raw"
		}

		ReadMode read_mode = ReadMode.SINGLE;
		TimeFormat time_format = TimeFormat.NATIVE;
		bool profile = false;
		Durability durability = Durability.HARD;
		GroupFormat group_format = GroupFormat.NATIVE;
		bool noreply = false;
		string db = "test";
		uint array_limit = 100000;
		BinaryFormat binary_format = BinaryFormat.NATIVE;
		uint min_batch_rows = 8;
		uint max_batch_rows = uint.max;
		uint max_batch_bytes = 1024 * 1024;
		float max_batch_seconds = 0.5;
		uint first_batch_scaledown_factor = 4;
	}

	this(ulong token, QueryType type=QueryType.START)
	{
		m_token = token;
		m_type = type;
	}

	ubyte[] serialize()
	{
		auto query = Json([cast(uint)m_type, m_term.toJson;
		auto len = cast(uint)query.length;
		ubyte[] message = new ubyte[12 + len];
		message[0 .. 8] = toBytes!ulong(m_token);
		message[8 .. 12] = toBytes!uint(len);
		message[12 .. $] = cast(ubyte[])query;
		return message;
	}

private:
	ulong m_token;
	QueryType m_type;
	QueryTerm m_term;
}