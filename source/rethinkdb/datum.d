//
// Datum type definition
// Currently this is just a wrapper around Vibe.d's Json type, as a stopgap for
// the future when time and binary psuedotypes are properly supported.
//

module rethinkdb.datum;

import std.meta;
import std.typecons;
import std.variant;
import vibe.data.json;

private alias DatumValue = Algebraic!(bool, int, float, string, This[], This[string]);

struct Datum
{
	this(T)(T value)
		if (DatumValue.allowed!T)
	{
		opAssign(value);
	}

	this(Args...)(Args args)
		if (allSatisfy!(DatumValue.allowed, Args))
	{
		opAssign(args);
	}

	this(Json value)
	{
		opAssign(value);
	}

	Datum opAssign(T)(T value)
		if (DatumValue.allowed!T)
	{
		m_datum = value;
		m_json = Json.undefined; // Reset cached JSON value
		return this;
	}

	Datum opAssign(Args...)(Args args)
		if (allSatisfy!(DatumValue.allowed, Args))
	{
		m_datum = new Datum[args.length];
		foreach(size_t i, arg; args) {
			m_datum[i] = Datum(arg);
		}
	}

	Datum opAssign(Json value)
	{
		m_json = value;
		if (value.type == Json.Type.array) {
			m_datum = new DatumValue[value.length];
			for (size_t i = 0; i < value.length; i++) {
				m_datum[i] = value[i];
			}
		} else if (value.type == Json.Type.object) {
			DatumValue[string] datum;
			foreach (string name, v; value) {
				datum[name] = v;
			}
			m_datum = datum;
		} else if (value.type == Json.Type.bool_) {
			m_datum = value.get!bool;
		} else if (value.type == Json.Type.float_) {
			m_datum = value.get!float;
		} else if (value.type == Json.Type.int_) {
			m_datum = value.get!int;
		} else if (value.type == Json.Type.string) {
			m_datum = value.get!string;
		} else if (value.type == Json.Type.null_) {
			m_datum = null;
		}
		return this;
	}

	// This property is lazily evaluated to avoid making the JSON conversion in the case
	// that a datum never ends up being serialized.
	@property Json json()
	{
		if (m_json == Json.undefined) {
			if (m_datum.isNull) {
				m_json = null;
			} else if (m_datum.peek!bool) {
				m_json = m_datum.get!bool;
			} else if (m_datum.peek!int) {
				m_json = m_datum.get!int;
			} else if (m_datum.peek!float) {
				m_json = m_datum.get!float;
			} else if (m_datum.peek!string) {
				m_json = m_datum.get!string;
			} else if (m_datum.peek!(DatumValue[])) {
				m_json = Json.emptyArray;
				foreach(d; m_datum.get!(DatumValue[])) {
					m_json.appendArrayElement(d.json);
				}
			} else if (m_datum.peek!(DatumValue[string])) {
				m_json = Json.emptyObject;
				foreach(key, d; m_datum.get!(DatumValue[string])) {
					m_json[key] = d.json;
				}
			}
		}
		return m_json;
	}

private:
	Nullable!DatumValue m_datum;
	Json m_json;
}