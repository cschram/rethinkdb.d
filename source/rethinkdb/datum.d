module rethinkdb.datum;

import std.variant;
import vibe.data.json;

alias Datum = Algebraic!(typeof(null), bool, double, string, This[], This[string]);

Json toJSON(Datum datum)
{
    if (datum.peek!(typeof(null)))
        return Json(null);
    else if (datum.peek!bool)
        return Json(datum.get!bool);
    else if (datum.peek!double)
        return Json(datum.get!double);
    else if (datum.peek!string)
        return Json(datum.get!string);
    else if (datum.peek!(Datum[]))
        return datum.get!(Datum[]).toJSON();
    else if (datum.peek!(Datum[string]))
        return datum.get!(Datum[string]).toJSON();
    return Json(null);
}

Json toJSON(Datum[] datum)
{
    Json r = Json.emptyArray;
    foreach (Datum d; datum) {
        r.appendArrayElement(d.toJSON());
    }
    return r;
}

Json toJSON(Datum[string] datum)
{
    Json r = Json.emptyObject;
    foreach (string name, Datum d; datum) {
        r[name] = d.toJSON();
    }
    return r;
}