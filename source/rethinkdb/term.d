module rethinkdb.term;

import std.typecons;
import vibe.data.json;
import ql = rethinkdb.ql2;

alias TermType = ql.Term.TermType;

struct Term
{
    TermType type;
    Nullable!Datum datum;
    Term[] args;
    Term[string] optArgs;

    this(Datum p_datum)
    {
        type = TermType.DATUM;
        datum = p_datum;
    }

    this(TermType p_type, Term[] p_args=[], Term[string] p_optArgs=[])
    {
        type = p_type;
        args = p_args;
        optArgs = p_optArgs;
    }

    Json toJSON()
    {
        if (type == TermType.DATUM) {
            assert(!datum.isNull);
            return datum.toJSON();
        }
        return Json([Json(cast(double)type), args.toJSON(), optArgs.toJSON()]);
    }
}

Json toJSON(Term[] terms)
{
    Json r = Json.emptyArray;
    foreach (Term term; terms) {
        r.appendArrayElement(term.toJSON());
    }
    return r;
}

Json toJSON(Term[string] terms)
{
    Json r = Json.emptyObject;
    foreach (string name, Term term; terms) {
        r[name] = term.toJSON();
    }
    return r;
}