module rethinkdb.term;

import std.typecons;
import vibe.core.connectionpool;
import vibe.data.json;
import rethinkdb.connection;
import rethinkdb.datum;
import ql = rethinkdb.ql2;
import rethinkdb.query;

alias TermType = ql.Term.TermType;

struct Term
{
    this(Datum datum)
    {
        m_type = TermType.DATUM;
        m_datum = datum;
    }

    this(TermType type, Term[] args)
    {
        m_type = type;
        m_args = args;
    }

    this(TermType type, Term[] args, Term[string] optArgs)
    {
        m_type = type;
        m_args = args;
        m_optArgs = optArgs;
    }

    Json toJson()
    {
        if (m_type == TermType.DATUM) {
            return m_datum.toJson;
        }
        if (m_optArgs.isNull) {
            return Json([Json(cast(double)m_type), m_args.toJson()]);
        }
        return Json([Json(cast(double)m_type), m_args.toJson(), m_optArgs.toJson()]);
    }

    Query run(ConnectionPool!Connection pool, Json globalOptArgs=[])
    in
    {
        assert(globalOptArgs.type == Json.Type.object);
    }
    body
    {
        auto conn = pool.lockConnection();
        return conn.query(toJson(), globalOptArgs);
    }

private:
    TermType m_type;
    Datum m_datum;
    Term[] m_args;
    Nullable!Term[string] m_optArgs;
}

Json toJson(ref Term[] terms)
{
    Json r = Json.emptyArray;
    foreach (Term term; terms) {
        r.appendArrayElement(term.toJson());
    }
    return r;
}

Json toJson(ref Term[string] terms)
{
    Json r = Json.emptyObject;
    foreach (string name, Term term; terms) {
        r[name] = term.toJson();
    }
    return r;
}

Term db(string name)
{
    return Term(TermType.DB, [Term()])
}