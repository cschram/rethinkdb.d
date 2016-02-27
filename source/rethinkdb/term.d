module rethinkdb.term;

import std.typecons;
import vibe.core.connectionpool;
import vibe.data.json;
import rethinkdb.connection;
import ql = rethinkdb.ql2;
import rethinkdb.query;

alias TermType = ql.Term.TermType;

final class Term
{
    this(Json datum)
    {
        m_type = TermType.DATUM;
        m_datum = datum;
    }

    this(TermType type, Term[] args=[], Term[string] optArgs=[])
    {
        m_type = type;
        m_args = args;
        m_optArgs = optArgs;
    }

    Json toJSON()
    {
        if (m_type == TermType.DATUM) {
            return m_datum;
        }
        if (m_optArgs.isNull) {
            return Json([Json(cast(double)m_type), m_args.toJSON()]);
        }
        return Json([Json(cast(double)m_type), m_args.toJSON(), m_optArgs.toJSON()]);
    }

    Query run(ConnectionPool!Connection pool, Json globalOptArgs=[])
    in
    {
        assert(globalOptArgs.type == Json.Type.object);
    }
    body
    {
        auto conn = pool.lockConnection();
        return conn.query(toJSON(), globalOptArgs);
    }

private:
    TermType m_type;
    Json m_datum;
    Term[] m_args;
    Nullable!Term[string] m_optArgs;
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