from .exceptions import PSqlParseError

cdef extern from "pg_query.h":
    ctypedef struct PgQueryError:
        char *message
        int lineno
        int cursorpos

    ctypedef struct PgQueryParseResult:
        char *parse_tree
        PgQueryError *error

    PgQueryParseResult pg_query_parse(const char* input)

    void pg_query_free_parse_result(PgQueryParseResult result);


def pg_parse(query) -> str:
    cdef PgQueryParseResult result

    result = pg_query_parse(query)
    if result.error:
        error = PSqlParseError(
            result.error.message.decode('utf8'),                   
            result.error.lineno, result.error.cursorpos
        )
        pg_query_free_parse_result(result)
        raise error

    result_utf8 = result.parse_tree.decode('utf8')
    pg_query_free_parse_result(result)
    return result_utf8
