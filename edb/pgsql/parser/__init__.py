from edb.pgsql.parser.exceptions import PSqlUnsupportedError
from .parser import pg_parse
from .ast_builder import build_queries

from typing import *

from edb.pgsql import ast as pgast


def parse(sql_query: str) -> List[pgast.Query]:
    import json

    ast_json = pg_parse(bytes(sql_query, encoding="UTF8"))

    try:
        return build_queries(json.loads(ast_json, strict=False))
    except IndexError:
        raise PSqlUnsupportedError()
