from __future__ import annotations

from edb.edgeql import ast as qlast
from edb.edgeql import Source, parser
from typing import Any, List, Sequence
import json
import uuid
from edb.errors import EdgeQLSyntaxError
import re


class EdbJSONEncoder(json.JSONEncoder):
    def default(self, x: Any) -> Any:
        if isinstance(x, uuid.UUID):
            return str(x)
        return super().default(x)


def parse_ddl(ddlstr: str) -> List[qlast.DDLOperation]:
    ddls = parser.parse_block(Source.from_string(ddlstr))
    assert all(isinstance(ddl, qlast.DDLOperation) for ddl in ddls)
    return ddls  # type: ignore[return-value]


def parse_ql(querystr: str) -> Sequence[qlast.Expr]:
    def notExpr(expr: qlast.Base) -> Any:
        raise EdgeQLSyntaxError("Not an Expression", span=expr.span)

    source = Source.from_string(querystr)
    base_statements = parser.parse_block(source)  # type : Sequence[Expr]
    statements: Sequence[qlast.Expr] = [
        s if isinstance(s, qlast.Expr) else notExpr(s) for s in base_statements
    ]
    return statements


def parse_sdl(sdlstr: str) -> qlast.Schema:
    # See test_docs.py if this doesn't work
    contains_module = re.match(
        r'''(?xm)\s*
            (\bmodule\s+\w+\s*{) |
            (^.*
                (type|annotation|link|property|constraint)
                \s+(\w+::\w+)\s+
                ({|extending)
            )
        ''',
        sdlstr,
    )
    if contains_module:
        return parser.parse_sdl(sdlstr)
    else:
        return parser.parse_sdl(f'module default {{{sdlstr}}}')
