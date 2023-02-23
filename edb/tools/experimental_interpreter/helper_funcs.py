
from edb.edgeql import ast as qlast
from edb.edgeql import Source, parse_block, parser
import json
from typing import *
import uuid
from edb.errors import EdgeQLSyntaxError

class EdbJSONEncoder(json.JSONEncoder):
    def default(self, x: Any) -> Any:
        if isinstance(x, uuid.UUID):
            return str(x)
        return super().default(x)

def parse_ql(querystr: str) -> Sequence[qlast.Expr]:
    def notExpr(e : qlast.Base) -> Any:
        raise EdgeQLSyntaxError("Not an Expression", context=e.context)

    source = Source.from_string(querystr)
    base_statements = parser.parse_block(source) # type : Sequence[Expr]
    statements : Sequence[qlast.Expr] = [s if isinstance(s, qlast.Expr) else notExpr(s)
                    for s in base_statements ]
    # # assert len(statements) == 1
    # # assert isinstance(statements[0], qlast.Expr)
    # return statements[0]
    return statements

def parse_sdl(sdlstr: str) -> qlast.Schema:
    # See test_docs.py if this doesn't work
    return parser.parse_sdl(f'module default {{{sdlstr}}}')