
from edb.edgeql import ast as qlast
from edb import edgeql
import json
from typing import *
import uuid
from edb.errors import EdgeQLSyntaxError

class EdbJSONEncoder(json.JSONEncoder):
    def default(self, x: Any) -> Any:
        if isinstance(x, uuid.UUID):
            return str(x)
        return super().default(x)

def parse(querystr: str) -> List[qlast.Expr]:
    def notExpr(e : qlast.Base) -> Any:
        raise EdgeQLSyntaxError("Not an Expression", context=e.context)

    source = edgeql.Source.from_string(querystr)
    base_statements = edgeql.parse_block(source) # type : List[Expr]
    statements : List[qlast.Expr] = [s if isinstance(s, qlast.Expr) else notExpr(s)
                    for s in base_statements ]
    # # assert len(statements) == 1
    # # assert isinstance(statements[0], qlast.Expr)
    # return statements[0]
    return statements
