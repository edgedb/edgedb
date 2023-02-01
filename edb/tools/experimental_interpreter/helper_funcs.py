
from edb.edgeql import ast as qlast
from edb import edgeql
import json
from typing import *
import uuid

class EdbJSONEncoder(json.JSONEncoder):
    def default(self, x: Any) -> Any:
        if isinstance(x, uuid.UUID):
            return str(x)
        return super().default(x)

def parse(querystr: str) -> List[qlast.Expr]:
    source = edgeql.Source.from_string(querystr)
    statements = edgeql.parse_block(source) # type : List[Expr]
    # # assert len(statements) == 1
    # # assert isinstance(statements[0], qlast.Expr)
    # return statements[0]
    return statements
