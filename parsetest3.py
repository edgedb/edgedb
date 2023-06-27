from typing import *

from edb.edgeql import parser
from edb.edgeql import ast as qlast


def parse(querystr: str) -> qlast.Expr:
    return parser.parse_block(querystr)

with open('edb/lib/_testmode.edgeql') as f:
    file = f.read()
ast = parse(file)
print(ast)