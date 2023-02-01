from typing import *

from edb.edgeql import ast as qlast
from data_ops import *
from edb.schema import pointers as s_pointers
import edb.errors as errors
from functools import singledispatch

from edb.common import debug

def shape_to_expr(node: Shape) -> Expr:
    debug.dump(node)
    raise ValueError("Not Implemented! (to expr)", node)



