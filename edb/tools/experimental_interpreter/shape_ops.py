from typing import *

from edb.edgeql import ast as qlast
from .data.data_ops import *
from .data.expr_ops import *
from edb.schema import pointers as s_pointers
import edb.errors as errors
from functools import singledispatch

from edb.common import debug
from .errors import *

def shape_to_expr(node: ShapeExpr) -> ProdExpr:
    def binding_error():
        raise ElaborationError()

    return ProdExpr({k : s.body 
        for (k, s) in node.shape.items()
        if binding_is_unnamed(s)
        })



