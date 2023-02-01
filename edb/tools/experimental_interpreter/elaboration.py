
from data_ops import *


from functools import singledispatch



from data_ops import *
from helper_funcs import *
import sys
import traceback
from edb.edgeql import ast as qlast
from edb import edgeql

import pprint

from shape_ops import *

from edb.common import debug


@singledispatch
def elab(node: qlast.Base):
    debug.dump(node)
    raise ValueError("Not Implemented!")

@elab.register(qlast.InsertQuery)
def elab_InsertQuery(expr : qlast.InsertQuery):
    debug.dump(expr)
    subject_type = expr.subject.name
    object_expr = to_expr_shape(qlast.Shape(elements=expr.shape))
    return InsertExpr(name=subject_type, new=object_expr)


@elab.register(qlast.StringConstant)
def elab_string_constant(e : qlast.StringConstant) -> Expr: 
    return StrVal(val=e.value)
