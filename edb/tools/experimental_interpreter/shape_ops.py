from typing import *

from edb.edgeql import ast as qlast
from data_ops import *
from edb.schema import pointers as s_pointers
import edb.errors as errors
from functools import singledispatch

from edb.common import debug

@singledispatch
def to_expr(node: qlast.Base):
    debug.dump(node)
    raise ValueError("Not Implemented! (to expr)", node)
@to_expr.register(qlast.StringConstant)
def to_expr_string_constant(e : qlast.StringConstant) -> Expr: 
    return StrVal(val=e.value)


def to_named_expr_shape_element(e : qlast.ShapeElement) -> Tuple[str, Expr]:
    match e.expr:
        case qlast.Path(steps=[qlast.Ptr(ptr=qlast.ObjectRef(name=pname), direction=s_pointers.PointerDirection.Outbound)]):
            comp = e.compexpr
            return [pname, to_expr(comp)]
            # if isinstance(comp, qlast.Shape):
            #     return [pname, shapeToObjectExpr(comp)]
            # else:
            #     raise ValueError("No Imp", comp)
        case qlast.Path(steps=st):
            raise ValueError(st)

    raise errors.QueryError(
            "mutation queries must specify values with ':='",
            context=e.context,
        )
    
@to_expr.register(qlast.Shape)
def to_expr_shape(shape : qlast.Shape) -> Expr:
    """ Convert a concrete syntax shape to object expressions"""
    result = BinProdUnitExpr()
    if shape.expr is not None:
        raise ValueError("Not implemented")
    elements = shape.elements
    [result := BinProdExpr(name, e, result )
    for se in elements
    for (name, e) in [to_named_expr_shape_element(se)]]
    return result
