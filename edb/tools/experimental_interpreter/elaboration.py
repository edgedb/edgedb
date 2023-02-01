


from functools import singledispatch

# import pprint

from .data.data_ops import *
from .data.expr_ops import *
from .helper_funcs import *
import sys
import traceback
from edb.edgeql import ast as qlast
from edb import edgeql
from edb import errors

import pprint

from .shape_ops import *

from edb.common import debug, parsing
from typing import *


def elab_error(msg : str, ctx : Optional[parsing.ParserContext]):
    raise errors.QueryError(msg, context=ctx)


@singledispatch
def elab(node: qlast.Base) -> Expr:
    # debug.dump(node)
    raise ValueError("Not Implemented!", node)

@elab.register(qlast.ShapeElement)
def elab_ShapeElement(e : qlast.ShapeElement) -> Tuple[str, BindingExpr]:
    match e.expr:
        case qlast.Path(steps=[qlast.Ptr(ptr=qlast.ObjectRef(name=pname), direction=s_pointers.PointerDirection.Outbound)]):
            comp = e.compexpr
            if comp is not None:
                return (pname, abstract_over_expr(elab(comp)))
            else: 
                raise ValueError("Not Implemented")
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
    
def elab_Shape(elements : List[qlast.ShapeElement]) -> ShapeExpr:
    """ Convert a concrete syntax shape to object expressions"""
    result = {}
    [result := {**result, name : e } 
        if name not in result.keys() 
        else (elab_error("Duplicate Value in Shapes", se.context))
    for se in elements
    for (name, e) in [elab_ShapeElement(se)]]
    return ShapeExpr(result)

@elab.register(qlast.InsertQuery)
def elab_InsertQuery(expr : qlast.InsertQuery) -> InsertExpr:
    # debug.dump(expr)
    subject_type = expr.subject.name
    object_shape = elab_Shape(expr.shape)
    object_expr = shape_to_expr(object_shape)
    return InsertExpr(name=subject_type, new=object_expr)



@elab.register(qlast.StringConstant)
def elab_string_constant(e : qlast.StringConstant) -> StrVal: 
    return StrVal(val=e.value)



# @to_expr.register(qlast.Shape)
# def to_expr_shape(shape : qlast.Shape) -> Expr:
    
