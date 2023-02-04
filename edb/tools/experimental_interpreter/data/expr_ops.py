
from .data_ops import *
from typing import *


def map_expr(f : Callable[[Expr, int], Optional[Expr]], expr : Expr, level : int = 1) -> Expr :
    """ maps a function over free variables and bound variables, 
    and does not modify other nodes

    level : this value refers to the first binder OUTSIDE of the expression
            being mapped, it should be called with initially = 1.
            Increases as we encounter abstractions
    """
    tentative = f(expr, level)
    if tentative is not None:
        return tentative
    else:
        def recur(expr):
            return map_expr(f, expr, level)
        match expr:
            case FreeVarExpr(_):
                return expr
            case BoundVarExpr(_):
                return expr
            case StrVal(_):
                return expr
            case BoolVal(_):
                return expr
            case IntVal(_):
                return expr
            case BindingExpr(body=body):
                return BindingExpr(body=map_expr(f, body, level+1)) # type: ignore[has-type]
            case UnnamedTupleExpr(val=val):
                return UnnamedTupleExpr(val=[map_expr(f, e, level) for e in val])
            case ProdProjExpr(subject=subject, label=label):
                return ProdProjExpr(subject=map_expr(f, subject, level=level), label=label)
            case FunAppExpr(fun=fname, args=args):
                return FunAppExpr(fun=map_expr(f, fname, level), args=[map_expr(f, arg, level) for arg in args])
            case FilterOrderExpr(subject=subject, filter=filter, order=order):
                return FilterOrderExpr(subject=recur(subject), filter=recur(filter), order=recur(order)) 
            case ShapedExprExpr(expr=expr, shape=shape):
                return ShapedExprExpr(expr=recur(expr), shape=recur(shape))
            case ShapeExpr(shape=shape):
                return ShapeExpr(shape=shape)

    raise ValueError("Not Implemented: map_expr ", expr) 

def map_var(f : Callable[[ VarExpr, int], Expr], expr : Expr) -> Expr :
    """ maps a function over free variables and bound variables, 
    and does not modify other nodes

    level : this value refers to the first binder OUTSIDE of the expression
            being mapped, it should be called with initially = 1.
            Increases as we encounter abstractions
    """
    def map_func(e : Expr, level : int) -> Optional[Expr]:
        match e:
            case FreeVarExpr(_):
                return f(e, level)
            case BoundVarExpr(_):
                return f(e, level)
        return None
    return map_expr(map_func, expr)
    
def instantiate_expr(e2 : Expr, e : BindingExpr) -> Expr:
    def map_func(e : VarExpr, level : int) -> Expr:
        match e:
            case BoundVarExpr(1):
                return e2
            case BoundVarExpr(i):
                return BoundVarExpr(i-1)
            case _:
                return e
    return map_var(map_func, e.body)

def abstract_over_expr(expr : Expr, var : Optional[str] = None) -> BindingExpr :
    """Construct a BindingExpr that binds var"""
    def replace_if_match(inner : VarExpr, level : int) -> Expr:
        match inner:
            case FreeVarExpr(fname):
                if var == fname:
                    return BoundVarExpr(level)
                else:
                    return inner
        return inner

    return BindingExpr(body=map_var(replace_if_match, expr))
    
def binding_is_unnamed(expr : BindingExpr) -> bool:
    class ReturnFalse(Exception):
        pass
    def map_func(e : VarExpr, outer_level : int) -> Expr:
        match e:
            case BoundVarExpr(idx):
                if idx == outer_level:
                    raise ReturnFalse()
                else:
                    return e
        return e
    
    try:
        map_var(map_func, expr.body)
    except ReturnFalse:
        return False
    return True


    

def get_object_val(val : Val) -> ProdVal:
    match val:
        case FreeVal(dictval):
            return dictval
        case RefVal(_, dictval):
            return dictval
    raise ValueError("Cannot get object val", val)

def coerce_to_storage(val : ProdVal, fmt : ProdTp) -> ProdVal:
    print("WARNING: coerce_to_storage not yet implemented")
    return val