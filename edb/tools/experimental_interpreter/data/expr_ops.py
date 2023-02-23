
from .data_ops import *
from typing import *


def map_expr(f : Callable[[Expr, int], Optional[Expr]], expr : Expr, level : int = 1) -> Expr :
    """ maps a function over free variables and bound variables, 
    and does not modify other nodes

    f : called with current expression and the current level, which refers to the first binder 
        outside of the entire expression expr
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
            case RefVal(_):
                return expr
            case BindingExpr(body=body):
                return BindingExpr(body=map_expr(f, body, level+1)) # type: ignore[has-type]
            case UnnamedTupleExpr(val=val):
                return UnnamedTupleExpr(val=[recur(e) for e in val])
            case NamedTupleExpr(val=val):
                return NamedTupleExpr(val={k : recur(e) for (k,e) in val.items()})
            case ObjectProjExpr(subject=subject, label=label):
                return ObjectProjExpr(subject=recur(subject), label=label)
            case BackLinkExpr(subject=subject, label=label):
                return BackLinkExpr(subject=recur(subject), label=label)
            case TpIntersectExpr(subject=subject, tp=tp_name):
                return TpIntersectExpr(subject=recur(subject), tp=tp_name)
            case LinkPropProjExpr(subject=subject, linkprop=label):
                return LinkPropProjExpr(subject=recur(subject), linkprop=label)
            case FunAppExpr(fun=fname, args=args, overloading_index = idx):
                return FunAppExpr(fun=fname, args=[recur(arg) for arg in args], overloading_index=idx)
            case FilterOrderExpr(subject=subject, filter=filter, order=order):
                return FilterOrderExpr(subject=recur(subject), filter=recur(filter), order=recur(order)) 
            case ShapedExprExpr(expr=expr, shape=shape):
                return ShapedExprExpr(expr=recur(expr), shape=recur(shape))
            case ShapeExpr(shape=shape):
                return ShapeExpr(shape={k : recur(e_1) for (k, e_1) in shape.items()})
            case TypeCastExpr(tp=tp, arg=arg):
                return TypeCastExpr(tp=tp, arg=recur(arg))
            case UnionExpr(left=left, right=right):
                return UnionExpr(left=recur(left), right=recur(right))
            case ArrExpr(elems=arr):
                return ArrExpr(elems=[recur(e) for e in arr])
            case MultiSetExpr(expr=arr):
                return MultiSetExpr(expr=[recur(e) for e in arr])
            case OffsetLimitExpr(subject=subject, offset=offset, limit=limit):
                return OffsetLimitExpr(subject=recur(subject), offset=recur(offset), limit=recur(limit))
            case WithExpr(bound=bound, next=next):
                return WithExpr(bound=recur(bound), next=recur(next))
            case InsertExpr(name=name, new=new):
                return InsertExpr(name=name, new=recur(new))
            case ObjectExpr(val=val):
                return ObjectExpr(val={label : recur(item) for (label, item) in val.items()})
            case DetachedExpr(expr=expr):
                return DetachedExpr(expr=recur(expr))
            case UpdateExpr(subject=subject, shape=shape):
                return UpdateExpr(subject=recur(subject), shape=recur(shape))
            case ForExpr(bound=bound, next=next):
                return ForExpr(bound=recur(bound), next=recur(next))

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
        # print("instantiating ", e, " at level ", level)
        match e:
            case BoundVarExpr(i):
                if i == level:
                    return e2
                else:
                    return BoundVarExpr(i)
                    # if i < level:
                    #     return BoundVarExpr(i-1)
                    # else:
                    #     raise ValueError("Locally named forms do not allow bound variables to reach outside")
            case _:
                return e
    result = map_var(map_func, e.body)
    # print("using", e2, "to instantiate", e, "has resulted in", result)
    return result

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

def subst_expr_for_expr(expr2 : Expr, replace : Expr, subject : Expr):
    def map_func(candidate : Expr, level : int) -> Optional[Expr]:
        if candidate == replace:
            return expr2
        else:
            return None
    return map_expr(map_func, subject)

def iterative_subst_expr_for_expr(expr2 : List[Expr], replace : List[Expr], subject : Expr):
    """ Iteratively perform substitution from right to left, 
        comptues: [expr2[0]/replace[0]]...[expr[n-1]/replace[n-1]]subject """

    assert len(expr2) == len(replace)
    result = subject
    for i in reversed(list(range(len(replace)))):
        result = subst_expr_for_expr(expr2[i], replace[i], result)
    return result



def appears_in_expr(search : Expr, subject : Expr):
    flag = False
    def map_func(candidate : Expr, level : int) -> Optional[Expr]:
        nonlocal flag
        if flag == True:
            return candidate
        if candidate == search:
            flag = True
            return candidate
        else:
            return None
    map_expr(map_func, subject)
    return flag


    
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


    

def get_object_val(val : Val) -> ObjectVal:
    match val:
        case FreeVal(dictval):
            return dictval
        case RefVal(_, dictval):
            return dictval
    raise ValueError("Cannot get object val", val)


def val_is_primitive(rt : Val) -> bool:
    match rt:
        case StrVal(_) | IntVal(_):
            return True
        case RefVal(_) | FreeVal(_):
            return False
    raise ValueError("not implemented")

def val_is_ref_val(rt : Val) -> bool:
    match rt:
        case RefVal(_):
            return True
    return False

def remove_link_props(rt : Val) -> Val:
    match rt:
        case RefVal(refid=id, val=ObjectVal(val=dic)):
            return RefVal(refid=id, val=ObjectVal(val=
                {k : v for (k,v) in dic.items() if isinstance(k, StrLabel)}
            ))
    raise ValueError("Expected RefVal")

def remove_unless_link_props(dic : ObjectVal) -> ObjectVal:
    return ObjectVal(val=
                {k : v for (k,v) in dic.val.items() if isinstance(k, LinkPropLabel)}
            )


def conversion_error():
    class ConversionError(Exception):
        pass
    raise ConversionError

def convert_to_link_prop_obj(dic : ObjectVal) -> ObjectVal:
    return ObjectVal(val=
        {(LinkPropLabel(k.label) if isinstance(k, StrLabel) else conversion_error())
        : v for (k,v) in dic.val.items() }
    )

def convert_back_from_link_prop_obj(dic : ObjectVal) -> ObjectVal:
    return ObjectVal(val=
        {(StrLabel(k.label) if isinstance(k, LinkPropLabel) else conversion_error())
        : v for (k,v) in dic.val.items() }
    )

def combine_object_val(o1 : ObjectVal, o2 : ObjectVal) -> ObjectVal:
    return ObjectVal({**o1.val, **o2.val})


def object_to_shape(expr : ObjectExpr) -> ShapeExpr:
    return ShapeExpr(shape={lbl : abstract_over_expr(e) for (lbl, e) in expr.val.items()})

def make_storage_atomic(val : Val, tp : Tp) -> Val:
    match val:
        case RefVal(id, obj):
            obj_link_prop = remove_unless_link_props(obj)
            match tp:
                case LinkPropTp(subject=tp_sub, linkprop=tp_linkprop):
                    temp_obj = convert_back_from_link_prop_obj(obj_link_prop)
                    after_obj = coerce_to_storage(temp_obj, tp_linkprop)
                    after_link_prop = convert_to_link_prop_obj(after_obj)
                    return RefVal(id, after_link_prop)
                case _:
                    if obj_link_prop.val.keys():
                        raise ValueError("Redundant Link Properties")
                    else:
                        return RefVal(id, ObjectVal({}))
        case _:
            return val



def coerce_to_storage(val : ObjectVal, fmt : ObjectTp) -> ObjectVal:
    # ensure no redundant keys
    extra_keys = [k for k in val.val.keys() if k not in fmt.val.keys()]
    if extra_keys:
        raise ValueError("Coercion failed, object contains redundant keys:", extra_keys, 
        "when coercing ", val, " to ", fmt)
    return ObjectVal(val={
        StrLabel(k) : (Visible(), 
            [make_storage_atomic(v, tp[0]) for v in val.val[StrLabel(k)][1]]
        ) for (k,tp) in fmt.val.items()
    })

