

from functools import singledispatch
from typing import Any, Dict, Optional, Sequence, Tuple, cast

from edb import errors
from edb.common import debug, parsing
from edb.edgeql import ast as qlast
from edb.edgeql import qltypes as qltypes
from edb.schema import pointers as s_pointers
from edb.schema.pointers import PointerDirection
from . import interpreter_logging as i_logging

# from .basis.built_ins import all_builtin_funcs, all_std_funcs
from .data import data_ops as e
from .data.data_ops import (
    ArrExpr, ArrTp, BackLinkExpr, BindingExpr, BoolVal, BoundVarExpr,
     DetachedExpr, Expr, FilterOrderExpr, ForExpr, FreeVarExpr,
    FunAppExpr,  IndirectionIndexOp, 
    InsertExpr,  IntVal,  Label, LinkPropLabel,
    LinkPropProjExpr, MultiSetExpr, NamedTupleExpr,
    ObjectProjExpr, OffsetLimitExpr, OptionalForExpr, OrderAscending,
    OrderDescending, OrderLabelSep, ShapedExprExpr, ShapeExpr, StrLabel,
     StrVal, SubqueryExpr, Tp, TpIntersectExpr, TypeCastExpr,
    UnionExpr, UnionTp, UnnamedTupleExpr, UpdateExpr, WithExpr,
    next_name)
from .data.expr_ops import (abstract_over_expr, instantiate_expr, is_path,
                            subst_expr_for_expr)
from .data import expr_ops as eops
# from .shape_ops import shape_to_expr

DEFAULT_HEAD_NAME = "___nchsxx_"
# used as the name for the leading dot notation!
# will always disappear when elaboration finishes


def elab_expr_with_default_head(node: qlast.Expr) -> BindingExpr:
    return abstract_over_expr(elab(node), DEFAULT_HEAD_NAME)


def elab_error(msg: str, ctx: Optional[parsing.ParserContext]) -> Any:
    raise errors.QueryError(msg, context=ctx)


def elab_not_implemented(node: qlast.Base, msg: str = "") -> Any:
    debug.dump(node)
    debug.dump_edgeql(node)
    raise ValueError("Not Implemented!", msg, node)


def elab_Shape(elements: Sequence[qlast.ShapeElement]) -> ShapeExpr:
    """ Convert a concrete syntax shape to object expressions"""
    result = {}
    for se in elements:
        if path_contains_splat(se.expr):
            i_logging.print_warning("Splat is not implemented")
            continue
        match elab_ShapeElement(se):
            case (name, e):
                if name not in result.keys():
                    result = {**result, name: e}
                else:
                    (elab_error("Duplicate Value in Shapes", se.context))
    return ShapeExpr(result)


@singledispatch
def elab(node: qlast.Base) -> Expr:
    return elab_not_implemented(node)

@elab.register(qlast.Introspect)
def elab_Introspect(node: qlast.Introspect) -> Expr:
    i_logging.print_warning("Introspect is not implemented")
    return e.StrVal("Introspect is not implemented")

@elab.register(qlast.IsOp)
def elab_TpIntersect(oper: qlast.IsOp) -> TpIntersectExpr:
    assert oper.op == 'IS'
    if isinstance(oper.right, qlast.TypeName):
        right = elab_TypeName(oper.right)
        # if not isinstance(right, e.UncheckedTypeName):
        #     raise ValueError("Expecting a type name here", right, oper.right)
    else:
        raise ValueError("Expecting a type name here")
    left = elab(oper.left)
    return TpIntersectExpr(left, right)


@elab.register(qlast.Path)
def elab_Path(p: qlast.Path) -> Expr:
    result: Expr = None  # type: ignore[assignment]
    if p.partial:
        result = FreeVarExpr(DEFAULT_HEAD_NAME)
    for step in p.steps:
        match step:
            case qlast.ObjectRef(name=name):
                if result is None:
                    result = FreeVarExpr(var=name)
                else:
                    raise ValueError("Unexpected ObjectRef in Path")
            case qlast.Ptr(name=path_name,
                           direction=PointerDirection.Outbound, type=ptr_type):
                if result is None:
                    raise ValueError("should not be")
                else:
                    if ptr_type == 'property':
                        result = LinkPropProjExpr(result, path_name)
                    else:
                        result = ObjectProjExpr(result, path_name)
            case qlast.Ptr(name=path_name,
                           direction=PointerDirection.Inbound, type=None):
                if result is None:
                    raise ValueError("should not be")
                else:
                    result = BackLinkExpr(result, path_name)
            case qlast.TypeIntersection(type=tp):
                if result is None:
                    raise ValueError("should not be")
                else:
                    match elab_single_type_expr(tp):
                        case e.UncheckedTypeName(name=tp_name):
                            result = TpIntersectExpr(result, tp_name)
                        case _:
                            raise ValueError(
                                "expecting single type name here")
            case _:
                if result is None:
                    result = elab(step)
                else:
                    elab_not_implemented(step, "in path")
    return result

def path_contains_splat(p: qlast.Path) -> bool:
    for step in p.steps:
        if isinstance(step, qlast.Splat):
                return True
    return False

def elab_label(p: qlast.Path) -> Label:
    """ Elaborates a single name e.g. in the left hand side of a shape """
    steps = [*p.steps]
    while steps[0] is not None and isinstance(steps[0], qlast.TypeIntersection):
        steps = steps[1:]
    match steps[0]:
        case qlast.Ptr(
                name=pname,
                direction=s_pointers.PointerDirection.Outbound):
            return StrLabel(pname)
        case qlast.Ptr(name=pname, type='property'):
            return LinkPropLabel(pname)
        case _:
            return elab_not_implemented(p, "label")


@elab.register(qlast.ShapeElement)
def elab_ShapeElement(s: qlast.ShapeElement) -> Tuple[Label, BindingExpr]:
    def default_post_processing(x):
        return x
    post_processing = default_post_processing

    if s.orderby or s.where:
        def process(e: BindingExpr) -> BindingExpr:
            return abstract_over_expr(
                FilterOrderExpr(
                    subject=instantiate_expr(
                        FreeVarExpr(DEFAULT_HEAD_NAME),
                        e),
                    filter=elab_where(s.where),
                    order=elab_orderby(s.orderby)),
                DEFAULT_HEAD_NAME)
        post_processing = process

    if s.compexpr is not None:
        # l := E -> l := ¶.e if E -> e
        if s.operation.op != qlast.ShapeOp.ASSIGN:
            return elab_not_implemented(s)
        else:
            name = elab_label(s.expr)
            val = abstract_over_expr(elab(s.compexpr), DEFAULT_HEAD_NAME)
            return (name, post_processing(val))
    elif s.elements:
        # l : S -> l := x. (x ⋅ l) s if S -> s
        name = elab_label(s.expr)
        match name:
            case StrLabel(_):
                var = next_name()
                return (name, post_processing(
                    BindingExpr(
                        var=var,
                        body=ShapedExprExpr(
                            ObjectProjExpr(BoundVarExpr(var), name.label),
                            elab_Shape(s.elements)))))
            case _:
                return elab_not_implemented(s, "link property with shapes")
    else:
        # l -> l := x. (x ⋅ l)
        name = elab_label(s.expr)
        match name:
            case StrLabel(_):
                var = next_name()
                return (name, post_processing(
                    BindingExpr(
                        var=var,
                        body=ObjectProjExpr(BoundVarExpr(var), name.label))))
            case LinkPropLabel(_):
                var = next_name()
                return (name, post_processing(
                        BindingExpr(
                            var=var,
                            body=LinkPropProjExpr(
                                BoundVarExpr(var), name.label))
                        ))
            case _:
                return elab_not_implemented(s)


@elab.register(qlast.Shape)
def elab_ShapedExpr(shape: qlast.Shape) -> ShapedExprExpr:
    if shape.expr is None:
        return ShapedExprExpr(
            expr=e.FreeObjectExpr(),
            shape=elab_Shape(shape.elements))
    else:
        return ShapedExprExpr(
            expr=elab(shape.expr),
            shape=elab_Shape(shape.elements))


@elab.register(qlast.InsertQuery)
def elab_InsertQuery(expr: qlast.InsertQuery) -> InsertExpr:
    # debug.dump(expr)
    subject_type = expr.subject.name
    object_shape = elab_Shape(expr.shape)
    # object_expr = shape_to_expr(object_shape)
    unshaped = {}
    for (k,v) in object_shape.shape.items():
        assert isinstance(k, StrLabel), "Expecting Plain Labels"
        assert eops.binding_is_unnamed(v), "Not expecting leading dot notaiton in Shapes"
        unshaped[k.label] = v.body
    
    return cast(
        InsertExpr,
        elab_aliases(
            expr.aliases,
            InsertExpr(name=e.UnqualifiedName(subject_type), new=unshaped))) #TODO: we should allow qualified names here


@elab.register(qlast.StringConstant)
def elab_StringConstant(e: qlast.StringConstant) -> StrVal:
    return StrVal(val=e.value)


@elab.register(qlast.IntegerConstant)
def elab_IntegerConstant(e: qlast.IntegerConstant) -> IntVal:
    abs_val = int(e.value)
    if e.is_negative:
        abs_val = -abs_val
    return IntVal(val=abs_val)


@elab.register(qlast.FloatConstant)
def elab_FloatConstant(expr: qlast.FloatConstant) -> e.ScalarVal:
    abs_val = float(expr.value)
    if expr.is_negative:
        abs_val = -abs_val
    return e.ScalarVal(tp=e.ScalarTp(e.QualifiedName(["std", "float64"])), val=abs_val)

@elab.register(qlast.BooleanConstant)
def elab_BooleanConstant(e: qlast.BooleanConstant) -> BoolVal:
    match e.value:
        case "True" | "true":
            return BoolVal(val=True)
        case "False" | "false":
            return BoolVal(val=False)
        case _:
            raise ValueError("Unknown Bool Value", e)


def elab_where(where: Optional[qlast.Expr]) -> BindingExpr:
    if where is None:
        return abstract_over_expr(BoolVal(True))
    else:
        return abstract_over_expr(elab(where), DEFAULT_HEAD_NAME)


def elab_orderby(qle: Optional[Sequence[qlast.SortExpr]]) -> Dict[str, BindingExpr]:
    if qle is None:
        # return abstract_over_expr(ObjectExpr({}))
        return {}
    result: Dict[str, Expr] = {}
    for (idx, sort_expr) in enumerate(qle):
        # if sort_expr.nones_order is not None:
        #     raise elab_not_implemented(sort_expr)

        empty_label = (e.OrderEmptyFirst if sort_expr.nones_order == qlast.NonesOrder.First or sort_expr.nones_order is None
                          else e.OrderEmptyLast if sort_expr.nones_order == qlast.NonesOrder.Last
                          else elab_error("unknown nones order", sort_expr.context))


        direction_label = (OrderAscending
             if sort_expr.direction == qlast.SortOrder.Asc else
             OrderDescending
             if sort_expr.direction == qlast.SortOrder.Desc else
             elab_error("unknown direction", sort_expr.context))

        key = (
            str(idx) + OrderLabelSep + direction_label + OrderLabelSep + empty_label
            )
        elabed_expr = elab(sort_expr.path)
        result = {**result, key: elabed_expr}

    return {l: abstract_over_expr(v, DEFAULT_HEAD_NAME) for (l, v) in result.items()}


@elab.register(qlast.SelectQuery)
def elab_SelectExpr(qle: qlast.SelectQuery) -> Expr:
    if qle.offset is not None or qle.limit is not None:
        return elab_aliases(
            qle.aliases,
            SubqueryExpr(
                OffsetLimitExpr(
                    subject=elab(qle.result),
                    offset=elab(qle.offset)
                    if qle.offset is not None else IntVal(0),
                    limit=elab(qle.limit)
                    if qle.limit is not None else IntInfVal(),)))
    else:
        subject_elab = elab(qle.result)
        # if isinstance(subject_elab, FreeVarExpr):
        #     subject_elab = e.ShapedExprExpr(expr=subject_elab, shape=ShapeExpr({})) # if selecting only a variable, we need to add an empty shape to shadow the 
        filter_elab = elab_where(qle.where)
        order_elab = elab_orderby(qle.orderby)
        if qle.result_alias is not None:
            # apply and reabstract the result alias
            subject_elab = SubqueryExpr(expr=subject_elab)
            alias_var = FreeVarExpr(qle.result_alias)
            filter_elab = abstract_over_expr(instantiate_expr(
                alias_var, filter_elab), qle.result_alias)
            order_elab = {l: abstract_over_expr(instantiate_expr(
                alias_var, o), qle.result_alias) for (l,o) in order_elab.items()}
        else:
            # abstract over if subject is a path
            # and select does not have an alias
            # Review the design here:
            # https://edgedb.slack.com/archives/C04JG7CR04T/p1677711136147779
            def path_abstraction(subject: Expr) -> None:
                nonlocal filter_elab, order_elab
                match subject:
                    case ShapedExprExpr(expr=e, shape=_):
                        return path_abstraction(e)
                    case _:
                        if is_path(subject):
                            name = next_name()
                            filter_elab = abstract_over_expr(
                                subst_expr_for_expr(
                                    FreeVarExpr(name),
                                    subject,
                                    instantiate_expr(
                                        FreeVarExpr(name),
                                        filter_elab)),
                                name)
                            order_elab = {l : abstract_over_expr(
                                subst_expr_for_expr(
                                    FreeVarExpr(name),
                                    subject,
                                    instantiate_expr(
                                        FreeVarExpr(name),
                                        o)),
                                name) for (l,o) in order_elab.items()}
                        return
            path_abstraction(subject_elab)

        without_alias = SubqueryExpr(FilterOrderExpr(
            subject=subject_elab,
            filter=filter_elab,
            order=order_elab
        ))
        return elab_aliases(qle.aliases, without_alias)


@elab.register(qlast.FunctionCall)
def elab_FunctionCall(fcall: qlast.FunctionCall) -> FunAppExpr:
    if fcall.window:
        return elab_not_implemented(fcall)
    if type(fcall.func) is str:
        fname = e.UnqualifiedName(fcall.func)
    else:
        assert type(fcall.func) is tuple
        fname = e.QualifiedName(list(fcall.func))
            # #     if fcall.func in all_builtin_funcs.keys()
            #     e.QualifiedName(["std", fcall.func])
            #     if ( fcall.func) in all_std_funcs.keys()
            #     else elab_error("unknown function name: " +
            #                     fcall.func, fcall.context))
    args = [elab(arg) for arg in fcall.args]
    kwargs = {k: elab(v) for (k,v) in fcall.kwargs.items()}
    return FunAppExpr(fname, None, args, kwargs)


@elab.register
def elab_UnaryOp(uop: qlast.UnaryOp) -> FunAppExpr:
    # if uop.op in all_builtin_funcs.keys():
    return FunAppExpr(
        fun=e.UnqualifiedName(uop.op), args=[elab(uop.operand)],
        overloading_index=None, kwargs={})
    # else:
    #     raise ValueError("Unknown Op Name", uop.op)


@elab.register(qlast.BinOp)
def elab_BinOp(binop: qlast.BinOp) -> FunAppExpr | UnionExpr:
    if binop.rebalanced:
        return elab_not_implemented(binop)
    left_expr = elab(binop.left)
    right_expr = elab(binop.right)
    if binop.op == "UNION":
        return UnionExpr(left_expr, right_expr)
    else:
        # if binop.op in all_builtin_funcs.keys():
        return FunAppExpr(
            fun=e.UnqualifiedName(binop.op), args=[left_expr, right_expr],
            overloading_index=None, kwargs={})
        # else:
        #     raise ValueError("Unknown Op Name", binop.op)


def elab_param_modifier(mod: qltypes.TypeModifier) -> e.ParamModifier:
    match mod:
        case qltypes.TypeModifier.OptionalType:
            return e.ParamOptional()
        case qltypes.TypeModifier.SingletonType:
            return e.ParamSingleton()
        case qltypes.TypeModifier.SetOfType:
            return e.ParamSetOf()
        case _:
            raise ValueError("Unknown Param Modifier", mod)


def elab_single_type_str(name: str, module_name: Optional[str]) -> Tp:
    match name:
        # case "int64":
        #     return IntTp()
        # case "str":
        #     return StrTp()
        # case "datetime":
        #     return DateTimeTp()
        # case "json":
        #     return JsonTp()
        case _:
            if name.startswith("any") and module_name is None:
                return e.AnyTp(name[3:])
            else:
                if module_name:
                    assert "::" not in module_name
                    return e.UncheckedTypeName(e.QualifiedName([module_name, name]))
                else:
                    return e.UncheckedTypeName(e.UnqualifiedName(name))


def elab_CompositeTp(basetp: qlast.ObjectRef, sub_tps: Sequence[Tp], labels=[]) -> Tp:
    if basetp.name in {k.value for k in e.CompositeTpKind}:
        return e.CompositeTp(kind=e.CompositeTpKind(basetp.name), tps=sub_tps, labels=labels)  
    else:
        raise ValueError("Unknown Composite Type", basetp.name)
@elab.register(qlast.TypeName)
def elab_TypeName(qle: qlast.TypeName) -> Tp:
    # if qle.name:
    #     return elab_not_implemented(qle)
    if qle.dimensions:
        return elab_not_implemented(qle)
        
    basetp = qle.maintype
    if isinstance(basetp, qlast.ObjectRef):
        if basetp.itemclass:
            return elab_not_implemented(qle)
        if qle.subtypes:
            if all(tp_name.name for tp_name in qle.subtypes) and basetp.name == "tuple":
                sub_tps = [elab_single_type_expr(subtype) for subtype in qle.subtypes]
                labels = [tp_name.name for tp_name in qle.subtypes]
                return elab_CompositeTp(basetp, sub_tps, labels)
            else:
                sub_tps = [elab_single_type_expr(subtype) for subtype in qle.subtypes]
                return elab_CompositeTp(basetp, sub_tps)
        return elab_single_type_str(basetp.name, basetp.module)
    elif isinstance(basetp, qlast.PseudoObjectRef):
        if basetp.name.startswith("any"):
            return e.AnyTp(basetp.name[3:])
        else:
            return elab_not_implemented(qle)
    else:
        raise ValueError("Unknown Type Name", qle)

    # raise ValueError("Unrecognized conversion type", basetp.name)
    # return elab_not_implemented(basetp, "unrecognized type " + basetp.name)


def elab_single_type_expr(typedef: qlast.TypeExpr) -> Tp:
    """ elaborates the target type of a
    concrete unknown pointer, i.e. links or properties"""
    if isinstance(typedef, qlast.TypeName):
        return elab_TypeName(typedef)
    else:
        match typedef:
            case qlast.TypeOp(left=left_type, op=op_name, right=right_type):
                if op_name == "|":
                    return UnionTp(
                        left=elab_single_type_expr(left_type),
                        right=elab_single_type_expr(right_type))
                else:
                    raise ValueError("Unknown Type Op")
        raise ValueError("MATCH")


@elab.register(qlast.TypeCast)
def elab_TypeCast(qle: qlast.TypeCast) -> TypeCastExpr:
    if isinstance(qle.expr, qlast.Parameter):
            if qle.cardinality_mod == qlast.CardinalityModifier.Optional:
                is_required = False
            else:
                raise ValueError("Unknown Cardinality Modifier", qle.cardinality_mod)
            return e.ParameterExpr(
                name=qle.expr.name,
                tp= elab_single_type_expr(qle.type),
                is_required=is_required,
            )
    if isinstance(qle.type, qlast.TypeName):
        tp = elab_TypeName(qle.type)
        expr = elab(qle.expr)
        return TypeCastExpr(tp=tp, arg=expr)
    else:
        return elab_not_implemented(qle)


@elab.register(qlast.Array)
def elab_Array(qle: qlast.Array) -> ArrExpr:
    return ArrExpr(elems=[elab(elem) for elem in qle.elements])


@elab.register(qlast.UpdateQuery)
def elab_UpdateQuery(qle: qlast.UpdateQuery):
    subject = FilterOrderExpr(
        subject=elab(qle.subject),
        filter=abstract_over_expr(elab(qle.where),
                                  DEFAULT_HEAD_NAME)
        if qle.where else abstract_over_expr(BoolVal(True)),
        order={},)
    shape = elab_Shape(qle.shape)
    return elab_aliases(
        qle.aliases, UpdateExpr(subject=subject, shape=shape))

@elab.register(qlast.DeleteQuery)
def elab_DeleteQuery(qle: qlast.DeleteQuery):
    subject = FilterOrderExpr(
        subject=elab(qle.subject),
        filter=abstract_over_expr(elab(qle.where),
                                  DEFAULT_HEAD_NAME)
        if qle.where else abstract_over_expr(BoolVal(True)),
        order={},)
    return elab_aliases(
        qle.aliases, e.DeleteExpr(subject=subject))



@elab.register(qlast.Set)
def elab_Set(qle: qlast.Set):
    return MultiSetExpr(expr=[elab(e) for e in qle.elements])


def elab_aliases(
    aliases:
    Optional
    [Sequence[qlast.AliasedExpr | qlast.ModuleAliasDecl]],
        tail_expr: Expr) -> Expr:
    if aliases is None:
        return tail_expr
    result = tail_expr
    for i in reversed(range(len(aliases))):
        cur_alias = aliases[i]
        if isinstance(cur_alias, qlast.AliasedExpr):
            result = WithExpr(elab(cur_alias.expr),
                              abstract_over_expr(result, cur_alias.alias))
        else:
            raise ValueError("Module Aliases")
    return result


@elab.register(qlast.DetachedExpr)
def elab_DetachedExpr(qle: qlast.DetachedExpr):
    if qle.preserve_path_prefix:
        return elab_not_implemented(qle)
    return DetachedExpr(expr=elab(qle.expr))


@elab.register(qlast.NamedTuple)
def elab_NamedTuple(qle: qlast.NamedTuple) -> NamedTupleExpr:
    # raise ValueError("TODO : FIX MYPY below")
    result : Dict[str, Expr] = {}

    for element in qle.elements:
        if element.name.name in result.keys():
            raise elab_error("Duplicate Value in Named Tuple", qle.context)
        result[element.name.name] = elab(element.val)

    return NamedTupleExpr(val=result)


@elab.register(qlast.Tuple)
def elab_UnnamedTuple(qle: qlast.Tuple) -> UnnamedTupleExpr:
    return UnnamedTupleExpr(val=[elab(e) for e in qle.elements])


@elab.register(qlast.ForQuery)
def elab_ForQuery(qle: qlast.ForQuery) -> ForExpr | OptionalForExpr:
    if qle.result_alias:
        raise elab_not_implemented(qle)
    return cast(
        (ForExpr | OptionalForExpr),
        elab_aliases(
            qle.aliases,
            cast(
                Expr,
                (OptionalForExpr
                 if qle.optional else ForExpr)
                (bound=elab(qle.iterator),
                 next=abstract_over_expr(
                     elab(qle.result),
                     qle.iterator_alias)))))


@elab.register
def elab_Indirection(qle: qlast.Indirection) -> FunAppExpr:
    subject = elab(qle.arg)
    match qle.indirection:
        case [qlast.Slice(start=None, stop=None)]:
            raise ValueError("Slice cannot be both empty")
        case [qlast.Slice(start=None, stop=stop)]:
            assert stop is not None  # required for mypy
            return FunAppExpr(
                fun=e.UnqualifiedName(e.IndirectionSliceStopOp),
                args=[subject, elab(stop)],
                overloading_index=None, kwargs={})
        case [qlast.Slice(start=start, stop=None)]:
            assert start is not None  # required for mypy
            return FunAppExpr(
                fun=e.UnqualifiedName(e.IndirectionSliceStartOp),
                args=[subject, elab(start)],
                overloading_index=None, kwargs={})
        case [qlast.Slice(start=start, stop=stop)]:
            assert start is not None  # required for mypy
            assert stop is not None  # required for mypy
            return FunAppExpr(
                fun=e.UnqualifiedName(e.IndirectionSliceStartStopOp),
                args=[subject, elab(start),
                      elab(stop)],
                overloading_index=None, kwargs={})
        case [qlast.Index(index=idx)]:
            return FunAppExpr(fun=e.UnqualifiedName(IndirectionIndexOp),
                              args=[subject, elab(idx)],
                              overloading_index=None,
                              kwargs={})
    raise ValueError("Not yet implemented indirection", qle)


@elab.register
def elab_IfElse(qle: qlast.IfElse) -> e.IfElseExpr:
    return e.IfElseExpr(
        then_branch=elab(qle.if_expr),
        condition=elab(qle.condition),
        else_branch=elab(qle.else_expr))
