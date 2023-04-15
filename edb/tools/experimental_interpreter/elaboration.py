

from functools import singledispatch
from typing import Any, Dict, Optional, Sequence, Tuple, cast

from edb import errors
from edb.common import debug, parsing
from edb.edgeql import ast as qlast
from edb.schema import pointers as s_pointers
from edb.schema.pointers import PointerDirection

from .basis.built_ins import all_builtin_funcs
from .data import data_ops as e
from .data.data_ops import (
    ArrExpr, ArrTp, BackLinkExpr, BindingExpr, BoolVal, BoundVarExpr,
    DateTimeTp, DetachedExpr, Expr, FilterOrderExpr, ForExpr, FreeVarExpr,
    FunAppExpr,  IndirectionIndexOp, IndirectionSliceOp,
    InsertExpr, IntInfVal, IntTp, IntVal, JsonTp, Label, LinkPropLabel,
    LinkPropProjExpr, MultiSetExpr, NamedTupleExpr, ObjectExpr,
    ObjectProjExpr, OffsetLimitExpr, OptionalForExpr, OrderAscending,
    OrderDescending, OrderLabelSep, ShapedExprExpr, ShapeExpr, StrLabel,
    StrTp, StrVal, SubqueryExpr, Tp, TpIntersectExpr, TypeCastExpr,
    UnionExpr, UnionTp, UnnamedTupleExpr, UpdateExpr, VarTp, WithExpr,
    next_name)
from .data.expr_ops import (abstract_over_expr, instantiate_expr, is_path,
                            subst_expr_for_expr)
from .shape_ops import shape_to_expr

DEFAULT_HEAD_NAME = "__no_clash_head_subject__"
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
    [result := {**result, name: e}
        if name not in result.keys()
        else (elab_error("Duplicate Value in Shapes", se.context))
     for se in elements
     for (name, e) in [elab_ShapeElement(se)]]
    return ShapeExpr(result)


@singledispatch
def elab(node: qlast.Base) -> Expr:
    return elab_not_implemented(node)



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
            case qlast.Ptr(ptr=qlast.ObjectRef(name=path_name),
                           direction=PointerDirection.Outbound, type=ptr_type):
                if result is None:
                    raise ValueError("should not be")
                else:
                    if ptr_type == 'property':
                        result = LinkPropProjExpr(result, path_name)
                    else:
                        result = ObjectProjExpr(result, path_name)
            case qlast.Ptr(ptr=qlast.ObjectRef(name=path_name),
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
                        case VarTp(name=tp_name):
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


def elab_label(p: qlast.Path) -> Label:
    """ Elaborates a single name e.g. in the left hand side of a shape """
    match p:
        case qlast.Path(steps=[qlast.Ptr(
                ptr=qlast.ObjectRef(name=pname),
                direction=s_pointers.PointerDirection.Outbound)]):
            return StrLabel(pname)
        case qlast.Path(steps=[qlast.Ptr(ptr=qlast.ObjectRef(name=pname),
                                         type='property')]):
            return LinkPropLabel(pname)
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

    return ShapedExprExpr(
        expr=elab(shape.expr)
        if shape.expr is not None else ObjectExpr({}),
        shape=elab_Shape(shape.elements))


@elab.register(qlast.InsertQuery)
def elab_InsertQuery(expr: qlast.InsertQuery) -> InsertExpr:
    # debug.dump(expr)
    subject_type = expr.subject.name
    object_shape = elab_Shape(expr.shape)
    # object_expr = shape_to_expr(object_shape)
    return cast(
        InsertExpr,
        elab_aliases(
            expr.aliases,
            InsertExpr(name=subject_type, new=object_shape)))


@elab.register(qlast.StringConstant)
def elab_StringConstant(e: qlast.StringConstant) -> StrVal:
    return StrVal(val=e.value)


@elab.register(qlast.IntegerConstant)
def elab_IntegerConstant(e: qlast.IntegerConstant) -> IntVal:
    abs_val = int(e.value)
    if e.is_negative:
        abs_val = -abs_val
    return IntVal(val=abs_val)


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


def elab_orderby(qle: Optional[Sequence[qlast.SortExpr]]) -> BindingExpr:
    if qle is None:
        return abstract_over_expr(ObjectExpr({}))
    result: Dict[str, Expr] = {}
    for (idx, sort_expr) in enumerate(qle):
        if sort_expr.nones_order is not None:
            raise elab_not_implemented(sort_expr)

        key = (
            str(idx) + OrderLabelSep +
            (OrderAscending
             if sort_expr.direction == qlast.SortOrder.Asc else
             OrderDescending
             if sort_expr.direction == qlast.SortOrder.Desc else
             elab_error("unknown direction", sort_expr.context)))
        elabed_expr = elab(sort_expr.path)
        result = {**result, key: elabed_expr}

    return abstract_over_expr(
        ObjectExpr({StrLabel(l): v for (l, v) in result.items()}),
        DEFAULT_HEAD_NAME)


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
        filter_elab = elab_where(qle.where)
        order_elab = elab_orderby(qle.orderby)
        if qle.result_alias is not None:
            # apply and reabstract the result alias
            subject_elab = SubqueryExpr(expr=subject_elab)
            alias_var = FreeVarExpr(qle.result_alias)
            filter_elab = abstract_over_expr(instantiate_expr(
                alias_var, filter_elab), qle.result_alias)
            order_elab = abstract_over_expr(instantiate_expr(
                alias_var, order_elab), qle.result_alias)
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
                            order_elab = abstract_over_expr(
                                subst_expr_for_expr(
                                    FreeVarExpr(name),
                                    subject,
                                    instantiate_expr(
                                        FreeVarExpr(name),
                                        order_elab)),
                                name)
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
    if fcall.window or fcall.kwargs:
        return elab_not_implemented(fcall)
    if type(fcall.func) is not str:
        return elab_not_implemented(fcall)
    fname = (fcall.func
             if fcall.func in all_builtin_funcs.keys()
             else "std::" + fcall.func
             if ("std::" + fcall.func) in all_builtin_funcs.keys()
             else elab_error("unknown function name: " +
                             fcall.func, fcall.context))
    args = [elab(arg) for arg in fcall.args]
    return FunAppExpr(fname, None, args)


@elab.register
def elab_UnaryOp(uop: qlast.UnaryOp) -> FunAppExpr:
    if uop.op in all_builtin_funcs.keys():
        return FunAppExpr(
            fun=uop.op, args=[elab(uop.operand)],
            overloading_index=None)
    else:
        raise ValueError("Unknown Op Name", uop.op)


@elab.register(qlast.BinOp)
def elab_BinOp(binop: qlast.BinOp) -> FunAppExpr | UnionExpr:
    if binop.rebalanced:
        return elab_not_implemented(binop)
    left_expr = elab(binop.left)
    right_expr = elab(binop.right)
    if binop.op == "UNION":
        return UnionExpr(left_expr, right_expr)
    else:
        if binop.op in all_builtin_funcs.keys():
            return FunAppExpr(
                fun=binop.op, args=[left_expr, right_expr],
                overloading_index=None)
        else:
            raise ValueError("Unknown Op Name", binop.op)


def elab_single_type_str(name: str) -> Tp:
    match name:
        case "int64":
            return IntTp()
        case "str":
            return StrTp()
        case "datetime":
            return DateTimeTp()
        case "json":
            return JsonTp()
        case _:
            return VarTp(name)


@elab.register(qlast.TypeName)
def elab_TypeName(qle: qlast.TypeName) -> Tp:
    if qle.name:
        return elab_not_implemented(qle)
    if qle.dimensions:
        return elab_not_implemented(qle)
    basetp: qlast.ObjectRef = cast(qlast.ObjectRef, qle.maintype)
    if basetp.module != "std" and basetp.module:
        return elab_not_implemented(qle)
    if basetp.itemclass:
        return elab_not_implemented(qle)
    if qle.subtypes:
        match (basetp.name, qle.subtypes):
            case ("array", [single_arg]):
                return ArrTp(tp=elab_single_type_expr(single_arg))
        return elab_not_implemented(qle)
    return elab_single_type_str(basetp.name)
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
    if qle.cardinality_mod:
        return elab_not_implemented(qle)
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
        order=abstract_over_expr(ObjectExpr({})),)
    shape = elab_Shape(qle.shape)
    return elab_aliases(
        qle.aliases, UpdateExpr(subject=subject, shape=shape))


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
    return NamedTupleExpr(
        val={(element.name.name
              if (
                  element.name.module is
                  None and element.name.itemclass is None) else
              elab_error("not implemented", qle.context)):
             elab(element.val) for element in qle.elements})


@elab.register(qlast.Tuple)
def elab_UnnamedTuple(qle: qlast.Tuple) -> UnnamedTupleExpr:
    return UnnamedTupleExpr(val=[elab(e) for e in qle.elements])


@elab.register(qlast.ForQuery)
def elab_ForQuery(qle: qlast.ForQuery) -> ForExpr | OptionalForExpr:
    if qle.result_alias:
        raise elab_not_implemented(qle)
    if len(qle.iterator_bindings) != 1:
        raise elab_not_implemented(qle)
    return cast(
        (ForExpr | OptionalForExpr),
        elab_aliases(
            qle.aliases,
            cast(
                Expr,
                (OptionalForExpr
                 if qle.iterator_bindings[0].optional else ForExpr)
                (bound=elab(qle.iterator_bindings[0].iterator),
                 next=abstract_over_expr(
                     elab(qle.result),
                     qle.iterator_bindings[0].iterator_alias)))))


@elab.register
def elab_Indirection(qle: qlast.Indirection) -> FunAppExpr:
    subject = elab(qle.arg)
    match qle.indirection:
        case [qlast.Slice(start=None, stop=None)]:
            raise ValueError("Slice cannot be both empty")
        case [qlast.Slice(start=None, stop=stop)]:
            assert stop is not None  # required for mypy
            return FunAppExpr(
                fun=IndirectionSliceOp,
                args=[subject, IntVal(0),
                      elab(stop)],
                overloading_index=None)
        case [qlast.Slice(start=start, stop=None)]:
            assert start is not None  # required for mypy
            return FunAppExpr(
                fun=IndirectionSliceOp,
                args=[subject, elab(start),
                      IntInfVal()],
                overloading_index=None)
        case [qlast.Slice(start=start, stop=stop)]:
            assert start is not None  # required for mypy
            assert stop is not None  # required for mypy
            return FunAppExpr(
                fun=IndirectionSliceOp,
                args=[subject, elab(start),
                      elab(stop)],
                overloading_index=None)
        case [qlast.Index(index=idx)]:
            return FunAppExpr(fun=IndirectionIndexOp,
                              args=[subject, elab(idx)],
                              overloading_index=None)
    raise ValueError("Not yet implemented indirection", qle)


@elab.register
def elab_IfElse(qle: qlast.IfElse) -> e.IfElseExpr:
    return e.IfElseExpr(
        then_branch=elab(qle.if_expr),
        condition=elab(qle.condition),
        else_branch=elab(qle.else_expr))
