from functools import singledispatch
from typing import Any, Dict, Optional, Sequence, Tuple, cast, List

from edb import errors

# import edb as edgedb
# import edgedb
from edb.common import debug
from edb.edgeql import ast as qlast
from edb.edgeql import qltypes as qltypes
from edb.schema import pointers as s_pointers
from edb.schema.pointers import PointerDirection
from . import interpreter_logging as i_logging

from .data import data_ops as e
from .data.data_ops import (
    ArrExpr,
    BackLinkExpr,
    BindingExpr,
    BoolVal,
    BoundVarExpr,
    DetachedExpr,
    Expr,
    FilterOrderExpr,
    ForExpr,
    FreeVarExpr,
    FunAppExpr,
    IndirectionIndexOp,
    InsertExpr,
    IntVal,
    Label,
    LinkPropLabel,
    LinkPropProjExpr,
    MultiSetExpr,
    NamedTupleExpr,
    ObjectProjExpr,
    OffsetLimitExpr,
    OptionalForExpr,
    OrderAscending,
    OrderDescending,
    OrderLabelSep,
    ShapedExprExpr,
    ShapeExpr,
    StrLabel,
    SubqueryExpr,
    Tp,
    TpIntersectExpr,
    TypeCastExpr,
    UnionExpr,
    UnionTp,
    UnnamedTupleExpr,
    UpdateExpr,
    WithExpr,
    next_name,
)
from .data.expr_ops import (
    abstract_over_expr,
    instantiate_expr,
    is_path,
    subst_expr_for_expr,
)
from .data import expr_ops as eops

DEFAULT_HEAD_NAME = "___nchsxx_"
# used as the name for the leading dot notation!
# will always disappear when elaboration finishes


def elab_expr_with_default_head(node: qlast.Expr) -> BindingExpr:
    return abstract_over_expr(elab(node), DEFAULT_HEAD_NAME)


def elab_error(msg: str, ctx: Optional[qlast.Span]) -> Any:
    raise errors.QueryError(msg, span=ctx)


def elab_not_implemented(node: qlast.Base, msg: str = "") -> Any:
    debug.dump(node)
    debug.dump_edgeql(node)
    raise ValueError("Not Implemented!", msg, node)


def elab_Shape(elements: Sequence[qlast.ShapeElement]) -> ShapeExpr:
    """Convert a concrete syntax shape to object expressions"""
    result: Dict[e.Label, e.BindingExpr] = {}
    for se in elements:
        if path_contains_splat(se.expr):
            i_logging.print_warning("Splat is not implemented")
            continue
        match elab_ShapeElement(se):
            case (name, elem):
                if name not in result.keys():
                    result = {**result, name: elem}
                else:
                    (elab_error("Duplicate Value in Shapes", se.span))
    return ShapeExpr(result)


@singledispatch
def elab(node: qlast.Base) -> Expr:
    return elab_not_implemented(node)


@elab.register(qlast.Parameter)
def elab_Parameter(node: qlast.Parameter) -> None:
    raise errors.QueryError("missing a type cast", span=node.span)


@elab.register(qlast.Introspect)
def elab_Introspect(node: qlast.Introspect) -> Expr:
    i_logging.print_warning("Introspect is not implemented")
    return e.StrVal("Introspect is not implemented")


@elab.register(qlast.IsOp)
def elab_IsTp(oper: qlast.IsOp) -> e.IsTpExpr:
    if oper.op != 'IS':
        raise ValueError("Unknown Op Name for IsTp", oper.op)
    if isinstance(oper.right, qlast.TypeName):
        right = elab_TypeName(oper.right)
    else:
        raise ValueError("Expecting a type name here")
    left = elab(oper.left)
    return e.IsTpExpr(left, right)


@elab.register(qlast.Path)
def elab_Path(p: qlast.Path) -> Expr:
    result: Expr = None  # type: ignore[assignment]
    if p.partial:
        result = FreeVarExpr(DEFAULT_HEAD_NAME)
    for step in p.steps:
        match step:
            case qlast.ObjectRef(name=name):
                if result is None:
                    if step.module:
                        result = e.QualifiedName(
                            [*step.module.split("::"), name]
                        )
                    else:
                        result = FreeVarExpr(var=name)
                else:
                    raise ValueError("Unexpected ObjectRef in Path")
            case qlast.Ptr(
                name=path_name,
                direction=PointerDirection.Outbound,
                type=ptr_type,
            ):
                if result is None:
                    raise ValueError("should not be")
                else:
                    if ptr_type == 'property':
                        result = LinkPropProjExpr(result, path_name)
                    else:
                        result = ObjectProjExpr(result, path_name)
            case qlast.Ptr(
                name=path_name, direction=PointerDirection.Inbound, type=None
            ):
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
                            raise ValueError("expecting single type name here")
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
    """Elaborates a single name e.g. in the left hand side of a shape"""
    steps = [*p.steps]
    while steps[0] is not None and isinstance(
        steps[0], qlast.TypeIntersection
    ):
        steps = steps[1:]
    match steps[0]:
        case qlast.Ptr(
            name=pname, direction=s_pointers.PointerDirection.Outbound
        ):
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
                        FreeVarExpr(DEFAULT_HEAD_NAME), e
                    ),
                    filter=elab_where(s.where),
                    order=elab_orderby(s.orderby),
                ),
                DEFAULT_HEAD_NAME,
            )

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
                return (
                    name,
                    post_processing(
                        BindingExpr(
                            var=var,
                            body=ShapedExprExpr(
                                ObjectProjExpr(BoundVarExpr(var), name.label),
                                elab_Shape(s.elements),
                            ),
                        )
                    ),
                )
            case _:
                return elab_not_implemented(s, "link property with shapes")
    else:
        # l -> l := x. (x ⋅ l)
        name = elab_label(s.expr)
        match name:
            case StrLabel(_):
                var = next_name()
                return (
                    name,
                    post_processing(
                        BindingExpr(
                            var=var,
                            body=ObjectProjExpr(BoundVarExpr(var), name.label),
                        )
                    ),
                )
            case LinkPropLabel(_):
                var = next_name()
                return (
                    name,
                    post_processing(
                        BindingExpr(
                            var=var,
                            body=LinkPropProjExpr(
                                BoundVarExpr(var), name.label
                            ),
                        )
                    ),
                )
            case _:
                return elab_not_implemented(s)


@elab.register(qlast.Shape)
def elab_ShapedExpr(shape: qlast.Shape) -> ShapedExprExpr:
    if shape.expr is None:
        return ShapedExprExpr(
            expr=e.FreeObjectExpr(), shape=elab_Shape(shape.elements)
        )
    else:
        return ShapedExprExpr(
            expr=elab(shape.expr), shape=elab_Shape(shape.elements)
        )


@elab.register(qlast.InsertQuery)
def elab_InsertQuery(expr: qlast.InsertQuery) -> InsertExpr:
    subject_type = expr.subject.name
    object_shape = elab_Shape(expr.shape)
    unshaped = {}
    for k, v in object_shape.shape.items():
        if not isinstance(k, StrLabel):
            raise ValueError("Expecting Plain Labels")
        assert eops.binding_is_unnamed(
            v
        ), "Not expecting leading dot notaiton in Shapes"
        unshaped[k.label] = v.body

    return cast(
        InsertExpr,
        elab_aliases(
            expr.aliases,
            InsertExpr(name=e.UnqualifiedName(subject_type), new=unshaped),
        ),
    )  # TODO: we should allow qualified names here


@elab.register(qlast.Constant)
def elab_Constant(expr: qlast.Constant) -> e.ScalarVal:
    match expr.kind:
        case qlast.ConstantKind.STRING:
            return e.StrVal(val=expr.value)
        case qlast.ConstantKind.INTEGER:
            return e.IntVal(val=int(expr.value))
        case qlast.ConstantKind.FLOAT:
            return e.ScalarVal(
                tp=e.ScalarTp(e.QualifiedName(["std", "float64"])),
                val=float(expr.value),
            )
        case qlast.ConstantKind.BOOLEAN:
            match expr.value:
                case "True" | "true":
                    return BoolVal(val=True)
                case "False" | "false":
                    return BoolVal(val=False)
                case _:
                    raise ValueError("Unknown Bool Value", expr)
        case _:
            raise ValueError("Unknown Constant Kind", expr.kind)


def elab_where(where: Optional[qlast.Expr]) -> BindingExpr:
    if where is None:
        return abstract_over_expr(BoolVal(True))
    else:
        return abstract_over_expr(elab(where), DEFAULT_HEAD_NAME)


def elab_orderby(
    qle: Optional[Sequence[qlast.SortExpr]],
) -> Dict[str, BindingExpr]:
    if qle is None:
        return {}
    result: Dict[str, Expr] = {}
    for idx, sort_expr in enumerate(qle):

        empty_label = (
            e.OrderEmptyFirst
            if sort_expr.nones_order == qlast.NonesOrder.First
            or sort_expr.nones_order is None
            else (
                e.OrderEmptyLast
                if sort_expr.nones_order == qlast.NonesOrder.Last
                else elab_error("unknown nones order", sort_expr.span)
            )
        )

        direction_label = (
            OrderAscending
            if sort_expr.direction == qlast.SortOrder.Asc
            else (
                OrderDescending
                if sort_expr.direction == qlast.SortOrder.Desc
                else elab_error("unknown direction", sort_expr.span)
            )
        )

        key = (
            str(idx)
            + OrderLabelSep
            + direction_label
            + OrderLabelSep
            + empty_label
        )
        elabed_expr = elab(sort_expr.path)
        result = {**result, key: elabed_expr}

    return {
        l: abstract_over_expr(v, DEFAULT_HEAD_NAME)
        for (l, v) in result.items()
    }


@elab.register(qlast.SelectQuery)
def elab_SelectExpr(qle: qlast.SelectQuery) -> Expr:
    if qle.offset is not None or qle.limit is not None:
        return elab_aliases(
            qle.aliases,
            SubqueryExpr(
                OffsetLimitExpr(
                    subject=elab(qle.result),
                    offset=(
                        elab(qle.offset)
                        if qle.offset is not None
                        else IntVal(0)
                    ),
                    limit=(
                        elab(qle.limit)
                        if qle.limit is not None
                        else e.MultiSetExpr([])
                    ),
                )
            ),
        )
    else:
        subject_elab = elab(qle.result)
        filter_elab = elab_where(qle.where)
        order_elab = elab_orderby(qle.orderby)
        if qle.result_alias is not None:
            # apply and reabstract the result alias
            subject_elab = SubqueryExpr(expr=subject_elab)
            alias_var = FreeVarExpr(qle.result_alias)
            filter_elab = abstract_over_expr(
                instantiate_expr(alias_var, filter_elab), qle.result_alias
            )
            order_elab = {
                l: abstract_over_expr(
                    instantiate_expr(alias_var, o), qle.result_alias
                )
                for (l, o) in order_elab.items()
            }
        else:

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
                                        FreeVarExpr(name), filter_elab
                                    ),
                                ),
                                name,
                            )
                            order_elab = {
                                l: abstract_over_expr(
                                    subst_expr_for_expr(
                                        FreeVarExpr(name),
                                        subject,
                                        instantiate_expr(FreeVarExpr(name), o),
                                    ),
                                    name,
                                )
                                for (l, o) in order_elab.items()
                            }
                        return

            path_abstraction(subject_elab)

        without_alias = SubqueryExpr(
            FilterOrderExpr(
                subject=subject_elab, filter=filter_elab, order=order_elab
            )
        )
        return elab_aliases(qle.aliases, without_alias)


@elab.register(qlast.FunctionCall)
def elab_FunctionCall(fcall: qlast.FunctionCall) -> FunAppExpr:
    if fcall.window:
        return elab_not_implemented(fcall)
    fname: e.RawName
    if isinstance(fcall.func, str):
        fname = e.UnqualifiedName(fcall.func)
    else:
        assert isinstance(fcall.func, tuple)
        fname = e.QualifiedName(list(fcall.func))
    args = [elab(arg) for arg in fcall.args]
    kwargs = {k: elab(v) for (k, v) in fcall.kwargs.items()}
    return FunAppExpr(fname, None, args, kwargs)


@elab.register
def elab_UnaryOp(uop: qlast.UnaryOp) -> FunAppExpr:
    return FunAppExpr(
        fun=e.UnqualifiedName(uop.op),
        args=[elab(uop.operand)],
        overloading_index=None,
        kwargs={},
    )


@elab.register(qlast.BinOp)
def elab_BinOp(binop: qlast.BinOp) -> FunAppExpr | UnionExpr:
    if binop.rebalanced:
        return elab_not_implemented(binop)
    left_expr = elab(binop.left)
    right_expr = elab(binop.right)
    if binop.op == "UNION":
        return UnionExpr(left_expr, right_expr)
    else:
        return FunAppExpr(
            fun=e.UnqualifiedName(binop.op),
            args=[left_expr, right_expr],
            overloading_index=None,
            kwargs={},
        )


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
    if name.startswith("any") and module_name is None:
        return e.AnyTp(name[3:])
    else:
        if module_name:
            return e.UncheckedTypeName(e.QualifiedName([module_name, name]))
        else:
            return e.UncheckedTypeName(e.UnqualifiedName(name))


def elab_CompositeTp(
    basetp: qlast.ObjectRef,
    sub_tps: List[Tp],
    labels: Optional[List[str]] = None,
) -> Tp:
    if labels is None:
        labels = []

    if basetp.name in {k.value for k in e.CompositeTpKind}:
        return e.CompositeTp(
            kind=e.CompositeTpKind(basetp.name), tps=sub_tps, labels=labels
        )
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
            if (
                all(tp_name.name for tp_name in qle.subtypes)
                and basetp.name == "tuple"
            ):
                sub_tps = [
                    elab_single_type_expr(subtype) for subtype in qle.subtypes
                ]
                labels = [tp_name.name for tp_name in qle.subtypes]
                return elab_CompositeTp(
                    basetp, sub_tps, labels  # type: ignore
                )
            else:
                sub_tps = [
                    elab_single_type_expr(subtype) for subtype in qle.subtypes
                ]
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
    """elaborates the target type of a
    concrete unknown pointer, i.e. links or properties"""
    if isinstance(typedef, qlast.TypeName):
        return elab_TypeName(typedef)
    else:
        match typedef:
            case qlast.TypeOp(left=left_type, op=op_name, right=right_type):
                if op_name == "|":
                    return UnionTp(
                        left=elab_single_type_expr(left_type),
                        right=elab_single_type_expr(right_type),
                    )
                else:
                    raise ValueError("Unknown Type Op")
        raise ValueError("MATCH")


@elab.register(qlast.TypeCast)
def elab_TypeCast(qle: qlast.TypeCast) -> TypeCastExpr | e.ParameterExpr:
    if isinstance(qle.expr, qlast.Parameter):
        if qle.cardinality_mod == qlast.CardinalityModifier.Optional:
            is_required = False
        else:
            is_required = True
        return e.ParameterExpr(
            name=qle.expr.name,
            tp=elab_single_type_expr(qle.type),
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
        filter=(
            abstract_over_expr(elab(qle.where), DEFAULT_HEAD_NAME)
            if qle.where
            else abstract_over_expr(BoolVal(True))
        ),
        order={},
    )
    shape = elab_Shape(qle.shape)
    return elab_aliases(qle.aliases, UpdateExpr(subject=subject, shape=shape))


@elab.register(qlast.DeleteQuery)
def elab_DeleteQuery(qle: qlast.DeleteQuery):
    subject = FilterOrderExpr(
        subject=elab(qle.subject),
        filter=(
            abstract_over_expr(elab(qle.where), DEFAULT_HEAD_NAME)
            if qle.where
            else abstract_over_expr(BoolVal(True))
        ),
        order={},
    )
    return elab_aliases(qle.aliases, e.DeleteExpr(subject=subject))


@elab.register(qlast.Set)
def elab_Set(qle: qlast.Set):
    return MultiSetExpr(expr=[elab(e) for e in qle.elements])


def elab_aliases(
    aliases: Optional[Sequence[qlast.Alias]],
    tail_expr: Expr,
) -> Expr:
    if aliases is None:
        return tail_expr
    result = tail_expr
    for i in reversed(range(len(aliases))):
        cur_alias = aliases[i]
        if isinstance(cur_alias, qlast.AliasedExpr):
            result = WithExpr(
                elab(cur_alias.expr),
                abstract_over_expr(result, cur_alias.alias),
            )
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
    result: Dict[str, Expr] = {}

    for element in qle.elements:
        if element.name.name in result.keys():
            raise elab_error("Duplicate Value in Named Tuple", qle.span)
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
                (OptionalForExpr if qle.optional else ForExpr)(
                    bound=elab(qle.iterator),
                    next=abstract_over_expr(
                        elab(qle.result), qle.iterator_alias
                    ),
                ),
            ),
        ),
    )


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
                overloading_index=None,
                kwargs={},
            )
        case [qlast.Slice(start=start, stop=None)]:
            assert start is not None  # required for mypy
            return FunAppExpr(
                fun=e.UnqualifiedName(e.IndirectionSliceStartOp),
                args=[subject, elab(start)],
                overloading_index=None,
                kwargs={},
            )
        case [qlast.Slice(start=start, stop=stop)]:
            assert start is not None  # required for mypy
            assert stop is not None  # required for mypy
            return FunAppExpr(
                fun=e.UnqualifiedName(e.IndirectionSliceStartStopOp),
                args=[subject, elab(start), elab(stop)],
                overloading_index=None,
                kwargs={},
            )
        case [qlast.Index(index=idx)]:
            return FunAppExpr(
                fun=e.UnqualifiedName(IndirectionIndexOp),
                args=[subject, elab(idx)],
                overloading_index=None,
                kwargs={},
            )
    raise ValueError("Not yet implemented indirection", qle)


@elab.register
def elab_IfElse(qle: qlast.IfElse) -> e.IfElseExpr:
    return e.IfElseExpr(
        then_branch=elab(qle.if_expr),
        condition=elab(qle.condition),
        else_branch=elab(qle.else_expr),
    )
