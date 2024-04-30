from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, cast

import itertools

from .data.data_ops import (
    ArrExpr,
    ArrVal,
    BackLinkExpr,
    BoolVal,
    DetachedExpr,
    Expr,
    FilterOrderExpr,
    ForExpr,
    FreeVarExpr,
    FunAppExpr,
    InsertExpr,
    Invisible,
    Label,
    LinkPropLabel,
    LinkPropProjExpr,
    Marker,
    MultiSetExpr,
    MultiSetVal,
    NamedTupleExpr,
    NamedTupleVal,
    ObjectProjExpr,
    ObjectVal,
    OffsetLimitExpr,
    OptionalForExpr,
    OrderAscending,
    OrderDescending,
    OrderLabelSep,
    ParamOptional,
    ParamSetOf,
    ParamSingleton,
    RefVal,
    ShapedExprExpr,
    ShapeExpr,
    StrLabel,
    StrVal,
    SubqueryExpr,
    TpIntersectExpr,
    UnionExpr,
    UnnamedTupleExpr,
    UnnamedTupleVal,
    UpdateExpr,
    Val,
    Visible,
    WithExpr,
    next_id,
)
from .data import data_ops as e
from .data import expr_ops as eops
from .data import type_ops as tops
from .data import module_ops as mops
from .data.expr_ops import (
    instantiate_expr,
    map_expand_multiset_val,
    val_is_ref_val,
)
from .data.type_ops import is_nominal_subtype_in_schema
from .db_interface import EdgeDatabase
from .evaluation_tools.storage_coercion import coerce_to_storage
from .interpreter_logging import print_warning


def get_param_reserved_name(param: str | int) -> str:
    return f"__edgedb_reserved_param_name_{param}_"


def eval_error(expr: Val | Expr | Sequence[Val], msg: str = "") -> Any:
    raise ValueError("Eval Error", msg, expr)


def eval_order_by(
    after_condition: Sequence[Val], orders: Sequence[Dict[str, Val]]
) -> Sequence[Val]:
    if len(after_condition) == 0:
        return after_condition
    if len(orders) == 0:
        return after_condition

    keys = [k for k in orders[0].keys()]
    if len(keys) == 0:
        return after_condition
    sort_specs = sorted(
        [
            (int(idx), spec, empty_order)
            for k in keys
            for [idx, spec, empty_order] in [k.split(OrderLabelSep)]
        ]
    )

    result: Sequence[Tuple[int, Val]] = list(enumerate(after_condition))
    # use reversed to achieve the desired effect
    for idx, spec, empty_order in reversed(sort_specs):

        def key_extract(
            elem: Tuple[int, Val], idx=idx, spec=spec, empty_order=empty_order
        ):
            order_elem = orders[elem[0]][
                (str(idx) + OrderLabelSep + spec + OrderLabelSep + empty_order)
            ]
            if empty_order == e.OrderEmptyLast:
                assert isinstance(order_elem, MultiSetVal)
                return (len(order_elem.getVals()) == 0, order_elem)
            else:
                return order_elem

        result = sorted(
            result,
            key=key_extract,
            reverse=(
                False
                if spec == OrderAscending
                else (
                    True
                    if spec == OrderDescending
                    else eval_error(
                        cast(Sequence[Val], orders), "unknown spec"
                    )
                )
            ),
        )

    return [elem for (_, elem) in result]


EvalEnv = Dict[str, MultiSetVal]


def ctx_extend(
    ctx: EvalEnv, bnd: e.BindingExpr, val: MultiSetVal
) -> Tuple[EvalEnv, Expr]:
    assert isinstance(val, MultiSetVal), "Expecting MultiSetVal"
    bnd_no_capture = eops.ensure_no_capture(list(ctx.keys()), bnd)
    return {**ctx, bnd_no_capture.var: val}, instantiate_expr(
        e.FreeVarExpr(bnd_no_capture.var), bnd_no_capture
    )


def apply_shape(
    ctx: EvalEnv, db: EdgeDatabase, shape: ShapeExpr, value: Val
) -> Val:
    def apply_shape_to_prodval(
        shape: ShapeExpr, objectval: ObjectVal
    ) -> ObjectVal:
        result: Dict[Label, Tuple[Marker, MultiSetVal]] = {}
        for key, (_, pval) in objectval.val.items():
            if key not in shape.shape.keys():
                result = {**result, key: (Invisible(), (pval))}
            else:
                pass
        for key, shape_elem in shape.shape.items():
            new_ctx, shape_body = ctx_extend(
                ctx, shape_elem, e.ResultMultiSetVal([value])
            )
            new_val: MultiSetVal = eval_expr(new_ctx, db, shape_body)
            result = {**result, key: (Visible(), (new_val))}

        return ObjectVal(result)

    match value:
        case RefVal(refid=id, tpname=tpname, val=dictval):
            return RefVal(
                refid=id,
                tpname=tpname,
                val=apply_shape_to_prodval(shape, dictval),
            )
        case _:
            return eval_error(value, "Cannot apply shape to value")


def eval_expr_list(
    ctx: EvalEnv, db: EdgeDatabase, exprs: Sequence[Expr]
) -> Sequence[MultiSetVal]:
    result: Sequence[MultiSetVal] = []
    for expr in exprs:
        val = eval_expr(ctx, db, expr)
        result = [*result, val]
    return result


# not sure why the semantics says to produce empty set when label not present


def singular_proj(
    ctx: EvalEnv, db: EdgeDatabase, subject: Val, label: Label
) -> MultiSetVal:
    match subject:
        case RefVal(refid=id, tpname=tpname, val=objVal):
            if label in objVal.val.keys():
                return objVal.val[label][1]
            elif isinstance(label, StrLabel):
                label_str = label.label
                if label_str == "id":
                    return e.ResultMultiSetVal([e.UuidVal(id)])
                elif label_str == "__type__":
                    print_warning(
                        "Introspection is not properly supported yet"
                    )
                    return e.ResultMultiSetVal(
                        [
                            e.RefVal(
                                next_id(),
                                e.QualifiedName(["schema", "ObjectType"]),
                                e.ObjectVal(
                                    {
                                        e.StrLabel("name"): (
                                            e.Visible(),
                                            e.MultiSetVal(
                                                [
                                                    e.StrVal(
                                                        "::".join(tpname.names)
                                                    )
                                                ]
                                            ),
                                        ),
                                    }
                                ),
                            )
                        ]
                    )
                else:
                    return db.project(id, tpname, label_str)
            else:
                raise ValueError("Label not found", label)
        case NamedTupleVal(val=dic):
            match label:
                case StrLabel(l):
                    if l in dic.keys():
                        return e.ResultMultiSetVal([dic[l]])
                    else:
                        if l.isdigit() and int(l) < len(dic.keys()):
                            return e.ResultMultiSetVal(
                                [dic[list(dic.keys())[int(l)]]]
                            )
                        else:
                            raise ValueError("key DNE")
            raise ValueError("Label not Str")
        case UnnamedTupleVal(val=arr):
            match label:
                case StrLabel(l):
                    if l.isdigit() and int(l) < len(arr):
                        return e.ResultMultiSetVal([arr[int(l)]])
                    else:
                        raise ValueError("key DNE")
            raise ValueError("Label not Str")
    raise ValueError("Cannot project, unknown subject", subject)


def offset_vals(val: Sequence[Val], offset: Val):
    match offset:
        case e.ScalarVal(_, v):
            if v < 0:
                raise ValueError("OFFSET must not be negative")
            return val[v:]
        case _:
            raise ValueError("offset must be an int")


def limit_vals(val: Sequence[Val], limit: Val) -> Sequence[Val]:
    match limit:
        case e.ScalarVal(_, v):
            if v < 0:
                raise ValueError("LIMIT must not be negative")
            return val[:v]
        case _:
            raise ValueError("offset must be an int")


def make_invisible(val: MultiSetVal) -> MultiSetVal:
    result: List[Val] = []
    for v in val.getVals():
        match v:
            case RefVal(refid=id, tpname=tpname, val=dictval):
                result = [
                    *result,
                    RefVal(
                        refid=id,
                        tpname=tpname,
                        val=ObjectVal(
                            {
                                k: (Invisible(), v)
                                for k, (_, v) in dictval.val.items()
                            }
                        ),
                    ),
                ]
            case _:
                result = [*result, v]
    return e.ResultMultiSetVal(result)


class EvaluationLogsWrapper:
    def __init__(self):
        self.original_eval_expr = None
        self.reset_logs(None)

    def reset_logs(self, logs: Optional[List[Any]]):
        self.logs = logs
        self.indexes: List[int] = []

    def __call__(
        self, eval_expr: Callable[[EvalEnv, EdgeDatabase, Expr], MultiSetVal]
    ):
        self.original_eval_expr = eval_expr

        def wrapper(ctx: EvalEnv, db: EdgeDatabase, expr: Expr) -> MultiSetVal:
            if self.logs is None:
                return self.original_eval_expr(ctx, db, expr)
            else:
                parent = self.logs
                [parent := parent[i] for i in self.indexes]
                self.indexes.append(len(parent))
                parent.append(
                    [(expr, e.ResultMultiSetVal([StrVal("NOT AVAILABLE!!!")]))]
                )
                rt_val = self.original_eval_expr(ctx, db, expr)
                parent[self.indexes[-1]][0] = (
                    parent[self.indexes[-1]][0][0],
                    rt_val,
                )
                assert len(parent[self.indexes[-1]][0]) == 2
                self.indexes.pop()
                if not isinstance(rt_val, e.MultiSetVal):
                    raise ValueError(
                        "Evaluation should always return MultiSetVal"
                    )
                return rt_val

        return wrapper


eval_logs_wrapper = EvaluationLogsWrapper()


def do_conditional_dedup(val: MultiSetVal) -> MultiSetVal:
    if all(val_is_ref_val(v) for v in val.getVals()):
        return e.ResultMultiSetVal(eops.object_dedup(val.getVals()))
    return val


# the database is a mutable reference that keeps track of a read snapshot inside
@eval_logs_wrapper
def eval_expr(ctx: EvalEnv, db: EdgeDatabase, expr: Expr) -> MultiSetVal:
    match expr:
        case e.ScalarVal(_):
            return e.ResultMultiSetVal([expr])
        case e.FreeObjectExpr():
            return e.ResultMultiSetVal(
                [
                    e.RefVal(
                        next_id(),
                        tpname=e.QualifiedName(["std", "FreeObject"]),
                        val=e.ObjectVal(val={}),
                    )
                ]
            )
        case e.ConditionalDedupExpr(expr=inner):
            inner_val = eval_expr(ctx, db, inner)
            return do_conditional_dedup(inner_val)
        case InsertExpr(tname, arg):
            assert isinstance(
                tname, e.QualifiedName
            ), "Should be updated during tcking"
            id = db.insert(tname, {})
            argv = {k: eval_expr(ctx, db, v) for (k, v) in arg.items()}
            arg_object = ObjectVal(
                {StrLabel(k): (e.Visible(), v) for (k, v) in argv.items()}
            )
            type_def = mops.resolve_type_name(db.storage.get_schema(), tname)
            if isinstance(type_def, e.ObjectTp):
                new_object = coerce_to_storage(
                    arg_object, tops.get_storage_tp(type_def)
                )
                db.update(id, tname, {k: v for k, v in new_object.items()})
                return e.ResultMultiSetVal(
                    [
                        RefVal(
                            id,
                            tname,
                            ObjectVal(
                                {
                                    k: (e.Invisible(), v)
                                    for k, (_, v) in arg_object.val.items()
                                }
                            ),
                        )
                    ]
                )
            else:
                raise ValueError("Cannot insert into scalar types")
            # inserts return empty dict
        case FilterOrderExpr(subject=subject, filter=filter, order=order):
            selected = eval_expr(ctx, db, subject)
            # assume data unchaged throught the evaluation of conditions
            conditions: Sequence[MultiSetVal] = [
                eval_expr(new_ctx, db, filter_body)
                for select_i in selected.getRawVals()
                for new_ctx, filter_body in [
                    ctx_extend(ctx, filter, e.ResultMultiSetVal([select_i]))
                ]
            ]
            after_condition: Sequence[Val] = [
                select_i
                for (select_i, condition) in zip(
                    selected.getRawVals(), conditions
                )
                if BoolVal(True) in condition.getVals()
            ]
            orders: Sequence[Dict[str, Val]] = []
            for after_condition_i in after_condition:
                current: Dict[str, Val] = {}
                for l, o in order.items():
                    new_ctx, o_body = ctx_extend(
                        ctx, o, e.ResultMultiSetVal([after_condition_i])
                    )
                    current = {**current, l: eval_expr(new_ctx, db, o_body)}
                orders = [*orders, current]
            after_order = eval_order_by(after_condition, orders)
            if isinstance(selected, e.ResultMultiSetVal):
                return e.ResultMultiSetVal(after_order)
            else:
                raise ValueError("Not Implemented", selected)
        case ShapedExprExpr(expr=subject, shape=shape):
            subjectv = eval_expr(ctx, db, subject)
            after_shape: Sequence[Val] = [
                apply_shape(ctx, db, shape, v) for v in subjectv.getVals()
            ]
            return e.ResultMultiSetVal(after_shape)
        case FreeVarExpr(var=name):
            if name in ctx.keys():
                # binder needs to be invisible when selected
                return ctx[name]
            else:
                raise ValueError("Variable not found", name)
        case e.QualifiedName(names=names):

            all_ids: Sequence[Val] = [
                RefVal(id, e.QualifiedName(names=names), ObjectVal({}))
                for id in db.storage.query_ids_for_a_type(
                    expr, e.EdgeDatabaseTrueFilter()
                )
            ]
            return e.ResultMultiSetVal(all_ids)
        case e.QualifiedNameWithFilter(name=name, filter=filter):

            def filter_map(filter_expr: Expr) -> Optional[Expr]:
                if isinstance(filter_expr, e.EdgeDatabaseSelectFilter):  # type: ignore
                    return None
                match filter_expr:
                    case e.FreeVarExpr(var=var):
                        return ctx[var]  # type: ignore
                    case e.ScalarVal(_):
                        return e.ResultMultiSetVal([filter_expr])  # type: ignore
                    case _:
                        raise ValueError(
                            "Unrecognized filter expression,"
                            " check post processing: ",
                            filter_expr,
                        )

            filter_val = eops.map_edge_select_filter(filter_map, filter)  # type: ignore
            assert isinstance(filter_val, e.EdgeDatabaseSelectFilter)  # type: ignore
            all_ids = [
                RefVal(id, name, ObjectVal({}))
                for id in db.storage.query_ids_for_a_type(
                    name, filter_val  # type: ignore
                )
            ]
            return e.ResultMultiSetVal(all_ids)

        case FunAppExpr(fun=fname, args=args, overloading_index=idx):
            assert (
                idx is not None
            ), "overloading index must be set in type checking"
            argsv = eval_expr_list(ctx, db, args)
            # argsv = map_assume_link_target(argsv)
            assert isinstance(
                fname, e.QualifiedName
            ), "Should resolve in type checking"
            looked_up_fun = mops.resolve_func_name(db.get_schema(), fname)[idx]
            # db.get_schema().fun_defs[fname]
            f_modifier = looked_up_fun.tp.args_mod
            assert len(f_modifier) == len(argsv)
            argv_final: Sequence[Sequence[Sequence[Val]]] = [[]]
            for i in range(len(f_modifier)):
                mod_i = f_modifier[i]
                argv_i: Sequence[Val] = argsv[i].getVals()
                match mod_i:
                    case ParamSingleton():
                        argv_final = [
                            [*cur, [new]]
                            for cur in argv_final
                            for new in argv_i
                        ]
                    case ParamOptional():
                        if len(argv_i) == 0:
                            argv_final = [[*cur, []] for cur in argv_final]
                        else:
                            argv_final = [
                                [*cur, [new]]
                                for cur in argv_final
                                for new in argv_i
                            ]
                    case ParamSetOf():
                        argv_final = [[*cur, argv_i] for cur in argv_final]
                    case _:
                        raise ValueError()

            after_fun_vals: Sequence[Val]
            if isinstance(looked_up_fun, e.BuiltinFuncDef):
                after_fun_vals = [
                    v for arg in argv_final for v in looked_up_fun.impl(arg)
                ]
            elif isinstance(looked_up_fun, e.DefinedFuncDef):
                after_fun_vals = []
                for vset in argv_final:
                    body = looked_up_fun.impl
                    for farg in vset:
                        assert isinstance(body, e.BindingExpr)
                        ctx, body = ctx_extend(
                            ctx, body, e.ResultMultiSetVal(farg)
                        )
                    after_fun_vals = [
                        *after_fun_vals,
                        *eval_expr(ctx, db, body).getVals(),
                    ]
            else:
                raise ValueError("Not implemented yet", looked_up_fun)
            return e.ResultMultiSetVal(after_fun_vals)
        case e.TupleProjExpr(subject=subject, label=label) | ObjectProjExpr(
            subject=subject, label=label
        ):
            subjectv = eval_expr(ctx, db, subject)
            projected = [
                p
                for v in subjectv.getVals()
                for p in singular_proj(
                    ctx, db, v, StrLabel(label)
                ).getRawVals()
            ]
            return e.ResultMultiSetVal(projected)
        case BackLinkExpr(subject=subject, label=label):
            subjectv = eval_expr(ctx, db, subject)
            # subjectv = assume_link_target(subjectv)
            subject_ids = [
                (
                    v.refid
                    if isinstance(v, RefVal)
                    else eval_error(v, "expecting references")
                )
                for v in subjectv.getVals()
            ]

            return db.storage.reverse_project(subject_ids, label)
        case e.IsTpExpr(subject=subject, tp=tp_name):
            if not isinstance(tp_name, e.QualifiedName):
                raise ValueError("Should be updated during tcking")
            subjectv = eval_expr(ctx, db, subject)
            is_result: List[Val] = []
            for v in subjectv.getVals():
                match v:
                    case RefVal(refid=_, tpname=val_tp, val=_):
                        is_subtype = is_nominal_subtype_in_schema(
                            db.get_schema(), val_tp, tp_name
                        )
                    case e.ScalarVal(tp=e.ScalarTp(s_name), val=_):
                        is_subtype = is_nominal_subtype_in_schema(
                            db.get_schema(), s_name, tp_name
                        )
                    case _:
                        raise ValueError("UnExpected Value Type")
                is_result = [*is_result, e.BoolVal(is_subtype)]
            return e.ResultMultiSetVal(is_result)
        case TpIntersectExpr(subject=subject, tp=tp_name):
            if not isinstance(tp_name, e.QualifiedName):
                raise ValueError("Should be updated during tcking")
            subjectv = eval_expr(ctx, db, subject)
            after_intersect: List[Val] = []
            for v in subjectv.getVals():
                match v:
                    case RefVal(refid=_, tpname=val_tp, val=_):
                        if is_nominal_subtype_in_schema(
                            db.get_schema(), val_tp, tp_name
                        ):
                            after_intersect = [*after_intersect, v]
                    case _:
                        raise ValueError("Expecting References")
            return e.ResultMultiSetVal(after_intersect)
        case e.CheckedTypeCastExpr(cast_tp=_, cast_spec=cast_spec, arg=arg):
            argv2 = eval_expr(ctx, db, arg)
            casted = [cast_spec.cast_fun(v) for v in argv2.getVals()]
            return e.ResultMultiSetVal(casted)
        case UnnamedTupleExpr(val=tuples):
            tuplesv = eval_expr_list(ctx, db, tuples)
            result_list: List[Val] = []
            for prod in itertools.product(*map_expand_multiset_val(tuplesv)):
                result_list.append(UnnamedTupleVal(list(prod)))
            return e.ResultMultiSetVal(result_list)
        case NamedTupleExpr(val=tuples):
            tuplesv = eval_expr_list(ctx, db, list(tuples.values()))
            result_list = []
            for prod in itertools.product(*map_expand_multiset_val(tuplesv)):
                result_list.append(
                    NamedTupleVal(
                        {
                            k: p
                            for (k, p) in zip(tuples.keys(), prod, strict=True)
                        }
                    )
                )
            return e.ResultMultiSetVal(result_list)
        case UnionExpr(left=l, right=r):
            lvals = eval_expr(ctx, db, l)
            rvals = eval_expr(ctx, db, r)
            return e.ResultMultiSetVal([*lvals.getVals(), *rvals.getVals()])
        case ArrExpr(elems=elems):
            elemsv = eval_expr_list(ctx, db, elems)
            arr_result = [
                ArrVal(list(el))
                for el in itertools.product(*map_expand_multiset_val(elemsv))
            ]
            return e.ResultMultiSetVal(arr_result)
        case e.DeleteExpr(subject=subject):
            subjectv = eval_expr(ctx, db, subject)
            if all([val_is_ref_val(v) for v in subjectv.getVals()]):
                delete_ref_ids = [
                    (v.refid, v.tpname) for v in subjectv.getVals()
                ]
                for delete_id, tpname in delete_ref_ids:
                    db.delete(delete_id, tpname)
                return subjectv
            else:
                return eval_error(expr, "expecting all references")
        case UpdateExpr(subject=subject, shape=shape):
            subjectv = eval_expr(ctx, db, subject)
            if all([val_is_ref_val(v) for v in subjectv.getVals()]):
                updated: Sequence[Val] = [
                    apply_shape(ctx, db, shape, v) for v in subjectv.getVals()
                ]  # type: ignore[misc]
                for u in cast(Sequence[RefVal], updated):
                    full_tp = tops.dereference_var_tp(
                        db.get_schema(), u.tpname
                    )
                    cut_tp = {
                        k: v
                        for (k, v) in full_tp.val.items()
                        if StrLabel(k) in u.val.val.keys()
                    }
                    db.update(
                        u.refid,
                        u.tpname,
                        coerce_to_storage(u.val, e.ObjectTp(cut_tp)),
                    )
                return e.ResultMultiSetVal(updated)
            else:
                return eval_error(expr, "expecting all references")
        case MultiSetExpr(expr=elems):
            elemsv = eval_expr_list(ctx, db, elems)
            result_list = [e for el in elemsv for e in el.getVals()]
            return e.ResultMultiSetVal(result_list)
        case WithExpr(bound=bound, next=next):
            boundv = eval_expr(ctx, db, bound)
            new_ctx, next_body = ctx_extend(ctx, next, boundv)
            nextv = eval_expr(new_ctx, db, next_body)
            return nextv
        case OffsetLimitExpr(subject=subject, offset=offset, limit=limit):
            subjectv = eval_expr(ctx, db, subject)
            offsetv_m = eval_expr(ctx, db, offset)
            assert len(offsetv_m.getVals()) <= 1
            offsetv = (
                offsetv_m.getVals()[0]
                if len(offsetv_m.getVals()) == 1
                else e.IntVal(0)
            )
            limitv_m = eval_expr(ctx, db, limit)
            offseted_result = offset_vals(subjectv.getVals(), offsetv)
            assert len(limitv_m.getVals()) <= 1
            if len(limitv_m.getVals()) == 1:
                limitv = limitv_m.getVals()[0]
                result_list = list(limit_vals(offseted_result, limitv))
            else:
                result_list = offseted_result
            return e.ResultMultiSetVal(result_list)
        case SubqueryExpr(expr=expr):
            exprv = eval_expr(ctx, db, expr)
            return exprv
        case DetachedExpr(expr=expr):
            exprv = eval_expr(ctx, db, expr)
            return exprv
        case LinkPropProjExpr(subject=subject, linkprop=label):
            subjectv = eval_expr(ctx, db, subject)
            projected = [
                p
                for v in subjectv.getVals()
                for p in singular_proj(
                    ctx, db, v, LinkPropLabel(label)
                ).getVals()
            ]
            return e.ResultMultiSetVal(projected)
        case ForExpr(bound=bound, next=next):
            boundv = eval_expr(ctx, db, bound)
            vv = []
            for v in boundv.getVals():
                new_ctx, next_body = ctx_extend(
                    ctx, next, e.ResultMultiSetVal([v])
                )
                nextv = eval_expr(new_ctx, db, next_body)
                vv.append(nextv)
            result_list = [p for v in vv for p in v.getVals()]
            return e.ResultMultiSetVal(result_list)
        case e.IfElseExpr(
            then_branch=then_branch,
            condition=condition,
            else_branch=else_branch,
        ):
            conditionv = eval_expr(ctx, db, condition)
            vv2 = eval_expr_list(
                ctx,
                db,
                [
                    (
                        then_branch
                        if v == e.BoolVal(True)
                        else (
                            else_branch
                            if v == e.BoolVal(False)
                            else eval_error(
                                condition, "condition must be a boolean"
                            )
                        )
                    )
                    for v in conditionv.getVals()
                ],
            )
            result_list = [p for v in vv2 for p in v.getVals()]
            return e.ResultMultiSetVal(result_list)
        case OptionalForExpr(bound=bound, next=next):
            boundv = eval_expr(ctx, db, bound)
            if boundv.getVals():
                vv = []
                for v in boundv.getVals():
                    new_ctx, next_body = ctx_extend(
                        ctx, next, e.ResultMultiSetVal([v])
                    )
                    nextv = eval_expr(new_ctx, db, next_body)
                    vv.append(nextv)
                result_list = [p for v in vv for p in v.getVals()]
                return e.ResultMultiSetVal(result_list)
            else:
                new_ctx, next_body = ctx_extend(
                    ctx, next, e.ResultMultiSetVal([])
                )
                return eval_expr(new_ctx, db, next_body)
        case e.ParameterExpr(name=name, tp=_, is_required=_):
            param_name = get_param_reserved_name(name)
            if param_name in ctx.keys():
                return ctx[param_name]
            else:
                raise ValueError("Parameter not found", name, param_name)

    raise ValueError("Not Implemented", expr)


def eval_ctx_from_variables(variables) -> EvalEnv:
    def get_prim_param_value(v) -> Val:
        if isinstance(v, str):
            return e.StrVal(v)
        elif isinstance(v, int):
            return e.IntVal(v)
        elif isinstance(v, list):
            return e.ArrVal([get_prim_param_value(vv) for vv in v])
        else:
            raise ValueError("Unimplemented")

    def get_prim_param_multiset_value(v) -> MultiSetVal:
        if v is None:
            return e.ResultMultiSetVal([])
        elif isinstance(v, str) or isinstance(v, int):
            return e.ResultMultiSetVal([get_prim_param_value(v)])
        elif isinstance(v, list):
            return e.ResultMultiSetVal([get_prim_param_value(v)])
        else:
            raise ValueError("Unimplemented")

    if isinstance(variables, dict):
        return {
            get_param_reserved_name(k): get_prim_param_multiset_value(v)
            for k, v in variables.items()
        }
    elif isinstance(variables, tuple):
        return {
            (get_param_reserved_name(i)): get_prim_param_multiset_value(v)
            for i, v in enumerate(variables)
        }
    else:
        raise ValueError("")


def eval_expr_toplevel(
    db: EdgeDatabase,
    expr: Expr,
    variables: Optional[Dict[str, Val] | Tuple[Val, ...]] = None,
    logs: Optional[Any] = None,
) -> MultiSetVal:

    # on exception, this is not none
    # assert eval_logs_wrapper.logs is None
    if logs is not None:
        eval_logs_wrapper.reset_logs(logs)

    initial_ctx = eval_ctx_from_variables(variables) if variables else {}

    final_v = eval_expr(initial_ctx, db, expr)
    # commit DML after evaluation
    db.commit_dml()

    # restore the decorator state
    eval_logs_wrapper.reset_logs(None)

    # Do not dedup (see one of the test cases)
    # i.e. should not return assume_link_target(final_v)
    return final_v
