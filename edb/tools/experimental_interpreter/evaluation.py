
import itertools
from dataclasses import dataclass
from typing import *

from .data.casts import type_cast
from .data.data_ops import (
    DB, ArrExpr, ArrVal, BackLinkExpr, BoolVal, DBEntry, DBSchema,
    DetachedExpr, Expr, FilterOrderExpr, ForExpr, FreeVal, FreeVarExpr,
    FunAppExpr, InsertExpr, IntInfVal, IntVal, Invisible, Label,
    LinkPropLabel, LinkPropProjExpr, LinkPropVal, Marker, MultiSetExpr,
    MultiSetVal, NamedTupleExpr, NamedTupleVal, ObjectExpr, ObjectProjExpr,
    ObjectVal, OffsetLimitExpr, OptionalForExpr, OrderAscending,
    OrderDescending, OrderLabelSep, ParamOptional, ParamSetOf,
    ParamSingleton, RefVal, ShapedExprExpr, ShapeExpr, StrLabel, StrVal,
    SubqueryExpr, TpIntersectExpr, TypeCastExpr, UnionExpr,
    UnnamedTupleExpr, UnnamedTupleVal, UpdateExpr, Val, VarTp, Visible,
    WithExpr, next_id)
from .data.expr_ops import (
    assume_link_target, coerce_to_storage, combine_object_val,
    convert_to_link, get_object_val, instantiate_expr,
    map_assume_link_target, val_is_link_convertible, val_is_ref_val)
from .data.type_ops import is_nominal_subtype_in_schema

# RT Stands for Run Time


@dataclass(frozen=True)
class RTData:
    cur_db: DB
    read_snapshots: Sequence[DB]
    schema: DBSchema
    eval_only: bool  # a.k.a. no DML, no effect


class RTExpr(NamedTuple):
    data: RTData
    expr: Expr


class RTVal(NamedTuple):
    data: RTData
    val: MultiSetVal


def make_eval_only(data: RTData) -> RTData:
    return RTData(data.cur_db, data.read_snapshots, data.schema, True)


def eval_error(expr: Val | Expr | Sequence[Val], msg: str = "") -> Any:
    raise ValueError("Eval Error", msg, expr)


def eval_order_by(
        after_condition: Sequence[Val],
        orders: Sequence[ObjectVal]) -> Sequence[Val]:
    if len(after_condition) == 0:
        return after_condition
    if len(orders) == 0:
        return after_condition

    keys = [k.label for k in orders[0].val.keys()]
    if len(keys) == 0:
        return after_condition
    sort_specs = sorted([(int(idx), spec) for k in keys for [
                        idx, spec] in [k.split(OrderLabelSep)]])

    result: Sequence[Tuple[int, Val]] = list(enumerate(after_condition))
    # use reversed to achieve the desired effect
    for (idx, spec) in reversed(sort_specs):
        def key_extract(elem: Tuple[int, Val], idx=idx, spec=spec):
            return orders[elem[0]].val[
                StrLabel(str(idx) + OrderLabelSep + spec)]
        result = sorted(
            result, key=key_extract,
            reverse=(False
                     if spec == OrderAscending else True
                     if spec == OrderDescending else
                     eval_error(
                         cast(Sequence[Val],
                              orders),
                         "unknown spec")))
    return [elem for (_, elem) in result]


def apply_shape(ctx: RTData, shape: ShapeExpr, value: Val) -> Val:
    def apply_shape_to_prodval(
            shape: ShapeExpr, objectval: ObjectVal) -> ObjectVal:
        result: Dict[Label, Tuple[Marker, MultiSetVal]] = {}
        for (key, (_, pval)) in objectval.val.items():
            if key not in shape.shape.keys():
                result = {
                    **result, key: (Invisible(), assume_link_target(pval))}
            else:
                pass
        for (key, shape_elem) in shape.shape.items():
            if key in objectval.val.keys():
                new_val: MultiSetVal = eval_config(
                    RTExpr(make_eval_only(ctx),
                           instantiate_expr(value, shape.shape[key]))).val
                result = {
                    **result, key: (Visible(), assume_link_target(new_val))}
            else:
                new_val = eval_config(RTExpr(make_eval_only(ctx),
                                             instantiate_expr(
                                                 value, shape_elem)
                                             )).val
                result = {
                    **result, key: (Visible(), assume_link_target(new_val))}

        return ObjectVal(result)

    # [value] = assume_link_target([value])
    match value:
        case FreeVal(val=dictval):
            return FreeVal(val=apply_shape_to_prodval(shape, dictval))
        case RefVal(refid=id, val=dictval):
            return RefVal(
                refid=id, val=apply_shape_to_prodval(shape, dictval))
        case LinkPropVal(obj=RefVal(refid=id, val=ObjectVal({})),
                         linkprop=objval):
            return RefVal(
                refid=id, val=apply_shape_to_prodval(shape, objval))
        case _:
            return eval_error(value, "Cannot apply shape to value")


def eval_expr_list(init_data: RTData,
                   exprs: Sequence[Expr]) -> Tuple[RTData,
                                                   Sequence[MultiSetVal]]:
    cur_data = init_data
    result: Sequence[MultiSetVal] = []
    for expr in exprs:
        (cur_data, val) = eval_config(RTExpr(cur_data, expr))
        result = [*result, val]
    return (cur_data, result)

# not sure why the semantics says to produce empty set when label not present


def singular_proj(data: RTData, subject: Val, label: Label) -> MultiSetVal:
    match subject:
        case FreeVal(val=objVal):
            if label in objVal.val.keys():
                return objVal.val[label][1]
            else:
                raise ValueError("Label not found")
        case RefVal(refid=id, val=objVal):
            entry_obj = data.read_snapshots[0].dbdata[id].data
            if label in objVal.val.keys():
                return objVal.val[label][1]
            elif label in entry_obj.val.keys():
                return entry_obj.val[label][1]
            else:
                raise ValueError("Label not found", label)
        case NamedTupleVal(val=dic):
            match label:
                case StrLabel(l):
                    if l in dic.keys():
                        return [dic[l]]
                    else:
                        if l.isdigit() and int(l) < len(dic.keys()):
                            return [dic[list(dic.keys())[int(l)]]]
                        else:
                            raise ValueError("key DNE")
            raise ValueError("Label not Str")
        case UnnamedTupleVal(val=arr):
            match label:
                case StrLabel(l):
                    if l.isdigit() and int(l) < len(arr):
                        return [arr[int(l)]]
                    else:
                        raise ValueError("key DNE")
            raise ValueError("Label not Str")
        case LinkPropVal(obj=obj, linkprop=linkprop):
            match label:
                case LinkPropLabel(_):
                    return singular_proj(
                        data, FreeVal(val=linkprop),
                        label=label)
                case StrLabel(_):
                    return singular_proj(data, obj, label=label)
                case _:
                    raise ValueError(label)
    raise ValueError("Cannot project, unknown subject", subject)


def offset_vals(val: Sequence[Val], offset: Val):
    match offset:
        case IntVal(val=v):
            return val[v:]
        case _:
            raise ValueError("offset must be an int")


def limit_vals(val: Sequence[RTVal], limit: Val):
    match limit:
        case IntVal(val=v):
            return val[:v]
        case IntInfVal():
            return val
        case _:
            raise ValueError("offset must be an int")


def trace_input_output(func):
    def wrapper(rt):
        indent = "| " * wrapper.depth
        print(f"{indent}input: {rt.expr} ")
        wrapper.depth += 1
        result = func(rt)
        wrapper.depth -= 1
        print(f"{indent}output: {result.val}")
        return result
    wrapper.depth = 0
    return wrapper


# @trace_input_output
def eval_config(rt: RTExpr) -> RTVal:
    match rt.expr:
        case (StrVal(_)
              | IntVal(_)
              | BoolVal(_)
              | RefVal(_)
              | ArrVal(_)
              | UnnamedTupleVal(_)
              | FreeVal(_)
              | LinkPropVal(_)
              ):
            return RTVal(rt.data, [rt.expr])
        case ObjectExpr(val=dic):
            cur_data = rt.data
            result: Dict[Label, Tuple[Marker, MultiSetVal]] = {}
            for (key, expr) in dic.items():  # type: ignore[has-type]
                (cur_data, val) = eval_config(RTExpr(cur_data, expr))
                result = {
                    **result, key: (Visible(), assume_link_target(val))}
            return RTVal(cur_data, [FreeVal(ObjectVal(result))])
        case InsertExpr(tname, arg):
            if rt.data.eval_only:
                eval_error(
                    rt.expr,
                    "Attempting to Insert in an Eval-Only evaluation")
            (new_data, argmv) = eval_config(RTExpr(rt.data, arg))
            [argv] = argmv
            id = next_id()
            new_object = coerce_to_storage(
                get_object_val(argv), new_data.schema.val[tname])
            new_db = DB(dbdata={**new_data.cur_db.dbdata,
                        id: DBEntry(tp=VarTp(tname), data=new_object)})
            # inserts return empty dict
            return RTVal(
                RTData(
                    new_db, new_data.read_snapshots, new_data.schema,
                    new_data.eval_only),
                [RefVal(id, ObjectVal({}))])
        case FilterOrderExpr(subject=subject, filter=filter, order=order):
            (new_data, selected) = eval_config(RTExpr(rt.data, subject))
            # assume data unchaged throught the evaluation of conditions
            conditions: Sequence[MultiSetVal] = [
                eval_config(
                    RTExpr(
                        make_eval_only(new_data),
                        instantiate_expr(select_i, filter))).val
                for select_i in selected]
            after_condition: Sequence[Val] = [
                select_i
                for (select_i, condition) in zip(selected, conditions)
                if BoolVal(True) in condition]
            orders: Sequence[ObjectVal] = [
                raw_order[0].val
                if
                type(raw_order[0]) is FreeVal and
                type(raw_order[0].val) is ObjectVal else
                eval_error(raw_order[0],
                           "Order must be an object val")
                for after_condition_i in after_condition
                for raw_order
                in
                [
                    eval_config(
                        RTExpr(
                            make_eval_only(new_data),
                            instantiate_expr(after_condition_i, order))).val]
                if len(raw_order) == 1]
            after_order = eval_order_by(after_condition, orders)
            return RTVal(new_data, after_order)
        case ShapedExprExpr(expr=subject, shape=shape):
            (new_data, subjectv) = eval_config(RTExpr(rt.data, subject))
            after_shape: Sequence[Val] = [apply_shape(
                new_data, shape, v) for v in subjectv]
            return RTVal(new_data, after_shape)
        case FreeVarExpr(var=name):
            cur_db_data = rt.data.read_snapshots[0].dbdata
            all_ids: Sequence[Val] = [
                RefVal(id, ObjectVal({}))
                for (id, item) in cur_db_data.items()
                if item.tp.name == name]
            return RTVal(rt.data, all_ids)
        case FunAppExpr(fun=fname, args=args, overloading_index=_):
            (new_data, argsv) = eval_expr_list(rt.data, args)
            looked_up_fun = rt.data.schema.fun_defs[fname]
            f_modifier = looked_up_fun.tp.args_mod
            assert len(f_modifier) == len(argsv)
            argv_final: Sequence[Sequence[MultiSetVal]] = [[]]
            for i in range(len(f_modifier)):
                mod_i = f_modifier[i]
                argv_i: MultiSetVal = argsv[i]
                match mod_i:
                    case ParamSingleton():
                        argv_final = [
                            [*cur, [new]]
                            for cur in argv_final for new in argv_i]
                    case ParamOptional():
                        if len(argv_i) == 0:
                            argv_final = [[*cur, []] for cur in argv_final]
                        else:
                            argv_final = [
                                [*cur, [new]]
                                for cur in argv_final for new in argv_i]
                    case ParamSetOf():
                        argv_final = [[*cur, argv_i] for cur in argv_final]
                    case _:
                        raise ValueError()
            argv_final = [map_assume_link_target(f) for f in argv_final]
            after_fun_vals: Sequence[Val] = [
                v for arg in argv_final for v in looked_up_fun.impl(arg)]
            return RTVal(new_data, after_fun_vals)
        case ObjectProjExpr(subject=subject, label=label):
            (new_data, subjectv) = eval_config(RTExpr(rt.data, subject))
            projected = [
                p
                for v in assume_link_target(subjectv)
                for p in singular_proj(new_data, v, StrLabel(label))]
            if all([val_is_link_convertible(v) for v in projected]):
                return RTVal(
                    new_data, [convert_to_link(v) for v in projected])
            elif all([not val_is_link_convertible(v) for v in projected]):
                return RTVal(new_data, projected)
            else:
                return eval_error(
                    projected, "Returned objects are not uniform")
        case BackLinkExpr(subject=subject, label=label):
            (new_data, subjectv) = eval_config(RTExpr(rt.data, subject))
            subject_ids = [v.refid
                           if
                           isinstance(v, RefVal) else
                           eval_error(v, "expecting references")
                           for v in subjectv]
            cur_read_data: Dict[int,
                                DBEntry] = rt.data.read_snapshots[0].dbdata
            results: List[Val] = []
            for (id, obj) in cur_read_data.items():
                if StrLabel(label) in obj.data.val.keys():
                    object_vals = obj.data.val[StrLabel(label)][1]
                    if all(isinstance(object_val, RefVal)
                           for object_val in object_vals):
                        object_id_mapping = {
                            object_val.refid: object_val.val
                            for object_val in object_vals
                            if isinstance(object_val, RefVal)}
                        for (object_id,
                             obj_linkprop_val) in object_id_mapping.items():
                            if object_id in subject_ids:
                                results = [
                                    *results,
                                    LinkPropVal(
                                        obj=RefVal(id, ObjectVal({})),
                                        linkprop=obj_linkprop_val)]
            return RTVal(new_data, results)
        case TpIntersectExpr(subject=subject, tp=tp_name):
            (new_data, subjectv) = eval_config(RTExpr(rt.data, subject))
            after_intersect: MultiSetVal = []
            for v in subjectv:
                match v:
                    case RefVal(refid=vid, val=_)  \
                        | LinkPropVal(obj=RefVal(refid=vid, val=_),
                                      linkprop=_):
                        if is_nominal_subtype_in_schema(
                                new_data.cur_db.dbdata[vid].tp.name, tp_name,
                                new_data.schema):
                            after_intersect = [*after_intersect, v]
                    case _:
                        raise ValueError("Expecting References")
            return RTVal(new_data, after_intersect)
        case TypeCastExpr(tp=tp, arg=arg):
            (new_data, argv2) = eval_config(RTExpr(rt.data, arg))
            casted = [type_cast(tp, v) for v in argv2]
            return RTVal(new_data, casted)
        case UnnamedTupleExpr(val=tuples):
            (new_data, tuplesv) = eval_expr_list(rt.data, tuples)
            return RTVal(
                new_data,
                [UnnamedTupleVal(list(p))
                 for p in itertools.product(
                     *map_assume_link_target(tuplesv))])
        case NamedTupleExpr(val=tuples):
            (new_data, tuplesv) = eval_expr_list(
                rt.data, list(tuples.values()))
            return RTVal(new_data,
                         [NamedTupleVal({k: p})
                          for prod in itertools.product(
                              *map_assume_link_target(tuplesv))
                          for (k, p) in zip(
                              tuples.keys(),
                              prod, strict=True)])
        case UnionExpr(left=l, right=r):
            (new_data, lvals) = eval_config(RTExpr(rt.data, l))
            (new_data2, rvals) = eval_config(RTExpr(new_data, r))
            return RTVal(new_data2, [*lvals, *rvals])
        case ArrExpr(elems=elems):
            (new_data, elemsv) = eval_expr_list(rt.data, elems)
            return RTVal(new_data,
                         [ArrVal(list(el))
                          for el in itertools.product(*elemsv)])
        case UpdateExpr(subject=subject, shape=shape):
            if rt.data.eval_only:
                eval_error(
                    rt.expr,
                    "Attempting to Update in an Eval-Only evaluation")
            (new_data, subjectv) = eval_config(RTExpr(rt.data, subject))
            if all([val_is_ref_val(v) for v in subjectv]):
                updated: Sequence[Val] = [apply_shape(
                    new_data, shape, v)
                    for v in subjectv]  # type: ignore[misc]
                old_dbdata = rt.data.cur_db.dbdata
                new_dbdata = {
                    **old_dbdata, **
                    {u.refid:
                     DBEntry(
                         old_dbdata[u.refid].tp,
                         combine_object_val(
                             old_dbdata[u.refid].data, u.val))
                     for u in cast(Sequence[RefVal],
                                   updated)}}
                return RTVal(
                    RTData(
                        DB(new_dbdata),
                        rt.data.read_snapshots, rt.data.schema,
                        rt.data.eval_only),
                    updated)
            else:
                return eval_error(rt.expr, "expecting all references")
        case MultiSetExpr(expr=elems):
            (new_data, elemsv) = eval_expr_list(rt.data, elems)
            return RTVal(new_data, [e for el in elemsv for e in el])
        case WithExpr(bound=bound, next=next):
            (new_data, boundv) = eval_config(RTExpr(rt.data, bound))
            (new_data2, nextv) = eval_config(RTExpr(new_data, instantiate_expr(
                MultiSetExpr(cast(Sequence[Expr], boundv)), next)))
            return RTVal(new_data2, nextv)
        case OffsetLimitExpr(subject=subject, offset=offset, limit=limit):
            (new_data, subjectv) = eval_config(RTExpr(rt.data, subject))
            (new_data2, [offsetv]) = eval_config(RTExpr(new_data, offset))
            (new_data3, [limitv]) = eval_config(RTExpr(new_data2, limit))
            return RTVal(new_data3,
                         limit_vals(
                             offset_vals(subjectv, offsetv),
                             limitv))
        case SubqueryExpr(expr=expr):
            (new_data, exprv) = eval_config(RTExpr(rt.data, expr))
            return RTVal(new_data, exprv)
        case DetachedExpr(expr=expr):
            (new_data, exprv) = eval_config(RTExpr(rt.data, expr))
            return RTVal(new_data, exprv)
        case LinkPropProjExpr(subject=subject, linkprop=label):
            (new_data, subjectv) = eval_config(RTExpr(rt.data, subject))
            projected = [p for v in subjectv for p in singular_proj(
                new_data, v, LinkPropLabel(label))]
            return RTVal(new_data, projected)
        case ForExpr(bound=bound, next=next):
            (new_data, boundv) = eval_config(RTExpr(rt.data, bound))
            (new_data2, vv) = eval_expr_list(new_data, [
                instantiate_expr(v, next) for v in boundv])
            return RTVal(new_data2, [p for v in vv for p in v])
        case OptionalForExpr(bound=bound, next=next):
            (new_data, boundv) = eval_config(RTExpr(rt.data, bound))
            if boundv:
                (new_data2, vv) = eval_expr_list(new_data, [
                    instantiate_expr(v, next) for v in boundv])
                return RTVal(new_data2, [p for v in vv for p in v])
            else:
                return eval_config(
                    RTExpr(
                        new_data, instantiate_expr(
                            MultiSetExpr([]),
                            next)))

    raise ValueError("Not Implemented", rt.expr)


def eval_config_toplevel(rt: RTExpr) -> RTVal:
    match (eval_config(rt)):
        case RTVal(data=data, val=val):
            # Do not dedup (see one of the test cases)
            return RTVal(data=data, val=(val))
            # return RTVal(data=data, val=assume_link_target(val))
        case v:
            raise ValueError(v)
