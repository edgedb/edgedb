
import itertools
from typing import *

from .data.casts import type_cast
from .data.data_ops import (
    DB, ArrExpr, ArrVal, BackLinkExpr, BoolVal, DBEntry,
    DetachedExpr, Expr, FilterOrderExpr, ForExpr, FreeVal, FreeVarExpr,
    FunAppExpr, InsertExpr, IntInfVal, IntVal, Invisible, Label,
    LinkPropLabel, LinkPropProjExpr, LinkPropVal, Marker, MultiSetExpr,
    MultiSetVal, NamedTupleExpr, NamedTupleVal, ObjectExpr, ObjectProjExpr,
    ObjectVal, OffsetLimitExpr, OptionalForExpr, OrderAscending,
    OrderDescending, OrderLabelSep, ParamOptional, ParamSetOf,
    ParamSingleton, RefVal, ShapedExprExpr, ShapeExpr, StrLabel, StrVal,
    SubqueryExpr, TpIntersectExpr, TypeCastExpr, UnionExpr,
    UnnamedTupleExpr, UnnamedTupleVal, UpdateExpr, Val, VarTp, Visible,
    WithExpr, next_id,  RTExpr, RTVal)
from .data import data_ops as e
from .data import expr_ops as eops
from .data import type_ops as tops
from .data.expr_ops import (
    assume_link_target, coerce_to_storage, combine_object_val,
    get_object_val, instantiate_expr,
    map_assume_link_target, map_expand_multiset_val,
      val_is_link_convertible, val_is_ref_val)
from .data.type_ops import is_nominal_subtype_in_schema
from .db_interface import *

# def make_eval_only(data: RTData) -> RTData:
#     return RTData(data.cur_db, data.read_snapshots, data.schema, True)


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
            reverse=(False if spec == OrderAscending else
                     True if spec == OrderDescending else
                     eval_error(
                         cast(Sequence[Val],
                              orders),
                         "unknown spec")))
    return [elem for (_, elem) in result]


EvalEnv = Dict[str, MultiSetVal]
def ctx_extend(ctx: EvalEnv, bnd : BindingExpr, val: MultiSetVal) -> Tuple[EvalEnv, Expr]:
    bnd_no_capture = ensure_no_capture(list(ctx.keys()), bnd)
    return {**ctx, bnd_no_capture.var: val}, bnd_no_capture.body

def apply_shape(ctx: EvalEnv, db : EdgeDatabaseInterface, shape: ShapeExpr, value: Val) -> Val:
    def apply_shape_to_prodval(
            shape: ShapeExpr, objectval: ObjectVal) -> ObjectVal:
        result: Dict[Label, Tuple[Marker, MultiSetVal]] = {}
        for (key, (_, pval)) in objectval.val.items():
            if key not in shape.shape.keys():
                result = {
                    **result, key: (Invisible(), (pval))}
            else:
                pass
        for (key, shape_elem) in shape.shape.items():
            new_ctx, shape_body = ctx_extend(ctx, shape_elem, MultiSetVal([value]))
            new_val: MultiSetVal = eval_expr(new_ctx, db, shape_body)
            result = { **result, key: (Visible(), (new_val))}

        return ObjectVal(result)

    # [value] = assume_link_target([value])
    match value:
        case FreeVal(val=dictval):
            return FreeVal(val=apply_shape_to_prodval(shape, dictval))
        case RefVal(refid=id, val=dictval):
            return RefVal(
                refid=id, val=apply_shape_to_prodval(shape, dictval))
        case LinkPropVal(refid=id, linkprop=_):
            return RefVal(
                refid=id, val=apply_shape_to_prodval(shape, ObjectVal({})))
        case _:
            return eval_error(value, "Cannot apply shape to value")


def eval_expr_list(ctx: EvalEnv,
                   db : EdgeDatabaseInterface,
                   exprs: Sequence[Expr]) -> Sequence[MultiSetVal]:
    result: Sequence[MultiSetVal] = []
    for expr in exprs:
        val = eval_expr(ctx, db, expr)
        result = [*result, val]
    return result

# not sure why the semantics says to produce empty set when label not present


def singular_proj(ctx: EvalEnv, db: EdgeDatabaseInterface, subject: Val, label: Label) -> MultiSetVal:
    match subject:
        case FreeVal(val=objVal):
            if label in objVal.val.keys():
                return objVal.val[label][1]
            else:
                raise ValueError("Label not found", label)
        case RefVal(refid=id, val=objVal):
            # entry_obj = data.read_snapshots[0].dbdata[id].data
            if label in objVal.val.keys():
                return objVal.val[label][1]
            elif isinstance(label, StrLabel):
                label_str = label.label
                if db.is_projectable(id, label_str):
                    return db.project(id, label_str)
                else:
                    t_name = db.get_type_for_an_id(id)
                    t_def = db.get_schema().val[t_name]
                    if label_str in t_def.val.keys():
                        target_tp = t_def.val[label_str].tp
                        match target_tp:
                            case e.ComputableTp(expr=comp_expr, tp=_):
                                new_ctx, comp_expr_body = ctx_extend(ctx, comp_expr, MultiSetVal([subject]))
                                return eval_expr(new_ctx, db, comp_expr_body)
                            case _:
                                raise ValueError("Label found, but not computable",
                                                label)
                    else:
                        raise ValueError("Label not found", label)
            else:
                raise ValueError("Label not found", label)
        case NamedTupleVal(val=dic):
            match label:
                case StrLabel(l):
                    if l in dic.keys():
                        return MultiSetVal([dic[l]])
                    else:
                        if l.isdigit() and int(l) < len(dic.keys()):
                            return MultiSetVal([dic[list(dic.keys())[int(l)]]])
                        else:
                            raise ValueError("key DNE")
            raise ValueError("Label not Str")
        case UnnamedTupleVal(val=arr):
            match label:
                case StrLabel(l):
                    if l.isdigit() and int(l) < len(arr):
                        return MultiSetVal([arr[int(l)]])
                    else:
                        raise ValueError("key DNE")
            raise ValueError("Label not Str")
        case LinkPropVal(refid=id, linkprop=linkprop):
            match label:
                case LinkPropLabel(label=lp_label):
                    return singular_proj(ctx, db,
                        FreeVal(val=linkprop), label=StrLabel(lp_label))
                case StrLabel(_):
                    return singular_proj(ctx, db,
                                         RefVal(refid=id, val=ObjectVal({})),
                                         label=label)
                case _:
                    raise ValueError(label)
    raise ValueError("Cannot project, unknown subject", subject)


def offset_vals(val: Sequence[Val], offset: Val):
    match offset:
        case IntVal(val=v):
            return val[v:]
        case _:
            raise ValueError("offset must be an int")


def limit_vals(val: Sequence[Val],
               limit: Val) -> Sequence[Val]:
    match limit:
        case IntVal(val=v):
            return val[:v]
        case IntInfVal():
            return val
        case _:
            raise ValueError("offset must be an int")

def object_tp_default_initial_step_with_insert(
        tp: e.ObjectTp,
        insert_shape: e.ShapeExpr,
        refid: int
        ) -> e.Expr:
    initial = tops.object_tp_default_initial(tp)
    step = tops.object_tp_default_step(tp)
    result: e.Expr = e.RefVal(refid=refid, val=initial)
    for _ in range(len(tp.val.keys())):
        result = eops.ShapedExprExpr(expr=result, shape=step)
    post_step = tops.object_tp_default_post_step(tp, insert_shape)
    result = e.ShapedExprExpr(expr=result, shape=insert_shape)
    for _ in range(len(tp.val.keys())):
        result = eops.ShapedExprExpr(expr=result, shape=post_step)
    result = e.ShapedExprExpr(expr=result, shape=insert_shape)
    return result



class EvaluationLogsWrapper:
    def __init__(self):
        self.original_eval_expr = None
        self.reset_logs(None)

    def reset_logs(self, logs: Optional[List[Any]]):
        self.logs = logs
        self.indexes: List[int] = []

    def __call__(self, eval_expr: Callable[[EvalEnv, EdgeDatabaseInterface, Expr], MultiSetVal]):
        self.original_eval_expr = eval_expr

        def wrapper(ctx: EvalEnv, db: EdgeDatabaseInterface, expr: Expr) -> MultiSetVal:
            if self.logs is None:
                return self.original_eval_expr(ctx, db, expr)
            else:
                parent = self.logs
                [parent := parent[i] for i in self.indexes]
                self.indexes.append(len(parent))
                parent.append([(expr, [StrVal("NOT AVAILABLE!!!")])])
                rt_val = self.original_eval_expr(ctx, db, expr)
                parent[self.indexes[-1]][0] = (parent[self.indexes[-1]][0][0],
                                               rt_val.val)
                assert len(parent[self.indexes[-1]][0]) == 2
                self.indexes.pop()
                return rt_val

        return wrapper


eval_logs_wrapper = EvaluationLogsWrapper()



# the database is a mutable reference that keeps track of a read snapshot inside
@eval_logs_wrapper
def eval_expr(ctx: EvalEnv,
                db: EdgeDatabaseInterface,
                expr: Expr) -> MultiSetVal:
    match expr:
        case (StrVal(_)
              | IntVal(_)
              | BoolVal(_)
              ):
            return MultiSetVal([expr])
        case ObjectExpr(val=dic):
            result: Dict[Label, Tuple[Marker, MultiSetVal]] = {}
            for (key, expr) in dic.items():  # type: ignore[has-type]
                val = eval_expr(ctx, db, expr)
                result = {**result, key: (Visible(), (val))}
            return  MultiSetVal([FreeVal(ObjectVal(result))])
        case InsertExpr(tname, arg):
            id = db.insert(tname, {})
            expr_with_default = object_tp_default_initial_step_with_insert(
                db.get_schema().val[tname], arg, id)
            raw_object = eval_expr(
                        ctx,
                        db,
                        expr_with_default)

            assert len(raw_object.vals) == 1, (
                "Insert shape should return one object"
            )
            assert raw_object.vals[0].refid == id, (
                "Insert should not change id"
            )

            new_object = coerce_to_storage(
                get_object_val(raw_object.vals[0]), db.get_schema().val[tname])
            db.update(id, {k : v  for k, v in new_object.items() })
            # inserts return empty dict
            return MultiSetVal([RefVal(id, ObjectVal({}))])
        case FilterOrderExpr(subject=subject, filter=filter, order=order):
            selected = eval_expr(ctx, db, subject)
            # assume data unchaged throught the evaluation of conditions
            conditions: Sequence[MultiSetVal] = [
                eval_expr(
                        new_ctx,
                        db, 
                        filter_body)
                for select_i in selected.vals
                for new_ctx, filter_body in [ctx_extend(ctx, filter, select_i)]]
            after_condition: Sequence[Val] = [
                select_i
                for (select_i, condition) in zip(selected.vals, conditions)
                if BoolVal(True) in condition.vals]
            orders: Sequence[ObjectVal] = [
                (raw_order[0].val
                    if
                    type(raw_order[0]) is FreeVal and
                    type(raw_order[0].val) is ObjectVal else
                    eval_error(raw_order[0],
                            "Order must be an object val"))
                if len(raw_order) == 1 else eval_error(raw_order)
                for after_condition_i in after_condition
                for new_ctx, order_body in [ctx_extend(ctx, order, MultiSetVal([after_condition_i]))]
                for raw_order
                in
                [eval_expr(
                        new_ctx,
                        db,
                        order_body)]]
            after_order = eval_order_by(after_condition, orders)
            return MultiSetVal(after_order)
        case ShapedExprExpr(expr=subject, shape=shape):
            subjectv = eval_expr(ctx, db, subject)
            after_shape: Sequence[Val] = [apply_shape(ctx, db, shape, v) for v in subjectv.vals]
            return MultiSetVal(after_shape)
        case FreeVarExpr(var=name):
            if name in ctx.keys():
                return ctx[name]
            else:
                all_ids: Sequence[Val] = [
                    RefVal(id, ObjectVal({}))
                    for id in db.query_ids_for_a_type(name)]
                return MultiSetVal(all_ids)
        case FunAppExpr(fun=fname, args=args, overloading_index=_):
            argsv = eval_expr_list(ctx, db, args)
            argsv = map_assume_link_target(argsv)
            looked_up_fun = db.get_schema().fun_defs[fname]
            f_modifier = looked_up_fun.tp.args_mod
            assert len(f_modifier) == len(argsv)
            argv_final: Sequence[Sequence[Sequence[Val]]] = [[]]
            for i in range(len(f_modifier)):
                mod_i = f_modifier[i]
                argv_i: Sequence[Val] = argsv[i].vals
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
            # argv_final = [map_assume_link_target(f) for f in argv_final]
            after_fun_vals: Sequence[Val] = [
                v for arg in argv_final for v in looked_up_fun.impl(arg)]
            return MultiSetVal(after_fun_vals)
        case ObjectProjExpr(subject=subject, label=label):
            subjectv = eval_expr(ctx, db, subject)
            projected = [
                p
                for v in assume_link_target(subjectv).vals
                for p in singular_proj(ctx, db, v, StrLabel(label)).vals]
            return MultiSetVal(projected)
            # if all([val_is_link_convertible(v) for v in projected]):
            #     return RTVal(
            #         new_data, [convert_to_link(v) for v in projected])
            # elif all([not val_is_link_convertible(v) for v in projected]):
            #     return RTVal(new_data, projected)
            # else:
            #     return eval_error(
            #         projected, "Returned objects are not uniform")
        case BackLinkExpr(subject=subject, label=label):
            subjectv = eval_expr(ctx, db, subject)
            subjectv = assume_link_target(subjectv)
            subject_ids = [v.refid
                           if
                           isinstance(v, RefVal) else
                           eval_error(v, "expecting references")
                           for v in subjectv.vals]
            return db.reverse_project(subject_ids, label)
        case TpIntersectExpr(subject=subject, tp=tp_name):
            subjectv = eval_expr(ctx, db, subject)
            after_intersect: List[Val] = []
            for v in subjectv.vals:
                match v:
                    case (RefVal(refid=vid, val=_)
                          | LinkPropVal(refid=vid,
                                        linkprop=_)):
                        if is_nominal_subtype_in_schema(
                                db.get_type_for_an_id(vid), tp_name,
                                db.get_schema()):
                            after_intersect = [*after_intersect, v]
                    case _:
                        raise ValueError("Expecting References")
            return MultiSetVal(after_intersect)
        case TypeCastExpr(tp=tp, arg=arg):
            argv2 = eval_expr(ctx, db, arg)
            casted = [type_cast(tp, v) for v in argv2.vals]
            return  MultiSetVal(casted)
        case UnnamedTupleExpr(val=tuples):
            tuplesv = eval_expr_list(ctx, db, tuples)
            constructed = [
                UnnamedTupleVal(list(p))
                for p in itertools.product(
                    *map_expand_multiset_val(
                      map_assume_link_target(tuplesv)))]
            return MultiSetVal(constructed)
        case NamedTupleExpr(val=tuples):
            tuplesv = eval_expr_list(ctx, db, list(tuples.values()))
            result_list: List[Val] = [
                           NamedTupleVal({k: p})
                           for prod in itertools.product(
                             *map_expand_multiset_val(
                               map_assume_link_target(tuplesv)))
                           for (k, p) in zip(
                               tuples.keys(),
                               prod, strict=True)]
            return MultiSetVal(result_list)
        case UnionExpr(left=l, right=r):
            lvals = eval_expr(ctx, db, l)
            rvals = eval_expr(ctx, db, r)
            return MultiSetVal([*lvals, *rvals])
        case ArrExpr(elems=elems):
            elemsv = eval_expr_list(ctx, db, elems)
            arr_result = [ArrVal(list(el))
                          for el in itertools.product(
                          *map_expand_multiset_val(
                              map_assume_link_target(elemsv)))]
            return MultiSetVal(arr_result)
        case e.DeleteExpr(subject=subject):
            subjectv = eval_expr(ctx, db, subject)
            subjectv = assume_link_target(subjectv)
            if all([val_is_ref_val(v) for v in subjectv.vals]):
                # old_dbdata = rt.data.cur_db.dbdata
                delete_ref_ids = [v.refid for v in subjectv.vals]
                # because write-through model, do not check whether 
                # the references are valid
                # deleted: Sequence[Val] = [
                #     v for v in subjectv.vals
                #     if v.refid in old_dbdata.keys()]  # type: ignore[misc]
                for delete_id in delete_ref_ids:
                    db.delete(delete_id)
                # new_dbdata = {
                #     k: v for k, v in old_dbdata.items()
                #     if k not in delete_ref_ids}
                return subjectv
            else:
                return eval_error(expr, "expecting all references")
        case UpdateExpr(subject=subject, shape=shape):
            subjectv = eval_expr(ctx, db, subject)
            subjectv = assume_link_target(subjectv)
            if all([val_is_ref_val(v) for v in subjectv.vals]):
                updated: Sequence[Val] = [apply_shape(ctx, db, shape, v)
                            for v in subjectv.vals]  # type: ignore[misc]
                # the idea is to construct an object type only on the updated properties,
                # and then perform on the coercion for the new constructed type
                for u in cast(Sequence[RefVal], updated):
                    full_tp = db.get_schema().val[db.get_type_for_an_id(u.refid)]
                    cut_tp = {k : v for (k,v) in full_tp.val.items() if StrLabel(k) in u.val.val.keys()}
                    db.update(
                        u.refid,
                        coerce_to_storage(u.val, ObjectTp(cut_tp))
                    )
                return MultiSetVal(updated)
            else:
                return eval_error(expr, "expecting all references")
        case MultiSetExpr(expr=elems):
            elemsv = eval_expr_list(ctx, db, elems)
            result_list = [e for el in elemsv for e in el.vals]
            return MultiSetVal(result_list)
        case WithExpr(bound=bound, next=next):
            boundv = eval_expr(ctx, db, bound)
            new_ctx, next_body = ctx_extend(ctx, next, boundv)
            nextv = eval_expr(new_ctx, db, next_body)
            return nextv
        case OffsetLimitExpr(subject=subject, offset=offset, limit=limit):
            subjectv = eval_expr(ctx, db, subject)
            offsetv_m = eval_expr(ctx, db, offset)
            assert len(offsetv_m.vals) <= 1
            offsetv = offsetv_m.vals[0] if len(offsetv_m.vals) == 1 else e.IntVal(0)
            limitv_m = eval_expr(ctx, db, limit)
            assert len(limitv_m.vals) <= 1
            limitv = limitv_m.vals[0] if len(limitv_m.vals) == 1 else e.IntInfVal()
            result_list = list(limit_vals(
                             offset_vals(subjectv.vals, offsetv),
                             limitv))
            return MultiSetVal(result_list)
        case SubqueryExpr(expr=expr):
            exprv = eval_expr(ctx, db , expr)
            return exprv
        case DetachedExpr(expr=expr):
            exprv = eval_expr(ctx, db , expr)
            return exprv
        case LinkPropProjExpr(subject=subject, linkprop=label):
            subjectv = eval_expr(ctx, db, subject)
            projected = [p for v in subjectv.vals for p in singular_proj(
                ctx, db, v, LinkPropLabel(label)).vals]
            return MultiSetVal(projected)
        case ForExpr(bound=bound, next=next):
            boundv = eval_expr(ctx, db, bound)
            vv = []
            for v in boundv.vals:
                new_ctx, next_body = ctx_extend(ctx, next, MultiSetVal([v]))
                nextv = eval_expr(new_ctx, db, next_body)
                vv.append(nextv)
            result_list = [p for v in vv for p in v.vals]
            return MultiSetVal(result_list)
        case e.IfElseExpr(then_branch=then_branch,
                          condition=condition,
                          else_branch=else_branch):
            conditionv = eval_expr(ctx, db, condition)
            vv2 = eval_expr_list(ctx, db, [
                then_branch if v == e.BoolVal(True) else
                else_branch if v == e.BoolVal(False) else
                eval_error(condition, "condition must be a boolean")
                for v in conditionv.vals])
            result_list = [p for v in vv2 for p in v.vals]
            return MultiSetVal(result_list)
        case OptionalForExpr(bound=bound, next=next):
            boundv = eval_expr(ctx, db, bound)
            if boundv.vals:
                vv = []
                for v in boundv.vals:
                    new_ctx, next_body = ctx_extend(ctx, next, MultiSetVal([v]))
                    nextv = eval_expr(new_ctx, db, next_body)
                    vv.append(nextv)
                result_list = [p for v in vv for p in v.vals]
                return MultiSetVal(result_list)
            else:
                new_ctx, next_body = ctx_extend(ctx, next, MultiSetVal([]))
                return eval_expr(new_ctx, db, next_body)

    raise ValueError("Not Implemented", expr)


def eval_expr_toplevel(db: EdgeDatabaseInterface, expr: Expr, logs: Optional[Any] = None) -> MultiSetVal:

    # on exception, this is not none
    # assert eval_logs_wrapper.logs is None
    if logs is not None:
        eval_logs_wrapper.reset_logs(logs)

    final_v = eval_expr([], db, expr)
    # commit DML after evaluation
    db.commit_dml()

    # restore the decorator state
    eval_logs_wrapper.reset_logs(None)

    # Do not dedup (see one of the test cases)
    # return assume_link_target(final_v)
    return final_v
