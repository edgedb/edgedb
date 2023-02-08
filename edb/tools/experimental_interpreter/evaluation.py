
from dataclasses import *
from functools import singledispatch
from typing import *

from .data.data_ops import *
from .data.expr_ops import *

# RT Stands for Run Time 

@dataclass(frozen=True)
class RTData:
    cur_db : DB
    read_snapshots: List[DB]
    schema : DBSchema
    eval_only : bool # a.k.a. no DML, no effect

class RTExpr(NamedTuple):
    data : RTData
    expr: Expr

class RTVal(NamedTuple):
    data : RTData
    val: MultiSetVal

def make_eval_only(data : RTData) -> RTData:
    return RTData(data.cur_db, data.read_snapshots, data.schema, True)

def eval_error(expr : Val | Expr, msg : str ="") -> Any:
    raise ValueError("Eval Error", msg, expr)

def eval_order_by(after_condition : List[Val], orders : List[UnnamedTupleVal]) -> List[Val]:
    if len(after_condition) == 0:
        return after_condition

    result : List[Tuple[int, Val]]= list(enumerate(after_condition))
    for i in range(len(orders[0].val)):
        def key_extract(elem : Tuple[int, Val]):
            return orders[elem[0]].val[i]
        result = sorted(result, key=key_extract, reverse=(i%2 == 1))
    return [elem for (_, elem) in result]

def apply_shape(ctx : RTData, shape : ShapeExpr, value : Val) -> Val:
    def apply_shape_to_prodval(shape : ShapeExpr, objectval : ObjectVal) -> ObjectVal:
        result : Dict[Label, Tuple[Marker, MultiSetVal]]= {}
        for (key, (_, pval)) in objectval.val.items():
            if key in shape.shape.keys():
                new_val : MultiSetVal = eval_config(RTExpr(make_eval_only(ctx), 
                    instantiate_expr(value, shape.shape[key]))).val ### instantiate with value not pval !! (see semantics)
                result = {**result, key : (Visible(), new_val)}
            else:
                result = {**result, key: (Invisible(), pval)}
        for (key, shape_elem) in shape.shape.items():
            if key in objectval.val.keys():
                pass
            else:
                new_val = eval_config(RTExpr(make_eval_only(ctx), 
                        instantiate_expr(value, shape_elem)
                    )).val
                result = {**result, key : (Visible(), new_val)}
        
        return ObjectVal(result)




    match value:
        case FreeVal(val=dictval):
            return FreeVal(val=apply_shape_to_prodval(shape, dictval))
        case RefVal(refid=id, val=dictval):
            return RefVal(refid=id, val=apply_shape_to_prodval(shape, dictval))
        case _:
            return eval_error(value, "Cannot apply shape to value")

def eval_config(rt : RTExpr) -> RTVal:
    match rt.expr:
        case StrVal(s):
            return RTVal(rt.data, MultiSetVal([StrVal(s)]))
        case ObjectExpr(val=dic):
            cur_data = rt.data
            result : Dict[Label, Tuple[Marker, MultiSetVal]] = {} 
            for (key, expr) in dic.items(): #type: ignore[has-type]
                (cur_data, val) = eval_config(RTExpr(cur_data, expr))
                result = {**result, key : (Visible(), val)}
            return RTVal(cur_data, MultiSetVal([FreeVal(ObjectVal(result))]))
        case InsertExpr(tname, arg):
            if rt.data.eval_only:
                eval_error(rt.expr, "Attempting to Insert in an Eval-Only evaluation")
            (new_data, argmv) = eval_config(RTExpr(rt.data, arg))
            [argv] = argmv.val
            id = next_id()
            new_object = coerce_to_storage(get_object_val(argv), ObjectTp({}))
            new_db = DB(dbdata={**new_data.cur_db.dbdata, id : DBEntry(tp=VarTp(tname), data=new_object)})
            return RTVal(RTData(new_db, new_data.read_snapshots, new_data.schema, new_data.eval_only), MultiSetVal([RefVal(id, ObjectVal({}))])) # inserts return empty dict
        case FilterOrderExpr(subject=subject, filter=filter, order=order):
            (new_data, selected) = eval_config(RTExpr(rt.data, subject))
            # assume data unchaged throught the evaluation of conditions
            conditions : List[MultiSetVal] = [eval_config(RTExpr(make_eval_only(new_data), instantiate_expr(select_i, filter))).val for select_i in selected.val]
            after_condition : List[Val] = [select_i for (select_i, condition) in zip(selected.val, conditions) if BoolVal(True) in condition.val]
            orders : List[UnnamedTupleVal] = [ raw_order.val[0] if type(raw_order.val[0]) is UnnamedTupleVal else eval_error(raw_order.val[0])
                        for after_condition_i in after_condition
                        for raw_order in [eval_config(RTExpr(make_eval_only(new_data), instantiate_expr(after_condition_i,order))).val]
                        if len(raw_order.val) == 1
                    ]
            after_order = eval_order_by(after_condition, orders)
            return RTVal(new_data, MultiSetVal(after_order))
        case ShapedExprExpr(expr=subject, shape=shape):
            (new_data, subjectv) = eval_config(RTExpr(rt.data, subject))
            after_shape : List[Val]= [apply_shape(rt.data, shape, v) for v in subjectv.val]
            return RTVal(new_data, MultiSetVal(after_shape))
        case FreeVarExpr(var=name):
            cur_db_data = rt.data.read_snapshots[0].dbdata
            all_ids : List[Val] = [RefVal(id, ObjectVal({})) for (id, item) in cur_db_data.items() 
                                                if item.tp.name == name]
            return RTVal(rt.data, MultiSetVal(all_ids))

    raise ValueError("Not Implemented", rt.expr)
