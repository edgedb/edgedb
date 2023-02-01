
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

# RTExpr = Tuple[RTData, Expr]
# RTVal = Tuple[RTData, Val]

# @dataclass
class RTExpr(NamedTuple):
    data : RTData
    expr: Expr


# @dataclass(frozen=False)
class RTVal(NamedTuple):
    data : RTData
    val: Val

# def rtexpr_new_expr(config : RTConfig, expr : Expr) -> RTConfig: 
#     return RTConfig(config.cur_data, expr)

# def rtexpr_new_val(config : RTConfig, val : Val) -> RTVal: 
#     return RTVal(config.cur_data, val)

# def rtval_new_expr(config : RTVal, expr : Expr) -> RTConfig: 
#     return RTConfig(config.cur_data, expr)

# def rtval_new_val(config : RTVal, val : Val) -> RTVal: 
#     return RTVal(config.cur_data, val)

def eval_config(rt : RTExpr) -> RTVal:
    match rt.expr:
        case StrVal(s):
            return RTVal(rt.data, StrVal(s))
        case ProdExpr(val=dic):
            cur_data = rt.data
            result : Dict[str, Val] = {} 
            for (key, expr) in dic.items(): #type: ignore[has-type]
                [cur_data, val] = eval_config(RTExpr(cur_data, expr))
                result = {**result, key : val}
            return RTVal(cur_data, FreeVal(ProdVal(result)))
        case InsertExpr(tname, arg):
            [new_data, argv] = eval_config(RTExpr(rt.data, arg))
            id = next_id()
            new_object = get_object_val(argv)
            new_db = DB(dbdata={**new_data.cur_db.dbdata, id : DBEntry(tp=VarTp(tname), data=new_object)})
            return RTVal(RTData(new_db, new_data.read_snapshots), RefVal(id, DictVal({}))) # inserts return empty dict
    raise ValueError("Not Implemented", rt.expr)
