
from typing import *
from dataclasses import *
from functools import singledispatch
from .data_ops import *

# RT Stands for Run Time 

@dataclass(frozen=True)
class RTConfig:
    cur_db : DB
    read_snapshots: List[DB]
    cur_expr: Expr


@dataclass(frozen=True)
class RTVal:
    cur_db : DB
    read_snapshots: List[DB]
    val: Val

def rtexpr_new_expr(config : RTConfig, expr : Expr) -> RTConfig: 
    return RTConfig(config.cur_db, config.read_snapshots, expr)

def rtexpr_new_val(config : RTConfig, val : Val) -> RTVal: 
    return RTVal(config.cur_db, config.read_snapshots, val)

def rtval_new_expr(config : RTVal, expr : Expr) -> RTConfig: 
    return RTConfig(config.cur_db, config.read_snapshots, expr)

def rtval_new_val(config : RTVal, val : Val) -> RTVal: 
    return RTVal(config.cur_db, config.read_snapshots, val)

def eval_config(rt : RTConfig) -> RTVal:
    match rt.cur_expr:
        case StrVal(s):
            return rtexpr_new_val(rt, StrVal(s))
        case BinProdExpr(lbl, this, next):
            thisv = eval_config(rtexpr_new_expr(rt, this))
            nextv = eval_config(rtval_new_expr(thisv, next))
            return rtval_new_val(nextv, FreeVal(BinProdVal(lbl, Visible(), thisv.val, nextv.val)))
        case BinProdUnitExpr():
            return rtexpr_new_val(rt, BinProdUnitVal())
        case InsertExpr(tname, arg):
            argv = eval_config(rtexpr_new_expr(rt, arg))
            id = next_id()
            rt.cur_db.dbdata[id] = DBEntry(tp=VarTp(tname), data=argv.val)
            return RTVal(rt.cur_db, rt.read_snapshots, RefVal(id, argv.val))
    raise ValueError("Not Implemented", rt.cur_expr)
