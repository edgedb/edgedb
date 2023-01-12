from __future__ import annotations
from typing import *

from dataclasses import dataclass

import uuid

def data(f):
    return dataclass(f, frozen=True)


def bsid(n: int) -> uuid.UUID:
    return uuid.UUID(f'ffffffff-ffff-ffff-ffff-{n:012x}')


### DEFINE TYPES

@data
class BinProdTp:
    label : str
    this : Tp
    next : Tp

@data
class BinProdUnitTp:
    pass

@data
class StrTp:
    pass

@data
class IntTp:
    pass


PrimTp = StrTp | IntTp 

@data
class VarTp:
    name : str

Tp = BinProdTp | BinProdUnitTp | PrimTp | VarTp

### DEFINE CARDINALITIES

@data
class ClosedCardinality:
    lower : int
    upper : int

    def __add__(self, other):
        sum_cardinality_modes(self, other)

    def __mul__(self, other):
        prod_cardinality_modes(self, other)


@data
class OpenCardinality:
    lower : int

    def __add__(self, other):
        sum_cardinality_modes(self, other)

    def __mul__(self, other):
        prod_cardinality_modes(self, other)

CardinalityModes = ClosedCardinality | OpenCardinality

def sum_cardinality_modes(card1 : CardinalityModes, card2 : CardinalityModes) -> CardinalityModes:
    match card1, card2:
        case ClosedCardinality(c1l, c1u), ClosedCardinality(c2l, c2u):
                return ClosedCardinality(c1l + c2l, c1u + c2u)
        case ClosedCardinality(c1l, c1u), OpenCardinality(c2l):
                return OpenCardinality(c1l + c2l)
        case OpenCardinality(c1l), ClosedCardinality(c2l, c2u):
                return OpenCardinality(c1l + c2l)
        case OpenCardinality(c1l), OpenCardinality(c2l):
                return OpenCardinality(c1l + c2l)
    raise ValueError("Cannot compute sums over", card1, "and", card2)

def prod_cardinality_modes(card1 : CardinalityModes, card2 : CardinalityModes) -> CardinalityModes:
    match card1, card2:
        case ClosedCardinality(c1l, c1u), ClosedCardinality(c2l, c2u):
                return ClosedCardinality(c1l * c2l, c1u * c2u)
        case ClosedCardinality(c1l, c1u), OpenCardinality(c2l):
                return OpenCardinality(c1l * c2l)
        case OpenCardinality(c1l), ClosedCardinality(c2l, c2u):
                return OpenCardinality(c1l * c2l)
        case OpenCardinality(c1l), OpenCardinality(c2l):
                return OpenCardinality(c1l * c2l)
    raise ValueError("Cannot compute prods over", card1, "and", card2)
        

CardOne = ClosedCardinality(1,1)
CardAtMostOne = ClosedCardinality(0,1)
CardAtLeastOne = OpenCardinality(1)
CardAny = OpenCardinality(0)

ResulTp = Tuple[Tp, CardinalityModes]
### DEFINE PARAMETER MODIFIERS

@data
class ParamSingleton:
    pass

@data
class ParamOptional:
    pass

@data 
class ParamSetOf:
    pass

ParamModifier = ParamSingleton | ParamOptional | ParamSetOf

@data
class FunType:
    args : List[Tuple[ParamModifier, Tp]]
    ret : ResulTp

### DEFINE PRIM VALUES
@data
class StrVal:
    val : str

@data 
class IntVal:
    val : int

@data 
class FunVal:
    fname : str

PrimVal = StrVal | IntVal | FunVal

## DEFINE EXPRESSIONS

@data
class UnionExpr:
    left : Expr
    right : Expr

@data 
class TypeCastExpr:
    tp : Tp
    arg : Expr


@data
class FunAppExpr:
    fun : Expr
    args : List[Expr]

@data
class BinProdExpr:
    label : str
    this : Expr
    next : Expr

@data
class BinProdUnitExpr:
    pass

@data
class VarExpr:
    var : str

@data 
class ProdProjExpr:
    subject : Expr
    label : str


@data
class WithExpr:
    bound : Expr
    var : str
    next : Expr

@data
class ForExpr:
    bound : Expr
    var : str
    next : Expr

@data 
class SelectExpr:
    name : str
    
@data
class InsertExpr:
    name : str
    new : Expr

@data 
class UpdateExpr:
    name : str
    var : str
    res : Expr

@data
class RefIdExpr:
    refid : int

@data 
class EmptyUnionExpr:
    name : str
    var : str
    res : Expr

Expr = (PrimVal | UnionExpr | TypeCastExpr | FunAppExpr | BinProdExpr | BinProdUnitExpr 
        | VarExpr | ProdProjExpr | WithExpr | ForExpr | SelectExpr | InsertExpr | UpdateExpr
        | RefIdExpr  | EmptyUnionExpr
        )

@data 
class DBEntry:
    tp : Tp
    data : Dict[str, Expr] ## actually values

@data
class DB:
    data: Dict[int, DBEntry] 
    # subtp : List[Tuple[TypeExpr, TypeExpr]]

BuiltinFuncTp : Dict[str, FunType] = {
        "+" : FunType([[IntTp, ParamSingleton], [IntTp, ParamSingleton]], [IntTp, CardOne])
    }



def add_fun(x, y):
    match x, y:
        case IntVal(a), IntVal(b):
            return IntVal(a + b)
    raise ValueError("cannot add ", x , y)



BuiltinFuncOp : Dict[str, Callable[..., ResulTp]] = {
    "+" : add_fun,
}


starting_id = 0

def next_id():
    global starting_id
    starting_id += 1
    return starting_id