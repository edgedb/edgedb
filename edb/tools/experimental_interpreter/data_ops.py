from __future__ import annotations
from typing import *

from dataclasses import dataclass


import uuid

def data(f):
    return dataclass(f, frozen=True)


# def bsid(n: int) -> uuid.UUID:
#     return uuid.UUID(f'ffffffff-ffff-ffff-ffff-{n:012x}')


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


@data
class Visible:
    pass

@data
class Invisible:
    pass

Marker = Visible | Invisible

    
    
### DEFINE CARDINALITIES

@data
class FiniteCardinal:
    value : int
    def __add__(self, other):
        match other:
            case FiniteCardinal(otherCard):
                return FiniteCardinal(self.value + otherCard)
            case InfiniteCardinal():
                return InfiniteCardinal()
        raise ValueError()

    def __mul__(self, other : Cardinal):
        match other:
            case FiniteCardinal(otherCard):
                return FiniteCardinal(self.value * otherCard)
            case InfiniteCardinal():
                return InfiniteCardinal()
        raise ValueError()

@data
class InfiniteCardinal:
    def __add__(self, other):
        match other:
            case FiniteCardinal(otherCard):
                return InfiniteCardinal()
            case InfiniteCardinal():
                return InfiniteCardinal()
        raise ValueError()

    def __mul__(self, other : Cardinal):
        match other:
            case FiniteCardinal(otherCard):
                return InfiniteCardinal()
            case InfiniteCardinal():
                return InfiniteCardinal()
        raise ValueError()

Cardinal = FiniteCardinal | InfiniteCardinal

def Inf():
    return InfiniteCardinal()

def Fin(i):
    return FiniteCardinal(i)
    
# @data
# class ClosedCardinality:
#     lower : int
#     upper : int

#     def __add__(self, other):
#         sum_cardinality_modes(self, other)

#     def __mul__(self, other):
#         prod_cardinality_modes(self, other)


# @data
# class OpenCardinality:
#     lower : int

#     def __add__(self, other):
#         sum_cardinality_modes(self, other)

#     def __mul__(self, other):
#         prod_cardinality_modes(self, other)

# CardinalityModes = ClosedCardinality | OpenCardinality

# def sum_cardinality_modes(card1 : CardinalityModes, card2 : CardinalityModes) -> CardinalityModes:
#     match card1, card2:
#         case ClosedCardinality(c1l, c1u), ClosedCardinality(c2l, c2u):
#                 return ClosedCardinality(c1l + c2l, c1u + c2u)
#         case ClosedCardinality(c1l, c1u), OpenCardinality(c2l):
#                 return OpenCardinality(c1l + c2l)
#         case OpenCardinality(c1l), ClosedCardinality(c2l, c2u):
#                 return OpenCardinality(c1l + c2l)
#         case OpenCardinality(c1l), OpenCardinality(c2l):
#                 return OpenCardinality(c1l + c2l)
#     raise ValueError("Cannot compute sums over", card1, "and", card2)

# def prod_cardinality_modes(card1 : CardinalityModes, card2 : CardinalityModes) -> CardinalityModes:
#     match card1, card2:
#         case ClosedCardinality(c1l, c1u), ClosedCardinality(c2l, c2u):
#                 return ClosedCardinality(c1l * c2l, c1u * c2u)
#         case ClosedCardinality(c1l, c1u), OpenCardinality(c2l):
#                 return OpenCardinality(c1l * c2l)
#         case OpenCardinality(c1l), ClosedCardinality(c2l, c2u):
#                 return OpenCardinality(c1l * c2l)
#         case OpenCardinality(c1l), OpenCardinality(c2l):
#                 return OpenCardinality(c1l * c2l)
#     raise ValError("Cannot compute prods over", card1, "and", card2)
        

@data
class CMMode:
    lower : Cardinal
    upper : Cardinal
    multiplicity : Cardinal = None

    def __post_init__(self):
        if self.multiplicity == None:
            object.__setattr__(self, 'multiplicity', self.upper)

    def __add__(self, other : CMMode):
        return CMMode(self.lower + other.lower, 
                      self.upper + other.upper, 
                      self.multiplicity + other.multiplicity)

    def __mul__(self, other : CMMode):
        return CMMode(self.lower * other.lower, 
                      self.upper * other.upper, 
                      self.multiplicity * other.multiplicity)



CardOne = CMMode(Fin(1),Fin(1))
CardAtMostOne = CMMode(Fin(0),Fin(1))
CardAtLeastOne = CMMode(Fin(1), Inf())
CardAny = CMMode(Fin(0), Inf())

ResulTp = Tuple[Tp, CMMode]


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

# @data
# class UnionExpr:
#     left : Expr
#     right : Expr

@data
class MultiSetExpr:
    val : List[Expr]

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
    marker : Marker
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


Expr = (PrimVal | TypeCastExpr | FunAppExpr | BinProdExpr | BinProdUnitExpr 
        | VarExpr | ProdProjExpr | WithExpr | ForExpr | SelectExpr | InsertExpr | UpdateExpr
        | RefIdExpr  | MultiSetExpr
        )

#### VALUES

@data
class BinProdVal:
    label : str
    marker : Marker
    this : Expr
    next : Expr

@data
class BinProdUnitVal:
    pass


@data
class FreeVal:
    val : DictVal
    
@data
class RefVal:
    refid : int
    val : DictVal

@data
class RefLinkVal:
    from_id : int
    to_id : int
    val : DictVal

@data
class MultiSetVal:
    val : List[DictVal]

@data 
class LinkWithPropertyVal:
    subject : Val
    link_properties : Val

    


DictVal = BinProdVal | BinProdUnitVal
Val = RefVal | FreeVal | MultiSetVal  | RefLinkVal | LinkWithPropertyVal

@data 
class DBEntry:
    tp : Tp
    data : Dict[str, DictVal] ## actually values

@data
class DB:
    dbdata: Dict[int, DBEntry] 
    # subtp : List[Tuple[TypeExpr, TypeExpr]]

def empty_db():
    return DB({})

BuiltinFuncTp : Dict[str, FunType] = {
        "+" : FunType([[IntTp, ParamSingleton], [IntTp, ParamSingleton]], [IntTp, CardOne])
    }



def add_fun(x, y):
    match x, y:
        case [IntVal(a)], [IntVal(b)]:
            return [IntVal(a + b)]
    raise ValueError("cannot add ", x , y)



BuiltinFuncOp : Dict[str, Callable[..., List[Expr]]] = {
    "+" : add_fun,
}


starting_id = 0

def next_id():
    global starting_id
    starting_id += 1
    return starting_id


def dict_to_val(data : Dict[str, Val]):
    result = BinProdUnitVal()
    [result := BinProdVal(k, Visible, v, result) for k,v in reversed(data.items)]
    return result
    reduce(lambda k,v: BinProdVal(k,Visible,v,), data.items(), BinProdUnitVal())

def ref(id):
    return RefVal(id, BinProdUnitVal)