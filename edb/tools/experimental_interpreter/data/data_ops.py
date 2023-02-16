from __future__ import annotations
from typing import *

from dataclasses import dataclass


# to use when we move to 3.11 
# and https://peps.python.org/pep-0681/ is implemented in mypy
# https://github.com/python/mypy/issues/14293
# @dataclass_transformer
# def data(f):
#     return dataclass(f, frozen=True)


### LABELS

@dataclass(frozen=True)
class StrLabel:
    label: str

@dataclass(frozen=True)
class LinkPropLabel:
    label: str

Label =  StrLabel | LinkPropLabel

### DEFINE TYPES



@dataclass(frozen=True)
class ObjectTp:
    val : Dict[str, ResulTp]

@dataclass(frozen=True)
class StrTp:
    pass

@dataclass(frozen=True)
class BoolTp:
    pass


@dataclass(frozen=True)
class IntTp:
    pass


@dataclass(frozen=True)
class DateTimeTp:
    pass

@dataclass(frozen=True)
class JsonTp:
    pass


PrimTp = StrTp | IntTp | BoolTp | DateTimeTp | JsonTp

@dataclass(frozen=True)
class VarTp:
    name : str

@dataclass(frozen=True)
class NamedTupleTp:
    val : Dict[str, Tp]

@dataclass(frozen=True)
class UnnamedTupleTp:
    val : List[Tp]

@dataclass(frozen=True)
class ArrayTp:
    tp : Tp

Tp = ObjectTp | PrimTp | VarTp | NamedTupleTp | UnnamedTupleTp | ArrayTp


@dataclass(frozen=True)
class Visible:
    pass

@dataclass(frozen=True)
class Invisible:
    pass

Marker = Visible | Invisible

    
    
### DEFINE CARDINALITIES

@dataclass(frozen=True)
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

@dataclass(frozen=True)
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
    
# @dataclass(frozen=True)
# class ClosedCardinality:
#     lower : int
#     upper : int

#     def __add__(self, other):
#         sum_cardinality_modes(self, other)

#     def __mul__(self, other):
#         prod_cardinality_modes(self, other)


# @dataclass(frozen=True)
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
        

@dataclass(frozen=True)
class CMMode:
    lower : Cardinal
    upper : Cardinal
    multiplicity : Cardinal = None # type: ignore

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

@dataclass(frozen=True)
class ParamSingleton:
    pass

@dataclass(frozen=True)
class ParamOptional:
    pass

@dataclass(frozen=True) 
class ParamSetOf:
    pass

ParamModifier = ParamSingleton | ParamOptional | ParamSetOf

@dataclass(frozen=True)
class FunType:
    args_tp : List[Tp]
    args_mod : List[ParamModifier]
    ret : ResulTp

### DEFINE PRIM VALUES
@dataclass(frozen=True)
class StrVal:
    val : str

@dataclass(frozen=True) 
class IntVal:
    val : int


@dataclass(frozen=True) 
class DateTimeVal:
    val : str

@dataclass(frozen=True) 
class JsonVal:
    val : str

@dataclass(frozen=True) 
class FunVal:
    fname : str

@dataclass(frozen=True) 
class IntInfVal: 
    """ the infinite integer, used as the default value for limit """
    pass

@dataclass(frozen=True)
class BoolVal:
    val: bool

PrimVal = StrVal | IntVal | FunVal | IntInfVal | BoolVal | DateTimeVal | JsonVal

## DEFINE EXPRESSIONS

@dataclass(frozen=True)
class UnionExpr:
    left : Expr
    right : Expr

@dataclass(frozen=True)
class MultiSetExpr:
    expr : List[Expr]

@dataclass(frozen=True) 
class TypeCastExpr:
    tp : Tp
    arg : Expr


@dataclass(frozen=True)
class FunAppExpr:
    fun : str
    overloading_index : Optional[int]
    args : List[Expr]

@dataclass(frozen=True)
class ObjectExpr:
    val : Dict[Label, Expr]

@dataclass(frozen=True)
class FreeVarExpr:
    var : str

@dataclass(frozen=True)
class BoundVarExpr:
    var : int

@dataclass(frozen=True) 
class ObjectProjExpr:
    subject : Expr
    label : str


@dataclass(frozen=True) 
class LinkPropProjExpr:
    subject : Expr
    linkprop : str

@dataclass(frozen=True)
class DetachedExpr:
    expr : Expr

@dataclass(frozen=True)
class WithExpr:
    bound : Expr
    next : BindingExpr

@dataclass(frozen=True)
class ForExpr:
    bound : Expr
    next : BindingExpr

@dataclass(frozen=True) 
class FilterOrderExpr:
    subject : Expr
    filter : BindingExpr
    order : BindingExpr

@dataclass(frozen=True) 
class OffsetLimitExpr:
    subject : Expr
    offset : Expr
    limit : Expr
    
@dataclass(frozen=True)
class InsertExpr:
    name : str
    new : Expr

@dataclass(frozen=True) 
class UpdateExpr:
    subject : Expr
    shape : ShapeExpr

# @dataclass(frozen=True)
# class RefIdExpr:
#     refid : int

@dataclass(frozen=True) 
class ShapedExprExpr:
    expr : Expr
    shape : ShapeExpr


@dataclass(frozen=True) 
class BindingExpr: 
    body : Expr


@dataclass(frozen=True)
class ShapeExpr:
    shape : Dict[Label, BindingExpr]


@dataclass(frozen=True)
class UnnamedTupleExpr:
    val : List[Expr]

@dataclass(frozen=True)
class NamedTupleExpr:
    val : Dict[str, Expr]

@dataclass(frozen=True)
class ArrayExpr:
    elems : List[Expr]


#### VALUES

# @dataclass(frozen=True)
# class BinProdVal:
#     label : str
#     marker : Marker
#     this : Val
#     next : DictVal

# @dataclass(frozen=True)
# class BinProdUnitVal:
#     pass


@dataclass(frozen=True)
class ObjectVal:
    val : Dict[Label, Tuple[Marker, MultiSetVal]]

@dataclass(frozen=True)
class FreeVal:
    val : ObjectVal
    
@dataclass(frozen=True)
class RefVal:
    refid : int
    val : ObjectVal

# @dataclass(frozen=True)
# class RefLinkVal:
#     from_id : int
#     to_id : int
#     val : ObjectVal


# @dataclass(frozen=True) 
# class LinkWithPropertyVal:
#     subject : Val
#     link_properties : Val

@dataclass(frozen=True)
class UnnamedTupleVal:
    val : List[Val]

@dataclass(frozen=True)
class NamedTupleVal:
    val : Dict[str, Val]

@dataclass(frozen=True)
class ArrayVal:
    val : List[Val]

# @dataclass(frozen=True)
# class MultiSetVal: # U
#     val : List[Val]
    

Val =  (PrimVal | RefVal | FreeVal 
        # | RefLinkVal | LinkWithPropertyVal 
        | UnnamedTupleVal | NamedTupleVal  | ArrayVal ) # V

MultiSetVal = List[Val]

VarExpr = (FreeVarExpr | BoundVarExpr)

Expr = (PrimVal | TypeCastExpr | FunAppExpr 
        | FreeVarExpr | BoundVarExpr| ObjectProjExpr | LinkPropProjExpr |  WithExpr | ForExpr 
        | FilterOrderExpr | OffsetLimitExpr | InsertExpr | UpdateExpr
        | MultiSetExpr 
        | ShapedExprExpr | ShapeExpr | ObjectExpr | BindingExpr
        | Val | UnnamedTupleExpr | NamedTupleExpr | ArrayExpr
        | Tp | UnionExpr   | DetachedExpr
        )


@dataclass(frozen=True) 
class DBEntry:
    tp : VarTp
    data : ObjectVal ## actually values

@dataclass(frozen=True)
class DB:
    dbdata: Dict[int, DBEntry] 
    # subtp : List[Tuple[TypeExpr, TypeExpr]]



@dataclass(frozen=True)
class BuiltinFuncDef():
    tp : FunType
    impl : Callable[[List[List[Val]]], List[Val]]

@dataclass(frozen=True)
class DBSchema: 
    val : Dict[str, ObjectTp]
    fun_defs : Dict[str, List[BuiltinFuncDef]] # list of definitions to support overloading
    

def empty_db():
    return DB({})


# def add_fun(x, y):
#     match x, y:
#         case [IntVal(a)], [IntVal(b)]:
#             return [IntVal(a + b)]
#     raise ValueError("cannot add ", x , y)



# BuiltinFuncOp : Dict[str, Callable[..., List[Expr]]] = {
#     "+" : add_fun,
# }


starting_id = 0

def next_id():
    global starting_id
    starting_id += 1
    return starting_id

def next_name() -> str:
    return "n" + str(next_id())

# def dict_to_val(data : Dict[str, Val]) -> DictVal:
#     result : DictVal = {}
#     [result := BinProdVal(k, Visible(), v, result) for k,v in reversed(data.items())]
#     return result

def ref(id):
    return RefVal(id, {})