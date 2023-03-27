from __future__ import annotations
from typing import Dict, NamedTuple, Sequence, Tuple, Optional, Callable

from dataclasses import dataclass


# to use when we move to 3.11
# and https://peps.python.org/pep-0681/ is implemented in mypy
# https://github.com/python/mypy/issues/14293
# @dataclass_transformer
# def data(f):
#     return dataclass(f, frozen=True)


# LABELS

@dataclass(frozen=True)
class StrLabel:
    label: str


@dataclass(frozen=True)
class LinkPropLabel:
    label: str


Label = StrLabel | LinkPropLabel

# DEFINE TYPES


@dataclass(frozen=True)
class ObjectTp:
    val: Dict[str, ResultTp]


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
class IntInfTp:
    pass


@dataclass(frozen=True)
class DateTimeTp:
    pass


@dataclass(frozen=True)
class JsonTp:
    pass


PrimTp = StrTp | IntTp | IntInfTp | BoolTp | DateTimeTp | JsonTp


@dataclass(frozen=True)
class VarTp:
    name: str


@dataclass(frozen=True)
class NamedTupleTp:
    val: Dict[str, Tp]


@dataclass(frozen=True)
class UnnamedTupleTp:
    val: Sequence[Tp]


@dataclass(frozen=True)
class ArrTp:
    tp: Tp


@dataclass(frozen=True)
class UnionTp:
    left: Tp
    right: Tp


@dataclass(frozen=True)
class IntersectTp:
    left: Tp
    right: Tp


@dataclass(frozen=True)
class LinkPropTp:
    subject: Tp
    linkprop: ObjectTp


@dataclass(frozen=True)
class AnyTp:
    pass


@dataclass(frozen=True)
class SomeTp:
    index: int


Tp = (ObjectTp | PrimTp | VarTp | LinkPropTp | NamedTupleTp | UnnamedTupleTp
      | ArrTp | AnyTp | SomeTp | UnionTp | IntersectTp)


@dataclass(frozen=True)
class Visible:
    pass


@dataclass(frozen=True)
class Invisible:
    pass


Marker = Visible | Invisible


# DEFINE CARDINALITIES

@dataclass(frozen=True)
class FiniteCardinal:
    value: int

    def __add__(self, other):
        match other:
            case FiniteCardinal(otherCard):
                return FiniteCardinal(self.value + otherCard)
            case InfiniteCardinal():
                return InfiniteCardinal()
        raise ValueError()

    def __mul__(self, other: Cardinal):
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
            case FiniteCardinal(_):
                return InfiniteCardinal()
            case InfiniteCardinal():
                return InfiniteCardinal()
        raise ValueError()

    def __mul__(self, other: Cardinal):
        match other:
            case FiniteCardinal(_):
                return InfiniteCardinal()
            case InfiniteCardinal():
                return InfiniteCardinal()
        raise ValueError()


Cardinal = FiniteCardinal | InfiniteCardinal


def Inf():
    return InfiniteCardinal()


def Fin(i):
    return FiniteCardinal(i)


@dataclass(frozen=True)
class CMMode:
    lower: Cardinal
    upper: Cardinal
    multiplicity: Cardinal = None  # type: ignore

    def __post_init__(self):
        if self.multiplicity is None:
            object.__setattr__(self, 'multiplicity', self.upper)

    def __add__(self, other: CMMode):
        return CMMode(self.lower + other.lower,
                      self.upper + other.upper,
                      self.multiplicity + other.multiplicity)

    def __mul__(self, other: CMMode):
        return CMMode(self.lower * other.lower,
                      self.upper * other.upper,
                      self.multiplicity * other.multiplicity)


CardOne = CMMode(Fin(1), Fin(1))
CardAtMostOne = CMMode(Fin(0), Fin(1))
CardAtLeastOne = CMMode(Fin(1), Inf())
CardAny = CMMode(Fin(0), Inf())

ResultTp = Tuple[Tp, CMMode]


# DEFINE PARAMETER MODIFIERS

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
class FunArgRetType:
    args_tp: Sequence[Tp]
    ret_tp: ResultTp


@dataclass(frozen=True)
class FunType:
    # all (overloaded) args need to have the same modifier
    args_mod: Sequence[ParamModifier]
    args_ret_types: Sequence[FunArgRetType]

# DEFINE PRIM VALUES


@dataclass(frozen=True, order=True)
class StrVal:
    val: str


@dataclass(frozen=True, order=True)
class IntVal:
    val: int


@dataclass(frozen=True)
class DateTimeVal:
    val: str


@dataclass(frozen=True)
class JsonVal:
    val: str


@dataclass(frozen=True)
class FunVal:
    fname: str


@dataclass(frozen=True)
class IntInfVal:
    """ the infinite integer, used as the default value for limit """
    pass


@dataclass(frozen=True)
class BoolVal:
    val: bool


PrimVal = (StrVal | IntVal | FunVal | IntInfVal | BoolVal
           | DateTimeVal | JsonVal)

# DEFINE EXPRESSIONS


@dataclass(frozen=True)
class UnionExpr:
    left: Expr
    right: Expr


@dataclass(frozen=True)
class MultiSetExpr:
    expr: Sequence[Expr]


@dataclass(frozen=True)
class TypeCastExpr:
    tp: Tp
    arg: Expr


@dataclass(frozen=True)
class FunAppExpr:
    fun: str
    overloading_index: Optional[int]
    args: Sequence[Expr]


@dataclass(frozen=True)
class ObjectExpr:
    val: Dict[Label, Expr]


@dataclass(frozen=True)
class FreeVarExpr:
    var: str


@dataclass(frozen=True)
class BoundVarExpr:
    var: str


@dataclass(frozen=True)
class ObjectProjExpr:
    subject: Expr
    label: str


@dataclass(frozen=True)
class BackLinkExpr:
    subject: Expr
    label: str


@dataclass(frozen=True)
class TpIntersectExpr:
    subject: Expr
    tp: str


@dataclass(frozen=True)
class LinkPropProjExpr:
    subject: Expr
    linkprop: str


@dataclass(frozen=True)
class SubqueryExpr:  # select e in formalism
    expr: Expr


@dataclass(frozen=True)
class DetachedExpr:
    expr: Expr


@dataclass(frozen=True)
class WithExpr:
    bound: Expr
    next: BindingExpr


@dataclass(frozen=True)
class ForExpr:
    bound: Expr
    next: BindingExpr


@dataclass(frozen=True)
class OptionalForExpr:
    bound: Expr
    next: BindingExpr


@dataclass(frozen=True)
class FilterOrderExpr:
    subject: Expr
    filter: BindingExpr
    order: BindingExpr


@dataclass(frozen=True)
class OffsetLimitExpr:
    subject: Expr
    offset: Expr
    limit: Expr


@dataclass(frozen=True)
class InsertExpr:
    name: str
    new: Expr


@dataclass(frozen=True)
class UpdateExpr:
    subject: Expr
    shape: ShapeExpr

# @dataclass(frozen=True)
# class RefIdExpr:
#     refid : int


@dataclass(frozen=True)
class ShapedExprExpr:
    expr: Expr
    shape: ShapeExpr


@dataclass(frozen=True)
class BindingExpr:
    var: str
    body: Expr


@dataclass(frozen=True)
class ShapeExpr:
    shape: Dict[Label, BindingExpr]


@dataclass(frozen=True)
class UnnamedTupleExpr:
    val: Sequence[Expr]


@dataclass(frozen=True)
class NamedTupleExpr:
    val: Dict[str, Expr]


@dataclass(frozen=True)
class ArrExpr:
    elems: Sequence[Expr]


# VALUES

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
    val: Dict[Label, Tuple[Marker, MultiSetVal]]


@dataclass(frozen=True)
class FreeVal:
    val: ObjectVal


@dataclass(frozen=True)
class RefVal:
    refid: int
    val: ObjectVal

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
    val: Sequence[Val]


@dataclass(frozen=True)
class NamedTupleVal:
    val: Dict[str, Val]


@dataclass(frozen=True)
class ArrVal:
    val: Sequence[Val]


@dataclass(frozen=True)
class LinkPropVal:
    refid: int
    linkprop: ObjectVal

# @dataclass(frozen=True)
# class MultiSetVal: # U
#     val : Sequence[Val]


Val = (PrimVal | RefVal | FreeVal
       # | RefLinkVal | LinkWithPropertyVal
       | UnnamedTupleVal | NamedTupleVal | ArrVal | LinkPropVal)  # V

MultiSetVal = Sequence[Val]

VarExpr = (FreeVarExpr | BoundVarExpr)

Expr = (
    PrimVal | TypeCastExpr | FunAppExpr | FreeVarExpr | BoundVarExpr |
    ObjectProjExpr | LinkPropProjExpr | WithExpr | ForExpr | OptionalForExpr |
    TpIntersectExpr | BackLinkExpr | FilterOrderExpr | OffsetLimitExpr |
    InsertExpr | UpdateExpr | MultiSetExpr | ShapedExprExpr | ShapeExpr |
    ObjectExpr | BindingExpr | Val | UnnamedTupleExpr | NamedTupleExpr |
    ArrExpr | Tp | UnionExpr | DetachedExpr | SubqueryExpr)


@dataclass(frozen=True)
class DBEntry:
    tp: VarTp
    data: ObjectVal  # actually values


@dataclass(frozen=True)
class DB:
    dbdata: Dict[int, DBEntry]
    # subtp : Sequence[Tuple[TypeExpr, TypeExpr]]


@dataclass(frozen=True)
class BuiltinFuncDef():
    tp: FunType
    impl: Callable[[Sequence[Sequence[Val]]], Sequence[Val]]


@dataclass(frozen=True)
class DBSchema:
    val: Dict[str, ObjectTp]
    fun_defs: Dict[str, BuiltinFuncDef]

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


def empty_db():
    return DB({})


# def add_fun(x, y):
#     match x, y:
#         case [IntVal(a)], [IntVal(b)]:
#             return [IntVal(a + b)]
#     raise ValueError("cannot add ", x , y)


# BuiltinFuncOp : Dict[str, Callable[..., Sequence[Expr]]] = {
#     "+" : add_fun,
# }

starting_id = 0


def next_id():
    global starting_id
    starting_id += 1
    return starting_id


def next_name(prefix: str = "n") -> str:
    return prefix + str(next_id())


def ref(id):
    return RefVal(id, {})


OrderLabelSep = "-"  # separates components of an order object label
OrderAscending = "ascending"
OrderDescending = "descending"


IndirectionIndexOp = "_[_]"
IndirectionSliceOp = "_[_:_]"
IfElseOp = "std::IF:_if_else_"
