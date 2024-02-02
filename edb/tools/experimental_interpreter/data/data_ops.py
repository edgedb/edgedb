from __future__ import annotations
from typing import Dict, NamedTuple, Sequence, Tuple, Optional, Callable, List, Any

from dataclasses import dataclass

from enum import Enum

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
    """ Object Type encapsulating val: Dict[str, ResultTp] """
    val: Dict[str, ResultTp]

    def __hash__(self):
        return hash(tuple(self.val.items()))

@dataclass(frozen=True)
class ScalarTp:
    name: QualifiedName


def BoolTp():
    return ScalarTp(QualifiedName(["std", "bool"]))
def StrTp():
    return ScalarTp(QualifiedName(["std", "str"]))
def IntTp():
    return ScalarTp(QualifiedName(["std", "int64"]))
def UuidTp():
    return ScalarTp(QualifiedName(["std", "uuid"]))



# @dataclass(frozen=True)
# class StrTp:
#     pass


# @dataclass(frozen=True)
# class BoolTp:
#     pass


# @dataclass(frozen=True)
# class IntTp:
#     pass


# @dataclass(frozen=True)
# class IntInfTp:
#     pass


# @dataclass(frozen=True)
# class DateTimeTp:
#     pass


# @dataclass(frozen=True)
# class JsonTp:
#     pass

# @dataclass(frozen=True)
# class UuidTp:
#     pass


# PrimTp = StrTp | IntTp | IntInfTp | BoolTp | DateTimeTp | JsonTp | UuidTp |
# ScalarTp


# @dataclass(frozen=True)
# class VarTp:
#     name: str


@dataclass(frozen=True)
class NamedTupleTp:
    val: Dict[str, Tp]


# @dataclass(frozen=True)
# class UnnamedTupleTp:
#     val: Sequence[Tp]

class TpCastKind(Enum):
    Implicit = "implicit" # implicit includes assignment
    Assignment = "assignment"
    Explicit = "explicit"

@dataclass(frozen=True)
class TpCast:
    kind: TpCastKind
    cast_fun: Callable[[Val], Val]

class CompositeTpKind(Enum):
    Array = "array"
    Tuple = "tuple"
    Enum = "enum"
    Range = "range"
    MultiRange = "multirange"
    

@dataclass(frozen=True)
class CompositeTp:
    kind: CompositeTpKind
    tps: List[Tp]
    labels: List[str]

    def __hash__(self):
        return hash((self.kind, tuple(self.tps), tuple(self.labels)))

# @dataclass(frozen=True)
# class TupleTp:
#     tps: List[Tp]
def ArrTp(tp: Tp):
    return CompositeTp(CompositeTpKind.Array, [tp], [])

def UnnamedTupleTp(tps: List[Tp]):
    return CompositeTp(CompositeTpKind.Tuple, tps, [])


@dataclass(frozen=True)
class UnionTp:
    left: Tp
    right: Tp

    def __post_init__(self):
        if self.left == self.right:
            raise ValueError("Do not use union tp for identical types")


@dataclass(frozen=True)
class IntersectTp:
    left: Tp
    right: Tp

    def __post_init__(self):
        if self.left == self.right:
            raise ValueError("Do not use intersect tp for identical types")


@dataclass(frozen=True)
class NamedNominalLinkTp:
    name: RawName
    linkprop: ObjectTp
    
@dataclass(frozen=True)
class UncheckedTypeName:
    name: RawName

# @dataclass(frozen=True)
# class UncheckedNamedNominalLinkTp:
#     name: str
#     linkprop: ObjectTp

@dataclass(frozen=True)
class NominalLinkTp:
    subject: ObjectTp
    name : QualifiedName
    linkprop: ObjectTp


@dataclass(frozen=True)
class ComputableTp:
    expr: BindingExpr
    tp: Tp


# Computable Tp Pending Type Inference
@dataclass(frozen=True)
class UncheckedComputableTp:
    expr: BindingExpr

@dataclass(frozen=True)
class OverloadedTargetTp:
    """ place holder for a overloaded type"""
    linkprop : Optional[ObjectTp] # overloaded or additional link props

@dataclass(frozen=True)
class DefaultTp:
    expr: BindingExpr
    tp: Tp


@dataclass(frozen=True)
class AnyTp:
    specifier: Optional[str] = None


@dataclass(frozen=True)
class SomeTp:
    index: int


# implementation trick for synthesizing the empty type
# @dataclass
# class UnifiableTp:
#     id: int
#     resolution: Optional[Tp] = None


Tp = (ObjectTp | NamedNominalLinkTp  | NominalLinkTp | ScalarTp | UncheckedTypeName
      | NamedTupleTp 
      | CompositeTp | AnyTp | SomeTp | UnionTp | IntersectTp  | OverloadedTargetTp
    #   | UnifiableTp
      | ComputableTp | DefaultTp | UncheckedComputableTp 
    #   | UncheckedNamedNominalLinkTp
      )


@dataclass(frozen=True)
class Visible:
    pass


@dataclass(frozen=True)
class Invisible:
    pass


Marker = Visible | Invisible


# DEFINE CARDINALITIES


@dataclass(frozen=True)
class ZeroCardinal:
    def __add__(self, other):
        return other

    def __mul__(self, other: Cardinal):
        assert not isinstance(other, InfiniteCardinal), "Cannot multiply zero by inf"
        return self

    def __le__(self, other: Cardinal):
        return True


@dataclass(frozen=True)
class OneCardinal:
    def __add__(self, other: Cardinal):
        match other:
            case ZeroCardinal():
                return OneCardinal()
            case OneCardinal():
                return InfiniteCardinal()
            case InfiniteCardinal():
                return InfiniteCardinal()
        raise ValueError()

    def __mul__(self, other: Cardinal):
        return other

    def __le__(self, other: Cardinal):
        match other:
            case ZeroCardinal():
                return False
            case OneCardinal():
                return True
            case InfiniteCardinal():
                return True
        raise ValueError()

@dataclass(frozen=True)
class InfiniteCardinal:
    def __add__(self, other: Cardinal):
        return InfiniteCardinal()

    def __mul__(self, other: Cardinal):
        assert not isinstance(other, ZeroCardinal), "cannot multiply zero by inf"
        return InfiniteCardinal()

    def __le__(self, other: Cardinal):
        match other:
            case InfiniteCardinal():
                return True
            case OneCardinal():
                return False
            case ZeroCardinal():
                return False
        raise ValueError()


Cardinal = ZeroCardinal | OneCardinal | InfiniteCardinal
LowerCardinal = ZeroCardinal | OneCardinal
UpperCardinal = OneCardinal | InfiniteCardinal

CardNumZero = ZeroCardinal()
CardNumOne = OneCardinal()
CardNumInf = InfiniteCardinal()


def max_cardinal(a: Cardinal, b: Cardinal):
    if a <= b:
        return b
    else:
        return a


def min_cardinal(a: Cardinal, b: Cardinal):
    if a <= b:
        return a
    else:
        return b

@dataclass(frozen=True)
class CMMode:
    lower: LowerCardinal
    upper: UpperCardinal
    # multiplicity: Cardinal = None  # type: ignore

    # def __post_init__(self):
    #     if self.multiplicity is None:
    #         object.__setattr__(self, 'multiplicity', self.upper)

    def __add__(self, other: CMMode):
        new_lower = self.lower + other.lower
        return CMMode(new_lower if new_lower != CardNumInf else CardNumOne,
                      self.upper + other.upper)
                      

    def __mul__(self, other: CMMode):
        return CMMode(self.lower * other.lower,
                      self.upper * other.upper)


# CardZero = CMMode(CardNumZero, CardNumZero)
CardOne = CMMode(CardNumOne, CardNumOne)
CardAtMostOne = CMMode(CardNumZero, CardNumOne)
CardAtLeastOne = CMMode(CardNumOne, CardNumInf)
CardAny = CMMode(CardNumZero, CardNumInf)

# ResultTp = Tuple[Tp, CMMode]


class ResultTp(NamedTuple):
    tp: Tp
    mode: CMMode


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
    args_mod: Sequence[ParamModifier]
    args_label: Sequence[str]
    ret_tp: ResultTp


# @dataclass(frozen=True)
# class FunType:
#     args_ret_types: List[FunArgRetType]
    # effect_free: bool = False

# DEFINE PRIM VALUES


# @dataclass(frozen=True, order=True)
# class StrVal:
#     val: str


# @dataclass(frozen=True, order=True)
# class IntVal:
#     val: int


# @dataclass(frozen=True)
# class DateTimeVal:
#     val: str


# @dataclass(frozen=True)
# class JsonVal:
#     val: str


# @dataclass(frozen=True)
# class FunVal:
#     fname: str

@dataclass(frozen=True, order=True)
class ScalarVal:
    tp: ScalarTp
    val: Any

def IntVal(val: int):
    return ScalarVal(IntTp(), val)

def StrVal(val: str):
    return ScalarVal(StrTp(), val)

def BoolVal(val: bool):
    return ScalarVal(BoolTp(), val)

EdgeID = int

def UuidVal(val: EdgeID):
    return ScalarVal(UuidTp(), val)




# @dataclass(frozen=True)
# class IntInfVal:
#     """ the infinite integer, used as the default value for limit """
#     pass


# @dataclass(frozen=True)
# class BoolVal:
#     val: bool


# PrimVal = (Scalar)

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
class CheckedTypeCastExpr:
    cast_tp: Tuple[Tp, Tp]
    cast_spec: TpCast
    arg: Expr

@dataclass(frozen=True)
class ParameterExpr:
    name: str
    tp: Tp
    is_required: bool


@dataclass(frozen=True)
class FunAppExpr:
    fun: UnqualifiedName | QualifiedName
    overloading_index: Optional[int]
    args: Sequence[Expr]
    kwargs: Dict[str, Expr]


# @dataclass(frozen=True)
# class ObjectExpr:
#     val: Dict[Label, Expr]

@dataclass(frozen=True)
class FreeObjectExpr:
    pass

@dataclass(frozen=True)
class ConditionalDedupExpr:
    expr: Expr


@dataclass(frozen=True)
class FreeVarExpr:
    var: str



@dataclass(frozen=True)
class BoundVarExpr:
    var: str

@dataclass(frozen=True)
class QualifiedName:
    names: List[str]

    def __hash__(self):
        return hash(tuple(self.names))


    # def __post_init__(self):
    #     if not isinstance(self.names, list) or not all(isinstance(name, str) for name in self.names):
    #         raise ValueError("The 'names' attribute must be a non-empty list of strings.")


@dataclass(frozen=True)
class UnqualifiedName:
    name: str

RawName = UnqualifiedName | QualifiedName

@dataclass(frozen=True)
class ObjectProjExpr:
    subject: Expr
    label: str

@dataclass(frozen=True)
class TupleProjExpr:
    subject: Expr
    label: str


@dataclass(frozen=True)
class BackLinkExpr:
    subject: Expr
    label: str


@dataclass(frozen=True)
class TpIntersectExpr:
    subject: Expr
    tp: Tp


@dataclass(frozen=True)
class LinkPropProjExpr:
    subject: Expr
    linkprop: str


@dataclass(frozen=True)
class SubqueryExpr:  # select e in formalism
    expr: Expr


# @dataclass(frozen=True)
# class SingularExpr:  # select e in formalism
#     expr: Expr


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
class IfElseExpr:
    then_branch: Expr
    condition: Expr
    else_branch: Expr

@dataclass(frozen=True)
class FilterOrderExpr:
    subject: Expr
    filter: BindingExpr
    order: Dict[str, BindingExpr] # keys are order-specifying list


@dataclass(frozen=True)
class OffsetLimitExpr:
    subject: Expr
    offset: Expr
    limit: Expr


@dataclass(frozen=True)
class InsertExpr:
    name: UnqualifiedName | QualifiedName
    new: Dict[str, Expr]


@dataclass(frozen=True)
class UpdateExpr:
    subject: Expr
    shape: ShapeExpr


@dataclass(frozen=True)
class DeleteExpr:
    subject: Expr

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


# @dataclass(frozen=True)
# class FreeVal:
#     val: ObjectVal


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


# @dataclass(frozen=True)
# class LinkPropVal:
#     refid: int
#     linkprop: ObjectVal


# TODO: Check the eval_order_by code to make sure 
# emptyfirst/emptylast is handled correctly
@dataclass(frozen=True, order=True)
class ResultMultiSetVal:
    _vals: Sequence[Val]
    # singleton: bool = False

    def getVals(self) -> Sequence[Val]:
        return self._vals

    def getRawVals(self) -> Sequence[Val]:
        return self._vals


# @dataclass(frozen=True, order=True)
# class ConditionalDedupMultiSetVal:
#     _vals: Sequence[Val]

#     def getVals(self) -> Sequence[Val]:
#         from . import expr_ops as eops
#         if all(isinstance(v, RefVal) for v in self._vals):
#             return eops.object_dedup(self._vals)
#         return self._vals
    
#     def getRawVals(self) -> Sequence[Val]:
#         return self._vals

MultiSetVal = ResultMultiSetVal 


Val = (ScalarVal | RefVal | UnnamedTupleVal | NamedTupleVal | ArrVal )  

# MultiSetVal = Sequence[Val]

VarExpr = (FreeVarExpr | BoundVarExpr)

Expr = (
    ScalarVal | TypeCastExpr | FunAppExpr | FreeVarExpr | BoundVarExpr | CheckedTypeCastExpr |
    ObjectProjExpr | LinkPropProjExpr | WithExpr | ForExpr | OptionalForExpr |
    TpIntersectExpr | BackLinkExpr | FilterOrderExpr | OffsetLimitExpr |
    InsertExpr | UpdateExpr | MultiSetExpr | ShapedExprExpr | ShapeExpr |
    FreeObjectExpr | ConditionalDedupExpr | TupleProjExpr | 
    # ObjectExpr | 
    BindingExpr | Val | UnnamedTupleExpr | NamedTupleExpr |
    ArrExpr | Tp | UnionExpr | DetachedExpr | SubqueryExpr
    #   | SingularExpr
    | IfElseExpr | DeleteExpr| QualifiedName)


@dataclass(frozen=True)
class DBEntry:
    tp: QualifiedName
    data: Dict[str, MultiSetVal]


@dataclass(frozen=True)
class DB:
    dbdata: Dict[int, DBEntry]
    # subtp : Sequence[Tuple[TypeExpr, TypeExpr]]


@dataclass(frozen=True)
class BuiltinFuncDef():
    tp: FunArgRetType
    impl: Callable[[Sequence[Sequence[Val]]], Sequence[Val]]
    defaults : Dict[str, Expr] 

@dataclass(frozen=True)
class DefinedFuncDef():
    tp: FunArgRetType
    impl: Expr # Has the same number of bindings as num_args = len(tp.args_tp)
    defaults : Dict[str, Expr]




FuncDef = BuiltinFuncDef | DefinedFuncDef

@dataclass(frozen=True)
class ExclusiveConstraint:
    delegated: bool

@dataclass(frozen=True)
class ExpressionConstraint:
    expr: Expr

Constraint = ExclusiveConstraint


@dataclass(frozen=True)
class ModuleEntityTypeDef:
    typedef: ObjectTp | ScalarTp
    is_abstract: bool
    constraints : Sequence[Constraint]



@dataclass(frozen=True)
class ModuleEntityFuncDef:
    funcdefs: List[FuncDef]

ModuleEntity = ModuleEntityTypeDef | ModuleEntityFuncDef 

@dataclass(frozen=True)
class DBModule:
    defs: Dict[str, ModuleEntity]


ModuleName = Tuple[str, ...]
@dataclass(frozen=True)
# @dataclass
class DBSchema:
    modules : Dict[Tuple[str, ...], DBModule]
    unchecked_modules : Dict[Tuple[str, ...], DBModule] # modules that are currently under type checking
    subtyping_relations: Dict[QualifiedName, List[QualifiedName]] # subtyping: indexed by subtypes, subtype -> immediate super types mapping
    unchecked_subtyping_relations : Dict[QualifiedName, List[Tuple[Tuple[str, ...], RawName]]] # name -> current declared module and raw name
    casts: Dict[Tuple[Tp, Tp], TpCast] 

# RT Stands for Run Time


# @dataclass(frozen=True)
# class RTData:
#     cur_db: DB
#     read_snapshots: Sequence[DB]
#     schema: DBSchema
#     eval_only: bool  # a.k.a. no DML, no effect


class RTExpr(NamedTuple):
    cur_db: DB
    expr: Expr


class RTVal(NamedTuple):
    cur_db: DB
    val: MultiSetVal


@dataclass
class TcCtx:
    schema: DBSchema
    current_module: Tuple[str, ...] # current module name, TODO: nested modules
    varctx: Dict[str, ResultTp]



class SubtypingMode(Enum):
    # Regular subtyping do not allow missing keys or additional keys
    Regular = 1
    # insert subtyping allow missing keys in subtypes of an object type
    Insert = 2
    # shape subtyping allow additional keys in subtypes of an object type
    Shape = 3


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
OrderEmptyFirst = "emptyfirst"
OrderEmptyLast = "emptylast"


IndirectionIndexOp = "_[_]"
IndirectionSliceStartStopOp = "_[_:_]"
IndirectionSliceStartOp = "_[_:]"
IndirectionSliceStopOp = "_[:_]"
# IfElseOp = "std::IF:_if_else_"
