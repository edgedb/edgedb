from __future__ import annotations
from typing import (
    Dict,
    NamedTuple,
    Sequence,
    Tuple,
    Optional,
    Callable,
    List,
    Any,
)

from dataclasses import dataclass

from enum import Enum


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
    """Object Type encapsulating val: Dict[str, ResultTp]"""

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


class TpCastKind(Enum):
    Implicit = "implicit"  # implicit includes assignment
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

    def __post_init__(self):
        if not isinstance(self.labels, list):
            raise ValueError("labels must be a list")


def ArrTp(tp: Tp):
    return CompositeTp(CompositeTpKind.Array, [tp], [])


def UnnamedTupleTp(tps: List[Tp]):
    return CompositeTp(CompositeTpKind.Tuple, tps, [])


def NamedTupleTp(val: Dict[str, Tp]):
    lbls = [*val.keys()]
    tps = [*val.values()]
    return CompositeTp(CompositeTpKind.Tuple, tps, lbls)


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

    def __post_init__(self):
        if not isinstance(self.linkprop, ObjectTp):
            raise ValueError("linkprop must be an object type")


@dataclass(frozen=True)
class UncheckedTypeName:
    name: RawName


@dataclass(frozen=True)
class NominalLinkTp:
    subject: ObjectTp
    name: QualifiedName
    linkprop: ObjectTp

    def __post_init__(self):
        if not isinstance(self.linkprop, ObjectTp):
            raise ValueError("linkprop must be an object type")


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
    """place holder for a overloaded type"""

    linkprop: Optional[ObjectTp]  # overloaded or additional link props


@dataclass(frozen=True)
class DefaultTp:
    expr: BindingExpr
    tp: Tp

    def __hash__(self) -> int:
        return hash(self.tp)


@dataclass(frozen=True)
class AnyTp:
    specifier: Optional[str] = None


@dataclass(frozen=True)
class SomeTp:
    index: int


Tp = (
    ObjectTp
    | NamedNominalLinkTp
    | NominalLinkTp
    | ScalarTp
    | UncheckedTypeName
    | CompositeTp
    | AnyTp
    | SomeTp
    | UnionTp
    | IntersectTp
    | OverloadedTargetTp
    | ComputableTp
    | DefaultTp
    | UncheckedComputableTp
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
        assert not isinstance(
            other, InfiniteCardinal
        ), "Cannot multiply zero by inf"
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
        assert not isinstance(
            other, ZeroCardinal
        ), "cannot multiply zero by inf"
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

    def __add__(self, other: CMMode):
        new_lower = self.lower + other.lower
        return CMMode(
            new_lower if new_lower != CardNumInf else CardNumOne,
            self.upper + other.upper,
        )

    def __mul__(self, other: CMMode):
        return CMMode(self.lower * other.lower, self.upper * other.upper)


CardOne = CMMode(CardNumOne, CardNumOne)
CardAtMostOne = CMMode(CardNumZero, CardNumOne)
CardAtLeastOne = CMMode(CardNumOne, CardNumInf)
CardAny = CMMode(CardNumZero, CardNumInf)


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


@dataclass(frozen=True, order=True)
class ScalarVal:
    tp: ScalarTp
    val: Any

    def __post_init__(self):
        if self.tp == ScalarTp(
            name=QualifiedName(["std", "int64"])
        ) and not isinstance(self.val, int):
            raise ValueError("val must be an int")
        if self.val is None:
            raise ValueError("val cannot be None")


def IntVal(val: int):
    return ScalarVal(IntTp(), val)


def StrVal(val: str):
    return ScalarVal(StrTp(), val)


def BoolVal(val: bool):
    return ScalarVal(BoolTp(), val)


EdgeID = int


def UuidVal(val: EdgeID):
    return ScalarVal(UuidTp(), val)


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

    def __post_init__(self):
        if not isinstance(self.names, list) or not all(
            isinstance(name, str) for name in self.names
        ):
            raise ValueError(
                "The 'names' attribute must be a non-empty list of strings."
            )


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
class IsTpExpr:
    subject: Expr
    tp: Tp | RawName


@dataclass(frozen=True)
class TpIntersectExpr:
    subject: Expr
    tp: Tp | RawName


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
class IfElseExpr:
    then_branch: Expr
    condition: Expr
    else_branch: Expr


@dataclass
class EdgeDatabaseEqFilter:
    propname: str
    # arg is a disjunction,
    # both .propname = <arg> and .propanme IN <arg>
    # are treated equivalently
    arg: Expr | MultiSetVal


@dataclass
class EdgeDatabaseConjunctiveFilter:
    conjuncts: Sequence[EdgeDatabaseSelectFilter]


@dataclass
class EdgeDatabaseDisjunctiveFilter:
    disjuncts: Sequence[EdgeDatabaseSelectFilter]


@dataclass
class EdgeDatabaseTrueFilter:
    pass


EdgeDatabaseSelectFilter = (
    EdgeDatabaseEqFilter
    | EdgeDatabaseConjunctiveFilter
    | EdgeDatabaseDisjunctiveFilter
    | EdgeDatabaseTrueFilter
)


@dataclass(frozen=True)
class QualifiedNameWithFilter:
    name: QualifiedName
    filter: EdgeDatabaseSelectFilter


@dataclass(frozen=True)
class FilterOrderExpr:
    subject: Expr
    filter: BindingExpr
    order: Dict[str, BindingExpr]  # keys are order-specifying list


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


@dataclass(frozen=True)
class ObjectVal:
    val: Dict[Label, Tuple[Marker, MultiSetVal]]

    def __post_init__(self):
        for lbl, (marker, val) in self.val.items():
            if not isinstance(val, MultiSetVal):
                raise ValueError("val must be a MultiSetVal")
            if not isinstance(marker, Marker):  # type: ignore
                raise ValueError("marker must be a Marker")
            if not isinstance(lbl, Label):  # type: ignore
                raise ValueError("label must be a Label")


@dataclass(frozen=True)
class RefVal:
    refid: int
    tpname: QualifiedName
    val: ObjectVal

    def __post_init__(self):
        if not isinstance(self.val, ObjectVal):
            raise ValueError("val must be an ObjectVal")
        if not isinstance(self.tpname, QualifiedName):
            raise ValueError("tpname must be a QualifiedName")


@dataclass(frozen=True, order=True)
class UnnamedTupleVal:
    val: Sequence[Val]


@dataclass(frozen=True)
class NamedTupleVal:
    val: Dict[str, Val]


@dataclass(frozen=True)
class ArrVal:
    val: Sequence[Val]


@dataclass(frozen=True, order=True)
class ResultMultiSetVal:
    _vals: Sequence[Val]
    # singleton: bool = False

    def getVals(self) -> Sequence[Val]:
        return self._vals

    def getRawVals(self) -> Sequence[Val]:
        return self._vals

    def __post_init__(self):
        if not isinstance(self._vals, list) or not all(
            isinstance(v, Val) for v in self._vals  # type: ignore
        ):
            raise ValueError("vals must be a list")


MultiSetVal = ResultMultiSetVal


Val = ScalarVal | RefVal | UnnamedTupleVal | NamedTupleVal | ArrVal


VarExpr = FreeVarExpr | BoundVarExpr

Expr = (
    ScalarVal
    | TypeCastExpr
    | FunAppExpr
    | FreeVarExpr
    | BoundVarExpr
    | CheckedTypeCastExpr
    | ObjectProjExpr
    | LinkPropProjExpr
    | WithExpr
    | ForExpr
    | OptionalForExpr
    | TpIntersectExpr
    | BackLinkExpr
    | FilterOrderExpr
    | OffsetLimitExpr
    | QualifiedNameWithFilter
    | InsertExpr
    | UpdateExpr
    | MultiSetExpr
    | ShapedExprExpr
    | ShapeExpr
    | FreeObjectExpr
    | ConditionalDedupExpr
    | TupleProjExpr
    | IsTpExpr
    | BindingExpr
    | Val
    | UnnamedTupleExpr
    | NamedTupleExpr
    | ParameterExpr
    | ArrExpr
    | Tp
    | UnionExpr
    | DetachedExpr
    | SubqueryExpr
    | IfElseExpr
    | DeleteExpr
    | QualifiedName
)


@dataclass(frozen=True)
class DBEntry:
    tp: QualifiedName
    data: Dict[str, MultiSetVal]


@dataclass(frozen=True)
class DB:
    dbdata: Dict[int, DBEntry]


@dataclass(frozen=True)
class BuiltinFuncDef:
    tp: FunArgRetType
    impl: Callable[[Sequence[Sequence[Val]]], Sequence[Val]]
    defaults: Dict[str, Expr]


@dataclass(frozen=True)
class DefinedFuncDef:
    tp: FunArgRetType
    impl: Expr  # Has the same number of bindings as num_args = len(tp.args_tp)
    defaults: Dict[str, Expr]


FuncDef = BuiltinFuncDef | DefinedFuncDef


@dataclass(frozen=True)
class ExclusiveConstraint:
    name: str
    delegated: bool


@dataclass(frozen=True)
class ExpressionConstraint:
    expr: Expr


Constraint = ExclusiveConstraint | ExpressionConstraint


@dataclass(frozen=True)
class ModuleEntityTypeDef:
    typedef: ObjectTp | ScalarTp
    is_abstract: bool
    constraints: Sequence[Constraint]
    # Indexes are a list of indexed properties (as a tuple),
    # e.g. if a type has (.a), (.a, .b) as indexes, we have [[.a], [.a, .b]]
    indexes: Sequence[Sequence[str]]


@dataclass(frozen=True)
class ModuleEntityFuncDef:
    funcdefs: List[FuncDef]


ModuleEntity = ModuleEntityTypeDef | ModuleEntityFuncDef


@dataclass(frozen=True)
class DBModule:
    defs: Dict[str, ModuleEntity]


ModuleName = Tuple[str, ...]


@dataclass(frozen=True)
class DBSchema:
    modules: Dict[Tuple[str, ...], DBModule]

    # modules that are currently under type checking
    unchecked_modules: Dict[Tuple[str, ...], DBModule]

    # subtyping_relations: indexed by subtypes,
    # subtype -> immediate super types mapping
    subtyping_relations: Dict[QualifiedName, List[QualifiedName]]
    unchecked_subtyping_relations: Dict[
        QualifiedName, List[Tuple[Tuple[str, ...], RawName]]
    ]  # name -> current declared module and raw name
    casts: Dict[Tuple[Tp, Tp], TpCast]


class RTExpr(NamedTuple):
    cur_db: DB
    expr: Expr


class RTVal(NamedTuple):
    cur_db: DB
    val: MultiSetVal


@dataclass
class TcCtx:
    schema: DBSchema
    current_module: Tuple[
        str, ...
    ]  # current module name, TODO: nested modules
    varctx: Dict[str, ResultTp]


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
