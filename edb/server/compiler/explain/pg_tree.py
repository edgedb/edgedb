#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# The types here modeled closely after postgres explain output.
# See postgres/src/backend/commands/explain.c
#
from __future__ import annotations
from typing import (
    Annotated,
    Any,
    ClassVar,
    Optional,
    Type,
    TypeVar,
    Union,
    Sequence,
    get_args,
    get_origin,
    get_type_hints,
    NewType,
    Text,
)

import dataclasses
import enum
import re
import uuid

from edb.common import ast

from edb.schema import constraints as s_constr
from edb.schema import indexes as s_indexes
from edb.schema import name as sn
from edb.schema import objects as so
from edb.schema import pointers as s_pointers

from edb.server.compiler import explain
from edb.server.compiler.explain import casefold
from edb.server.compiler.explain import to_json


uuid_core = '[a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12}'
uuid_re = re.compile(
    rf'(\.?"?({uuid_core})"?)',
    re.I
)

T = TypeVar('T')
FromJsonT = TypeVar('FromJsonT', bound='FromJson')


class FromJson(ast.AST, to_json.ToJson):
    @classmethod
    def from_json(
        cls: Type[FromJsonT],
        json: dict[str, Any],
        ctx: explain.AnalyzeContext,
    ) -> FromJsonT:
        annotations = get_type_hints(cls)
        result = cls()
        for name, value in json.items():
            name = casefold.to_snake_case(name)
            if not (prop := annotations.get(name)):
                # extra values are okay
                setattr(result, name, value)
                continue

            if get_origin(prop) is Annotated:
                prop = get_args(prop)[0]
            if get_origin(prop) is Union:  # actually an option
                prop = get_args(prop)[0]
                if value is None:
                    setattr(result, name, value)
                    continue

            if prop is Index:
                setattr(result, name, _translate_index(value, ctx))
            elif prop is Relation:
                setattr(result, name, _translate_relation(value, ctx))
            elif get_origin(prop) is list:
                inner = get_args(prop)[0]
                if type(inner) is type and issubclass(inner, FromJson):
                    setattr(result, name,
                            [inner.from_json(v, ctx) for v in value])
                else:
                    setattr(result, name, value)
            elif type(prop) is type and issubclass(prop, FromJson):
                setattr(result, name, prop.from_json(value, ctx))
            else:
                setattr(result, name, value)

        # lists are always there for convenience
        for name, prop in annotations.items():
            name = casefold.to_snake_case(name)
            if (
                get_origin(prop) is list and
                getattr(result, name, None) is None
            ):
                setattr(result, name, [])

        return result

    def to_json(self) -> Any:
        dic = super().to_json()
        dic['node_type'] = self.__class__.__name__
        return dic


def _obj_to_name(
    sobj: so.Object,
    ctx: explain.AnalyzeContext,
    dotted: bool=False,
) -> str:
    if isinstance(sobj, s_pointers.Pointer):
        # If a pointer is on the RHS of a dot, just use
        # the short name. But otherwise, grab the source
        # and link it up
        s = str(sobj.get_shortname(ctx.schema).name)
        if sobj.is_link_property(ctx.schema):
            s = f'@{s}'
        if not dotted and (src := sobj.get_source(ctx.schema)):
            src_name = _translate_name(
                src.get_name(ctx.schema),
                ctx.reverse_mod_aliases,
            )
            s = f'{src_name}.{s}'
    elif isinstance(sobj, s_constr.Constraint):
        s = sobj.get_verbosename(ctx.schema, with_parent=True)
    elif isinstance(sobj, s_indexes.Index):
        s = sobj.get_verbosename(ctx.schema, with_parent=True)
        if expr := sobj.get_expr(ctx.schema):
            s += f' on ({expr.text})'
    else:
        s = _translate_name(
            sobj.get_name(ctx.schema),
            ctx.reverse_mod_aliases,
        )

    if dotted:
        s = '.' + s

    return s


def _translate_index(name: str, ctx: explain.AnalyzeContext) -> Index:
    # Try to replace all ids with textual names
    had_index = False
    for (full, m) in uuid_re.findall(name):
        uid = uuid.UUID(m)
        sobj = ctx.schema.get_by_id(uid, default=None)
        if sobj:
            had_index |= isinstance(sobj, s_indexes.Index)
            dotted = full[0] == '.'
            s = _obj_to_name(sobj, ctx, dotted=dotted)
            name = uuid_re.sub(s, name, count=1)

    name = name.replace('_source_target_key', ' forward link index')
    name = name.replace(';schemaconstr', '')
    name = name.replace('_target_key', ' backward link index')
    # If the index name is from an actual index or constraint,
    # the `_index` part of the name just total noise, but if it
    # is from a link, it might be slightly informative
    if had_index:
        name = name.replace('_index', '')
    else:
        name = name.replace('_index', ' index')
    return Index(name)


def _translate_relation(name: str, ctx: explain.AnalyzeContext) -> Relation:
    try:
        id = uuid.UUID(name)
    except ValueError:
        # For introspection queries there are tables are named pg_*
        return Relation(name)
    return Relation(_obj_to_name(ctx.schema.get_by_id(id), ctx))


def _translate_name(
    name: sn.Name,
    reverse_mod_aliases: dict[str, Optional[str]],
) -> str:
    if not isinstance(name, sn.QualName):
        return str(name)
    if name.module in reverse_mod_aliases:
        module = reverse_mod_aliases[name.module]
        if module is None:
            return name.name
        else:
            return f"{module}::{name.name}"
    else:
        module = name.module
        suffix = f"::{name.name}"
        while True:
            # looking for the longest prefix first
            try:
                prefix, submodule = module.rsplit("::", 1)
            except ValueError:
                return str(name)
            suffix = f"::{submodule}{suffix}"

            # Note: we don't strip default alias here so only absolute paths
            # are generated
            if aliased := reverse_mod_aliases.get(prefix):
                return aliased + suffix

            module = prefix

# Legend:
# * show, shown -- something visible in the text explain
# * if xxx -- means some condition when parameter exists, option to explain
# * str values also often have a list of options in the comment
#   (we do not use enums, because no exhaustivity guarantee)
# * `kB` is unit for these values
# * no key with list == empty list


Expr = NewType('Expr', str)
Kbytes = NewType('Kbytes', int)
Millis = NewType('Millis', float)
Index = NewType('Index', str)
Relation = NewType('Relation', str)


class PropType(enum.Enum):
    KBYTES = "kB"
    MILLIS = "ms"
    EXPR = "expr"
    INDEX = "index"
    RELATION = "relation"
    TEXT = "text"
    INT = "int"
    FLOAT = "float"

    LIST_KBYTES = "list:kB"
    LIST_MILLIS = "list:ms"
    LIST_EXPR = "list:expr"
    LIST_INDEX = "list:index"
    LIST_RELATION = "list:relation"
    LIST_TEXT = "list:text"
    LIST_INT = "list:int"
    LIST_FLOAT = "list:float"


TYPES = {
    Kbytes: PropType.KBYTES,
    Millis: PropType.MILLIS,
    Expr: PropType.EXPR,
    Index: PropType.INDEX,
    Relation: PropType.RELATION,
    str: PropType.TEXT,
    int: PropType.INT,
    float: PropType.FLOAT,
}


class Important:
    __slots__ = ()


important = Important()


@dataclasses.dataclass
class PropInfo:
    type: Type[object]
    enum_type: PropType
    important: bool


class JitOptions(FromJson):
    # show all
    inlining: bool
    expressions: bool
    optimization: bool
    deforming: bool


class JitTiming(FromJson):
    generation: float  # ms
    inilining: float  # ms
    optimization: float  # ms
    emission: float  # ms
    total: float  # ms


class JitInfo(FromJson):
    functions: int
    options: JitOptions
    timing: JitTiming


class Worker(FromJson):
    worker_number: int
    actual_startup_time: Optional[float]  # if timing
    actual_total_time: Optional[float]  # if timing
    actual_rows: Optional[float]
    actual_loops: Optional[float]

    jit: Optional[JitInfo]  # if bunch of options


PlanT = TypeVar('PlanT', bound='Plan')


@dataclasses.dataclass(kw_only=True)
class CostMixin:
    # if cost
    startup_cost: float
    total_cost: float
    plan_rows: float
    plan_width: int

    # if analyze (zeros if never executed)
    actual_startup_time: Optional[float] = None  # if timing
    actual_total_time: Optional[float] = None  # if timing
    actual_rows: Optional[float] = None
    actual_loops: Optional[float] = None

    # if buffers
    shared_hit_blocks: Optional[int] = None
    shared_read_blocks: Optional[int] = None
    shared_dirtied_blocks: Optional[int] = None
    shared_written_blocks: Optional[int] = None
    local_hit_blocks: Optional[int] = None
    local_read_blocks: Optional[int] = None
    local_dirtied_blocks: Optional[int] = None
    local_written_blocks: Optional[int] = None
    temp_read_blocks: Optional[int] = None
    temp_written_blocks: Optional[int] = None


class Plan(FromJson, CostMixin):
    # TODO(tailhook) output is lost somewhere
    node_type: str
    plan_id: uuid.UUID
    parent_relationship: Optional[str]
    subplan_name: Optional[str]  # shown
    parallel_aware: bool  # true always shown as a prefix of node name
    async_capable: bool  # true always shown as a prefix of node name
    workers: Sequence[Worker]  # shown if non-empty

    plans: list[Plan]

    __subclasses: ClassVar[dict[str, Type[Plan]]] = dict()

    def __init_subclass__(cls, **kwargs: Any):
        super().__init_subclass__(**kwargs)
        cls.__subclasses[cls.__name__] = cls

    @classmethod
    def from_json(
        cls,
        json: dict[str, Any],
        ctx: explain.AnalyzeContext,
    ) -> Plan:
        copy = json.copy()
        copy['plan_id'] = uuid.uuid4()
        node_type = casefold.to_camel_case(copy.pop("Node Type"))
        subclass = cls.__subclasses.get(node_type, cls)
        return super(Plan, subclass).from_json(copy, ctx)

    @classmethod
    def get_props(cls) -> dict[str, PropInfo]:
        result = {}
        for name, prop in get_type_hints(cls, include_extras=True).items():
            if name in CostMixin.__annotations__:
                # these are stored in the node itself
                continue
            if get_origin(prop) is Annotated:
                imp = important in get_args(prop)
                prop = get_args(prop)[0]
            else:
                imp = False
            try:
                if get_origin(prop) is list:
                    enum_type = PropType["LIST_" + TYPES[prop].name]
                elif get_origin(prop) is Union:  # optional
                    enum_type = TYPES[get_args(prop)[0]]
                else:
                    enum_type = TYPES[prop]
            except KeyError:
                # Unknown types are skipped, they are probably
                # nested structures, we don't support yet, and plan_id
                continue

            result[name] = PropInfo(
                type=prop,
                enum_type=enum_type,
                important=imp,
            )
        return result


# Base types

class BaseScan(Plan):
    schema: Optional[str]  # if verbose

    # It should have been required, but in ModifyTable it's optional, so
    # we try to make it compatible. We don't rely on it being required in
    # the code anyways.
    alias: Optional[str]


class RelationScan(BaseScan):
    # It should have been required, but in ModifyTable it's optional, so
    # we try to make it compatible. We don't rely on it being required in
    # the code anyways.
    relation_name: Annotated[Optional[Relation], important]


class FilterScan(FromJson):  # mixin
    filter: Expr
    rows_removed_by_filter: Annotated[Optional[float], important]


# Specific types

class Result(Plan, FilterScan):
    one_time_filter: Expr


class ProjectSet(Plan):
    pass


class TargetTable(FromJson):
    schema: Optional[str]  # if verbose
    alias: Optional[str]
    relation_name: Annotated[Optional[Relation], important]
    cte_name: Optional[str]
    tuplestore_name: Optional[str]
    tablefunction_name: Optional[str]
    function_name: Optional[str]
    # Also pluggable explain FDW


class ModifyTable(RelationScan, TargetTable):
    operation: str  # title
    target_tables: list[TargetTable]  # show, if mult otherwise inherited props

    # Looks like conflict is only possible for single table, but
    # it's not clear
    #
    # if conflict
    conflict_resolution: Optional[str]  # NOTHING, UPDATE
    conflict_arbiter_indexes: list[str]
    conflict_filter: Expr
    rows_removed_by_conflict_filter: float

    tuples_inserted: Optional[float]  # if analyze
    conflicting_tuples: Optional[float]  # if analyze


class Append(Plan):
    pass


class MergeAppend(Plan):
    sort_key: Annotated[list[Expr], important]  # show
    presorted_key: Annotated[list[Expr], important]


class RecursiveUnion(Plan):
    pass


class BitmapAnd(Plan):
    pass


class BitmapOr(Plan):
    pass


class UniqueJoin(Plan, FilterScan):
    inner_unique: bool

    # Inner, Left, Full, Right, Semi, Anti, show
    join_type: Annotated[str, important]

    join_filter: Expr
    rows_removed_by_join_filter: Optional[float]


class NestedLoop(UniqueJoin):
    pass


class MergeJoin(UniqueJoin):
    merge_cond: Expr


class HashJoin(UniqueJoin):
    hash_cond: Expr


class SeqScan(RelationScan, FilterScan):
    pass


class SampleScan(RelationScan, FilterScan):
    sampling_method: Annotated[Text, important]  # show
    sampling_parameters: list[str]
    repeatable_seed: Annotated[Optional[str], important]  # show


class Gather(Plan, FilterScan):
    workers_planned: int
    workers_launched: Optional[int]  # analyze
    params_evaluated: Optional[list[str]]
    single_copy: bool


class GatherMerge(Plan, FilterScan):
    workers_planned: int
    workers_launched: Optional[int]  # analyze
    params_evaluated: Optional[list[str]]


class IndexScan(RelationScan, FilterScan):
    # Backwards, Forward, NoMovement, show: opt Backward
    scan_direction: Annotated[str, important]

    index_name: Annotated[Index, important]  # show
    index_cond: Expr
    rows_removed_by_index_recheck: Annotated[Optional[float], important]
    order_by: Expr


class IndexOnlyScan(IndexScan):
    heap_fetches: Optional[float]  # if analyze


class BitmapIndexScan(Plan):
    index_name: Annotated[Index, important]  # show
    index_cond: Expr


class BitmapHeapScan(RelationScan, FilterScan):
    recheck_cond: Expr
    rows_removed_by_index_recheck: Optional[float]
    exact_heap_blocks: Optional[int]  # if analyze, show
    lossy_heap_blocks: Optional[int]  # if analyze, show


class TidScan(RelationScan, FilterScan):
    tid_cond: Expr


class TidRangeScan(RelationScan, FilterScan):
    tid_cond: Expr


class SubqueryScan(Plan, FilterScan):
    pass


class FunctionScan(BaseScan, FilterScan):
    function_name: str
    function_call: Expr  # if verbose


class TableFunctionScan(BaseScan, FilterScan):
    table_function_name: str  # always == 'xmltable'
    table_function_call: Expr  # if verbose


class ValuesScan(BaseScan, FilterScan):
    pass


class CTEScan(BaseScan, FilterScan):
    cte_name: str


class NamedTuplestoreScan(BaseScan, FilterScan):
    tuplestore_name: str


class WorkTableScan(BaseScan, FilterScan):
    cte_name: str


class ForeignScan(RelationScan, FilterScan):
    operation: Annotated[Optional[str], important]  # show: title


class CustomScan(RelationScan, FilterScan):
    custom_plan_provider: Optional[str]
    # extra info that is gather via custom function :shrug:


class Materialize(Plan):
    pass


class MemoizeWorker(Worker):
    # show if analyze && cache_misses > 0 (probably if enabled)
    cache_hits: int
    cache_misses: int
    cache_evictions: int
    cache_overflows: int
    peak_memory_usage: int  # kB


class Memoize(Plan):
    cache_key: str  # show
    cache_mode: str  # {binary, logical}, show

    # show if analyze && cache_misses > 0 (probably if enabled)
    cache_hits: int
    cache_misses: int
    cache_evictions: int
    cache_overflows: int
    peak_memory_usage: int  # kB

    workers: Sequence[MemoizeWorker]


class SortWorker(Worker):
    sort_method: str  # show
    sort_space_used: int  # show, kB
    sort_space_type: str  # show


class Sort(Plan):
    sort_key: Annotated[list[Expr], important]  # show
    presorted_key: list[Expr]

    sort_method: Annotated[str, important]  # show
    # * still in progress
    # * top-N heapsort
    # * quicksort
    # * external sort
    # * external merge

    sort_space_used: Annotated[Kbytes, important]  # show, kB
    sort_space_type: Annotated[Text, important]  # Disk, Memory, show

    workers: Sequence[SortWorker]  # overrides


class SortSpaceInfo(FromJson):
    average_sort_space_used: Annotated[Kbytes, important]  # kB, show
    peak_sort_space_used: Annotated[Kbytes, important]  # kB, show


class SortGroupsInfo(FromJson):
    group_count: int
    sort_methods_used: list[str]  # see Sort.sort_method
    sort_space_memory: SortSpaceInfo  # show non-zero
    sort_space_disk: SortSpaceInfo  # show non-zero


class IncrementalSortWorker(Worker):
    full_sort_groups: Optional[SortGroupsInfo]  # show
    pre_sorted_groups: Optional[SortGroupsInfo]  # show


class IncrementalSort(Plan):
    sort_key: Annotated[list[Expr], important]  # show
    presorted_key: list[Expr]

    full_sort_groups: Optional[SortGroupsInfo]  # show
    pre_sorted_groups: Optional[SortGroupsInfo]  # show

    workers: Sequence[SortWorker]  # overrides


class Group(Plan, FilterScan):
    pass


class Aggregate(Plan, FilterScan):
    strategy: Annotated[str, important]  # show: title
    partial_mode: Annotated[str, important]   # Partial, Finalize, Simple


class WindowAgg(Plan):
    pass


class Unique(Plan):
    pass


class SetOp(Plan):
    strategy: str  # Sorted, Hashed, show: title: SetOp, HashSetOp
    command: str  # Intersect, Intersect All, Except, ExceptAll, show


class LockRows(Plan):
    pass


class Limit(Plan):
    pass


class Hash(Plan):
    hash_buckets: Annotated[int, important]  # show
    original_hash_buckets: int  # show if differs
    hash_batches: Annotated[int, important]  # show
    original_hash_batches: int  # show if differs
    peak_memory_usage: Annotated[Kbytes, important]  # kB  # show
