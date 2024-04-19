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

from __future__ import annotations
from typing import Optional, Iterator, FrozenSet

import dataclasses
import enum
import uuid

from edb.server.compiler.explain import to_json
from edb.server.compiler.explain import ir_analyze
from edb.server.compiler.explain import pg_tree
from edb.server.compiler.explain import fine_grained


COST_KEYS = frozenset((
    'plan_rows',
    'plan_width',
    'self_cost',
    'total_cost',
    'startup_cost',
))


class _Index:
    by_id: dict[int, _PlanInfo]
    by_alias: dict[str, _PlanInfo]

    def __init__(self, plan: fine_grained.Plan, idx: fine_grained.Index):
        by_id = {}
        ancestors: list[fine_grained.Plan] = []

        def index(node: fine_grained.Plan) -> None:
            pinfo = _PlanInfo(
                plan=node,
                ancestors=list(reversed(ancestors)),
            )
            by_id[id(node)] = pinfo
            ancestors.append(node)
            try:
                for sub in node.subplans:
                    index(sub)
            finally:
                ancestors.pop()

        index(plan)
        self.by_id = by_id
        self.by_alias = {a: by_id[id(p)] for a, p in idx.by_alias.items()}


@dataclasses.dataclass
class _PlanInfo:
    plan: fine_grained.Plan
    ancestors: list[fine_grained.Plan]
    shape_mark: Optional[str] = None

    @property
    def id(self) -> uuid.UUID:
        return self.plan.pipeline[-1].plan_id

    def self_and_ancestors(self, index: _Index) -> Iterator[_PlanInfo]:
        yield self
        for node in self.ancestors:
            yield index.by_id[id(node)]


@dataclasses.dataclass
class Node(to_json.ToJson, pg_tree.CostMixin):
    plan_id: uuid.UUID
    relations: FrozenSet[str]
    contexts: Optional[list[ir_analyze.ContextDesc]]
    children: list[Child]


# Note: clients should consider this open-ended list
class ChildKind(enum.Enum):
    POINTER = "pointer"  # TODO(tailhook) property/link ?
    FILTER = "filter"


@dataclasses.dataclass
class Child(to_json.ToJson):
    kind: ChildKind
    name: Optional[str]  # currently set only for POINTER
    node: Node


def _scan_relations(
    path: str, plan: fine_grained.Plan, index: _Index
) -> Iterator[pg_tree.Relation]:
    info = index.by_id[id(plan)]
    if info.shape_mark == path or info.shape_mark is None:
        for stage in plan.pipeline:
            if relation := getattr(stage, 'relation_name', None):
                yield relation
        for node in plan.subplans:
            yield from _scan_relations(path, node, index)


def _build_shape(
    path: str,
    plan: fine_grained.Plan,
    shape: ir_analyze.ShapeInfo,
    contexts: Optional[list[ir_analyze.ContextDesc]],
    index: _Index,
) -> Node:
    # Coarse-grained tree is built like this:
    #
    # 1. Scan IR we find all the shapes, and mark aliases that belong to
    #    them or their pointers (done in ir_analyze module)
    # 2. For each shape and property we try to find the node of fine-grained
    #    tree that represents them (by using alias and walking up).
    # 3. And we output tree containing only those nodes marked in step (2)

    _shape_mark(path, shape, index)

    pointers = {}
    for name, pointer in shape.pointers.items():
        subpath = f"{path}.{name}"

        if (
            pointer.main_alias is not None and
            (c_info := index.by_alias.get(pointer.main_alias)) is not None
        ):
            info = c_info
        else:
            for alias in pointer.aliases:
                if c_info := index.by_alias.get(alias):
                    info = c_info
                    break
            else:
                continue

        start = info
        last_context = info.plan.contexts
        for plan_info in info.self_and_ancestors(index):
            mark = plan_info.shape_mark
            if mark is not None and mark != subpath:
                break
            start = plan_info
            if start.plan.contexts:
                last_context = start.plan.contexts

        pointers[name] = _build_shape(
            f"{path}.{name}",
            start.plan,
            pointer,
            last_context,
            index,
        )

    relations = frozenset(_scan_relations(path, plan, index))

    # sometimes context can be in inner node, hoist it
    if (
        not contexts and
        (main_alias := shape.main_alias) and
        (main_info := index.by_alias.get(main_alias))
    ):
        alias = main_alias
        contexts = main_info.plan.contexts

    top = plan.pipeline[0]
    return Node(
        plan_id=plan.pipeline[0].plan_id,
        relations=relations,
        children=[Child(kind=ChildKind.POINTER, name=name, node=node)
                  for name, node in pointers.items()],
        contexts=contexts,
        # cost vars
        startup_cost=top.startup_cost,
        total_cost=top.total_cost,
        plan_rows=top.plan_rows,
        plan_width=top.plan_width,
        actual_startup_time=top.actual_startup_time,
        actual_total_time=top.actual_total_time,
        actual_rows=top.actual_rows,
        actual_loops=top.actual_loops,
        shared_hit_blocks=top.shared_hit_blocks,
        shared_read_blocks=top.shared_read_blocks,
        shared_dirtied_blocks=top.shared_dirtied_blocks,
        shared_written_blocks=top.shared_written_blocks,
        local_hit_blocks=top.local_hit_blocks,
        local_read_blocks=top.local_read_blocks,
        local_dirtied_blocks=top.local_dirtied_blocks,
        local_written_blocks=top.local_written_blocks,
        temp_read_blocks=top.temp_read_blocks,
        temp_written_blocks=top.temp_written_blocks,
    )


def _shape_mark(path: str, shape: ir_analyze.ShapeInfo, index: _Index) -> None:
    path_prefix = path + "."
    for alias in shape.all_aliases:
        info = index.by_alias.get(alias)
        if not info:
            continue
        for plan_info in info.self_and_ancestors(index):
            if plan_info.shape_mark:
                break
            plan_info.shape_mark = path

    for name, _subshape in shape.pointers.items():
        subpath = f"{path}.{name}"
        for alias in shape.all_aliases:
            info = index.by_alias.get(alias)
            if not info:
                continue
            for plan_info in info.self_and_ancestors(index):
                cur_mark = plan_info.shape_mark
                if cur_mark is None:
                    plan_info.shape_mark = subpath
                elif cur_mark == path:
                    break
                elif cur_mark == subpath:
                    break
                elif cur_mark.startswith(path_prefix):
                    # Two pointers met together, this means it's a
                    # branching point. We need to cleanup all pointers
                    # from the ancestors now (just continue loop and it'll
                    # do the job)
                    plan_info.shape_mark = path


def build(
    plan: fine_grained.Plan,
    info: ir_analyze.AnalysisInfo,
    index: fine_grained.Index
) -> Node:
    idx = _Index(plan, index)
    return _build_shape('🌳', plan, info.shape_tree, plan.contexts, idx)
