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
from typing import Any, Optional, Tuple, Iterable

import uuid
import dataclasses

from edb.server.compiler.explain import to_json
from edb.server.compiler.explain import pg_tree
from edb.server.compiler.explain import ir_analyze
from edb.server.compiler import explain


PropValue = str | int | float | list[str | int | float]


@dataclasses.dataclass
class Prop(to_json.ToJson):
    title: str
    value: PropValue
    type: Optional[pg_tree.PropType]
    important: bool

    @property
    def attribute_name(self) -> str:
        return self.title


class Properties(to_json.ToJson):

    def __init__(self, props: Iterable[Prop]):
        self._props = {p.attribute_name: p for p in props}

    def to_json(self) -> Any:
        return list(self._props.values())

    def __repr__(self) -> str:
        return repr({k: v.value for k, v in self._props.items()})


@dataclasses.dataclass(kw_only=True)
class Stage(to_json.ToJson, pg_tree.CostMixin):
    plan_type: str
    plan_id: uuid.UUID
    properties: Properties

    def __getattr__(self, name: str) -> PropValue:
        try:
            return self.properties._props[name].value
        except KeyError:
            raise AttributeError(name) from None


@dataclasses.dataclass
class Plan(to_json.ToJson):
    contexts: Optional[list[ir_analyze.ContextDesc]]
    pipeline: list[Stage]
    subplans: list[Plan]
    alias: Optional[str] = None


@dataclasses.dataclass
class Index:
    by_id: dict[uuid.UUID, Plan]
    by_alias: dict[str, Plan]


def context_diff(
    left: Optional[list[ir_analyze.ContextDesc]],
    right: Optional[list[ir_analyze.ContextDesc]],
) -> list[ir_analyze.ContextDesc]:
    if not left:
        return []
    if not right:
        return left
    result = [ctx for ctx in left if ctx not in right]
    return result


def context_intersect(
    left: Optional[list[ir_analyze.ContextDesc]],
    right: Optional[list[ir_analyze.ContextDesc]],
) -> list[ir_analyze.ContextDesc]:
    if not left:
        return []
    if not right:
        return []
    return [ctx for ctx in left if ctx in right]


def context_optimize(
    items: Optional[list[ir_analyze.ContextDesc]],
) -> Optional[list[ir_analyze.ContextDesc]]:
    if not items:
        return None
    # We assume that context are ordered:
    # 1. In single location (alias): from the most specific to the broadest
    # 2. Location that belong to single buffer or alias are subsequent
    #
    # Postgres marks by alias the most specific thing (i.e. table scan mostly)
    # But since we try to hoist context to nearest node having no context, that
    # usually matches broadest context. Although, this is just a heuristic.
    #
    # So we only keep the last context from each group (alias/buffer) by
    # squashing contexts that are inside of each other
    result: list[ir_analyze.ContextDesc] = []
    for ctx in reversed(items):
        for maybe_parent in result:
            if ctx.is_subcontext_of(maybe_parent):
                break
        else:
            result.append(ctx)
    result.reverse()
    return result


class TreeBuilder:
    alias_info: dict[str, ir_analyze.AliasInfo]
    by_id: dict[uuid.UUID, Plan]
    by_alias: dict[str, Plan]

    def __init__(self, info: ir_analyze.AnalysisInfo):
        self.alias_info = info.alias_info
        self.by_alias = {}
        self.by_id = {}

    def build(self, plan: pg_tree.Plan, args: explain.Arguments) -> Plan:
        # For fine-grained tree (this one will be displayed in \verbose mode or
        # whatever we name it) we do three things:

        # 1. Remove cheap scalar Result nodes. In my examples, they are:
        #    variable in LIMIT clause, or scalar expressions, like string
        #    concatenation. We ensure that eliminated nodes are less than 1
        #    percent of parent node cost/time,

        # 2. Squash nested nodes having one child into pipeline list. This
        #    should allow less nested presentation of the tree.
        #
        # 3. For contexts:
        #    a) Hoist them through the tree of one-child node
        #    b) If contexts of all children are equal we move context to higher
        #       level
        #    c) If contexts of children are partly equal, we move equal
        #       contexts to parent removing them from children
        #    d) Eliminate overlapping contexts after that

        # 3c, works for things like x := count(.a) + count(.b). There are two
        # nodes, one starting from .a and one from .b and both of them have
        # contexts up to the whole expression starting from x :=.

        pipeline = []
        aliases = set()

        pipeline.append(self._make_stage(plan))
        alias = getattr(plan, 'alias', None)
        if alias:
            aliases.add(alias)

        plans = _filter_plans(plan, args)
        while len(plans) == 1 and not alias:
            node = plans[0]
            pipeline.append(self._make_stage(node))
            plans = _filter_plans(node, args)
            alias = getattr(node, 'alias', None)
            if alias:
                aliases.add(alias)

        subplans = [self.build(subplan, args)
                    for subplan in plans]

        alias_info = self.alias_info.get(alias)
        contexts = alias_info.contexts if alias_info else None
        if not contexts and subplans and (contexts := subplans[0].contexts):
            # hoist contexts that are common in child branches
            for ch_plan in subplans[1:]:
                if inner_contexts := ch_plan.contexts:
                    contexts = context_intersect(contexts, inner_contexts)

            if contexts:  # some contexts are hoisted
                for (sub, node) in zip(subplans, plans):
                    sub.contexts = context_diff(sub.contexts, contexts)
                    if (
                        not sub.contexts and
                        (subalias := getattr(node, 'alias', None))
                    ):
                        aliases.add(subalias)

        # optimize after hoisting
        for sub in subplans:
            sub.contexts = context_optimize(sub.contexts)

        result = Plan(
            contexts=contexts,
            pipeline=pipeline,
            subplans=subplans,
        )

        for stage in pipeline:
            self.by_id[stage.plan_id] = result
        # Note: this overwrites children with this alias by this node
        # when contexts are hoisted, which is a good thing
        for alias in aliases:
            self.by_alias[alias] = result

        return result

    def _get_contexts(
        self,
        plan: pg_tree.Plan,
    ) -> Optional[list[ir_analyze.ContextDesc]]:
        if not (alias := getattr(plan, 'alias', None)):
            return None
        if not (ainfo := self.alias_info.get(alias)):
            return None
        return ainfo.contexts

    def _make_stage(self, plan: pg_tree.Plan) -> Stage:
        properties = []
        for name, prop in plan.get_props().items():
            if (value := getattr(plan, name, None)) is not None:
                properties.append(Prop(
                    title=name,
                    value=value,
                    type=prop.enum_type,
                    important=prop.important,
                ))

        return Stage(
            plan_type=type(plan).__name__,
            plan_id=plan.plan_id,
            properties=Properties(properties),
            # cost vars
            startup_cost=plan.startup_cost,
            total_cost=plan.total_cost,
            plan_rows=plan.plan_rows,
            plan_width=plan.plan_width,
            actual_startup_time=plan.actual_startup_time,
            actual_total_time=plan.actual_total_time,
            actual_rows=plan.actual_rows,
            actual_loops=plan.actual_loops,
            shared_hit_blocks=plan.shared_hit_blocks,
            shared_read_blocks=plan.shared_read_blocks,
            shared_dirtied_blocks=plan.shared_dirtied_blocks,
            shared_written_blocks=plan.shared_written_blocks,
            local_hit_blocks=plan.local_hit_blocks,
            local_read_blocks=plan.local_read_blocks,
            local_dirtied_blocks=plan.local_dirtied_blocks,
            local_written_blocks=plan.local_written_blocks,
            temp_read_blocks=plan.temp_read_blocks,
            temp_written_blocks=plan.temp_written_blocks,
        )


def _filter_plans(
    node: pg_tree.Plan, args: explain.Arguments
) -> list[pg_tree.Plan]:
    min_cost = node.total_cost * 0.01
    # TODO(tailhook) maybe we should scan inner plans to figure out that
    # there are no inner contexts in the children
    plans = [
        p
        for p in node.plans
        if not isinstance(p, pg_tree.Result) or
        p.total_cost > min_cost or p.plan_rows > 1
    ]
    return plans


def build(
    plan: pg_tree.Plan,
    info: ir_analyze.AnalysisInfo,
    args: explain.Arguments,
) -> Tuple[Plan, Index]:
    tree = TreeBuilder(info)
    result = tree.build(plan, args)
    result.contexts = context_optimize(result.contexts)
    index = Index(by_id=tree.by_id, by_alias=tree.by_alias)
    return result, index
