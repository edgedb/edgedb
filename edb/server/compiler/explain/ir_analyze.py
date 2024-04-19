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
from typing import Any, Optional, Iterator, cast

import dataclasses

from edb.common import ast
from edb.common import debug

from edb.edgeql import ast as qlast

from edb.ir import ast as irast

from edb.pgsql import ast as pgast
from edb.pgsql.compiler import astutils

from edb.server.compiler import explain
from edb.server.compiler.explain import to_json


@dataclasses.dataclass(eq=True, frozen=True)
class ContextDesc(to_json.ToJson):
    start: int
    end: int
    buffer_idx: int
    text: str

    def is_subcontext_of(self, other: ContextDesc) -> bool:
        return (
            self.buffer_idx == other.buffer_idx and
            self.start >= other.start and
            self.end <= other.end
        )


@dataclasses.dataclass
class AliasInfo(to_json.ToJson):
    contexts: list[ContextDesc]


@dataclasses.dataclass
class ShapeInfo(to_json.ToJson):
    aliases: set[str]
    pointers: dict[str, ShapeInfo]
    main_alias: Optional[str] = None

    @property
    def all_aliases(self) -> Iterator[str]:
        if self.main_alias:
            yield self.main_alias
        yield from self.aliases


@dataclasses.dataclass
class AnalysisInfo(to_json.ToJson):
    alias_info: dict[str, AliasInfo]
    buffers: list[tuple[str, str]]
    shape_tree: ShapeInfo


class VisitShapes(ast.NodeVisitor):
    ir_node_to_alias: dict[irast.Set, str] = {}
    skip_hidden = True
    extra_skips = frozenset(('shape', 'source', 'target'))

    def __init__(self, ir_node_to_alias: dict[irast.Set, str], **kwargs: Any):
        self.ir_node_to_alias = ir_node_to_alias
        self.current_shape = ShapeInfo(aliases=set(), pointers={})
        super().__init__(**kwargs)

    def visit_Set(self, node: irast.Set) -> Any:
        alias = self.ir_node_to_alias.get(node)
        if not alias:
            return self.generic_visit(node)

        if not node.shape:
            self.current_shape.aliases.add(alias)
            return self.generic_visit(node)

        parent_shape = self.current_shape
        parent_shape.main_alias = alias
        parent_shape.aliases.discard(alias)
        for (item, _oper) in node.shape:
            if not (rptr_name := item.path_id.rptr_name()):
                continue
            name = str(rptr_name.name)

            self.current_shape = self.current_shape.pointers.setdefault(
                name,
                ShapeInfo(aliases=set(), pointers={}),
            )
            try:
                self.generic_visit(item)
            finally:
                self.current_shape = parent_shape

        # Simple scalar expressions have the same alias for some reason
        # so we have to discard them
        for sub in parent_shape.pointers.values():
            sub.aliases.discard(parent_shape.main_alias)
            sub.aliases.difference_update(parent_shape.aliases)

        return self.generic_visit(node)  # this skips node.shape


# Do a bunch of analysis of the queries. Currently we produce more
# info than we actually consume, since we are still in a somewhat
# exploratory phase.
def analyze_queries(
    ql: qlast.Base,
    ir: irast.Statement,
    pg: pgast.Base,
    ctx: explain.AnalyzeContext,
) -> AnalysisInfo:
    debug_spew = debug.flags.edgeql_explain

    assert ql.span
    contexts = {(ql.span.buffer, ql.span.name): 0}

    def get_context(node: irast.Set) -> ContextDesc:
        assert node.span, node
        span = node.span
        key = span.buffer, span.name
        if (idx := contexts.get(key)) is None:
            idx = len(contexts)
            contexts[key] = idx
        text = span.buffer[span.start:span.end]

        return ContextDesc(
            start=span.start,
            end=span.end,
            buffer_idx=idx,
            text=text,
        )

    rvars = ast.find_children(pg, pgast.BaseRangeVar)
    queries = ast.find_children(pg, pgast.Query)

    # Map subqueries back to their rvars
    subq_to_rvar: dict[pgast.Query, pgast.RangeSubselect] = {}
    for rvar in rvars:
        if isinstance(rvar, pgast.RangeSubselect):
            assert rvar.subquery not in subq_to_rvar
            for subq in astutils.each_query_in_set(rvar.subquery):
                subq_to_rvar[subq] = rvar

    # Find all *references* to an rvar in path_rvar_maps
    # Maps rvars to the queries that join them
    reverse_path_rvar_map: dict[
        pgast.BaseRangeVar,
        list[pgast.Query],
    ] = {}
    for qry in queries:
        qrvars = []
        if isinstance(qry, (pgast.SelectStmt, pgast.UpdateStmt)):
            qrvars.extend(qry.from_clause)
        if isinstance(qry, pgast.DeleteStmt):
            qrvars.extend(qry.using_clause)

        for orvar in qrvars:
            for rvar in astutils.each_base_rvar(orvar):
                reverse_path_rvar_map.setdefault(rvar, []).append(qry)

    # Map aliases to rvars and then to path ids
    aliases = {
        rvar.alias.aliasname: rvar for rvar in rvars if rvar.alias.aliasname
    }

    alias_contexts: dict[str, list[ContextDesc]] = {}
    ir_node_to_alias: dict[irast.Set, str] = {}

    # Try to produce good contexts
    # KEY FACT: We often duplicate code for with bindings. This means
    # we want to expose that through the contexts we include.
    for alias, rvar in aliases.items():
        # Run up the tree looking both for contexts to associate with
        # and the next node in the tree to go up to
        asets = []
        while True:
            ns = cast(list[irast.Set], rvar.ir_origins or [])
            if len(ns) >= 1 and ns[0].span:
                if ns[0] not in asets:
                    asets.append(ns[0])

            for node in ns:
                ir_node_to_alias[node] = alias
                break

            # Find the enclosing
            sources = reverse_path_rvar_map.get(rvar, ())
            if debug_spew:
                print(f'SOURCES for {alias} 1/{len(ns)}', sources)
            if sources:
                source = sources[0]
                if source not in subq_to_rvar:
                    break
            else:
                break

            rvar = subq_to_rvar[source]

        spans = [get_context(x) for x in asets if x.span]
        if debug_spew:
            print(alias, asets)
            for x in asets:
                debug.dump(x.span)

        # Using the first set of contexts found
        alias_contexts.setdefault(alias, spans)

    alias_info = {
        alias: AliasInfo(
            contexts=alias_contexts.pop(alias, []),
        )
        for alias in aliases
    }

    visitor = VisitShapes(ir_node_to_alias=ir_node_to_alias)
    visitor.visit(ir)
    shape_tree = visitor.current_shape

    return AnalysisInfo(
        alias_info=alias_info,
        buffers=[text for text, _id in contexts.keys()],
        shape_tree=shape_tree,
    )
