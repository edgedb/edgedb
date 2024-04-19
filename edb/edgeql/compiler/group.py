#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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

from typing import Any, Optional, Sequence, Dict, Set, FrozenSet

from edb.common import ast as ast_visitor

from edb.edgeql import qltypes
from edb.ir import ast as irast

from . import context
from . import inference
from . import setgen


class FindAggregatingUses(ast_visitor.NodeVisitor):
    """
    Find aggregated uses of a target node that can be hoisted.
    """
    skip_hidden = True
    extra_skips = frozenset(['materialized_sets'])

    def __init__(
        self,
        target: irast.PathId,
        *,
        ctx: context.ContextLevel,
    ) -> None:
        super().__init__()
        self.target = target
        self.aggregate: Optional[irast.Set] = None
        self.sightings: Set[Optional[irast.Set]] = set()
        self.ctx = ctx
        # Track pathids that we've seen. pathids that we are interested
        # in but haven't seen get marked as False.
        self.seen: Dict[irast.PathId, bool] = {}
        self.skippable: Dict[
            Optional[irast.Set], FrozenSet[irast.PathId]] = {}
        self.scope_tree = ctx.path_scope
        # We don't bother trying to reuse the existing inference
        # context because we make singleton assumptions that it
        # wouldn't and because ignore_computed_cards could invalidate
        # it.
        self.infctx = inference.make_ctx(ctx.env)._replace(
            singletons=frozenset({target}),
            ignore_computed_cards=True,
            # Don't update the IR with the results!
            make_updates=False,
        )

    def visit_Stmt(self, stmt: irast.Stmt) -> Any:
        # Sometimes there is sharing, so we want the official scope
        # for a node to be based on its appearance in the result,
        # not in a subquery.
        # I think it might not actually matter, though.

        old = self.aggregate

        # Can't handle ORDER/LIMIT/OFFSET which operate on the whole set
        # TODO: but often we probably could with arguments to the
        # aggregates, as long as the argument to the aggregate is just
        # a reference
        if isinstance(stmt, irast.SelectStmt) and (
            stmt.orderby or stmt.limit or stmt.offset or stmt.materialized_sets
        ):
            self.aggregate = None

        self.visit(stmt.bindings)
        if stmt.iterator_stmt:
            self.visit(stmt.iterator_stmt)
        if isinstance(stmt, (irast.MutatingStmt, irast.GroupStmt)):
            self.visit(stmt.subject)
        if isinstance(stmt, irast.GroupStmt):
            for v in stmt.using.values():
                self.visit(v)
        self.visit(stmt.result)

        res = self.generic_visit(stmt)

        self.aggregate = old

        return res

    def repeated_node_visit(self, node: irast.Base) -> None:
        if isinstance(node, irast.Set):
            self.seen[node.path_id] = True

    def visit_Set(self, node: irast.Set, skip_rptr: bool = False) -> None:
        self.seen[node.path_id] = True

        if node.path_id == self.target:
            self.sightings.add(self.aggregate)
            return

        old_scope = self.scope_tree
        if node.path_scope_id is not None:
            self.scope_tree = self.ctx.env.scope_tree_nodes[node.path_scope_id]

        # We also can't handle references inside of a semi-join,
        # because the bodies are executed one at a time, and so the
        # semi-join deduplication doesn't work.
        is_semijoin = (
            isinstance(node.expr, irast.Pointer)
            and node.path_id.is_objtype_path()
            and not self.scope_tree.is_visible(node.expr.source.path_id)
        )

        old = self.aggregate
        if is_semijoin:
            self.aggregate = None

        self.visit(node.shape)

        if isinstance(node.expr, irast.Pointer):
            sub_expr = node.expr.expr
            if not sub_expr:
                self.visit(node.expr.source)
            else:
                if node.expr.source.path_id not in self.seen:
                    self.seen[node.expr.source.path_id] = False
        else:
            sub_expr = node.expr

        if isinstance(sub_expr, irast.Call):
            self.process_call(sub_expr, node)
        else:
            self.visit(sub_expr)

        self.aggregate = old
        self.scope_tree = old_scope

    def process_call(self, node: irast.Call, ir_set: irast.Set) -> None:
        # It needs to be backed by an actual SQL function and must
        # not return SET OF
        returns_set = node.typemod == qltypes.TypeModifier.SetOfType
        calls_sql_func = (
            isinstance(node, irast.FunctionCall)
            and node.func_sql_function
        )
        for arg in node.args.values():
            typemod = arg.param_typemod
            old = self.aggregate
            # If this *returns* a set, it is going to mess things up since
            # the operation can't actually run on multiple things...

            old_seen = None

            # TODO: we would like to do better in some cases with
            # DISTINCT and the like where there are built in features
            # to do it in a GROUP
            if returns_set:
                self.aggregate = None
            elif (
                calls_sql_func
                and typemod == qltypes.TypeModifier.SetOfType
                # Don't hoist aggregates whose outputs contain objects
                # (I think this can only be array_agg).
                #
                # We have to eta-expand to put a shape on them anyway,
                # so there's no real point, and we mishandled that
                # case in a few places.  Eventually we'll want to properly
                # be able to serialize in the first place, though.
                and not setgen.get_set_type(
                    ir_set, ctx=self.ctx).contains_object(self.ctx.env.schema)
            ):
                old_seen = self.seen
                self.seen = {}
                self.aggregate = ir_set
            self.visit(arg)
            self.aggregate = old

            force_fail = False
            if old_seen is not None:
                self.skippable[ir_set] = frozenset({
                    k for k, v in self.seen.items() if not v
                    and self.scope_tree.is_visible(k)
                })
                for k, was_seen in self.seen.items():
                    # If we referred to some visible set and also
                    # spotted the target, we can't actually compile
                    # the target separately, so ditch it.
                    if (
                        was_seen
                        and self.scope_tree.is_visible(k)
                        and ir_set in self.sightings
                    ):
                        force_fail = True
                        self.sightings.discard(ir_set)
                        self.sightings.add(None)
                    old_seen[k] = self.seen.get(k, False) | was_seen

                # If, assuming the target is single, the aggregate is
                # still multi, then we can't extract it, since that
                # would lead to actually return multiple elements in a
                # SQL subquery.
                if (
                    ir_set in self.sightings
                    and inference.infer_cardinality(
                        arg.expr, scope_tree=self.scope_tree,
                        ctx=self.infctx).is_multi()
                ):
                    force_fail = True

                self.seen = old_seen

            if force_fail:
                self.sightings.discard(ir_set)
                self.sightings.add(None)


def infer_group_aggregates(
    irs: Sequence[irast.Base],
    *,
    ctx: context.ContextLevel,
) -> None:
    groups = ast_visitor.find_children(irs, irast.GroupStmt)
    for stmt in groups:
        visitor = FindAggregatingUses(
            stmt.group_binding.path_id,
            ctx=ctx,
        )
        visitor.visit(stmt.result)
        stmt.group_aggregate_sets = {
            k: visitor.skippable.get(k, frozenset())
            for k in visitor.sightings
        }
