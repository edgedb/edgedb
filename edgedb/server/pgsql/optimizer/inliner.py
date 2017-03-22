##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import copy
import typing

from edgedb.lang.common import ast
from edgedb.server.pgsql import ast as pgast

from . import analyzer


class NameUpdater(ast.NodeVisitor):

    def __init__(self, range_aliases, prefix: str):
        self.prefix = prefix
        self.range_aliases = range_aliases
        super().__init__()

    def visit_Alias(self, node: pgast.Alias):
        if node.aliasname in self.range_aliases:
            node.aliasname = self.prefix + node.aliasname

    def visit_ColumnRef(self, node: pgast.ColumnRef):
        if node.name[0] in self.range_aliases:
            node.name[0] = self.prefix + node.name[0]

    @classmethod
    def update(cls, range_aliases, prefix: str, node: pgast.Alias):
        nu = cls(range_aliases, prefix)
        nu.visit(node)


def copy_subtree(idx: int,
                 root: pgast.Base,
                 range_aliases: typing.Set[str],
                 deepcopy: bool):

    if deepcopy:
        new_root = copy.deepcopy(root)
    else:
        new_root = root

    NameUpdater.update(range_aliases, f'i{idx}~', new_root)
    return new_root


def merge(qi: analyzer.QueryInfo, rel: analyzer.Relation,
          alias: str, query: pgast.Query, inline_count: int):

    if (rel.node.query.group_clause and query.group_clause or
            rel.node.query.limit_offset and query.limit_offset or
            rel.node.query.limit_count and query.limit_count or
            rel.node.query.sort_clause and query.sort_clause):
        # if both the query and the cte we want to
        # inline have GROUP BY/LIMIT/etc:
        # skip inlining
        return False

    # If the CTE is used in more than one place, always deepcopy it.
    deepcopy = len(rel.used_in) > 1

    range_aliases = rel.get_range_aliases()

    if alias in qi.col_refs:
        # (if the column is referenced in more than one place - deepcopy)
        col_deepcopy = deepcopy or len(qi.col_refs[alias]) > 1

        for col_ref in qi.col_refs[alias]:
            col: pgast.ColumnRef = col_ref.node
            target = rel.get_target(col.name[1])

            col_ref.node = copy_subtree(
                inline_count,
                target,
                range_aliases,
                col_deepcopy)

    if alias in qi.range_refs:
        # (if the range is referenced in more than one place - deepcopy)
        range_deepcopy = deepcopy or len(qi.range_refs[alias]) > 1

        assert len(rel.node.query.from_clause) == 1
        for range_ref in qi.range_refs[alias]:
            range_ref.node = copy_subtree(
                inline_count,
                rel.node.query.from_clause[0],
                range_aliases,
                range_deepcopy)

    if rel.node.query.group_clause:
        query.group_clause = copy_subtree(
            inline_count,
            rel.node.query.group_clause,
            range_aliases,
            deepcopy)

    if rel.node.query.limit_offset:
        query.limit_offset = copy_subtree(
            rel,
            inline_count,
            rel.node.query.limit_offset,
            range_aliases,
            deepcopy)

    if rel.node.query.limit_count:
        query.limit_count = copy_subtree(
            inline_count,
            rel.node.query.limit_count,
            range_aliases,
            deepcopy)

    if rel.node.query.sort_clause:
        query.sort_clause = copy_subtree(
            inline_count,
            rel.node.query.sort_clause,
            range_aliases,
            deepcopy)

    if rel.node.query.where_clause:
        new_where = copy_subtree(
            inline_count,
            rel.node.query.where_clause,
            range_aliases,
            deepcopy)

        if query.where_clause:
            query.where_clause = pgast.Expr(
                kind=pgast.ExprKind.OP,
                name='AND',
                lexpr=new_where,
                rexpr=query.where_clause)
        else:
            query.where_clause = new_where

    if rel.node.query.ctes:
        query.ctes.extend(rel.node.query.ctes)

    return True


def inline_subquery(qi: analyzer.QueryInfo, rel: analyzer.Relation,
                    alias: str, query: pgast.Query):

    inlined = False

    if alias in qi.range_refs:
        for range_ref in qi.range_refs[alias]:
            new_query = copy.deepcopy(rel.node.query)

            range_ref.node = pgast.RangeSubselect(
                alias=range_ref.node.alias,
                subquery=new_query)

            inlined = True

    return inlined


def optimize(qi: analyzer.QueryInfo):
    inline_count = 0
    for rel in qi.rels:
        if rel.strategy is analyzer.InlineStrategy.Skip:
            continue

        remove_rel = True
        for query, alias in rel.used_in.items():

            if rel.strategy is analyzer.InlineStrategy.Merge:
                inline_count += 1
                inlined = merge(qi, rel, alias, query, inline_count)

                if not inlined:
                    inline_count -= 1
                    remove_rel = False

            elif rel.strategy is analyzer.InlineStrategy.Subquery:
                inlined = inline_subquery(qi, rel, alias, query)
                if not inlined:
                    remove_rel = False

            else:
                remove_rel = False

        if remove_rel:
            rel.parent.ctes.remove(rel.node)
