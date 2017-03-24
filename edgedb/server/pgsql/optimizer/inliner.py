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

from . import meta


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


def merge_query(qi: meta.QueryInfo, rel: meta.Relation,
                alias: str, query: pgast.Query):

    if (rel.query.group_clause and query.group_clause or
            rel.query.limit_offset and query.limit_offset or
            rel.query.limit_count and query.limit_count or
            rel.query.sort_clause and query.sort_clause):
        # if both the query and the cte we want to
        # inline have GROUP BY/LIMIT/etc:
        # skip inlining
        return False

    inline_index = qi.get_new_inline_index()

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
                inline_index,
                target,
                range_aliases,
                col_deepcopy)

    if alias in qi.range_refs:
        # (if the range is referenced in more than one place - deepcopy)
        range_deepcopy = deepcopy or len(qi.range_refs[alias]) > 1

        assert len(rel.query.from_clause) == 1
        for range_ref in qi.range_refs[alias]:
            range_ref.node = copy_subtree(
                inline_index,
                rel.query.from_clause[0],
                range_aliases,
                range_deepcopy)

    if rel.query.group_clause:
        query.group_clause = copy_subtree(
            inline_index,
            rel.query.group_clause,
            range_aliases,
            deepcopy)

    if rel.query.limit_offset:
        query.limit_offset = copy_subtree(
            rel,
            inline_index,
            rel.query.limit_offset,
            range_aliases,
            deepcopy)

    if rel.query.limit_count:
        query.limit_count = copy_subtree(
            inline_index,
            rel.query.limit_count,
            range_aliases,
            deepcopy)

    if rel.query.sort_clause:
        query.sort_clause = copy_subtree(
            inline_index,
            rel.query.sort_clause,
            range_aliases,
            deepcopy)

    if rel.query.where_clause:
        new_where = copy_subtree(
            inline_index,
            rel.query.where_clause,
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

    if rel.query.ctes:
        query.ctes.extend(rel.query.ctes)

    return True


def merge_cte_relation(qi: meta.QueryInfo, rel: meta.Relation):
    assert rel.is_cte

    for used_in_rel, alias in list(rel.used_in.items()):
        inlined = merge_query(qi, rel, alias, used_in_rel.query)
        if inlined:
            del rel.used_in[used_in_rel]

    if not rel.used_in:
        qi.remove_relation(rel)


def inline_cte_relation(qi: meta.QueryInfo, rel: meta.Relation):
    assert rel.is_cte
    rel_query = rel.query

    for used_in_rel, alias in list(rel.used_in.items()):
        assert alias in qi.range_refs

        for range_ref in qi.range_refs[alias]:
            new_query = copy.deepcopy(rel_query)

            range_ref.node = pgast.RangeSubselect(
                alias=range_ref.node.alias,
                subquery=new_query)

        del rel.used_in[used_in_rel]

    if not rel.used_in:
        qi.remove_relation(rel)


def optimize(qi: meta.QueryInfo):
    for rel in list(qi.rels):
        if not rel.is_cte or not rel.is_inlineable:
            continue

        query = rel.query

        if len(query.from_clause) == 0 and len(rel.used_in) == 1:
            inline_cte_relation(qi, rel)

        elif len(query.from_clause) == 1:
            if not query.group_clause:
                if (isinstance(query.having, pgast.Constant) and
                        query.having.val is True):
                    # `HAVING True` is a special hint to inline
                    # this CTE as a subquery.
                    query.having = None
                    inline_cte_relation(qi, rel)
                else:
                    merge_cte_relation(qi, rel)

            elif len(rel.used_in) == 1:
                inline_cte_relation(qi, rel)
