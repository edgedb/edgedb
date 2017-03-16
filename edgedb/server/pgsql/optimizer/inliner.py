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

    def __init__(self, rel, range_aliases, prefix: str):
        self.rel = rel
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
    def update(cls, rel, range_aliases, prefix: str, node: pgast.Alias):
        nu = cls(rel, range_aliases, prefix)
        nu.visit(node)


class MergeStrategy(ast.NodeTransformer):

    def __init__(self, rel: analyzer.Relation,
                 inline_count: int,
                 current_alias: str,
                 range_aliases: typing.Set[str]):
        super().__init__()
        self.rel = rel
        self.inline_count = inline_count
        self.current_alias = current_alias
        self.range_aliases = range_aliases

    @staticmethod
    def copy_subtree(rel: analyzer.Relation,
                     idx: int,
                     root: pgast.Base,
                     range_aliases: typing.Set[str]):

        new_root = copy.deepcopy(root)
        NameUpdater.update(rel, range_aliases, f'i{idx}~', new_root)
        return new_root

    def visit_RangeVar(self, node):
        if isinstance(node.relation, pgast.CommonTableExpr):
            relation_name = node.relation.name
        elif isinstance(node.relation, pgast.Relation):
            relation_name = node.relation.relname

        if node.alias is not None:
            relation_alias = node.alias.aliasname
        else:
            relation_alias = relation_name

        if (relation_alias == self.current_alias and
                node.relation.name == self.rel.name):
            return self.copy_subtree(
                self.rel,
                self.inline_count,
                self.rel.node.query.from_clause[0],
                self.range_aliases)
        else:
            return self.generic_visit(node)

    def visit_ColumnRef(self, node):
        if node.name[0] != self.current_alias:
            return self.generic_visit(node)

        try:
            target = self.rel.get_target(node.name[1])
        except IndexError:
            target = None

        if target is None:
            return self.generic_visit(node)

        return self.copy_subtree(
            self.rel,
            self.inline_count,
            target,
            self.range_aliases)

    @classmethod
    def apply(cls, rel, alias, tree, query, inline_count):
        if (rel.node.query.group_clause and query.group_clause or
                rel.node.query.limit_offset and query.limit_offset or
                rel.node.query.limit_count and query.limit_count or
                rel.node.query.sort_clause and query.sort_clause):
            # if both the query and the cte we want to
            # inline have GROUP BY/LIMIT/etc:
            # skip inlining
            return False

        range_aliases = rel.get_range_aliases()

        inliner = cls(rel, inline_count, alias, range_aliases)
        inliner.visit(tree)

        if rel.node.query.group_clause:
            query.group_clause = cls.copy_subtree(
                rel,
                inline_count,
                rel.node.query.group_clause,
                range_aliases)

        if rel.node.query.limit_offset:
            query.limit_offset = cls.copy_subtree(
                rel,
                inline_count,
                rel.node.query.limit_offset,
                range_aliases)

        if rel.node.query.limit_count:
            query.limit_count = cls.copy_subtree(
                rel,
                inline_count,
                rel.node.query.limit_count,
                range_aliases)

        if rel.node.query.sort_clause:
            query.sort_clause = cls.copy_subtree(
                rel,
                inline_count,
                rel.node.query.sort_clause,
                range_aliases)

        if rel.node.query.where_clause:
            new_where = cls.copy_subtree(
                rel,
                inline_count,
                rel.node.query.where_clause,
                range_aliases)

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


class SubqueryStrategy(ast.NodeTransformer):

    def __init__(self, rel: analyzer.Relation,
                 current_alias: str):
        super().__init__()
        self.rel = rel
        self.current_alias = current_alias
        self.inlined = False

    def visit_RangeVar(self, node):
        if node.alias.aliasname == self.current_alias and \
                node.relation.name == self.rel.name:

            self.inlined = True
            new_query = copy.deepcopy(self.rel.node.query)

            return pgast.RangeSubselect(
                alias=node.alias,
                subquery=new_query)

        else:
            return self.generic_visit(node)

    @classmethod
    def apply(cls, rel, current_alias, query):
        st = cls(rel, current_alias)
        st.visit(query)
        return st.inlined


def optimize(tree: pgast.Base,
             relations: typing.Mapping[str, analyzer.Relation]):

    inline_count = 0
    for rel in relations.values():
        if rel.strategy is analyzer.InlineStrategy.Skip:
            continue

        remove_rel = True
        for query, alias in rel.used_in.items():

            if rel.strategy is analyzer.InlineStrategy.Merge:
                inline_count += 1
                inlined = MergeStrategy.apply(
                    rel, alias, tree, query, inline_count)

                if not inlined:
                    inline_count -= 1
                    remove_rel = False

            elif rel.strategy is analyzer.InlineStrategy.Subquery:
                inlined = SubqueryStrategy.apply(rel, alias, query)
                if not inlined:
                    remove_rel = False

            else:
                remove_rel = False

        if remove_rel:
            rel.parent.ctes.remove(rel.node)

    return tree
