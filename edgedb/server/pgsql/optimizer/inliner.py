##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import copy
import typing

from edgedb.lang.common import ast
from edgedb.lang.common import compiler
from edgedb.server.pgsql import ast as pgast

from . import analyzer


class NameUpdater(ast.NodeVisitor):

    def __init__(self, prefix: str):
        self.prefix = prefix
        super().__init__()

    def visit_Alias(self, node: pgast.Alias):
        node.aliasname = self.prefix + node.aliasname

    def visit_ColumnRef(self, node: pgast.ColumnRef):
        node.name[0] = self.prefix + node.name[0]

    @classmethod
    def update(cls, prefix: str, node: pgast.Alias):
        nu = cls(prefix)
        nu.visit(node)


class Inliner(ast.NodeTransformer):

    def __init__(self, rel: analyzer.Relation,
                 inline_count: int,
                 current_alias: str):
        super().__init__()
        self.rel = rel
        self.inline_count = inline_count
        self.current_alias = current_alias

    @staticmethod
    def copy_subtree(idx:int, root: pgast.Base):
        new_root = copy.deepcopy(root)
        NameUpdater.update(f'i{idx}~', new_root)
        return new_root

    def visit_RangeVar(self, node):
        if node.alias.aliasname == self.current_alias and \
                node.relation.name == self.rel.name:
            return self.copy_subtree(
                self.inline_count, self.rel.node.query.from_clause[0])
        else:
            return self.generic_visit(node)

    def visit_ColumnRef(self, node):
        if node.name[0] != self.current_alias:
            return self.generic_visit(node)
        if node.name[1] not in self.rel.targets:
            return self.generic_visit(node)

        return self.copy_subtree(
            self.inline_count, self.rel.get_target(node.name[1]))

    @classmethod
    def inline(cls, tree: pgast.Base,
               relations: typing.Mapping[str, analyzer.Relation]):

        inline_count = 0
        for rel in relations.values():
            remove_rel = True

            for query, alias in rel.used_in.items():
                if (rel.node.query.group_clause and query.group_clause or
                        rel.node.query.limit_offset and query.limit_offset or
                        rel.node.query.limit_count and query.limit_count or
                        rel.node.query.sort_clause and query.sort_clause):

                    # if both the query and the cte we want to
                    # inline have GROUP BY/LIMIT/etc:
                    # skip inlining
                    remove_rel = False
                    continue

                inline_count += 1

                inliner = cls(rel, inline_count, alias)
                inliner.visit(tree)

                if rel.node.query.group_clause:
                    query.group_clause = cls.copy_subtree(
                        inline_count, rel.node.query.group_clause)

                if rel.node.query.limit_offset:
                    query.limit_offset = cls.copy_subtree(
                        inline_count, rel.node.query.limit_offset)

                if rel.node.query.limit_count:
                    query.limit_count = cls.copy_subtree(
                        inline_count, rel.node.query.limit_count)

                if rel.node.query.sort_clause:
                    query.sort_clause = cls.copy_subtree(
                        inline_count, rel.node.query.sort_clause)

                if rel.node.query.where_clause:
                    new_where = cls.copy_subtree(
                        inline_count, rel.node.query.where_clause)

                    if query.where_clause:
                        query.where_clause = pgast.Expr(
                            kind=pgast.ExprKind.OP,
                            name='AND',
                            lexpr=new_where,
                            rexpr=query.where_clause)
                    else:
                        query.where_clause = new_where

            if remove_rel:
                rel.parent.ctes.remove(rel.node)

        return tree
