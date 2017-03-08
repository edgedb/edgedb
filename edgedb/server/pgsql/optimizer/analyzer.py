##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import typing

from edgedb.lang.common import ast
from edgedb.lang.common import compiler
from edgedb.server.pgsql import ast as pgast


class Relation(ast.AST):

    name: str
    targets: set
    has_joins: bool = False

    node: pgast.CommonTableExpr
    parent: pgast.SelectStmt
    used_in: collections.OrderedDict

    def get_target(self, name: str):
        query: pgast.SelectStmt = self.node.query

        for node in query.target_list:
            if node.name == name:
                return node.val

        raise IndexError(
            f'could not find target {name!r} in cte {self.name}')


class ContextLevel(compiler.ContextLevel):
    current_select: pgast.SelectStmt
    current_cte: Relation

    def __init__(self, prevlevel=None, mode=None):
        if prevlevel is None:
            self.current_select = None
            self.current_cte = None
        else:
            self.current_select = prevlevel.current_select
            self.current_cte = prevlevel.current_cte


class Context(compiler.CompilerContext):
    ContextLevelClass = ContextLevel
    default_mode = None


class Analyzer(ast.NodeVisitor):

    def __init__(self):
        super().__init__()
        self.rels = collections.OrderedDict()
        self.context = Context()

    def _process_cte(self, parent: pgast.SelectStmt,
                     cte_node: pgast.CommonTableExpr):
        node = cte_node.query
        name = cte_node.name

        if not (
                all(isinstance(t, pgast.ResTarget) and
                    not t.indirection
                    for t in node.target_list) and

                len(node.from_clause) == 1 and

                not node.window_clause and
                not node.values and
                not node.locking_clause and

                not node.op and
                not node.all and
                not node.larg and
                not node.rarg):
            return

        return Relation(name=name, node=cte_node, parent=parent)

    def _process_select(self, node: pgast.SelectStmt):
        with self.context.new() as ctx:
            ctx.current_select = node
            self.generic_visit(node)

    def visit_ResTarget(self, node: pgast.ResTarget):
        ctx = self.context.current

        if node.name is None:
            if isinstance(node.val, pgast.ColumnRef):
                # TODO find a cleaner way
                node.name = node.val.name[1]

        if ctx.current_cte is not None:
            if node.name is None:
                raise RuntimeError('cannot optimize: empty target name')

            ctx.current_cte.targets.add(node.name)

        self.generic_visit(node)

    def visit_RangeVar(self, node: pgast.RangeVar):
        ctx = self.context.current

        if isinstance(node.relation, pgast.CommonTableExpr):
            rcte = node.relation
            if rcte.name in self.rels:
                rel = self.rels[rcte.name]
                if ctx.current_select in rel.used_in:
                    return

                rel.used_in[ctx.current_select] = node.alias.aliasname

        self.generic_visit(node)

    def visit_SelectStmt(self, node: pgast.SelectStmt):
        for cte in node.ctes:
            if isinstance(cte.query, pgast.SelectStmt):
                rel = self._process_cte(node, cte)
                with self.context.new() as ctx:
                    if rel is not None:
                        ctx.current_cte = rel

                    self.visit(cte.query)

                    if rel is not None:
                        # Register CTE after we visit the query to maintain
                        # correct dependency order.
                        self.rels[rel.name] = rel

        self._process_select(node)

    @classmethod
    def analyze(cls, tree: pgast.Base) -> typing.Mapping[str, Relation]:
        analyzer = cls()
        analyzer.visit(tree)
        return analyzer.rels
