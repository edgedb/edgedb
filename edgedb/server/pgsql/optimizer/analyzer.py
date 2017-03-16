##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import enum
import typing

from edgedb.lang.common import ast
from edgedb.lang.common import compiler
from edgedb.server.pgsql import ast as pgast


class InlineStrategy(enum.Enum):
    Skip = 0
    Merge = 1
    Subquery = 2


class Relation(ast.AST):

    name: str
    has_joins: bool = False

    node: pgast.CommonTableExpr
    parent: pgast.SelectStmt
    used_in: collections.OrderedDict

    strategy: InlineStrategy = InlineStrategy.Skip

    def get_target(self, name: str):
        # NOTE: We can't cache the search results because
        # self.node.query can be mutated at any time.

        query: pgast.SelectStmt = self.node.query

        for node in query.target_list:
            if node.name is not None:
                if node.name == name:
                    return node.val
                else:
                    continue

            if isinstance(node.val, pgast.ColumnRef):
                if node.val.name[-1] == name:
                    return node.val
                elif (len(node.val.name) == 2 and
                        isinstance(node.val.name[1], pgast.Star)):
                    return pgast.ColumnRef(name=[node.val.name[0], name])
                else:
                    continue

        raise IndexError(
            f'could not find target {name!r} in cte {self.name}')

    def get_range_aliases(self):
        return RangeAnalyzer.analyze(self)


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

                not node.window_clause and
                not node.values and
                not node.locking_clause and

                not node.op and
                not node.all and
                not node.larg and
                not node.rarg):
            return

        return Relation(name=name, node=cte_node, parent=parent)

    def visit_RangeVar(self, node: pgast.RangeVar):
        ctx = self.context.current

        if ctx.current_select is None:
            # UPDATE or other statement
            self.generic_visit(node)
            return

        if isinstance(node.relation, pgast.CommonTableExpr):
            rcte = node.relation
            if rcte.name in self.rels:
                rel = self.rels[rcte.name]
                if ctx.current_select in rel.used_in:
                    return

                if node.alias is not None:
                    alias = node.alias.aliasname
                else:
                    alias = node.relation.name

                rel.used_in[ctx.current_select] = alias

        self.generic_visit(node)

    def visit_Query(self, node: pgast.SelectStmt):
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

        with self.context.new() as ctx:
            ctx.current_select = node
            self.generic_visit(node)

    @classmethod
    def analyze(cls, tree: pgast.Base) -> typing.Mapping[str, Relation]:
        analyzer = cls()
        analyzer.visit(tree)
        rels = analyzer.rels

        for rel in rels.values():
            query = rel.node.query

            if len(query.from_clause) == 0 and len(rel.used_in) == 1:
                rel.strategy = InlineStrategy.Subquery

            elif len(query.from_clause) == 1:
                if not query.group_clause:
                    rel.strategy = InlineStrategy.Merge

                elif (len(query.group_clause) == 1 and
                        isinstance(query.group_clause[0], pgast.Constant) and
                        query.group_clause[0].val is True):
                    # `GROUP BY True` is a special hint to inline
                    # this CTE as a subquery.
                    query.group_clause = []
                    rel.strategy = InlineStrategy.Subquery

                elif len(rel.used_in) == 1:
                    rel.strategy = InlineStrategy.Subquery

                else:
                    # TODO: learn how to inline CREs with GROUP BY.
                    rel.strategy = InlineStrategy.Skip
            else:
                # TODO: try to estimate the complexity of this CTE by
                # analyzing if it has WHERE / JOINS. Also, handle
                # `GROUP BY True` here.
                rel.strategy = InlineStrategy.Skip

        return rels


class RangeAnalyzer(ast.NodeVisitor):

    def __init__(self):
        super().__init__()
        self.aliases = set()

    def visit_RangeVar(self, node):
        self.aliases.add(node.alias.aliasname)

    def visit_RangeSubselect(self, node):
        self.aliases.add(node.alias.aliasname)

    def visit_SelectStmt(self, node):
        # Skip Select nodes
        pass

    @classmethod
    def analyze(cls, rel: Relation) -> typing.Set[str]:
        analyzer = cls()
        for fc in rel.node.query.from_clause:
            analyzer.visit(fc)
        return analyzer.aliases
