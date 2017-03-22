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


class Relation:

    name: str
    has_joins: bool = False

    node: pgast.CommonTableExpr
    parent: pgast.Query
    used_in: collections.OrderedDict

    strategy: InlineStrategy = InlineStrategy.Skip

    def __init__(self, name, node, parent):
        self.name = name
        self.node = node
        self.parent = parent
        self.used_in = collections.OrderedDict()

    def get_target(self, name: str):
        # NOTE: We can't cache the search results because
        # self.node.query can be mutated at any time.

        query: pgast.SelectStmt = self.node.query

        for node in query.target_list:  # type: pgast.ResTarget
            col_ref = node.val
            if isinstance(col_ref, pgast._Ref):
                col_ref = col_ref.node

            if node.name is not None:
                if node.name == name:
                    return col_ref
                else:
                    continue

            if isinstance(col_ref, pgast.ColumnRef):
                if col_ref.name[-1] == name:
                    return col_ref
                elif (len(col_ref.name) == 2 and
                        isinstance(col_ref.name[1], pgast.Star)):
                    return pgast.ColumnRef(name=[col_ref.name[0], name])
                else:
                    continue

        raise IndexError(
            f'could not find target {name!r} in CTE {self.name}')

    def get_range_aliases(self):
        return RangeAnalyzer.analyze(self)


class QueryInfo:

    tree: pgast.Query
    rels: typing.List[Relation]
    col_refs: typing.Mapping[str, typing.List[pgast.ColumnRefTypes]]
    range_refs: typing.Mapping[str, typing.List[pgast.RangeTypes]]

    def __init__(self, tree, rels, col_refs, range_refs):
        self.tree = tree
        self.rels = rels
        self.col_refs = col_refs
        self.range_refs = range_refs


class ContextLevel(compiler.ContextLevel):

    current_query: pgast.Query

    def __init__(self, prevlevel=None, mode=None):
        if prevlevel is None:
            self.current_query = None
        else:
            self.current_query = prevlevel.current_query


class Context(compiler.CompilerContext):

    ContextLevelClass = ContextLevel
    default_mode = None


class Analyzer(ast.NodeTransformer):

    def __init__(self):
        super().__init__()
        self.rels = collections.OrderedDict()
        self.range_refs = {}
        self.col_refs = {}
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
        if isinstance(node.relation, pgast.CommonTableExpr):
            relation_name = node.relation.name
            # If node.relation is a CTE, replace it with a Relation node
            # to make sure we don't do unnecessary deep copies.
            node.relation = pgast.Relation(relname=relation_name)
        elif isinstance(node.relation, pgast.Relation):
            relation_name = node.relation.relname
        else:
            raise TypeError('unexpected type in RangeVar.relation')

        if node.alias is not None:
            alias = node.alias.aliasname
        else:
            alias = relation_name

        ref = pgast._Ref(node=node)

        if alias not in self.range_refs:
            self.range_refs[alias] = []
        self.range_refs[alias].append(ref)

        if relation_name in self.rels:
            ctx = self.context.current
            rel = self.rels[relation_name]
            if ctx.current_query not in rel.used_in:
                rel.used_in[ctx.current_query] = alias

        return ref

    def visit_ColumnRef(self, node: pgast.ColumnRef):
        rel_name = node.name[0]

        ref = pgast._Ref(node=node)

        if rel_name not in self.col_refs:
            self.col_refs[rel_name] = []
        self.col_refs[rel_name].append(ref)

        return ref

    def visit_Query(self, node: pgast.Query):
        for cte in node.ctes:
            if isinstance(cte.query, pgast.SelectStmt):
                rel = self._process_cte(node, cte)
                self.visit(cte.query)
                if rel is not None:
                    self.rels[rel.name] = rel

        with self.context.new() as ctx:
            ctx.current_query = node

            # Because NodeVisitor keeps an internal memo of visited nodes,
            # CTE/ColumnRef/RangeVar nodes won't be re-visited.
            self.generic_visit(node)

        return node

    @classmethod
    def analyze(cls, tree: pgast.Base) -> QueryInfo:
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

        return QueryInfo(
            tree,
            list(rels.values()),
            analyzer.col_refs,
            analyzer.range_refs)


class RangeAnalyzer(ast.NodeVisitor):

    def __init__(self):
        super().__init__()
        self.aliases = set()

    def visit_RangeVar(self, node):
        self.aliases.add(node.alias.aliasname)

    def visit_RangeSubselect(self, node):
        self.aliases.add(node.alias.aliasname)

    def visit_Query(self, node):
        # Skip Select/Insert/etc nodes
        pass

    @classmethod
    def analyze(cls, rel: Relation) -> typing.Set[str]:
        analyzer = cls()
        for fc in rel.node.query.from_clause:
            analyzer.visit(fc)
        return analyzer.aliases
