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

    name: typing.Optional[str]
    is_cte: bool = False

    query: pgast.Query
    parent: typing.Optional['Relation']
    used_in: collections.OrderedDict

    strategy: InlineStrategy = InlineStrategy.Skip

    def __init__(self, *, name: str,
                 query: pgast.Query, parent: 'Relation', is_cte: bool):
        self.name = name
        self.query = query
        self.parent = parent
        self.used_in = collections.OrderedDict()
        self.is_cte = is_cte

    def get_target(self, name: str):
        # NOTE: We can't cache the search results because
        # self.node.query can be mutated at any time.

        query: pgast.SelectStmt = self.query

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

    current_rel: Relation = None

    def __init__(self, prevlevel=None, mode=None):
        if prevlevel is None:
            self.current_rel = None
        else:
            self.current_rel = prevlevel.current_rel


class Context(compiler.CompilerContext):

    ContextLevelClass = ContextLevel
    default_mode = None


class Analyzer(ast.NodeTransformer):

    def __init__(self):
        super().__init__()
        self.ctes = {}
        self.rels = []
        self.range_refs = {}
        self.col_refs = {}
        self.context = Context()
        self.processed_queries = set()

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

        if relation_name in self.ctes:
            ctx = self.context.current
            rel = self.ctes[relation_name]
            if ctx.current_rel not in rel.used_in:
                rel.used_in[ctx.current_rel] = alias

        return ref

    def visit_ColumnRef(self, node: pgast.ColumnRef):
        rel_name = node.name[0]

        ref = pgast._Ref(node=node)

        if rel_name not in self.col_refs:
            self.col_refs[rel_name] = []
        self.col_refs[rel_name].append(ref)

        return ref

    def visit_Query(self, node: pgast.Query):
        ctx = self.context.current

        rel = Relation(
            name=None,
            query=node,
            is_cte=False,
            parent=ctx.current_rel)

        self.process_query(node, rel)
        return node

    def is_inlineable_cte(self, node: pgast.Query):
        return (
            isinstance(node, pgast.SelectStmt) and

            all(isinstance(t, pgast.ResTarget) and
                not t.indirection
                for t in node.target_list) and

            not node.window_clause and
            not node.values and
            not node.locking_clause and

            not node.op and
            not node.all and
            not node.larg and
            not node.rarg
        )

    def process_query(self, node: pgast.Query, rel: Relation):
        if node in self.processed_queries:
            # Because we use "generic_visit" to visit queries nodes
            # we need to maintain our own visiting memo.
            return node
        else:
            self.processed_queries.add(node)

        for cte in node.ctes:
            cte_name = cte.name
            cte_rel = Relation(
                name=cte_name,
                query=cte.query,
                is_cte=True,
                parent=rel)

            self.process_query(cte.query, cte_rel)

            if self.is_inlineable_cte(cte.query):
                self.ctes[cte_name] = cte_rel

        with self.context.new() as ctx:
            ctx.current_rel = rel
            self.generic_visit(node)

        self.rels.append(rel)

    @classmethod
    def analyze(cls, tree: pgast.Query) -> QueryInfo:
        analyzer = cls()
        analyzer.visit(tree)

        rels = analyzer.rels
        for rel in rels:
            query = rel.query

            if not rel.is_cte or not len(rel.used_in):
                rel.strategy = InlineStrategy.Skip
                continue

            if len(query.from_clause) == 0 and len(rel.used_in) == 1:
                rel.strategy = InlineStrategy.Subquery

            elif len(query.from_clause) == 1:
                if not query.group_clause:
                    if (isinstance(query.having, pgast.Constant) and
                            query.having.val is True):
                        # `HAVING True` is a special hint to inline
                        # this CTE as a subquery.
                        rel.strategy = InlineStrategy.Subquery
                        query.having = None
                    else:
                        rel.strategy = InlineStrategy.Merge

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
            rels,
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
        for fc in rel.query.from_clause:
            analyzer.visit(fc)
        return analyzer.aliases
