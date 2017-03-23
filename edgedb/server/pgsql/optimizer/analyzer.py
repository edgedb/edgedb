##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast
from edgedb.lang.common import compiler
from edgedb.server.pgsql import ast as pgast

from . import meta


class ContextLevel(compiler.ContextLevel):

    current_rel: meta.Relation = None

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

        rel = meta.Relation(
            name=None,
            query=node,
            is_cte=False,
            is_inlineable=self.is_inlineable_query(node),
            parent=ctx.current_rel)

        self.process_query(node, rel)
        return node

    def is_inlineable_query(self, node: pgast.Query):
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

    def process_query(self, node: pgast.Query, rel: meta.Relation):
        if node in self.processed_queries:
            # Because we use "generic_visit" to visit queries nodes
            # we need to maintain our own visiting memo.
            return node
        else:
            self.processed_queries.add(node)

        for cte in node.ctes:
            cte_name = cte.name
            cte_rel = meta.Relation(
                name=cte_name,
                query=cte.query,
                is_cte=True,
                is_inlineable=self.is_inlineable_query(cte.query),
                parent=rel)

            self.process_query(cte.query, cte_rel)

            if self.is_inlineable_query(cte.query):
                self.ctes[cte_name] = cte_rel

        with self.context.new() as ctx:
            ctx.current_rel = rel
            self.generic_visit(node)

        self.rels.append(rel)

    @classmethod
    def analyze(cls, tree: pgast.Query) -> meta.QueryInfo:
        analyzer = cls()
        analyzer.visit(tree)

        return meta.QueryInfo(
            tree,
            analyzer.rels,
            analyzer.col_refs,
            analyzer.range_refs)
