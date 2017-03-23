##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import typing

from edgedb.lang.common import ast
from edgedb.server.pgsql import ast as pgast


class Relation:

    # Relation name if it is a CTE
    name: typing.Optional[str]

    # Is Relation a CTE?
    is_cte: bool = False

    # Can this Relation be inlined.  For instance non-select queries
    # do not qualify for inlining in general and are left as is.
    is_inlineable: bool = False

    # A reference to the query AST node.
    query: pgast.Query

    # A reference to the containing Relation.
    parent: typing.Optional['Relation']

    # A mapping of Relations where this Relation is referenced from
    # to the referencing alias names.
    used_in: typing.Mapping['Relation', str]

    def __init__(self, *, name: str,
                 query: pgast.Query, parent: 'Relation',
                 is_cte: bool, is_inlineable: bool):
        self.name = name
        self.query = query
        self.parent = parent
        self.used_in = collections.OrderedDict()
        self.is_cte = is_cte
        self.is_inlineable = is_inlineable

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
        self._inline_counter = 0

    def remove_relation(self, rel: Relation):
        def remove_cte(query: pgast.Query, cte_name: str):
            new_ctes = []
            for cte in query.ctes:
                if cte.name != cte_name:
                    new_ctes.append(cte)
            query.ctes = new_ctes

        self.rels.remove(rel)
        remove_cte(rel.parent.query, rel.name)

    def get_new_inline_index(self):
        self._inline_counter += 1
        return self._inline_counter


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
