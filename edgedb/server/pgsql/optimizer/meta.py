##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import copy
import typing

from edgedb.lang.common import ast
from edgedb.lang.common import markup

from edgedb.server.pgsql import codegen as pgcodegen
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

    range_names: typing.Set[str]

    def __init__(self, *, name: str,
                 query: pgast.Query, parent: 'Relation',
                 is_cte: bool, is_inlineable: bool):
        self.name = name
        self.query = query
        self.parent = parent
        self.used_in = collections.OrderedDict()
        self.is_cte = is_cte
        self.is_inlineable = is_inlineable
        self.range_names = set()

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

    def __repr__(self):
        return f'<Relation {id(self):#x}>'


class QueryInfo:

    tree: pgast.Query
    rels: typing.List[Relation]
    col_refs: typing.Mapping[str, typing.Set[pgast._Ref]]
    range_refs: typing.Mapping[str, typing.Set[pgast._Ref]]

    def __init__(self, tree):
        self.tree = tree
        self.rels = []
        self.col_refs = {}
        self.range_refs = {}
        self._inline_counter = 0

    def discard_relation(self, rel: Relation):
        def remove_cte(query: pgast.Query, cte_name: str):
            new_ctes = []
            for cte in query.ctes:
                if cte.name != cte_name:
                    new_ctes.append(cte)
            query.ctes = new_ctes

        self.rels.remove(rel)

        if rel.is_cte:
            remove_cte(rel.parent.query, rel.name)

    def get_new_inline_index(self):
        self._inline_counter += 1
        return self._inline_counter

    def merge_range_names(self, *,
                          target_rel: Relation, source_rel: Relation,
                          inline_index: int):
        for al in source_rel.range_names:
            target_rel.range_names.add(f'i{inline_index}~{al}')

    def track_col_ref(self, name: str, ref: pgast._Ref):
        if name not in self.col_refs:
            self.col_refs[name] = set()
        self.col_refs[name].add(ref)

    def track_range_ref(self, name: str, ref: pgast._Ref):
        if name not in self.range_refs:
            self.range_refs[name] = set()
        self.range_refs[name].add(ref)

    def _deepcopy_subtree(self, x, memo, renames, prefix, _nil=[]):
        d = id(x)
        y = memo.get(d, _nil)
        if y is not _nil:
            return y

        if ast.is_ast_node(x):
            xt = x.__class__
            y = xt()

            for field_name, field in x._fields.items():
                if field.meta:
                    continue

                field_value = getattr(x, field_name, _nil)
                if field_value is not _nil:
                    if ast.is_ast_node(field_value):
                        field_value = self._deepcopy_subtree(
                            field_value, memo, renames, prefix)
                        field_value.parent = y

                    elif ast.is_container(field_value):
                        new_values = []
                        for v in field_value:
                            v = self._deepcopy_subtree(
                                v, memo, renames, prefix)
                            if ast.is_ast_node(v):
                                v.parent = y
                            new_values.append(v)

                        if field_value.__class__ is list:
                            field_value = new_values
                        else:
                            field_value = field_value.__class__(new_values)

                    else:
                        field_value = copy.deepcopy(
                            field_value, memo, _nil=_nil)

                    setattr(y, field_name, field_value)

            if renames is not None:
                if xt is pgast.Alias:
                    if y.aliasname in renames:
                        y.aliasname = prefix + y.aliasname

                elif xt is pgast.ColumnRef:
                    if y.name[0] in renames:
                        y.name[0] = prefix + y.name[0]

            if xt is pgast._Ref:
                RefsUpdater.update_refs_index(self, y)

            memo[d] = y
            return y
        else:
            return copy.deepcopy(x, memo)

    def copy_subtree_and_rename_refs(self, tree: pgast.Base, *,
                                     source_rel: Relation,
                                     inline_index: int,
                                     deepcopy):
        prefix = f'i{inline_index}~'
        if deepcopy:
            return self._deepcopy_subtree(
                tree, {}, source_rel.range_names, prefix)
        else:
            tree = copy.copy(tree)
            RefsUpdater.update(self, source_rel.range_names, prefix, tree)
            return tree

    def copy_subtree(self, tree: pgast.Base, *,
                     source_rel: Relation,
                     deepcopy):
        if deepcopy:
            return self._deepcopy_subtree(tree, {}, None, None)
        else:
            return copy.copy(tree)


class RefsUpdater(ast.NodeVisitor):

    def __init__(self, qi, range_aliases, prefix: str):
        self.prefix = prefix
        self.range_aliases = range_aliases
        self.qi = qi
        super().__init__()

    @staticmethod
    def update_refs_index(qi: QueryInfo, ref: pgast._Ref):
        if isinstance(ref.node, pgast.ColumnRef):
            qi.track_col_ref(ref.node.name[0], ref)

        elif isinstance(ref.node, pgast.RangeVar):
            # RangeVars are normalized by analyzer.visit_RangeVar
            alias = ref.node.relation.relname
            if ref.node.alias is not None:
                alias = ref.node.alias.aliasname
            qi.track_range_ref(alias, ref)

    def visit__Ref(self, node: pgast._Ref):
        self.visit(node.node)
        self.update_refs_index(self.qi, node)

    def visit_Alias(self, node: pgast.Alias):
        if node.aliasname in self.range_aliases:
            node.aliasname = self.prefix + node.aliasname

    def visit_ColumnRef(self, node: pgast.ColumnRef):
        if node.name[0] in self.range_aliases:
            node.name[0] = self.prefix + node.name[0]

    @classmethod
    def update(cls, qi, range_aliases, prefix: str, node: pgast.Alias):
        nu = cls(qi, range_aliases, prefix)
        nu.visit(node)


@markup.serializer.serializer.register(Relation)
def _serialize_to_markup(rel, *, ctx):
    node = markup.elements.lang.TreeNode(
        id=id(rel),
        name=type(rel).__name__)

    attrs = set(Relation.__annotations__)
    attrs.discard('query')
    attrs.discard('parent')

    if rel.parent is not None:
        node.add_child(
            label='parent',
            node=markup.elements.lang.Object(
                id=id(rel.parent),
                class_module='',
                classname=type(rel.parent).__name__,
                repr=repr(rel.parent)
            ))

    for attr in attrs:
        node.add_child(
            label=attr,
            node=markup.serialize(getattr(rel, attr, None), ctx=ctx))

    codegen = pgcodegen.SQLSourceGenerator()
    codegen.visit(rel.query)
    code = ''.join(codegen.result)

    node.add_child(
        label='query',
        node=markup.serialize_code(code, lexer='SQL'))

    return node
