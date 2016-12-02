##
# Copyright (c) 2008-2015 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL to IR compiler."""

import collections
import itertools

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import types as s_types

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import errors
from edgedb.lang.edgeql import parser

from edgedb.lang.common import ast
from edgedb.lang.common import debug
from edgedb.lang.common import exceptions as edgedb_error
from edgedb.lang.common import markup  # NOQA

from .. import parser
from . import compiler2
from .decompiler import decompile_ir  # NOQA

from . import pathmerger
from . import rewriter


Void = object()


class ParseContextLevel(object):
    def __init__(self, prevlevel=None, mode=None):
        if prevlevel is not None:
            if mode == ParseContext.SUBQUERY:
                self.graph = None
                self.pathvars = {}
                self.anchors = {}
                self.namespaces = prevlevel.namespaces.copy()
                self.location = None
                self.groupprefixes = None
                self.in_aggregate = []
                self.in_func_call = False
                self.arguments = prevlevel.arguments
                self.context_vars = prevlevel.context_vars
                self.schema = prevlevel.schema
                self.module_aliases = prevlevel.module_aliases
                self.aliascnt = prevlevel.aliascnt.copy()
                self.subgraphs_map = {}
                self.cge_map = prevlevel.cge_map.copy()
                self.weak_path = None
                self.apply_access_control_rewrite = \
                    prevlevel.apply_access_control_rewrite
                self.local_link_source = None
                self.in_type_check = False
            else:
                self.graph = prevlevel.graph
                self.pathvars = prevlevel.pathvars
                self.anchors = prevlevel.anchors
                self.namespaces = prevlevel.namespaces
                self.location = prevlevel.location
                self.groupprefixes = prevlevel.groupprefixes
                self.in_aggregate = prevlevel.in_aggregate[:]
                self.in_func_call = prevlevel.in_func_call
                self.arguments = prevlevel.arguments
                self.context_vars = prevlevel.context_vars
                self.schema = prevlevel.schema
                self.module_aliases = prevlevel.module_aliases
                self.aliascnt = prevlevel.aliascnt
                self.subgraphs_map = prevlevel.subgraphs_map
                self.cge_map = prevlevel.cge_map
                self.weak_path = prevlevel.weak_path
                self.apply_access_control_rewrite = \
                    prevlevel.apply_access_control_rewrite
                self.local_link_source = prevlevel.local_link_source
                self.in_type_check = False
        else:
            self.graph = None
            self.pathvars = {}
            self.anchors = {}
            self.namespaces = {}
            self.location = None
            self.groupprefixes = None
            self.in_aggregate = []
            self.in_func_call = False
            self.arguments = {}
            self.context_vars = {}
            self.schema = None
            self.module_aliases = None
            self.aliascnt = {}
            self.subgraphs_map = {}
            self.cge_map = {}
            self.weak_path = None
            self.apply_access_control_rewrite = False
            self.local_link_source = None
            self.in_type_check = False

    def genalias(self, hint=None):
        if hint is None:
            hint = 'a'

        if hint not in self.aliascnt:
            self.aliascnt[hint] = 1
        else:
            self.aliascnt[hint] += 1

        alias = hint
        number = self.aliascnt[hint]
        if number > 1:
            alias += str(number)

        return alias


class ParseContext(object):
    CURRENT, NEW, SUBQUERY = range(0, 3)

    def __init__(self):
        self.stack = []
        self.push()

    def push(self, mode=None):
        level = ParseContextLevel(self.current, mode)
        self.stack.append(level)

        return level

    def pop(self):
        self.stack.pop()

    def __call__(self, mode=None):
        if not mode:
            mode = ParseContext.NEW
        return ParseContextWrapper(self, mode)

    def _current(self):
        if len(self.stack) > 0:
            return self.stack[-1]
        else:
            return None

    current = property(_current)


class ParseContextWrapper(object):
    def __init__(self, context, mode):
        self.context = context
        self.mode = mode

    def __enter__(self):
        if self.mode == ParseContext.CURRENT:
            return self.context
        else:
            self.context.push()
            return self.context

    def __exit__(self, exc_type, exc_value, traceback):
        self.context.pop()


class EdgeQLCompilerError(edgedb_error.EdgeDBError):
    pass


def get_ns_aliases_cges(node):
    namespaces = []
    aliases = []
    cges = []

    for alias in node.aliases:
        if isinstance(alias, qlast.NamespaceAliasDeclNode):
            namespaces.append(alias)
        elif isinstance(alias, qlast.CGENode):
            cges.append(alias)
        else:
            aliases.append(alias)

    return namespaces, aliases, cges


def is_aggregated_expr(expr, deep=False):
    agg = getattr(expr, 'aggregates', False)

    if not agg and deep:
        return bool(
            list(
                ast.find_children(
                    expr, lambda i: getattr(i, 'aggregates', None))))
    return agg


class FixupTransformer(ast.NodeTransformer):
    """Attempt to fixup potential brokenness of a fully processed tree."""

    def visit_EntitySet(self, expr):
        if expr.rlink:
            self.visit(expr.rlink.source)

        if expr.conjunction.paths:
            # Move non-filtering paths out of a conjunction
            # into a disjunction where it belongs.

            cpaths = set()
            dpaths = set()

            for path in expr.conjunction.paths:
                if isinstance(path, irast.EntitySet):
                    if 'generator' not in path.users:
                        dpaths.add(path)
                    else:
                        cpaths.add(path)
                elif isinstance(path, irast.EntityLink):
                    if (path.target and
                            'generator' not in path.target.users and
                            'generator' not in path.users):
                        dpaths.add(path)
                    else:
                        cpaths.add(path)

            expr.conjunction.paths = frozenset(cpaths)
            if dpaths:
                expr.disjunction.paths = expr.disjunction.paths | dpaths

        return expr


class SubqueryLinker(ast.NodeTransformer):
    """Link subqueries to the main query."""

    def visit_GraphExpr(self, expr):
        paths = irutils.get_path_index(expr)

        subpaths = irutils.extract_paths(
            expr,
            reverse=True,
            resolve_arefs=False,
            recurse_subqueries=2,
            all_fragments=True)

        if isinstance(subpaths, irast.PathCombination):
            irutils.flatten_path_combination(subpaths, recursive=True)
            subpaths = subpaths.paths
        elif subpaths is not None:
            subpaths = {subpaths}
        else:
            subpaths = set()

        for subpath in subpaths:
            if isinstance(subpath, irast.EntitySet):
                outer = paths.getlist(subpath.id)

                if outer and subpath not in outer:
                    subpath.reference = outer[0]

        return self.generic_visit(expr)


class AggregateSorter(ast.NodeTransformer):
    """Reorder aggregate expressions."""

    def node_visit(self, expr):
        if getattr(expr, 'aggregates', False):
            # No need to drill-down, the expression is known to be a
            # pure aggregate.
            return expr
        else:
            return super().node_visit(expr)

    def visit_FunctionCall(self, expr):
        has_agg_args = False

        for arg in expr.args:
            self.visit(arg)

            if is_aggregated_expr(arg):
                has_agg_args = True
            elif has_agg_args and not isinstance(expr, irast.Constant):
                raise EdgeQLCompilerError(
                    'invalid expression mix of aggregates '
                    'and non-aggregates')

        if has_agg_args:
            expr.aggregates = True

        return expr

    def visit_BinOp(self, expr):
        left = self.visit(expr.left)
        right = self.visit(expr.right)

        left_aggregates = is_aggregated_expr(left)
        right_aggregates = is_aggregated_expr(right)

        if (left_aggregates and
                (right_aggregates or isinstance(right, irast.Constant))
                or (isinstance(left, irast.Constant)
                    and right_aggregates)):
            expr.aggregates = True

        elif expr.op == ast.ops.AND:
            if right_aggregates:
                # Reorder the operands so that aggregate expr is
                # always on the left.
                expr.left, expr.right = expr.right, expr.left

        elif left_aggregates or right_aggregates:
            raise EdgeQLCompilerError(
                'invalid expression mix of aggregates and non-aggregates')

        return expr

    def visit_Sequence(self, expr):
        has_agg_elems = False
        for item in expr.elements:
            self.visit(item)
            if is_aggregated_expr(item):
                has_agg_elems = True
            elif has_agg_elems and not isinstance(expr, irast.Constant):
                raise EdgeQLCompilerError(
                    'invalid expression mix of aggregates '
                    'and non-aggregates')

        if has_agg_elems:
            expr.aggregates = True

        return expr

    visit_Record = visit_Sequence


class EdgeQLCompiler:
    def __init__(self, schema, module_aliases=None):
        self.schema = schema
        self.module_aliases = module_aliases

    def _init_context(self,
                      arg_types,
                      module_aliases,
                      anchors,
                      *,
                      security_context=None):
        self.context = context = ParseContext()
        self.context.current.schema = self.schema
        self.context.current.module_aliases = \
            module_aliases or self.module_aliases

        if self.context.current.module_aliases:
            self.context.current.namespaces.update(
                self.context.current.module_aliases)

        if anchors:
            self._populate_anchors(context, anchors)

        if security_context:
            self.context.current.apply_access_control_rewrite = True

        return context

    def _is_func_agg(self, name):
        if isinstance(name, str):
            name = (None, name)

        return self._get_schema_object(
            self.context, name=name[1], module=name[0]).aggregate

    def transform(self,
                  edgeql_tree,
                  arg_types,
                  module_aliases=None,
                  anchors=None,
                  security_context=None):
        context = self._init_context(
            arg_types,
            module_aliases,
            anchors,
            security_context=security_context)

        if isinstance(edgeql_tree, qlast.SelectQueryNode):
            stree = self._transform_select(context, edgeql_tree, arg_types)
        elif isinstance(edgeql_tree, qlast.InsertQueryNode):
            stree = self._transform_insert(context, edgeql_tree, arg_types)
        elif isinstance(edgeql_tree, qlast.UpdateQueryNode):
            stree = self._transform_update(context, edgeql_tree, arg_types)
        elif isinstance(edgeql_tree, qlast.DeleteQueryNode):
            stree = self._transform_delete(context, edgeql_tree, arg_types)
        else:
            msg = 'unexpected statement type: {!r}'.format(edgeql_tree)
            raise ValueError(msg)

        FixupTransformer.run(stree)
        rewriter.RewriteTransformer.run(stree, context=self.context)

        return stree

    def transform_fragment(self,
                           edgeql_tree,
                           arg_types,
                           module_aliases=None,
                           anchors=None,
                           location=None):
        context = self._init_context(arg_types, module_aliases, anchors)
        context.current.location = location or 'generator'

        if isinstance(edgeql_tree, qlast.SelectQueryNode):
            stree = self._transform_select(context, edgeql_tree, arg_types)
        else:
            stree = self._process_expr(context, edgeql_tree)

        return stree

    def _populate_anchors(self, context, anchors):
        for anchor, scls in anchors.items():
            if isinstance(scls, s_obj.NodeClass):
                step = irast.EntitySet()
                step.concept = scls
                step.id = irutils.LinearPath([step.concept])
                step.anchor = anchor
                step.show_as_anchor = anchor
                # XXX
                # step.users =
            elif isinstance(scls, s_links.Link):
                if scls.source:
                    src = irast.EntitySet()
                    src.concept = scls.source
                    src.id = irutils.LinearPath([src.concept])
                else:
                    src = None

                step = irast.EntityLink(link_class=scls, source=src)
                step.anchor = anchor
                step.show_as_anchor = anchor

                if src:
                    src.disjunction.update(step)

            elif isinstance(scls, s_lprops.LinkProperty):
                ptr_name = scls.shortname

                if scls.source.source:
                    src = irast.EntitySet()
                    src.concept = scls.source.source
                    src.id = irutils.LinearPath([src.concept])
                else:
                    src = None

                link = irast.EntityLink(
                    link_class=scls.source,
                    source=src,
                    direction=s_pointers.PointerDirection.Outbound)

                if src:
                    src.disjunction.update(link)
                    pref_id = irutils.LinearPath(src.id)
                else:
                    pref_id = irutils.LinearPath([])

                pref_id.add(scls.source, s_pointers.PointerDirection.Outbound,
                            scls.target)

                step = irast.LinkPropRefSimple(
                    name=ptr_name, ref=link, id=pref_id, ptr_class=scls)
                step.anchor = anchor
                step.show_as_anchor = anchor

            else:
                step = scls

            context.current.anchors[anchor] = step

    def _transform_select(self, context, edgeql_tree, arg_types):
        self.arg_types = arg_types or {}

        graph = context.current.graph = irast.GraphExpr()

        pathvars = context.current.pathvars
        namespaces = context.current.namespaces
        t_ns, t_aliases, t_cges = get_ns_aliases_cges(edgeql_tree)

        with context():
            context.current.location = 'generator'

            for alias_decl in t_ns:
                namespaces[alias_decl.alias] = alias_decl.namespace

            for alias_decl in t_aliases:
                expr = self._process_expr(context, alias_decl.expr)
                if isinstance(expr, irast.Path):
                    expr.pathvar = alias_decl.alias

                pathvars[alias_decl.alias] = expr

        if context.current.module_aliases:
            context.current.namespaces.update(context.current.module_aliases)

        if t_cges:
            graph.cges = []

            for cge in t_cges:
                with context(ParseContext.SUBQUERY):
                    _cge = self._transform_select(context, cge.expr, arg_types)
                context.current.cge_map[cge.alias] = _cge
                graph.cges.append(
                    irast.CommonGraphExpr(
                        expr=_cge, alias=cge.alias))

        if edgeql_tree.op:
            graph.set_op = qlast.SetOperator(edgeql_tree.op)
            graph.set_op_larg = self._transform_select(
                context, edgeql_tree.op_larg, arg_types)
            graph.set_op_rarg = self._transform_select(
                context, edgeql_tree.op_rarg, arg_types)
        else:
            graph.generator = self._process_select_where(context,
                                                         edgeql_tree.where)

            graph.grouper = self._process_grouper(context, edgeql_tree.groupby)
            if graph.grouper:
                groupgraph = irast.Disjunction(paths=frozenset(graph.grouper))
                context.current.groupprefixes = \
                    irutils.extract_prefixes(groupgraph)
            else:
                # Check if query() or order() contain any aggregate
                # expressions and if so, add a sentinel group prefix
                # instructing the transformer that we are implicitly grouping
                # the whole set.
                def checker(n):
                    if isinstance(n, qlast.FunctionCallNode):
                        return self._is_func_agg(n.func)
                    elif isinstance(n, qlast.SelectQueryNode):
                        # Make sure we don't dip into subqueries
                        raise ast.SkipNode()

                for node in itertools.chain(edgeql_tree.orderby or [],
                                            edgeql_tree.targets or []):
                    if ast.find_children(node, checker, force_traversal=True):
                        context.current.groupprefixes = {True: True}
                        break

            graph.selector = self._process_select_targets(context,
                                                          edgeql_tree.targets)

            if (len(edgeql_tree.targets) == 1 and
                    isinstance(edgeql_tree.targets[0].expr, qlast.PathNode) and
                    not graph.generator):
                # This is a node selector query, ensure it is treated as
                # a generator path even in potential absence of an explicit
                # generator expression.
                def augmenter(n):
                    if isinstance(n, irast.Record):
                        if n.rlink is not None:
                            n.rlink.users.add('generator')
                            n.rlink.source.users.add('generator')
                            self._postprocess_expr(n.rlink.source)
                        raise ast.SkipNode()
                    if hasattr(n, 'users'):
                        n.users.add('generator')
                    if isinstance(n, irast.EntitySet):
                        self._postprocess_expr(n)

                with context():
                    context.current.location = 'generator'
                    for expr in graph.selector:
                        ast.find_children(
                            expr, augmenter, force_traversal=True)

        graph.sorter = self._process_sorter(context, edgeql_tree.orderby)
        if edgeql_tree.offset:
            graph.offset = irast.Constant(
                value=edgeql_tree.offset.value,
                index=edgeql_tree.offset.index,
                type=context.current.schema.get('int'))

        if edgeql_tree.limit:
            graph.limit = irast.Constant(
                value=edgeql_tree.limit.value,
                index=edgeql_tree.limit.index,
                type=context.current.schema.get('int'))

        context.current.location = 'top'

        # Merge selector and sorter disjunctions first
        paths = [s.expr for s in graph.selector] + \
                [s.expr for s in graph.sorter] + \
                [s for s in graph.grouper]
        union = irast.Disjunction(paths=frozenset(paths))
        pathmerger.flatten_and_unify_path_combination(
            context, union, deep=True, merge_filters=True)

        # Merge the resulting disjunction with generator conjunction
        if graph.generator:
            paths = [graph.generator] + list(union.paths)
            union = irast.Disjunction(paths=frozenset(paths))
            pathmerger.flatten_and_unify_path_combination(
                context, union, deep=True, merge_filters=True)

        # Reorder aggregate expressions so that all of them appear as the
        # first sub-tree in the generator expression.
        #
        if graph.generator:
            self.reorder_aggregates(graph.generator)

        graph.result_types = self.get_selector_types(graph.selector,
                                                     self.schema)
        graph.argument_types = self.context.current.arguments
        graph.context_vars = self.context.current.context_vars

        self.link_subqueries(graph)

        return graph

    def _transform_insert(self, context, edgeql_tree, arg_types):
        self.arg_types = arg_types or {}

        graph = context.current.graph = irast.GraphExpr()
        graph.op = 'insert'
        t_ns, t_aliases, t_cges = get_ns_aliases_cges(edgeql_tree)

        for ns in t_ns:
            context.current.namespaces[ns.alias] = ns.namespace

        if context.current.module_aliases:
            context.current.namespaces.update(context.current.module_aliases)

        if t_cges:
            graph.cges = []

            for cge in t_cges:
                with context(ParseContext.SUBQUERY):
                    _cge = self._transform_select(context, cge.expr, arg_types)
                context.current.cge_map[cge.alias] = _cge
                graph.cges.append(
                    irast.CommonGraphExpr(
                        expr=_cge, alias=cge.alias))

        tgt = graph.optarget = self._process_select_where(context,
                                                          edgeql_tree.subject)

        idname = sn.Name('std::id')
        idref = irast.AtomicRefSimple(
            name=idname, ref=tgt, ptr_class=tgt.concept.pointers[idname])
        tgt.atomrefs.add(idref)
        selexpr = irast.SelectorExpr(expr=idref, name=None)
        graph.selector.append(selexpr)

        if edgeql_tree.targets:
            with context():
                context.current.location = 'optarget_shaper'
                graph.opselector = self._process_select_targets(
                    context, edgeql_tree.targets)
        else:
            rec = self.entityref_to_record(tgt, self.schema)
            graph.opselector = [
                irast.SelectorExpr(
                    expr=rec,
                    name=context.current.genalias('o')
                )
            ]

        if edgeql_tree.pathspec:
            with context():
                context.current.location = 'opvalues'
                graph.opvalues = self._process_insert_values(
                    context, graph, edgeql_tree.pathspec)

        context.current.location = 'top'

        paths = [s.expr for s in graph.opselector] + \
                [s.expr for s in graph.selector] + [graph.optarget]

        if graph.generator:
            paths.append(graph.generator)

        union = irast.Disjunction(paths=frozenset(paths))
        pathmerger.flatten_and_unify_path_combination(
            context, union, deep=True, merge_filters=True)

        # Reorder aggregate expressions so that all of them appear as the
        # first sub-tree in the generator expression.
        #
        if graph.generator:
            self.reorder_aggregates(graph.generator)

        graph.result_types = self.get_selector_types(graph.opselector,
                                                     self.schema)
        graph.argument_types = self.context.current.arguments
        graph.context_vars = self.context.current.context_vars

        self.link_subqueries(graph)

        return graph

    def _transform_update(self, context, edgeql_tree, arg_types):
        self.arg_types = arg_types or {}

        graph = context.current.graph = irast.GraphExpr()
        graph.op = 'update'
        t_ns, t_aliases, t_cges = get_ns_aliases_cges(edgeql_tree)

        for ns in t_ns:
            context.current.namespaces[ns.alias] = ns.namespace

        if context.current.module_aliases:
            context.current.namespaces.update(context.current.module_aliases)

        if t_cges:
            graph.cges = []

            for cge in t_cges:
                with context(ParseContext.SUBQUERY):
                    _cge = self._transform_select(context, cge.expr, arg_types)
                context.current.cge_map[cge.alias] = _cge
                graph.cges.append(
                    irast.CommonGraphExpr(
                        expr=_cge, alias=cge.alias))

        tgt = graph.optarget = self._process_select_where(context,
                                                          edgeql_tree.subject)

        idname = sn.Name('std::id')
        idref = irast.AtomicRefSimple(
            name=idname, ref=tgt, ptr_class=tgt.concept.pointers[idname])
        tgt.atomrefs.add(idref)
        selexpr = irast.SelectorExpr(expr=idref, autoname='std::id')
        graph.selector.append(selexpr)

        graph.generator = self._process_select_where(context,
                                                     edgeql_tree.where)

        if edgeql_tree.targets:
            with context():
                context.current.location = 'optarget_shaper'
                graph.opselector = self._process_select_targets(
                    context, edgeql_tree.targets)
        else:
            rec = self.entityref_to_record(tgt, self.schema)
            graph.opselector = [irast.SelectorExpr(expr=rec)]

        with context():
            context.current.location = 'opvalues'
            graph.opvalues = self._process_update_values(context, graph,
                                                         edgeql_tree.pathspec)

        context.current.location = 'top'

        paths = [s.expr for s in graph.opselector] + \
                [s.expr for s in graph.selector] + [graph.optarget]

        if graph.generator:
            paths.append(graph.generator)

        union = irast.Disjunction(paths=frozenset(paths))
        pathmerger.flatten_and_unify_path_combination(
            context, union, deep=True, merge_filters=True)

        # Reorder aggregate expressions so that all of them appear as the
        # first sub-tree in the generator expression.
        #
        if graph.generator:
            self.reorder_aggregates(graph.generator)

        graph.result_types = self.get_selector_types(graph.opselector,
                                                     self.schema)
        graph.argument_types = self.context.current.arguments
        graph.context_vars = self.context.current.context_vars

        self.link_subqueries(graph)

        return graph

    def _process_insert_values(self, context, graph, opvalues):
        refs = []

        for pathspec in opvalues:
            value = pathspec.compexpr

            tpath = pathspec.expr

            targetexpr = self._process_path(
                context, tpath, path_tip=graph.optarget)

            if isinstance(value, qlast.ConstantNode):
                v = self._process_constant(context, value)
            else:
                v = self._process_expr(context, value)
                paths = irast.Conjunction(paths=frozenset((v, graph.optarget)))
                pathmerger.flatten_and_unify_path_combination(
                    context, paths, deep=True, merge_filters=True)
                # self._check_update_expr(graph.optarget, targetexpr, v)

            ref = irast.UpdateExpr(expr=targetexpr, value=v)
            refs.append(ref)

        return refs

    def _process_update_values(self, context, graph, opvalues):
        refs = []

        for pathspec in opvalues:
            value = pathspec.compexpr

            tpath = pathspec.expr

            targetexpr = self._process_path(
                context, tpath, path_tip=graph.optarget)

            if isinstance(value, qlast.ConstantNode):
                v = self._process_constant(context, value)
            else:
                with context():
                    context.current.local_link_source = graph.optarget
                    v = self._process_expr(context, value)

                paths = irast.Conjunction(paths=frozenset((v, graph.optarget)))
                pathmerger.flatten_and_unify_path_combination(
                    context, paths, deep=True, merge_filters=True)
                # self._check_update_expr(graph.optarget, targetexpr, v)

            ref = irast.UpdateExpr(expr=targetexpr, value=v)
            refs.append(ref)

        return refs

    def _check_update_expr(self, source, path, expr):
        # Check that all refs in expr point to source and are atomic,
        # or, if not, are in the form ptr := ptr {+|-} set
        #
        schema_scope = self.get_query_schema_scope(expr)
        ok = (len(schema_scope) == 0 or (len(schema_scope) == 1 and
                                         (schema_scope[0].id == source.id or
                                          schema_scope[0].id == path.id)))

        if not ok:
            msg = "update expression can only reference local atoms"
            raise errors.EdgeQLError(msg, context=path.context)

    def _transform_delete(self, context, edgeql_tree, arg_types):
        self.arg_types = arg_types or {}

        graph = context.current.graph = irast.GraphExpr()
        graph.op = 'delete'
        t_ns, t_aliases, t_cges = get_ns_aliases_cges(edgeql_tree)

        for ns in t_ns:
            context.current.namespaces[ns.alias] = ns.namespace

        if context.current.module_aliases:
            context.current.namespaces.update(context.current.module_aliases)

        if t_cges:
            graph.cges = []

            for cge in t_cges:
                with context(ParseContext.SUBQUERY):
                    _cge = self._transform_select(context, cge.expr, arg_types)
                context.current.cge_map[cge.alias] = _cge
                graph.cges.append(
                    irast.CommonGraphExpr(
                        expr=_cge, alias=cge.alias))

        tgt = graph.optarget = self._process_select_where(context,
                                                          edgeql_tree.subject)

        if (isinstance(tgt, irast.LinkPropRefSimple) and
                tgt.name == 'std::target'):

            idpropname = sn.Name('std::linkid')
            idprop_class = tgt.ref.link_class.pointers[idpropname]
            idref = irast.LinkPropRefSimple(
                name=idpropname, ref=tgt.ref, ptr_class=idprop_class)
            graph.optarget.ref.proprefs.add(idref)

            selexpr = irast.SelectorExpr(expr=idref, name=None)
            graph.selector.append(selexpr)

        elif isinstance(tgt, irast.AtomicRefSimple):
            idname = sn.Name('std::id')
            idref = irast.AtomicRefSimple(
                name=idname, ref=tgt.ref, ptr_class=tgt.ptr_class)
            tgt.ref.atomrefs.add(idref)
            selexpr = irast.SelectorExpr(expr=idref, name=None)
            graph.selector.append(selexpr)

        else:
            idname = sn.Name('std::id')
            idref = irast.AtomicRefSimple(
                name=idname, ref=tgt, ptr_class=tgt.concept.pointers[idname])
            tgt.atomrefs.add(idref)
            selexpr = irast.SelectorExpr(expr=idref, name=None)
            graph.selector.append(selexpr)

        graph.generator = self._process_select_where(context,
                                                     edgeql_tree.where)

        if edgeql_tree.targets:
            with context():
                context.current.location = 'optarget_shaper'
                graph.opselector = self._process_select_targets(
                    context, edgeql_tree.targets)
        else:
            rec = self.entityref_to_record(tgt, self.schema)
            graph.opselector = [
                irast.SelectorExpr(
                    expr=rec,
                    name=context.current.genalias('o')
                )
            ]

        context.current.location = 'top'

        paths = [s.expr for s in graph.opselector] + \
                [s.expr for s in graph.selector] + [graph.optarget]

        if graph.generator:
            paths.append(graph.generator)

        union = irast.Disjunction(paths=frozenset(paths))
        pathmerger.flatten_and_unify_path_combination(
            context, union, deep=True, merge_filters=True)

        # Reorder aggregate expressions so that all of them appear as the
        # first sub-tree in the generator expression.
        #
        if graph.generator:
            self.reorder_aggregates(graph.generator)

        graph.result_types = self.get_selector_types(graph.opselector,
                                                     self.schema)
        graph.argument_types = self.context.current.arguments
        graph.context_vars = self.context.current.context_vars

        self.link_subqueries(graph)

        return graph

    def _process_select_where(self, context, where):
        with context():
            context.current.location = 'generator'

            if where:
                expr = self._process_expr(context, where)
                expr = self._merge_paths(context, expr)
                self.postprocess_expr(expr)
                return expr
            else:
                return None

    def _process_expr(self, context, expr):
        node = None

        if isinstance(expr, qlast.SelectQueryNode):
            node = context.current.subgraphs_map.get(expr)

            if node is None:
                with self.context(ParseContext.SUBQUERY):
                    node = self._transform_select(context, expr,
                                                  self.arg_types)

                if len(node.selector) > 1:
                    err = ('subquery must return only one column')
                    raise errors.EdgeQLError(err)

                node.referrers.append(context.current.location)
                context.current.graph.subgraphs.add(node)
                context.current.subgraphs_map[expr] = node

            refname = node.selector[0].name or node.selector[0].autoname
            node.attrrefs.add(refname)
            node = irast.SubgraphRef(ref=node, name=refname)

        elif isinstance(expr, qlast.BinOpNode):
            left = self._process_expr(context, expr.left)

            if isinstance(expr.op, ast.ops.TypeCheckOperator):
                right = self.process_type_ref_expr(context, expr.right)
                if isinstance(left, irast.Record):
                    left = left.elements[0].ref
            else:
                right = self._process_expr(context, expr.right)

            node = self.process_binop(left, right, expr.op)

        elif isinstance(expr, qlast.PathNode):
            node = self._process_path(context, expr)

            if (context.current.groupprefixes
                    and context.current.location in ('sorter', 'selector')
                    and not context.current.in_aggregate):
                for p in node.paths:
                    if p.id not in context.current.groupprefixes:
                        err = ('node reference "%s" must appear in the '
                               'GROUP BY expression or '
                               'used in an aggregate function ') % p.id
                        raise errors.EdgeQLError(err)

            if ((context.current.location not in
                 {'generator', 'selector', 'opvalues'} and
                 not context.current.in_func_call) or
                    context.current.in_aggregate):
                if isinstance(node, irast.EntitySet):
                    node = self.entityref_to_record(node, self.schema)

        elif isinstance(expr, qlast.ConstantNode):
            node = self._process_constant(context, expr)

        elif isinstance(expr, qlast.SequenceNode):
            elements = [self._process_expr(context, e) for e in expr.elements]
            node = irast.Sequence(elements=elements)

            # Squash the sequence if it comes from IS (type,...), since
            # we unconditionally transform ClassRefNodes into list-type
            # constants below.
            #
            squash_homogeneous = expr.elements and \
                isinstance(expr.elements[0], qlast.ClassRefNode)
            node = self.process_sequence(
                node, squash_homogeneous=squash_homogeneous)

        elif isinstance(expr, qlast.ArrayNode):
            elements = [self._process_expr(context, e) for e in expr.elements]
            node = irast.Sequence(elements=elements, is_array=True)

        elif isinstance(expr, qlast.FunctionCallNode):
            with context():
                if self._is_func_agg(expr.func):
                    context.current.in_aggregate.append(expr)
                context.current.in_func_call = True

                args = []
                kwargs = {}

                for a in expr.args:
                    if isinstance(a, qlast.NamedArgNode):
                        kwargs[a.name] = self._process_expr(context, a.arg)
                    else:
                        args.append(self._process_expr(context, a))

                node = irast.FunctionCall(
                    name=expr.func, args=args, kwargs=kwargs)

                if expr.agg_sort:
                    node.agg_sort = [
                        irast.SortExpr(
                            expr=self._process_expr(context, e.path),
                            direction=e.direction) for e in expr.agg_sort
                    ]

                elif expr.window:
                    if expr.window.orderby:
                        node.agg_sort = [
                            irast.SortExpr(
                                expr=self._process_expr(context, e.path),
                                direction=e.direction)
                            for e in expr.window.orderby
                        ]

                    if expr.window.partition:
                        for partition_expr in expr.window.partition:
                            partition_expr = self._process_expr(context,
                                                                partition_expr)
                            node.partition.append(partition_expr)

                    node.window = True

                if expr.agg_filter:
                    node.agg_filter = self._process_expr(context,
                                                         expr.agg_filter)

                node = self.process_function_call(node)

        elif isinstance(expr, qlast.ClassRefNode):
            if expr.module:
                name = sn.Name(name=expr.name, module=expr.module)
            else:
                name = expr.name
            node = self.schema.get(
                name=name,
                module_aliases=context.current.namespaces,
                type=s_obj.NodeClass)

            node = irast.Constant(value=(node, ), type=(list, node.__class__))

        elif isinstance(expr, qlast.UnaryOpNode):
            if (expr.op == ast.ops.NOT and
                    isinstance(expr.operand, qlast.NoneTestNode)):
                # Make sure NOT(IS NULL) does not produce weak paths, as IS
                # NULL would normally do.
                self.context.current.weak_path = False

            operand = self._process_expr(context, expr.operand)
            node = self.process_unaryop(operand, expr.op)

        elif isinstance(expr, qlast.NoneTestNode):
            with context():
                if self.context.current.weak_path is None:
                    self.context.current.weak_path = True
                expr = self._process_expr(context, expr.expr)
                nt = irast.NoneTest(expr=expr)
                node = self.process_none_test(nt, context.current.schema)

        elif isinstance(expr, qlast.ExistsPredicateNode):
            subquery = self._process_expr(context, expr.expr)
            if (isinstance(subquery, irast.EntitySet) and
                    isinstance(subquery.concept, s_concepts.Concept)):
                _id = sn.Name('std::id')
                schema = context.current.schema
                aref = irast.AtomicRefSimple(
                    name=_id,
                    ref=subquery,
                    ptr_class=subquery.concept.resolve_pointer(
                        schema, _id)
                )
                subquery.atomrefs.add(aref)
                subquery = irast.SubgraphRef(
                    ref=irast.GraphExpr(
                        selector=[
                            irast.SelectorExpr(
                                expr=aref
                            )
                        ],
                        generator=irast.UnaryOp(
                            op=ast.ops.NOT,
                            expr=irast.NoneTest(
                                expr=aref
                            )
                        )
                    )
                )

                subquery.ref.referrers.append('exists')
                node = irast.ExistPred(expr=subquery)

            elif isinstance(subquery, irast.BaseRef):
                node = irast.UnaryOp(
                    op=ast.ops.NOT,
                    expr=irast.NoneTest(expr=subquery)
                )

            else:
                if isinstance(subquery, irast.SubgraphRef):
                    subquery.ref.referrers.append('exists')
                node = irast.ExistPred(expr=subquery)

        elif isinstance(expr, qlast.TypeCastNode):
            maintype = expr.type.maintype
            subtypes = expr.type.subtypes

            schema = context.current.schema

            if subtypes:
                typ = irast.TypeRef(
                    maintype=maintype.name,
                    subtypes=[]
                )

                for subtype in subtypes:
                    if isinstance(subtype, qlast.PathNode):
                        stype = self._process_path(context, subtype)
                        if isinstance(stype, irast.LinkPropRefSimple):
                            stype = stype.ref
                        elif not isinstance(stype, irast.EntityLink):
                            stype = stype.rlink

                        if subtype.pathspec:
                            pathspec = self._process_pathspec(
                                context, stype.link_class, None,
                                subtype.pathspec)
                        else:
                            pathspec = None

                        subtype = irast.CompositeType(
                            node=stype, pathspec=pathspec)
                    else:
                        subtype = self._get_schema_object(
                            context, subtype.name, subtype.module)

                    typ.subtypes.append(subtype.name)
            else:
                typ = irast.TypeRef(
                    maintype=self._get_schema_object(
                        context, maintype.name, maintype.module).name,
                    subtypes=[]
                )

            node = irast.TypeCast(
                expr=self._process_expr(context, expr.expr), type=typ)

        elif isinstance(expr, qlast.IndirectionNode):
            node = self._process_expr(context, expr.arg)
            for indirection_el in expr.indirection:
                if isinstance(indirection_el, qlast.IndexNode):
                    idx = self._process_expr(context, indirection_el.index)
                    node = irast.IndexIndirection(expr=node, index=idx)

                elif isinstance(indirection_el, qlast.SliceNode):
                    if indirection_el.start:
                        start = self._process_expr(context,
                                                   indirection_el.start)
                    else:
                        start = irast.Constant(value=None)

                    if indirection_el.stop:
                        stop = self._process_expr(context, indirection_el.stop)
                    else:
                        stop = irast.Constant(value=None)

                    node = irast.SliceIndirection(
                        expr=node, start=start, stop=stop)
                else:
                    raise ValueError('unexpected indirection node: '
                                     '{!r}'.format(indirection_el))

        else:
            assert False, "Unexpected expr: %s" % expr

        return node

    def _process_constant(self, context, expr):
        if expr.index is not None:
            type = self.arg_types.get(expr.index)
            if type is not None:
                type = s_types.normalize_type(type, self.schema)
            node = irast.Constant(
                value=expr.value, index=expr.index, type=type)
            context.current.arguments[expr.index] = type
        else:
            type = s_types.normalize_type(expr.value.__class__,
                                          self.schema)
            node = irast.Constant(
                value=expr.value, index=expr.index, type=type)

        return node

    def _process_unlimited_recursion(self):
        type = s_types.normalize_type((0).__class__,
                                      self.schema)
        return irast.Constant(
            value=0, index=None, type=type)

    def _process_pathspec(self, context, source, rlink_class, pathspec):
        result = []

        for ptrspec in pathspec:
            if isinstance(ptrspec, qlast.PointerGlobNode):
                filter_exprs = []
                glob_specs = []

                if ptrspec.filters:
                    for ptrspec_flt in ptrspec.filters:
                        if ptrspec_flt.any:
                            continue

                        if ptrspec_flt.property == 'loading':
                            value = s_pointers.PointerLoading(
                                ptrspec_flt.value)
                            ptrspec_flt = \
                                (lambda p: p.get_loading_behaviour(), value)
                        else:
                            msg = 'invalid pointer property in pointer glob: '\
                                  '{!r}'.format(ptrspec_flt.property)
                            raise errors.EdgeQLError(msg)
                        filter_exprs.append(ptrspec_flt)

                if ptrspec.type == 'link':
                    for ptr in source.pointers.values():
                        if not filter_exprs or all((f[0](ptr) == f[1])
                                                   for f in filter_exprs):
                            glob_specs.append(irast.PtrPathSpec(ptr_class=ptr))

                elif ptrspec.type == 'property':
                    if rlink_class is None:
                        msg = 'link properties are not available at this ' \
                              'point in path'
                        raise errors.EdgeQLError(msg)

                    for ptr_name, ptr in rlink_class.pointers.items():
                        if ptr.is_special_pointer():
                            continue

                        if not filter_exprs or all((f[0](ptr) == f[1])
                                                   for f in filter_exprs):
                            glob_specs.append(irast.PtrPathSpec(ptr_class=ptr))

                else:
                    msg = 'unexpected pointer spec type'
                    raise errors.EdgeQLError(msg)

                result = self._merge_pathspecs(
                    result, glob_specs, target_most_generic=False)

            else:
                steps = ptrspec.expr.steps
                ptrsource = source

                if len(steps) == 2:
                    ptrsource = self._get_schema_object(
                        context, steps[0].expr, steps[0].namespace)
                    lexpr = steps[1].expr
                elif len(steps) == 1:
                    lexpr = steps[0].expr

                ptrname = (lexpr.namespace, lexpr.name)

                if lexpr.type == 'property':
                    if rlink_class is None:
                        if isinstance(source, s_links.Link):
                            ptrsource = source
                        else:
                            msg = 'link properties are not available at ' \
                                  'this point in path'
                            raise errors.EdgeQLError(msg)
                    else:
                        ptrsource = rlink_class

                    ptrtype = s_lprops.LinkProperty
                else:
                    ptrtype = s_links.Link

                if lexpr.target is not None:
                    target_name = (lexpr.target.module, lexpr.target.name)
                else:
                    target_name = None

                ptr_direction = \
                    lexpr.direction or s_pointers.PointerDirection.Outbound

                if ptrspec.compexpr is not None:
                    schema = context.current.schema
                    compexpr = self._process_expr(context, ptrspec.compexpr)
                    if not isinstance(
                            compexpr, (irast.GraphExpr, irast.SubgraphRef)):
                        compexpr = irast.GraphExpr(
                            selector=[irast.SelectorExpr(
                                expr=compexpr
                            )]
                        )

                    target_class = irutils.infer_type(compexpr, schema)
                    assert target_class is not None
                    ptr = s_links.Link(
                        name=sn.SchemaName(
                            module=ptrname[0] or ptrsource.name.module,
                            name=ptrname[1]),
                    ).derive(schema, ptrsource, target_class)

                    if ptr.shortname == 'std::__class__':
                        msg = 'cannot assign to __class__'
                        raise errors.EdgeQLError(msg, context=lexpr.context)
                else:
                    ptr = self._resolve_ptr(
                        context,
                        ptrsource,
                        ptrname,
                        ptr_direction,
                        ptr_type=ptrtype,
                        target=target_name)
                    target_class = ptr.get_far_endpoint(ptr_direction)
                    compexpr = None

                if ptrspec.recurse:
                    if ptrspec.recurse_limit is not None:
                        recurse = self._process_constant(
                            context, ptrspec.recurse_limit)
                    else:
                        # XXX - temp hack
                        recurse = self._process_unlimited_recursion()
                else:
                    recurse = None

                if ptrspec.where:
                    generator = self._process_select_where(context,
                                                           ptrspec.where)
                else:
                    generator = None

                if ptrspec.orderby:
                    sorter = self._process_sorter(context, ptrspec.orderby)
                else:
                    sorter = None

                if ptrspec.offset is not None:
                    offset = self._process_expr(context, ptrspec.offset)
                else:
                    offset = None

                if ptrspec.limit is not None:
                    limit = self._process_expr(context, ptrspec.limit)
                else:
                    limit = None

                node = irast.PtrPathSpec(
                    ptr_class=ptr,
                    ptr_direction=ptr_direction,
                    compexpr=compexpr,
                    recurse=recurse,
                    target_class=target_class,
                    generator=generator,
                    sorter=sorter,
                    trigger=irast.ExplicitPathSpecTrigger(),
                    offset=offset,
                    limit=limit)

                if ptrspec.pathspec is not None:
                    node.pathspec = self._process_pathspec(
                        context, target_class, ptr, ptrspec.pathspec)
                else:
                    node.pathspec = None

                result = self._merge_pathspecs(
                    result, [node], target_most_generic=False)

        self._normalize_pathspec_recursion(result, source,
                                           context.current.schema)

        return result

    def _process_path(self, context, path, path_tip=None):
        pathvars = context.current.pathvars
        anchors = context.current.anchors

        for i, node in enumerate(path.steps):
            if isinstance(node, qlast.TypeInterpretationNode):
                path_tip = self._process_expr(context, node.expr)
                path_tip.as_type = self._get_schema_object(
                    context, node.type.maintype)
                continue

            elif isinstance(node, (qlast.PathNode, qlast.PathStepNode)):
                if isinstance(node, qlast.PathNode):
                    if len(node.steps) > 1:
                        raise errors.EdgeQLError(
                            'unsupported subpath expression')

                    tip = self._get_path_tip(node)

                elif isinstance(node, qlast.PathStepNode):
                    if i == 0:
                        refnode = None

                        try:
                            refnode = anchors[node.expr]
                        except KeyError:
                            try:
                                refnode = pathvars[node.expr]
                            except KeyError:
                                try:
                                    fq_name = '{}::{}'.format(node.namespace,
                                                              node.expr)
                                    refnode = pathvars[fq_name]
                                except KeyError:
                                    pass

                        if refnode:
                            if isinstance(refnode, irast.Path):
                                path_copy = irutils.copy_path(refnode)
                                path_copy.users.add(context.current.location)
                                path_tip = path_copy

                            else:
                                path_tip = refnode

                            continue

                        elif node.expr in context.current.cge_map:
                            cge = context.current.cge_map[node.expr]
                            path_tip = cge
                            continue

                    tip = node

                else:
                    raise errors.EdgeQLError(
                        'unexpected path node: {!r}'.format(node))

            else:
                tip = node

            tip_is_link = path_tip and isinstance(path_tip, irast.EntityLink)
            tip_is_cge = path_tip and isinstance(path_tip, irast.GraphExpr)

            if (path_tip is None and
                    context.current.local_link_source is not None and
                    isinstance(tip, qlast.PathStepNode)):
                scls = self._get_schema_object(context, tip.expr,
                                               tip.namespace)
                if isinstance(scls, s_links.Link):
                    tip = qlast.LinkExprNode(expr=qlast.LinkNode(
                        namespace=tip.namespace, name=tip.expr))
                    path_tip = context.current.local_link_source

            if isinstance(tip, qlast.PathStepNode):

                scls = self._get_schema_object(context, tip.expr,
                                               tip.namespace)

                if isinstance(scls, s_obj.NodeClass):
                    step = irast.EntitySet()
                    step.concept = scls
                    step.id = irutils.LinearPath([step.concept])
                    step.users.add(context.current.location)
                else:
                    step = irast.EntityLink(link_class=scls)

                path_tip = step

            elif isinstance(tip, qlast.LinkExprNode) and tip_is_cge:
                path_tip = irast.SubgraphRef(ref=path_tip, name=tip.expr.name)

            elif isinstance(tip, qlast.LinkExprNode) and not tip_is_link:
                # LinkExprNode
                link_expr = tip.expr
                link_target = None

                direction = (link_expr.direction or
                             s_pointers.PointerDirection.Outbound)
                if link_expr.target:
                    link_target = self._get_schema_object(
                        context, link_expr.target.name,
                        link_expr.target.module)

                linkname = (link_expr.namespace, link_expr.name)

                link_class = self._resolve_ptr(
                    context,
                    path_tip.as_type or path_tip.concept,
                    linkname,
                    direction,
                    target=link_target)

                path_tip = irutils.extend_path(
                    path_tip, link_class, direction=direction,
                    location=context.current.location,
                    weak_path=context.current.weak_path)

            elif isinstance(tip, qlast.LinkPropExprNode) or tip_is_link:
                # LinkExprNode
                link_expr = tip.expr

                if path_tip and isinstance(path_tip, irast.GraphExpr):
                    subgraph = path_tip
                    subgraph.attrrefs.add(link_expr.name)
                    sgref = irast.SubgraphRef(
                        ref=subgraph, name=link_expr.name)
                    path_tip = sgref
                else:
                    prop_name = (link_expr.namespace, link_expr.name)

                    if (isinstance(path_tip, irast.LinkPropRefSimple) and
                            not path_tip.ptr_class.is_endpoint_pointer()):
                        # We are at propref point, the only valid
                        # step from here is @source taking us back
                        # to the link context.
                        if link_expr.name != 'source':
                            msg = 'invalid reference: {}.{}'.format(
                                path_tip.ptr_class, link_expr.name)
                            raise errors.EdgeQLReferenceError(msg)

                        link = path_tip.ref
                        target = link.target
                        path_tip = target

                    else:
                        if isinstance(path_tip, irast.LinkPropRefSimple):
                            link = path_tip.ref
                            link_class = link.link_class
                            id = path_tip.id
                        elif isinstance(path_tip, irast.EntityLink):
                            link = path_tip
                            link_class = link.link_class
                            id = irutils.LinearPath([None])
                            id.add(link_class,
                                   s_pointers.PointerDirection.Outbound, None)
                        else:
                            link = path_tip.rlink
                            link_class = link.link_class
                            id = path_tip.id

                        prop_class = self._resolve_ptr(
                            context,
                            link_class,
                            prop_name,
                            s_pointers.PointerDirection.Outbound,
                            ptr_type=s_lprops.LinkProperty)

                        propref = irast.LinkPropRefSimple(
                            name=prop_class.shortname,
                            ref=link,
                            id=id,
                            ptr_class=prop_class)
                        link.proprefs.add(propref)

                        path_tip = propref

            else:
                assert False, 'Unexpected path step expression: "%s"' % tip

        if (isinstance(path_tip, irast.EntityLink) and
                path_tip.source is not None):
            # Dangling link reference, possibly from an anchor ref,
            # complement it to target ref
            link_class = path_tip.link_class

            pref_id = irutils.LinearPath(path_tip.source.id)
            pref_id.add(link_class, s_pointers.PointerDirection.Outbound,
                        link_class.target)
            ptr_class = link_class.pointers['std::target']
            ptr_name = ptr_class.shortname
            propref = irast.LinkPropRefSimple(
                name=ptr_name, ref=path_tip, id=pref_id, ptr_class=ptr_class)
            propref.anchor = path_tip.anchor
            propref.show_as_anchor = path_tip.show_as_anchor
            propref.pathvar = path_tip.pathvar
            path_tip.proprefs.add(propref)

            path_tip = propref

        return path_tip

    def _resolve_ptr(self,
                     context,
                     near_endpoint,
                     ptr_name,
                     direction,
                     ptr_type=s_links.Link,
                     target=None):
        ptr_module, ptr_nqname = ptr_name

        if ptr_module:
            ptr_fqname = sn.Name(module=ptr_module, name=ptr_nqname)
            modaliases = context.current.namespaces
            pointer = self.schema.get(ptr_fqname,
                                      module_aliases=modaliases,
                                      type=ptr_type)
            pointer_name = pointer.name
        else:
            pointer_name = ptr_fqname = ptr_nqname

        if target is not None and not isinstance(target, s_obj.NodeClass):
            target_name = '.'.join(filter(None, target))
            modaliases = context.current.namespaces
            target = self.schema.get(target_name,
                                     module_aliases=modaliases)

        if ptr_nqname == '%':
            pointer_name = self.schema.get_root_class(ptr_type).name

        if target is not None:
            far_endpoints = (target, )
        else:
            far_endpoints = None

        ptr = near_endpoint.resolve_pointer(
            self.schema,
            pointer_name,
            direction=direction,
            look_in_children=False,
            include_inherited=True,
            far_endpoints=far_endpoints)

        if not ptr:
            msg = ('[{near_endpoint}].[{direction}{ptr_name}{far_endpoint}] '
                   'does not resolve to any known path')
            far_endpoint_str = '({})'.format(target.name) if target else ''
            msg = msg.format(
                near_endpoint=near_endpoint.name,
                direction=direction,
                ptr_name=pointer_name,
                far_endpoint=far_endpoint_str)
            raise errors.EdgeQLReferenceError(msg)

        return ptr

    def _get_schema_object(self, context, name, module=None):
        if isinstance(name, qlast.ClassRefNode):
            module = name.module
            name = name.name

        if module:
            name = sn.Name(name=name, module=module)

        return self.schema.get(
            name=name, module_aliases=context.current.namespaces)

    def _process_select_targets(self, context, targets):
        selector = list()

        with context():
            context.current.location = 'selector'
            for target in targets:
                expr = self._process_expr(context, target.expr)
                expr = self._merge_paths(context, expr)
                params = {'autoname': context.current.genalias()}

                if isinstance(expr, irast.Disjunction):
                    path = next(iter(expr.paths))
                else:
                    path = expr

                if isinstance(path, irast.EntitySet):
                    if target.expr.pathspec is not None:
                        if path.rlink is not None:
                            rlink_class = path.rlink.link_class
                        else:
                            rlink_class = None
                        pathspec = self._process_pathspec(
                            context, path.concept, rlink_class,
                            target.expr.pathspec)
                    else:
                        pathspec = None

                    if not isinstance(path.concept, s_atoms.Atom):
                        expr = self.entityref_to_record(
                            expr,
                            self.schema,
                            pathspec=pathspec)

                t = irast.SelectorExpr(expr=expr, **params)
                selector.append(t)

        return selector

    def _get_path_tip(self, path):
        if len(path.steps) == 0:
            return None

        last = path.steps[-1]

        if isinstance(last, qlast.PathNode):
            return self._get_path_tip(last)
        else:
            return last

    def _process_sorter(self, context, sorters):

        result = []

        if sorters:
            with context():
                context.current.location = 'sorter'
                for sorter in sorters:
                    expr = self._process_expr(context, sorter.path)
                    expr = self._merge_paths(context, expr)
                    if isinstance(expr, irast.PathCombination):
                        assert len(expr.paths) == 1
                        expr = next(iter(expr.paths))
                    s = irast.SortExpr(
                        expr=expr,
                        direction=sorter.direction,
                        nones_order=sorter.nones_order)
                    result.append(s)

        return result

    def _process_grouper(self, context, groupers):

        result = []

        if groupers:
            with context():
                context.current.location = 'grouper'
                for grouper in groupers:
                    expr = self._process_expr(context, grouper)
                    expr = self._merge_paths(context, expr)
                    result.append(expr)

        return result

    def add_path_user(self, path, user):
        while path:
            path.users.add(user)

            if isinstance(path, irast.EntitySet):
                rlink = path.rlink
            else:
                rlink = path

            if rlink:
                rlink.users.add(user)
                path = rlink.source
            else:
                path = None
        return path

    def entityref_to_record(self,
                            expr,
                            schema,
                            *,
                            pathspec=None,
                            prefixes=None,
                            _visited_records=None,
                            _recurse=True,
                            _include_implicit=True):
        """Convert an EntitySet node into an Record."""
        if not isinstance(expr, irast.PathCombination):
            expr = irast.Conjunction(paths=frozenset((expr, )))

        p = next(iter(expr.paths))

        if not isinstance(p, irast.EntitySet):
            return expr

        if _visited_records is None:
            _visited_records = {}

        recurse_links = None

        concepts = {c.concept for c in expr.paths}
        assert len(concepts) == 1

        elements = []
        atomrefs = []

        concept = p.concept
        ref = p if len(expr.paths) == 1 else expr
        rec = irast.Record(elements=elements, concept=concept, rlink=p.rlink)

        _new_visited_records = _visited_records.copy()

        if isinstance(concept, s_concepts.Concept):
            _new_visited_records[concept] = rec

        if concept.is_virtual:
            ptrs = concept.get_children_common_pointers(schema)
            ptrs = {ptr.shortname: ptr for ptr in ptrs}
            ptrs.update(concept.pointers)
        else:
            ptrs = concept.pointers

        if _include_implicit:
            implicit_links = (sn.Name('std::id'),)

            recurse_links = {(l, s_pointers.PointerDirection.Outbound):
                             irast.PtrPathSpec(ptr_class=ptrs[l])
                             for l in implicit_links}
        else:
            recurse_links = {}

        if pathspec is not None:
            for ps in pathspec:
                if isinstance(ps.ptr_class, s_links.Link):
                    k = ps.ptr_class.shortname, ps.ptr_direction
                    recurse_links[k] = ps

        for (link_name, link_direction), recurse_spec in recurse_links.items():
            el = None

            link = recurse_spec.ptr_class
            link_direction = \
                recurse_spec.ptr_direction \
                or s_pointers.PointerDirection.Outbound

            root_link_class = link.bases[0]
            link_class = link

            if link_direction == s_pointers.PointerDirection.Outbound:
                link_target_class = link.target
                link_singular = \
                    link.mapping in {s_links.LinkMapping.OneToOne,
                                     s_links.LinkMapping.ManyToOne}
            else:
                link_target_class = link.source
                link_singular = \
                    link.mapping in {s_links.LinkMapping.OneToOne,
                                     s_links.LinkMapping.OneToMany}

            if recurse_spec.target_class is not None:
                target_class = recurse_spec.target_class
            else:
                target_class = link_target_class

            recurse_link = \
                recurse_spec.recurse if recurse_spec is not None else None

            full_path_id = irutils.LinearPath(ref.id)
            full_path_id.add(link_class, link_direction, target_class)

            if not link_singular or recurse_link is not None:
                lref = irutils.copy_path(ref, connect_to_origin=True)
                lref.reference = ref
            else:
                lref = ref

            if recurse_link is not None:
                lref.rlink = None

            if prefixes and full_path_id in prefixes and lref is ref:
                targetstep = prefixes[full_path_id]
                targetstep = next(iter(targetstep))
                link_node = targetstep.rlink
                reusing_target = True
            else:
                targetstep = irast.EntitySet(
                    conjunction=irast.Conjunction(),
                    disjunction=irast.Disjunction(),
                    users={self.context.current.location},
                    concept=target_class,
                    id=full_path_id)

                link_node = irast.EntityLink(
                    source=lref,
                    target=targetstep,
                    link_class=link_class,
                    direction=link_direction,
                    users={'selector'})

                targetstep.rlink = link_node
                reusing_target = False

            if recurse_spec.trigger is not None:
                link_node.pathspec_trigger = recurse_spec.trigger

            if recurse_spec.compexpr is not None:
                if not isinstance(recurse_spec.compexpr, irast.SubgraphRef):
                    if not isinstance(recurse_spec.compexpr, irast.GraphExpr):
                        compexpr = irast.GraphExpr(
                            selector=[irast.SelectorExpr(
                                expr=recurse_spec.compexpr
                            )]
                        )
                        force_inline = True
                        refname = link_name
                    else:
                        compexpr = recurse_spec.compexpr
                        force_inline = False
                        refname = (
                            compexpr.selector[0].name
                            or compexpr.selector[0].autoname
                        )

                    el = irast.SubgraphRef(
                        ref=compexpr, name=refname, rlink=link_node,
                        force_inline=force_inline)
                    elements.append(el)
                else:
                    recurse_spec.compexpr.rlink = link_node
                    elements.append(recurse_spec.compexpr)

                continue

            if not link.atomic():
                lref.conjunction.update(link_node)

            filter_generator = None
            sorter = []

            newstep = targetstep

            if recurse_link is not None:
                newstep = lref

            if recurse_spec.generator is not None:
                filter_generator = recurse_spec.generator

            if recurse_spec.sorter:
                sort_source = newstep

                for sortexpr in recurse_spec.sorter:
                    sortpath = sortexpr.expr

                    sort_target = irutils.copy_path(sortpath)

                    if isinstance(sortpath, irast.LinkPropRef):
                        if (sortpath.ref.link_class.shortname ==
                                link_node.link_class.shortname):
                            sort_target.ref = link_node
                            link_node.proprefs.add(sort_target)
                        else:
                            raise ValueError('Cannot sort by property ref of '
                                             'link other than self')
                    else:
                        sortpath_link = sortpath.rlink
                        sort_target.ref = sort_source

                        sort_link = irast.EntityLink(
                            source=sort_source,
                            target=sort_target,
                            link_class=sortpath_link.link_class,
                            direction=sortpath_link.direction,
                            users=sortpath_link.users.copy())

                        sort_target.rlink = sort_link
                        sort_source.atomrefs.add(sort_target)

                    sorter.append(
                        irast.SortExpr(
                            expr=sort_target,
                            direction=sortexpr.direction,
                            nones_order=sortexpr.nones_order))

            if isinstance(target_class, s_atoms.Atom):
                if link_singular:
                    if not reusing_target:
                        newstep = irast.AtomicRefSimple(
                            ref=lref,
                            name=link_name,
                            id=full_path_id,
                            ptr_class=link_class,
                            rlink=link_node,
                            users=link_node.users.copy())
                        link_node.target = newstep
                        atomrefs.append(newstep)
                else:
                    ptr_name = sn.Name('std::target')
                    prop_id = irutils.LinearPath(ref.id)
                    prop_id.add(root_link_class,
                                s_pointers.PointerDirection.Outbound, None)
                    prop_class = link.pointers[ptr_name]
                    newstep = irast.LinkPropRefSimple(
                        name=ptr_name, id=full_path_id, ptr_class=prop_class)

                el = newstep
            else:
                if _recurse:
                    _memo = _new_visited_records

                    if (recurse_spec is not None and
                            recurse_spec.recurse is not None):
                        _memo = {}
                        new_recurse = True
                    elif isinstance(recurse_spec.trigger,
                                    irast.ExplicitPathSpecTrigger):
                        new_recurse = True
                    elif newstep.concept not in _visited_records:
                        new_recurse = True
                    else:
                        new_recurse = False

                    recurse_pathspec = \
                        recurse_spec.pathspec \
                        if recurse_spec is not None else None
                    el = self.entityref_to_record(
                        newstep,
                        schema,
                        pathspec=recurse_pathspec,
                        _visited_records=_memo,
                        _recurse=new_recurse)

            prop_elements = []
            if link.has_user_defined_properties():
                if recurse_spec.pathspec is not None:
                    recurse_props = {}

                    for ps in recurse_spec.pathspec:
                        if (isinstance(ps.ptr_class, s_lprops.LinkProperty) and
                                not ps.ptr_class.is_endpoint_pointer()):
                            recurse_props[ps.ptr_class.shortname] = ps
                else:
                    recurse_props = {}

                for prop_name, prop_class in link.pointers.items():
                    if prop_name not in recurse_props:
                        continue

                    prop_id = irutils.LinearPath(ref.id)
                    prop_id.add(root_link_class,
                                s_pointers.PointerDirection.Outbound, None)
                    prop_ref = irast.LinkPropRefSimple(
                        name=prop_name,
                        id=full_path_id,
                        ptr_class=prop_class,
                        ref=link_node)
                    prop_elements.append(prop_ref)
                    link_node.proprefs.add(prop_ref)

                if prop_elements:
                    if not isinstance(el, irast.Record):
                        std_tgt = link_class.pointers['std::target']
                        std_tgt_ref = irast.LinkPropRefSimple(
                            name=std_tgt.shortname,
                            id=full_path_id,
                            ptr_class=std_tgt,
                            ref=link_node)
                        link_node.proprefs.add(std_tgt_ref)
                        el = irast.Record(elements=[std_tgt_ref],
                                          concept=target_class,
                                          rlink=link_node)

                    el.elements.extend(prop_elements)

            if not link.atomic() or prop_elements:
                lref.conjunction.update(link_node)

            if isinstance(newstep, irast.LinkPropRefSimple):
                newstep.ref = link_node
                lref.disjunction.update(link_node)

            if (not link_singular or
                    recurse_spec.recurse is not None) and el is not None:
                if link.atomic():
                    link_node.proprefs.add(newstep)

                generator = irast.Conjunction(paths=frozenset((targetstep, )))

                if filter_generator is not None:
                    ref_gen = irast.Conjunction(paths=frozenset(
                        (targetstep, )))
                    generator.paths = frozenset(generator.paths |
                                                {filter_generator})
                    generator = pathmerger.PathMerger.run(
                        generator, context=self.context)

                    # merge_paths fails to update all refs, because
                    # some nodes that need to be updated are behind
                    # non-traversable rlink attribute.  Do the
                    # proper update manually.  Also, make sure
                    # aggregate_result below gets a correct ref.
                    #
                    new_targetstep = next(iter(ref_gen.paths))
                    if new_targetstep != targetstep:
                        el.replace_refs(
                            [targetstep], new_targetstep, deep=True)

                        for elem in el.elements:
                            try:
                                rlink = elem.rlink
                            except AttributeError:
                                pass
                            else:
                                if rlink:
                                    rlink.replace_refs(
                                        [targetstep],
                                        new_targetstep,
                                        deep=True)

                        targetstep = new_targetstep

                subgraph = irast.GraphExpr()
                subgraph.generator = generator
                subgraph.aggregate_result = irast.FunctionCall(
                    name=('agg', 'list'), aggregates=True, args=[targetstep])

                if sorter:
                    subgraph.sorter = sorter

                subgraph.offset = recurse_spec.offset
                subgraph.limit = recurse_spec.limit

                if recurse_spec.recurse is not None:
                    subgraph.recurse_link = link_node
                    subgraph.recurse_depth = recurse_spec.recurse

                selexpr = irast.SelectorExpr(expr=el, name=link_name)

                subgraph.selector.append(selexpr)
                el = irast.SubgraphRef(
                    ref=subgraph, name=link_name, rlink=link_node)

            # Record element may be none if link target is non-atomic
            # and recursion has been prohibited on this level to prevent
            # infinite looping.
            if el is not None:
                elements.append(el)

        for p in expr.paths:
            p.atomrefs.update(atomrefs)

        expr = rec
        return expr

    def entityref_to_idref(self, expr, schema):
        p = next(iter(expr.paths))
        if isinstance(p, irast.EntitySet):
            concepts = {c.concept for c in expr.paths}
            assert len(concepts) == 1

            ref = p if len(expr.paths) == 1 else expr
            link_name = sn.Name('std::id')
            link_class = schema.get(link_name)
            target_class = link_class.target
            id = irutils.LinearPath(ref.id)
            id.add(link_class, s_pointers.PointerDirection.Outbound,
                   target_class)
            expr = irast.AtomicRefSimple(
                ref=ref, name=link_name, id=id, ptr_class=link_class)

        return expr

    def _normalize_pathspec_recursion(self, pathspec, source, schema):
        for ptrspec in pathspec:
            if ptrspec.recurse is not None:
                # Push the looping spec one step down so that the
                # loop starts on the first  pointer target, not the source.
                #

                if ptrspec.pathspec is None:
                    ptrspec.pathspec = []

                    for ptr in ptrspec.target_class.pointers.values():
                        if ptr.get_loading_behaviour(
                        ) == s_pointers.PointerLoading.Eager:
                            ptrspec.pathspec.append(
                                irast.PtrPathSpec(ptr_class=ptr))

                recspec = irast.PtrPathSpec(
                    ptr_class=ptrspec.ptr_class,
                    ptr_direction=ptrspec.ptr_direction,
                    recurse=ptrspec.recurse,
                    target_class=ptrspec.target_class,
                    generator=ptrspec.generator,
                    sorter=ptrspec.sorter,
                    limit=ptrspec.limit,
                    offset=ptrspec.offset,
                    pathspec=ptrspec.pathspec[:])

                for i, subptrspec in enumerate(ptrspec.pathspec):
                    if (subptrspec.ptr_class.shortname ==
                            ptrspec.ptr_class.shortname):
                        ptrspec.pathspec[i] = recspec
                        break
                else:
                    ptrspec.pathspec.append(recspec)

                ptrspec.recurse = None
                ptrspec.sorter = []

    def _merge_pathspecs(self, pathspec1, pathspec2, target_most_generic=True):
        merged_right = set()

        if not pathspec1:
            return pathspec2

        if not pathspec2:
            return pathspec1

        result = []

        for item1 in pathspec1:
            item1_subspec = item1.pathspec

            item1_lname = item1.ptr_class.shortname
            item1_ldir = item1.ptr_direction

            for i, item2 in enumerate(pathspec2):
                if i in merged_right:
                    continue

                item2_subspec = item2.pathspec

                item2_lname = item2.ptr_class.shortname
                item2_ldir = item2.ptr_direction

                if item1_lname == item2_lname and item1_ldir == item2_ldir:
                    # Do merge

                    if item1.target_class != item2.target_class:
                        schema = self.context.current.schema
                        minimize_by = \
                            'most_generic' if target_most_generic \
                            else 'least_generic'
                        target = item1.ptr_class.create_common_target(
                            schema, (item1, item2), minimize_by=minimize_by)
                    else:
                        target = item1.target_class

                    subspec = self._merge_pathspecs(
                        item1_subspec,
                        item2_subspec,
                        target_most_generic=target_most_generic)

                    merged = item1.__class__(
                        ptr_class=item1.ptr_class,
                        pathspec=subspec,
                        ptr_direction=item1.ptr_direction,
                        target_class=target,
                        recurse=item1.recurse,
                        sorter=item1.sorter,
                        generator=item1.generator,
                        offset=item1.offset,
                        limit=item1.limit)

                    result.append(merged)

                    merged_right.add(i)

                    break
            else:
                result.append(item1)

        for i, item2 in enumerate(pathspec2):
            if i not in merged_right:
                result.append(item2)

        return result

    def reorder_aggregates(self, expr):
        return AggregateSorter.run(expr)

    def link_subqueries(self, expr):
        return SubqueryLinker.run(expr)

    def _merge_paths(self, context, expr):
        return pathmerger.PathMerger.run(expr, context=context)

    def postprocess_expr(self, expr):
        paths = irutils.extract_paths(expr, reverse=True)

        if paths:
            if isinstance(paths, irast.PathCombination):
                paths = paths.paths
            else:
                paths = {paths}

            for path in paths:
                self._postprocess_expr(path)

    def _postprocess_expr(self, expr):
        if isinstance(expr, irast.EntitySet):
            if self.context.current.location == 'generator':
                if (len(expr.disjunction.paths) == 1
                        and len(expr.conjunction.paths) == 0
                        and not expr.disjunction.fixed):
                    # Generator by default produces strong paths, that must
                    # limit every other path in the query.  However, to
                    # accommodate for possible disjunctions in generator
                    # expressions, links are put into disjunction.  If,
                    # in fact, there was not disjunctive expressions in
                    # generator, the link must be turned into conjunction,
                    # but only if the disjunction was not merged from a weak
                    # binary op, like OR.
                    #
                    expr.conjunction = irast.Conjunction(
                        paths=expr.disjunction.paths)
                    expr.disjunction = irast.Disjunction()

            for path in expr.conjunction.paths:
                self._postprocess_expr(path)

            for path in expr.disjunction.paths:
                self._postprocess_expr(path)

        elif isinstance(expr, irast.PathCombination):
            for path in expr.paths:
                self._postprocess_expr(path)

        elif isinstance(expr, irast.EntityLink):
            if expr.target:
                self._postprocess_expr(expr.target)

        elif isinstance(expr, irast.BaseRef):
            pass

        else:
            assert False, "Unexpexted expression: %s" % expr

    def is_weak_op(self, op):
        return irutils.is_weak_op(op) \
            or self.context.current.location != 'generator'

    @classmethod
    def get_query_schema_scope(cls, tree):
        """Determine query scope."""
        entity_paths = ast.find_children(
            tree, lambda i: isinstance(i, irast.EntitySet))

        return list({p
                     for p in entity_paths
                     if isinstance(p.concept, s_concepts.Concept)})

    def process_function_call(self, node):
        if node.name in (('search', 'rank'), ('search', 'headline')):
            if not isinstance(node.args[0], irast.Sequence):
                refs = set()
                for arg in node.args:
                    if isinstance(arg, irast.EntitySet):
                        refs.add(arg)
                    else:

                        def testfn(n):
                            if isinstance(n, irast.EntitySet):
                                return True
                            elif isinstance(n, irast.SubgraphRef):
                                raise ast.SkipNode()

                        refs.update(
                            ast.find_children(
                                arg, testfn, force_traversal=True))

                assert len(refs) == 1

                ref = next(iter(refs))

                cols = []
                for link_name, link in ref.concept.get_searchable_links():
                    id = irutils.LinearPath(ref.id)
                    id.add(link, s_pointers.PointerDirection.Outbound,
                           link.target)
                    cols.append(
                        irast.AtomicRefSimple(
                            ref=ref, name=link_name, ptr_class=link, id=id))

                if not cols:
                    raise edgedb_error.EdgeDBError(
                        '{} call on concept {} without any search '
                        'configuration'.format(node.name, ref.concept.name),
                        hint='Configure search for "%s"' % ref.concept.name)

                ref.atomrefs.update(cols)
                vector = irast.Sequence(elements=cols)
            else:
                vector = node.args[0]

            node = irast.FunctionCall(
                name=node.name,
                args=[vector, node.args[1]],
                kwargs=node.kwargs)

        elif self._is_func_agg(node.name):
            node.aggregates = True

        elif node.name[0] == 'window':
            node.window = True

        elif node.name == 'type':
            if len(node.args) != 1:
                raise edgedb_error.EdgeDBError(
                    'type() function takes exactly one argument, {} given'
                    .format(len(node.args)))

            arg = next(iter(node.args))

            if isinstance(arg, irast.Disjunction):
                arg = next(iter(arg.paths))
            elif not isinstance(arg, irast.EntitySet):
                raise edgedb_error.EdgeDBError(
                    'type() function only supports concept arguments')

            node = irast.FunctionCall(name=node.name, args=[arg])
            return node

        if node.args:
            for arg in node.args:
                if not isinstance(arg, irast.Constant):
                    break
            else:
                node = irast.Constant(expr=node, type=node.args[0].type)

        return node

    def process_sequence(self, seq, squash_homogeneous=False):
        pathdict = {}
        proppathdict = {}
        elems = []

        const = True
        const_type = Void

        for elem in seq.elements:
            if isinstance(elem, (irast.BaseRef, irast.Disjunction)):
                if not isinstance(elem, irast.Disjunction):
                    elem = irast.Disjunction(paths=frozenset({elem}))
                elif len(elem.paths) > 1:
                    break

                pd = self.check_atomic_disjunction(elem, irast.AtomicRef)
                if not pd:
                    pd = self.check_atomic_disjunction(elem, irast.LinkPropRef)
                    if not pd:
                        break
                    proppathdict.update(pd)
                else:
                    pathdict.update(pd)

                if pathdict and proppathdict:
                    break

                elems.append(next(iter(elem.paths)))
                const = False
            elif const and isinstance(elem, irast.Constant):
                if elem.index is None:
                    if const_type is Void:
                        const_type = elem.type
                    elif const_type != elem.type:
                        const_type = None
            else:
                # The sequence is not all atoms
                break
        else:
            if const:
                if const_type in (None, Void) or not squash_homogeneous:
                    # Non-homogeneous sequence
                    return irast.Constant(expr=seq)
                else:
                    val = []

                    if isinstance(const_type, tuple) and const_type[0] == list:
                        for elem in seq.elements:
                            if elem.value is not None:
                                val.extend(elem.value)

                        val = tuple(val)
                    else:
                        val.extend(c.value for c in seq.elements
                                   if c.value is not None)

                        if len(val) == 1:
                            val = val[0]
                        elif len(val) == 0:
                            val = None
                        else:
                            val = tuple(val)
                            const_type = (list, const_type)

                    return irast.Constant(value=val, type=const_type)
            else:
                if len(pathdict) == 1:
                    exprtype = irast.AtomicRefExpr
                elif len(proppathdict) == 1:
                    exprtype = irast.LinkPropRefExpr
                    pathdict = proppathdict
                else:
                    exprtype = None

                if exprtype:
                    # The sequence is composed of references to atoms of
                    # the same node.
                    ref = list(pathdict.values())[0]

                    for elem in elems:
                        if elem.ref is not ref.ref:
                            elem.replace_refs([elem.ref], ref.ref, deep=True)

                    return exprtype(expr=irast.Sequence(elements=elems))

        return seq

    def check_atomic_disjunction(self, expr, typ):
        """Check that all paths in disjunction are atom references.

        Return a dict mapping path prefixes to a corresponding node.
        """
        pathdict = {}
        for ref in expr.paths:
            # Check that refs in the operand are all atomic: non-atoms do not
            # coerce to literals.
            #
            if not isinstance(ref, typ):
                return None

            if isinstance(ref, irast.AtomicRef):
                ref_id = ref.ref.get_id()
            else:
                if not ref.get_id():
                    if ref.ref.target:
                        ref_id = ref.ref.target.get_id()
                    elif ref.ref.source:
                        ref_id = ref.ref.source.get_id()
                    else:
                        ref_id = irutils.LinearPath([ref.ref.link_class])
                else:
                    ref_id = ref.get_id()

            pathdict[ref_id] = ref
        return pathdict

    def process_binop(self, left, right, op):
        try:
            result = self._process_binop(left, right, op, reversed=False)
        except EdgeQLCompilerError:
            result = self._process_binop(right, left, op, reversed=True)

        return result

    def process_type_ref_elem(self, context, expr, qlcontext):
        if isinstance(expr, irast.EntitySet):
            if expr.rlink is not None:
                raise errors.EdgeQLSyntaxError(
                    'expecting a type reference',
                    context=qlcontext)

            result = irast.TypeRef(
                maintype=expr.concept.name,
            )

        else:
            raise errors.EdgeQLSyntaxError(
                'expecting a type reference',
                context=qlcontext)

        return result

    def process_type_ref_expr(self, context, qlexpr):
        expr = self._process_expr(context, qlexpr)

        if isinstance(expr, irast.Sequence):
            elems = []

            for elem in expr.elements:
                ref_elem = self.process_type_ref_elem(
                    context, elem, elem.context)

                elems.append(ref_elem)

            expr.elements = elems
            expr.is_array = True

        else:
            expr = self.process_type_ref_elem(context, expr, expr.context)

        return expr

    def is_join(self, left, right, op, reversed):
        return (isinstance(left, irast.Path)
                and isinstance(right, irast.Path)
                and op in (ast.ops.EQ, ast.ops.NE))

    def is_type_check(self, left, right, op, reversed):
        return (not reversed and op in (ast.ops.IS, ast.ops.IS_NOT)
                and isinstance(left, irast.Path))

    def is_concept_path(self, expr):
        if isinstance(expr, irast.PathCombination):
            return all(self.is_concept_path(p) for p in expr.paths)
        elif isinstance(expr, irast.EntitySet):
            return isinstance(expr.concept, s_concepts.Concept)
        else:
            return False

    def is_const_idfilter(self, left, right, op, reversed):
        return (self.is_concept_path(left)
                and isinstance(right, irast.Constant)
                and (op in (ast.ops.IN, ast.ops.NOT_IN)
                or (not reversed and op in (ast.ops.EQ, ast.ops.NE))))

    def is_constant(self, expr):
        flt = lambda node: isinstance(node, irast.Path)
        paths = ast.visitor.find_children(expr, flt)
        return not paths and not isinstance(expr, irast.Path)

    def get_multipath(self, expr: irast.Path):
        if not isinstance(expr, irast.PathCombination):
            expr = irast.Disjunction(paths=frozenset((expr, )))
        return expr

    def path_from_set(self, paths):
        if len(paths) == 1:
            return next(iter(paths))
        else:
            return irast.Disjunction(paths=frozenset(paths))

    def uninline(self, expr):
        cc = ast.visitor.find_children(
            expr, lambda i: isinstance(i, irast.BaseRefExpr))
        for node in cc:
            node.inline = False
        if isinstance(expr, irast.BaseRefExpr):
            expr.inline = False

    def _process_binop(self, left, right, op, reversed=False):
        result = None

        def newbinop(left, right, operation=None, uninline=False):
            operation = operation or op

            if uninline and not isinstance(operation, ast.ops.BooleanOperator):
                self.uninline(left)
                self.uninline(right)

            if reversed:
                return irast.BinOp(left=right, op=operation, right=left)
            else:
                return irast.BinOp(left=left, op=operation, right=right)

        left_paths = irutils.extract_paths(
            left,
            reverse=False,
            resolve_arefs=False,
            extract_subgraph_refs=True)

        if isinstance(left_paths, irast.Path):
            # If both left and right operands are references to atoms of the
            # same node, or one of the operands is a reference to an atom and
            # other is a constant, then fold the expression into an in-line
            # filter of that node.
            #

            left_exprs = self.get_multipath(left_paths)

            pathdict = self.check_atomic_disjunction(left_exprs,
                                                     irast.AtomicRef)
            proppathdict = self.check_atomic_disjunction(left_exprs,
                                                         irast.LinkPropRef)

            is_agg = (is_aggregated_expr(left, deep=True)
                      or is_aggregated_expr(right, deep=True))

            if is_agg:
                result = newbinop(left, right, uninline=True)

            elif not pathdict and not proppathdict:

                if self.is_join(left, right, op, reversed):
                    # Concept join expression: <path> {==|!=} <path>

                    right_exprs = self.get_multipath(right)

                    id_col = sn.Name('std::id')
                    lrefs = [irast.AtomicRefSimple(
                        ref=p, name=id_col) for p in left_exprs.paths]
                    rrefs = [irast.AtomicRefSimple(
                        ref=p, name=id_col) for p in right_exprs.paths]

                    l = irast.Disjunction(paths=frozenset(lrefs))
                    r = irast.Disjunction(paths=frozenset(rrefs))
                    result = newbinop(l, r)

                    for lset, rset in itertools.product(left_exprs.paths,
                                                        right_exprs.paths):
                        lset.joins.add(rset)
                        rset.backrefs.add(lset)
                        rset.joins.add(lset)
                        lset.backrefs.add(rset)

                elif self.is_type_check(left, right, op, reversed):
                    # Type check expression: <path> IS [NOT] <concept>
                    paths = set()
                    cls_ptr = sn.Name('std::__class__')
                    id_col = sn.Name('std::id')

                    for path in left_exprs.paths:
                        clsref = irutils.get_path_step(path, cls_ptr)
                        if clsref is None:
                            clsptr = path.concept.resolve_pointer(
                                self.context.current.schema,
                                cls_ptr
                            )
                            clsref = irutils.extend_path(path, clsptr)
                        ref = irast.AtomicRefSimple(ref=clsref, name=id_col)
                        expr = irast.BinOp(left=ref, right=right, op=op)
                        paths.add(irast.AtomicRefExpr(expr=expr))

                    result = self.path_from_set(paths)

                elif self.is_const_idfilter(left, right, op, reversed):
                    # Constant id filter expressions:
                    #       <path> IN <const_id_list>
                    #       <const_id> IN <path>
                    #       <path> = <const_id>

                    id_col = sn.Name('std::id')

                    # <Constant> IN <EntitySet> is interpreted as a membership
                    # check of entity with ID represented by Constant in
                    # the EntitySet, which is equivalent to
                    # <EntitySet>.id = <Constant>
                    #
                    if reversed:
                        membership_op = \
                            ast.ops.EQ if op == ast.ops.IN else ast.ops.NE
                    else:
                        membership_op = op

                    if isinstance(right.type, s_concepts.Concept):
                        id_t = self.context.current.schema.get('uuid')
                        const_filter = irast.Constant(
                            value=right.value,
                            index=right.index,
                            expr=right.expr,
                            type=id_t)
                    else:
                        const_filter = right

                    paths = set()
                    for p in left_exprs.paths:
                        ref = irast.AtomicRefSimple(ref=p, name=id_col)
                        expr = irast.BinOp(
                            left=ref, right=const_filter, op=membership_op)
                        paths.add(irast.AtomicRefExpr(expr=expr))

                    result = self.path_from_set(paths)

                elif (isinstance(op, irast.TextSearchOperator) and all(
                        isinstance(p, irast.EntitySet)
                        for p in left_exprs.paths)):
                    paths = set()
                    for p in left_exprs.paths:
                        searchable = list(p.concept.get_searchable_links())
                        if not searchable:
                            err = (
                                '{} operator called on concept {} without '
                                'any search configuration'.format(
                                    op, p.concept.name))
                            hint = 'Configure search for "{}"'.format(
                                p.concept.name)
                            raise edgedb_error.EdgeDBError(err, hint=hint)

                        # A SEARCH operation on an entity set is always an
                        # inline filter ATM.
                        paths.add(irast.AtomicRefExpr(expr=newbinop(p, right)))

                    result = self.path_from_set(paths)

                if not result:
                    result = newbinop(left, right, uninline=True)
            else:
                right_paths = irutils.extract_paths(
                    right,
                    reverse=False,
                    resolve_arefs=False,
                    extract_subgraph_refs=True)

                if self.is_constant(right):
                    paths = set()

                    if proppathdict:
                        exprnode_type = irast.LinkPropRefExpr
                        refdict = proppathdict
                    else:
                        exprnode_type = irast.AtomicRefExpr
                        refdict = pathdict

                    if isinstance(left, irast.Path):
                        # We can only break up paths, and must not pick
                        # paths out of other expressions.
                        #
                        for ref in left_exprs.paths:
                            if isinstance(ref, exprnode_type):
                                _leftref = ref.expr
                            else:
                                _leftref = ref

                            paths.add(
                                exprnode_type(expr=newbinop(_leftref, right)))
                        else:
                            result = self.path_from_set(paths)

                    elif len(refdict) == 1:
                        # Left operand references a single entity
                        _binop = newbinop(left, right)

                        aexprs = ast.find_children(
                            _binop, lambda i: isinstance(i, exprnode_type))
                        for aexpr in aexprs:
                            aexpr.inline = False

                        _binop = pathmerger.PathMerger.run(
                            _binop, context=self.context)

                        result = exprnode_type(expr=_binop)
                    else:
                        result = newbinop(left, right, uninline=True)

                elif isinstance(right_paths, irast.Path):
                    right_exprs = self.get_multipath(right_paths)

                    rightdict = self.check_atomic_disjunction(right_exprs,
                                                              irast.AtomicRef)
                    rightpropdict = self.check_atomic_disjunction(
                        right_exprs, irast.LinkPropRef)

                    if (rightdict and pathdict
                            or rightpropdict and proppathdict):
                        paths = set()

                        if proppathdict:
                            exprtype = irast.LinkPropRefExpr
                            leftdict = proppathdict
                            rightdict = rightpropdict
                        else:
                            exprtype = irast.AtomicRefExpr
                            leftdict = pathdict

                        # If both operands are atom references, then we check
                        # if the referenced atom parent concepts intersect, and
                        # if they do we fold the expression into the atom ref
                        # for those common concepts only.  If there are no
                        # common concepts, a regular binary operation is
                        # returned.
                        #
                        if isinstance(left, irast.Path) and isinstance(
                                right, irast.Path):
                            # We can only break up paths, and must not pick
                            # paths out of other expressions.
                            #
                            for ref in left_exprs.paths:
                                if isinstance(ref, irast.AtomicRef):
                                    left_id = ref.ref.get_id()
                                else:
                                    left_id = ref.get_id()

                                right_expr = rightdict.get(left_id)

                                if right_expr:
                                    right_expr.replace_refs(
                                        [right_expr.ref], ref.ref, deep=True)

                                    # Simplify RefExprs
                                    if isinstance(ref, exprtype):
                                        _leftref = ref.expr
                                    else:
                                        _leftref = ref

                                    if isinstance(right_expr, exprtype):
                                        _rightref = right_expr.expr
                                    else:
                                        _rightref = right_expr

                                    filterop = newbinop(_leftref, _rightref)
                                    paths.add(exprtype(expr=filterop))

                            if paths:
                                result = self.path_from_set(paths)
                            else:
                                result = newbinop(left, right, uninline=True)

                        elif len(rightdict) == 1 and len(leftdict) == 1 and \
                                next(iter(leftdict)) == next(iter(rightdict)):

                            newref = next(iter(leftdict.values()))
                            refs = [p.ref for p in right_exprs.paths]
                            right.replace_refs(refs, newref.ref, deep=True)
                            # Left and right operand reference the same single
                            # path

                            _binop = newbinop(left, right)
                            _binop = pathmerger.PathMerger.run(
                                _binop, context=self.context)
                            result = exprtype(expr=_binop)

                        else:
                            result = newbinop(left, right, uninline=True)
                    else:
                        result = newbinop(left, right, uninline=True)

                elif (isinstance(right, irast.PathCombination)
                        and isinstance(next(iter(right.paths)),
                                       irast.SubgraphRef)):
                    result = newbinop(left, right, uninline=True)

                elif (isinstance(right, irast.BinOp)
                        and op == right.op
                        and isinstance(left, irast.Path)):
                    # Got a bin-op, that was not folded into an atom ref.
                    # Re-check it since we may use operator associativity
                    # to fold one of the operands.
                    #
                    assert not proppathdict

                    folded_operand = None
                    for operand in (right.left, right.right):
                        if isinstance(operand, irast.AtomicRef):
                            operand_id = operand.ref.id
                            ref = pathdict.get(operand_id)
                            if ref:
                                ref.expr = irutils.extend_binop(
                                    ref.expr, operand, op=op, reverse=reversed)
                                folded_operand = operand
                                break

                    if folded_operand:
                        other_operand = \
                            right.left if folded_operand is right.right \
                            else right.right
                        result = newbinop(left, other_operand)
                    else:
                        result = newbinop(left, right, uninline=True)

        elif isinstance(left, irast.Constant):
            if isinstance(right, irast.Constant):
                l, r = (right, left) if reversed else (left, right)

                schema = self.context.current.schema
                if isinstance(op, (ast.ops.ComparisonOperator,
                                   ast.ops.TypeCheckOperator)):
                    result_type = schema.get('std::bool')
                else:
                    if l.type == r.type:
                        result_type = l.type
                    else:
                        result_type = s_types.TypeRules.get_result(op, (
                            l.type, r.type), schema)
                result = irast.Constant(
                    expr=newbinop(left, right), type=result_type)

        elif isinstance(left, irast.BinOp):
            result = newbinop(left, right, uninline=True)

        elif isinstance(left, irast.UnaryOp):
            result = newbinop(left, right, uninline=True)

        elif isinstance(left, irast.TypeCast):
            result = newbinop(left, right, uninline=True)

        elif isinstance(left, irast.FunctionCall):
            result = newbinop(left, right, uninline=True)

        elif isinstance(left, irast.ExistPred):
            result = newbinop(left, right, uninline=True)

        elif isinstance(left, irast.SubgraphRef):
            result = newbinop(left, right, uninline=True)

        if not result:
            raise EdgeQLCompilerError('unexpected binop operands: %s, %s' %
                                      (left, right))

        return result

    def process_unaryop(self, expr, operator):
        if isinstance(expr, irast.AtomicRef):
            result = irast.AtomicRefExpr(expr=irast.UnaryOp(
                expr=expr, op=operator))

        elif isinstance(expr, irast.LinkPropRef):
            result = irast.LinkPropRefExpr(expr=irast.UnaryOp(
                expr=expr, op=operator))

        elif isinstance(expr, irast.Constant):
            result = irast.Constant(expr=irast.UnaryOp(expr=expr, op=operator))

        else:
            paths = irutils.extract_paths(
                expr, reverse=False, resolve_arefs=False)
            exprs = self.get_multipath(paths)
            arefs = self.check_atomic_disjunction(exprs, irast.AtomicRef)
            proprefs = self.check_atomic_disjunction(exprs, irast.LinkPropRef)

            if arefs and len(arefs) == 1:
                result = irast.AtomicRefExpr(expr=irast.UnaryOp(
                    expr=expr, op=operator))

            elif proprefs and len(proprefs) == 1:
                result = irast.LinkPropRefExpr(expr=irast.UnaryOp(
                    expr=expr, op=operator))

            else:
                result = irast.UnaryOp(expr=expr, op=operator)

        return result

    def process_none_test(self, expr, schema):
        if isinstance(expr.expr, irast.AtomicRef):
            expr = irast.AtomicRefExpr(expr=expr)
        elif isinstance(expr.expr, irast.LinkPropRef):
            expr = irast.LinkPropRefExpr(expr=expr)
        elif isinstance(expr.expr, irast.EntitySet):
            c = irast.Conjunction(paths=frozenset((expr.expr, )))
            aref = self.entityref_to_idref(c, schema)
            expr = irast.AtomicRefExpr(expr=irast.NoneTest(expr=aref))
        elif isinstance(expr.expr, irast.Constant):
            expr = irast.Constant(expr=expr)

        return expr

    def get_selector_types(self, selector, schema):
        result = collections.OrderedDict()

        for i, selexpr in enumerate(selector):
            expr_type = irutils.infer_type(selexpr.expr, schema)

            if isinstance(selexpr.expr, irast.Constant):
                expr_kind = 'constant'
            elif isinstance(selexpr.expr,
                            (irast.EntitySet, irast.AtomicRefSimple,
                             irast.LinkPropRefSimple, irast.Record)):
                expr_kind = 'path'
            elif (isinstance(selexpr.expr, irast.AtomicRefExpr) and
                  selexpr.expr.ptr_class is not None):
                # RefExpr represents a computable
                expr_kind = 'path'
            elif isinstance(selexpr.expr, irast.PathCombination):
                for p in selexpr.expr.paths:
                    if (not isinstance(p, (irast.EntitySet,
                                           irast.AtomicRefSimple,
                                           irast.LinkPropRefSimple))
                            and not (isinstance(p, irast.AtomicRefExpr)
                                     and p.ptr_class is not None)):
                        expr_kind = 'expression'
                        break
                else:
                    expr_kind = 'path'
            else:
                expr_kind = 'expression'
            result[selexpr.name or str(i)] = (expr_type, expr_kind)

        return result


def compile_fragment_to_ir(expr,
                           schema,
                           *,
                           anchors=None,
                           location=None,
                           modaliases=None):
    """Compile given EdgeQL expression fragment into EdgeDB IR."""
    tree = parser.parse_fragment(expr)
    trans = compiler2.EdgeQLCompiler(schema, modaliases)
    return trans.transform_fragment(
        tree, (), anchors=anchors, location=location)


def compile_ast_fragment_to_ir(tree,
                               schema,
                               *,
                               anchors=None,
                               location=None,
                               modaliases=None):
    """Compile given EdgeQL AST fragment into EdgeDB IR."""
    trans = compiler2.EdgeQLCompiler(schema, modaliases)
    return trans.transform_fragment(
        tree, (), anchors=anchors, location=location)


@debug.debug
def compile_to_ir(expr,
                  schema,
                  *,
                  anchors=None,
                  arg_types=None,
                  security_context=None,
                  modaliases=None):
    """Compile given EdgeQL statement into EdgeDB IR."""
    """LOG [edgeql.compile] EdgeQL TEXT:
    print(expr)
    """
    tree = parser.parse(expr, modaliases)
    """LOG [edgeql.compile] EdgeQL AST:
    from edgedb.lang.common import markup
    markup.dump(tree)
    """
    trans = compiler2.EdgeQLCompiler(schema, modaliases)

    ir = trans.transform(
        tree,
        arg_types,
        modaliases=modaliases,
        anchors=anchors,
        security_context=security_context)
    """LOG [edgeql.compile] EdgeDB IR:
    from edgedb.lang.common import markup
    markup.dump(ir)
    """

    return ir


@debug.debug
def compile_ast_to_ir(tree,
                      schema,
                      *,
                      anchors=None,
                      arg_types=None,
                      security_context=None,
                      modaliases=None):
    """Compile given EdgeQL AST into EdgeDB IR."""
    """LOG [edgeql.compile] EdgeQL AST:
    from edgedb.lang.common import markup
    markup.dump(tree)
    """
    trans = compiler2.EdgeQLCompiler(schema, modaliases)

    ir = trans.transform(
        tree,
        arg_types,
        modaliases=modaliases,
        anchors=anchors,
        security_context=security_context)
    """LOG [edgeql.compile] EdgeDB IR:
    from edgedb.lang.common import markup
    markup.dump(ir)
    """

    return ir
