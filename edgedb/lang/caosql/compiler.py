##
# Copyright (c) 2008-2015 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##

"""CaosQL to IR compiler"""


import collections
import itertools
import operator

from metamagic.caos import types as caos_types
from metamagic.caos import ir
from metamagic.caos.ir import ast as irast
from metamagic.caos import name as caos_name
from metamagic.caos import utils as caos_utils
from metamagic.caos.caosql import ast as qlast
from metamagic.caos.caosql import errors
from metamagic.caos.caosql import parser

from metamagic.utils import ast
from metamagic.utils import debug


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
                self.proto_schema = prevlevel.proto_schema
                self.module_aliases = prevlevel.module_aliases
                self.aliascnt = prevlevel.aliascnt.copy()
                self.subgraphs_map = {}
                self.cge_map = prevlevel.cge_map.copy()
                self.weak_path = None
                self.apply_access_control_rewrite = \
                        prevlevel.apply_access_control_rewrite
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
                self.proto_schema = prevlevel.proto_schema
                self.module_aliases = prevlevel.module_aliases
                self.aliascnt = prevlevel.aliascnt
                self.subgraphs_map = prevlevel.subgraphs_map
                self.cge_map = prevlevel.cge_map
                self.weak_path = prevlevel.weak_path
                self.apply_access_control_rewrite = \
                        prevlevel.apply_access_control_rewrite
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
            self.proto_schema = None
            self.module_aliases = None
            self.aliascnt = {}
            self.subgraphs_map = {}
            self.cge_map = {}
            self.weak_path = None
            self.apply_access_control_rewrite = False

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


class CaosQLCompiler(ir.transformer.TreeTransformer):
    def __init__(self, proto_schema, module_aliases=None):
        self.proto_schema = proto_schema
        self.module_aliases = module_aliases

    def _init_context(self, arg_types, module_aliases, anchors,
                            *, security_context=None):
        self.context = context = ParseContext()
        self.context.current.proto_schema = self.proto_schema
        self.context.current.module_aliases = module_aliases or self.module_aliases

        if self.context.current.module_aliases:
            self.context.current.namespaces.update(self.context.current.module_aliases)

        if anchors:
            self._populate_anchors(context, anchors)

        if security_context:
            self.context.current.apply_access_control_rewrite = True

        return context

    def transform(self, caosql_tree, arg_types, module_aliases=None,
                        anchors=None, security_context=None):
        context = self._init_context(arg_types, module_aliases, anchors,
                                     security_context=security_context)

        if isinstance(caosql_tree, qlast.SelectQueryNode):
            stree = self._transform_select(context, caosql_tree, arg_types)
        elif isinstance(caosql_tree, qlast.UpdateQueryNode):
            stree = self._transform_update(context, caosql_tree, arg_types)
        elif isinstance(caosql_tree, qlast.DeleteQueryNode):
            stree = self._transform_delete(context, caosql_tree, arg_types)
        else:
            msg = 'unexpected statement type: {!r}'.format(caosql_tree)
            raise ValueError(msg)

        self.apply_fixups(stree)
        self.apply_rewrites(stree)
        return stree

    def transform_fragment(self, caosql_tree, arg_types, module_aliases=None, anchors=None,
                                 location=None):
        context = self._init_context(arg_types, module_aliases, anchors)
        context.current.location = location or 'generator'

        if isinstance(caosql_tree, qlast.SelectQueryNode):
            stree = self._transform_select(context, caosql_tree, arg_types)
        else:
            stree = self._process_expr(context, caosql_tree)

        return stree

    def _populate_anchors(self, context, anchors):
        for anchor, proto in anchors.items():
            if isinstance(proto, caos_types.ProtoNode):
                step = irast.EntitySet()
                step.concept = proto
                step.id = caos_utils.LinearPath([step.concept])
                step.anchor = anchor
                step.show_as_anchor = anchor
                # XXX
                # step.users =
            elif isinstance(proto, caos_types.ProtoLink):
                if proto.source:
                    src = irast.EntitySet()
                    src.concept = proto.source
                    src.id = caos_utils.LinearPath([src.concept])
                else:
                    src = None

                step = irast.EntityLink(link_proto=proto, source=src)
                step.anchor = anchor
                step.show_as_anchor = anchor

                if src:
                    src.disjunction.update(step)

            elif isinstance(proto, caos_types.ProtoLinkProperty):
                ptr_name = proto.normal_name()

                if proto.source.source:
                    src = irast.EntitySet()
                    src.concept = proto.source.source
                    src.id = caos_utils.LinearPath([src.concept])
                else:
                    src = None

                link = irast.EntityLink(link_proto=proto.source, source=src,
                                           direction=caos_types.OutboundDirection)

                if src:
                    src.disjunction.update(link)
                    pref_id = caos_utils.LinearPath(src.id)
                else:
                    pref_id = caos_utils.LinearPath([])

                pref_id.add(proto.source, caos_types.OutboundDirection, proto.target)

                step = irast.LinkPropRefSimple(name=ptr_name, ref=link, id=pref_id,
                                                  ptr_proto=proto)
                step.anchor = anchor
                step.show_as_anchor = anchor

            else:
                step = proto

            context.current.anchors[anchor] = step

    def _transform_select(self, context, caosql_tree, arg_types):
        self.arg_types = arg_types or {}

        graph = context.current.graph = irast.GraphExpr()

        if caosql_tree.namespaces:
            for ns in caosql_tree.namespaces:
                context.current.namespaces[ns.alias] = ns.namespace

        if context.current.module_aliases:
            context.current.namespaces.update(context.current.module_aliases)

        if caosql_tree.cges:
            graph.cges = []

            for cge in caosql_tree.cges:
                with context(ParseContext.SUBQUERY):
                    _cge = self._transform_select(context, cge.expr, arg_types)
                context.current.cge_map[cge.alias] = _cge
                graph.cges.append(irast.CommonGraphExpr(expr=_cge, alias=cge.alias))

        if caosql_tree.op:
            graph.set_op = irast.SetOperator(caosql_tree.op)
            graph.set_op_larg = self._transform_select(
                                    context, caosql_tree.op_larg, arg_types)
            graph.set_op_rarg = self._transform_select(
                                    context, caosql_tree.op_rarg, arg_types)
        else:
            graph.generator = self._process_select_where(context, caosql_tree.where)

            graph.grouper = self._process_grouper(context, caosql_tree.groupby)
            if graph.grouper:
                groupgraph = irast.Disjunction(paths=frozenset(graph.grouper))
                context.current.groupprefixes = self.extract_prefixes(groupgraph)
            else:
                # Check if query() or order() contain any aggregate
                # expressions and if so, add a sentinel group prefix
                # instructing the transformer that we are implicitly grouping
                # the whole set.
                def checker(n):
                    if (isinstance(n, qlast.FunctionCallNode)
                            and n.func[0] == 'agg'):
                        return True
                    elif isinstance(n, (qlast.SubqueryNode,
                                        qlast.SelectQueryNode)):
                        # Make sure we don't dip into subqueries
                        raise ast.SkipNode()

                for node in itertools.chain(caosql_tree.orderby or [],
                                            caosql_tree.targets or []):
                    if ast.find_children(node, checker, force_traversal=True):
                        context.current.groupprefixes = {True: True}
                        break

            graph.selector = self._process_select_targets(
                                context, caosql_tree.targets)

            if (len(caosql_tree.targets) == 1
                  and isinstance(caosql_tree.targets[0].expr, qlast.PathNode)
                  and not graph.generator):
                # This is a node selector query, ensure it is treated as
                # a generator path even in potential absense of an explicit
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
                        ast.find_children(expr, augmenter, force_traversal=True)


        graph.sorter = self._process_sorter(context, caosql_tree.orderby)
        if caosql_tree.offset:
            graph.offset = irast.Constant(value=caosql_tree.offset.value,
                                             index=caosql_tree.offset.index,
                                             type=context.current.proto_schema.get('int'))

        if caosql_tree.limit:
            graph.limit = irast.Constant(value=caosql_tree.limit.value,
                                             index=caosql_tree.limit.index,
                                             type=context.current.proto_schema.get('int'))

        context.current.location = 'top'

        # Merge selector and sorter disjunctions first
        paths = [s.expr for s in graph.selector] + \
                [s.expr for s in graph.sorter] + \
                [s for s in graph.grouper]
        union = irast.Disjunction(paths=frozenset(paths))
        self.flatten_and_unify_path_combination(union, deep=True,
                                                merge_filters=True)

        # Merge the resulting disjunction with generator conjunction
        if graph.generator:
            paths = [graph.generator] + list(union.paths)
            union = irast.Disjunction(paths=frozenset(paths))
            self.flatten_and_unify_path_combination(union, deep=True,
                                                    merge_filters=True)

        # Reorder aggregate expressions so that all of them appear as the
        # first sub-tree in the generator expression.
        #
        if graph.generator:
            self.reorder_aggregates(graph.generator)

        graph.result_types = self.get_selector_types(graph.selector, self.proto_schema)
        graph.argument_types = self.context.current.arguments
        graph.context_vars = self.context.current.context_vars

        self.link_subqueries(graph)

        return graph

    def _transform_update(self, context, caosql_tree, arg_types):
        self.arg_types = arg_types or {}

        graph = context.current.graph = irast.GraphExpr()
        graph.op = 'update'

        if caosql_tree.namespaces:
            for ns in caosql_tree.namespaces:
                context.current.namespaces[ns.alias] = ns.namespace

        if context.current.module_aliases:
            context.current.namespaces.update(context.current.module_aliases)

        if caosql_tree.cges:
            graph.cges = []

            for cge in caosql_tree.cges:
                with context(ParseContext.SUBQUERY):
                    _cge = self._transform_select(context, cge.expr, arg_types)
                context.current.cge_map[cge.alias] = _cge
                graph.cges.append(
                    irast.CommonGraphExpr(expr=_cge, alias=cge.alias))

        tgt = graph.optarget = self._process_select_where(
                                context, caosql_tree.subject)

        idname = caos_name.Name('metamagic.caos.builtins.id')
        idref = irast.AtomicRefSimple(
                    name=idname, ref=tgt,
                    ptr_proto=tgt.concept.pointers[idname])
        tgt.atomrefs.add(idref)
        selexpr = irast.SelectorExpr(expr=idref, name=None)
        graph.selector.append(selexpr)

        graph.generator = self._process_select_where(
                            context, caosql_tree.where)

        with context():
            context.current.location = 'optarget_shaper'
            graph.opselector = self._process_select_targets(
                                    context, caosql_tree.targets)

        with context():
            context.current.location = 'opvalues'
            graph.opvalues = self._process_op_values(context, graph,
                                                     caosql_tree.values)

        context.current.location = 'top'

        paths = [s.expr for s in graph.opselector] + \
                    [s.expr for s in graph.selector] + [graph.optarget]

        if graph.generator:
            paths.append(graph.generator)

        union = irast.Disjunction(paths=frozenset(paths))
        self.flatten_and_unify_path_combination(union, deep=True,
                                                merge_filters=True)

        # Reorder aggregate expressions so that all of them appear as the
        # first sub-tree in the generator expression.
        #
        if graph.generator:
            self.reorder_aggregates(graph.generator)

        graph.result_types = self.get_selector_types(
                                graph.opselector, self.proto_schema)
        graph.argument_types = self.context.current.arguments
        graph.context_vars = self.context.current.context_vars

        self.link_subqueries(graph)

        return graph

    def _process_op_values(self, context, graph, opvalues):
        refs = []

        for updexpr in opvalues:
            targetexpr = updexpr.expr
            value = updexpr.value

            tpath = qlast.PathNode(
                steps=[
                    qlast.LinkExprNode(
                        expr=qlast.LinkNode(
                            name=targetexpr.name,
                            namespace=targetexpr.module
                        )
                    )
                ]
            )

            targetexpr = self._process_path(context, tpath,
                                            path_tip=graph.optarget)

            if not isinstance(targetexpr, irast.AtomicRefSimple):
                msg = 'operation update list can only reference atoms'
                raise errors.CaosQLError(msg)

            if isinstance(value, qlast.ConstantNode):
                v = self._process_constant(context, value)
            else:
                v = self._process_expr(context, value)
                paths = irast.Conjunction(
                            paths=frozenset((v, graph.optarget)))
                self.flatten_and_unify_path_combination(
                    paths, deep=True, merge_filters=True)
                self._check_update_expr(graph.optarget, v)

            ref = irast.UpdateExpr(expr=targetexpr, value=v)
            refs.append(ref)

        return refs

    def _check_update_expr(self, source, expr):
        # Check that all refs in expr point to source and are atomic
        schema_scope = self.get_query_schema_scope(expr)
        ok = (len(schema_scope) == 0 or
                    (len(schema_scope) == 1
                        and schema_scope[0].concept == source.concept))

        if not ok:
            msg = "update expression can only reference local atoms"
            raise errors.CaosQLError(msg)

    def _transform_delete(self, context, caosql_tree, arg_types):
        self.arg_types = arg_types or {}

        graph = context.current.graph = irast.GraphExpr()
        graph.op = 'delete'

        if caosql_tree.namespaces:
            for ns in caosql_tree.namespaces:
                context.current.namespaces[ns.alias] = ns.namespace

        if context.current.module_aliases:
            context.current.namespaces.update(context.current.module_aliases)

        if caosql_tree.cges:
            graph.cges = []

            for cge in caosql_tree.cges:
                with context(ParseContext.SUBQUERY):
                    _cge = self._transform_select(context, cge.expr, arg_types)
                context.current.cge_map[cge.alias] = _cge
                graph.cges.append(
                    irast.CommonGraphExpr(expr=_cge, alias=cge.alias))

        tgt = graph.optarget = self._process_select_where(
                                context, caosql_tree.subject)

        if (isinstance(tgt, irast.LinkPropRefSimple)
                and tgt.name == 'metamagic.caos.builtins.target'):

            idpropname = caos_name.Name('metamagic.caos.builtins.linkid')
            idprop_proto = tgt.ref.link_proto.pointers[idpropname]
            idref = irast.LinkPropRefSimple(name=idpropname,
                                               ref=tgt.ref,
                                               ptr_proto=idprop_proto)
            graph.optarget.ref.proprefs.add(idref)

            selexpr = irast.SelectorExpr(expr=idref, name=None)
            graph.selector.append(selexpr)

        elif isinstance(tgt, irast.AtomicRefSimple):
            idname = caos_name.Name('metamagic.caos.builtins.id')
            idref = irast.AtomicRefSimple(name=idname, ref=tgt.ref,
                                             ptr_proto=tgt.ptr_proto)
            tgt.ref.atomrefs.add(idref)
            selexpr = irast.SelectorExpr(expr=idref, name=None)
            graph.selector.append(selexpr)

        else:
            idname = caos_name.Name('metamagic.caos.builtins.id')
            idref = irast.AtomicRefSimple(
                        name=idname, ref=tgt,
                        ptr_proto=tgt.concept.pointers[idname])
            tgt.atomrefs.add(idref)
            selexpr = irast.SelectorExpr(expr=idref, name=None)
            graph.selector.append(selexpr)

        graph.generator = self._process_select_where(
                            context, caosql_tree.where)

        graph.opselector = self._process_select_targets(
                                context, caosql_tree.targets)

        context.current.location = 'top'

        paths = [s.expr for s in graph.opselector] + \
                    [s.expr for s in graph.selector] + [graph.optarget]

        if graph.generator:
            paths.append(graph.generator)

        union = irast.Disjunction(paths=frozenset(paths))
        self.flatten_and_unify_path_combination(union, deep=True,
                                                merge_filters=True)

        # Reorder aggregate expressions so that all of them appear as the
        # first sub-tree in the generator expression.
        #
        if graph.generator:
            self.reorder_aggregates(graph.generator)

        graph.result_types = self.get_selector_types(
                                graph.opselector, self.proto_schema)
        graph.argument_types = self.context.current.arguments
        graph.context_vars = self.context.current.context_vars

        self.link_subqueries(graph)

        return graph

    def _process_select_where(self, context, where):
        with context():
            context.current.location = 'generator'

            if where:
                expr = self._process_expr(context, where)
                expr = self.merge_paths(expr)
                self.postprocess_expr(expr)
                return expr
            else:
                return None

    def _process_expr(self, context, expr):
        node = None

        if isinstance(expr, qlast.SubqueryNode):
            node = self._process_expr(context, expr.expr)

        elif isinstance(expr, qlast.SelectQueryNode):
            node = context.current.subgraphs_map.get(expr)

            if node is None:
                with self.context(ParseContext.SUBQUERY):
                    node = self._transform_select(context, expr, self.arg_types)

                if len(node.selector) > 1:
                    err = ('subquery must return only one column')
                    raise errors.CaosQLError(err)

                node.referrers.append(context.current.location)
                context.current.graph.subgraphs.add(node)
                context.current.subgraphs_map[expr] = node

            refname = node.selector[0].name or node.selector[0].autoname
            node.attrrefs.add(refname)
            node = irast.SubgraphRef(ref=node, name=refname)

        elif isinstance(expr, qlast.BinOpNode):
            left = self._process_expr(context, expr.left)
            right = self._process_expr(context, expr.right)

            # The entityref_to_record transform must be reverted for typecheck ops
            if isinstance(expr.op, ast.ops.EquivalenceOperator) \
                    and isinstance(left, irast.Record) \
                    and isinstance(right, irast.Constant) \
                    and (isinstance(right.type, caos_types.PrototypeClass) or
                         isinstance(right.type, tuple) and
                         isinstance(right.type[1], caos_types.PrototypeClass)):
                left = left.elements[0].ref

            node = self.process_binop(left, right, expr.op)

        elif isinstance(expr, qlast.PathNode):
            node = self._process_path(context, expr)

            if not isinstance(node, irast.BaseRef):
                if (context.current.location in
                        {'sorter', 'optarget_shaper', 'opvalues'} \
                            and not context.current.in_func_call):
                    if context.current.location == 'sorter':
                        loc = 'order()'
                    elif context.current.location == 'grouper':
                        loc = 'group()'
                    else:
                        if context.current.graph.op == 'update':
                            loc = 'update()'
                        elif context.current.graph.op == 'delete':
                            loc = 'delete()'

                        if context.current.location == 'optarget_shaper':
                            loc += ' return list'

                    err = 'unexpected reference to non-atom path in %s' % loc
                    raise errors.CaosQLError(err)

            if (context.current.groupprefixes
                    and context.current.location in ('sorter', 'selector')\
                    and not context.current.in_aggregate):
                for p in node.paths:
                    if isinstance(p, irast.MetaRef):
                        p = p.ref

                    if p.id not in context.current.groupprefixes:
                        err = ('node reference "%s" must appear in the GROUP BY expression or '
                               'used in an aggregate function ') % p.id
                        raise errors.CaosQLError(err)

            if (context.current.location not in {'generator', 'selector'} \
                            and not context.current.in_func_call) or context.current.in_aggregate:
                if isinstance(node, irast.EntitySet):
                    node = self.entityref_to_record(node, self.proto_schema)

        elif isinstance(expr, qlast.ConstantNode):
            node = self._process_constant(context, expr)

        elif isinstance(expr, qlast.SequenceNode):
            elements=[self._process_expr(context, e) for e in expr.elements]
            node = irast.Sequence(elements=elements)
            # Squash the sequence if it comes from IS (type,...), since we unconditionally
            # transform PrototypeRefNodes into list-type constants below.
            #
            squash_homogeneous = expr.elements and isinstance(expr.elements[0],
                                                              qlast.PrototypeRefNode)
            node = self.process_sequence(node, squash_homogeneous=squash_homogeneous)

        elif isinstance(expr, qlast.FunctionCallNode):
            with context():
                if expr.func[0] == 'agg':
                    context.current.in_aggregate.append(expr)
                context.current.in_func_call = True

                args = []
                kwargs = {}

                for a in expr.args:
                    if isinstance(a, qlast.NamedArgNode):
                        kwargs[a.name] = self._process_expr(context, a.arg)
                    else:
                        args.append(self._process_expr(context, a))

                node = irast.FunctionCall(name=expr.func, args=args,
                                             kwargs=kwargs)

                if expr.agg_sort:
                    node.agg_sort = [
                        irast.SortExpr(
                            expr=self._process_expr(context, e.path),
                            direction=e.direction
                        )
                        for e in expr.agg_sort
                    ]

                elif expr.window:
                    if expr.window.orderby:
                        node.agg_sort = [
                            irast.SortExpr(
                                expr=self._process_expr(context, e.path),
                                direction=e.direction
                            )
                            for e in expr.window.orderby
                        ]

                    if expr.window.partition:
                        for partition_expr in expr.window.partition:
                            partition_expr = self._process_expr(
                                                context, partition_expr)
                            node.partition.append(partition_expr)

                    node.window = True

                node = self.process_function_call(node)

        elif isinstance(expr, qlast.PrototypeRefNode):
            if expr.module:
                name = caos_name.Name(name=expr.name, module=expr.module)
            else:
                name = expr.name
            node = self.proto_schema.get(name=name, module_aliases=context.current.namespaces,
                                         type=caos_types.ProtoNode)

            node = irast.Constant(value=(node,), type=(list, node.__class__))

        elif isinstance(expr, qlast.UnaryOpNode):
            if (expr.op == ast.ops.NOT
                    and isinstance(expr.operand, qlast.NoneTestNode)):
                # Make sure NOT(IS NONE) does not produce weak paths, as IS
                # NONE would normally do.
                self.context.current.weak_path = False

            operand = self._process_expr(context, expr.operand)
            node = self.process_unaryop(operand, expr.op)

        elif isinstance(expr, qlast.NoneTestNode):
            with context():
                if self.context.current.weak_path is None:
                    self.context.current.weak_path = True
                expr = self._process_expr(context, expr.expr)
                nt = irast.NoneTest(expr=expr)
                node = self.process_none_test(nt, context.current.proto_schema)

        elif isinstance(expr, qlast.ExistsPredicateNode):
            subquery = self._process_expr(context, expr.expr)
            if isinstance(subquery, irast.SubgraphRef):
                subquery.ref.referrers.append('exists')
            node = irast.ExistPred(expr=subquery)

        elif isinstance(expr, qlast.TypeCastNode):
            if isinstance(expr.type, tuple):
                typ = expr.type[1]
            else:
                typ = expr.type

            typ = context.current.proto_schema.get(typ, module_aliases=context.current.namespaces)

            if isinstance(expr.type, tuple):
                typ = (expr.type[0], typ)

            node = irast.TypeCast(expr=self._process_expr(context, expr.expr), type=typ)

        elif isinstance(expr, qlast.IndirectionNode):
            node = self._process_expr(context, expr.arg)
            for indirection_el in expr.indirection:
                if isinstance(indirection_el, qlast.IndexNode):
                    idx = self._process_expr(context, indirection_el.index)
                    node = irast.FunctionCall(name='getitem', args=[node, idx])

                elif isinstance(indirection_el, qlast.SliceNode):
                    if indirection_el.start:
                        start = self._process_expr(context, indirection_el.start)
                    else:
                        start = irast.Constant(value=None)

                    if indirection_el.stop:
                        stop = self._process_expr(context, indirection_el.stop)
                    else:
                        stop = irast.Constant(value=None)

                    node = irast.FunctionCall(name='getslice', args=[node, start, stop])
                else:
                    raise ValueError('unexpected indirection node: {!r}'.format(indirection_el))

        else:
            assert False, "Unexpected expr: %s" % expr

        return node

    def _process_constant(self, context, expr):
        if expr.index is not None:
            type = self.arg_types.get(expr.index)
            if type is not None:
                type = caos_types.normalize_type(type, self.proto_schema)
            node = irast.Constant(value=expr.value, index=expr.index, type=type)
            context.current.arguments[expr.index] = type
        else:
            type = caos_types.normalize_type(expr.value.__class__, self.proto_schema)
            node = irast.Constant(value=expr.value, index=expr.index, type=type)

        return node

    def _process_pathspec(self, context, source, rlink_proto, pathspec):
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
                            value = caos_types.PointerLoading(ptrspec_flt.value)
                            ptrspec_flt = (lambda p: p.get_loading_behaviour(), value)
                        else:
                            msg = 'invalid pointer property in pointer glob: {!r}'. \
                                                                format(ptrspec_flt.property)
                            raise errors.CaosQLError(msg)
                        filter_exprs.append(ptrspec_flt)

                if ptrspec.type == 'link':
                    for ptr in source.pointers.values():
                        if not filter_exprs or all(((f[0](ptr) == f[1]) for f in filter_exprs)):
                            glob_specs.append(irast.PtrPathSpec(ptr_proto=ptr))

                elif ptrspec.type == 'property':
                    if rlink_proto is None:
                        msg = 'link properties are not available at this point in path'
                        raise errors.CaosQLError(msg)

                    for ptr_name, ptr in rlink_proto.pointers.items():
                        if ptr.is_special_pointer():
                            continue

                        if not filter_exprs or all(((f[0](ptr) == f[1]) for f in filter_exprs)):
                            glob_specs.append(irast.PtrPathSpec(ptr_proto=ptr))

                else:
                    msg = 'unexpected pointer spec type'
                    raise errors.CaosQLError(msg)

                result = self._merge_pathspecs(result, glob_specs, target_most_generic=False)

            elif isinstance(ptrspec, qlast.SelectTypeRefNode):
                type_prop_name = ptrspec.attrs[0].expr.name
                type_prop = source.get_type_property(
                                type_prop_name, context.current.proto_schema)

                node = irast.PtrPathSpec(
                            ptr_proto=type_prop,
                            ptr_direction=caos_types.OutboundDirection,
                            target_proto=type_prop.target,
                            trigger=irast.ExplicitPathSpecTrigger())

                result.append(node)

            else:
                lexpr = ptrspec.expr.expr
                ptrname = (lexpr.namespace, lexpr.name)

                if lexpr.type == 'property':
                    if rlink_proto is None:
                        msg = 'link properties are not available at this point in path'
                        raise errors.CaosQLError(msg)

                    ptrsource = rlink_proto
                    ptrtype = caos_types.ProtoLinkProperty
                else:
                    ptrsource = source
                    ptrtype = caos_types.ProtoLink

                if lexpr.target is not None:
                    target_name = (lexpr.target.module, lexpr.target.name)
                else:
                    target_name = None

                ptr_direction = lexpr.direction or caos_types.OutboundDirection
                ptr = self._resolve_ptr(context, ptrsource, ptrname,
                                        ptr_direction, ptr_type=ptrtype,
                                        target=target_name)

                if ptrspec.recurse is not None:
                    recurse = self._process_constant(context, ptrspec.recurse)
                else:
                    recurse = None

                target_proto = ptr.get_far_endpoint(ptr_direction)

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
                            ptr_proto=ptr, ptr_direction=ptr_direction,
                            recurse=recurse, target_proto=target_proto,
                            generator=generator, sorter=sorter,
                            trigger=irast.ExplicitPathSpecTrigger(),
                            offset=offset, limit=limit)

                if ptrspec.pathspec is not None:
                    node.pathspec = self._process_pathspec(
                                        context, target_proto,
                                        ptr, ptrspec.pathspec)
                else:
                    node.pathspec = None

                result = self._merge_pathspecs(result, [node],
                                               target_most_generic=False)

        self._normalize_pathspec_recursion(result, source,
                                           context.current.proto_schema)

        return result

    def _process_path(self, context, path, path_tip=None):
        pathvars = context.current.pathvars
        anchors = context.current.anchors
        typeref = None

        for i, node in enumerate(path.steps):
            pathvar = None
            link_pathvar = None

            if isinstance(node, (qlast.PathNode, qlast.PathStepNode)):
                if isinstance(node, qlast.PathNode):
                    if len(node.steps) > 1:
                        raise errors.CaosQLError('unsupported subpath expression')

                    pathvar = node.var.name if node.var else None
                    link_pathvar = node.lvar.name if node.lvar else None

                    if pathvar in pathvars:
                        raise errors.CaosQLError('duplicate path variable: %s' % pathvar)

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
                                    fq_name = '{}.{}'.format(node.namespace, node.expr)
                                    refnode = pathvars[fq_name]
                                except KeyError:
                                    pass

                        if refnode:
                            if isinstance(refnode, irast.Path):
                                path_copy = self.copy_path(refnode)
                                path_copy.users.add(context.current.location)
                                concept = getattr(refnode, 'concept', None)
                                path_tip = path_copy

                            else:
                                path_tip = refnode

                            continue

                        elif node.expr in context.current.cge_map:
                            cge = context.current.cge_map[node.expr]
                            path_tip = cge
                            continue

                    tip = node

                elif isinstance(node, qlast.TypeRefNode):
                    tip = node
            else:
                tip = node

            tip_is_link = path_tip and isinstance(path_tip, irast.EntityLink)
            tip_is_cge = path_tip and isinstance(path_tip, irast.GraphExpr)

            if isinstance(tip, qlast.PathStepNode):

                proto = self._normalize_concept(
                            context, tip.expr, tip.namespace)

                if isinstance(proto, caos_types.ProtoNode):
                    step = irast.EntitySet()
                    step.concept = proto
                    step.id = caos_utils.LinearPath([step.concept])
                    step.pathvar = pathvar
                    step.users.add(context.current.location)
                else:
                    step = irast.EntityLink(link_proto=proto)

                path_tip = step

                if pathvar:
                    pathvars[pathvar] = step

            elif isinstance(tip, qlast.TypeRefNode):
                typeref = self._process_path(context, tip.expr)
                if isinstance(typeref, irast.PathCombination):
                    if len(typeref.paths) > 1:
                        msg = "type() argument must not be a path combination"
                        raise errors.CaosQLError(msg)
                    typeref = next(iter(typeref.paths))

            elif isinstance(tip, qlast.LinkExprNode) and typeref:
                path_tip = irast.MetaRef(ref=typeref, name=tip.expr.name)

            elif isinstance(tip, qlast.LinkExprNode) and tip_is_cge:
                path_tip = irast.SubgraphRef(ref=path_tip,
                                                name=tip.expr.name)

            elif (isinstance(tip, qlast.LinkExprNode)
                    and not tip_is_link and not typeref):
                # LinkExprNode
                link_expr = tip.expr
                link_target = None

                if isinstance(link_expr, qlast.LinkNode):
                    direction = (link_expr.direction
                                    or caos_types.OutboundDirection)
                    if link_expr.target:
                        link_target = self._normalize_concept(
                                            context, link_expr.target.name,
                                            link_expr.target.module)
                else:
                    msg = "complex link expressions are not supported yet"
                    raise errors.CaosQLError()

                linkname = (link_expr.namespace, link_expr.name)

                link_proto = self._resolve_ptr(
                                    context, path_tip.concept, linkname,
                                    direction, target=link_target)

                target = link_proto.get_far_endpoint(direction)

                gen_link_proto = self.proto_schema.get(
                                    link_proto.normal_name())

                if isinstance(target, caos_types.ProtoConcept):
                    target_set = irast.EntitySet()
                    target_set.concept = target
                    target_set.id = caos_utils.LinearPath(path_tip.id)
                    target_set.id.add(link_proto, direction,
                                      target_set.concept)
                    target_set.users.add(context.current.location)

                    link = irast.EntityLink(source=path_tip,
                                               target=target_set,
                                               direction=direction,
                                               link_proto=link_proto,
                                               pathvar=link_pathvar,
                                               users={context.current.location})

                    path_tip.disjunction.update(link)
                    path_tip.disjunction.fixed = context.current.weak_path
                    target_set.rlink = link

                    path_tip = target_set

                elif isinstance(target, caos_types.ProtoAtom):
                    target_set = irast.EntitySet()
                    target_set.concept = target
                    target_set.id = caos_utils.LinearPath(path_tip.id)
                    target_set.id.add(link_proto, direction,
                                      target_set.concept)
                    target_set.users.add(context.current.location)

                    link = irast.EntityLink(source=path_tip,
                                               target=target_set,
                                               link_proto=link_proto,
                                               direction=direction,
                                               pathvar=link_pathvar,
                                               users={context.current.location})

                    target_set.rlink = link

                    atomref_id = caos_utils.LinearPath(path_tip.id)
                    atomref_id.add(link_proto, direction, target)

                    if not link_proto.singular():
                        ptr_name = caos_name.Name(
                                        'metamagic.caos.builtins.target')
                        ptr_proto = link_proto.pointers[ptr_name]
                        atomref = irast.LinkPropRefSimple(
                                        name=ptr_name, ref=link,
                                        id=atomref_id, ptr_proto=ptr_proto)
                        link.proprefs.add(atomref)
                        path_tip.disjunction.update(link)
                        path_tip.disjunction.fixed = context.current.weak_path
                    else:
                        atomref = irast.AtomicRefSimple(
                                        name=link_proto.normal_name(),
                                        ref=path_tip, id=atomref_id,
                                        rlink=link, ptr_proto=link_proto)
                        path_tip.atomrefs.add(atomref)
                        link.target = atomref

                    path_tip = atomref

                if pathvar:
                    pathvars[pathvar] = path_tip

                if link_pathvar:
                    pathvars[link_pathvar] = link

            elif isinstance(tip, qlast.LinkPropExprNode) or tip_is_link:
                # LinkExprNode
                link_expr = tip.expr

                if path_tip and isinstance(path_tip, irast.GraphExpr):
                    subgraph = path_tip
                    subgraph.attrrefs.add(link_expr.name)
                    sgref = irast.SubgraphRef(ref=subgraph,
                                                 name=link_expr.name)
                    path_tip = sgref
                else:
                    prop_name = (link_expr.namespace, link_expr.name)

                    if (isinstance(path_tip, irast.LinkPropRefSimple)
                        and not path_tip.ptr_proto.is_endpoint_pointer()):
                        # We are at propref point, the only valid
                        # step from here is @source taking us back
                        # to the link context.
                        if link_expr.name != 'source':
                            msg = 'invalid reference: {}.{}'\
                                        .format(path_tip.ptr_proto,
                                                link_expr.name)
                            raise errors.CaosQLReferenceError(msg)

                        link = path_tip.ref
                        target = link.target
                        path_tip = target

                    else:
                        if isinstance(path_tip, irast.LinkPropRefSimple):
                            link = path_tip.ref
                            link_proto = link.link_proto
                            id = path_tip.id
                        elif isinstance(path_tip, irast.EntityLink):
                            link = path_tip
                            link_proto = link.link_proto
                            id = caos_utils.LinearPath([None])
                            id.add(link_proto, caos_types.OutboundDirection, None)
                        else:
                            link = path_tip.rlink
                            link_proto = link.link_proto
                            id = path_tip.id

                        prop_proto = self._resolve_ptr(
                            context, link_proto, prop_name,
                            caos_types.OutboundDirection,
                            ptr_type=caos_types.ProtoLinkProperty)

                        propref = irast.LinkPropRefSimple(
                            name=prop_proto.normal_name(),
                            ref=link, id=id, ptr_proto=prop_proto)
                        link.proprefs.add(propref)

                        path_tip = propref

            else:
                assert False, 'Unexpected path step expression: "%s"' % tip

        if isinstance(path_tip, irast.EntityLink):
            # Dangling link reference, possibly from an anchor ref,
            # complement it to target ref
            link_proto = path_tip.link_proto

            pref_id = caos_utils.LinearPath(path_tip.source.id)
            pref_id.add(link_proto, caos_types.OutboundDirection,
                        link_proto.target)
            ptr_proto = link_proto.pointers['metamagic.caos.builtins.target']
            ptr_name = ptr_proto.normal_name()
            propref = irast.LinkPropRefSimple(
                        name=ptr_name, ref=path_tip, id=pref_id,
                        ptr_proto=ptr_proto)
            propref.anchor = path_tip.anchor
            propref.show_as_anchor = path_tip.show_as_anchor
            propref.pathvar = path_tip.pathvar
            path_tip.proprefs.add(propref)

            path_tip = propref

        return path_tip

    def _resolve_ptr(self, context, near_endpoint, ptr_name, direction,
                           ptr_type=caos_types.ProtoLink, target=None):
        ptr_module, ptr_nqname = ptr_name

        if ptr_module:
            ptr_fqname = caos_name.Name(module=ptr_module, name=ptr_nqname)
            modaliases = context.current.namespaces
            pointer = self.proto_schema.get(ptr_fqname,
                                            module_aliases=modaliases,
                                            type=ptr_type)
            pointer_name = pointer.name
        else:
            pointer_name = ptr_fqname = ptr_nqname

        if target is not None and not isinstance(target, caos_types.ProtoNode):
            target_name = '.'.join(filter(None, target))
            modaliases = context.current.namespaces
            target = self.proto_schema.get(target_name,
                                           module_aliases=modaliases)

        if ptr_nqname == '%':
            pointer_name = self.proto_schema.get_root_class(ptr_type).name

        if target is not None:
            far_endpoints = (target,)
        else:
            far_endpoints = None

        ptr = near_endpoint.resolve_pointer(
                    self.proto_schema, pointer_name,
                    direction=direction,
                    look_in_children=True,
                    include_inherited=True,
                    far_endpoints=far_endpoints)

        if not ptr:
            msg = ('[{near_endpoint}].[{direction}{ptr_name}{far_endpoint}] '
                   'does not resolve to any known path')
            far_endpoint_str = '({})'.format(target.name) if target else ''
            msg = msg.format(near_endpoint=near_endpoint.name,
                             direction=direction,
                             ptr_name=pointer_name,
                             far_endpoint=far_endpoint_str)
            raise errors.CaosQLReferenceError(msg)

        return ptr

    def _normalize_concept(self, context, concept, namespace):
        if concept == '%':
            concept = self.proto_schema.get(name='metamagic.caos.builtins.BaseObject')
        else:
            if namespace:
                name = caos_name.Name(name=concept, module=namespace)
            else:
                name = concept
            concept = self.proto_schema.get(name=name, module_aliases=context.current.namespaces)
        return concept

    def _process_select_targets(self, context, targets):
        selector = list()

        with context():
            context.current.location = 'selector'
            for target in targets:
                expr = self._process_expr(context, target.expr)
                expr = self.merge_paths(expr)
                if target.alias:
                    params = {'name': target.alias}
                else:
                    params = {'autoname': context.current.genalias()}

                if isinstance(expr, irast.Disjunction):
                    path = next(iter(expr.paths))
                else:
                    path = expr

                if isinstance(path, irast.EntitySet):
                    if target.expr.pathspec is not None:
                        if path.rlink is not None:
                            rlink_proto = path.rlink.link_proto
                        else:
                            rlink_proto = None
                        pathspec = self._process_pathspec(context, path.concept,
                                                          rlink_proto,
                                                          target.expr.pathspec)
                    else:
                        pathspec = None
                    expr = self.entityref_to_record(expr, self.proto_schema, pathspec=pathspec)

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
                    expr = self.merge_paths(expr)
                    if isinstance(expr, irast.PathCombination):
                        assert len(expr.paths) == 1
                        expr = next(iter(expr.paths))
                    s = irast.SortExpr(expr=expr, direction=sorter.direction,
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
                    expr = self.merge_paths(expr)
                    result.append(expr)

        return result


def compile_fragment_to_ir(expr, schema, *, anchors=None, location=None,
                                            module_aliases=None):
    """Compile given CaosQL expression fragment into Caos IR"""

    tree = parser.parse_fragment(expr)
    trans = CaosQLCompiler(schema, module_aliases)
    return trans.transform_fragment(tree, (), anchors=anchors,
                                    location=location)


@debug.debug
def compile_to_ir(expr, schema, *, anchors=None, arg_types=None,
                                   security_context=None,
                                   module_aliases=None):
    """Compile given CaosQL statement into Caos IR"""

    """LOG [caosql.compile] CaosQL TEXT:
    print(expr)
    """
    tree = parser.parse(expr, module_aliases)

    """LOG [caosql.compile] CaosQL AST:
    from metamagic.utils import markup
    markup.dump(tree)
    """
    trans = CaosQLCompiler(schema, module_aliases)

    ir = trans.transform(tree, arg_types, module_aliases=module_aliases,
                         anchors=anchors, security_context=security_context)

    """LOG [caosql.compile] Caos IR:
    from metamagic.utils import markup
    markup.dump(ir)
    """

    return ir
