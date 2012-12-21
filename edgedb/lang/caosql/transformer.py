##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import itertools
import operator

from metamagic.caos import types as caos_types
from metamagic.caos import tree
from metamagic.caos import name as caos_name
from metamagic.caos import utils as caos_utils
from metamagic.caos.caosql import ast as qlast
from metamagic.caos.caosql import errors
from metamagic.caos.caosql import parser as caosql_parser
from metamagic.utils import ast


class ParseContextLevel(object):
    def __init__(self, prevlevel=None, mode=None):
        if prevlevel is not None:
            if mode == ParseContext.SUBQUERY:
                self.graph = None
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
                self.resolve_computables = prevlevel.resolve_computables
                self.cge_map = prevlevel.cge_map.copy()
            else:
                self.graph = prevlevel.graph
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
                self.resolve_computables = prevlevel.resolve_computables
                self.cge_map = prevlevel.cge_map
        else:
            self.graph = None
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
            self.resolve_computables = True
            self.cge_map = {}

    def genalias(self, hint=None):
        if hint is None:
            hint = 'a'

        if hint not in self.aliascnt:
            self.aliascnt[hint] = 1
        else:
            self.aliascnt[hint] += 1

        alias = hint + str(self.aliascnt[hint])

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


class CaosqlReverseTransformer(tree.transformer.TreeTransformer):
    def transform(self, caos_tree):
        return self._process_expr(caos_tree)

    def _pathspec_from_record(self, expr):
        pathspec = []

        if expr.linkprop_xvalue:
            if isinstance(expr.elements[0], tree.ast.Record):
                pathspec = self._pathspec_from_record(expr.elements[0])
            else:
                noderef = self._process_expr(expr.elements[0])
                pathspec = [qlast.SelectPathSpecNode(expr=noderef.steps[-1])]

            for propref in expr.elements[1].elements:
                if propref.ptr_proto.get_loading_behaviour() == caos_types.EagerLoading:
                    continue

                propref = self._process_expr(propref)
                pathspec.append(qlast.SelectPathSpecNode(expr=propref))

        else:
            ptr_iters = set()

            for el in expr.elements:
                if isinstance(el, tree.ast.MetaRef):
                    continue

                elif isinstance(el, (tree.ast.AtomicRefSimple, tree.ast.LinkPropRefSimple)):
                    rlink = el.rlink if isinstance(el, tree.ast.AtomicRefSimple) else el.ref

                    trigger = rlink.pathspec_trigger
                    if trigger is None:
                        continue

                    if isinstance(trigger, tree.ast.PointerIteratorPathSpecTrigger):
                        ptr_iters.add(frozenset(trigger.filters.items()))
                    elif isinstance(trigger, tree.ast.ExplicitPathSpecTrigger):
                        refpath = self._process_expr(el)
                        sitem = qlast.SelectPathSpecNode(expr=refpath.steps[-1])
                    else:
                        msg = 'unexpected pathspec trigger in record ref: {!r}'.format(trigger)
                        raise ValueError(msg)

                elif isinstance(el, tree.ast.SubgraphRef):
                    rlink = el.rlink

                    trigger = rlink.pathspec_trigger
                    if trigger is None:
                        continue
                    elif isinstance(trigger, tree.ast.PointerIteratorPathSpecTrigger):
                        ptr_iters.add(frozenset(trigger.filters.items()))
                        continue
                    elif not isinstance(trigger, tree.ast.ExplicitPathSpecTrigger):
                        msg = 'unexpected pathspec trigger in record ref: {!r}'.format(trigger)
                        raise ValueError(msg)

                    target = el.rlink.target.concept.name
                    target = qlast.PrototypeRefNode(name=target.name, module=target.module)

                    refpath = qlast.LinkNode(name=el.rlink.link_proto.normal_name().name,
                                             namespace=el.rlink.link_proto.normal_name().module,
                                             target=target)
                    refpath = qlast.LinkExprNode(expr=refpath)
                    sitem = qlast.SelectPathSpecNode(expr=refpath)
                    rec = el.ref.selector[0].expr

                    if isinstance(rec, tree.ast.Record):
                        sitem.pathspec = self._pathspec_from_record(rec)

                    elif isinstance(rec, tree.ast.LinkPropRefSimple):
                        proprefpath = self._process_expr(rec)
                        sitem = qlast.SelectPathSpecNode(expr=proprefpath.steps[-1])

                    else:
                        raise ValueError('unexpected node in subgraph ref: {!r}'.format(rec))

                elif isinstance(el, tree.ast.Record):
                    rlink = el.rlink

                    trigger = rlink.pathspec_trigger

                    if trigger is None:
                        continue
                    elif isinstance(trigger, tree.ast.PointerIteratorPathSpecTrigger):
                        ptr_iters.add(frozenset(trigger.filters.items()))
                        continue
                    elif not isinstance(trigger, tree.ast.ExplicitPathSpecTrigger):
                        msg = 'unexpected pathspec trigger in record ref: {!r}'.format(trigger)
                        raise ValueError(msg)

                    target = el.concept.name
                    target = qlast.PrototypeRefNode(name=target.name, module=target.module)

                    refpath = qlast.LinkNode(name=el.rlink.link_proto.normal_name().name,
                                             namespace=el.rlink.link_proto.normal_name().module,
                                             target=target)
                    refpath = qlast.LinkExprNode(expr=refpath)
                    sitem = qlast.SelectPathSpecNode(expr=refpath)
                    sitem.pathspec = self._pathspec_from_record(el)

                else:
                    raise ValueError('unexpected node in record: {!r}'.format(el))

                pathspec.append(sitem)

            for ptr_iter in ptr_iters:
                filters = []
                for prop, val in ptr_iter:
                    flt = qlast.PointerGlobFilter(property=prop, value=val, any=val is None)
                    filters.append(flt)

                pathspec.append(qlast.PointerGlobNode(filters=filters))

        return pathspec

    def _process_expr(self, expr):
        if isinstance(expr, tree.ast.GraphExpr):
            result = qlast.SelectQueryNode()

            if expr.generator and not isinstance(expr.generator, tree.ast.Path):
                result.where = self._process_expr(expr.generator)
            else:
                result.where = None
            result.groupby = [self._process_expr(e) for e in expr.grouper]
            result.orderby = [self._process_expr(e) for e in expr.sorter]
            result.targets = [self._process_expr(e) for e in expr.selector]

            if expr.limit is not None:
                result.limit = self._process_expr(expr.limit)

            if expr.offset is not None:
                result.offset = self._process_expr(expr.offset)

        elif isinstance(expr, tree.ast.InlineFilter):
            result = self._process_expr(expr.expr)

        elif isinstance(expr, tree.ast.InlinePropFilter):
            result = self._process_expr(expr.expr)

        elif isinstance(expr, tree.ast.Constant):
            result = qlast.ConstantNode(value=expr.value, index=expr.index)

        elif isinstance(expr, tree.ast.SelectorExpr):
            result = qlast.SelectExprNode(expr=self._process_expr(expr.expr), alias=expr.name)

        elif isinstance(expr, tree.ast.SortExpr):
            result = qlast.SortExprNode(path=self._process_expr(expr.expr),
                                        direction=expr.direction,
                                        nones_order=expr.nones_order)

        elif isinstance(expr, tree.ast.FunctionCall):
            args = [self._process_expr(arg) for arg in expr.args]
            result = qlast.FunctionCallNode(func=expr.name, args=args)

        elif isinstance(expr, tree.ast.Record):
            if expr.rlink:
                result = self._process_expr(expr.rlink)
            else:
                path = qlast.PathNode()
                step = qlast.PathStepNode(expr=expr.concept.name.name,
                                          namespace=expr.concept.name.module)
                path.steps.append(step)
                path.pathspec = self._pathspec_from_record(expr)
                result = path

        elif isinstance(expr, tree.ast.UnaryOp):
            operand = self._process_expr(expr.expr)
            result = qlast.UnaryOpNode(op=expr.op, operand=operand)

        elif isinstance(expr, tree.ast.BinOp):
            left = self._process_expr(expr.left)
            right = self._process_expr(expr.right)
            result = qlast.BinOpNode(left=left, op=expr.op, right=right)

        elif isinstance(expr, tree.ast.ExistPred):
            result = qlast.ExistsPredicateNode(expr=self._process_expr(expr.expr))

        elif isinstance(expr, tree.ast.MetaRef):
            typstep = qlast.TypeRefNode(expr=self._process_expr(expr.ref))
            refstep = qlast.LinkExprNode(expr=qlast.LinkNode(name=expr.name))
            result = qlast.PathNode(steps=[typstep, refstep])

        elif isinstance(expr, tree.ast.AtomicRefSimple):
            path = self._process_expr(expr.ref)
            link = qlast.LinkNode(name=expr.name.name, namespace=expr.name.module)
            link = qlast.LinkExprNode(expr=link)
            path.steps.append(link)
            result = path

        elif isinstance(expr, tree.ast.AtomicRefExpr):
            result = self._process_expr(expr.expr)

        elif isinstance(expr, tree.ast.EntitySet):
            links = []

            while expr.rlink:
                linknode = expr.rlink
                linkproto = linknode.link_proto
                lname = linkproto.normal_name()

                target = linknode.target.concept.name
                target = qlast.PrototypeRefNode(name=target.name, module=target.module)
                link = qlast.LinkNode(name=lname.name, namespace=lname.module,
                                      direction=linknode.direction, target=target)
                link = qlast.LinkExprNode(expr=link)
                links.append(link)

                expr = expr.rlink.source

            path = qlast.PathNode()
            step = qlast.PathStepNode(expr=expr.concept.name.name,
                                      namespace=expr.concept.name.module)
            path.steps.append(step)
            path.steps.extend(reversed(links))

            result = path

        elif isinstance(expr, tree.ast.Disjunction):
            paths = list(expr.paths)
            if len(paths) == 1:
                result = self._process_expr(paths[0])
            else:
                assert False, "path combinations are not supported yet"

        elif isinstance(expr, tree.ast.SubgraphRef):
            result = self._process_expr(expr.ref)

        elif isinstance(expr, tree.ast.LinkPropRefSimple):
            path = self._process_expr(expr.ref)
            if expr.name != 'metamagic.caos.builtins.target':
                # Make sure its not a multiatom
                link = qlast.LinkNode(name=expr.name.name, namespace=expr.name.module,
                                      type='property')
                link = qlast.LinkPropExprNode(expr=link)
                path.steps.append(link)

            result = path

        elif isinstance(expr, tree.ast.LinkPropRefExpr):
            result = self._process_expr(expr.expr)

        elif isinstance(expr, tree.ast.EntityLink):
            if expr.source:
                path = self._process_expr(expr.source)
            else:
                path = qlast.PathNode()

            linkproto = expr.link_proto
            lname = linkproto.normal_name()

            if expr.target:
                target = expr.target.concept.name
                target = qlast.PrototypeRefNode(name=target.name, module=target.module)
            else:
                target = None

            if path.steps:
                link = qlast.LinkNode(name=lname.name, namespace=lname.module, target=target)
                link = qlast.LinkExprNode(expr=link)
            else:
                link = qlast.LinkNode(name=lname.name, namespace=lname.module, target=target)

            path.steps.append(link)
            result = path

        elif isinstance(expr, tree.ast.Sequence):
            elements = [self._process_expr(e) for e in expr.elements]
            result = qlast.SequenceNode(elements=elements)
            result = self.process_sequence(result)

        elif isinstance(expr, tree.ast.TypeCast):
            result = qlast.TypeCastNode(expr=self._process_expr(expr.expr),
                                        type=expr.type.name)

        else:
            assert False, "Unexpected expression type: %r" % expr

        return result


class CaosqlTreeTransformer(tree.transformer.TreeTransformer):
    def __init__(self, proto_schema, module_aliases=None):
        self.proto_schema = proto_schema
        self.module_aliases = module_aliases
        self.parser = caosql_parser.CaosQLParser()

    def _init_context(self, arg_types, module_aliases, anchors):
        self.context = context = ParseContext()
        self.context.current.proto_schema = self.proto_schema
        self.context.current.module_aliases = module_aliases or self.module_aliases
        if self.context.current.module_aliases:
            self.context.current.namespaces.update(self.context.current.module_aliases)

        if anchors:
            self._populate_anchors(context, anchors)
        return context

    def transform(self, caosql_tree, arg_types, module_aliases=None, anchors=None):
        context = self._init_context(arg_types, module_aliases, anchors)
        stree = self._transform_select(context, caosql_tree, arg_types)
        self.apply_fixups(stree)
        self.apply_rewrites(stree)
        return stree

    def transform_fragment(self, caosql_tree, arg_types, module_aliases=None, anchors=None,
                                 location=None, resolve_computables=True):
        context = self._init_context(arg_types, module_aliases, anchors)
        context.current.location = location or 'generator'
        context.current.resolve_computables = resolve_computables
        stree = self._process_expr(context, caosql_tree)
        return stree

    def normalize_refs(self, caosql_tree, module_aliases):
        self.context = context = ParseContext()
        self.context.current.proto_schema = self.proto_schema
        self.context.current.module_aliases = module_aliases or self.module_aliases
        return self._normalize_refs(context, caosql_tree)

    def _populate_anchors(self, context, anchors):
        for anchor, proto in anchors.items():
            if isinstance(proto, tree.ast.EntitySet):
                step = proto
            elif isinstance(proto, caos_types.ProtoConcept):
                step = tree.ast.EntitySet()
                step.concept = proto
                step.id = tree.transformer.LinearPath([step.concept])
                step.anchor = anchor
                # XXX
                # step.users =
            else:
                step = tree.ast.EntityLink(link_proto=proto)
                step.anchor = anchor

            context.current.anchors[anchor] = step

    def _transform_select(self, context, caosql_tree, arg_types):
        self.arg_types = arg_types or {}

        graph = context.current.graph = tree.ast.GraphExpr()

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
                graph.cges.append(tree.ast.CommonGraphExpr(expr=_cge, alias=cge.alias))

        graph.generator = self._process_select_where(context, caosql_tree.where)

        graph.grouper = self._process_grouper(context, caosql_tree.groupby)
        if graph.grouper:
            groupgraph = tree.ast.Disjunction(paths=frozenset(graph.grouper))
            context.current.groupprefixes = self.extract_prefixes(groupgraph)

        graph.selector = self._process_select_targets(context, caosql_tree.targets)
        graph.sorter = self._process_sorter(context, caosql_tree.orderby)
        if caosql_tree.offset:
            graph.offset = tree.ast.Constant(value=caosql_tree.offset.value,
                                             index=caosql_tree.offset.index,
                                             type=context.current.proto_schema.get('int'))

        if caosql_tree.limit:
            graph.limit = tree.ast.Constant(value=caosql_tree.limit.value,
                                             index=caosql_tree.limit.index,
                                             type=context.current.proto_schema.get('int'))

        # Merge selector and sorter disjunctions first
        paths = [s.expr for s in graph.selector] + [s.expr for s in graph.sorter] + \
                [s for s in graph.grouper]
        union = tree.ast.Disjunction(paths=frozenset(paths))
        self.flatten_and_unify_path_combination(union, deep=True, merge_filters=True)

        # Merge the resulting disjunction with generator conjunction
        if graph.generator:
            paths = [graph.generator] + list(union.paths)
        else:
            paths = union.paths
        union = tree.ast.Disjunction(paths=frozenset(paths))
        self.flatten_and_unify_path_combination(union, deep=True, merge_filters=True)

        # Reorder aggregate expressions so that all of them appear as the first sub-tree in
        # the generator expression.
        #
        if graph.generator:
            self.reorder_aggregates(graph.generator)

        graph.result_types = self.get_selector_types(graph.selector, self.proto_schema)
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

    def _normalize_refs(self, context, expr):
        if isinstance(expr, qlast.SelectQueryNode):
            for attr in ('targets', 'groupby', 'orderby'):
                items = getattr(expr, attr)
                if items:
                    for item in items:
                        self._normalize_refs(context, item)

            if expr.where:
                self._normalize_refs(context, expr.where)

        elif isinstance(expr, qlast.SelectExprNode):
            self._normalize_refs(context, expr.expr)

        elif isinstance(expr, qlast.SortExprNode):
            self._normalize_refs(context, expr.path)

        elif isinstance(expr, qlast.UnaryOpNode):
            self._normalize_refs(context, expr.operand)

        elif isinstance(expr, qlast.BinOpNode):
            self._normalize_refs(context, expr.left)
            self._normalize_refs(context, expr.right)

        elif isinstance(expr, qlast.PathNode):
            for node in expr.steps:
                self._normalize_refs(context, node)

        elif isinstance(expr, qlast.PathStepNode):
            if expr.expr != 'self':
                expr.namespace = context.current.module_aliases.get(expr.namespace, expr.namespace)

        elif isinstance(expr, qlast.LinkExprNode):
            self._normalize_refs(context, expr.expr)

        elif isinstance(expr, qlast.LinkNode):
            if expr.namespace:
                expr.namespace = context.current.module_aliases.get(expr.namespace, expr.namespace)

        elif isinstance(expr, qlast.LinkPropExprNode):
            pass

        elif isinstance(expr, qlast.ConstantNode):
            pass

        elif isinstance(expr, qlast.SequenceNode):
            for e in expr.elements:
                self._normalize_refs(context, e)

        elif isinstance(expr, qlast.FunctionCallNode):
            for arg in expr.args:
                self._normalize_refs(context, arg)

        elif isinstance(expr, qlast.PrototypeRefNode):
            pass

        else:
            assert False, "Unexpected expr: %s" % expr

        return expr


    def _process_expr(self, context, expr):
        node = None

        if isinstance(expr, qlast.SelectQueryNode):
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
            node = tree.ast.SubgraphRef(ref=node, name=refname)

        elif isinstance(expr, qlast.BinOpNode):
            left = self._process_expr(context, expr.left)
            right = self._process_expr(context, expr.right)

            # The entityref_to_record transform must be reverted for typecheck ops
            if isinstance(expr.op, ast.ops.EquivalenceOperator) \
                    and isinstance(left, tree.ast.Record) \
                    and isinstance(right, tree.ast.Constant) \
                    and (isinstance(right.type, caos_types.PrototypeClass) or
                         isinstance(right.type, tuple) and
                         isinstance(right.type[1], caos_types.PrototypeClass)):
                left = left.elements[0].ref
            node = self.process_binop(left, right, expr.op)

        elif isinstance(expr, qlast.PathNode):
            node = self._process_path(context, expr)
            if context.current.groupprefixes and context.current.location in ('sorter', 'selector')\
                                             and not context.current.in_aggregate:
                for p in node.paths:
                    if p.id not in context.current.groupprefixes:
                        err = ('node reference "%s" must appear in the GROUP BY expression or '
                               'used in an aggregate function ') % p.id
                        raise errors.CaosQLError(err)

            if (context.current.location not in {'generator', 'selector'} \
                            and not context.current.in_func_call) or context.current.in_aggregate:
                node = self.entityref_to_record(node, self.proto_schema)

        elif isinstance(expr, qlast.ConstantNode):
            node = self._process_constant(context, expr)

        elif isinstance(expr, qlast.SequenceNode):
            elements=[self._process_expr(context, e) for e in expr.elements]
            node = tree.ast.Sequence(elements=elements)
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
                args = [self._process_expr(context, a) for a in expr.args]
                agg_sort = [tree.ast.SortExpr(expr=self._process_expr(context, e.path),
                                              direction=e.direction)
                            for e in expr.agg_sort] if expr.agg_sort else []
                node = tree.ast.FunctionCall(name=expr.func, args=args, agg_sort=agg_sort)
                node = self.process_function_call(node)

        elif isinstance(expr, qlast.PrototypeRefNode):
            if expr.module:
                name = caos_name.Name(name=expr.name, module=expr.module)
            else:
                name = expr.name
            node = self.proto_schema.get(name=name, module_aliases=context.current.namespaces,
                                         type=caos_types.ProtoNode)

            node = tree.ast.Constant(value=(node,), type=(list, node.__class__))

        elif isinstance(expr, qlast.UnaryOpNode):
            node = self.process_unaryop(self._process_expr(context, expr.operand), expr.op)

        elif isinstance(expr, qlast.ExistsPredicateNode):
            subquery = self._process_expr(context, expr.expr)
            if isinstance(subquery, tree.ast.SubgraphRef):
                subquery.ref.referrers.append('exists')
            node = tree.ast.ExistPred(expr=subquery)

        elif isinstance(expr, qlast.TypeCastNode):
            if isinstance(expr.type, tuple):
                typ = expr.type[1]
            else:
                typ = expr.type

            typ = context.current.proto_schema.get(typ, module_aliases=context.current.namespaces)

            if isinstance(expr.type, tuple):
                typ = (expr.type[0], typ)

            node = tree.ast.TypeCast(expr=self._process_expr(context, expr.expr), type=typ)

        else:
            assert False, "Unexpected expr: %s" % expr

        return node

    def _process_constant(self, context, expr):
        if expr.index is not None:
            type = self.arg_types.get(expr.index)
            if type is not None:
                type = caos_types.normalize_type(type, self.proto_schema)
            node = tree.ast.Constant(value=expr.value, index=expr.index, type=type)
            context.current.arguments[expr.index] = type
        else:
            type = caos_types.normalize_type(expr.value.__class__, self.proto_schema)
            node = tree.ast.Constant(value=expr.value, index=expr.index, type=type)

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
                            glob_specs.append(tree.ast.PtrPathSpec(ptr_proto=ptr))

                elif ptrspec.type == 'property':
                    if rlink_proto is None:
                        msg = 'link properties are not available at this point in path'
                        raise errors.CaosQLError(msg)

                    for ptr_name, ptr in rlink_proto.pointers.items():
                        if ptr.is_special_pointer():
                            continue

                        if not filter_exprs or all(((f[0](ptr) == f[1]) for f in filter_exprs)):
                            glob_specs.append(tree.ast.PtrPathSpec(ptr_proto=ptr))

                else:
                    msg = 'unexpected pointer spec type'
                    raise errors.CaosQLError(msg)

                result = self._merge_pathspecs(result, glob_specs, target_most_generic=False)
            else:
                ptrname = (ptrspec.expr.namespace, ptrspec.expr.name)

                if ptrspec.expr.type == 'property':
                    if rlink_proto is None:
                        msg = 'link properties are not available at this point in path'
                        raise errors.CaosQLError(msg)

                    ptrsource = rlink_proto
                    ptrtype = caos_types.ProtoLinkProperty
                else:
                    ptrsource = source
                    ptrtype = caos_types.ProtoLink

                if ptrspec.expr.target is not None:
                    target_name = (ptrspec.expr.target.module, ptrspec.expr.target.name)
                else:
                    target_name = None

                ptr_direction = ptrspec.expr.direction or caos_types.OutboundDirection
                ptr = self._resolve_ptr(context, ptrsource, ptrname, ptr_direction,
                                        ptr_type=ptrtype, target=target_name)

                if ptr_direction == caos_types.OutboundDirection:
                    ptr = next(iter(ptr[1]))
                else:
                    ptr = next(iter(ptr[2]))

                if ptrspec.recurse is not None:
                    recurse = self._process_constant(context, ptrspec.recurse)
                else:
                    recurse = None

                if target_name is not None:
                    if target_name[0]:
                        target_fq_name = '{}.{}'.format(*target_name)
                    else:
                        target_fq_name = target_name[1]
                    modaliases = context.current.namespaces
                    target_proto = context.current.proto_schema.get(target_fq_name,
                                                                    module_aliases=modaliases)
                else:
                    target_proto = ptr.target

                node = tree.ast.PtrPathSpec(ptr_proto=ptr, ptr_direction=ptr_direction,
                                            recurse=recurse, target_proto=target_proto)

                if ptrspec.pathspec is not None:
                    node.pathspec = self._process_pathspec(context, target_proto,
                                                           ptr, ptrspec.pathspec)
                else:
                    node.pathspec = None

                result = self._merge_pathspecs(result, [node], target_most_generic=False)

        self._normalize_pathspec_recursion(result, source, context.current.proto_schema)

        return result

    def _process_path(self, context, path):
        anchors = context.current.anchors
        tips = {}
        typeref = None

        for i, node in enumerate(path.steps):
            anchor = None
            link_anchor = None

            if isinstance(node, (qlast.PathNode, qlast.PathStepNode)):
                if isinstance(node, qlast.PathNode):
                    if len(node.steps) > 1:
                        raise errors.CaosQLError('unsupported subpath expression')

                    anchor = node.var.name if node.var else None
                    link_anchor = node.lvar.name if node.lvar else None

                    if anchor in anchors:
                        raise errors.CaosQLError('duplicate anchor: %s' % anchor)

                    tip = self._get_path_tip(node)

                elif isinstance(node, qlast.PathStepNode):
                    if node.expr in anchors and i == 0:
                        refnode = anchors[node.expr]

                        if refnode:
                            if isinstance(refnode, tree.ast.Disjunction):
                                tips = {}
                                for p in refnode.paths:
                                    concept = getattr(p, 'concept', None)
                                    path_copy = self.copy_path(p)
                                    path_copy.users.add(context.current.location)
                                    if concept in tips:
                                        tips[concept].add(path_copy)
                                    else:
                                        tips[concept] = {path_copy}
                            else:
                                path_copy = self.copy_path(refnode)
                                path_copy.users.add(context.current.location)
                                concept = getattr(refnode, 'concept', None)
                                tips = {concept: {path_copy}}
                        continue

                    elif node.expr in context.current.cge_map and i == 0:
                        cge = context.current.cge_map[node.expr]
                        tips = {None: {cge}}
                        continue

                    tip = node

                elif isinstance(node, qlast.TypeRefNode):
                    tip = node
            else:
                tip = node

            tip_is_link = tips and next(iter(tips.keys())) is None

            if isinstance(tip, qlast.PathStepNode):

                proto = self._normalize_concept(context, tip.expr, tip.namespace)

                if isinstance(proto, (caos_types.ProtoConcept, caos_types.ProtoAtom)):
                    step = tree.ast.EntitySet()
                    step.concept = proto
                    tips = {step.concept: {step}}
                    step.id = tree.transformer.LinearPath([step.concept])
                    step.anchor = anchor

                    step.users.add(context.current.location)
                else:
                    step = tree.ast.EntityLink(link_proto=proto)
                    tips = {None: {step}}

                if anchor:
                    anchors[anchor] = step

            elif isinstance(tip, qlast.TypeRefNode):
                typeref = self._process_path(context, tip.expr)
                if isinstance(typeref, tree.ast.PathCombination):
                    if len(typeref.paths) > 1:
                        raise errors.CaosQLError("type() argument must not be a path combination")
                    typeref = next(iter(typeref.paths))

            elif isinstance(tip, qlast.LinkExprNode) and typeref:
                tips = {None: {tree.ast.MetaRef(ref=typeref, name=tip.expr.name)}}

            elif isinstance(tip, qlast.LinkExprNode) and not tip_is_link and not typeref:
                # LinkExprNode
                link_expr = tip.expr
                link_target = None

                if isinstance(link_expr, qlast.LinkNode):
                    direction = link_expr.direction or caos_types.OutboundDirection
                    if link_expr.target:
                        link_target = self._normalize_concept(context, link_expr.target.name,
                                                              link_expr.target.module)
                else:
                    raise errors.CaosQLError("complex link expressions are not supported yet")

                newtips = {}
                sources = set()

                linkname = (link_expr.namespace, link_expr.name)

                for concept, tip in tips.items():
                    try:
                        ptr_resolution = self._resolve_ptr(context, concept, linkname, direction,
                                                                    target=link_target)
                    except errors.CaosQLReferenceError:
                        continue

                    sources, outbound, inbound = ptr_resolution

                    seen_concepts = seen_atoms = False

                    if len(sources) > 1:
                        newtip = set()
                        for t in tip:
                            if t.rlink:
                                paths = set(t.rlink.source.disjunction.paths)

                                for source in sources:
                                    t_id = tree.transformer.LinearPath(t.id[:-2])
                                    t_id.add(t.rlink.link_proto, t.rlink.direction, source)

                                    new_target = tree.ast.EntitySet()
                                    new_target.concept = source
                                    new_target.id = t_id
                                    new_target.users = t.users.copy()

                                    newtip.add(new_target)

                                    link = tree.ast.EntityLink(source=t.rlink.source,
                                                               target=new_target,
                                                               link_proto=t.rlink.link_proto,
                                                               direction=t.rlink.direction)

                                    new_target.rlink = link
                                    paths.add(link)

                                paths.remove(t.rlink)
                                t.rlink.source.disjunction.paths = frozenset(paths)
                            else:
                                newtip.add(t)

                            tip = newtip

                    links = {caos_types.OutboundDirection: outbound,
                             caos_types.InboundDirection: inbound}

                    if direction == caos_types.OutboundDirection:
                        links = outbound
                    else:
                        links = inbound

                    if len(links) > 1:
                        modaliases = context.current.namespaces

                        if not linkname[0]:
                            lname = (concept.name.module, linkname[1])
                        else:
                            lname = linkname

                        if lname[1] == '%':
                            lname = ('metamagic.caos.builtins', 'link')

                        link_item = self.proto_schema.get('.'.join(lname),
                                                          module_aliases=modaliases)

                        targets = {l.get_far_endpoint(direction) for l in links}
                        link_target = link_item.create_common_target(self.proto_schema, targets,
                                                                     minimize_by='most_generic')

                    else:
                        try:
                            link_item = list(links)[0]
                        except IndexError:
                            continue

                    if link_target is not None:
                        target = link_target
                    else:
                        target = link_item.get_far_endpoint(direction)

                    assert target

                    if isinstance(link_item, caos_types.ProtoComputable) and \
                            context.current.resolve_computables:
                        newtips[target] = {self.link_computable(link_item, tip)}
                        continue

                    link_proto = link_item
                    link_item = self.proto_schema.get(link_item.normal_name())

                    anchor_links = []

                    if isinstance(target, caos_types.ProtoConcept):
                        if seen_atoms:
                            raise errors.CaosQLError('path expression results in an '
                                                     'invalid atom/concept mix')
                        seen_concepts = True

                        for t in tip:
                            target_set = tree.ast.EntitySet()
                            target_set.concept = target
                            target_set.id = tree.transformer.LinearPath(t.id)
                            target_set.id.add(link_item, direction,
                                              target_set.concept)
                            target_set.users.add(context.current.location)

                            link = tree.ast.EntityLink(source=t, target=target_set,
                                                       direction=direction,
                                                       link_proto=link_proto,
                                                       anchor=link_anchor,
                                                       users={context.current.location})

                            t.disjunction.update(link)
                            target_set.rlink = link

                            anchor_links.append(link)

                            if target in newtips:
                                newtips[target].add(target_set)
                            else:
                                newtips[target] = {target_set}

                    elif isinstance(target, caos_types.ProtoAtom):
                        if seen_concepts:
                            raise errors.CaosQLError('path expression results in an '
                                                     'invalid atom/concept mix')
                        seen_atoms = True

                        newtips[target] = set()
                        for t in tip:
                            target_set = tree.ast.EntitySet()
                            target_set.concept = target
                            target_set.id = tree.transformer.LinearPath(t.id)
                            target_set.id.add(link_proto, direction,
                                              target_set.concept)
                            target_set.users.add(context.current.location)

                            link = tree.ast.EntityLink(source=t, target=target_set,
                                                       link_proto=link_proto,
                                                       direction=direction,
                                                       anchor=link_anchor,
                                                       users={context.current.location})

                            target_set.rlink = link

                            anchor_links.append(link)

                            atomref_id = tree.transformer.LinearPath(t.id)
                            atomref_id.add(link_item, direction, target)

                            if not link_proto.singular():
                                ptr_name = caos_name.Name('metamagic.caos.builtins.target')
                                ptr_proto = link_proto.pointers[ptr_name]
                                atomref = tree.ast.LinkPropRefSimple(name=ptr_name,
                                                                     ref=link,
                                                                     id=atomref_id,
                                                                     ptr_proto=ptr_proto)
                                t.disjunction.update(link)
                            else:
                                atomref = tree.ast.AtomicRefSimple(name=link_item.normal_name(),
                                                                   ref=t, id=atomref_id,
                                                                   rlink=link,
                                                                   ptr_proto=link_proto)
                            newtips[target].add(atomref)

                    else:
                        assert False, 'unexpected link target type: %s' % target

                if not newtips:
                    source_name = ','.join(concept.name for concept in tips)
                    ptr_fqname = '.'.join(filter(None, linkname))
                    raise errors.CaosQLReferenceError('could not resolve "{}"."{}" pointer'\
                                                      .format(source_name, ptr_fqname))

                if anchor:
                    paths = itertools.chain.from_iterable(newtips.values())
                    anchors[anchor] = tree.ast.Disjunction(paths=frozenset(paths))

                if link_anchor:
                    if len(anchor_links) > 1:
                        anchors[link_anchor] = tree.ast.Disjunction(paths=frozenset(anchor_links))
                    else:
                        anchors[link_anchor] = anchor_links[0]

                tips = newtips

            elif isinstance(tip, qlast.LinkPropExprNode) or tip_is_link:
                # LinkExprNode
                link_expr = tip.expr

                newtips = {}

                tip = tips.get(None)

                if tip and isinstance(next(iter(tip)), tree.ast.GraphExpr):
                    subgraph = next(iter(tip))
                    subgraph.attrrefs.add(link_expr.name)
                    sgref = tree.ast.SubgraphRef(ref=subgraph, name=link_expr.name)
                    newtips = {None: {sgref}}
                else:
                    for concept, tip in tips.items():
                        for entset in tip:
                            if isinstance(entset, tree.ast.EntityLink):
                                link = entset
                                link_proto = link.link_proto
                                id = tree.transformer.LinearPath([None])
                                id.add(link_proto, caos_types.OutboundDirection, None)
                            elif isinstance(entset, tree.ast.LinkPropRefSimple):
                                link = entset.ref
                                id = entset.id
                            else:
                                link = entset.rlink
                                id = entset.id

                            link_proto = link.link_proto
                            prop_name = (link_expr.namespace, link_expr.name)
                            ptr_resolution = self._resolve_ptr(context, link_proto,
                                                               prop_name,
                                                               caos_types.OutboundDirection,
                                                               ptr_type=caos_types.ProtoLinkProperty)
                            sources, outbound, inbound = ptr_resolution

                            for prop_proto in outbound:
                                propref = tree.ast.LinkPropRefSimple(name=prop_proto.normal_name(),
                                                                     ref=link, id=id,
                                                                     ptr_proto=prop_proto)
                                newtips[prop_proto] = {propref}

                tips = newtips
            else:
                assert False, 'Unexpected path step expression: "%s"' % tip

        paths = itertools.chain.from_iterable(tips.values())
        return tree.ast.Disjunction(paths=frozenset(paths))

    def _resolve_ptr(self, context, source, ptr_name, direction, ptr_type=caos_types.ProtoLink,
                                                                 target=None):
        ptr_module, ptr_nqname = ptr_name

        if ptr_module:
            ptr_fqname = caos_name.Name(module=ptr_module, name=ptr_nqname)
            modaliases = context.current.namespaces
            pointer = self.proto_schema.get(ptr_fqname, module_aliases=modaliases, type=ptr_type)
            pointer_name = pointer.name
        else:
            pointer_name = ptr_fqname = ptr_nqname

        if target is not None and not isinstance(target, caos_types.ProtoNode):
            target_name = '.'.join(filter(None, target))
            modaliases = context.current.namespaces
            target = self.proto_schema.get(target_name, module_aliases=modaliases)

        if ptr_nqname == '%':
            pointer_name = self.proto_schema.get_root_class(ptr_type).name

        sources, outbound, inbound = source.resolve_pointer(self.proto_schema, pointer_name,
                                                            direction=direction,
                                                            look_in_children=True,
                                                            include_inherited=True)

        if not sources:
            msg = 'could not resolve "{}".{}"{}" pointer'.format(source.name, direction,
                                                                 ptr_fqname)
            raise errors.CaosQLReferenceError(msg)

        if target is not None:
            if direction == caos_types.OutboundDirection:
                ptrs = outbound
                link_end = 'target'
            else:
                ptrs = inbound
                link_end = 'source'

            flt = lambda p: target.issubclass(getattr(p, link_end))
            ptrs = tuple(filter(flt, ptrs))

            if direction == caos_types.OutboundDirection:
                outbound = ptrs
            else:
                inbound = ptrs

        return sources, outbound, inbound

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

                if isinstance(expr, tree.ast.Disjunction):
                    path = next(iter(expr.paths))
                    if isinstance(path, tree.ast.EntitySet):
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

                t = tree.ast.SelectorExpr(expr=expr, **params)
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
                    s = tree.ast.SortExpr(expr=expr, direction=sorter.direction,
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

    def link_computable(self, computable_proto, path_tip):
        path_tip = next(iter(path_tip))
        anchors = {'self': path_tip}
        caosql_tree = self.parser.parse(computable_proto.expression)
        result = self.transform_fragment(caosql_tree, (), anchors=anchors,
                                       location='computable')
        result.ptr_proto = computable_proto
        return result
