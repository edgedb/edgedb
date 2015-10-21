##
# Copyright (c) 2008-2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
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


class CaosqlExprShortener:
    def transform(self, caosql_tree):
        context = ParseContext()
        self._process_expr(context, caosql_tree)

        nses = []
        for alias, fq_name in context.current.namespaces.items():
            decl = qlast.NamespaceDeclarationNode(namespace=fq_name,
                                                  alias=alias)
            nses.append(decl)

        if caosql_tree.namespaces is not None:
            caosql_tree.namespaces[:] = nses
        else:
            caosql_tree.namespaces = nses

        return caosql_tree

    def _process_expr(self, context, expr):
        if isinstance(expr, qlast.SelectQueryNode):
            if expr.namespaces:
                context.current.namespaces.update(
                    (ns.alias, ns.namespace) for ns in expr.namespaces)

            if expr.where:
                self._process_expr(context, expr.where)

            if expr.groupby:
                for gb in expr.groupby:
                    self._process_expr(context, gb)

            for tgt in expr.targets:
                self._process_expr(context, tgt.expr)

            if expr.orderby:
                for sort in expr.orderby:
                    self._process_expr(context, sort.path)

            if expr.offset:
                self._process_expr(context, expr.offset)

            if expr.limit:
                self._process_expr(context, expr.limit)

        elif isinstance(expr, qlast.UpdateQueryNode):
            if expr.namespaces:
                context.current.namespaces.update(
                    (ns.alias, ns.namespace) for ns in expr.namespaces)

            self._process_expr(context, expr.subject)

            if expr.where:
                self._process_expr(context, expr.where)

            for v in expr.values:
                self._process_expr(context, v.expr)
                self._process_expr(context, v.value)

            for tgt in expr.targets:
                self._process_expr(context, tgt.expr)

        elif isinstance(expr, qlast.DeleteQueryNode):
            if expr.namespaces:
                context.current.namespaces.update(
                    (ns.alias, ns.namespace) for ns in expr.namespaces)

            self._process_expr(context, expr.subject)

            if expr.where:
                self._process_expr(context, expr.where)

            for tgt in expr.targets:
                self._process_expr(context, tgt.expr)

        elif isinstance(expr, qlast.SubqueryNode):
            with context(ParseContext.SUBQUERY):
                self._process_expr(context, expr.expr)

        elif isinstance(expr, qlast.PredicateNode):
            self._process_expr(context, expr.expr)

        elif isinstance(expr, qlast.BinOpNode):
            self._process_expr(context, expr.left)
            self._process_expr(context, expr.right)

        elif isinstance(expr, qlast.FunctionCallNode):
            for arg in expr.args:
                self._process_expr(context, arg)

            if expr.agg_sort:
                for sort in expr.agg_sort:
                    self._process_expr(context, sort.path)

            if expr.window:
                self._process_expr(context, expr.window)

        elif isinstance(expr, qlast.WindowSpecNode):
            if expr.orderby:
                for orderby in expr.orderby:
                    self._process_expr(context, orderby.path)

            if expr.partition:
                for partition in expr.partition:
                    self._process_expr(context, partition)

        elif isinstance(expr, qlast.UnaryOpNode):
            self._process_expr(context, expr.operand)

        elif isinstance(expr, qlast.PostfixOpNode):
            self._process_expr(context, expr.operand)

        elif isinstance(expr, qlast.SequenceNode):
            for el in expr.elements:
                self._process_expr(context, el)

        elif isinstance(expr, qlast.TypeCastNode):
            self._process_expr(context, expr.expr)
            self._process_expr(context, expr.type)

        elif isinstance(expr, qlast.TypeRefNode):
            self._process_expr(context, expr.expr)

        elif isinstance(expr, qlast.NoneTestNode):
            self._process_expr(context, expr.expr)

        elif isinstance(expr, qlast.PrototypeRefNode):
            if expr.module:
                expr.module = self._process_module_ref(
                                context, expr.module)

        elif isinstance(expr, qlast.PathNode):
            if expr.pathspec:
                self._process_pathspec(context, expr.pathspec)

            for step in expr.steps:
                self._process_expr(context, step)

        elif isinstance(expr, qlast.PathStepNode):
            if expr.namespace:
                expr.namespace = self._process_module_ref(
                                    context, expr.namespace)

        elif isinstance(expr, (qlast.LinkExprNode, qlast.LinkPropExprNode)):
            self._process_expr(context, expr.expr)

        elif isinstance(expr, qlast.LinkNode):
            if expr.namespace:
                expr.namespace = self._process_module_ref(
                                    context, expr.namespace)

            if expr.target:
                self._process_expr(context, expr.target)

    def _process_pathspec(self, context, pathspec):
        for spec in pathspec:
            if isinstance(spec, qlast.SelectPathSpecNode):
                if spec.where:
                    self._process_expr(context, spec.where)

                if spec.orderby:
                    for orderby in spec.orderby:
                        self._process_expr(context, orderby.path)

                self._process_expr(context, spec.expr)

                if spec.pathspec:
                    self._process_pathspec(context, spec.pathspec)

    def _process_module_ref(self, context, module):
        if module == 'metamagic.caos.builtins':
            return None

        if '.' in module:
            modmap = {v: k for k, v in context.current.namespaces.items()}
            try:
                alias = modmap[module]
            except KeyError:
                mhead, _, mtail = module.rpartition('.')
                if mtail == 'objects' and mhead:
                    # schemas are commonly in the form <module>.objects
                    mhead, _, mtail = mhead.rpartition('.')
                alias = context.current.genalias(hint=mtail)
                context.current.namespaces[alias] = module

            return alias
        else:
            return module


class ReverseParseContext:
    pass


class CaosqlReverseTransformer(tree.transformer.TreeTransformer):
    def transform(self, caos_tree, inline_anchors=False):
        context = ReverseParseContext()
        context.inline_anchors = inline_anchors
        return self._process_expr(context, caos_tree)

    def _pathspec_from_record(self, context, expr):
        pathspec = []

        if expr.linkprop_xvalue:
            if isinstance(expr.elements[0], tree.ast.LinkPropRefSimple):
                return []
            elif isinstance(expr.elements[0], tree.ast.Record):
                pathspec = self._pathspec_from_record(context, expr.elements[0])
            else:
                noderef = self._process_expr(context, expr.elements[0])
                pathspec = [qlast.SelectPathSpecNode(expr=noderef.steps[-1])]

            for propref in expr.elements[1].elements:
                if propref.ptr_proto.get_loading_behaviour() == caos_types.EagerLoading:
                    continue

                propref = self._process_expr(context, propref)
                pathspec.append(qlast.SelectPathSpecNode(expr=propref))

        else:
            ptr_iters = set()

            for el in expr.elements:
                if el.rewrite_original:
                    el = el.rewrite_original

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
                        refpath = self._process_expr(context, el)
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
                                             target=target, direction=el.rlink.direction)
                    refpath = qlast.LinkExprNode(expr=refpath)
                    sitem = qlast.SelectPathSpecNode(expr=refpath)
                    rec = el.ref.selector[0].expr

                    if isinstance(rec, tree.ast.Record):
                        sitem.pathspec = self._pathspec_from_record(context, rec)

                    elif isinstance(rec, tree.ast.LinkPropRefSimple):
                        proprefpath = self._process_expr(context, rec)
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
                                             target=target, direction=el.rlink.direction)
                    refpath = qlast.LinkExprNode(expr=refpath)
                    sitem = qlast.SelectPathSpecNode(expr=refpath)
                    sitem.pathspec = self._pathspec_from_record(context, el)

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

    def _process_path_combination_as_filter(self, context, expr):
        if isinstance(expr, tree.ast.Disjunction):
            op = ast.ops.OR
        else:
            op = ast.ops.AND

        operands = []

        for elem in expr.paths:
            if isinstance(elem, tree.ast.PathCombination):
                elem = self._process_path_combination_as_filter(context, elem)
            elif not isinstance(elem, tree.ast.Path):
                elem = self._process_expr(context, elem)
            else:
                elem = None

            if elem is not None:
                operands.append(elem)

        if len(operands) == 0:
            return None
        elif len(operands) == 1:
            return operands[0]
        else:
            result = qlast.BinOpNode(left=operands[0], right=operands[1], op=op)

            for operand in operands[2:]:
                result = qlast.BinOpNode(left=result, right=operand, op=op)

            return result

    def _is_none(self, context, expr):
        return (isinstance(expr, (tree.ast.Constant, qlast.ConstantNode))
                and expr.value is None and expr.index is None)

    def _process_function(self, context, expr):
        args = [self._process_expr(context, arg) for arg in expr.args]

        if expr.name == 'getslice':
            result = qlast.IndirectionNode(
                arg=args[0],
                indirection=[
                    qlast.SliceNode(
                        start=None if self._is_none(context, args[1]) else args[1],
                        stop=None if self._is_none(context, args[2]) else args[2],
                    )
                ]
            )
        elif expr.name == 'getitem':
            result = qlast.IndirectionNode(
                arg=args[0],
                indirection=[
                    qlast.IndexNode(
                        index=args[1]
                    )
                ]
            )
        else:
            result = qlast.FunctionCallNode(func=expr.name, args=args)

        return result

    def _process_expr(self, context, expr):
        if expr.rewrite_original:
            # Process original node instead of a rewrite
            return self._process_expr(context, expr.rewrite_original)
        elif expr.is_rewrite_product:
            # Skip all rewrite products
            return None

        if isinstance(expr, tree.ast.GraphExpr):
            result = qlast.SelectQueryNode()

            if expr.generator:
                if not isinstance(expr.generator, tree.ast.Path):
                    result.where = self._process_expr(context, expr.generator)
                elif isinstance(expr.generator, tree.ast.PathCombination):
                    result.where = self._process_path_combination_as_filter(context, expr.generator)
            else:
                result.where = None
            result.groupby = [self._process_expr(context, e) for e in expr.grouper]
            result.orderby = [self._process_expr(context, e) for e in expr.sorter]
            result.targets = [self._process_expr(context, e) for e in expr.selector]

            if expr.limit is not None:
                result.limit = self._process_expr(context, expr.limit)

            if expr.offset is not None:
                result.offset = self._process_expr(context, expr.offset)

        elif isinstance(expr, tree.ast.InlineFilter):
            result = self._process_expr(context, expr.expr)

        elif isinstance(expr, tree.ast.InlinePropFilter):
            result = self._process_expr(context, expr.expr)

        elif isinstance(expr, tree.ast.Constant):
            if expr.expr is not None:
                result = self._process_expr(context, expr.expr)
            else:
                value = expr.value
                index = expr.index

                if  (isinstance(value, collections.Container)
                                and not isinstance(value, (str, bytes))):

                    elements = []

                    for v in value:
                        if isinstance(v, caos_types.ProtoObject):
                            v = qlast.PrototypeRefNode(module=v.name.module, name=v.name.name)
                        elements.append(v)

                    result = qlast.SequenceNode(elements=elements)
                else:
                    result = qlast.ConstantNode(value=value, index=index)

        elif isinstance(expr, tree.ast.SelectorExpr):
            result = qlast.SelectExprNode(expr=self._process_expr(context, expr.expr),
                                          alias=expr.name)

        elif isinstance(expr, tree.ast.SortExpr):
            result = qlast.SortExprNode(path=self._process_expr(context, expr.expr),
                                        direction=expr.direction,
                                        nones_order=expr.nones_order)

        elif isinstance(expr, tree.ast.FunctionCall):
            result = self._process_function(context, expr)

        elif isinstance(expr, tree.ast.Record):
            if expr.rlink:
                result = self._process_expr(context, expr.rlink)
            else:
                path = qlast.PathNode()
                step = qlast.PathStepNode(expr=expr.concept.name.name,
                                          namespace=expr.concept.name.module)
                path.steps.append(step)
                path.pathspec = self._pathspec_from_record(context, expr)
                result = path

        elif isinstance(expr, tree.ast.UnaryOp):
            operand = self._process_expr(context, expr.expr)
            result = qlast.UnaryOpNode(op=expr.op, operand=operand)

        elif isinstance(expr, tree.ast.BinOp):
            left = self._process_expr(context, expr.left)
            right = self._process_expr(context, expr.right)

            if left is not None and right is not None:
                result = qlast.BinOpNode(left=left, op=expr.op, right=right)
            else:
                result = left or right

        elif isinstance(expr, tree.ast.ExistPred):
            result = qlast.ExistsPredicateNode(expr=self._process_expr(context, expr.expr))

        elif isinstance(expr, tree.ast.MetaRef):
            typstep = qlast.TypeRefNode(expr=self._process_expr(context, expr.ref))
            refstep = qlast.LinkExprNode(expr=qlast.LinkNode(name=expr.name))
            result = qlast.PathNode(steps=[typstep, refstep])

        elif isinstance(expr, tree.ast.AtomicRefSimple):
            path = self._process_expr(context, expr.ref)
            link = qlast.LinkNode(name=expr.name.name, namespace=expr.name.module)
            link = qlast.LinkExprNode(expr=link)
            path.steps.append(link)
            result = path

        elif isinstance(expr, tree.ast.AtomicRefExpr):
            result = self._process_expr(context, expr.expr)

        elif isinstance(expr, tree.ast.EntitySet):
            links = []

            while expr.rlink and (not expr.show_as_anchor or context.inline_anchors):
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

            if expr.show_as_anchor and not context.inline_anchors:
                step = qlast.PathStepNode(expr=expr.show_as_anchor)
            else:
                step = qlast.PathStepNode(expr=expr.concept.name.name,
                                          namespace=expr.concept.name.module)

            path.steps.append(step)
            path.steps.extend(reversed(links))

            result = path

        elif isinstance(expr, tree.ast.PathCombination):
            paths = list(expr.paths)
            if len(paths) == 1:
                result = self._process_expr(context, paths[0])
            else:
                assert False, "path combinations are not supported yet"

        elif isinstance(expr, tree.ast.SubgraphRef):
            result = self._process_expr(context, expr.ref)

        elif isinstance(expr, tree.ast.LinkPropRefSimple):
            if expr.show_as_anchor:
                result = qlast.PathStepNode(expr=expr.show_as_anchor)
            else:
                path = self._process_expr(context, expr.ref)

                # Skip transformed references to a multiatom link, as those are not actual
                # property refs.
                #
                if expr.name != 'metamagic.caos.builtins.target' or expr.ref.source is None:
                    link = qlast.LinkNode(name=expr.name.name, namespace=expr.name.module,
                                          type='property')
                    link = qlast.LinkPropExprNode(expr=link)
                    path.steps.append(link)

                result = path

        elif isinstance(expr, tree.ast.LinkPropRefExpr):
            result = self._process_expr(context, expr.expr)

        elif isinstance(expr, tree.ast.EntityLink):
            if expr.show_as_anchor and not context.inline_anchors:
                result = qlast.PathNode(steps=[qlast.PathStepNode(expr=expr.show_as_anchor)])
            else:
                if expr.source:
                    path = self._process_expr(context, expr.source)
                else:
                    path = qlast.PathNode()

                linkproto = expr.link_proto
                lname = linkproto.normal_name()

                if expr.target and isinstance(expr.target, tree.ast.EntitySet):
                    target = expr.target.concept.name
                    target = qlast.PrototypeRefNode(name=target.name, module=target.module)
                else:
                    target = None

                if path.steps:
                    link = qlast.LinkNode(name=lname.name, namespace=lname.module, target=target,
                                          direction=expr.direction)
                    link = qlast.LinkExprNode(expr=link)
                else:
                    link = qlast.LinkNode(name=lname.name, namespace=lname.module, target=target,
                                          direction=expr.direction)

                path.steps.append(link)
                result = path

        elif isinstance(expr, tree.ast.Sequence):
            elements = [self._process_expr(context, e) for e in expr.elements]
            result = qlast.SequenceNode(elements=elements)
            result = self.process_sequence(result)

        elif isinstance(expr, tree.ast.TypeCast):
            if isinstance(expr.type, tuple):
                if expr.type[0] is not list:
                    raise ValueError('unexpected collection type: {!r}'.format(expr.type[0]))

                typ = (expr.type[0], expr.type[1].name)
            else:
                typ = expr.type.name

            result = qlast.TypeCastNode(expr=self._process_expr(context, expr.expr), type=typ)

        elif isinstance(expr, tree.ast.NoneTest):
            arg = self._process_expr(context, expr.expr)
            result = qlast.NoneTestNode(expr=arg)

        else:
            assert False, "Unexpected expression type: %r" % expr

        return result


class CaosqlTreeTransformer(tree.transformer.TreeTransformer):
    def __init__(self, proto_schema, module_aliases=None):
        self.proto_schema = proto_schema
        self.module_aliases = module_aliases
        self.parser = caosql_parser.CaosQLParser()

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
                step = tree.ast.EntitySet()
                step.concept = proto
                step.id = tree.transformer.LinearPath([step.concept])
                step.anchor = anchor
                step.show_as_anchor = anchor
                # XXX
                # step.users =
            elif isinstance(proto, caos_types.ProtoLink):
                if proto.source:
                    src = tree.ast.EntitySet()
                    src.concept = proto.source
                    src.id = tree.transformer.LinearPath([src.concept])
                else:
                    src = None

                step = tree.ast.EntityLink(link_proto=proto, source=src)
                step.anchor = anchor
                step.show_as_anchor = anchor

                if src:
                    src.disjunction.update(step)

            elif isinstance(proto, caos_types.ProtoLinkProperty):
                ptr_name = proto.normal_name()

                if proto.source.source:
                    src = tree.ast.EntitySet()
                    src.concept = proto.source.source
                    src.id = tree.transformer.LinearPath([src.concept])
                else:
                    src = None

                link = tree.ast.EntityLink(link_proto=proto.source, source=src,
                                           direction=caos_types.OutboundDirection)

                if src:
                    src.disjunction.update(link)
                    pref_id = tree.transformer.LinearPath(src.id)
                else:
                    pref_id = tree.transformer.LinearPath([])

                pref_id.add(proto.source, caos_types.OutboundDirection, proto.target)

                step = tree.ast.LinkPropRefSimple(name=ptr_name, ref=link, id=pref_id,
                                                  ptr_proto=proto)
                step.anchor = anchor
                step.show_as_anchor = anchor

            else:
                step = proto

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

        if caosql_tree.op:
            graph.set_op = tree.ast.SetOperator(caosql_tree.op)
            graph.set_op_larg = self._transform_select(
                                    context, caosql_tree.op_larg, arg_types)
            graph.set_op_rarg = self._transform_select(
                                    context, caosql_tree.op_rarg, arg_types)
        else:
            graph.generator = self._process_select_where(context, caosql_tree.where)

            graph.grouper = self._process_grouper(context, caosql_tree.groupby)
            if graph.grouper:
                groupgraph = tree.ast.Disjunction(paths=frozenset(graph.grouper))
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
                    if isinstance(n, tree.ast.Record):
                        if n.rlink is not None:
                            n.rlink.users.add('generator')
                            n.rlink.source.users.add('generator')
                            self._postprocess_expr(n.rlink.source)
                        raise ast.SkipNode()
                    if hasattr(n, 'users'):
                        n.users.add('generator')
                    if isinstance(n, tree.ast.EntitySet):
                        self._postprocess_expr(n)

                with context():
                    context.current.location = 'generator'
                    for expr in graph.selector:
                        ast.find_children(expr, augmenter, force_traversal=True)


        graph.sorter = self._process_sorter(context, caosql_tree.orderby)
        if caosql_tree.offset:
            graph.offset = tree.ast.Constant(value=caosql_tree.offset.value,
                                             index=caosql_tree.offset.index,
                                             type=context.current.proto_schema.get('int'))

        if caosql_tree.limit:
            graph.limit = tree.ast.Constant(value=caosql_tree.limit.value,
                                             index=caosql_tree.limit.index,
                                             type=context.current.proto_schema.get('int'))

        context.current.location = 'top'

        # Merge selector and sorter disjunctions first
        paths = [s.expr for s in graph.selector] + \
                [s.expr for s in graph.sorter] + \
                [s for s in graph.grouper]
        union = tree.ast.Disjunction(paths=frozenset(paths))
        self.flatten_and_unify_path_combination(union, deep=True,
                                                merge_filters=True)

        # Merge the resulting disjunction with generator conjunction
        if graph.generator:
            paths = [graph.generator] + list(union.paths)
            union = tree.ast.Disjunction(paths=frozenset(paths))
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

        graph = context.current.graph = tree.ast.GraphExpr()
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
                    tree.ast.CommonGraphExpr(expr=_cge, alias=cge.alias))

        tgt = graph.optarget = self._process_select_where(
                                context, caosql_tree.subject)

        idname = caos_name.Name('metamagic.caos.builtins.id')
        idref = tree.ast.AtomicRefSimple(
                    name=idname, ref=tgt,
                    ptr_proto=tgt.concept.pointers[idname])
        tgt.atomrefs.add(idref)
        selexpr = tree.ast.SelectorExpr(expr=idref, name=None)
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

        union = tree.ast.Disjunction(paths=frozenset(paths))
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

            if not isinstance(targetexpr, tree.ast.AtomicRefSimple):
                msg = 'operation update list can only reference atoms'
                raise errors.CaosQLError(msg)

            if isinstance(value, qlast.ConstantNode):
                v = self._process_constant(context, value)
            else:
                v = self._process_expr(context, value)
                paths = tree.ast.Conjunction(
                            paths=frozenset((v, graph.optarget)))
                self.flatten_and_unify_path_combination(
                    paths, deep=True, merge_filters=True)
                self._check_update_expr(graph.optarget, v)

            ref = tree.ast.UpdateExpr(expr=targetexpr, value=v)
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

        graph = context.current.graph = tree.ast.GraphExpr()
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
                    tree.ast.CommonGraphExpr(expr=_cge, alias=cge.alias))

        tgt = graph.optarget = self._process_select_where(
                                context, caosql_tree.subject)

        if (isinstance(tgt, tree.ast.LinkPropRefSimple)
                and tgt.name == 'metamagic.caos.builtins.target'):

            idpropname = caos_name.Name('metamagic.caos.builtins.linkid')
            idprop_proto = tgt.ref.link_proto.pointers[idpropname]
            idref = tree.ast.LinkPropRefSimple(name=idpropname,
                                               ref=tgt.ref,
                                               ptr_proto=idprop_proto)
            graph.optarget.ref.proprefs.add(idref)

            selexpr = tree.ast.SelectorExpr(expr=idref, name=None)
            graph.selector.append(selexpr)

        elif isinstance(tgt, tree.ast.AtomicRefSimple):
            idname = caos_name.Name('metamagic.caos.builtins.id')
            idref = tree.ast.AtomicRefSimple(name=idname, ref=tgt.ref,
                                             ptr_proto=tgt.ptr_proto)
            tgt.ref.atomrefs.add(idref)
            selexpr = tree.ast.SelectorExpr(expr=idref, name=None)
            graph.selector.append(selexpr)

        else:
            idname = caos_name.Name('metamagic.caos.builtins.id')
            idref = tree.ast.AtomicRefSimple(
                        name=idname, ref=tgt,
                        ptr_proto=tgt.concept.pointers[idname])
            tgt.atomrefs.add(idref)
            selexpr = tree.ast.SelectorExpr(expr=idref, name=None)
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

        union = tree.ast.Disjunction(paths=frozenset(paths))
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

            if not isinstance(node, tree.ast.BaseRef):
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
                    if isinstance(p, tree.ast.MetaRef):
                        p = p.ref

                    if p.id not in context.current.groupprefixes:
                        err = ('node reference "%s" must appear in the GROUP BY expression or '
                               'used in an aggregate function ') % p.id
                        raise errors.CaosQLError(err)

            if (context.current.location not in {'generator', 'selector'} \
                            and not context.current.in_func_call) or context.current.in_aggregate:
                if isinstance(node, tree.ast.EntitySet):
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

                args = []
                kwargs = {}

                for a in expr.args:
                    if isinstance(a, qlast.NamedArgNode):
                        kwargs[a.name] = self._process_expr(context, a.arg)
                    else:
                        args.append(self._process_expr(context, a))

                node = tree.ast.FunctionCall(name=expr.func, args=args,
                                             kwargs=kwargs)

                if expr.agg_sort:
                    node.agg_sort = [
                        tree.ast.SortExpr(
                            expr=self._process_expr(context, e.path),
                            direction=e.direction
                        )
                        for e in expr.agg_sort
                    ]

                elif expr.window:
                    if expr.window.orderby:
                        node.agg_sort = [
                            tree.ast.SortExpr(
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

            node = tree.ast.Constant(value=(node,), type=(list, node.__class__))

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
                nt = tree.ast.NoneTest(expr=expr)
                node = self.process_none_test(nt, context.current.proto_schema)

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

        elif isinstance(expr, qlast.IndirectionNode):
            node = self._process_expr(context, expr.arg)
            for indirection_el in expr.indirection:
                if isinstance(indirection_el, qlast.IndexNode):
                    idx = self._process_expr(context, indirection_el.index)
                    node = tree.ast.FunctionCall(name='getitem', args=[node, idx])

                elif isinstance(indirection_el, qlast.SliceNode):
                    if indirection_el.start:
                        start = self._process_expr(context, indirection_el.start)
                    else:
                        start = tree.ast.Constant(value=None)

                    if indirection_el.stop:
                        stop = self._process_expr(context, indirection_el.stop)
                    else:
                        stop = tree.ast.Constant(value=None)

                    node = tree.ast.FunctionCall(name='getslice', args=[node, start, stop])
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

            elif isinstance(ptrspec, qlast.SelectTypeRefNode):
                type_prop_name = ptrspec.attrs[0].expr.name
                type_prop = source.get_type_property(
                                type_prop_name, context.current.proto_schema)

                node = tree.ast.PtrPathSpec(
                            ptr_proto=type_prop,
                            ptr_direction=caos_types.OutboundDirection,
                            target_proto=type_prop.target,
                            trigger=tree.ast.ExplicitPathSpecTrigger())

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

                node = tree.ast.PtrPathSpec(
                            ptr_proto=ptr, ptr_direction=ptr_direction,
                            recurse=recurse, target_proto=target_proto,
                            generator=generator, sorter=sorter,
                            trigger=tree.ast.ExplicitPathSpecTrigger(),
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
                            if isinstance(refnode, tree.ast.Path):
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

            tip_is_link = path_tip and isinstance(path_tip, tree.ast.EntityLink)
            tip_is_cge = path_tip and isinstance(path_tip, tree.ast.GraphExpr)

            if isinstance(tip, qlast.PathStepNode):

                proto = self._normalize_concept(
                            context, tip.expr, tip.namespace)

                if isinstance(proto, caos_types.ProtoNode):
                    step = tree.ast.EntitySet()
                    step.concept = proto
                    step.id = tree.transformer.LinearPath([step.concept])
                    step.pathvar = pathvar
                    step.users.add(context.current.location)
                else:
                    step = tree.ast.EntityLink(link_proto=proto)

                path_tip = step

                if pathvar:
                    pathvars[pathvar] = step

            elif isinstance(tip, qlast.TypeRefNode):
                typeref = self._process_path(context, tip.expr)
                if isinstance(typeref, tree.ast.PathCombination):
                    if len(typeref.paths) > 1:
                        msg = "type() argument must not be a path combination"
                        raise errors.CaosQLError(msg)
                    typeref = next(iter(typeref.paths))

            elif isinstance(tip, qlast.LinkExprNode) and typeref:
                path_tip = tree.ast.MetaRef(ref=typeref, name=tip.expr.name)

            elif isinstance(tip, qlast.LinkExprNode) and tip_is_cge:
                path_tip = tree.ast.SubgraphRef(ref=path_tip,
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
                    target_set = tree.ast.EntitySet()
                    target_set.concept = target
                    target_set.id = tree.transformer.LinearPath(path_tip.id)
                    target_set.id.add(link_proto, direction,
                                      target_set.concept)
                    target_set.users.add(context.current.location)

                    link = tree.ast.EntityLink(source=path_tip,
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
                    target_set = tree.ast.EntitySet()
                    target_set.concept = target
                    target_set.id = tree.transformer.LinearPath(path_tip.id)
                    target_set.id.add(link_proto, direction,
                                      target_set.concept)
                    target_set.users.add(context.current.location)

                    link = tree.ast.EntityLink(source=path_tip,
                                               target=target_set,
                                               link_proto=link_proto,
                                               direction=direction,
                                               pathvar=link_pathvar,
                                               users={context.current.location})

                    target_set.rlink = link

                    atomref_id = tree.transformer.LinearPath(path_tip.id)
                    atomref_id.add(link_proto, direction, target)

                    if not link_proto.singular():
                        ptr_name = caos_name.Name(
                                        'metamagic.caos.builtins.target')
                        ptr_proto = link_proto.pointers[ptr_name]
                        atomref = tree.ast.LinkPropRefSimple(
                                        name=ptr_name, ref=link,
                                        id=atomref_id, ptr_proto=ptr_proto)
                        link.proprefs.add(atomref)
                        path_tip.disjunction.update(link)
                        path_tip.disjunction.fixed = context.current.weak_path
                    else:
                        atomref = tree.ast.AtomicRefSimple(
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

                if path_tip and isinstance(path_tip, tree.ast.GraphExpr):
                    subgraph = path_tip
                    subgraph.attrrefs.add(link_expr.name)
                    sgref = tree.ast.SubgraphRef(ref=subgraph,
                                                 name=link_expr.name)
                    path_tip = sgref
                else:
                    prop_name = (link_expr.namespace, link_expr.name)

                    if (isinstance(path_tip, tree.ast.LinkPropRefSimple)
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
                        if isinstance(path_tip, tree.ast.LinkPropRefSimple):
                            link = path_tip.ref
                            link_proto = link.link_proto
                            id = path_tip.id
                        elif isinstance(path_tip, tree.ast.EntityLink):
                            link = path_tip
                            link_proto = link.link_proto
                            id = tree.transformer.LinearPath([None])
                            id.add(link_proto, caos_types.OutboundDirection, None)
                        else:
                            link = path_tip.rlink
                            link_proto = link.link_proto
                            id = path_tip.id

                        prop_proto = self._resolve_ptr(
                            context, link_proto, prop_name,
                            caos_types.OutboundDirection,
                            ptr_type=caos_types.ProtoLinkProperty)

                        propref = tree.ast.LinkPropRefSimple(
                            name=prop_proto.normal_name(),
                            ref=link, id=id, ptr_proto=prop_proto)
                        link.proprefs.add(propref)

                        path_tip = propref

            else:
                assert False, 'Unexpected path step expression: "%s"' % tip

        if isinstance(path_tip, tree.ast.EntityLink):
            # Dangling link reference, possibly from an anchor ref,
            # complement it to target ref
            link_proto = path_tip.link_proto

            pref_id = tree.transformer.LinearPath(path_tip.source.id)
            pref_id.add(link_proto, caos_types.OutboundDirection,
                        link_proto.target)
            ptr_proto = link_proto.pointers['metamagic.caos.builtins.target']
            ptr_name = ptr_proto.normal_name()
            propref = tree.ast.LinkPropRefSimple(
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

                if isinstance(expr, tree.ast.Disjunction):
                    path = next(iter(expr.paths))
                else:
                    path = expr

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
                    if isinstance(expr, tree.ast.PathCombination):
                        assert len(expr.paths) == 1
                        expr = next(iter(expr.paths))
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
