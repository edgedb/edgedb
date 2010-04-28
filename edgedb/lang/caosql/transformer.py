##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import itertools

from semantix.caos import types as caos_types
from semantix.caos import tree
from semantix.caos import name as caos_name
from semantix.caos.caosql import ast as qlast
from semantix.caos.caosql import CaosQLError


class ParseContextLevel(object):
    def __init__(self, prevlevel=None):
        if prevlevel is not None:
            self.anchors = prevlevel.anchors.copy()
            self.namespaces = prevlevel.namespaces.copy()
            self.location = prevlevel.location
        else:
            self.anchors = {}
            self.namespaces = {}
            self.location = None


class ParseContext(object):
    stack = []

    def __init__(self):
        self.push()

    def push(self):
        level = ParseContextLevel()
        self.stack.append(level)

        return level

    def pop(self):
        self.stack.pop()

    def _current(self):
        if len(self.stack) > 0:
            return self.stack[-1]
        else:
            return None

    current = property(_current)


class ParseContextWrapper(object):
    def __init__(self, context):
        self.context = context

    def __enter__(self):
        self.context.push()
        return self.context

    def __exit__(self, exc_type, exc_value, traceback):
        self.context.pop()


class CaosqlTreeTransformer(tree.transformer.TreeTransformer):
    def __init__(self, realm, module_aliases=None):
        self.realm = realm
        self.module_aliases = module_aliases

    def transform(self, caosql_tree, arg_types):
        self.context = context = ParseContext()
        stree = self._transform_select(context, caosql_tree, arg_types)

        return stree

    def _transform_select(self, context, caosql_tree, arg_types):
        self.arg_types = arg_types or {}

        graph = context.current.graph = tree.ast.GraphExpr()

        if caosql_tree.namespaces:
            for ns in caosql_tree.namespaces:
                context.current.namespaces[ns.alias] = ns.namespace

        if self.module_aliases:
            context.current.namespaces.update(self.module_aliases)

        graph.generator = self._process_select_where(context, caosql_tree.where)
        graph.selector = self._process_select_targets(context, caosql_tree.targets)
        graph.sorter = self._process_sorter(context, caosql_tree.orderby)
        if caosql_tree.offset:
            graph.offset = tree.ast.Constant(value=caosql_tree.offset.value,
                                             index=caosql_tree.offset.index,
                                             type=int)

        if caosql_tree.limit:
            graph.limit = tree.ast.Constant(value=caosql_tree.limit.value,
                                             index=caosql_tree.limit.index,
                                             type=int)

        paths = [graph.generator] + [s.expr for s in graph.selector] + [s.expr for s in graph.sorter]

        union = tree.ast.Disjunction(paths=frozenset(paths))
        self.flatten_and_unify_path_combination(union, deep=True, merge_filters=True)

        return graph

    def _process_select_where(self, context, where):
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

        if isinstance(expr, qlast.BinOpNode):
            left = self._process_expr(context, expr.left)
            right = self._process_expr(context, expr.right)
            node = self.process_binop(left, right, expr.op)

        elif isinstance(expr, qlast.PathNode):
            node = self._process_path(context, expr)

        elif isinstance(expr, qlast.ConstantNode):
            type = self.arg_types.get(expr.index)
            node = tree.ast.Constant(value=expr.value, index=expr.index, type=type)

        elif isinstance(expr, qlast.SequenceNode):
            elements=[self._process_expr(context, e) for e in expr.elements]
            node = tree.ast.Sequence(elements=elements)

        elif isinstance(expr, qlast.FunctionCallNode):
            args = [self._process_expr(context, a) for a in expr.args]
            node = tree.ast.FunctionCall(name=expr.func, args=args)

        elif isinstance(expr, qlast.PrototypeRefNode):
            if expr.module:
                name = caos_name.Name(name=expr.name, module=expr.module)
            else:
                name = expr.name
            node = self.realm.meta.get(name=name, module_aliases=context.current.namespaces,
                                       type=caos_types.ProtoNode)

        else:
            assert False, "Unexpected expr: %s" % expr

        return node

    def _process_path(self, context, path):
        anchors = context.current.anchors
        tips = {}

        for i, node in enumerate(path.steps):
            anchor = None

            if isinstance(node, (qlast.PathNode, qlast.PathStepNode)):
                if isinstance(node, qlast.PathNode):
                    if len(node.steps) > 1:
                        raise CaosQLError('unsupported subpath expression')

                    anchor = node.var.name if node.var else None

                    if anchor in anchors:
                        raise CaosQLError('duplicate anchor: %s' % anchor)

                    tip = self._get_path_tip(node)

                elif isinstance(node, qlast.PathStepNode):
                    if node.expr in anchors and (i == 0 or node.epxr.beginswith('#')):
                        refnode = anchors[node.expr]

                        if refnode:
                            if isinstance(refnode, tree.ast.Disjunction):
                                tips = {}
                                for p in refnode.paths:
                                    concept = next(iter(p.concepts))
                                    if concept in tips:
                                        tips[concept].add(self.copy_path(p))
                                    else:
                                        tips[concept] = {self.copy_path(p)}
                            else:
                                tips = {next(iter(refnode.concepts)): {self.copy_path(refnode)}}
                        continue

                    tip = node
            else:
                tip = node

            if isinstance(tip, qlast.PathStepNode):

                step = tree.ast.EntitySet()

                tips = {self._normalize_concept(context, tip.expr, tip.namespace): {step}}

                step.concepts = frozenset(tips.keys())
                step.id = tree.transformer.LinearPath([step.concepts])
                step.anchor = anchor

                if anchor:
                    anchors[anchor] = step

            elif isinstance(tip, qlast.LinkExprNode):
                # LinkExprNode
                link_expr = tip.expr

                if isinstance(link_expr, qlast.LinkNode):
                    direction = link_expr.direction or caos_types.OutboundDirection
                else:
                    raise CaosQLError("complex link expressions are not supported yet")

                newtips = {}

                for concept, tip in tips.items():
                    module = link_expr.namespace or concept.name.module
                    link_protos = self._normalize_link(context, link_expr.name, module)

                    seen_concepts = seen_atoms = False
                    all_links = link_protos[0].name == 'semantix.caos.builtins.link'

                    outbound, inbound = concept.match_links(self.realm, link_protos, direction,
                                                            skip_atomic=all_links)

                    links = {caos_types.OutboundDirection: outbound, caos_types.InboundDirection: inbound}

                    for dir, linksets in links.items():
                        for linkset_proto in linksets:
                            for link_item in linkset_proto:
                                if dir is caos_types.OutboundDirection:
                                    target = link_item.target
                                else:
                                    target = link_item.source
                                assert target

                                if link_item.implicit_derivative:
                                    link_item = link_item.get_class_base(self.realm)[0]

                                if isinstance(target, caos_types.ProtoConcept):
                                    if seen_atoms:
                                        raise CaosQLError('path expression results in invalid atom/concept mix')
                                    seen_concepts = True

                                    link_spec = tree.ast.EntityLinkSpec(labels=frozenset([link_item]),
                                                                        direction=dir)

                                    for t in tip:
                                        target_set = tree.ast.EntitySet()
                                        target_set.concepts = frozenset((target,))
                                        target_set.id = tree.transformer.LinearPath(t.id)
                                        target_set.id.add(link_spec.labels, link_spec.direction, target_set.concepts)

                                        link = tree.ast.EntityLink(source=t, target=target_set, filter=link_spec)

                                        t.disjunction.update(link)
                                        target_set.rlink = link

                                        if target in newtips:
                                            newtips[target].add(target_set)
                                        else:
                                            newtips[target] = {target_set}

                                elif isinstance(target, caos_types.ProtoAtom):
                                    if seen_concepts:
                                        raise CaosQLError('path expression results in invalid atom/concept mix')
                                    seen_atoms = True

                                    newtips[target] = set()
                                    for t in tip:
                                        atomref = tree.ast.AtomicRefSimple(name=linkset_proto.name, ref=t)
                                        newtips[target].add(atomref)

                                else:
                                    assert False, 'unexpected link target type: %s' % target

                if not newtips:
                    raise CaosQLError('path expression always yields an empty set')

                if anchor:
                    paths = itertools.chain.from_iterable(newtips.values())
                    anchors[anchor] = tree.ast.Disjunction(paths=frozenset(paths))

                tips = newtips
        paths = itertools.chain.from_iterable(tips.values())
        return tree.ast.Disjunction(paths=frozenset(paths))

    def _normalize_link(self, context, link, namespace):
        if link == '%':
            links = [self.realm.meta.get(name='semantix.caos.builtins.link')]
        else:
            links = self.realm.meta.match(name=link, module_aliases=context.current.namespaces,
                                          type=caos_types.ProtoLink)
        return links

    def _normalize_concept(self, context, concept, namespace):
        if concept == '%':
            concept = self.realm.meta.get(name='semantix.caos.builtins.BaseObject')
        else:
            concept = self.realm.meta.get(name=concept, module_aliases=context.current.namespaces,
                                          type=caos_types.ProtoNode)
        return concept

    def _process_select_targets(self, context, targets):
        selector = list()

        context.current.location = 'selector'
        for target in targets:
            expr = self._process_expr(context, target.expr)
            expr = self.merge_paths(expr)
            t = tree.ast.SelectorExpr(expr=expr, name=target.alias)
            selector.append(t)

        context.current.location = None
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
        context.current.location = 'sorter'

        result = []

        for sorter in sorters:
            expr = self._process_expr(context, sorter.path)
            expr = self.merge_paths(expr)
            s = tree.ast.SortExpr(expr=expr, direction=sorter.direction)
            result.append(s)

        return result
