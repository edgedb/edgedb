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
from semantix.caos import proto as caos_proto
from semantix.caos.caosql import ast as qlast
from semantix.caos.caosql import errors


class ParseContextLevel(object):
    def __init__(self, prevlevel=None):
        if prevlevel is not None:
            self.anchors = prevlevel.anchors
            self.namespaces = prevlevel.namespaces
            self.location = prevlevel.location
            self.groupprefixes = prevlevel.groupprefixes
            self.in_aggregate = prevlevel.in_aggregate[:]
            self.arguments = prevlevel.arguments
        else:
            self.anchors = {}
            self.namespaces = {}
            self.location = None
            self.groupprefixes = None
            self.in_aggregate = []
            self.arguments = {}


class ParseContext(object):
    def __init__(self):
        self.stack = []
        self.push()

    def push(self):
        level = ParseContextLevel(self.current)
        self.stack.append(level)

        return level

    def pop(self):
        self.stack.pop()

    def __call__(self):
        return ParseContextWrapper(self)

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


class CaosqlReverseTransformer(tree.transformer.TreeTransformer):
    def transform(self, caos_tree):
        return self._process_expr(caos_tree)

    def _process_expr(self, expr):
        if isinstance(expr, tree.ast.BinOp):
            left = self._process_expr(expr.left)
            right = self._process_expr(expr.right)
            result = qlast.BinOpNode(left=left, op=expr.op, right=right)

        elif isinstance(expr, tree.ast.AtomicRef):
            path = self._process_expr(expr.ref)
            link = qlast.LinkNode(name=expr.name.name, namespace=expr.name.module)
            link = qlast.LinkExprNode(expr=link)
            path.steps.append(link)
            result = path

        elif isinstance(expr, tree.ast.EntitySet):
            links = []

            while expr.rlink:
                linknode = expr.rlink
                linkproto = next(iter(linknode.filter.labels))

                link = qlast.LinkNode(name=linkproto.name.name, namespace=linkproto.name.module,
                                      direction=linknode.filter.direction)
                link = qlast.LinkExprNode(expr=link)
                links.append(link)

                expr = expr.rlink.source

            path = qlast.PathNode()
            step = qlast.PathStepNode(expr=expr.concept.name.name,
                                      namespace=expr.concept.name.module)
            path.steps.append(step)
            path.steps.extend(reversed(links))

            result = path

        elif isinstance(expr, tree.ast.LinkPropRef):
            path = self._process_expr(expr.ref)
            link = qlast.LinkNode(name=expr.name.name, namespace=expr.name.module)
            link = qlast.LinkPropExprNode(expr=link)
            path.steps.append(link)
            result = path

        elif isinstance(expr, tree.ast.EntityLink):
            if expr.source:
                path = self._process_expr(expr.source)
            else:
                path = qlast.PathNode()

            linkproto = next(iter(expr.filter.labels))

            if path.steps:
                link = qlast.LinkNode(name=linkproto.name.name, namespace=linkproto.name.module)
                link = qlast.LinkExprNode(expr=link)
            else:
                link = qlast.PathStepNode(expr=linkproto.name.name, namespace=linkproto.name.module)

            path.steps.append(link)
            result = path

        elif isinstance(expr, tree.ast.Sequence):
            elements = [self._process_expr(e) for e in expr.elements]
            result = qlast.SequenceNode(elements=elements)
            result = self.process_sequence(result)

        else:
            assert False, "Unexpected expression type: %r" % expr

        return result


class CaosqlTreeTransformer(tree.transformer.TreeTransformer):
    def __init__(self, proto_schema, module_aliases=None):
        self.proto_schema = proto_schema
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

        graph.grouper = self._process_grouper(context, caosql_tree.groupby)
        if graph.grouper:
            groupgraph = tree.ast.Disjunction(paths=frozenset(graph.grouper))
            context.current.groupprefixes = self.extract_prefixes(groupgraph)

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

    def _process_expr(self, context, expr, *, selector_top_level=False):
        node = None

        if isinstance(expr, qlast.BinOpNode):
            left = self._process_expr(context, expr.left)
            right = self._process_expr(context, expr.right)
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

            if context.current.location != 'generator' or context.current.in_aggregate:
                node = self.entityref_to_idref(node, self.proto_schema,
                                               full_record=selector_top_level)

        elif isinstance(expr, qlast.ConstantNode):
            type = self.arg_types.get(expr.index)
            node = tree.ast.Constant(value=expr.value, index=expr.index, type=type)
            if expr.index:
                context.current.arguments[expr.index] = type

        elif isinstance(expr, qlast.SequenceNode):
            elements=[self._process_expr(context, e) for e in expr.elements]
            node = tree.ast.Sequence(elements=elements)
            node = self.process_sequence(node)

        elif isinstance(expr, qlast.FunctionCallNode):
            with context():
                if expr.func[0] == 'agg':
                    context.current.in_aggregate.append(expr)
                args = [self._process_expr(context, a) for a in expr.args]
                node = tree.ast.FunctionCall(name=expr.func, args=args)
                node = self.process_function_call(node)

        elif isinstance(expr, qlast.PrototypeRefNode):
            if expr.module:
                name = caos_name.Name(name=expr.name, module=expr.module)
            else:
                name = expr.name
            node = self.proto_schema.get(name=name, module_aliases=context.current.namespaces,
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
                        raise errors.CaosQLError('unsupported subpath expression')

                    anchor = node.var.name if node.var else None

                    if anchor in anchors:
                        raise errors.CaosQLError('duplicate anchor: %s' % anchor)

                    tip = self._get_path_tip(node)

                elif isinstance(node, qlast.PathStepNode):
                    if node.expr in anchors and (i == 0 or node.epxr.beginswith('#')):
                        refnode = anchors[node.expr]

                        if refnode:
                            if isinstance(refnode, tree.ast.Disjunction):
                                tips = {}
                                for p in refnode.paths:
                                    concept = p.concept
                                    path_copy = self.copy_path(p)
                                    path_copy.users.add(context.current.location)
                                    if concept in tips:
                                        tips[concept].add(path_copy)
                                    else:
                                        tips[concept] = {path_copy}
                            else:
                                path_copy = self.copy_path(refnode)
                                path_copy.users.add(context.current.location)
                                tips = {refnode.concept: {path_copy}}
                        continue

                    tip = node
            else:
                tip = node

            if isinstance(tip, qlast.PathStepNode):

                proto = self._normalize_concept(context, tip.expr, tip.namespace)

                if isinstance(proto, caos_types.ProtoConcept):
                    step = tree.ast.EntitySet()
                    step.concept = proto
                    tips = {step.concept: {step}}
                    step.id = tree.transformer.LinearPath([step.concept])
                    step.anchor = anchor

                    step.users.add(context.current.location)
                else:
                    linkspec = tree.ast.EntityLinkSpec(labels=frozenset((proto,)))
                    step = tree.ast.EntityLink(filter=linkspec)
                    tips = {None: {step}}

                if anchor:
                    anchors[anchor] = step

            elif isinstance(tip, qlast.LinkExprNode):
                # LinkExprNode
                link_expr = tip.expr

                if isinstance(link_expr, qlast.LinkNode):
                    direction = link_expr.direction or caos_types.OutboundDirection
                else:
                    raise errors.CaosQLError("complex link expressions are not supported yet")

                newtips = {}
                sources = set()

                for concept, tip in tips.items():
                    module = link_expr.namespace
                    source_and_link = self._normalize_link(context, concept, link_expr.name,
                                                           module)

                    if not source_and_link:
                        raise errors.CaosQLReferenceError('reference to an undefined link "%s"' \
                                                          % link_expr.name)

                    sources, link_protos = source_and_link

                    seen_concepts = seen_atoms = False
                    all_links = link_protos[0].name == 'semantix.caos.builtins.link'

                    inbound = set()
                    outbound = set()
                    for source in sources:
                        out, inb = source.match_links(self.proto_schema, link_protos,
                                                      direction, skip_atomic=all_links)
                        outbound.update(out)
                        inbound.update(inb)

                    if len(sources) > 1 or list(sources)[0] != concept:
                        newtip = set()
                        for t in tip:
                            paths = set(t.rlink.source.disjunction.paths)

                            for source in sources:
                                link_spec = t.rlink.filter
                                t_id = tree.transformer.LinearPath(t.id[:-2])
                                t_id.add(link_spec.labels, link_spec.direction, source)

                                new_target = tree.ast.EntitySet()
                                new_target.concept = source
                                new_target.id = t_id
                                new_target.users = t.users.copy()

                                newtip.add(new_target)

                                link = tree.ast.EntityLink(source=t.rlink.source,
                                                           target=new_target,
                                                           filter=link_spec,
                                                           link_proto=t.rlink.link_proto)

                                new_target.rlink = link
                                paths.add(link)

                            paths.remove(t.rlink)
                            t.rlink.source.disjunction.paths = frozenset(paths)
                            tip = newtip

                    links = {caos_types.OutboundDirection: outbound,
                             caos_types.InboundDirection: inbound}

                    for dir, linksets in links.items():
                        for linkset_proto in linksets:
                            for link_item in linkset_proto:
                                if dir is caos_types.OutboundDirection:
                                    target = link_item.target
                                else:
                                    target = link_item.source
                                assert target

                                if not link_item.generic():
                                    link_proto = link_item
                                    link_item = self.proto_schema.get(link_item.normal_name())
                                else:
                                    assert False

                                if isinstance(target, caos_types.ProtoConcept):
                                    if seen_atoms:
                                        raise errors.CaosQLError('path expression results in an '
                                                                 'invalid atom/concept mix')
                                    seen_concepts = True

                                    link_spec = tree.ast.EntityLinkSpec(labels=frozenset([link_item]),
                                                                        direction=dir)

                                    for t in tip:
                                        target_set = tree.ast.EntitySet()
                                        target_set.concept = target
                                        target_set.id = tree.transformer.LinearPath(t.id)
                                        target_set.id.add(link_spec.labels, link_spec.direction,
                                                          target_set.concept)
                                        target_set.users.add(context.current.location)

                                        link = tree.ast.EntityLink(source=t, target=target_set,
                                                                   filter=link_spec,
                                                                   link_proto=link_proto)

                                        t.disjunction.update(link)
                                        target_set.rlink = link

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
                                        atomref_id = tree.transformer.LinearPath(t.id)
                                        atomref_id.add(link_item, dir, target)
                                        atomref = tree.ast.AtomicRefSimple(name=linkset_proto.name,
                                                                           ref=t, id=atomref_id)
                                        newtips[target].add(atomref)

                                else:
                                    assert False, 'unexpected link target type: %s' % target

                if not newtips:
                    msg = '%s is not a valid pointer of %s' % \
                                (' or '.join('"%s"' % p.normal_name() for p in link_protos),
                                 ' or '.join('"%s"' % p.name for p in tips.keys()))
                    source = list(tips.keys())[0]
                    pointer = list(link_protos)[0]
                    raise errors.CaosQLReferenceError(msg, source=source, pointer=pointer)

                if anchor:
                    paths = itertools.chain.from_iterable(newtips.values())
                    anchors[anchor] = tree.ast.Disjunction(paths=frozenset(paths))

                tips = newtips

            elif isinstance(tip, qlast.LinkPropExprNode):
                # LinkExprNode
                link_expr = tip.expr

                newtips = {}

                for concept, tip in tips.items():
                    module = link_expr.namespace

                    for entset in tip:
                        if isinstance(entset, tree.ast.EntityLink):
                            link = entset
                            link_proto = next(iter(link.filter.labels))
                            id = tree.transformer.LinearPath([None])
                            id.add((link_proto,), caos_types.OutboundDirection, None)
                        else:
                            link = entset.rlink
                            id = entset.id

                        link_proto = next(iter(link.filter.labels))
                        sources, prop_protos = self._normalize_link(context, link_proto,
                                                                    link_expr.name, module,
                                                                    type=caos_proto.LinkProperty)

                        for prop_proto in prop_protos:
                            propref = tree.ast.LinkPropRefSimple(name=prop_proto.name, ref=link,
                                                                 id=id)
                            newtips[prop_proto] = {propref}

                tips = newtips
            else:
                assert False, 'Unexpected path step expression: "%s"' % tip

        paths = itertools.chain.from_iterable(tips.values())
        return tree.ast.Disjunction(paths=frozenset(paths))

    def _normalize_link(self, context, concept, link, namespace, type=caos_types.ProtoLink):
        if link == '%':
            links = [self.proto_schema.get(name='semantix.caos.builtins.link')]
            sources = [concept]
        else:
            if not namespace:
                assert '%' not in link
                linkset = concept.get_attr(self.proto_schema, link)

                if not linkset:
                    children, link_name = concept.get_closest_children_defining_pointer(
                                                                            self.proto_schema, link)

                    if not children:
                        raise errors.CaosQLReferenceError('"%s" is not a valid pointer of "%s"' \
                                                          % (link, concept.name))

                    sources = children
                    links = [self.proto_schema.get(link_name, type=type)]

                else:
                    links = [self.proto_schema.get(linkset.normal_name(), type=type)]
                    sources = [concept]

            else:
                name = caos_name.Name(name=link, module=namespace)
                links = self.proto_schema.match(name=name, module_aliases=context.current.namespaces,
                                                type=type)
                if not links:
                    raise errors.CaosQLReferenceError('no valid pointers of "%s" match "%s"' \
                                                      % (concept.name, link))
                sources = [concept]

        return sources, links

    def _normalize_concept(self, context, concept, namespace):
        if concept == '%':
            concept = self.proto_schema.get(name='semantix.caos.builtins.BaseObject')
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
                expr = self._process_expr(context, target.expr, selector_top_level=True)
                expr = self.merge_paths(expr)
                t = tree.ast.SelectorExpr(expr=expr, name=target.alias)
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

        with context():
            context.current.location = 'sorter'
            for sorter in sorters:
                expr = self._process_expr(context, sorter.path)
                expr = self.merge_paths(expr)
                s = tree.ast.SortExpr(expr=expr, direction=sorter.direction)
                result.append(s)

        return result

    def _process_grouper(self, context, groupers):

        result = []

        with context():
            context.current.location = 'grouper'
            for grouper in groupers:
                expr = self._process_expr(context, grouper)
                expr = self.merge_paths(expr)
                result.append(expr)

        return result
