##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import itertools

from metamagic.caos import name as caos_name
from metamagic.caos import error as caos_error
from metamagic.caos import utils as caos_utils
from metamagic.caos.utils import LinearPath
from metamagic.caos import types as caos_types
from metamagic.caos.tree import ast as caos_ast

from metamagic.utils.algos import boolean
from metamagic.utils import datastructures, ast, debug, markup
from metamagic.utils.datastructures import Void
from metamagic.utils.functional import checktypes

from metamagic import exceptions


class PathIndex(dict):
    """
    Graph path mapping path identifiers to AST nodes
    """

    def update(self, other):
        for k, v in other.items():
            if k in self:
                super().__getitem__(k).update(v)
            else:
                self[k] = v

    def __setitem__(self, key, value):
        if not isinstance(key, (LinearPath, str)):
            raise TypeError('Invalid key type for PathIndex: %s' % key)

        if not isinstance(value, set):
            value = {value}

        super().__setitem__(key, value)

    """
    def __getitem__(self, key):
        result = set()
        for k, v in self.items():
            if k == key:
                result.update(v)
        if not result:
            raise KeyError
        return result
    """

    """
    def __contains__(self, key):
        for k in self.keys():
            if k == key:
                return True
        return False
    """


class TreeTransformerError(exceptions.MetamagicError):
    pass


class InternalTreeTransformerError(TreeTransformerError):
    pass


class TreeError(TreeTransformerError):
    pass


class TreeTransformerExceptionContext(markup.MarkupExceptionContext):
    title = 'Caos Tree Transformer Context'

    def __init__(self, tree):
        super().__init__()
        self.tree = tree

    @classmethod
    def as_markup(cls, self, *, ctx):
        tree = markup.serialize(self.tree, ctx=ctx)
        return markup.elements.lang.ExceptionContext(title=self.title, body=[tree])


@checktypes
class TreeTransformer:

    def extract_prefixes(self, expr, prefixes=None):
        prefixes = prefixes if prefixes is not None else PathIndex()

        if isinstance(expr, caos_ast.PathCombination):
            for path in expr.paths:
                self.extract_prefixes(path, prefixes)

        elif isinstance(expr, (caos_ast.EntitySet, caos_ast.AtomicRefSimple)):
            key = getattr(expr, 'pathvar', None) or expr.id

            if key:
                # XXX AtomicRefs with PathCombinations in ref don't have an id
                if key not in prefixes:
                    prefixes[key] = {expr}
                else:
                    prefixes[key].add(expr)

            if isinstance(expr, caos_ast.EntitySet) and expr.rlink:
                self.extract_prefixes(expr.rlink.source, prefixes)
            elif isinstance(expr, caos_ast.AtomicRefSimple):
                self.extract_prefixes(expr.ref, prefixes)

        elif isinstance(expr, caos_ast.EntityLink):
            self.extract_prefixes(expr.target or expr.source, prefixes)

        elif isinstance(expr, caos_ast.LinkPropRefSimple):
            self.extract_prefixes(expr.ref, prefixes)

        elif isinstance(expr, caos_ast.BinOp):
            self.extract_prefixes(expr.left, prefixes)
            self.extract_prefixes(expr.right, prefixes)

        elif isinstance(expr, caos_ast.UnaryOp):
            self.extract_prefixes(expr.expr, prefixes)

        elif isinstance(expr, caos_ast.ExistPred):
            self.extract_prefixes(expr.expr, prefixes)

        elif isinstance(expr, (caos_ast.InlineFilter, caos_ast.InlinePropFilter)):
            self.extract_prefixes(expr.ref, prefixes)
            self.extract_prefixes(expr.expr, prefixes)

        elif isinstance(expr, (caos_ast.AtomicRefExpr, caos_ast.LinkPropRefExpr)):
            self.extract_prefixes(expr.expr, prefixes)

        elif isinstance(expr, caos_ast.FunctionCall):
            for arg in expr.args:
                self.extract_prefixes(arg, prefixes)
            for sortexpr in expr.agg_sort:
                self.extract_prefixes(sortexpr.expr, prefixes)
            for partition_expr in expr.partition:
                self.extract_prefixes(partition_expr, prefixes)

        elif isinstance(expr, caos_ast.TypeCast):
            self.extract_prefixes(expr.expr, prefixes)

        elif isinstance(expr, caos_ast.NoneTest):
            self.extract_prefixes(expr.expr, prefixes)

        elif isinstance(expr, (caos_ast.Sequence, caos_ast.Record)):
            for path in expr.elements:
                self.extract_prefixes(path, prefixes)

        elif isinstance(expr, caos_ast.Constant):
            pass

        elif isinstance(expr, caos_ast.GraphExpr):
            pass
            """
            if expr.generator:
                self.extract_prefixes(expr.generator)

            if expr.selector:
                for e in expr.selector:
                    self.extract_prefixes(e.expr, prefixes)

            if expr.grouper:
                for e in expr.grouper:
                    self.extract_prefixes(e, prefixes)

            if expr.sorter:
                for e in expr.sorter:
                    self.extract_prefixes(e, prefixes)
            """

        elif isinstance(expr, caos_ast.SubgraphRef):
            self.extract_prefixes(expr.ref, prefixes)

        elif isinstance(expr, caos_ast.SearchVector):
            for ref in expr.items:
                self.extract_prefixes(ref.ref, prefixes)

        else:
            assert False, 'unexpected node: "%r"' % expr

        return prefixes

    def apply_fixups(self, expr):
        """A rather dumb pass that attempts to fixup potential brokenness of a fully processed tree.
        """

        if isinstance(expr, caos_ast.PathCombination):
            for path in expr.paths:
                self.apply_fixups(path)

        elif isinstance(expr, caos_ast.AtomicRefSimple):
            self.apply_fixups(expr.ref)

        elif isinstance(expr, caos_ast.EntitySet):
            if expr.rlink:
                self.apply_fixups(expr.rlink.source)

            if expr.conjunction.paths:
                # Move non-filtering paths out of a conjunction into a disjunction where
                # it belongs.

                cpaths = set()
                dpaths = set()

                for path in expr.conjunction.paths:
                    if isinstance(path, caos_ast.EntitySet):
                        if 'generator' not in path.users:
                            dpaths.add(path)
                        else:
                            cpaths.add(path)
                    elif isinstance(path, caos_ast.EntityLink):
                        if path.target and 'generator' not in path.target.users \
                                                        and 'generator' not in path.users:
                            dpaths.add(path)
                        else:
                            cpaths.add(path)

                expr.conjunction.paths = frozenset(cpaths)
                if dpaths:
                    expr.disjunction.paths = expr.disjunction.paths | dpaths

        elif isinstance(expr, caos_ast.EntityLink):
            if expr.source is not None:
                self.apply_fixups(expr.source)

        elif isinstance(expr, caos_ast.LinkPropRefSimple):
            self.apply_fixups(expr.ref)

        elif isinstance(expr, caos_ast.BinOp):
            self.apply_fixups(expr.left)
            self.apply_fixups(expr.right)

        elif isinstance(expr, caos_ast.UnaryOp):
            self.apply_fixups(expr.expr)

        elif isinstance(expr, caos_ast.ExistPred):
            self.apply_fixups(expr.expr)

        elif isinstance(expr, (caos_ast.InlineFilter, caos_ast.InlinePropFilter)):
            self.apply_fixups(expr.ref)
            self.apply_fixups(expr.expr)

        elif isinstance(expr, (caos_ast.AtomicRefExpr, caos_ast.LinkPropRefExpr)):
            self.apply_fixups(expr.expr)

        elif isinstance(expr, caos_ast.FunctionCall):
            for arg in expr.args:
                self.apply_fixups(arg)
            for sortexpr in expr.agg_sort:
                self.apply_fixups(sortexpr.expr)
            for partition_expr in expr.partition:
                self.apply_fixups(partition_expr)

        elif isinstance(expr, caos_ast.TypeCast):
            self.apply_fixups(expr.expr)

        elif isinstance(expr, caos_ast.NoneTest):
            self.apply_fixups(expr.expr)

        elif isinstance(expr, (caos_ast.Sequence, caos_ast.Record)):
            for path in expr.elements:
                self.apply_fixups(path)

        elif isinstance(expr, caos_ast.Constant):
            pass

        elif isinstance(expr, caos_ast.GraphExpr):
            if expr.generator:
                self.apply_fixups(expr.generator)

            if expr.selector:
                for e in expr.selector:
                    self.apply_fixups(e.expr)

            if expr.grouper:
                for e in expr.grouper:
                    self.apply_fixups(e)

            if expr.sorter:
                for e in expr.sorter:
                    self.apply_fixups(e)

            if expr.set_op:
                self.apply_fixups(expr.set_op_larg)
                self.apply_fixups(expr.set_op_rarg)

        elif isinstance(expr, caos_ast.SortExpr):
            self.apply_fixups(expr.expr)

        elif isinstance(expr, caos_ast.SubgraphRef):
            self.apply_fixups(expr.ref)

        elif isinstance(expr, caos_ast.SearchVector):
            for ref in expr.items:
                self.apply_fixups(ref.ref)

        else:
            assert False, 'unexpected node: "%r"' % expr

    def _apply_rewrite_hooks(self, expr, type):
        sources = []
        ln = None

        if isinstance(expr, caos_ast.EntitySet):
            sources = [expr.concept]
        elif isinstance(expr, caos_ast.EntityLink):
            ln = expr.link_proto.normal_name()

            if expr.source.concept.is_virtual:
                schema = self.context.current.proto_schema
                for c in expr.source.concept.children(schema):
                    if ln in c.pointers:
                        sources.append(c)
            else:
                sources = [expr.source.concept]
        else:
            raise TypeError('unexpected node to _apply_rewrite_hooks: {!r}'.format(expr))

        for source in sources:
            mro = source.get_mro()

            mro = [cls for cls in mro if isinstance(cls, caos_types.ProtoObject)]

            for proto in mro:
                if ln:
                    key = (proto.name, ln)
                else:
                    key = proto.name

                try:
                    hooks = caos_types._rewrite_hooks[key, 'read', type]
                except KeyError:
                    pass
                else:
                    for hook in hooks:
                        result = hook(self.context.current.graph, expr,
                                      self.context.current.context_vars)
                        if result:
                            self.apply_rewrites(result)
                    break
            else:
                continue

            break
        else:
            if isinstance(expr, caos_ast.EntityLink):
                if type == 'computable' and expr.link_proto.is_pure_computable():
                    deflt = expr.link_proto.default[0]
                    if isinstance(deflt, caos_types.LiteralDefaultSpec):
                        caosql_expr = "'" + str(deflt.value).replace("'", "''") + "'"
                        target_type = expr.link_proto.target.name
                        caosql_expr = 'CAST ({} AS [{}])'.format(caosql_expr, target_type)
                    else:
                        caosql_expr = deflt.value

                    anchors = {'self': expr.source.concept}
                    self._rewrite_with_caosql_expr(expr, caosql_expr, anchors)

    def _rewrite_with_caosql_expr(self, expr, caosql, anchors):
        from metamagic.caos.caosql import expr as caosql_expr
        cexpr = caosql_expr.CaosQLExpression(proto_schema=self.context.current.proto_schema)

        expr_tree = cexpr.transform_expr_fragment(caosql, anchors=anchors)

        node = expr.source
        rewrite_target = expr.target

        path_id = LinearPath([node.concept])
        nodes = ast.find_children(expr_tree,
                                  lambda n: isinstance(n, caos_ast.EntitySet) and n.id == path_id)

        for expr_node in nodes:
            expr_node.reference = node

        ptrname = expr.link_proto.normal_name()

        expr_ref = caos_ast.SubgraphRef(name=ptrname, force_inline=True, rlink=expr,
                                        is_rewrite_product=True, rewrite_original=rewrite_target)

        self.context.current.graph.replace_refs([rewrite_target], expr_ref, deep=True)

        if not isinstance(expr_tree, caos_ast.GraphExpr):
            expr_tree = caos_ast.GraphExpr(
                selector = [
                    caos_ast.SelectorExpr(expr=expr_tree, name=ptrname)
                ]
            )

        expr_tree.referrers.append('exists')
        expr_tree.referrers.append('generator')

        self.context.current.graph.subgraphs.add(expr_tree)
        expr_ref.ref = expr_tree

    def apply_rewrites(self, expr):
        """Apply rewrites from policies
        """

        if isinstance(expr, caos_ast.PathCombination):
            for path in expr.paths:
                self.apply_rewrites(path)

        elif isinstance(expr, caos_ast.AtomicRefSimple):
            if expr.rlink is not None:
                self.apply_rewrites(expr.rlink)
            self.apply_rewrites(expr.ref)

        elif isinstance(expr, caos_ast.EntitySet):
            if expr.rlink:
                self.apply_rewrites(expr.rlink)

            if ('access_rewrite' not in expr.rewrite_flags and expr.reference is None
                    and expr.origin is None
                    and getattr(self.context.current, 'apply_access_control_rewrite', False)):
                self._apply_rewrite_hooks(expr, 'filter')
                expr.rewrite_flags.add('access_rewrite')

        elif isinstance(expr, caos_ast.EntityLink):
            if expr.source is not None:
                self.apply_rewrites(expr.source)

            if 'lang_rewrite' not in expr.rewrite_flags:
                schema = self.context.current.proto_schema
                localizable = schema.get('metamagic.caos.extras.l10n.localizable',
                                         default=None)

                link_proto = expr.link_proto

                if localizable is not None and link_proto.issubclass(localizable):
                    cvars = self.context.current.context_vars

                    lang = caos_ast.Constant(index='__context_lang',
                                             type=schema.get('metamagic.caos.builtins.str'))
                    cvars['lang'] = 'en-US'

                    propn = caos_name.Name('metamagic.caos.extras.l10n.lang')

                    for langprop in expr.proprefs:
                        if langprop.name == propn:
                            break
                    else:
                        lprop_proto = link_proto.pointers[propn]
                        langprop = caos_ast.LinkPropRefSimple(name=propn, ref=expr,
                                                              ptr_proto=lprop_proto)
                        expr.proprefs.add(langprop)

                    eq_lang = caos_ast.BinOp(left=langprop, right=lang, op=ast.ops.EQ)
                    lang_none = caos_ast.NoneTest(expr=lang)
                    # Test for property emptiness is for LEFT JOIN cases
                    prop_none = caos_ast.NoneTest(expr=langprop)
                    lang_prop_none = caos_ast.BinOp(left=lang_none, right=prop_none, op=ast.ops.OR)
                    lang_test = caos_ast.BinOp(left=lang_prop_none, right=eq_lang, op=ast.ops.OR,
                                               strong=True)
                    expr.propfilter = self.extend_binop(expr.propfilter, lang_test)
                    expr.rewrite_flags.add('lang_rewrite')

            if ('access_rewrite' not in expr.rewrite_flags and expr.source is not None
                    # An optimization to avoid applying filtering rewrite unnecessarily.
                    and expr.source.reference is None
                    and expr.source.origin is None
                    and getattr(self.context.current, 'apply_access_control_rewrite', False)):
                self._apply_rewrite_hooks(expr, 'filter')
                expr.rewrite_flags.add('access_rewrite')

            if 'computable_rewrite' not in expr.rewrite_flags and expr.source is not None:
                self._apply_rewrite_hooks(expr, 'computable')
                expr.rewrite_flags.add('computable_rewrite')

        elif isinstance(expr, caos_ast.LinkPropRefSimple):
            self.apply_rewrites(expr.ref)

        elif isinstance(expr, caos_ast.BinOp):
            self.apply_rewrites(expr.left)
            self.apply_rewrites(expr.right)

        elif isinstance(expr, caos_ast.UnaryOp):
            self.apply_rewrites(expr.expr)

        elif isinstance(expr, caos_ast.ExistPred):
            self.apply_rewrites(expr.expr)

        elif isinstance(expr, (caos_ast.InlineFilter, caos_ast.InlinePropFilter)):
            self.apply_rewrites(expr.ref)
            self.apply_rewrites(expr.expr)

        elif isinstance(expr, (caos_ast.AtomicRefExpr, caos_ast.LinkPropRefExpr)):
            self.apply_rewrites(expr.expr)

        elif isinstance(expr, caos_ast.FunctionCall):
            for arg in expr.args:
                self.apply_rewrites(arg)
            for sortexpr in expr.agg_sort:
                self.apply_rewrites(sortexpr.expr)
            for partition_expr in expr.partition:
                self.apply_rewrites(partition_expr)

        elif isinstance(expr, caos_ast.TypeCast):
            self.apply_rewrites(expr.expr)

        elif isinstance(expr, caos_ast.NoneTest):
            self.apply_rewrites(expr.expr)

        elif isinstance(expr, (caos_ast.Sequence, caos_ast.Record)):
            for path in expr.elements:
                self.apply_rewrites(path)

        elif isinstance(expr, caos_ast.Constant):
            pass

        elif isinstance(expr, caos_ast.GraphExpr):
            if expr.generator:
                self.apply_rewrites(expr.generator)

            if expr.selector:
                for e in expr.selector:
                    self.apply_rewrites(e.expr)

            if expr.grouper:
                for e in expr.grouper:
                    self.apply_rewrites(e)

            if expr.sorter:
                for e in expr.sorter:
                    self.apply_rewrites(e)

            if expr.set_op:
                self.apply_rewrites(expr.set_op_larg)
                self.apply_rewrites(expr.set_op_rarg)

        elif isinstance(expr, caos_ast.SortExpr):
            self.apply_rewrites(expr.expr)

        elif isinstance(expr, caos_ast.SubgraphRef):
            self.apply_rewrites(expr.ref)

        elif isinstance(expr, caos_ast.SearchVector):
            for ref in expr.items:
                self.apply_rewrites(ref.ref)

        else:
            assert False, 'unexpected node: "%r"' % expr

    def add_path_user(self, path, user):
        while path:
            path.users.add(user)

            if isinstance(path, caos_ast.EntitySet):
                rlink = path.rlink
            else:
                rlink = path

            if rlink:
                rlink.users.add(user)
                path = rlink.source
            else:
                path = None
        return path

    def entityref_to_record(self, expr, schema, *, pathspec=None, prefixes=None,
                                                   _visited_records=None, _recurse=True):
        """Convert an EntitySet node into an Record referencing eager-pointers of EntitySet concept
        """

        if not isinstance(expr, caos_ast.PathCombination):
            expr = caos_ast.Conjunction(paths=frozenset((expr,)))

        if _visited_records is None:
            _visited_records = {}

        p = next(iter(expr.paths))

        recurse_links = None
        recurse_metarefs = ['id', 'name']

        if isinstance(p, caos_ast.EntitySet):
            concepts = {c.concept for c in expr.paths}
            assert len(concepts) == 1

            elements = []
            atomrefs = []

            concept = p.concept
            ref = p if len(expr.paths) == 1 else expr
            rec = caos_ast.Record(elements=elements, concept=concept, rlink=p.rlink)

            _new_visited_records = _visited_records.copy()

            if isinstance(concept, caos_types.ProtoConcept):
                _new_visited_records[concept] = rec

            if concept.is_virtual:
                ptrs = concept.get_children_common_pointers(schema)
                ptrs = {ptr.normal_name(): ptr for ptr in ptrs}
                ptrs.update(concept.pointers)
            else:
                ptrs = concept.pointers

            if pathspec is not None:
                must_have_links = (
                    caos_name.Name('metamagic.caos.builtins.id'),
                    caos_name.Name('metamagic.caos.builtins.mtime'),
                    caos_name.Name('metamagic.caos.builtins.ctime')
                )

                recurse_links = {(l, caos_types.OutboundDirection):
                                 caos_ast.PtrPathSpec(ptr_proto=ptrs[l]) for l in must_have_links}

                for ps in pathspec:
                    if isinstance(ps.ptr_proto, caos_types.ProtoLink):
                        recurse_links[ps.ptr_proto.normal_name(), ps.ptr_direction] = ps

                    elif isinstance(ps.ptr_proto, str):
                        # metaref
                        recurse_metarefs.append(ps.ptr_proto)

            if recurse_links is None:
                recurse_links = {(pn, caos_types.OutboundDirection):
                                    caos_ast.PtrPathSpec(ptr_proto=p)
                                    for pn, p in ptrs.items()
                                        if p.get_loading_behaviour() == caos_types.EagerLoading and
                                           p.target not in _visited_records}

            for (link_name, link_direction), recurse_spec in recurse_links.items():
                el = None

                link = recurse_spec.ptr_proto
                link_direction = recurse_spec.ptr_direction or caos_types.OutboundDirection

                root_link_proto = schema.get(link_name)
                link_proto = link

                if link_direction == caos_types.OutboundDirection:
                    link_target_proto = link.target
                    link_singular = link.mapping in {caos_types.OneToOne, caos_types.ManyToOne}
                else:
                    link_target_proto = link.source
                    link_singular = link.mapping in {caos_types.OneToOne, caos_types.OneToMany}

                if recurse_spec.target_proto is not None:
                    target_proto = recurse_spec.target_proto
                else:
                    target_proto = link_target_proto

                recurse_link = recurse_spec.recurse if recurse_spec is not None else None

                full_path_id = LinearPath(ref.id)
                full_path_id.add(link_proto, link_direction, target_proto)

                if not link_singular or recurse_link is not None:
                    lref = self.copy_path(ref, connect_to_origin=True)
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
                    targetstep = caos_ast.EntitySet(conjunction=caos_ast.Conjunction(),
                                                    disjunction=caos_ast.Disjunction(),
                                                    users={self.context.current.location},
                                                    concept=target_proto, id=full_path_id)

                    link_node = caos_ast.EntityLink(source=lref, target=targetstep,
                                                    link_proto=link_proto,
                                                    direction=link_direction,
                                                    users={'selector'})

                    targetstep.rlink = link_node
                    reusing_target = False

                if recurse_spec.trigger is not None:
                    link_node.pathspec_trigger = recurse_spec.trigger

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

                        sort_target = self.copy_path(sortpath)

                        if isinstance(sortpath, caos_ast.LinkPropRef):
                            if sortpath.ref.link_proto == link_node.link_proto:
                                sort_target.ref = link_node
                                link_node.proprefs.add(sort_target)
                            else:
                                raise ValueError('Cannot sort by property ref of link other than self')
                        else:
                            sortpath_link = sortpath.rlink
                            sort_target.ref = sort_source

                            sort_link = caos_ast.EntityLink(source=sort_source,
                                                            target=sort_target,
                                                            link_proto=sortpath_link.link_proto,
                                                            direction=sortpath_link.direction,
                                                            users=sortpath_link.users)

                            sort_target.rlink = sort_link
                            sort_source.atomrefs.add(sort_target)

                        sorter.append(caos_ast.SortExpr(expr=sort_target,
                                                        direction=sortexpr.direction,
                                                        nones_order=sortexpr.nones_order))

                if isinstance(target_proto, caos_types.ProtoAtom):
                    if link_singular:
                        if not reusing_target:
                            newstep = caos_ast.AtomicRefSimple(ref=lref, name=link_name,
                                                               id=full_path_id,
                                                               ptr_proto=link_proto,
                                                               rlink=link_node)
                            link_node.target = newstep
                            atomrefs.append(newstep)
                    else:
                        ptr_name = caos_name.Name('metamagic.caos.builtins.target')
                        prop_id = LinearPath(ref.id)
                        prop_id.add(root_link_proto, caos_types.OutboundDirection, None)
                        prop_proto = link.pointers[ptr_name]
                        newstep = caos_ast.LinkPropRefSimple(name=ptr_name, id=full_path_id,
                                                             ptr_proto=prop_proto)

                    el = newstep
                else:
                    if _recurse:
                        _memo = _new_visited_records

                        if recurse_spec is not None and recurse_spec.recurse is not None:
                            _memo = {}
                            new_recurse = True
                        elif isinstance(recurse_spec.trigger, caos_ast.ExplicitPathSpecTrigger):
                            new_recurse = True
                        elif newstep.concept not in _visited_records:
                            new_recurse = True
                        else:
                            new_recurse = False

                        recurse_pathspec = recurse_spec.pathspec if recurse_spec is not None \
                                                                 else None
                        el = self.entityref_to_record(newstep, schema,
                                                      pathspec=recurse_pathspec,
                                                      _visited_records=_memo,
                                                      _recurse=new_recurse)

                prop_elements = []
                if link.has_user_defined_properties():
                    if recurse_spec.pathspec is not None:
                        must_have_props = (
                            caos_name.Name('metamagic.caos.builtins.linkid'),
                        )

                        recurse_props = {propn: caos_ast.PtrPathSpec(ptr_proto=link.pointers[propn])
                                         for propn in must_have_props}

                        for ps in recurse_spec.pathspec:
                            if (isinstance(ps.ptr_proto, caos_types.ProtoLinkProperty)
                                    and not ps.ptr_proto.is_endpoint_pointer()):
                                recurse_props[ps.ptr_proto.normal_name()] = ps
                    else:
                        recurse_props = {pn: caos_ast.PtrPathSpec(ptr_proto=p)
                                            for pn, p in link.pointers.items()
                                                if p.get_loading_behaviour() == caos_types.EagerLoading
                                                   and not p.is_endpoint_pointer()}

                    proprec = caos_ast.Record(elements=prop_elements, concept=root_link_proto)

                    for prop_name, prop_proto in link.pointers.items():
                        if prop_name not in recurse_props:
                            continue

                        prop_id = LinearPath(ref.id)
                        prop_id.add(root_link_proto, caos_types.OutboundDirection, None)
                        prop_ref = caos_ast.LinkPropRefSimple(name=prop_name, id=full_path_id,
                                                              ptr_proto=prop_proto,
                                                              ref=link_node)
                        prop_elements.append(prop_ref)
                        link_node.proprefs.add(prop_ref)

                    if prop_elements:
                        xvalue_elements = [el, proprec]
                        if isinstance(link_node.target, caos_ast.AtomicRefSimple):
                            ln = link_node.link_proto.normal_name()
                            concept = link_node.source.concept.pointers[ln]
                        else:
                            concept = link_node.target.concept
                        el = caos_ast.Record(elements=xvalue_elements,
                                             concept=concept,
                                             rlink=link_node, linkprop_xvalue=True)

                if not link.atomic() or prop_elements:
                    lref.conjunction.update(link_node)

                if isinstance(newstep, caos_ast.LinkPropRefSimple):
                    newstep.ref = link_node
                    lref.disjunction.update(link_node)

                if (not link_singular or recurse_spec.recurse is not None) and el is not None:
                    if link.atomic():
                        link_node.proprefs.add(newstep)

                    generator = caos_ast.Conjunction(paths=frozenset((targetstep,)))

                    if filter_generator is not None:
                        ref_gen = caos_ast.Conjunction(paths=frozenset((targetstep,)))
                        generator.paths = frozenset(generator.paths | {filter_generator})
                        generator = self.merge_paths(generator)

                        # merge_paths fails to update all refs, because
                        # some nodes that need to be updated are behind
                        # non-traversable rlink attribute.  Do the
                        # proper update manually.  Also, make sure
                        # aggregate_result below gets a correct ref.
                        #
                        new_targetstep = next(iter(ref_gen.paths))
                        if new_targetstep != targetstep:
                            el.replace_refs([targetstep], new_targetstep,
                                            deep=True)

                            for elem in el.elements:
                                try:
                                    rlink = elem.rlink
                                except AttributeError:
                                    pass
                                else:
                                    if rlink:
                                        rlink.replace_refs([targetstep],
                                                           new_targetstep,
                                                           deep=True)

                            targetstep = new_targetstep

                    subgraph = caos_ast.GraphExpr()
                    subgraph.generator = generator
                    subgraph.aggregate_result = caos_ast.FunctionCall(name=('agg', 'list'),
                                                                      aggregates=True,
                                                                      args=[targetstep])

                    if sorter:
                        subgraph.sorter = sorter

                    subgraph.offset = recurse_spec.offset
                    subgraph.limit = recurse_spec.limit

                    if recurse_spec.recurse is not None:
                        subgraph.recurse_link = link_node
                        subgraph.recurse_depth = recurse_spec.recurse

                    selexpr = caos_ast.SelectorExpr(expr=el, name=link_name)

                    subgraph.selector.append(selexpr)
                    el = caos_ast.SubgraphRef(ref=subgraph, name=link_name, rlink=link_node)

                # Record element may be none if link target is non-atomic and
                # recursion has been prohibited on this level to prevent infinite looping.
                if el is not None:
                    elements.append(el)

            metarefs = []

            for metaref in recurse_metarefs:
                metarefs.append(caos_ast.MetaRef(name=metaref, ref=ref))

            for p in expr.paths:
                p.atomrefs.update(atomrefs)
                p.metarefs.update(metarefs)

            elements.extend(metarefs)
            expr = rec

        return expr

    def entityref_to_idref(self, expr, schema):
        p = next(iter(expr.paths))
        if isinstance(p, caos_ast.EntitySet):
            concepts = {c.concept for c in expr.paths}
            assert len(concepts) == 1

            ref = p if len(expr.paths) == 1 else expr
            link_name = caos_name.Name('metamagic.caos.builtins.id')
            link_proto = schema.get(link_name)
            target_proto = link_proto.target
            id = LinearPath(ref.id)
            id.add(link_proto, caos_types.OutboundDirection, target_proto)
            expr = caos_ast.AtomicRefSimple(ref=ref, name=link_name, id=id, ptr_proto=link_proto)

        return expr

    def _normalize_pathspec_recursion(self, pathspec, source, proto_schema):
        for ptrspec in pathspec:
            if ptrspec.recurse is not None:
                # Push the looping spec one step down so that the loop starts on the first
                # pointer target, not the source.
                #

                if ptrspec.pathspec is None:
                    ptrspec.pathspec = []

                    for ptr in ptrspec.target_proto.pointers.values():
                        if ptr.get_loading_behaviour() == caos_types.EagerLoading:
                            ptrspec.pathspec.append(caos_ast.PtrPathSpec(ptr_proto=ptr))

                recspec = caos_ast.PtrPathSpec(ptr_proto=ptrspec.ptr_proto,
                                               ptr_direction=ptrspec.ptr_direction,
                                               recurse=ptrspec.recurse,
                                               target_proto=ptrspec.target_proto,
                                               generator=ptrspec.generator,
                                               sorter=ptrspec.sorter,
                                               limit=ptrspec.limit,
                                               offset=ptrspec.offset,
                                               pathspec=ptrspec.pathspec[:])

                for i, subptrspec in enumerate(ptrspec.pathspec):
                    if subptrspec.ptr_proto.normal_name() == ptrspec.ptr_proto.normal_name():
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

            item1_lname = item1.ptr_proto.normal_name()
            item1_ldir = item1.ptr_direction

            for i, item2 in enumerate(pathspec2):
                if i in merged_right:
                    continue

                item2_subspec = item2.pathspec

                item2_lname = item2.ptr_proto.normal_name()
                item2_ldir = item2.ptr_direction

                if item1_lname == item2_lname and item1_ldir == item2_ldir:
                    # Do merge

                    if item1.target_proto != item2.target_proto:
                        schema = self.context.current.proto_schema
                        minimize_by = 'most_generic' if target_most_generic else 'least_generic'
                        target = item1.ptr_proto.create_common_target(schema, (item1, item2),
                                                                      minimize_by=minimize_by)
                    else:
                        target = item1.target_proto

                    subspec = self._merge_pathspecs(item1_subspec, item2_subspec,
                                                    target_most_generic=target_most_generic)

                    merged = item1.__class__(
                        ptr_proto=item1.ptr_proto,
                        pathspec=subspec,
                        ptr_direction=item1.ptr_direction,
                        target_proto=target,
                        recurse=item1.recurse,
                        sorter=item1.orderexprs,
                        generator=item1.generator,
                        offset=item1.offset,
                        limit=item1.limit
                    )

                    result.append(merged)

                    merged_right.add(i)

                    break
            else:
                result.append(item1)

        for i, item2 in enumerate(pathspec2):
            if i not in merged_right:
                result.append(item2)

        return result

    def _dump(self, tree):
        if tree is not None:
            markup.dump(tree)
        else:
            markup.dump(None)

    def extend_binop(self, binop, *exprs, op=ast.ops.AND, reversed=False, cls=caos_ast.BinOp):
        exprs = list(exprs)
        binop = binop or exprs.pop(0)

        for expr in exprs:
            if expr is not binop:
                if reversed:
                    binop = cls(right=binop, op=op, left=expr)
                else:
                    binop = cls(left=binop, op=op, right=expr)

        return binop

    def is_aggregated_expr(self, expr, deep=False):
        agg = getattr(expr, 'aggregates', False) or \
                   (isinstance(expr, caos_types.NodeClass) and \
                        caos_utils.get_path_id(expr) in self.context.current.groupprefixes)

        if not agg and deep:
            return bool(list(ast.find_children(expr, lambda i: getattr(i, 'aggregates', None))))
        return agg

    def reorder_aggregates(self, expr):
        if getattr(expr, 'aggregates', False):
            # No need to drill-down, the expression is known to be a pure aggregate
            return expr

        if isinstance(expr, caos_ast.FunctionCall):
            has_agg_args = False

            for arg in expr.args:
                self.reorder_aggregates(arg)

                if self.is_aggregated_expr(arg):
                    has_agg_args = True
                elif has_agg_args and not isinstance(expr, caos_ast.Constant):
                    raise TreeError('invalid expression mix of aggregates and non-aggregates')

            if has_agg_args:
                expr.aggregates = True

        elif isinstance(expr, caos_ast.BinOp):
            left = self.reorder_aggregates(expr.left)
            right = self.reorder_aggregates(expr.right)

            left_aggregates = self.is_aggregated_expr(left)
            right_aggregates = self.is_aggregated_expr(right)

            if (left_aggregates and (right_aggregates or isinstance(right, caos_ast.Constant))) \
               or (isinstance(left, caos_ast.Constant) and right_aggregates):
                expr.aggregates = True

            elif expr.op == ast.ops.AND:
                if right_aggregates:
                    # Reorder the operands so that aggregate expr is always on the left
                    expr.left, expr.right = expr.right, expr.left

            elif left_aggregates or right_aggregates:
                raise TreeError('invalid expression mix of aggregates and non-aggregates')

        elif isinstance(expr, caos_ast.UnaryOp):
            self.reorder_aggregates(expr.expr)

        elif isinstance(expr, caos_ast.ExistPred):
            self.reorder_aggregates(expr.expr)

        elif isinstance(expr, caos_ast.NoneTest):
            self.reorder_aggregates(expr.expr)

        elif isinstance(expr, caos_ast.TypeCast):
            self.reorder_aggregates(expr.expr)

        elif isinstance(expr, (caos_ast.BaseRef, caos_ast.Constant, caos_ast.InlineFilter,
                               caos_ast.EntitySet, caos_ast.InlinePropFilter,
                               caos_ast.EntityLink)):
            pass

        elif isinstance(expr, caos_ast.PathCombination):
            for p in expr.paths:
                self.reorder_aggregates(p)

        elif isinstance(expr, (caos_ast.Sequence, caos_ast.Record)):
            has_agg_elems = False
            for item in expr.elements:
                self.reorder_aggregates(item)
                if self.is_aggregated_expr(item):
                    has_agg_elems = True
                elif has_agg_elems and not isinstance(expr, caos_ast.Constant):
                    raise TreeError('invalid expression mix of aggregates and non-aggregates')

            if has_agg_elems:
                expr.aggregates = True

        elif isinstance(expr, caos_ast.GraphExpr):
            pass

        elif isinstance(expr, caos_ast.SubgraphRef):
            pass

        elif isinstance(expr, caos_ast.SearchVector):
            for ref in expr.items:
                self.reorder_aggregates(ref.ref)

        else:
            # All other nodes fall through
            assert False, 'unexpected node "%r"' % expr

        return expr


    def build_paths_index(self, graph):
        paths = self.extract_paths(graph, reverse=True, resolve_arefs=False,
                                   recurse_subqueries=1, all_fragments=True)

        if isinstance(paths, caos_ast.PathCombination):
            self.flatten_path_combination(paths, recursive=True)
            paths = paths.paths
        else:
            paths = [paths]

        path_idx = datastructures.Multidict()
        for path in paths:
            if isinstance(path, caos_ast.EntitySet):
                path_idx.add(path.id, path)

        return path_idx


    def link_subqueries(self, expr):
        if isinstance(expr, caos_ast.FunctionCall):
            for arg in expr.args:
                self.link_subqueries(arg)

        elif isinstance(expr, caos_ast.BinOp):
            self.link_subqueries(expr.left)
            self.link_subqueries(expr.right)

        elif isinstance(expr, caos_ast.UnaryOp):
            self.link_subqueries(expr.expr)

        elif isinstance(expr, caos_ast.ExistPred):
            self.link_subqueries(expr.expr)

        elif isinstance(expr, caos_ast.NoneTest):
            self.link_subqueries(expr.expr)

        elif isinstance(expr, caos_ast.TypeCast):
            self.link_subqueries(expr.expr)

        elif isinstance(expr, (caos_ast.BaseRef, caos_ast.Constant, caos_ast.InlineFilter,
                               caos_ast.EntitySet, caos_ast.InlinePropFilter,
                               caos_ast.EntityLink)):
            pass

        elif isinstance(expr, caos_ast.PathCombination):
            for p in expr.paths:
                self.link_subqueries(p)

        elif isinstance(expr, (caos_ast.Sequence, caos_ast.Record)):
            for item in expr.elements:
                self.link_subqueries(item)

        elif isinstance(expr, caos_ast.GraphExpr):
            paths = self.build_paths_index(expr)

            subpaths = self.extract_paths(expr, reverse=True, resolve_arefs=False,
                                          recurse_subqueries=2, all_fragments=True)

            if isinstance(subpaths, caos_ast.PathCombination):
                self.flatten_path_combination(subpaths, recursive=True)
                subpaths = subpaths.paths
            else:
                subpaths = {subpaths}

            for subpath in subpaths:
                if isinstance(subpath, caos_ast.EntitySet):
                    outer = paths.getlist(subpath.id)

                    if outer and subpath not in outer:
                        subpath.reference = outer[0]

            if expr.generator:
                self.link_subqueries(expr.generator)

            if expr.selector:
                for e in expr.selector:
                    self.link_subqueries(e.expr)

            if expr.grouper:
                for e in expr.grouper:
                    self.link_subqueries(e)

            if expr.sorter:
                for e in expr.sorter:
                    self.link_subqueries(e)

            if expr.set_op:
                self.link_subqueries(expr.set_op_larg)
                self.link_subqueries(expr.set_op_rarg)

        elif isinstance(expr, caos_ast.SubgraphRef):
            self.link_subqueries(expr.ref)

        elif isinstance(expr, caos_ast.SortExpr):
            self.link_subqueries(expr.expr)

        elif isinstance(expr, caos_ast.SelectorExpr):
            self.link_subqueries(expr.expr)

        elif isinstance(expr, caos_ast.SearchVector):
            for ref in expr.items:
                self.link_subqueries(ref.ref)

        else:
            # All other nodes fall through
            assert False, 'unexpected node "%r"' % expr

        return expr

    def postprocess_expr(self, expr):
        paths = self.extract_paths(expr, reverse=True)

        if paths:
            if isinstance(paths, caos_ast.PathCombination):
                paths = paths.paths
            else:
                paths = {paths}

            for path in paths:
                self._postprocess_expr(path)

    def _postprocess_expr(self, expr):
        if isinstance(expr, caos_ast.EntitySet):
            if self.context.current.location == 'generator':
                if len(expr.disjunction.paths) == 1 and len(expr.conjunction.paths) == 0 \
                                                    and not expr.disjunction.fixed:
                    # Generator by default produces strong paths, that must limit every other
                    # path in the query.  However, to accommodate for possible disjunctions
                    # in generator expressions, links are put into disjunction.  If, in fact,
                    # there was not disjunctive expressions in generator, the link must
                    # be turned into conjunction, but only if the disjunction was not merged
                    # from a weak binary op, like OR.
                    #
                    expr.conjunction = caos_ast.Conjunction(paths=expr.disjunction.paths)
                    expr.disjunction = caos_ast.Disjunction()

            for path in expr.conjunction.paths:
                self._postprocess_expr(path)

            for path in expr.disjunction.paths:
                self._postprocess_expr(path)

        elif isinstance(expr, caos_ast.PathCombination):
            for path in expr.paths:
                self._postprocess_expr(path)

        elif isinstance(expr, caos_ast.EntityLink):
            if expr.target:
                self._postprocess_expr(expr.target)

        elif isinstance(expr, caos_ast.BaseRef):
            pass

        else:
            assert False, "Unexpexted expression: %s" % expr

    @classmethod
    def _is_weak_op(self, op):
        return op in (ast.ops.OR, ast.ops.IN, ast.ops.NOT_IN)

    def is_weak_op(self, op):
        return self._is_weak_op(op) or self.context.current.location != 'generator'

    def merge_paths(self, expr):
        if isinstance(expr, caos_ast.AtomicRefExpr):
            if self.context.current.location == 'generator' and expr.inline:
                expr.ref.filter = self.extend_binop(expr.ref.filter, expr.expr)
                self.merge_paths(expr.ref)
                arefs = ast.find_children(expr, lambda i: isinstance(i, caos_ast.AtomicRefSimple))
                for aref in arefs:
                    self.merge_paths(aref)
                expr = caos_ast.InlineFilter(expr=expr.ref.filter, ref=expr.ref)
            else:
                self.merge_paths(expr.expr)

        elif isinstance(expr, caos_ast.LinkPropRefExpr):
            if self.context.current.location == 'generator' and expr.inline:
                prefs = ast.find_children(expr.expr, lambda i:
                                            (isinstance(i, caos_ast.LinkPropRefSimple)
                                             and i.ref == expr.ref))
                expr.ref.proprefs.update(prefs)
                expr.ref.propfilter = self.extend_binop(expr.ref.propfilter, expr.expr)
                if expr.ref.target:
                    self.merge_paths(expr.ref.target)
                else:
                    self.merge_paths(expr.ref.source)
                expr = caos_ast.InlinePropFilter(expr=expr.ref.propfilter, ref=expr.ref)
            else:
                self.merge_paths(expr.expr)

        elif isinstance(expr, caos_ast.BinOp):
            left = self.merge_paths(expr.left)
            right = self.merge_paths(expr.right)

            weak_op = self.is_weak_op(expr.op)

            if weak_op:
                combination = caos_ast.Disjunction
            else:
                combination = caos_ast.Conjunction

            paths = set()
            for operand in (left, right):
                if isinstance(operand, (caos_ast.InlineFilter, caos_ast.AtomicRefSimple)):
                    paths.add(operand.ref)
                else:
                    paths.add(operand)

            e = combination(paths=frozenset(paths))
            merge_filters = self.context.current.location != 'generator' or weak_op
            if merge_filters:
                merge_filters = expr.op
            self.flatten_and_unify_path_combination(e, deep=False, merge_filters=merge_filters)

            if len(e.paths) > 1:
                expr = caos_ast.BinOp(left=left, op=expr.op, right=right, aggregates=expr.aggregates)
            else:
                expr = next(iter(e.paths))

        elif isinstance(expr, caos_ast.UnaryOp):
            expr.expr = self.merge_paths(expr.expr)

        elif isinstance(expr, caos_ast.ExistPred):
            expr.expr = self.merge_paths(expr.expr)

        elif isinstance(expr, caos_ast.TypeCast):
            expr.expr = self.merge_paths(expr.expr)

        elif isinstance(expr, caos_ast.NoneTest):
            expr.expr = self.merge_paths(expr.expr)

        elif isinstance(expr, caos_ast.PathCombination):
            expr = self.flatten_and_unify_path_combination(expr, deep=True)

        elif isinstance(expr, caos_ast.MetaRef):
            expr.ref.metarefs.add(expr)

        elif isinstance(expr, caos_ast.AtomicRefSimple):
            expr.ref.atomrefs.add(expr)

        elif isinstance(expr, caos_ast.LinkPropRefSimple):
            expr.ref.proprefs.add(expr)

        elif isinstance(expr, caos_ast.EntitySet):
            if expr.rlink:
                self.merge_paths(expr.rlink.source)

        elif isinstance(expr, caos_ast.EntityLink):
            if expr.source:
                self.merge_paths(expr.source)

        elif isinstance(expr, (caos_ast.InlineFilter, caos_ast.Constant, caos_ast.InlinePropFilter)):
            pass

        elif isinstance(expr, caos_ast.FunctionCall):
            args = []
            for arg in expr.args:
                args.append(self.merge_paths(arg))

            for sortexpr in expr.agg_sort:
                self.merge_paths(sortexpr.expr)

            for partition_expr in expr.partition:
                self.merge_paths(partition_expr)

            if len(args) > 1 or expr.agg_sort or expr.partition:
                # Make sure that function args are properly merged against each other.
                # This is simply a matter of unification of the conjunction of paths
                # generated by function argument expressions.
                #
                paths = []
                for arg in args:
                    path = self.extract_paths(arg, reverse=True)
                    if path:
                        paths.append(path)
                for sortexpr in expr.agg_sort:
                    path = self.extract_paths(sortexpr, reverse=True)
                    if path:
                        paths.append(path)
                for partition_expr in expr.partition:
                    path = self.extract_paths(partition_expr, reverse=True)
                    if path:
                        paths.append(path)
                e = caos_ast.Conjunction(paths=frozenset(paths))
                self.flatten_and_unify_path_combination(e)

            expr = expr.__class__(name=expr.name, args=args, aggregates=expr.aggregates,
                                  kwargs=expr.kwargs, agg_sort=expr.agg_sort,
                                  window=expr.window, partition=expr.partition)

        elif isinstance(expr, (caos_ast.Sequence, caos_ast.Record)):
            elements = []
            for element in expr.elements:
                elements.append(self.merge_paths(element))

            self.unify_paths(paths=elements, mode=caos_ast.Disjunction)

            if isinstance(expr, caos_ast.Record):
                expr = expr.__class__(elements=elements, concept=expr.concept,
                                      rlink=expr.rlink)
            else:
                expr = expr.__class__(elements=elements)

        elif isinstance(expr, caos_ast.GraphExpr):
            pass

        elif isinstance(expr, caos_ast.SubgraphRef):
            pass

        elif isinstance(expr, caos_ast.SearchVector):
            refs = []
            for elem in expr.items:
                elem.ref = self.merge_paths(elem.ref)
                refs.append(elem.ref)

            if len(refs) > 1:
                # Make sure that all refs in search vector are merged with each other
                # properly.
                #
                paths = []
                for ref in refs:
                    path = self.extract_paths(ref, reverse=True)
                    if path:
                        paths.append(path)
                e = caos_ast.Conjunction(paths=frozenset(paths))
                self.flatten_and_unify_path_combination(e)

        else:
            assert False, 'unexpected node "%r"' % expr

        return expr

    @classmethod
    def flatten_path_combination(cls, expr, recursive=False):
        paths = set()
        for path in expr.paths:
            if isinstance(path, expr.__class__) or \
                        (recursive and isinstance(path, caos_ast.PathCombination)):
                if recursive:
                    cls.flatten_path_combination(path, recursive=True)
                    paths.update(path.paths)
                else:
                    paths.update(path.paths)
            else:
                paths.add(path)

        expr.paths = frozenset(paths)
        return expr

    def flatten_and_unify_path_combination(self, expr, deep=False, merge_filters=False):
        ##
        # Flatten nested disjunctions and conjunctions since they are associative
        #
        assert isinstance(expr, caos_ast.PathCombination)

        self.flatten_path_combination(expr)

        if deep:
            newpaths = set()
            for path in expr.paths:
                path = self.merge_paths(path)
                newpaths.add(path)

            expr = expr.__class__(paths=frozenset(newpaths))

        self.unify_paths(expr.paths, mode=expr.__class__, merge_filters=merge_filters)

        """LOG [caos.graph.merge] UNIFICATION RESULT
        self._dump(expr)
        """

        expr.paths = frozenset(p for p in expr.paths)
        return expr

    nest = 0

    def unify_paths(self, paths, mode, reverse=True, merge_filters=False):
        mypaths = set(paths)

        result = None

        while mypaths and not result:
            result = self.extract_paths(mypaths.pop(), reverse)

        return self._unify_paths(result, mypaths, mode, reverse, merge_filters)

    @debug.debug
    def _unify_paths(self, result, paths, mode, reverse=True, merge_filters=False):
        mypaths = set(paths)

        while mypaths:
            path = self.extract_paths(mypaths.pop(), reverse)

            if not path or result is path:
                continue

            if issubclass(mode, caos_ast.Disjunction):
                """LOG [caos.graph.merge] ADDING
                print(' ' * self.nest, 'ADDING', result, path, getattr(result, 'id', '??'), getattr(path, 'id', '??'), merge_filters)
                self.nest += 2

                self._dump(result)
                self._dump(path)
                """

                result = self.add_paths(result, path, merge_filters=merge_filters)
                assert result

                """LOG [caos.graph.merge] ADDITION RESULT
                self.nest -= 2
                self._dump(result)
                """
            else:
                """LOG [caos.graph.merge] INTERSECTING
                print(' ' * self.nest, result, path, getattr(result, 'id', '??'), getattr(path, 'id', '??'), merge_filters)
                self.nest += 2
                """

                result = self.intersect_paths(result, path, merge_filters=merge_filters)
                assert result

                """LOG [caos.graph.merge] INTERSECTION RESULT
                self._dump(result)
                self.nest -= 2
                """

        return result

    def miniterms_from_conjunctions(self, paths):
        variables = collections.OrderedDict()

        terms = []

        for path in paths:
            term = 0

            if isinstance(path, caos_ast.Conjunction):
                for subpath in path.paths:
                    if subpath not in variables:
                        variables[subpath] = len(variables)
                    term += 1 << variables[subpath]

            elif isinstance(path, caos_ast.EntityLink):
                if path not in variables:
                    variables[path] = len(variables)
                term += 1 << variables[path]

            terms.append(term)

        return list(variables), boolean.ints_to_terms(*terms)

    def conjunctions_from_miniterms(self, terms, variables):
        paths = set()

        for term in terms:
            conjpaths = [variables[i] for i, bit in enumerate(term) if bit]
            if len(conjpaths) > 1:
                paths.add(caos_ast.Conjunction(paths=frozenset(conjpaths)))
            else:
                paths.add(conjpaths[0])
        return paths

    def minimize_disjunction(self, paths):
        variables, miniterms = self.miniterms_from_conjunctions(paths)
        minimized = boolean.minimize(miniterms)
        paths = self.conjunctions_from_miniterms(minimized, variables)
        result = caos_ast.Disjunction(paths=frozenset(paths))
        return result

    def add_sets(self, left, right, merge_filters=False):
        if left is right:
            return left

        if merge_filters:
            if isinstance(merge_filters, ast.ops.Operator) and self.is_weak_op(merge_filters):
                merge_op = ast.ops.OR
            else:
                merge_op = ast.ops.AND

        match = self.match_prefixes(left, right, ignore_filters=merge_filters)
        if match:
            if isinstance(left, caos_ast.EntityLink):
                left_link = left
                left = left.target
            else:
                left_link = left.rlink

            if isinstance(right, caos_ast.EntityLink):
                right_link = right
                right = right.target
            else:
                right_link = right.rlink

            if left_link:
                self.fixup_refs([right_link], left_link)
                if merge_filters and right_link.propfilter:
                    left_link.propfilter = self.extend_binop(left_link.propfilter,
                                                             right_link.propfilter, op=merge_op)

                left_link.proprefs.update(right_link.proprefs)
                left_link.users.update(right_link.users)
                if right_link.target:
                    left_link.target = right_link.target

            if left and right:
                self.fixup_refs([right], left)

                if merge_filters and right.filter:
                    left.filter = self.extend_binop(left.filter, right.filter, op=ast.ops.AND)

                if merge_filters:
                    paths_left = set()
                    for dpath in right.disjunction.paths:
                        if isinstance(dpath, (caos_ast.EntitySet, caos_ast.EntityLink)):
                            merged = self.intersect_paths(left.conjunction, dpath, merge_filters)
                            if merged is not left.conjunction:
                                paths_left.add(dpath)
                        else:
                            paths_left.add(dpath)
                    right.disjunction = caos_ast.Disjunction(paths=frozenset(paths_left))

                left.disjunction = self.add_paths(left.disjunction,
                                                  right.disjunction, merge_filters)

                if merge_filters and merge_op == ast.ops.OR:
                    left.disjunction.fixed = True

                left.atomrefs.update(right.atomrefs)
                left.metarefs.update(right.metarefs)
                left.users.update(right.users)
                left.joins.update(right.joins)
                left.joins.discard(left)

                if left.origin is None and right.origin is not None:
                    left.origin = right.origin

                if right.concept.issubclass(left.concept):
                    left.concept = right.concept

                if merge_filters:
                    left.conjunction = self.intersect_paths(left.conjunction,
                                                            right.conjunction, merge_filters)

                    # If greedy disjunction merging is requested, we must also try to
                    # merge disjunctions.
                    paths = frozenset(left.conjunction.paths) | frozenset(left.disjunction.paths)
                    self.unify_paths(paths, caos_ast.Conjunction, reverse=False, merge_filters=merge_filters)
                    left.disjunction.paths = left.disjunction.paths - left.conjunction.paths
                else:
                    conjunction = self.add_paths(left.conjunction, right.conjunction, merge_filters)
                    if conjunction.paths:
                        left.disjunction.update(conjunction)
                    left.conjunction.paths = frozenset()

            if isinstance(left, caos_ast.EntitySet):
                return left
            elif isinstance(right, caos_ast.EntitySet):
                return right
            else:
                return left_link
        else:
            result = caos_ast.Disjunction(paths=frozenset((left, right)))

        return result

    def add_to_disjunction(self, disjunction, path, merge_filters):
        # Other operand is a disjunction -- look for path we can merge with,
        # if not found, append to disjunction.
        for dpath in disjunction.paths:
            if isinstance(dpath, (caos_ast.EntityLink, caos_ast.EntitySet)):
                merge = self.add_sets(dpath, path, merge_filters)
                if merge is dpath:
                    break
        else:
            disjunction.update(path)

        return disjunction

    def add_to_conjunction(self, conjunction, path, merge_filters):
        result = None
        if merge_filters:
            for cpath in conjunction.paths:
                if isinstance(cpath, (caos_ast.EntityLink, caos_ast.EntitySet)):
                    merge = self.add_sets(cpath, path, merge_filters)
                    if merge is cpath:
                        result = conjunction
                        break

        if not result:
            result = caos_ast.Disjunction(paths=frozenset({conjunction, path}))

        return result

    def add_disjunctions(self, left, right, merge_filters=False):
        result = caos_ast.Disjunction()
        result.update(left)
        result.update(right)

        if len(result.paths) > 1:
            self.unify_paths(result.paths, mode=result.__class__, reverse=False,
                             merge_filters=merge_filters)
            result.paths = frozenset(p for p in result.paths)

        return result

    def add_conjunction_to_disjunction(self, disjunction, conjunction):
        if disjunction.paths and conjunction.paths:
            return caos_ast.Disjunction(paths=frozenset({disjunction, conjunction}))
        elif disjunction.paths:
            return disjunction
        elif conjunction.paths:
            return caos_ast.Disjunction(paths=frozenset({conjunction}))
        else:
            return caos_ast.Disjunction()

    def add_conjunctions(self, left, right):
        paths = frozenset(p for p in (left, right) if p.paths)
        return caos_ast.Disjunction(paths=paths)

    def add_paths(self, left, right, merge_filters=False):
        if isinstance(left, (caos_ast.EntityLink, caos_ast.EntitySet)):
            if isinstance(right, (caos_ast.EntityLink, caos_ast.EntitySet)):
                # Both operands are sets -- simply merge them
                result = self.add_sets(left, right, merge_filters)

            elif isinstance(right, caos_ast.Disjunction):
                result = self.add_to_disjunction(right, left, merge_filters)

            elif isinstance(right, caos_ast.Conjunction):
                result = self.add_to_conjunction(right, left, merge_filters)

        elif isinstance(left, caos_ast.Disjunction):
            if isinstance(right, (caos_ast.EntityLink, caos_ast.EntitySet)):
                result = self.add_to_disjunction(left, right, merge_filters)

            elif isinstance(right, caos_ast.Disjunction):
                result = self.add_disjunctions(left, right, merge_filters)

            elif isinstance(right, caos_ast.Conjunction):
                result = self.add_conjunction_to_disjunction(left, right)

        elif isinstance(left, caos_ast.Conjunction):
            if isinstance(right, (caos_ast.EntityLink, caos_ast.EntitySet)):
                result = self.add_to_conjunction(left, right, merge_filters)

            elif isinstance(right, caos_ast.Disjunction):
                result = self.add_conjunction_to_disjunction(right, left)

            elif isinstance(right, caos_ast.Conjunction):
                result = self.add_conjunctions(left, right)

        else:
            assert False, 'unexpected nodes "{!r}", "{!r}"'.format(left, right)

        return result


    def intersect_sets(self, left, right, merge_filters=False):
        if left is right:
            return left

        match = self.match_prefixes(left, right, ignore_filters=True)
        if match:
            if isinstance(left, caos_ast.EntityLink):
                left_set = left.target
                right_set = right.target
                left_link = left
                right_link = right
            else:
                left_set = left
                right_set = right
                left_link = left.rlink
                right_link = right.rlink

            if left_link:
                self.fixup_refs([right_link], left_link)
                if right_link.propfilter:
                    left_link.propfilter = self.extend_binop(left_link.propfilter,
                                                             right_link.propfilter, op=ast.ops.AND)

                left_link.proprefs.update(right_link.proprefs)
                left_link.users.update(right_link.users)
                if right_link.target:
                    left_link.target = right_link.target

            if right_set and left_set:
                self.fixup_refs([right_set], left_set)

                if right_set.filter:
                    left_set.filter = self.extend_binop(left_set.filter, right_set.filter,
                                                        op=ast.ops.AND)

                left_set.conjunction = self.intersect_paths(left_set.conjunction,
                                                            right_set.conjunction, merge_filters)
                left_set.atomrefs.update(right_set.atomrefs)
                left_set.metarefs.update(right_set.metarefs)
                left_set.users.update(right_set.users)
                left_set.joins.update(right_set.joins)
                left_set.joins.discard(left_set)

                if left_set.origin is None and right_set.origin is not None:
                    left_set.origin = right_set.origin

                if right_set.concept.issubclass(left_set.concept):
                    left_set.concept = right_set.concept

                disjunction = self.intersect_paths(left_set.disjunction,
                                                   right_set.disjunction, merge_filters)

                left_set.disjunction = caos_ast.Disjunction()

                if isinstance(disjunction, caos_ast.Disjunction):
                    self.unify_paths(left_set.conjunction.paths | disjunction.paths,
                                     caos_ast.Conjunction, reverse=False,
                                     merge_filters=merge_filters)

                    left_set.disjunction = disjunction

                    if len(left_set.disjunction.paths) == 1:
                        first_disj = next(iter(left_set.disjunction.paths))
                        if isinstance(first_disj, caos_ast.Conjunction):
                            left_set.conjunction = first_disj
                            left_set.disjunction = caos_ast.Disjunction()

                elif disjunction.paths:
                    left_set.conjunction = self.intersect_paths(left_set.conjunction,
                                                                disjunction, merge_filters)

                    self.flatten_path_combination(left_set.conjunction)

                    if len(left_set.conjunction.paths) == 1:
                        first_conj = next(iter(left_set.conjunction.paths))
                        if isinstance(first_conj, caos_ast.Disjunction):
                            left_set.disjunction = first_conj
                            left_set.conjunction = caos_ast.Conjunction()

            if isinstance(left, caos_ast.EntitySet):
                return left
            elif isinstance(right, caos_ast.EntitySet):
                return right
            else:
                return left_link

        else:
            result = caos_ast.Conjunction(paths=frozenset({left, right}))

        return result

    def intersect_with_disjunction(self, disjunction, path):
        result = caos_ast.Conjunction(paths=frozenset((disjunction, path)))
        return result

    def intersect_with_conjunction(self, conjunction, path):
        # Other operand is a disjunction -- look for path we can merge with,
        # if not found, append to conjunction.
        for cpath in conjunction.paths:
            if isinstance(cpath, (caos_ast.EntityLink, caos_ast.EntitySet)):
                merge = self.intersect_sets(cpath, path)
                if merge is cpath:
                    break
        else:
            conjunction = caos_ast.Conjunction(paths=frozenset(conjunction.paths | {path}))

        return conjunction

    def intersect_conjunctions(self, left, right, merge_filters=False):
        result = caos_ast.Conjunction(paths=left.paths)
        result.update(right)

        if len(result.paths) > 1:
            self.flatten_path_combination(result)
            self.unify_paths(result.paths, mode=result.__class__, reverse=False,
                             merge_filters=merge_filters)
            result.paths = frozenset(p for p in result.paths)

        return result

    def intersect_disjunctions(self, left, right):
        """Produce a conjunction of two disjunctions"""

        if left.paths and right.paths:
            # (a | b) & (c | d) --> a & c | a & d | b & c | b & d
            # We unroll the expression since it is highly probable that
            # the resulting conjunctions will merge and we'll get a simpler
            # expression which is we further attempt to minimize using boolean
            # minimizer.
            #
            paths = set()

            for l in left.paths:
                for r in right.paths:
                    paths.add(self.intersect_paths(l, r))

            result = self.minimize_disjunction(paths)
            return result

        else:
            # Degenerate case
            if not left.paths:
                paths = right.paths
                fixed = right.fixed
            elif not right.paths:
                paths = left.paths
                fixed = left.fixed

            if len(paths) <= 1 and not fixed:
                return caos_ast.Conjunction(paths=frozenset(paths))
            else:
                return caos_ast.Disjunction(paths=frozenset(paths), fixed=fixed)

    def intersect_disjunction_with_conjunction(self, disjunction, conjunction):
        if disjunction.paths and conjunction.paths:
            return caos_ast.Disjunction(paths=frozenset({disjunction, conjunction}))
        elif conjunction.paths:
            return conjunction
        elif disjunction.paths:
            return caos_ast.Conjunction(paths=frozenset({disjunction}))
        else:
            return caos_ast.Conjunction()

    def intersect_paths(self, left, right, merge_filters=False):
        if isinstance(left, (caos_ast.EntityLink, caos_ast.EntitySet)):
            if isinstance(right, (caos_ast.EntityLink, caos_ast.EntitySet)):
                # Both operands are sets -- simply merge them
                result = self.intersect_sets(left, right, merge_filters)

            elif isinstance(right, caos_ast.Disjunction):
                result = self.intersect_with_disjunction(right, left)

            elif isinstance(right, caos_ast.Conjunction):
                result = self.intersect_with_conjunction(right, left)

        elif isinstance(left, caos_ast.Disjunction):
            if isinstance(right, (caos_ast.EntityLink, caos_ast.EntitySet)):
                result = self.intersect_with_disjunction(left, right)

            elif isinstance(right, caos_ast.Disjunction):
                result = self.intersect_disjunctions(left, right)

            elif isinstance(right, caos_ast.Conjunction):
                result = self.intersect_disjunction_with_conjunction(left, right)

        elif isinstance(left, caos_ast.Conjunction):
            if isinstance(right, (caos_ast.EntityLink, caos_ast.EntitySet)):
                result = self.intersect_with_conjunction(left, right)

            elif isinstance(right, caos_ast.Disjunction):
                result = self.intersect_disjunction_with_conjunction(right, left)

            elif isinstance(right, caos_ast.Conjunction):
                result = self.intersect_conjunctions(left, right, merge_filters)

        return result

    @debug.debug
    def match_prefixes(self, our, other, ignore_filters):
        result = None

        if isinstance(our, caos_ast.EntityLink):
            link = our
            our_node = our.target
            if our_node is None:
                our_id = caos_utils.LinearPath(our.source.id)
                our_id.add(link.link_proto, link.direction, None)
                our_node = our.source
            else:
                our_id = our_node.id
        else:
            link = None
            our_node = our
            our_id = our.id

        if isinstance(other, caos_ast.EntityLink):
            other_link = other
            other_node = other.target
            if other_node is None:
                other_node = other.source
                other_id = caos_utils.LinearPath(other.source.id)
                other_id.add(other_link.link_proto, other_link.direction, None)
            else:
                other_id = other_node.id
        else:
            other_link = None
            other_node = other
            other_id = other.id

        if our_id[-1] is None and other_id[-1] is not None:
            other_id = caos_utils.LinearPath(other_id)
            other_id[-1] = None

        if other_id[-1] is None and our_id[-1] is not None:
            our_id = caos_utils.LinearPath(our_id)
            our_id[-1] = None


        """LOG [caos.graph.merge] MATCH PREFIXES
        print(' ' * self.nest, our, other, ignore_filters)
        print(' ' * self.nest, '   PATHS: ', our_id)
        print(' ' * self.nest, '      *** ', other_id)
        print(' ' * self.nest, '       EQ ', our_id == other_id)
        """

        ok = ((our_node is None and other_node is None) or
              (our_node is not None and other_node is not None and
                (our_id == other_id
                 and our_node.pathvar == other_node.pathvar
                 and (ignore_filters or (not our_node.filter and not other_node.filter
                                         and not our_node.conjunction.paths
                                         and not other_node.conjunction.paths))))
              and (not link or (link.link_proto == other_link.link_proto
                                and link.direction == other_link.direction)))

        if ok:
            if other_link:
                result = other_link
            else:
                result = other_node

        """LOG [caos.graph.merge] MATCH PREFIXES RESULT
        print(' ' * self.nest, '    ----> ', result)
        """

        return result

    def fixup_refs(self, refs, newref):
        caos_ast.Base.fixup_refs(refs, newref)

    @classmethod
    def extract_paths(cls, path, reverse=False, resolve_arefs=True, recurse_subqueries=0,
                                 all_fragments=False, extract_subgraph_refs=False):
        if isinstance(path, caos_ast.GraphExpr):
            if recurse_subqueries <= 0:
                return None
            else:
                paths = set()

                recurse_subqueries -= 1

                if path.generator:
                    normalized = cls.extract_paths(path.generator, reverse, resolve_arefs,
                                                   recurse_subqueries, all_fragments,
                                                   extract_subgraph_refs)
                    if normalized:
                        paths.add(normalized)

                for part in ('selector', 'grouper', 'sorter'):
                    e = getattr(path, part)
                    if e:
                        for p in e:
                            normalized = cls.extract_paths(p, reverse, resolve_arefs,
                                                           recurse_subqueries,
                                                           all_fragments,
                                                           extract_subgraph_refs)
                            if normalized:
                                paths.add(normalized)

                if path.set_op:
                    for arg in (path.set_op_larg, path.set_op_rarg):
                        normalized = cls.extract_paths(arg, reverse, resolve_arefs,
                                                       recurse_subqueries, all_fragments,
                                                       extract_subgraph_refs)
                        if normalized:
                            paths.add(normalized)

                if len(paths) == 1:
                    return next(iter(paths))
                elif len(paths) == 0:
                    return None
                else:
                    result = caos_ast.Disjunction(paths=frozenset(paths))
                    return cls.flatten_path_combination(result)

        elif isinstance(path, caos_ast.SubgraphRef):
            if not recurse_subqueries and extract_subgraph_refs:
                return path
            else:
                return cls.extract_paths(path.ref, reverse, resolve_arefs, recurse_subqueries,
                                         all_fragments, extract_subgraph_refs)

        elif isinstance(path, caos_ast.SelectorExpr):
            return cls.extract_paths(path.expr, reverse, resolve_arefs, recurse_subqueries,
                                     all_fragments, extract_subgraph_refs)

        elif isinstance(path, caos_ast.SortExpr):
            return cls.extract_paths(path.expr, reverse, resolve_arefs, recurse_subqueries,
                                     all_fragments, extract_subgraph_refs)

        elif isinstance(path, (caos_ast.EntitySet, caos_ast.InlineFilter, caos_ast.AtomicRef)):
            if isinstance(path, (caos_ast.InlineFilter, caos_ast.AtomicRef)) and \
                                                    (resolve_arefs or reverse):
                result = path.ref
            else:
                result = path

            if isinstance(result, caos_ast.EntitySet):
                if reverse:
                    paths = []
                    paths.append(result)

                    while result.rlink:
                        result = result.rlink.source
                        paths.append(result)

                    if len(paths) == 1 or not all_fragments:
                        result = paths[-1]
                    else:
                        result = caos_ast.Disjunction(paths=frozenset(paths))

            return result

        elif isinstance(path, caos_ast.InlinePropFilter):
            return cls.extract_paths(path.ref, reverse, resolve_arefs, recurse_subqueries,
                                     all_fragments, extract_subgraph_refs)

        elif isinstance(path, caos_ast.LinkPropRef):
            if resolve_arefs or reverse:
                return cls.extract_paths(path.ref, reverse, resolve_arefs, recurse_subqueries,
                                         all_fragments, extract_subgraph_refs)
            else:
                return path

        elif isinstance(path, caos_ast.EntityLink):
            if reverse:
                result = path
                if path.source:
                    result = path.source
                    while result.rlink:
                        result = result.rlink.source
            else:
                result = path
            return result

        elif isinstance(path, caos_ast.PathCombination):
            result = set()
            for p in path.paths:
                normalized = cls.extract_paths(p, reverse, resolve_arefs, recurse_subqueries,
                                               all_fragments, extract_subgraph_refs)
                if normalized:
                    result.add(normalized)
            if len(result) == 1:
                return next(iter(result))
            elif len(result) == 0:
                return None
            else:
                return cls.flatten_path_combination(path.__class__(paths=frozenset(result)))

        elif isinstance(path, caos_ast.BinOp):
            combination = caos_ast.Disjunction if cls._is_weak_op(path.op) else caos_ast.Conjunction

            paths = set()
            for p in (path.left, path.right):
                normalized = cls.extract_paths(p, reverse, resolve_arefs, recurse_subqueries,
                                               all_fragments, extract_subgraph_refs)
                if normalized:
                    paths.add(normalized)

            if len(paths) == 1:
                return next(iter(paths))
            elif len(paths) == 0:
                return None
            else:
                return cls.flatten_path_combination(combination(paths=frozenset(paths)))

        elif isinstance(path, caos_ast.UnaryOp):
            return cls.extract_paths(path.expr, reverse, resolve_arefs, recurse_subqueries,
                                     all_fragments, extract_subgraph_refs)

        elif isinstance(path, caos_ast.ExistPred):
            return cls.extract_paths(path.expr, reverse, resolve_arefs, recurse_subqueries,
                                     all_fragments, extract_subgraph_refs)

        elif isinstance(path, caos_ast.TypeCast):
            return cls.extract_paths(path.expr, reverse, resolve_arefs, recurse_subqueries,
                                     all_fragments, extract_subgraph_refs)

        elif isinstance(path, caos_ast.NoneTest):
            return cls.extract_paths(path.expr, reverse, resolve_arefs, recurse_subqueries,
                                     all_fragments, extract_subgraph_refs)

        elif isinstance(path, caos_ast.FunctionCall):
            paths = set()
            for p in path.args:
                p = cls.extract_paths(p, reverse, resolve_arefs, recurse_subqueries,
                                      all_fragments, extract_subgraph_refs)
                if p:
                    paths.add(p)

            for p in path.agg_sort:
                p = cls.extract_paths(p, reverse, resolve_arefs, recurse_subqueries,
                                      all_fragments, extract_subgraph_refs)
                if p:
                    paths.add(p)

            for p in path.partition:
                p = cls.extract_paths(p, reverse, resolve_arefs, recurse_subqueries,
                                      all_fragments, extract_subgraph_refs)
                if p:
                    paths.add(p)

            if len(paths) == 1:
                return next(iter(paths))
            elif len(paths) == 0:
                return None
            else:
                return caos_ast.Conjunction(paths=frozenset(paths))

        elif isinstance(path, (caos_ast.Sequence, caos_ast.Record)):
            paths = set()
            for p in path.elements:
                p = cls.extract_paths(p, reverse, resolve_arefs, recurse_subqueries,
                                      all_fragments, extract_subgraph_refs)
                if p:
                    paths.add(p)

            if len(paths) == 1:
                return next(iter(paths))
            elif len(paths) == 0:
                return None
            else:
                return caos_ast.Disjunction(paths=frozenset(paths))

        elif isinstance(path, caos_ast.Constant):
            return None

        elif isinstance(path, caos_ast.SearchVector):
            paths = set()
            for p in path.items:
                p = cls.extract_paths(p.ref, reverse, resolve_arefs, recurse_subqueries,
                                      all_fragments, extract_subgraph_refs)
                if p:
                    paths.add(p)

            if len(paths) == 1:
                return next(iter(paths))
            elif len(paths) == 0:
                return None
            else:
                return caos_ast.Disjunction(paths=frozenset(paths))

        else:
            assert False, 'unexpected node "%r"' % path

    @classmethod
    def get_query_schema_scope(cls, tree):
        """Determine query scope, i.e the schema nodes it will potentially traverse"""

        entity_paths = ast.find_children(tree, lambda i: isinstance(i, caos_ast.EntitySet))

        return list({p for p in entity_paths if isinstance(p.concept, caos_types.ProtoConcept)})

    @classmethod
    def copy_path(cls, path: (caos_ast.EntitySet, caos_ast.EntityLink, caos_ast.BaseRef),
                       connect_to_origin=False):

        if isinstance(path, caos_ast.EntitySet):
            result = caos_ast.EntitySet(id=path.id, pathvar=path.pathvar,
                                        concept=path.concept,
                                        users=path.users, joins=path.joins,
                                        rewrite_flags=path.rewrite_flags.copy(),
                                        anchor=path.anchor,
                                        show_as_anchor=path.show_as_anchor)
            rlink = path.rlink

            if connect_to_origin:
                result.origin = path.origin if path.origin is not None else path

        elif isinstance(path, caos_ast.BaseRef):
            args = dict(id=path.id, ref=path.ref, ptr_proto=path.ptr_proto,
                        rewrite_flags=path.rewrite_flags.copy(),
                        pathvar=path.pathvar, anchor=path.anchor,
                        show_as_anchor=path.show_as_anchor)

            if isinstance(path, caos_ast.BaseRefExpr):
                args['expr'] = path.expr
                args['inline'] = path.inline

            result = path.__class__(**args)
            rlink = path.rlink

            if isinstance(path, (caos_ast.AtomicRefSimple, caos_ast.LinkPropRefSimple)):
                result.name = path.name
        else:
            result = None
            rlink = path

        current = result

        while rlink:
            link = caos_ast.EntityLink(target=current,
                                       link_proto=rlink.link_proto,
                                       direction=rlink.direction,
                                       propfilter=rlink.propfilter,
                                       users=rlink.users.copy(),
                                       pathvar=rlink.pathvar,
                                       anchor=rlink.anchor,
                                       show_as_anchor=rlink.show_as_anchor,
                                       rewrite_flags=rlink.rewrite_flags.copy(),
                                       pathspec_trigger=rlink.pathspec_trigger)

            if not result:
                result = link

            parent_path = rlink.source

            if parent_path:
                parent = caos_ast.EntitySet(id=parent_path.id,
                                            pathvar=parent_path.pathvar,
                                            anchor=parent_path.anchor,
                                            show_as_anchor=parent_path.show_as_anchor,
                                            concept=parent_path.concept,
                                            users=parent_path.users,
                                            joins=parent_path.joins,
                                            rewrite_flags=parent_path.rewrite_flags.copy())
                parent.disjunction = caos_ast.Disjunction(paths=frozenset((link,)))

                if connect_to_origin:
                    parent.origin = parent_path.origin if parent_path.origin is not None else parent_path

                link.source = parent

                if current:
                    current.rlink = link
                current = parent
                rlink = parent_path.rlink

            else:
                rlink = None

        return result

    def process_function_call(self, node):
        if node.name in (('search', 'rank'), ('search', 'headline')):
            if not isinstance(node.args[0], caos_ast.SearchVector):
                refs = set()
                for arg in node.args:
                    if isinstance(arg, caos_ast.EntitySet):
                        refs.add(arg)
                    else:
                        def testfn(n):
                            if isinstance(n, caos_ast.EntitySet):
                                return True
                            elif isinstance(n, caos_ast.SubgraphRef):
                                raise ast.SkipNode()

                        refs.update(ast.find_children(arg, testfn, force_traversal=True))

                assert len(refs) == 1

                ref = next(iter(refs))

                cols = []
                for link_name, link in ref.concept.get_searchable_links():
                    id = LinearPath(ref.id)
                    id.add(link, caos_types.OutboundDirection, link.target)
                    cols.append(caos_ast.AtomicRefSimple(ref=ref, name=link_name,
                                                         ptr_proto=link,
                                                         id=id))

                if not cols:
                    raise caos_error.CaosError('%s call on concept %s without any search configuration'\
                                               % (node.name, ref.concept.name),
                                               hint='Configure search for "%s"' % ref.concept.name)

                ref.atomrefs.update(cols)
                vector = caos_ast.Sequence(elements=cols)
            else:
                vector = node.args[0]

            node = caos_ast.FunctionCall(name=node.name,
                                         args=[vector, node.args[1]],
                                         kwargs=node.kwargs)

        elif node.name[0] == 'agg':
            node.aggregates = True

        elif node.name[0] == 'window':
            node.window = True

        elif node.name == 'type':
            if len(node.args) != 1:
                raise caos_error.CaosError('type() function takes exactly one argument, {} given'
                                           .format(len(node.args)))

            arg = next(iter(node.args))

            if isinstance(arg, caos_ast.Disjunction):
                arg = next(iter(arg.paths))
            elif not isinstance(arg, caos_ast.EntitySet):
                raise caos_error.CaosError('type() function only supports concept arguments')

            node = caos_ast.FunctionCall(name=node.name, args=[arg])
            return node

        if node.args:
            for arg in node.args:
                if not isinstance(arg, caos_ast.Constant):
                    break
            else:
                node = caos_ast.Constant(expr=node, type=node.args[0].type)

        return node

    def process_sequence(self, seq, squash_homogeneous=False):
        pathdict = {}
        proppathdict = {}
        elems = []

        const = True
        const_type = Void

        for elem in seq.elements:
            if isinstance(elem, (caos_ast.BaseRef, caos_ast.Disjunction)):
                if not isinstance(elem, caos_ast.Disjunction):
                    elem = caos_ast.Disjunction(paths=frozenset({elem}))
                elif len(elem.paths) > 1:
                    break

                pd = self.check_atomic_disjunction(elem, caos_ast.AtomicRef)
                if not pd:
                    pd = self.check_atomic_disjunction(elem, caos_ast.LinkPropRef)
                    if not pd:
                        break
                    proppathdict.update(pd)
                else:
                    pathdict.update(pd)

                if pathdict and proppathdict:
                    break

                elems.append(next(iter(elem.paths)))
                const = False
            elif const and isinstance(elem, caos_ast.Constant):
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
                    return caos_ast.Constant(expr=seq)
                else:
                    val = []

                    if isinstance(const_type, tuple) and const_type[0] == list:
                        for elem in seq.elements:
                            if elem.value is not None:
                                val.extend(elem.value)

                        val = tuple(val)
                    else:
                        val.extend(c.value for c in seq.elements if c.value is not None)

                        if len(val) == 1:
                            val = val[0]
                        elif len(val) == 0:
                            val = None
                        else:
                            val = tuple(val)
                            const_type = (list, const_type)

                    return caos_ast.Constant(value=val, type=const_type)
            else:
                if len(pathdict) == 1:
                    exprtype = caos_ast.AtomicRefExpr
                elif len(proppathdict) == 1:
                    exprtype = caos_ast.LinkPropRefExpr
                    pathdict = proppathdict
                else:
                    exprtype = None

                if exprtype:
                    # The sequence is composed of references to atoms of the same node
                    ref = list(pathdict.values())[0]

                    for elem in elems:
                        if elem.ref is not ref.ref:
                            elem.replace_refs([elem.ref], ref.ref, deep=True)

                    return exprtype(expr=caos_ast.Sequence(elements=elems))

        return seq

    def check_atomic_disjunction(self, expr, typ):
        """Check that all paths in disjunction are atom references.

           Return a dict mapping path prefixes to a corresponding node.
        """
        pathdict = {}
        for ref in expr.paths:
            # Check that refs in the operand are all atomic: non-atoms do not coerce
            # to literals.
            #
            if not isinstance(ref, typ):
                return None

            if isinstance(ref, caos_ast.AtomicRef):
                ref_id = ref.ref.id
            else:
                if not ref.id:
                    if ref.ref.target or ref.ref.source:
                        ref_id = ref.ref.target.id if ref.ref.target else ref.ref.source.id
                    else:
                        ref_id = LinearPath([ref.ref.link_proto])
                else:
                    ref_id = ref.id

            #assert not pathdict.get(ref_id)
            pathdict[ref_id] = ref
        return pathdict

    def process_binop(self, left, right, op):
        try:
            result = self._process_binop(left, right, op, reversed=False)
        except TreeError:
            result = self._process_binop(right, left, op, reversed=True)

        return result

    def is_join(self, left, right, op, reversed):
        return isinstance(left, caos_ast.Path) and isinstance(right, caos_ast.Path) and \
               op in (ast.ops.EQ, ast.ops.NE)

    def is_type_check(self, left, right, op, reversed):
        return not reversed and op in (ast.ops.IS, ast.ops.IS_NOT) and \
                isinstance(left, caos_ast.Path) and \
                isinstance(right, caos_ast.Constant) and \
                (isinstance(right.type, caos_types.PrototypeClass)
                 or isinstance(right.type, tuple) and
                    isinstance(right.type[1], caos_types.PrototypeClass))

    def is_concept_path(self, expr):
        if isinstance(expr, caos_ast.PathCombination):
            return all(self.is_concept_path(p) for p in expr.paths)
        elif isinstance(expr, caos_ast.EntitySet):
            return isinstance(expr.concept, caos_types.ProtoConcept)
        else:
            return False

    def is_const_idfilter(self, left, right, op, reversed):
        return self.is_concept_path(left) and isinstance(right, caos_ast.Constant) and \
                (op in (ast.ops.IN, ast.ops.NOT_IN) or \
                 (not reversed and op in (ast.ops.EQ, ast.ops.NE)))

    def is_constant(self, expr):
        flt = lambda node: isinstance(node, caos_ast.Path)
        paths = ast.visitor.find_children(expr, flt)
        return not paths and not isinstance(expr, caos_ast.Path)

    def get_multipath(self, expr:caos_ast.Path):
        if not isinstance(expr, caos_ast.PathCombination):
            expr = caos_ast.Disjunction(paths=frozenset((expr,)))
        return expr

    def path_from_set(self, paths):
        if len(paths) == 1:
            return next(iter(paths))
        else:
            return caos_ast.Disjunction(paths=frozenset(paths))

    def uninline(self, expr):
        cc = ast.visitor.find_children(expr, lambda i: isinstance(i, caos_ast.BaseRefExpr))
        for node in cc:
            node.inline = False
        if isinstance(expr, caos_ast.BaseRefExpr):
            expr.inline = False

    def _process_binop(self, left, right, op, reversed=False):
        result = None

        def newbinop(left, right, operation=None, uninline=False):
            operation = operation or op

            if uninline and not isinstance(operation, ast.ops.BooleanOperator):
                self.uninline(left)
                self.uninline(right)

            if reversed:
                return caos_ast.BinOp(left=right, op=operation, right=left)
            else:
                return caos_ast.BinOp(left=left, op=operation, right=right)

        left_paths = self.extract_paths(left, reverse=False, resolve_arefs=False,
                                              extract_subgraph_refs=True)

        if isinstance(left_paths, caos_ast.Path):
            # If both left and right operands are references to atoms of the same node,
            # or one of the operands is a reference to an atom and other is a constant,
            # then fold the expression into an in-line filter of that node.
            #

            left_exprs = self.get_multipath(left_paths)

            pathdict = self.check_atomic_disjunction(left_exprs, caos_ast.AtomicRef)
            proppathdict = self.check_atomic_disjunction(left_exprs, caos_ast.LinkPropRef)

            is_agg = self.is_aggregated_expr(left, deep=True) or \
                     self.is_aggregated_expr(right, deep=True)

            if is_agg:
                result = newbinop(left, right, uninline=True)

            elif not pathdict and not proppathdict:

                if self.is_join(left, right, op, reversed):
                    # Concept join expression: <path> {==|!=} <path>

                    right_exprs = self.get_multipath(right)

                    id_col = caos_name.Name('metamagic.caos.builtins.id')
                    lrefs = [caos_ast.AtomicRefSimple(ref=p, name=id_col)
                                for p in left_exprs.paths]
                    rrefs = [caos_ast.AtomicRefSimple(ref=p, name=id_col)
                                for p in right_exprs.paths]

                    l = caos_ast.Disjunction(paths=frozenset(lrefs))
                    r = caos_ast.Disjunction(paths=frozenset(rrefs))
                    result = newbinop(l, r)

                    for lset, rset in itertools.product(left_exprs.paths, right_exprs.paths):
                        lset.joins.add(rset)
                        rset.backrefs.add(lset)
                        rset.joins.add(lset)
                        lset.backrefs.add(rset)

                elif self.is_type_check(left, right, op, reversed):
                    # Type check expression: <path> IS [NOT] <concept>
                    paths = set()

                    if isinstance(right.type, tuple) and issubclass(right.type[0], (tuple, list)):
                        filter_op = ast.ops.IN if op == ast.ops.IS else ast.ops.NOT_IN
                    else:
                        filter_op = ast.ops.EQ if op == ast.ops.IS else ast.ops.NE

                    for path in left_exprs.paths:
                        ref = caos_ast.MetaRef(ref=path, name='id')
                        expr = caos_ast.BinOp(left=ref, right=right, op=filter_op)
                        paths.add(caos_ast.MetaRefExpr(expr=expr))

                    result = self.path_from_set(paths)

                elif self.is_const_idfilter(left, right, op, reversed):
                    # Constant id filter expressions:
                    #       <path> IN <const_id_list>
                    #       <const_id> IN <path>
                    #       <path> = <const_id>

                    id_col = caos_name.Name('metamagic.caos.builtins.id')

                    # <Constant> IN <EntitySet> is interpreted as a membership
                    # check of entity with ID represented by Constant in the EntitySet,
                    # which is equivalent to <EntitySet>.id = <Constant>
                    #
                    if reversed:
                        membership_op = ast.ops.EQ if op == ast.ops.IN else ast.ops.NE
                    else:
                        membership_op = op

                    if isinstance(right.type, caos_types.ProtoConcept):
                        id_t = self.context.current.proto_schema.get('uuid')
                        const_filter = caos_ast.Constant(value=right.value, index=right.index,
                                                         expr=right.expr, type=id_t)
                    else:
                        const_filter = right

                    paths = set()
                    for p in left_exprs.paths:
                        ref = caos_ast.AtomicRefSimple(ref=p, name=id_col)
                        expr = caos_ast.BinOp(left=ref, right=const_filter, op=membership_op)
                        paths.add(caos_ast.AtomicRefExpr(expr=expr))

                    result = self.path_from_set(paths)

                elif isinstance(op, caos_ast.TextSearchOperator):
                    paths = set()
                    for p in left_exprs.paths:
                        searchable = list(p.concept.get_searchable_links())
                        if not searchable:
                            err = '%s operator called on concept %s without any search configuration'\
                                                       % (op, p.concept.name)
                            hint = 'Configure search for "%s"' % p.concept.name
                            raise caos_error.CaosError(err, hint=hint)

                        # A SEARCH operation on an entity set is always an inline filter ATM
                        paths.add(caos_ast.AtomicRefExpr(expr=newbinop(p, right)))

                    result = self.path_from_set(paths)

                if not result:
                    result = newbinop(left, right, uninline=True)
            else:
                right_paths = self.extract_paths(right, reverse=False, resolve_arefs=False,
                                                        extract_subgraph_refs=True)

                if self.is_constant(right):
                    paths = set()

                    if proppathdict:
                        exprnode_type = caos_ast.LinkPropRefExpr
                        refdict = proppathdict
                    else:
                        exprnode_type = caos_ast.AtomicRefExpr
                        refdict = pathdict

                    if isinstance(left, caos_ast.Path):
                        # We can only break up paths, and must not pick paths out of other
                        # expressions
                        #
                        for ref in left_exprs.paths:
                            if isinstance(ref, exprnode_type):
                                _leftref = ref.expr
                            else:
                                _leftref = ref

                            paths.add(exprnode_type(expr=newbinop(_leftref, right)))
                        else:
                            result = self.path_from_set(paths)

                    elif len(refdict) == 1:
                        # Left operand references a single entity
                        _binop = newbinop(left, right)

                        aexprs = ast.find_children(_binop, lambda i: isinstance(i, exprnode_type))
                        for aexpr in aexprs:
                            aexpr.inline = False

                        _binop = self.merge_paths(_binop)

                        result = exprnode_type(expr=_binop)
                    else:
                        result = newbinop(left, right, uninline=True)

                elif isinstance(right_paths, caos_ast.Path):
                    right_exprs = self.get_multipath(right_paths)

                    rightdict = self.check_atomic_disjunction(right_exprs, caos_ast.AtomicRef)
                    rightpropdict = self.check_atomic_disjunction(right_exprs, caos_ast.LinkPropRef)

                    if rightdict and pathdict or rightpropdict and proppathdict:
                        paths = set()

                        if proppathdict:
                            exprtype = caos_ast.LinkPropRefExpr
                            leftdict = proppathdict
                            rightdict = rightpropdict
                        else:
                            exprtype = caos_ast.AtomicRefExpr
                            leftdict = pathdict

                        # If both operands are atom references, then we check if the referenced
                        # atom parent concepts intersect, and if they do we fold the expression
                        # into the atom ref for those common concepts only.  If there are no common
                        # concepts, a regular binary operation is returned.
                        #
                        if isinstance(left, caos_ast.Path) and isinstance(right, caos_ast.Path):
                            # We can only break up paths, and must not pick paths out of other
                            # expressions
                            #

                            for ref in left_exprs.paths:
                                if isinstance(ref, caos_ast.AtomicRef):
                                    left_id = ref.ref.id
                                else:
                                    left_id = ref.id

                                right_expr = rightdict.get(left_id)

                                if right_expr:
                                    right_expr.replace_refs([right_expr.ref], ref.ref, deep=True)

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
                            # Left and right operand reference the same single path

                            _binop = newbinop(left, right)
                            _binop = self.merge_paths(_binop)
                            result = exprtype(expr=_binop)

                        else:
                            result = newbinop(left, right, uninline=True)
                    else:
                        result = newbinop(left, right, uninline=True)

                elif isinstance(right, caos_ast.PathCombination) \
                        and isinstance(next(iter(right.paths)), caos_ast.SubgraphRef):
                    result = newbinop(left, right, uninline=True)

                elif isinstance(right, caos_ast.BinOp) and op == right.op and \
                                                           isinstance(left, caos_ast.Path):
                    # Got a bin-op, that was not folded into an atom ref.  Re-check it since
                    # we may use operator associativity to fold one of the operands
                    #
                    assert not proppathdict

                    folded_operand = None
                    for operand in (right.left, right.right):
                        if isinstance(operand, caos_ast.AtomicRef):
                            operand_id = operand.ref.id
                            ref = pathdict.get(operand_id)
                            if ref:
                                ref.expr = self.extend_binop(ref.expr, operand, op=op,
                                                                                reverse=reversed)
                                folded_operand = operand
                                break

                    if folded_operand:
                        other_operand = right.left if folded_operand is right.right else right.right
                        result = newbinop(left, other_operand)
                    else:
                        result = newbinop(left, right, uninline=True)

        elif isinstance(left, caos_ast.Constant):
            if isinstance(right, caos_ast.Constant):
                l, r = (right, left) if reversed else (left, right)

                schema = self.context.current.proto_schema
                if isinstance(op, (ast.ops.ComparisonOperator, ast.ops.EquivalenceOperator)):
                    result_type = schema.get('metamagic.caos.builtins.bool')
                else:
                    if l.type == r.type:
                        result_type = l.type
                    else:
                        result_type = caos_types.TypeRules.get_result(op, (l.type, r.type), schema)
                result = caos_ast.Constant(expr=newbinop(left, right), type=result_type)

        elif isinstance(left, caos_ast.BinOp):
            result = newbinop(left, right, uninline=True)

        elif isinstance(left, caos_ast.UnaryOp):
            result = newbinop(left, right, uninline=True)

        elif isinstance(left, caos_ast.TypeCast):
            result = newbinop(left, right, uninline=True)

        elif isinstance(left, caos_ast.FunctionCall):
            result = newbinop(left, right, uninline=True)

        elif isinstance(left, caos_ast.ExistPred):
            result = newbinop(left, right, uninline=True)

        elif isinstance(left, caos_ast.SubgraphRef):
            result = newbinop(left, right, uninline=True)

        elif isinstance(left, caos_ast.SearchVector):
            result = newbinop(left, right, uninline=True)

        if not result:
            raise TreeError('unexpected binop operands: %s, %s' % (left, right))

        return result

    def process_unaryop(self, expr, operator):
        if isinstance(expr, caos_ast.AtomicRef):
            result = caos_ast.AtomicRefExpr(expr=caos_ast.UnaryOp(expr=expr, op=operator))
        elif isinstance(expr, caos_ast.LinkPropRef):
            result = caos_ast.LinkPropRefExpr(expr=caos_ast.UnaryOp(expr=expr, op=operator))
        elif isinstance(expr, caos_ast.Constant):
            result = caos_ast.Constant(expr=caos_ast.UnaryOp(expr=expr, op=operator))
        else:
            paths = self.extract_paths(expr, reverse=False, resolve_arefs=False)
            exprs = self.get_multipath(paths)
            arefs = self.check_atomic_disjunction(exprs, caos_ast.AtomicRef)
            proprefs = self.check_atomic_disjunction(exprs, caos_ast.LinkPropRef)

            if arefs and len(arefs) == 1:
                result = caos_ast.AtomicRefExpr(expr=caos_ast.UnaryOp(expr=expr, op=operator))
            elif proprefs and len(proprefs) == 1:
                result = caos_ast.LinkPropRefExpr(expr=caos_ast.UnaryOp(expr=expr, op=operator))
            else:
                result = caos_ast.UnaryOp(expr=expr, op=operator)

        return result

    def process_none_test(self, expr, schema):
        if isinstance(expr.expr, caos_ast.AtomicRef):
            expr = caos_ast.AtomicRefExpr(expr=expr)
        elif isinstance(expr.expr, caos_ast.LinkPropRef):
            expr = caos_ast.LinkPropRefExpr(expr=expr)
        elif isinstance(expr.expr, caos_ast.EntitySet):
            c = caos_ast.Conjunction(paths=frozenset((expr.expr,)))
            aref = self.entityref_to_idref(c, schema)
            expr = caos_ast.AtomicRefExpr(expr=caos_ast.NoneTest(expr=aref))
        elif isinstance(expr.expr, caos_ast.Constant):
            expr = caos_ast.Constant(expr=expr)

        return expr

    def get_expr_type(self, expr, schema):
        if isinstance(expr, caos_ast.MetaRef):
            result = schema.get('metamagic.caos.builtins.str')

        elif isinstance(expr, caos_ast.AtomicRefSimple):
            if isinstance(expr.ref, caos_ast.PathCombination):
                targets = [t.concept for t in expr.ref.paths]
                concept = caos_utils.get_prototype_nearest_common_ancestor(targets)
            else:
                concept = expr.ref.concept

            sources, outbound, inbound = concept.resolve_pointer(schema, expr.name,
                                                                look_in_children=True)
            assert sources, '"%s" is not a link of "%s"' % (expr.name, concept.name)
            targets = [l.target for linkset in outbound for l in linkset]

            if len(targets) == 1:
                result = targets[0]
            else:
                result = caos_utils.get_prototype_nearest_common_ancestor(targets)

        elif isinstance(expr, caos_ast.LinkPropRefSimple):
            if isinstance(expr.ref, caos_ast.PathCombination):
                targets = [t.link_proto for t in expr.ref.paths]
                link = caos_utils.get_prototype_nearest_common_ancestor(targets)
            else:
                link = expr.ref.link_proto

            prop = link.getptr(schema, expr.name)
            assert prop, '"%s" is not a property of "%s"' % (expr.name, link.name)
            result = prop.target

        elif isinstance(expr, caos_ast.BaseRefExpr):
            result = self.get_expr_type(expr.expr, schema)

        elif isinstance(expr, caos_ast.Record):
            result = expr.concept

        elif isinstance(expr, caos_ast.FunctionCall):
            argtypes = tuple(self.get_expr_type(arg, schema) for arg in expr.args)
            result = caos_types.TypeRules.get_result(expr.name, argtypes, schema)

            if result is None:
                fcls = caos_types.FunctionMeta.get_function_class(expr.name)
                if fcls:
                    signature = fcls.get_signature(argtypes, schema=schema)
                    if signature and signature[2]:
                        if isinstance(signature[2], tuple):
                            result = (signature[2][0], schema.get(signature[2][1]))
                        else:
                            result = schema.get(signature[2])

        elif isinstance(expr, caos_ast.Constant):
            if expr.expr:
                result = self.get_expr_type(expr.expr, schema)
            else:
                result = expr.type

        elif isinstance(expr, caos_ast.BinOp):
            if isinstance(expr.op, (ast.ops.ComparisonOperator,
                                    ast.ops.EquivalenceOperator,
                                    ast.ops.MembershipOperator)):
                result = schema.get('metamagic.caos.builtins.bool')
            else:
                left_type = self.get_expr_type(expr.left, schema)
                right_type = self.get_expr_type(expr.right, schema)
                result = caos_types.TypeRules.get_result(expr.op, (left_type, right_type), schema)
                if result is None:
                    result = caos_types.TypeRules.get_result((expr.op, 'reversed'),
                                                             (right_type, left_type), schema)

        elif isinstance(expr, caos_ast.UnaryOp):
            operand_type = self.get_expr_type(expr.expr, schema)
            result = caos_types.TypeRules.get_result(expr.op, (operand_type,), schema)

        elif isinstance(expr, caos_ast.EntitySet):
            result = expr.concept

        elif isinstance(expr, caos_ast.PathCombination):
            if expr.paths:
                result = self.get_expr_type(next(iter(expr.paths)), schema)
            else:
                result = None

        elif isinstance(expr, caos_ast.TypeCast):
            result = expr.type

        elif isinstance(expr, caos_ast.SubgraphRef):
            subgraph = expr.ref
            if len(subgraph.selector) == 1:
                result = self.get_expr_type(subgraph.selector[0].expr, schema)
            else:
                result = None

        elif isinstance(expr, caos_ast.ExistPred):
            result = schema.get('metamagic.caos.builtins.bool')

        else:
            result = None

        if result is not None:
            allowed = (caos_types.ProtoObject, caos_types.PrototypeClass)
            assert isinstance(result, allowed) or \
                   (isinstance(result, (tuple, list)) and isinstance(result[1], allowed)), \
                   "get_expr_type({!r}) retured {!r} instead of a prototype".format(expr, result)

        return result

    def get_selector_types(self, selector, schema):
        result = collections.OrderedDict()

        for i, selexpr in enumerate(selector):
            expr_type = self.get_expr_type(selexpr.expr, schema)

            if isinstance(selexpr.expr, caos_ast.Constant):
                expr_kind = 'constant'
            elif isinstance(selexpr.expr, (caos_ast.EntitySet, caos_ast.AtomicRefSimple,
                                           caos_ast.LinkPropRefSimple, caos_ast.Record)):
                expr_kind = 'path'
            elif isinstance(selexpr.expr, caos_ast.AtomicRefExpr) and \
                                                                  selexpr.expr.ptr_proto is not None:
                # RefExpr represents a computable
                expr_kind = 'path'
            elif isinstance(selexpr.expr, caos_ast.PathCombination):
                for p in selexpr.expr.paths:
                    if not isinstance(p, (caos_ast.EntitySet, caos_ast.AtomicRefSimple,
                                          caos_ast.LinkPropRefSimple)) \
                       and not (isinstance(p, caos_ast.AtomicRefExpr) and p.ptr_proto is not None):
                        expr_kind = 'expression'
                        break
                else:
                    expr_kind = 'path'
            else:
                expr_kind = 'expression'
            result[selexpr.name or str(i)] = (expr_type, expr_kind)

        return result


class PathResolverContextLevel:
    def __init__(self, prevlevel=None):
        self.include_filters = False
        self.included_filters = set()
        self.expr_root = None
        self.path_shift = None

        if prevlevel:
            self.include_filters = prevlevel.include_filters
            self.path_shift = prevlevel.path_shift
            self.included_filters = prevlevel.included_filters


class PathResolverContext:
    def __init__(self):
        self.stack = []
        self.push()

    def push(self, mode=None):
        level = PathResolverContextLevel(self.current)
        self.stack.append(level)

        return level

    def pop(self):
        self.stack.pop()

    def __call__(self, mode=None):
        return PathResolverContextWrapper(self)

    @property
    def current(self):
        if len(self.stack) > 0:
            return self.stack[-1]
        else:
            return None


class PathResolverContextWrapper:
    def __init__(self, context):
        self.context = context

    def __enter__(self):
        self.context.push()
        return self.context

    def __exit__(self, exc_type, exc_value, traceback):
        self.context.pop()



class PathResolver(TreeTransformer):
    def serialize_path(self, path, include_filters=False):
        context = PathResolverContext()
        context.current.include_filters = include_filters

        return self._serialize_path(context, path)

    def _collect_quals(self, context, path):
        quals = []

        if path.conjunction and path.conjunction.paths:
            for ptr in path.conjunction.paths:
                if (ptr.target and ptr.target.filter
                        and (ptr.target.origin or ptr.target) not in context.current.included_filters):
                    qual = self._serialize_filter(context, ptr.target.filter)
                    quals.append(qual)

                    context.current.included_filters.add(ptr.target.origin or ptr.target)
                    quals.extend(self._collect_quals(context, ptr.target))

        return quals

    def _serialize_path(self, context, path):
        if path.rewrite_original is not None:
            return self._serialize_path(context, path.rewrite_original)

        if isinstance(path, caos_ast.MetaRef):
            source = self._serialize_path(context, path.ref)
            result = ('getclassattr', source, path.name)

        elif isinstance(path, caos_ast.AtomicRefSimple):
            source = self._serialize_path(context, path.ref)
            result = ('getattr', source, path.name)

        elif (isinstance(path, caos_ast.AtomicRefExpr)
                and path.ptr_proto is not None):
            source = self._serialize_path(context, path.ref)
            result = ('getattr', source, path.ptr_proto.normal_name())

        elif isinstance(path, caos_ast.LinkPropRefSimple):
            if path.name == 'metamagic.caos.builtins.target':
                result = self._serialize_link(context, path.ref)
            else:
                source = self._serialize_path(context, path.ref)
                result = ('getattr', source, path.name)

        elif isinstance(path, caos_ast.EntitySet):
            target = ('getcls', path.concept.name)

            if context.current.include_filters:
                quals = []

                if path.filter:
                    with context():
                        context.current.expr_root = path
                        context.current.include_filters = False
                        context.current.included_filters.add(path.origin or path)
                        quals.append(self._serialize_filter(context,
                                                            path.filter))

                if path.conjunction and path.conjunction.paths:
                    with context():
                        context.current.expr_root = path
                        context.current.include_filters = False
                        quals.extend(self._collect_quals(context, path))

            if path.rlink and path != context.current.expr_root:
                link_proto = path.rlink.link_proto
                dir = path.rlink.direction
                source = self._serialize_path(context, path.rlink.source)

                result = ('step', source, target, link_proto.normal_name(), dir)
            else:
                result = target

            if context.current.include_filters and quals:
                qual = quals[0]
                for right in quals[1:]:
                    qual = ('binop', 'and', qual, right)
                result = ('filter', result, qual)

        elif isinstance(path, caos_ast.EntityLink):
            link = self._serialize_link(context, path)
            result = ('as_link', link)

        elif isinstance(path, caos_ast.TypeCast):
            result = self._serialize_path(context, path.expr)

        elif isinstance(path, caos_ast.Record):
            for elem in path.elements:
                if isinstance(elem, caos_ast.BaseRef):
                    result = self._serialize_path(context, elem.ref)
                    break
            else:
                raise AssertionError('unexpected record structure: no atomrefs found')

        elif isinstance(path, caos_ast.Constant):
            result = []

        else:
            raise TreeError('unexpected node: "%r"' % path)

        return result

    def _serialize_filter(self, context, expr):
        if isinstance(expr, caos_ast.BinOp):
            left = self._serialize_filter(context, expr.left)
            right = self._serialize_filter(context, expr.right)
            result = ('binop', str(expr.op), left, right)

        elif isinstance(expr, caos_ast.AtomicRefSimple):
            result = self._serialize_path(context, expr)

        elif isinstance(expr, caos_ast.Constant):
            if expr.index is not None:
                msg = 'filters in path expressions do not support symbolic vars'
                raise ValueError(msg)

            typ = str(expr.type.name) if expr.type else None
            result = ('const', expr.value, typ)

        elif isinstance(expr, caos_ast.TypeCast):
            cast_expr = self._serialize_filter(context, expr.expr)
            typ = str(expr.type.name)
            result = ('cast', cast_expr, typ)

        else:
            msg = 'unsupported node in path expression filter: {!r}'
            raise ValueError(msg.format(expr))

        return result


    def _serialize_link(self, context, link):
        source = self._serialize_path(context, link.source)
        if link.direction == caos_types.OutboundDirection:
            result = ('getattr', source, link.link_proto.normal_name())
        else:
            target = self._serialize_path(context, link.target)
            result = ('step', source, target,
                      link.link_proto.normal_name(), link.direction)

        return result

    def unserialize_path(self, script, class_factory):
        return self._exec_cmd(script, class_factory)

    def _exec_cmd(self, cmd, class_factory):
        from metamagic.caos import expr as caos_expr

        cmd, *args = cmd

        if cmd == 'getattr':

            sources = []
            src = self._exec_cmd(args[0], class_factory)
            ptr = args[1]
            result = getattr(src, ptr)

        elif cmd == 'getclassattr':
            result = []

            if args[1] == 'id':
                atom_name = 'metamagic.caos.builtins.int'
            else:
                atom_name = 'metamagic.caos.builtins.str'

            source = self._exec_cmd(args[0], class_factory)
            metadata = dict(source_expr=getattr(caos_expr.type(source), args[1]))
            atom = class_factory.get_class(atom_name)
            result = atom._copy_(metadata=metadata)

        elif cmd == 'step':
            source = self._exec_cmd(args[0], class_factory)
            target = self._exec_cmd(args[1], class_factory)

            result = caos_utils.create_path_step(
                        class_factory, source, target, args[2], args[3])

        elif cmd == 'getcls':
            result = class_factory.get_class(args[0])

        elif cmd == 'as_link':
            source = self._exec_cmd(args[0], class_factory)
            result = source.as_link()

        elif cmd == 'filter':
            result = self._exec_cmd(args[0], class_factory)

        else:
            msg = 'unexpected path resolver command: "{}"'.format(cmd)
            raise TreeError(msg)

        return result

    def convert_to_entity_path(self, script, path_shift):
        context = PathResolverContext()
        context.current.path_shift = path_shift
        return self._convert_to_entity_path(context, script)

    def _convert_to_entity_path(self, context, cmd):
        cmd, *args = cmd

        if cmd == 'getattr':
            source = self._convert_to_entity_path(context, args[0])
            ptr = args[1]
            result = ('follow', source, ptr, caos_types.OutboundDirection)

        elif cmd == 'getclassattr':
            source = self._convert_to_entity_path(context, args[0])
            result = ('getclassattr', source, args[1])

        elif cmd == 'step':
            source = self._convert_to_entity_path(context, args[0])

            if context.current.path_shift > 0:
                result = source
                context.current.path_shift -= 1
            else:
                pointer = args[2]
                direction = args[3]
                target = args[1][1]
                result = ('follow', source, pointer, direction, target)

        elif cmd == 'getcls':
            result = ('this',)

        elif cmd == 'as_link':
            source = self._convert_to_entity_path(context, args[0])
            result = ('as_link', source)

        elif cmd == 'filter':
            source = self._convert_to_entity_path(context, args[0])
            qual = self._convert_to_entity_path(context, args[1])
            result = ('filter', source, qual)

        elif cmd == 'binop':
            op = args[0]
            left = self._convert_to_entity_path(context, args[1])
            right = self._convert_to_entity_path(context, args[2])
            result = ('binop', op, left, right)

        elif cmd == 'const':
            result = (cmd,) + tuple(args)

        elif cmd == 'cast':
            cast_expr = self._convert_to_entity_path(context, args[0])
            result = (cmd, cast_expr) + tuple(args[1:])

        else:
            msg = 'unexpected path resolver command: "{}"'.format(cmd)
            raise TreeError(msg)

        return result

    def resolve_path(self, tree, class_factory=None):
        targets = list(self._resolve_path(tree, class_factory))
        assert len(targets) <= 1
        return targets[0]

    def _resolve_path(self, path, class_factory):
        if path.rewrite_original is not None:
            return self._resolve_path(path.rewrite_original, class_factory)

        if isinstance(path, caos_ast.Disjunction):
            result = set()

            for path in path.paths:
                result.update(self._resolve_path(path, class_factory))

        elif isinstance(path, caos_ast.MetaRef):
            import metamagic.caos.expr as caos_expr
            expr = self._resolve_path(path.ref, class_factory)

            if path.name == 'id':
                atom_name = 'metamagic.caos.builtins.int'
            else:
                atom_name = 'metamagic.caos.builtins.str'

            result = []

            for e in expr:
                metadata = dict(source_expr=getattr(caos_expr.type(e), path.name))
                atom = class_factory.get_class(atom_name)
                result.append(atom._copy_(metadata=metadata))

        elif isinstance(path, caos_ast.AtomicRefSimple):
            expr = self._resolve_path(path.ref, class_factory)
            result = (getattr(e, path.name) for e in expr)

        elif isinstance(path, caos_ast.AtomicRefExpr) and path.ptr_proto is not None:
            expr = self._resolve_path(path.ref, class_factory)
            result = (getattr(e, path.ptr_proto.normal_name()) for e in expr)

        elif isinstance(path, caos_ast.LinkPropRefSimple):
            expr = self._resolve_path(path.ref, class_factory)
            result = (getattr(e, path.name) for e in expr)

        elif isinstance(path, caos_ast.EntitySet):
            result = class_factory.get_class(path.concept.name)

            if path.rlink:
                result = self._step_from_link(path.rlink, result, class_factory)
            else:
                result = (result,)

        elif isinstance(path, caos_ast.EntityLink):
            link = path
            link_proto = link.link_proto
            source = self._resolve_path(link.source, class_factory)
            target = next(iter(self._resolve_path(link.target, class_factory)))
            result = []

            for e in source:
                target = e.follow(link_proto.normal_name(), link.direction, targets=target)
                result.append(target.as_link())

        elif isinstance(path, caos_ast.TypeCast):
            result = self._resolve_path(path.expr, class_factory)

        elif isinstance(path, caos_ast.Record):
            for elem in path.elements:
                if isinstance(elem, caos_ast.BaseRef):
                    result = self._resolve_path(elem.ref, class_factory)
                    break
            else:
                raise AssertionError('unexpected record structure: no atomrefs found')

        elif isinstance(path, caos_ast.Constant):
            result = ()

        else:
            raise TreeError('unexpected node: "%r"' % path)

        return result

    def _step_from_link(self, link, target, class_factory):
        link_proto = link.link_proto
        dir = link.direction
        source = self._resolve_path(link.source, class_factory)
        result = (caos_utils.create_path_step(class_factory, expr, target,
                                              link_proto.normal_name(),
                                              dir) for expr in source)
        return result
