##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL to IR compiler implementation."""

import itertools

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import expr as s_expr
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import sources as s_sources
from edgedb.lang.schema import types as s_types

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import errors
from edgedb.lang.edgeql import parser as qlparser

from edgedb.lang.common import ast
from edgedb.lang.common import exceptions as edgedb_error
from edgedb.lang.common import markup  # NOQA


class ParseContextLevel(object):
    def __init__(self, prevlevel=None, mode=None):
        if prevlevel is not None:
            self.toplevel_shape_rptrcls = None

            if mode == ParseContext.SUBQUERY:
                self.stmt = None
                self.sets = {}
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
                self.modaliases = prevlevel.modaliases
                self.aliascnt = prevlevel.aliascnt.copy()
                self.subgraphs_map = {}
                self.cge_map = prevlevel.cge_map.copy()
                self.apply_access_control_rewrite = \
                    prevlevel.apply_access_control_rewrite
                self.local_link_source = None
                self.arg_types = prevlevel.arg_types
            else:
                self.stmt = prevlevel.stmt
                if mode == ParseContext.NEWSETS:
                    self.sets = {}
                else:
                    self.sets = prevlevel.sets
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
                self.modaliases = prevlevel.modaliases
                self.aliascnt = prevlevel.aliascnt
                self.subgraphs_map = prevlevel.subgraphs_map
                self.cge_map = prevlevel.cge_map
                self.apply_access_control_rewrite = \
                    prevlevel.apply_access_control_rewrite
                self.local_link_source = prevlevel.local_link_source
                self.arg_types = prevlevel.arg_types
        else:
            self.stmt = None
            self.sets = {}
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
            self.modaliases = None
            self.aliascnt = {}
            self.subgraphs_map = {}
            self.cge_map = {}
            self.apply_access_control_rewrite = False
            self.local_link_source = None
            self.arg_types = {}
            self.toplevel_shape_rptrcls = None

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
    CURRENT, NEW, SUBQUERY, NEWSETS = range(0, 4)

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
            self.context.push(self.mode)
            return self.context

    def __exit__(self, exc_type, exc_value, traceback):
        self.context.pop()


class EdgeQLCompilerError(edgedb_error.EdgeDBError):
    pass


class PathExtractor(ast.visitor.NodeVisitor):
    def __init__(self):
        super().__init__()
        self.prefixes = irutils.PathIndex()

    def visit_Set(self, expr):
        key = expr.path_id

        if key:
            if key not in self.prefixes:
                self.prefixes[key] = {expr}
            else:
                self.prefixes[key].add(expr)

        if expr.expr is not None:
            self.visit(expr.expr)

        if expr.rptr is not None:
            self.visit(expr.rptr.source)


def extract_prefixes(expr):
    extractor = PathExtractor()
    extractor.visit(expr)
    return extractor.prefixes


def get_prefix_trie(prefixes):
    trie = {}

    for path_id in prefixes:
        branch = trie
        for path_prefix in path_id.iter_prefixes():
            branch = branch.setdefault(tuple(path_prefix), {})

    return trie


def get_common_prefixes(exprs):
    prefixes = {}
    for expr in exprs:
        prefixes.update(extract_prefixes(expr))

    trie = get_prefix_trie(prefixes)

    trails = []

    for root, subtrie in trie.items():
        path_id = root
        current = subtrie
        while current:
            if len(current) == 1:
                path_id, current = next(iter(current.items()))
            else:
                break

        trails.append(irutils.LinearPath(path_id))

    return {trail: prefixes[trail] for trail in trails}


class EdgeQLCompiler(ast.visitor.NodeVisitor):
    def __init__(self, schema, modaliases=None):
        super().__init__()
        self.schema = schema
        self.modaliases = modaliases

    def transform(self,
                  edgeql_tree,
                  arg_types,
                  modaliases=None,
                  anchors=None,
                  security_context=None):

        self._init_context(arg_types, modaliases, anchors,
                           security_context=security_context)

        return self.visit(edgeql_tree)

    def transform_fragment(self,
                           edgeql_tree,
                           arg_types,
                           modaliases=None,
                           anchors=None,
                           location=None):

        context = self._init_context(arg_types, modaliases, anchors)
        context.current.location = location or 'generator'
        return self.visit(edgeql_tree)

    def generic_visit(self, node, *, combine_results=None):
        raise NotImplementedError(
            'no EdgeQL compiler handler for {}'.format(node.__class__))

    def visit_SelectQueryNode(self, edgeql_tree):
        ctx = self.context.current

        stmt = ctx.stmt = irast.SelectStmt()
        self._visit_with_block(edgeql_tree)

        if edgeql_tree.op:  # UNION/INTERSECT/EXCEPT
            stmt.set_op = qlast.SetOperator(edgeql_tree.op)
            stmt.set_op_larg = self.visit(edgeql_tree.op_larg)
            stmt.set_op_rarg = self.visit(edgeql_tree.op_rarg)
        else:
            stmt.where = self._process_select_where(edgeql_tree.where)

            stmt.groupby = self._process_groupby(edgeql_tree.groupby)
            if stmt.groupby:
                ctx.groupprefixes = extract_prefixes(stmt.groupby)
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
                        ctx.groupprefixes = {True: True}
                        break

            stmt.result = self._process_stmt_result(edgeql_tree.targets[0])

        stmt.orderby = self._process_orderby(edgeql_tree.orderby)

        if edgeql_tree.offset:
            stmt.offset = self.visit(edgeql_tree.offset)

        if edgeql_tree.limit:
            stmt.limit = self.visit(edgeql_tree.limit)

        stmt.result_types = self._get_selector_types(stmt.result)
        stmt.argument_types = self.context.current.arguments
        stmt.context_vars = self.context.current.context_vars

        return stmt

    def visit_InsertQueryNode(self, edgeql_tree):
        ctx = self.context.current

        stmt = ctx.stmt = irast.InsertStmt()
        self._visit_with_block(edgeql_tree)

        with self.context():
            self.context.current.location = 'selector'
            subject = self.visit(edgeql_tree.subject)

            if edgeql_tree.targets:
                stmt.result = self._process_stmt_result(edgeql_tree.targets[0])
            else:
                stmt.result = self._process_shape(subject, None, [])

        stmt.shape = self._process_shape(
            subject, None, edgeql_tree.pathspec,
            require_expressions=True,
            include_implicit=False)

        explicit_ptrs = {
            el.rptr.ptrcls.shortname for el in stmt.shape.elements
        }

        for pn, ptrcls in subject.scls.pointers.items():
            if (not ptrcls.default or
                    pn in explicit_ptrs or
                    ptrcls.is_special_pointer()):
                continue

            targetstep = self._extend_path(subject, ptrcls)

            if isinstance(ptrcls.default, s_expr.ExpressionText):
                default_expr = qlparser.parse(ptrcls.default)
            else:
                default_expr = qlast.ConstantNode(value=ptrcls.default)

            el = irast.SubstmtRef(
                stmt=self.visit(default_expr),
                rptr=targetstep.rptr)

            stmt.shape.elements.append(el)

        stmt.result_types = self._get_selector_types(stmt.result)
        stmt.argument_types = self.context.current.arguments
        stmt.context_vars = self.context.current.context_vars

        return stmt

    def visit_UpdateQueryNode(self, edgeql_tree):
        ctx = self.context.current

        stmt = ctx.stmt = irast.UpdateStmt()
        self._visit_with_block(edgeql_tree)

        with self.context():
            self.context.current.location = 'selector'
            subject = self.visit(edgeql_tree.subject)

        stmt.where = self._process_select_where(edgeql_tree.where)
        if edgeql_tree.targets:
            stmt.result = self._process_stmt_result(edgeql_tree.targets[0])
        else:
            stmt.result = self._process_shape(subject, None, [])

        stmt.shape = self._process_shape(
            subject, None, edgeql_tree.pathspec,
            require_expressions=True,
            include_implicit=False)

        stmt.result_types = self._get_selector_types(stmt.result)
        stmt.argument_types = self.context.current.arguments
        stmt.context_vars = self.context.current.context_vars

        return stmt

    def visit_DeleteQueryNode(self, edgeql_tree):
        ctx = self.context.current

        stmt = ctx.stmt = irast.DeleteStmt()
        self._visit_with_block(edgeql_tree)

        with self.context():
            self.context.current.location = 'selector'
            subject = self.visit(edgeql_tree.subject)

        stmt.where = self._process_select_where(edgeql_tree.where)
        if edgeql_tree.targets:
            stmt.result = self._process_stmt_result(edgeql_tree.targets[0])
        else:
            stmt.result = self._process_shape(subject, None, [])

        stmt.shape = self._process_shape(
            subject, None, [],
            require_expressions=True,
            include_implicit=False)

        stmt.result_types = self._get_selector_types(stmt.result)
        stmt.argument_types = self.context.current.arguments
        stmt.context_vars = self.context.current.context_vars

        return stmt

    def visit_PathNode(self, expr):
        ctx = self.context.current

        pathvars = ctx.pathvars
        anchors = ctx.anchors

        path_tip = None

        for i, step in enumerate(expr.steps):
            if isinstance(step, qlast.TypeInterpretationNode):
                # First of all, handle the (Expr AS Type) expressions,
                # as that alters path resolution.
                path_tip = self.visit(step.expr)
                path_tip.as_type = self._get_schema_object(step.type.maintype)
                continue

            elif isinstance(step, qlast.PathStepNode):
                if i > 0:
                    raise RuntimeError(
                        'unexpected PathStepNode as a non-first path item')

                refnode = None

                if not step.namespace:
                    # Check if the starting path label is a known anchor
                    refnode = anchors.get(step.expr)

                if refnode is None:
                    # Check if the starting path label is a known
                    # path variable (defined in a WITH clause).
                    refnode = pathvars.get(step.expr)

                if refnode is None and not step.namespace:
                    # Finally, check if the starting path label is
                    # a query defined in a WITH clause.
                    refnode = ctx.cge_map.get(step.expr)

                if refnode is not None:
                    path_tip = refnode
                    continue

            if isinstance(step, qlast.PathStepNode):
                # Starting path label.  Must be a valid reference to an
                # existing Concept class, as aliases and path variables
                # have been checked above.
                scls = self._get_schema_object(step.expr, step.namespace)
                path_id = irutils.LinearPath([scls])

                try:
                    # We maintain a registry of Set nodes for each unique
                    # Path to achieve path prefix matching.
                    path_tip = ctx.sets[path_id]
                except KeyError:
                    path_tip = ctx.sets[path_id] = irast.Set()
                    path_tip.scls = scls
                    path_tip.path_id = path_id

            elif isinstance(step, (qlast.LinkExprNode,
                                   qlast.LinkPropExprNode)):
                # Pointer traversal step
                qlptr = step
                ptr_expr = qlptr.expr
                ptr_target = None

                direction = (ptr_expr.direction or
                             s_pointers.PointerDirection.Outbound)
                if ptr_expr.target:
                    # ... link [TO Target]
                    ptr_target = self._get_schema_object(
                        ptr_expr.target.name, ptr_expr.target.module)

                ptr_name = (ptr_expr.namespace, ptr_expr.name)

                if isinstance(step, qlast.LinkPropExprNode):
                    # Link property reference; the source is the
                    # link immediately preceding this step in the path.
                    source = path_tip.rptr.ptrcls
                else:
                    # Link reference, the source is the current path tip.
                    source = (path_tip.as_type or path_tip.scls)

                ptrcls = self._resolve_ptr(
                    source, ptr_name, direction, target=ptr_target)

                target = ptrcls.get_far_endpoint(direction)

                path_tip = self._extend_path(
                    path_tip, ptrcls, direction, target)

            else:
                raise RuntimeError(
                    'Unexpected path step expression: {!r}'.format(step))

        if (ctx.groupprefixes and
                ctx.location in ('orderby', 'selector') and
                not ctx.in_aggregate):
            if path_tip.path_id not in ctx.groupprefixes:
                err = ('{!r} must appear in the '
                       'GROUP BY expression or used in an aggregate '
                       'function '.format(path_tip.path_id))
                raise errors.EdgeQLError(err)

        return path_tip

    def visit_BinOpNode(self, expr):
        ctx = self.context.current

        left, right = self.visit((expr.left, expr.right))

        if isinstance(expr.op, ast.ops.TypeCheckOperator):
            right = self._process_type_ref_expr(right)

        binop = irast.BinOp(left=left, right=right, op=expr.op)
        result_type = irutils.infer_type(binop, ctx.schema)

        if result_type is None:
            left_type = irutils.infer_type(binop.left, ctx.schema)
            right_type = irutils.infer_type(binop.right, ctx.schema)
            err = 'operator does not exist: {} {} {}'.format(
                left_type.name, expr.op, right_type.name)

            raise errors.EdgeQLError(err, context=expr.context)

        prefixes = get_common_prefixes([left, right])

        sources = set(itertools.chain.from_iterable(prefixes.values()))

        if sources:
            node = irast.Set(
                path_id=irutils.LinearPath([]),
                scls=result_type,
                expr=binop,
                sources=sources,
                source_conjunction=expr.op == ast.ops.AND
            )
        else:
            node = binop

        return node

    def visit_ConstantNode(self, expr):
        ctx = self.context.current

        if expr.index is not None:
            type = ctx.arg_types.get(expr.index)
            if type is not None:
                type = s_types.normalize_type(type, ctx.schema)
            node = irast.Constant(
                value=expr.value, index=expr.index, type=type)
            self.context.current.arguments[expr.index] = type
        else:
            type = s_types.normalize_type(expr.value.__class__, ctx.schema)
            node = irast.Constant(
                value=expr.value, index=expr.index, type=type)

        return node

    def visit_SequenceNode(self, expr):
        elements = self.visit(expr.elements)
        return irast.Sequence(elements=elements)

    def visit_ArrayNode(self, expr):
        elements = self.visit(expr.elements)
        return irast.Sequence(elements=elements, is_array=True)

    def visit_FunctionCallNode(self, expr):
        with self.context():
            ctx = self.context.current

            if isinstance(expr.func, str):
                funcname = (None, expr.func)
            else:
                funcname = expr.func

            funcobj = self._get_schema_object(
                name=funcname[1], module=funcname[0])

            if funcobj.aggregate:
                ctx.in_aggregate.append(expr)

            ctx.in_func_call = True

            args = []
            kwargs = {}

            for a in expr.args:
                if isinstance(a, qlast.NamedArgNode):
                    kwargs[a.name] = self.visit(a.arg)
                else:
                    args.append(self.visit(a))

            node = irast.FunctionCall(
                func=funcobj,
                args=args,
                kwargs=kwargs)

            if expr.agg_sort:
                node.agg_sort = [
                    irast.SortExpr(
                        expr=self.visit(e.path),
                        direction=e.direction) for e in expr.agg_sort
                ]

            elif expr.window:
                if expr.window.orderby:
                    node.agg_sort = [
                        irast.SortExpr(
                            expr=self.visit(e.path),
                            direction=e.direction)
                        for e in expr.window.orderby
                    ]

                if expr.window.partition:
                    for partition_expr in expr.window.partition:
                        partition_expr = self.visit(partition_expr)
                        node.partition.append(partition_expr)

                node.window = True

            if expr.agg_filter:
                node.agg_filter = self.visit(expr.agg_filter)

            if node.args:
                for arg in node.args:
                    if not isinstance(arg, irast.Constant):
                        break
                else:
                    node = irast.Constant(expr=node, type=node.args[0].type)

        return node

    def visit_IfElseNode(self, expr):
        return irast.IfElseExpr(
            condition=self.visit(expr.condition),
            if_expr=self.visit(expr.if_expr),
            else_expr=self.visit(expr.else_expr))

    def visit_UnaryOpNode(self, expr):
        ctx = self.context.current

        operand = self.visit(expr.operand)
        result = irast.UnaryOp(expr=operand, op=expr.op)
        set_expr = self._is_set_expr(operand)
        if set_expr is not None:
            result_type = irutils.infer_type(result, ctx.schema)
            if result_type is None:
                raise RuntimeError(
                    'could not resolve the unaryop '
                    'result type: {} {}'.format(expr.op, operand))

            result = irast.Set(
                path_id=irutils.LinearPath([]),
                scls=result_type,
                expr=result,
                sources=set((set_expr,))
            )

        return result

    def visit_ExistsPredicateNode(self, expr):
        subexpr = self.visit(expr.expr)
        return irast.ExistPred(expr=subexpr)

    def visit_TypeCastNode(self, expr):
        maintype = expr.type.maintype
        subtypes = expr.type.subtypes

        if subtypes:
            typ = irast.TypeRef(
                maintype=maintype.name,
                subtypes=[]
            )

            for subtype in subtypes:
                if isinstance(subtype, qlast.PathNode):
                    stype = self.visit(subtype)
                    if isinstance(stype, irast.LinkPropRefSimple):
                        stype = stype.ref
                    elif not isinstance(stype, irast.EntityLink):
                        stype = stype.rptr

                    if subtype.pathspec:
                        shape = self._process_shape(
                            stype.ptrcls, None, subtype.pathspec)
                    else:
                        shape = None

                    subtype = irast.CompositeType(node=stype, shape=shape)
                else:
                    subtype = self._get_schema_object(
                        subtype.name, subtype.module)

                typ.subtypes.append(subtype.name)
        else:
            typ = irast.TypeRef(
                maintype=self._get_schema_object(
                    maintype.name, maintype.module).name,
                subtypes=[]
            )

        return irast.TypeCast(expr=self.visit(expr.expr), type=typ)

    def visit_IndirectionNode(self, expr):
        node = self.visit(expr.arg)
        for indirection_el in expr.indirection:
            if isinstance(indirection_el, qlast.IndexNode):
                idx = self.visit(indirection_el.index)
                node = irast.IndexIndirection(expr=node, index=idx)

            elif isinstance(indirection_el, qlast.SliceNode):
                if indirection_el.start:
                    start = self.visit(indirection_el.start)
                else:
                    start = irast.Constant(value=None)

                if indirection_el.stop:
                    stop = self.visit(indirection_el.stop)
                else:
                    stop = irast.Constant(value=None)

                node = irast.SliceIndirection(
                    expr=node, start=start, stop=stop)
            else:
                raise ValueError('unexpected indirection node: '
                                 '{!r}'.format(indirection_el))

        return node

    def _init_context(self,
                      arg_types,
                      modaliases,
                      anchors,
                      *,
                      security_context=None):
        self.context = context = ParseContext()
        self.context.current.schema = self.schema
        self.context.current.modaliases = modaliases or self.modaliases
        if arg_types:
            self.context.current.arg_types = arg_types

        if self.context.current.modaliases:
            self.context.current.namespaces.update(
                self.context.current.modaliases)

        if anchors:
            self._populate_anchors(anchors)

        if security_context:
            self.context.current.apply_access_control_rewrite = True

        return context

    def _populate_anchors(self, anchors):
        ctx = self.context.current

        for anchor, scls in anchors.items():
            if isinstance(scls, s_obj.NodeClass):
                step = irast.Set()
                step.scls = scls
                step.path_id = irutils.LinearPath([step.scls])
                step.anchor = anchor
                step.show_as_anchor = anchor

            elif isinstance(scls, s_links.Link):
                if scls.source:
                    path = irast.Set()
                    path.scls = scls.source
                    path.path_id = irutils.LinearPath([path.scls])
                    path = self._extend_path(
                        path, scls,
                        s_pointers.PointerDirection.Outbound,
                        scls.target)
                else:
                    path = irast.Set()
                    path.scls = ctx.schema.get('std::Object')
                    path.path_id = irutils.LinearPath([path.scls])
                    ptrcls = scls.get_derived(
                        ctx.schema, path.scls, ctx.schema.get('std::Object'),
                        mark_derived=True, add_to_schema=False)
                    path = self._extend_path(
                        path, ptrcls,
                        s_pointers.PointerDirection.Outbound,
                        ptrcls.target)

                step = path
                step.anchor = anchor
                step.show_as_anchor = anchor

            elif isinstance(scls, s_lprops.LinkProperty):
                if scls.source.source:
                    path = irast.Set()
                    path.scls = scls.source.source
                    path.path_id = irutils.LinearPath([path.scls])
                    path = self._extend_path(
                        path, scls.source,
                        s_pointers.PointerDirection.Outbound,
                        scls.source.target)
                else:
                    path = irast.Set()
                    path.scls = ctx.schema.get('std::Object')
                    path.path_id = irutils.LinearPath([path.scls])
                    ptrcls = scls.source.get_derived(
                        ctx.schema, path.scls, ctx.schema.get('std::Object'),
                        mark_derived=True, add_to_schema=False)
                    path = self._extend_path(
                        path, ptrcls,
                        s_pointers.PointerDirection.Outbound,
                        ptrcls.target)

                step = self._extend_path(
                    path, scls,
                    s_pointers.PointerDirection.Outbound,
                    scls.target)

                step.anchor = anchor
                step.show_as_anchor = anchor

            else:
                step = scls

            ctx.anchors[anchor] = step

    def _visit_with_block(self, edgeql_tree):
        ctx = self.context.current

        stmt = ctx.stmt
        stmt.substmts = []

        for with_entry in edgeql_tree.aliases:
            if isinstance(with_entry, qlast.NamespaceAliasDeclNode):
                ctx.namespaces[with_entry.alias] = with_entry.namespace

            elif isinstance(with_entry, qlast.CGENode):
                with self.context(ParseContext.SUBQUERY):
                    _cge = self.visit(with_entry.expr)
                    _cge.name = with_entry.alias
                ctx.cge_map[with_entry.alias] = _cge
                stmt.substmts.append(_cge)

            elif isinstance(with_entry, qlast.DetachedPathDeclNode):
                with self.context(ParseContext.NEWSETS):
                    expr = self.visit(with_entry.expr)
                    expr.path_id = irutils.LinearPath([
                        s_concepts.Concept(
                            name=sn.Name(
                                module='__detached__',
                                name=with_entry.alias
                            )
                        )
                    ])
                ctx.pathvars[with_entry.alias] = expr

            else:
                expr = self.visit(with_entry.expr)
                ctx.pathvars[with_entry.alias] = expr

        if ctx.modaliases:
            ctx.namespaces.update(ctx.modaliases)

    def _process_unlimited_recursion(self):
        type = s_types.normalize_type((0).__class__, self.schema)
        return irast.Constant(value=0, index=None, type=type)

    def _process_shape(self, source_expr, rptrcls, shapespec, *,
                       require_expressions=False, include_implicit=True,
                       _visited=None, _recurse=True):
        """Build a Shape node given shape spec."""
        ctx = self.context.current

        if _visited is None:
            _visited = {}

        scls = source_expr.scls

        elements = []

        shape = irast.Shape(elements=elements, scls=scls,
                            set=source_expr, rptr=source_expr.rptr)

        _new_visited = _visited.copy()

        if isinstance(scls, s_concepts.Concept):
            _new_visited[scls] = shape

            if include_implicit:
                implicit_ptrs = (sn.Name('std::id'),)

                implicit_shape_els = []

                for pn in implicit_ptrs:
                    shape_el = qlast.SelectPathSpecNode(
                        expr=qlast.PathNode(steps=[
                            qlast.PathStepNode(
                                expr=qlast.LinkNode(
                                    name=pn.name,
                                    namespace=pn.module
                                )
                            )
                        ])
                    )

                    implicit_shape_els.append(shape_el)

                shapespec = implicit_shape_els + list(shapespec)

        else:
            _new_visited[scls] = shape

            if include_implicit:
                implicit_ptrs = (sn.Name('std::target'),)

                implicit_shape_els = []

                for pn in implicit_ptrs:
                    shape_el = qlast.SelectPathSpecNode(
                        expr=qlast.PathNode(steps=[
                            qlast.PathStepNode(
                                expr=qlast.LinkNode(
                                    name=pn.name,
                                    namespace=pn.module,
                                    type='property'
                                )
                            )
                        ])
                    )

                    implicit_shape_els.append(shape_el)

                shapespec = implicit_shape_els + list(shapespec)

        for shape_el in shapespec:
            steps = shape_el.expr.steps
            ptrsource = scls

            if len(steps) == 2:
                # Pointers may be qualified by the explicit source
                # class, which is equivalent to (Expr AS Type).
                ptrsource = self._get_schema_object(
                    steps[0].expr, steps[0].namespace)
                lexpr = steps[1].expr
            elif len(steps) == 1:
                lexpr = steps[0].expr

            ptrname = (lexpr.namespace, lexpr.name)

            if lexpr.type == 'property':
                if rptrcls is None:
                    raise errors.EdgeQLError(
                        'invalid reference to link property '
                        'in top level shape')

                ptrsource = rptrcls
                ptr_metacls = s_lprops.LinkProperty
            else:
                ptr_metacls = s_links.Link

            if lexpr.target is not None:
                target_name = (lexpr.target.module, lexpr.target.name)
            else:
                target_name = None

            ptr_direction = \
                lexpr.direction or s_pointers.PointerDirection.Outbound

            if shape_el.compexpr is not None:
                # The shape element is defined as a computable expression.

                schema = ctx.schema

                if ptrname[0]:
                    pointer_name = sn.SchemaName(
                        module=ptrname[0], name=ptrname[1])
                else:
                    pointer_name = ptrname[1]

                ptrcls = ptrsource.resolve_pointer(
                    self.schema,
                    pointer_name,
                    direction=ptr_direction,
                    look_in_children=False,
                    include_inherited=True)

                with self.context():
                    # Put current pointer class in context, so
                    # that references to link properties in sub-SELECT
                    # can be resolved.  This is necessary for proper
                    # evaluation of link properties on computable links,
                    # most importantly, in INSERT/UPDATE context.
                    self.context.current.toplevel_shape_rptrcls = ptrcls
                    compexpr = self.visit(shape_el.compexpr)

                if not isinstance(compexpr, (irast.Stmt, irast.SubstmtRef)):
                    compexpr = irast.SelectStmt(
                        result=compexpr
                    )

                target_class = irutils.infer_type(compexpr, schema)
                if target_class is None:
                    msg = 'cannot determine expression result type'
                    raise errors.EdgeQLError(msg, context=lexpr.context)

                if ptrcls is None:
                    if (isinstance(ctx.stmt, irast.MutatingStmt) and
                            ctx.location != 'selector'):
                        raise errors.EdgeQLError(
                            'reference to unknown pointer',
                            context=lexpr.context)

                    ptrcls = ptr_metacls(
                        name=sn.SchemaName(
                            module=ptrname[0] or ptrsource.name.module,
                            name=ptrname[1]),
                    ).derive(schema, ptrsource, target_class)

                    if isinstance(shape_el.compexpr, qlast.StatementNode):
                        if shape_el.compexpr.single:
                            ptrcls.mapping = s_links.LinkMapping.ManyToOne
                        else:
                            ptrcls.mapping = s_links.LinkMapping.ManyToMany

                if ptrcls.shortname == 'std::__class__':
                    msg = 'cannot assign to __class__'
                    raise errors.EdgeQLError(msg, context=lexpr.context)

                if (isinstance(ctx.stmt, irast.MutatingStmt) and
                        ctx.location != 'selector'):
                    if (isinstance(ptrcls.target, s_concepts.Concept) and
                            not target_class.issubclass(ptrcls.target) and
                            target_class.name != 'std::Object'):
                        # Validate that the insert/update expression is
                        # of the correct class.  Make an exception for
                        # expressions returning std::Object, as the
                        # GraphQL translator relies on that to support
                        # insert-by-object-id.  XXX: remove this
                        # exemption once support for class casts is added
                        # to DML.
                        lname = f'{ptrsource.name}.{ptrcls.shortname.name}'
                        expected = [repr(str(ptrcls.target.name))]
                        raise edgedb_error.InvalidPointerTargetError(
                            f'invalid target for link {str(lname)!r}: '
                            f'{str(target_class.name)!r} (expecting '
                            f'{" or ".join(expected)})'
                        )

            else:
                ptrcls = self._resolve_ptr(
                    ptrsource,
                    ptrname,
                    direction=ptr_direction,
                    target=target_name)
                target_class = ptrcls.get_far_endpoint(ptr_direction)
                compexpr = None

            if shape_el.recurse:
                if shape_el.recurse_limit is not None:
                    recurse = self.visit(shape_el.recurse_limit)
                else:
                    # XXX - temp hack
                    recurse = self._process_unlimited_recursion()
            else:
                recurse = None

            if shape_el.where:
                where = self._process_select_where(shape_el.where)
            else:
                where = None

            if shape_el.orderby:
                orderby = self._process_orderby(shape_el.orderby)
            else:
                orderby = None

            if shape_el.offset is not None:
                offset = self.visit(shape_el.offset)
            else:
                offset = None

            if shape_el.limit is not None:
                limit = self.visit(shape_el.limit)
            else:
                limit = None

            ptr_singular = ptrcls.singular(ptr_direction)

            targetstep = self._extend_path(
                source_expr, ptrcls, ptr_direction, target_class)

            ptr_node = targetstep.rptr

            if compexpr is not None:
                if not isinstance(compexpr, irast.SubstmtRef):
                    el = irast.SubstmtRef(stmt=compexpr, rptr=ptr_node)
                    elements.append(el)
                else:
                    compexpr.rptr = ptr_node
                    elements.append(compexpr)

                continue

            if _recurse and shape_el.pathspec:
                _memo = _new_visited

                if (isinstance(ctx.stmt, irast.MutatingStmt) and
                        ctx.location != 'selector'):

                    mutation_pathspec = []
                    for subel in shape_el.pathspec or []:
                        if not isinstance(subel.expr.steps[0],
                                          qlast.LinkPropExprNode):
                            mutation_pathspec.append(subel)

                    el = self._process_shape(
                        targetstep,
                        ptrcls,
                        mutation_pathspec,
                        _visited=_memo,
                        _recurse=True,
                        require_expressions=require_expressions,
                        include_implicit=include_implicit)

                    returning_pathspec = []
                    for subel in shape_el.pathspec or []:
                        if isinstance(subel.expr.steps[0],
                                      qlast.LinkPropExprNode):
                            returning_pathspec.append(subel)

                    substmt = irast.InsertStmt(
                        shape=el,
                        result=self._process_shape(
                            targetstep,
                            ptrcls,
                            returning_pathspec,
                            include_implicit=True
                        )
                    )
                    el = irast.SubstmtRef(stmt=substmt, rptr=ptr_node)
                    elements.append(el)
                    continue

                else:
                    el = self._process_shape(
                        targetstep,
                        ptrcls,
                        shape_el.pathspec or [],
                        _visited=_memo,
                        _recurse=True,
                        require_expressions=require_expressions,
                        include_implicit=include_implicit)
            else:
                el = targetstep

            if (not ptr_singular or recurse is not None) and el is not None:
                substmt = irast.SelectStmt()
                substmt.where = where

                if orderby:
                    substmt.orderby = orderby

                substmt.offset = offset
                substmt.limit = limit

                if recurse is not None:
                    substmt.recurse_ptr = ptr_node
                    substmt.recurse_depth = recurse

                substmt.result = el
                el = irast.SubstmtRef(stmt=substmt, rptr=ptr_node)

            # Record element may be none if ptrcls target is non-atomic
            # and recursion has been prohibited on this level to prevent
            # infinite looping.
            if el is not None:
                elements.append(el)

        return shape

    def _extend_path(self, source_set, ptrcls,
                     direction=s_pointers.PointerDirection.Outbound,
                     target=None):
        """Return a Set node representing the new path tip."""
        ctx = self.context.current

        if target is None:
            target = ptrcls.get_far_endpoint(direction)

        path_id = irutils.LinearPath(source_set.path_id)
        path_id.add(ptrcls, direction, target)

        try:
            target_set = ctx.sets[path_id]
        except KeyError:
            target_set = ctx.sets[path_id] = irast.Set()
            target_set.scls = target
            target_set.path_id = path_id

            ptr = irast.Pointer(
                source=source_set,
                target=target_set,
                ptrcls=ptrcls,
                direction=direction
            )

            target_set.rptr = ptr

        return target_set

    def _get_subset(self, parent_set):
        return irast.Set(
            scls=parent_set.scls,
            path_id=parent_set.path_id
        )

    def _resolve_ptr(self,
                     near_endpoint,
                     ptr_name,
                     direction,
                     target=None):
        ptr_module, ptr_nqname = ptr_name

        if ptr_module:
            ptr_fqname = sn.Name(module=ptr_module, name=ptr_nqname)
            modaliases = self.context.current.namespaces
            pointer = self.schema.get(ptr_fqname,
                                      module_aliases=modaliases)
            pointer_name = pointer.name
        else:
            pointer_name = ptr_fqname = ptr_nqname

        if target is not None and not isinstance(target, s_obj.NodeClass):
            target_name = '::'.join(filter(None, target))
            modaliases = self.context.current.namespaces
            target = self.schema.get(target_name,
                                     module_aliases=modaliases)

        if target is not None:
            far_endpoints = (target, )
        else:
            far_endpoints = None

        ptr = None

        if isinstance(near_endpoint, s_sources.Source):
            ptr = near_endpoint.resolve_pointer(
                self.schema,
                pointer_name,
                direction=direction,
                look_in_children=False,
                include_inherited=True,
                far_endpoints=far_endpoints)
        else:
            if direction == s_pointers.PointerDirection.Outbound:
                modaliases = self.context.current.namespaces
                bptr = self.schema.get(pointer_name, module_aliases=modaliases)
                schema_cls = self.schema.get('schema::Atom')
                if bptr.shortname == 'std::__class__':
                    ptr = bptr.derive(self.schema, near_endpoint, schema_cls)

        if not ptr:
            msg = ('({near_endpoint}).{direction}({ptr_name}{far_endpoint}) '
                   'does not resolve to any known path')
            far_endpoint_str = ' TO {}'.format(target.name) if target else ''
            msg = msg.format(
                near_endpoint=near_endpoint.name,
                direction=direction,
                ptr_name=pointer_name,
                far_endpoint=far_endpoint_str)
            raise errors.EdgeQLReferenceError(msg)

        return ptr

    def _get_schema_object(self, name, module=None):
        ctx = self.context.current

        if isinstance(name, qlast.ClassRefNode):
            module = name.module
            name = name.name

        if module:
            name = sn.Name(name=name, module=module)

        return ctx.schema.get(name=name, module_aliases=ctx.namespaces)

    def _process_stmt_result(self, target):
        toplevel_rptrcls = self.context.current.toplevel_shape_rptrcls

        with self.context():
            self.context.current.location = 'selector'

            expr = self.visit(target.expr)

            if (isinstance(expr, irast.Set) and
                    isinstance(expr.scls, s_concepts.Concept)):
                if expr.rptr is not None:
                    rptrcls = expr.rptr.ptrcls
                else:
                    rptrcls = toplevel_rptrcls

                expr = self._process_shape(
                    expr, rptrcls, target.expr.pathspec or [])

        return expr

    def _process_select_where(self, where):
        with self.context():
            self.context.current.location = 'generator'

            if where is not None:
                return self.visit(where)
            else:
                return None

    def _process_orderby(self, sortexprs):

        result = []

        if not sortexprs:
            return result

        with self.context():
            self.context.current.location = 'orderby'
            exprs = self.visit([s.path for s in sortexprs])

            for i, sortexpr in enumerate(sortexprs):
                result.append(
                    irast.SortExpr(
                        expr=exprs[i],
                        direction=sortexpr.direction,
                        nones_order=sortexpr.nones_order))

        return result

    def _process_groupby(self, groupers):

        result = []

        if groupers:
            with self.context():
                self.context.current.location = 'grouper'
                for grouper in groupers:
                    expr = self.visit(grouper)
                    result.append(expr)

        return result

    def _process_type_ref_elem(self, expr, qlcontext):
        if isinstance(expr, irast.Set):
            if expr.rptr is not None:
                raise errors.EdgeQLSyntaxError(
                    'expecting a type reference',
                    context=qlcontext)

            result = irast.TypeRef(
                maintype=expr.scls.name,
            )

        else:
            raise errors.EdgeQLSyntaxError(
                'expecting a type reference',
                context=qlcontext)

        return result

    def _process_type_ref_expr(self, expr):
        if isinstance(expr, irast.Sequence):
            elems = []

            for elem in expr.elements:
                ref_elem = self._process_type_ref_elem(elem, elem.context)

                elems.append(ref_elem)

            expr.elements = elems
            expr.is_array = True

        else:
            expr = self._process_type_ref_elem(expr, expr.context)

        return expr

    def _is_set_expr(self, expr):
        if isinstance(expr, irast.Set):
            return expr
        elif (isinstance(expr, irast.ExistPred) and
                isinstance(expr.expr, irast.Set)):
            return expr.expr
        else:
            return None

    def _is_type_check(self, left, right, op):
        return (not reversed and op in (ast.ops.IS, ast.ops.IS_NOT) and
                isinstance(left, irast.Path))

    def _is_constant(self, expr):
        flt = lambda node: isinstance(node, irast.Path)
        paths = ast.visitor.find_children(expr, flt)
        return not paths and not isinstance(expr, irast.Path)

    def _is_func_agg(self, name):
        if isinstance(name, str):
            name = (None, name)

        return self._get_schema_object(
            name=name[1], module=name[0]).aggregate

    def _get_selector_types(self, selexpr):
        schema = self.context.current.schema

        expr_type = irutils.infer_type(selexpr, schema)

        if isinstance(selexpr, irast.Constant):
            expr_kind = 'constant'
        elif isinstance(selexpr, (irast.Set, irast.Shape)):
            expr_kind = 'path'
        else:
            expr_kind = 'expression'
        return (expr_type, expr_kind)
