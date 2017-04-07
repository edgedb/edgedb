##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""EdgeQL to IR compiler implementation."""

import collections
import itertools

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import inference as irinference
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import expr as s_expr
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import sources as s_sources
from edgedb.lang.schema import types as s_types
from edgedb.lang.schema import utils as s_utils

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.edgeql import errors
from edgedb.lang.edgeql import parser as qlparser

from edgedb.lang.common import ast
from edgedb.lang.common import exceptions as edgedb_error

from .context import CompilerContext


class EdgeQLCompilerError(edgedb_error.EdgeDBError):
    pass


class PathExtractor(ast.visitor.NodeVisitor):
    def __init__(self, roots_only=False):
        super().__init__()
        self.paths = collections.OrderedDict()
        self.roots_only = roots_only

    def visit_Stmt(self, expr):
        pass

    def visit_Set(self, expr):
        key = expr.path_id

        if expr.expr is not None:
            self.visit(expr.expr)

        if expr.rptr is not None:
            self.visit(expr.rptr.source)

        if key and (not self.roots_only or expr.rptr is None):
            if key not in self.paths:
                self.paths[key] = {expr}
            else:
                self.paths[key].add(expr)

    def visit_FunctionCall(self, expr):
        if expr.func.aggregate:
            pass
        else:
            self.generic_visit(expr)


def extract_prefixes(expr, roots_only=False):
    extractor = PathExtractor(roots_only=roots_only)
    extractor.visit(expr)
    return extractor.paths


class EdgeQLCompiler(ast.visitor.NodeVisitor):
    def __init__(self, schema, modaliases=None):
        super().__init__()
        self.schema = schema
        self.modaliases = modaliases

    @property
    def memo(self):
        return {}

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
        context.current.clause = location or 'where'
        return self.visit(edgeql_tree)

    def generic_visit(self, node, *, combine_results=None):
        raise NotImplementedError(
            'no EdgeQL compiler handler for {}'.format(node.__class__))

    def visit_SelectQuery(self, edgeql_tree):
        parent_ctx = self.context.current
        toplevel_shape_rptr = parent_ctx.toplevel_shape_rptr
        is_toplevel = parent_ctx.stmt is None

        with self.context.subquery() as ctx:
            stmt = ctx.stmt = irast.SelectStmt()
            stmt.parent_stmt = parent_ctx.stmt
            self._visit_with_block(edgeql_tree)

            output = edgeql_tree.result

            if (isinstance(output, qlast.Shape) and
                    isinstance(output.expr, qlast.Path) and
                    output.expr.steps):
                ctx.result_path_steps = output.expr.steps

            stmt.result = self._process_stmt_result(
                edgeql_tree.result, toplevel_shape_rptr,
                edgeql_tree.result_alias)

            stmt.where = self._process_select_where(edgeql_tree.where)

            stmt.orderby = self._process_orderby(edgeql_tree.orderby)

            if edgeql_tree.offset:
                offset_ir = self.visit(edgeql_tree.offset)
                offset_ir.context = edgeql_tree.offset.context
                self._enforce_singleton(offset_ir)
                stmt.offset = offset_ir

            if edgeql_tree.limit:
                limit_ir = self.visit(edgeql_tree.limit)
                limit_ir.context = edgeql_tree.limit.context
                self._enforce_singleton(limit_ir)
                stmt.limit = limit_ir

            if is_toplevel:
                stmt.argument_types = self.context.current.arguments
                result = stmt
            else:
                result = self._generated_set(stmt)
                if isinstance(stmt.result, irast.Set):
                    result.path_id = stmt.result.path_id

            stmt.aggregated_scope = set(ctx.aggregated_scope)
            stmt.path_scope = ctx.path_scope
            stmt.specific_path_scope = {
                ctx.sets[p] for p in ctx.stmt_path_scope
                if p in ctx.sets and p in stmt.path_scope
            }

        # Query cardinality inference must be ran in parent context.
        if edgeql_tree.single:
            stmt.result.context = edgeql_tree.result.context
            # XXX: correct cardinality inference depends on
            # query selectivity estimator, which is not done yet.
            # self._enforce_singleton(stmt.result)
            stmt.singleton = True

        return result

    def visit_GroupQuery(self, edgeql_tree):
        parent_ctx = self.context.current
        toplevel_shape_rptr = parent_ctx.toplevel_shape_rptr
        is_toplevel = parent_ctx.stmt is None
        parent_path_scope = parent_ctx.path_scope.copy()

        with self.context.subquery() as ctx:
            stmt = ctx.stmt = irast.GroupStmt()
            stmt.parent_stmt = parent_ctx.stmt
            self._visit_with_block(edgeql_tree)

            with self.context.new() as subjctx:
                subjctx.clause = 'input'
                stmt.subject = self._declare_aliased_set(
                    self.visit(edgeql_tree.subject),
                    edgeql_tree.subject_alias)

            stmt.groupby = self._process_groupby(edgeql_tree.groupby)
            ctx.group_paths = set(extract_prefixes(stmt.groupby))

            output = edgeql_tree.result

            if (isinstance(output, qlast.Shape) and
                    isinstance(output.expr, qlast.Path) and
                    output.expr.steps):
                ctx.result_path_steps = output.expr.steps

            with self.context.subquery(), self.context.newscope() as sctx:
                sctx.group_paths = ctx.group_paths.copy()
                # Ignore scope in GROUP ... BY
                sctx.path_scope = parent_path_scope

                o_stmt = sctx.stmt = irast.SelectStmt()

                o_stmt.result = self._process_stmt_result(
                    edgeql_tree.result, toplevel_shape_rptr,
                    edgeql_tree.result_alias)

                o_stmt.where = self._process_select_where(edgeql_tree.where)
                o_stmt.orderby = self._process_orderby(edgeql_tree.orderby)

                if edgeql_tree.offset:
                    offset_ir = self.visit(edgeql_tree.offset)
                    offset_ir.context = edgeql_tree.offset.context
                    self._enforce_singleton(offset_ir)
                    o_stmt.offset = offset_ir

                if edgeql_tree.limit:
                    limit_ir = self.visit(edgeql_tree.limit)
                    limit_ir.context = edgeql_tree.limit.context
                    self._enforce_singleton(limit_ir)
                    o_stmt.limit = limit_ir

                o_stmt.path_scope = sctx.path_scope

                o_stmt.specific_path_scope = {
                    sctx.sets[p] for p in sctx.stmt_path_scope
                    if p in sctx.sets and p in o_stmt.path_scope
                }

            stmt.result = self._generated_set(o_stmt)

            if is_toplevel:
                stmt.argument_types = self.context.current.arguments
                result = stmt
            else:
                result = self._generated_set(stmt)
                if isinstance(stmt.result, irast.Set):
                    result.path_id = stmt.result.path_id

            stmt.aggregated_scope = set(ctx.aggregated_scope)
            stmt.path_scope = ctx.path_scope
            stmt.specific_path_scope = {
                ctx.sets[p] for p in ctx.stmt_path_scope if p in ctx.sets
            }

        return result

    def visit_InsertQuery(self, edgeql_tree):
        parent_ctx = self.context.current
        is_toplevel = parent_ctx.stmt is None

        with self.context.subquery() as ctx:
            stmt = ctx.stmt = irast.InsertStmt()
            stmt.parent_stmt = parent_ctx.stmt
            self._visit_with_block(edgeql_tree)

            subject = self.visit(edgeql_tree.subject)

            stmt.subject = self._process_shape(
                subject, edgeql_tree.shape,
                require_expressions=True,
                include_implicit=False)

            stmt.result = subject

            explicit_ptrs = {
                el.rptr.ptrcls.shortname for el in stmt.subject.shape
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
                    default_expr = qlast.Constant(value=ptrcls.default)

                substmt = self._ensure_stmt(self.visit(default_expr))
                el = self._generated_set(substmt)
                el.rptr = targetstep.rptr
                stmt.subject.shape.append(el)

            if is_toplevel:
                stmt.argument_types = self.context.current.arguments
                result = stmt
            else:
                result = self._generated_set(stmt)
                if isinstance(stmt.result, irast.Set):
                    result.path_id = stmt.result.path_id

            stmt.aggregated_scope = set(ctx.aggregated_scope)
            stmt.path_scope = ctx.path_scope
            stmt.specific_path_scope = {
                ctx.sets[p] for p in ctx.stmt_path_scope if p in ctx.sets
            }

            return result

    def visit_UpdateQuery(self, edgeql_tree):
        parent_ctx = self.context.current
        is_toplevel = parent_ctx.stmt is None

        with self.context.subquery() as ctx:
            stmt = ctx.stmt = irast.UpdateStmt()
            stmt.parent_stmt = parent_ctx.stmt
            self._visit_with_block(edgeql_tree)

            subject = self._declare_aliased_set(
                self.visit(edgeql_tree.subject), edgeql_tree.subject_alias)

            subj_type = irutils.infer_type(subject, ctx.schema)
            if not isinstance(subj_type, s_concepts.Concept):
                raise errors.EdgeQLError(
                    f'cannot update non-Concept objects',
                    context=edgeql_tree.subject.context
                )

            stmt.where = self._process_select_where(edgeql_tree.where)

            stmt.subject = self._process_shape(
                subject, edgeql_tree.shape,
                require_expressions=True,
                include_implicit=False)

            stmt.result = subject

            if is_toplevel:
                stmt.argument_types = self.context.current.arguments
                result = stmt
            else:
                result = self._generated_set(stmt)
                if isinstance(stmt.result, irast.Set):
                    result.path_id = stmt.result.path_id

            stmt.aggregated_scope = set(ctx.aggregated_scope)
            stmt.path_scope = ctx.path_scope
            stmt.specific_path_scope = {
                ctx.sets[p] for p in ctx.stmt_path_scope if p in ctx.sets
            }

            return result

    def visit_DeleteQuery(self, edgeql_tree):
        parent_ctx = self.context.current
        is_toplevel = parent_ctx.stmt is None

        with self.context.subquery() as ctx:
            stmt = ctx.stmt = irast.DeleteStmt()
            stmt.parent_stmt = parent_ctx.stmt
            self._visit_with_block(edgeql_tree)

            subject = self._declare_aliased_set(
                self.visit(edgeql_tree.subject), edgeql_tree.subject_alias)

            subj_type = irutils.infer_type(subject, ctx.schema)
            if not isinstance(subj_type, s_concepts.Concept):
                raise errors.EdgeQLError(
                    f'cannot delete non-Concept objects',
                    context=edgeql_tree.subject.context
                )

            stmt.subject = stmt.result = subject

            if is_toplevel:
                stmt.argument_types = self.context.current.arguments
                result = stmt
            else:
                result = self._generated_set(stmt)
                if isinstance(stmt.result, irast.Set):
                    result.path_id = stmt.result.path_id

            stmt.aggregated_scope = set(ctx.aggregated_scope)
            stmt.path_scope = ctx.path_scope
            stmt.specific_path_scope = {
                ctx.sets[p] for p in ctx.stmt_path_scope if p in ctx.sets
            }

            return result

    def visit_Shape(self, expr):
        subj = self.visit(expr.expr)
        return self._process_shape(subj, expr.elements)

    def visit_Path(self, expr):
        ctx = self.context.current

        pathvars = ctx.pathvars
        anchors = ctx.anchors

        path_tip = None

        if expr.partial:
            if ctx.result_path_steps:
                expr.steps = ctx.result_path_steps + expr.steps
            else:
                raise errors.EdgeQLError('could not resolve partial path ',
                                         context=expr.context)

        for i, step in enumerate(expr.steps):
            if isinstance(step, qlast.ClassRef):
                if i > 0:
                    raise RuntimeError(
                        'unexpected ClassRef as a non-first path item')

                refnode = None

                if not step.module:
                    # Check if the starting path label is a known anchor
                    refnode = anchors.get(step.name)

                if refnode is None:
                    # Check if the starting path label is a known
                    # path variable (defined in a WITH clause).
                    refnode = pathvars.get(step.name)

                if refnode is None:
                    # Finally, check if the starting path label is
                    # a query defined as a view.
                    if path_tip is not None:
                        src_path_id = path_tip.path_id
                    else:
                        src_path_id = None

                    if not step.module:
                        refnode = ctx.substmts.get((step.name, src_path_id))

                    if refnode is None:
                        schema_name = self._resolve_schema_name(
                            step.name, step.module)
                        refnode = ctx.substmts.get((schema_name, src_path_id))

                if refnode is not None:
                    path_tip = refnode
                    continue

            if isinstance(step, qlast.ClassRef):
                # Starting path label.  Must be a valid reference to an
                # existing Concept class, as aliases and path variables
                # have been checked above.
                scls = self._get_schema_object(step.name, step.module)

                path_id = irast.PathId([scls])

                try:
                    # We maintain a registry of Set nodes for each unique
                    # Path to achieve path prefix matching.
                    path_tip = ctx.sets[path_id]
                except KeyError:
                    path_tip = ctx.sets[path_id] = self._class_set(scls)

            elif isinstance(step, qlast.Ptr):
                # Pointer traversal step
                ptr_expr = step
                ptr_target = None

                direction = (ptr_expr.direction or
                             s_pointers.PointerDirection.Outbound)
                if ptr_expr.target:
                    # ... link [IS Target]
                    ptr_target = self._get_schema_object(
                        ptr_expr.target.name, ptr_expr.target.module)
                    if not isinstance(ptr_target, s_concepts.Concept):
                        raise errors.EdgeQLError(
                            f'invalid type filter operand: {ptr_target.name} '
                            f'is not a concept',
                            context=ptr_expr.target.context)

                ptr_name = (ptr_expr.ptr.module, ptr_expr.ptr.name)

                if ptr_expr.type == 'property':
                    # Link property reference; the source is the
                    # link immediately preceding this step in the path.
                    source = path_tip.rptr.ptrcls
                else:
                    source = path_tip.scls

                path_tip, _ = self._path_step(
                    path_tip, source, ptr_name, direction, ptr_target,
                    source_context=step.context)

            else:
                # Arbitrary expression
                if i > 0:
                    raise RuntimeError(
                        'unexpected expression as a non-first path item')

                expr = self.visit(step)
                if isinstance(expr, irast.Set):
                    path_tip = expr
                else:
                    path_tip = self._generated_set(expr)

        if isinstance(path_tip, irast.Set):
            self._register_path_scope(path_tip.path_id)

            if ctx.in_aggregate or ctx.clause == 'groupby':
                ctx.aggregated_scope[path_tip.path_id] = expr.context

                if (isinstance(path_tip.scls, s_atoms.Atom) and
                        path_tip.rptr is not None and
                        path_tip.rptr.ptrcls.singular(
                            path_tip.rptr.direction)):
                    ctx.aggregated_scope[path_tip.rptr.source.path_id] = \
                        expr.context

                if path_tip.path_id in ctx.unaggregated_scope:
                    srcctx = ctx.unaggregated_scope.get(path_tip.path_id)
                    raise errors.EdgeQLError(
                        f'{path_tip.path_id!r} must appear in the '
                        'GROUP ... BY expression or used in an '
                        'aggregate function.', context=srcctx)
            elif (not isinstance(ctx.stmt, irast.GroupStmt) or
                    ctx.clause not in {'input', 'groupby'}):
                for agg_path in ctx.aggregated_scope:
                    if (path_tip.path_id.startswith(agg_path) and
                            path_tip.path_id not in ctx.group_paths):
                        raise errors.EdgeQLError(
                            f'{path_tip.path_id!r} must appear in the '
                            'GROUP ... BY expression or used in an '
                            'aggregate function.', context=expr.context)

                ctx.unaggregated_scope[path_tip.path_id] = expr.context

        return path_tip

    def _try_fold_arithmetic_binop(self, op, left: irast.BinOp,
                                   right: irast.BinOp):
        ctx = self.context.current

        left_type = irutils.infer_type(left, ctx.schema)
        right_type = irutils.infer_type(right, ctx.schema)

        if (left_type.name not in {'std::int', 'std::float'} or
                right_type.name not in {'std::int', 'std::float'}):
            return

        result_type = left_type
        if right_type.name == 'std::float':
            result_type = right_type

        if op == ast.ops.ADD:
            value = left.value + right.value
        elif op == ast.ops.SUB:
            value = left.value - right.value
        elif op == ast.ops.MUL:
            value = left.value * right.value
        elif op == ast.ops.DIV:
            if left_type.name == right_type.name == 'std::int':
                value = left.value // right.value
            else:
                value = left.value / right.value
        elif op == ast.ops.POW:
            value = left.value ** right.value
        elif op == ast.ops.MOD:
            value = left.value % right.value
        else:
            value = None

        if value is not None:
            return irast.Constant(value=value, type=result_type)

    def _try_fold_binop(self, binop: irast.BinOp):
        ctx = self.context.current

        result_type = irutils.infer_type(binop, ctx.schema)
        folded = None

        left = binop.left
        if isinstance(left, irast.Set) and left.expr is not None:
            left = left.expr
        right = binop.right
        if isinstance(right, irast.Set) and right.expr is not None:
            right = right.expr
        op = binop.op

        if (isinstance(left, irast.Constant) and
                isinstance(right, irast.Constant) and
                result_type.name in {'std::int', 'std::float'}):

            # Left and right nodes are constants.
            folded = self._try_fold_arithmetic_binop(op, left, right)

        elif op in {ast.ops.ADD, ast.ops.MUL}:
            # Let's check if we have (CONST + (OTHER_CONST + X))
            # tree, which can be optimized to ((CONST + OTHER_CONST) + X)

            my_const = left
            other_binop = right
            if isinstance(right, irast.Constant):
                my_const, other_binop = other_binop, my_const

            if (isinstance(my_const, irast.Constant) and
                    isinstance(other_binop, irast.BinOp) and
                    other_binop.op == op):

                other_const = other_binop.left
                other_binop_node = other_binop.right
                if isinstance(other_binop_node, irast.Constant):
                    other_binop_node, other_const = \
                        other_const, other_binop_node

                if isinstance(other_const, irast.Constant):
                    new_const = self._try_fold_arithmetic_binop(
                        op, other_const, my_const)

                    if new_const is not None:
                        folded = irast.BinOp(
                            left=new_const,
                            right=other_binop_node,
                            op=op)

        return folded

    def visit_BinOp(self, expr):
        if isinstance(expr.op, ast.ops.TypeCheckOperator):
            op_node = self._visit_type_check_op(expr)
        elif isinstance(expr.op, qlast.SetOperator):
            op_node = self._visit_set_op(expr)
        elif isinstance(expr.op, qlast.EquivalenceOperator):
            op_node = self._visit_equivalence_op(expr)
        else:
            left = self.visit(expr.left)
            right = self.visit(expr.right)
            op_node = irast.BinOp(left=left, right=right, op=expr.op)

        folded = self._try_fold_binop(op_node)
        if folded is not None:
            return folded
        else:
            return self._generated_set(op_node)

    def _visit_type_check_op(self, expr):
        # <Expr> IS <Type>
        ctx = self.context.current

        left = self.visit(expr.left)
        with self.context.new() as subctx:
            subctx.path_as_type = True
            right = self.visit(expr.right)

        ltype = irutils.infer_type(left, ctx.schema)
        left, _ = self._path_step(
            left, ltype, ('std', '__class__'),
            s_pointers.PointerDirection.Outbound, None,
            expr.context)
        right = self._process_type_ref_expr(right)

        return irast.BinOp(left=left, right=right, op=expr.op)

    def _visit_set_op(self, expr):
        # UNION/EXCEPT/INTERSECT
        ctx = self.context.current

        left = self.visit(self._ensure_qlstmt(expr.left))
        right = self.visit(self._ensure_qlstmt(expr.right))

        result = irast.SetOp(left=left.expr, right=right.expr, op=expr.op)
        rtype = irutils.infer_type(result, ctx.schema)
        path_id = irast.PathId([rtype])
        ctx.path_scope[path_id] += 1
        ctx.stmt_path_scope[path_id] += 1

        return result

    def _visit_equivalence_op(self, expr):
        #
        # a ?= b is defined as:
        #   a = b IF EXISTS a AND EXISTS b ELSE EXISTS a = EXISTS b
        # a ?!= b is defined as:
        #   a != b IF EXISTS a AND EXISTS b ELSE EXISTS a != EXISTS b
        #
        op = ast.ops.EQ if expr.op == qlast.EQUIVALENT else ast.ops.NE

        ex_left = qlast.ExistsPredicate(expr=expr.left)
        ex_right = qlast.ExistsPredicate(expr=expr.right)

        condition = qlast.BinOp(
            left=ex_left,
            right=ex_right,
            op=ast.ops.AND
        )

        if_expr = qlast.BinOp(
            left=expr.left,
            right=expr.right,
            op=op
        )

        else_expr = qlast.BinOp(
            left=ex_left,
            right=ex_right,
            op=op
        )

        return self._transform_ifelse(
            condition, if_expr, else_expr, expr.context)

    def visit_Parameter(self, expr):
        ctx = self.context.current

        pt = ctx.arguments.get(expr.name)
        if pt is not None and not isinstance(pt, s_obj.NodeClass):
            pt = s_types.normalize_type(pt, ctx.schema)

        return irast.Parameter(type=pt, name=expr.name)

    def visit_EmptySet(self, expr):
        return irast.EmptySet()

    def visit_Constant(self, expr):
        ctx = self.context.current

        ct = s_types.normalize_type(expr.value.__class__, ctx.schema)
        # TODO: visit expr.value?
        return irast.Constant(value=expr.value, type=ct)

    def visit_EmptyCollection(self, expr):
        raise errors.EdgeQLError(
            f'could not determine type of empty collection',
            context=expr.context)

    def visit_TupleElement(self, expr):
        name = expr.name.name
        if expr.name.module:
            name = f'{expr.name.module}::{name}'

        val = self._ensure_set(self.visit(expr.val))

        element = irast.TupleElement(
            name=name,
            val=val,
        )

        return element

    def visit_NamedTuple(self, expr):
        elements = self.visit(expr.elements)
        return self._generated_set(irast.Tuple(elements=elements, named=True))

    def visit_Tuple(self, expr):
        elements = []

        for i, el in enumerate(expr.elements):
            element = irast.TupleElement(
                name=str(i),
                val=self.visit(el)
            )
            elements.append(element)

        return self._generated_set(irast.Tuple(elements=elements))

    def visit_Mapping(self, expr):
        keys = [self.visit(k) for k in expr.keys]
        values = [self.visit(v) for v in expr.values]
        return irast.Mapping(keys=keys, values=values)

    def visit_Array(self, expr):
        elements = self.visit(expr.elements)
        return irast.Array(elements=elements)

    def _check_function(self, func, arg_types):
        if not func.paramtypes:
            if not arg_types:
                # Match: `func` is a function without parameters
                # being called with no arguments.
                return True
            else:
                # No match: `func` is a function without parameters
                # being called with some arguments.
                return False

        if not arg_types:
            # Call without arguments
            for pi, pd in enumerate(func.paramdefaults, 1):
                if pd is None and pi != func.varparam:
                    # There is at least one non-variadic parameter
                    # without default; hence this function cannot
                    # be called without arguments.
                    return False
            return True

        for pt, at in itertools.zip_longest(func.paramtypes, arg_types):
            if pt is None:
                # We have more arguments then parameters.
                if func.varparam is not None:
                    # Function has a variadic parameter
                    # (which must be the last one).
                    pt = func.paramtypes[func.varparam - 1]  # varparam is +1
                else:
                    # No variadic parameter, hence no match.
                    return False

            if not at.issubclass(pt):
                return False

        # Match, the `func` passed all checks.
        return True

    def _process_func_args(self, expr, funcname):
        ctx = self.context.current

        args = []
        kwargs = {}
        arg_types = []
        for ai, a in enumerate(expr.args):
            if isinstance(a, qlast.NamedArg):
                arg = self._ensure_set(self.visit(a.arg))
                kwargs[a.name] = arg
                aname = a.name
            else:
                arg = self._ensure_set(self.visit(a))
                args.append(arg)
                aname = ai

            arg_type = irutils.infer_type(arg, ctx.schema)
            if arg_type is None:
                raise errors.EdgeQLError(
                    f'could not resolve the type of argument '
                    f'${aname} of function {funcname}',
                    context=a.context)
            arg_types.append(arg_type)

        return args, kwargs, arg_types

    def visit_FunctionCall(self, expr):
        with self.context.new():
            ctx = self.context.current

            if isinstance(expr.func, str):
                funcname = expr.func
            else:
                funcname = sn.Name(expr.func[1], expr.func[0])

            funcs = ctx.schema.get_functions(
                funcname, module_aliases=ctx.namespaces)

            if funcs is None:
                raise errors.EdgeQLError(
                    f'could not resolve function name {funcname}',
                    context=expr.context)

            ctx.in_func_call = True

            is_agg = any(f.aggregate for f in funcs)
            if is_agg:
                ctx.in_aggregate = True

                if expr.agg_set_modifier == qlast.AggNONE:
                    raise errors.EdgeQLError(
                        f"aggregate function {funcname} is missing a required"
                        " modifier 'ALL' or 'DISTINCT'",
                        context=expr.context)

            path_scope = {}
            stmt_path_scope = set()
            agg_sort = []
            agg_filter = None
            partition = []
            window = False

            if is_agg:
                # When processing calls to aggregate functions,
                # we do not want to affect the statement-wide path scope,
                # so put a newscope barrier here.  Store the scope
                # obtained by processing the agg call in the resulting
                # IR Set.
                with self.context.newscope() as scope_ctx:
                    args, kwargs, arg_types = \
                        self._process_func_args(expr, funcname)

                    if expr.agg_sort:
                        agg_sort = [
                            irast.SortExpr(
                                expr=self.visit(e.path),
                                direction=e.direction) for e in expr.agg_sort
                        ]

                    elif expr.window:
                        if expr.window.orderby:
                            agg_sort = [
                                irast.SortExpr(
                                    expr=self.visit(e.path),
                                    direction=e.direction)
                                for e in expr.window.orderby
                            ]

                        if expr.window.partition:
                            for partition_expr in expr.window.partition:
                                partition_expr = self.visit(partition_expr)
                                partition.append(partition_expr)

                        window = True

                    if expr.agg_filter:
                        agg_filter = self.visit(expr.agg_filter)

                    path_scope = scope_ctx.path_scope.copy()
                    stmt_path_scope = {
                        ctx.sets[p] for p in scope_ctx.stmt_path_scope
                        if p in ctx.sets
                    }

                self._update_pending_path_scope(ctx.path_scope)

            else:
                args, kwargs, arg_types = \
                    self._process_func_args(expr, funcname)

            for funcobj in funcs:
                if self._check_function(funcobj, arg_types):
                    break
            else:
                raise errors.EdgeQLError(
                    f'could not find a function variant {funcname}',
                    context=expr.context)

            node = irast.FunctionCall(
                func=funcobj, args=args, kwargs=kwargs,
                window=window, partition=partition,
                agg_sort=agg_sort, agg_filter=agg_filter,
                agg_set_modifier=expr.agg_set_modifier)

            if funcobj.initial_value is not None:
                rtype = irutils.infer_type(node, ctx.schema)
                iv_ql = qlast.TypeCast(
                    expr=qlparser.parse_fragment(funcobj.initial_value),
                    type=self._type_to_ql_typeref(rtype)
                )
                node.initial_value = self.visit(iv_ql)

        ir_set = self._generated_set(node)
        ir_set.path_scope = path_scope
        ir_set.stmt_path_scope = stmt_path_scope

        return ir_set

    def _transform_ifelse(self, condition, if_expr, else_expr, src_context):
        ctx = self.context.current

        if_expr = self._ensure_qlstmt(if_expr)
        if_expr.where = self._extend_qlbinop(if_expr.where, condition)

        not_condition = qlast.UnaryOp(operand=condition, op=ast.ops.NOT)
        else_expr = self._ensure_qlstmt(else_expr)
        else_expr.where = self._extend_qlbinop(else_expr.where, not_condition)

        if_expr = self.visit(if_expr)
        else_expr = self.visit(else_expr)

        if_expr_type = irutils.infer_type(if_expr, ctx.schema)
        else_expr_type = irutils.infer_type(else_expr, ctx.schema)

        result = s_utils.get_class_nearest_common_ancestor(
            [if_expr_type, else_expr_type])

        if result is None:
            raise errors.EdgeQLError(
                'if/else clauses must be of related types, got: {}/{}'.format(
                    if_expr_type.name, else_expr_type.name),
                context=src_context)

        return irast.SetOp(left=if_expr.expr, right=else_expr.expr,
                           op=qlast.UNION)

    def visit_IfElse(self, expr):
        ifelse_op = self._transform_ifelse(expr.condition, expr.if_expr,
                                           expr.else_expr, expr.context)
        return self._generated_set(ifelse_op)

    def visit_UnaryOp(self, expr):
        ctx = self.context.current

        operand = self.visit(expr.operand)

        if self._is_exists_expr_set(operand):
            operand.expr.negated = not operand.expr.negated
            return operand

        unop = irast.UnaryOp(expr=operand, op=expr.op)
        result_type = irutils.infer_type(unop, ctx.schema)

        if (isinstance(operand, irast.Constant) and
                result_type.name in {'std::int', 'std::float'}):
            # Fold the operation to constant if possible
            if expr.op == ast.ops.UMINUS:
                return irast.Constant(value=-operand.value, type=result_type)
            elif expr.op == ast.ops.UPLUS:
                return operand

        return self._generated_set(unop)

    def visit_ExistsPredicate(self, expr):
        with self.context.newscope() as ctx:
            # EXISTS is a special aggregate, so we need to put a scope
            # fence for the same reasons we do for aggregates.
            operand = self.visit(expr.expr)
            if self._is_strict_subquery_set(operand):
                operand = operand.expr
            ir_set = self._generated_set(irast.ExistPred(expr=operand))

            ir_set.path_scope = ctx.path_scope.copy()
            ir_set.stmt_path_scope = {
                ctx.sets[p] for p in ctx.stmt_path_scope
                if p in ctx.sets
            }

        self._update_pending_path_scope(ctx.path_scope)

        return ir_set

    def visit_Coalesce(self, expr):
        if all(isinstance(a, qlast.EmptySet) for a in expr.args):
            return irast.EmptySet()

        return irast.Coalesce(args=self.visit(expr.args))

    def visit_TypeCast(self, expr):
        maintype = expr.type.maintype
        typ = self._ql_typeref_to_ir_typeref(expr.type)

        if isinstance(expr.expr, qlast.EmptyCollection):
            if maintype.name == 'array':
                wrapped = irast.Array()
            elif maintype.name == 'map':
                wrapped = irast.Mapping()
            else:
                wrapped = self.visit(expr.expr)
        else:
            wrapped = self.visit(expr.expr)

        return irast.TypeCast(expr=wrapped, type=typ)

    def visit_TypeFilter(self, expr):
        # Expr[IS Type] expressions,
        arg = self.visit(expr.expr)
        arg_type = irutils.infer_type(arg, self.context.current.schema)
        if not isinstance(arg_type, s_concepts.Concept):
            raise errors.EdgeQLError(
                f'invalid type filter operand: {arg_type.name} '
                f'is not a concept',
                context=expr.expr.context)

        typ = self._get_schema_object(expr.type.maintype)
        if not isinstance(typ, s_concepts.Concept):
            raise errors.EdgeQLError(
                f'invalid type filter operand: {typ.name} is not a concept',
                context=expr.type.context)

        return self._generated_set(
            irast.TypeFilter(
                expr=arg,
                type=irast.TypeRef(
                    maintype=typ.name
                )
            )
        )

    def visit_Indirection(self, expr):
        node = self.visit(expr.arg)
        int_type = self._get_schema_object('std::int')
        for indirection_el in expr.indirection:
            if isinstance(indirection_el, qlast.Index):
                idx = self.visit(indirection_el.index)
                node = irast.IndexIndirection(expr=node, index=idx)

            elif isinstance(indirection_el, qlast.Slice):
                if indirection_el.start:
                    start = self.visit(indirection_el.start)
                else:
                    start = irast.Constant(value=None, type=int_type)

                if indirection_el.stop:
                    stop = self.visit(indirection_el.stop)
                else:
                    stop = irast.Constant(value=None, type=int_type)

                node = irast.SliceIndirection(
                    expr=node, start=start, stop=stop)
            else:
                raise ValueError('unexpected indirection node: '
                                 '{!r}'.format(indirection_el))

        return node

    def _init_context(self, arg_types, modaliases, anchors, *,
                      security_context=None):
        self.context = context = CompilerContext()
        ctx = self.context.current
        ctx.schema = self.schema

        if modaliases or self.modaliases:
            ctx.namespaces.update(modaliases or self.modaliases)

        if arg_types:
            ctx.arguments.update(arg_types)

        if anchors:
            self._populate_anchors(anchors)

        return context

    def _populate_anchors(self, anchors):
        ctx = self.context.current

        for anchor, scls in anchors.items():
            if isinstance(scls, s_obj.NodeClass):
                step = self._class_set(scls)
                step.anchor = anchor
                step.show_as_anchor = anchor

            elif isinstance(scls, s_links.Link):
                if scls.source:
                    path = self._extend_path(
                        self._class_set(scls.source), scls,
                        s_pointers.PointerDirection.Outbound,
                        scls.target)
                else:
                    Object = ctx.schema.get('std::Object')

                    ptrcls = scls.get_derived(
                        ctx.schema, Object, Object,
                        mark_derived=True, add_to_schema=False)

                    path = self._extend_path(
                        self._class_set(Object), ptrcls,
                        s_pointers.PointerDirection.Outbound,
                        ptrcls.target)

                step = path
                step.anchor = anchor
                step.show_as_anchor = anchor

            elif isinstance(scls, s_lprops.LinkProperty):
                if scls.source.source:
                    path = self._extend_path(
                        self._class_set(scls.source.source), scls.source,
                        s_pointers.PointerDirection.Outbound,
                        scls.source.target)
                else:
                    Object = ctx.schema.get('std::Object')
                    ptrcls = scls.source.get_derived(
                        ctx.schema, Object, Object,
                        mark_derived=True, add_to_schema=False)
                    path = self._extend_path(
                        self._class_set(Object), ptrcls,
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

    def _declare_view(self, expr, alias):
        ctx = self.context.current

        if not isinstance(expr, qlast.Statement):
            expr = qlast.SelectQuery(result=expr)

        with self.context.new() as subctx:
            subctx.stmt = ctx.stmt.parent_stmt
            substmt = self.visit(expr)

        if self._is_subquery_set(substmt):
            substmt = substmt.expr

        result_type = irutils.infer_type(substmt, ctx.schema)

        view_name = sn.Name(module='__view__', name=alias)
        if isinstance(result_type, (s_atoms.Atom, s_concepts.Concept)):
            c = result_type.__class__(name=view_name, bases=[result_type])
            c.acquire_ancestor_inheritance(ctx.schema)
        else:
            c = s_concepts.Concept(name=view_name)

        path_id = irast.PathId([c])

        if isinstance(substmt.result, irast.Set):
            real_path_id = substmt.result.path_id
        else:
            real_path_id = irast.PathId([result_type])

        substmt.main_stmt = ctx.stmt
        substmt.parent_stmt = ctx.stmt.parent_stmt
        substmt_set = irast.Set(
            path_id=path_id,
            real_path_id=real_path_id,
            scls=result_type,
            expr=substmt
        )

        ctx.sets[substmt_set.path_id] = substmt_set
        ctx.substmts[(alias, None)] = substmt_set
        ctx.stmt.substmts.append(substmt_set)
        return substmt_set

    def _declare_aliased_set(self, expr, alias=None):
        ctx = self.context.current

        ir_set = self._ensure_set(expr)

        if alias is not None:
            key = (alias, None)
        elif not isinstance(ir_set.scls, s_obj.Collection):
            rptr = getattr(expr, 'rptr', None)
            if rptr is not None:
                key = (rptr.ptrcls.shortname, rptr.source.path_id)
            else:
                key = (ir_set.path_id[0].name, None)
        else:
            key = None

        if key is not None:
            ctx.substmts[key] = ir_set

        return ir_set

    def _visit_with_block(self, edgeql_tree):
        ctx = self.context.current

        stmt = ctx.stmt
        stmt.substmts = []

        for with_entry in edgeql_tree.aliases:
            if isinstance(with_entry, qlast.NamespaceAliasDecl):
                ctx.namespaces[with_entry.alias] = with_entry.namespace

            elif isinstance(with_entry, qlast.AliasedExpr):
                with self.context.newscope():
                    self._declare_view(with_entry.expr, with_entry.alias)

            else:
                expr = self.visit(with_entry.expr)
                ctx.pathvars[with_entry.alias] = expr

    def _process_unlimited_recursion(self):
        type = s_types.normalize_type((0).__class__, self.schema)
        return irast.Constant(value=0, index=None, type=type)

    def _process_shape(self, source_expr, shapespec, *, rptr=None,
                       require_expressions=False, include_implicit=True,
                       _visited=None, _recurse=True):
        """Build a Shape node given shape spec."""
        ctx = self.context.current

        if _visited is None:
            _visited = {}
        else:
            _visited = _visited.copy()

        scls = source_expr.scls

        elements = []

        if isinstance(scls, s_concepts.Concept):
            if include_implicit:
                implicit_ptrs = (sn.Name('std::id'),)

                implicit_shape_els = []

                for pn in implicit_ptrs:
                    shape_el = qlast.ShapeElement(
                        expr=qlast.Path(steps=[
                            qlast.Ptr(
                                ptr=qlast.ClassRef(
                                    name=pn.name,
                                    module=pn.module
                                )
                            )
                        ])
                    )

                    implicit_shape_els.append(shape_el)

                shapespec = implicit_shape_els + list(shapespec)

        else:
            if include_implicit:
                implicit_ptrs = (sn.Name('std::target'),)

                implicit_shape_els = []

                for pn in implicit_ptrs:
                    shape_el = qlast.ShapeElement(
                        expr=qlast.Path(steps=[
                            qlast.Ptr(
                                ptr=qlast.ClassRef(
                                    name=pn.name,
                                    module=pn.module
                                ),
                                type='property'
                            )
                        ])
                    )

                    implicit_shape_els.append(shape_el)

                shapespec = implicit_shape_els + list(shapespec)

        for shape_el in shapespec:
            with self.context.newscope():
                el = self._process_shape_el(
                    source_expr, shape_el, scls,
                    rptr=rptr,
                    require_expressions=require_expressions,
                    include_implicit=include_implicit,
                    _visited=_visited,
                    _recurse=_recurse,
                    parent_ctx=ctx)

            # Record element may be none if ptrcls target is non-atomic
            # and recursion has been prohibited on this level to prevent
            # infinite looping.
            if el is not None:
                elements.append(el)

        result = irast.Set(
            scls=source_expr.scls,
            path_id=source_expr.path_id,
            source=source_expr,
            shape=elements,
            rptr=source_expr.rptr
        )

        return result

    def _process_shape_el(self, source_expr, shape_el, scls, *, rptr,
                          require_expressions=False, include_implicit=True,
                          _visited=None, _recurse=True, parent_ctx):
        ctx = self.context.current
        ctx.result_path_steps += shape_el.expr.steps

        steps = shape_el.expr.steps
        ptrsource = scls

        if len(steps) == 2:
            # Pointers may be qualified by the explicit source
            # class, which is equivalent to Expr[IS Type].
            ptrsource = self._get_schema_object(
                steps[0].name, steps[0].module)
            lexpr = steps[1]
        elif len(steps) == 1:
            lexpr = steps[0]

        ptrname = (lexpr.ptr.module, lexpr.ptr.name)
        is_linkprop = lexpr.type == 'property'

        if is_linkprop:
            if rptr is None:
                raise errors.EdgeQLError(
                    'invalid reference to link property '
                    'in top level shape')

            ptrsource = rptr.ptrcls

        ptr_direction = \
            lexpr.direction or s_pointers.PointerDirection.Outbound

        if shape_el.compexpr is not None:
            # The shape element is defined as a computable expression.
            targetstep = self._process_shape_compexpr(
                source_expr, shape_el, ptrname, ptrsource, ptr_direction,
                is_linkprop, rptr, lexpr.context)

            ptrcls = targetstep.rptr.ptrcls

        else:
            if lexpr.target is not None:
                ptr_target = self._get_schema_object(
                    module=lexpr.target.module, name=lexpr.target.name)
            else:
                ptr_target = None

            targetstep, ptrcls = self._path_step(
                source_expr, ptrsource, ptrname, ptr_direction, ptr_target,
                source_context=shape_el.context)

            ctx.singletons.add(targetstep)

        self._register_path_scope(targetstep.path_id, ctx=parent_ctx)

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
            orderby = []

        if shape_el.offset is not None:
            offset = self.visit(shape_el.offset)
        else:
            offset = None

        if shape_el.limit is not None:
            limit = self.visit(shape_el.limit)
        else:
            limit = None

        ptr_singular = ptrcls.singular(ptr_direction)
        ptr_node = targetstep.rptr

        if _recurse and shape_el.elements:
            if isinstance(ctx.stmt, irast.InsertStmt):
                el = self._process_insert_nested_shape(
                    targetstep, shape_el.elements)
            elif isinstance(ctx.stmt, irast.UpdateStmt):
                el = self._process_update_nested_shape(
                    targetstep, shape_el.elements)
            else:
                el = self._process_shape(
                    targetstep,
                    shape_el.elements or [],
                    rptr=ptr_node,
                    _visited=_visited,
                    _recurse=True,
                    require_expressions=require_expressions,
                    include_implicit=include_implicit)
        else:
            el = targetstep

        if ((not ptr_singular or recurse is not None) and
                el is not None and shape_el.compexpr is None and
                not isinstance(ctx.stmt, irast.MutatingStmt)):
            substmt = irast.SelectStmt(
                result=el,
                where=where,
                orderby=orderby,
                offset=offset,
                limit=limit,
                path_scope=ctx.path_scope,
                specific_path_scope={
                    ctx.sets[p] for p in ctx.stmt_path_scope
                    if p in ctx.sets and p in ctx.path_scope
                }
            )

            if recurse is not None:
                substmt.recurse_ptr = ptr_node
                substmt.recurse_depth = recurse

            el = self._generated_set(substmt, path_id=el.path_id)
            el.rptr = ptr_node

        return el

    def _process_shape_compexpr(self, source_expr, shape_el, ptrname,
                                ptrsource, ptr_direction, is_linkprop,
                                rptr, source_ctx):
        ctx = self.context.current

        schema = ctx.schema

        if is_linkprop:
            ptr_metacls = s_lprops.LinkProperty
        else:
            ptr_metacls = s_links.Link

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

        with self.context.new() as shape_expr_ctx:
            # Put current pointer class in context, so
            # that references to link properties in sub-SELECT
            # can be resolved.  This is necessary for proper
            # evaluation of link properties on computable links,
            # most importantly, in INSERT/UPDATE context.
            shape_expr_ctx.toplevel_shape_rptr = irast.Pointer(
                source=source_expr,
                ptrcls=ptrcls,
                direction=ptr_direction
            )
            shape_expr_ctx.stmt_path_scope = collections.defaultdict(int)
            compexpr = self.visit(shape_el.compexpr)

        target_class = irutils.infer_type(compexpr, schema)
        if target_class is None:
            msg = 'cannot determine expression result type'
            raise errors.EdgeQLError(msg, context=source_ctx)

        if ptrcls is None:
            if (isinstance(ctx.stmt, irast.MutatingStmt) and
                    ctx.clause != 'result'):
                raise errors.EdgeQLError(
                    'reference to unknown pointer',
                    context=source_ctx)

            ptrcls = ptr_metacls(
                name=sn.SchemaName(
                    module=ptrname[0] or ptrsource.name.module,
                    name=ptrname[1]),
            ).derive(schema, ptrsource, target_class)

            if isinstance(shape_el.compexpr, qlast.Statement):
                if shape_el.compexpr.single:
                    ptrcls.mapping = s_links.LinkMapping.ManyToOne
                else:
                    ptrcls.mapping = s_links.LinkMapping.ManyToMany

        compexpr = self._ensure_stmt(compexpr, shape_expr_ctx)
        if compexpr.result.path_id not in compexpr.path_scope:
            compexpr.path_scope[compexpr.result.path_id] += 1

        if is_linkprop:
            path_id = rptr.source.path_id.extend(
                rptr.ptrcls, rptr.direction, source_expr.scls
            ).extend(
                ptrcls, ptr_direction, target_class
            )
        else:
            path_id = source_expr.path_id.extend(
                ptrcls, ptr_direction, target_class)

        targetstep = irast.Set(
            path_id=path_id,
            scls=target_class,
            expr=compexpr
        )

        ctx.singletons.add(targetstep)

        targetstep.rptr = irast.Pointer(
            source=source_expr,
            target=targetstep,
            ptrcls=ptrcls,
            direction=ptr_direction
        )

        if ptrcls.shortname == 'std::__class__':
            msg = 'cannot assign to __class__'
            raise errors.EdgeQLError(msg, context=source_ctx)

        if (isinstance(ctx.stmt, irast.MutatingStmt) and
                ctx.clause != 'result'):
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

        return targetstep

    def _process_insert_nested_shape(self, targetstep, elements):
        ctx = self.context.current

        mutation_shape = []
        for subel in elements or []:
            is_prop = (
                isinstance(subel.expr.steps[0], qlast.Ptr) and
                subel.expr.steps[0].type == 'property'
            )
            if not is_prop:
                mutation_shape.append(subel)

        ptr_node = targetstep.rptr

        el = self._process_shape(
            targetstep,
            mutation_shape,
            rptr=ptr_node,
            _recurse=True,
            require_expressions=True,
            include_implicit=False)

        returning_shape = []
        for subel in elements or []:
            is_prop = (
                isinstance(subel.expr.steps[0], qlast.Ptr) and
                subel.expr.steps[0].type == 'property'
            )
            if is_prop:
                returning_shape.append(subel)

        substmt = irast.InsertStmt(
            subject=el,
            result=self._process_shape(
                targetstep,
                returning_shape,
                rptr=ptr_node,
                include_implicit=True
            ),
            path_scope=ctx.path_scope,
            specific_path_scope={
                ctx.sets[p] for p in ctx.stmt_path_scope
                if p in ctx.sets and p in ctx.path_scope
            }
        )

        result = self._generated_set(substmt)
        result.rptr = ptr_node
        return result

    def _process_update_nested_shape(self, targetstep, elements):
        ctx = self.context.current

        for subel in elements or []:
            is_prop = (
                isinstance(subel.expr.steps[0], qlast.Ptr) and
                subel.expr.steps[0].type == 'property'
            )
            if not is_prop:
                raise errors.EdgeQLError(
                    'only references to link properties are allowed '
                    'in nested UPDATE shapes', context=subel.context)

        ptr_node = targetstep.rptr

        el = self._process_shape(
            targetstep,
            elements,
            rptr=ptr_node,
            _recurse=True,
            require_expressions=True,
            include_implicit=False)

        substmt = irast.SelectStmt(
            result=el,
            path_scope=ctx.path_scope,
            specific_path_scope={
                ctx.sets[p] for p in ctx.stmt_path_scope
                if p in ctx.sets and p in ctx.path_scope
            }
        )

        result = self._generated_set(substmt)
        result.rptr = ptr_node
        return result

    def _path_step(self, path_tip, source, ptr_name, direction, ptr_target,
                   source_context):
        ctx = self.context.current

        if isinstance(source, s_obj.Tuple):
            if ptr_name[0] is not None:
                el_name = '::'.join(ptr_name)
            else:
                el_name = ptr_name[1]

            if el_name in source.element_types:
                expr = irast.TupleIndirection(
                    expr=path_tip, name=el_name,
                    context=source_context)
            else:
                raise errors.EdgeQLReferenceError(
                    f'{el_name} is not a member of a struct')

            path_tip = self._generated_set(expr)

            return path_tip, None

        else:
            # Check if the tip of the path has an associated shape.
            # This would be the case for paths on views.
            ptrcls = None
            shape_el = None
            view_source = None
            view_set = None

            if irutils.is_view_set(path_tip):
                view_set = irutils.get_subquery_shape(path_tip)

            if view_set is None:
                view_set = path_tip

            # Search for the pointer in the shape associated with
            # the tip of the path, i.e. a view.
            for shape_el in view_set.shape:
                shape_ptrcls = shape_el.rptr.ptrcls
                shape_pn = shape_ptrcls.shortname

                if ((ptr_name[0] and ptr_name == shape_pn.as_tuple()) or
                        ptr_name[1] == shape_pn.name):
                    # Found a match!
                    ptrcls = shape_ptrcls
                    if shape_el.expr is not None:
                        view_source = shape_el
                    break

            if ptrcls is None:
                # Try to resolve a pointer using the schema.
                ptrcls = self._resolve_ptr(
                    source, ptr_name, direction, target=ptr_target)

            target = ptrcls.get_far_endpoint(direction)
            target_path_id = path_tip.path_id.extend(
                ptrcls, direction, target)

            if (view_source is None or shape_el.path_id != target_path_id or
                    path_tip.expr is not None):
                path_tip = irutils.get_canonical_set(path_tip)
                path_tip = self._extend_path(
                    path_tip, ptrcls, direction, target)

                path_tip.view_source = view_source
            else:
                path_tip = shape_el
                self._register_path_scope(path_tip.path_id)

            if (isinstance(target, s_concepts.Concept) and
                    target.is_virtual and
                    ptr_target is not None):
                try:
                    path_tip = ctx.sets[path_tip.path_id, ptr_target.name]
                except KeyError:
                    pf = irast.TypeFilter(
                        path_id=path_tip.path_id,
                        expr=path_tip,
                        type=irast.TypeRef(maintype=ptr_target.name)
                    )

                    new_path_tip = self._generated_set(pf)
                    new_path_tip.rptr = path_tip.rptr
                    path_tip = new_path_tip
                    ctx.sets[path_tip.path_id, ptr_target.name] = path_tip

            return path_tip, ptrcls

    def _extend_path(self, source_set, ptrcls,
                     direction=s_pointers.PointerDirection.Outbound,
                     target=None):
        """Return a Set node representing the new path tip."""
        ctx = self.context.current

        if target is None:
            target = ptrcls.get_far_endpoint(direction)

        path_id = source_set.path_id.extend(ptrcls, direction, target)

        if not source_set.expr or irutils.is_strictly_view_set(source_set):
            target_set = ctx.sets.get(path_id)
        else:
            target_set = None

        if target_set is None:
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

            self._register_path_scope(target_set.path_id)

        return target_set

    def _class_set(self, scls):
        path_id = irast.PathId([scls])
        ir_set = irast.Set(path_id=path_id, scls=scls)
        self._register_path_scope(ir_set.path_id)
        return ir_set

    def _generated_set(self, expr, path_id=None):
        ctx = self.context.current
        return irutils.new_expression_set(expr, ctx.schema, path_id)

    def _register_path_scope(self, path_id, *, ctx=None, stmt_scope=True):
        if ctx is None:
            ctx = self.context.current

        if not ctx.path_as_type:
            for prefix in path_id.iter_prefixes():
                if (ctx.in_aggregate or not ctx.aggregated_scope or
                        prefix in ctx.unaggregated_scope):
                    ctx.path_scope[prefix] += 1
                    if stmt_scope:
                        ctx.stmt_path_scope[prefix] += 1

    def _update_pending_path_scope(self, scope):
        ctx = self.context.current

        scope = set(scope)
        promoted_scope = ctx.pending_path_scope & scope
        new_pending_scope = scope - promoted_scope
        ctx.pending_path_scope -= promoted_scope
        ctx.pending_path_scope.update(new_pending_scope)

        for path_id in promoted_scope:
            self._register_path_scope(path_id, stmt_scope=False)

    def _resolve_ptr(self,
                     near_endpoint,
                     ptr_name,
                     direction,
                     target=None):
        ptr_module, ptr_nqname = ptr_name

        if ptr_module:
            ptr_fqname = sn.Name(module=ptr_module, name=ptr_nqname)
            modaliases = self.context.current.namespaces
            pointer = self.schema.get(ptr_fqname, module_aliases=modaliases)
            pointer_name = pointer.name
        else:
            pointer_name = ptr_fqname = ptr_nqname

        ptr = None

        if isinstance(near_endpoint, s_sources.Source):
            ptr = near_endpoint.resolve_pointer(
                self.schema,
                pointer_name,
                direction=direction,
                look_in_children=False,
                include_inherited=True,
                far_endpoint=target)
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

    def _resolve_schema_name(self, name, module):
        ctx = self.context.current
        schema_module = ctx.namespaces.get(module)
        if schema_module is None:
            return None
        else:
            return sn.Name(name=name, module=schema_module)

    def _get_schema_object(self, name, module=None):
        ctx = self.context.current

        if isinstance(name, qlast.ClassRef):
            module = name.module
            name = name.name

        if module:
            name = sn.Name(name=name, module=module)

        return ctx.schema.get(name=name, module_aliases=ctx.namespaces)

    def _process_stmt_result(self, result, toplevel_rptr, result_alias=None):
        with self.context.new() as ctx:
            ctx.clause = 'result'

            if isinstance(result, qlast.Shape):
                expr = self.visit(result.expr)
                shape = result.elements
            else:
                expr = self.visit(result)
                if (isinstance(expr, irast.Set) and
                        isinstance(expr.scls, s_concepts.Concept) and
                        (not self._is_subquery_set(expr) or
                            isinstance(expr.expr, irast.MutatingStmt)) and
                        not self._is_set_op_set(expr)):
                    shape = []
                else:
                    shape = None

            self._update_singletons(expr)

            if shape is not None:
                if expr.rptr is not None:
                    rptr = expr.rptr
                else:
                    rptr = toplevel_rptr

                expr = self._process_shape(expr, shape, rptr=rptr)

        expr = self._ensure_set(expr)
        self._declare_aliased_set(expr, result_alias)
        return expr

    def _process_select_where(self, where):
        with self.context.new():
            self.context.current.clause = 'where'

            if where is not None:
                return self.visit(where)
            else:
                return None

    def _process_orderby(self, sortexprs):
        result = []
        if not sortexprs:
            return result

        with self.context.new() as ctx:
            ctx.clause = 'orderby'

            for sortexpr in sortexprs:
                ir_sortexpr = self.visit(sortexpr.path)
                ir_sortexpr.context = sortexpr.context
                self._enforce_singleton(ir_sortexpr)
                result.append(
                    irast.SortExpr(
                        expr=ir_sortexpr,
                        direction=sortexpr.direction,
                        nones_order=sortexpr.nones_order))

        return result

    def _process_groupby(self, groupexprs):
        result = []
        if not groupexprs:
            return result

        with self.context.new() as ctx:
            ctx.clause = 'groupby'

            ir_groupexprs = []
            for groupexpr in groupexprs:
                ir_groupexpr = self.visit(groupexpr)
                ir_groupexpr.context = groupexpr.context
                ir_groupexprs.append(ir_groupexpr)

            self._update_singletons(ir_groupexprs)

        return ir_groupexprs

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
        if isinstance(expr.expr, irast.Tuple):
            elems = []

            for elem in expr.expr.elements:
                ref_elem = self._process_type_ref_elem(elem.val, elem.context)

                elems.append(ref_elem)

            expr = irast.Array(elements=elems)

        else:
            expr = self._process_type_ref_elem(expr, expr.context)

        return expr

    def _enforce_singleton(self, expr):
        ctx = self.context.current
        cardinality = irinference.infer_cardinality(
            expr, ctx.singletons, ctx.schema)
        if cardinality != 1:
            raise errors.EdgeQLError(
                'possibly more than one element returned by an expression '
                'where only singletons are allowed',
                context=expr.context)

    def _update_singletons(self, expr):
        ctx = self.context.current
        for prefix, ir_sets in extract_prefixes(expr).items():
            for ir_set in ir_sets:
                ir_set = irutils.get_canonical_set(ir_set)
                ctx.singletons.add(ir_set)
                if self._is_type_filter(ir_set):
                    ctx.singletons.add(ir_set.expr.expr)

    def _is_set_expr(self, expr):
        if isinstance(expr, irast.Set):
            return expr
        elif (isinstance(expr, irast.ExistPred) and
                isinstance(expr.expr, irast.Set)):
            return expr.expr
        else:
            return None

    def _is_type_filter(self, ir_expr):
        return (
            isinstance(ir_expr, irast.Set) and
            isinstance(ir_expr.expr, irast.TypeFilter)
        )

    def _is_constant(self, expr):
        flt = lambda node: isinstance(node, irast.Path)
        paths = ast.visitor.find_children(expr, flt)
        return not paths and not isinstance(expr, irast.Path)

    def _is_func_agg(self, name):
        if isinstance(name, str):
            name = (None, name)

        return self._get_schema_object(
            name=name[1], module=name[0]).aggregate

    def _is_subquery_set(self, ir_expr):
        return (
            isinstance(ir_expr, irast.Set) and
            isinstance(ir_expr.expr, irast.Stmt)
        )

    def _is_strict_subquery_set(self, ir_expr):
        return (
            self._is_subquery_set(ir_expr) and
            not irutils.is_strictly_view_set(ir_expr)
        )

    def _is_exists_expr_set(self, ir_expr):
        return (
            isinstance(ir_expr, irast.Set) and
            isinstance(ir_expr.expr, irast.ExistPred)
        )

    def _is_set_op_set(self, ir_expr):
        return (
            isinstance(ir_expr, irast.Set) and
            isinstance(ir_expr.expr, irast.SetOp)
        )

    def _extend_binop(self, binop, *exprs, op=ast.ops.AND):
        exprs = list(exprs)
        binop = binop or exprs.pop(0)

        for expr in exprs:
            if expr is not binop:
                binop = irast.BinOp(
                    left=binop,
                    right=expr,
                    op=op
                )

        return binop

    def _extend_qlbinop(self, binop, *exprs, op=ast.ops.AND):
        exprs = list(exprs)
        binop = binop or exprs.pop(0)

        for expr in exprs:
            if expr is not binop:
                binop = qlast.BinOp(
                    left=binop,
                    right=expr,
                    op=op
                )

        return binop

    def _ensure_set(self, expr):
        if not isinstance(expr, irast.Set):
            expr = self._generated_set(expr)
        return expr

    def _ensure_stmt(self, expr, ctx=None):
        if ctx is None:
            ctx = self.context.current
        if not isinstance(expr, irast.Stmt):
            expr = irast.SelectStmt(
                result=self._ensure_set(expr),
                path_scope=ctx.path_scope
            )
            expr.specific_path_scope = {
                ctx.sets[p] for p in ctx.stmt_path_scope
                if p in ctx.sets
            }
        return expr

    def _ensure_qlstmt(self, expr):
        if not isinstance(expr, qlast.Statement):
            expr = qlast.SelectQuery(
                result=expr,
            )
        return expr

    def _type_to_ql_typeref(self, t):
        if not isinstance(t, s_obj.Collection):
            result = qlast.TypeName(
                maintype=qlast.ClassRef(
                    module=t.name.module,
                    name=t.name.name
                )
            )
        else:
            result = qlast.TypeName(
                maintype=qlast.ClassRef(
                    name=t.schema_name
                ),
                subtypes=[
                    self._type_to_ql_typeref(st) for st in t.get_subtypes()
                ]
            )

        return result

    def _ql_typeref_to_ir_typeref(self, ql_t):
        maintype = ql_t.maintype
        subtypes = ql_t.subtypes

        if subtypes:
            typ = irast.TypeRef(
                maintype=maintype.name,
                subtypes=[]
            )

            for subtype in subtypes:
                subtype = self._ql_typeref_to_ir_typeref(subtype)
                typ.subtypes.append(subtype)
        else:
            typ = irast.TypeRef(
                maintype=self._get_schema_object(
                    maintype.name, maintype.module).name,
                subtypes=[]
            )

        return typ
