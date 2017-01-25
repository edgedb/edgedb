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


def extract_prefixes(expr, roots_only=False):
    extractor = PathExtractor(roots_only=roots_only)
    extractor.visit(expr)
    return extractor.paths


def get_prefix_trie(prefixes):
    trie = {}

    for path_id in prefixes:
        branch = trie
        for path_prefix in path_id.iter_prefixes():
            branch = branch.setdefault(tuple(path_prefix), {})

    return trie


def get_common_prefixes(exprs):
    """Get a set of longest common path prefixes for given expressions."""

    prefix_counts = {}
    prefixes = {}

    for expr in exprs:
        for prefix, ir_set in extract_prefixes(expr).items():
            try:
                prefix_counts[tuple(prefix)] += 1
            except KeyError:
                prefix_counts[tuple(prefix)] = 1
                prefixes[prefix] = ir_set

    trie = get_prefix_trie(prefixes)

    trails = []

    for root, subtrie in trie.items():
        path_id = root
        current = subtrie

        root_count = prefix_counts.get(path_id, 0)

        while current:
            if len(current) == 1:
                next_path_id, current = next(iter(current.items()))

                if prefix_counts[next_path_id] >= root_count:
                    path_id = next_path_id
                else:
                    break
            else:
                break

        trails.append(irast.PathId(path_id))

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

    def visit_SelectQuery(self, edgeql_tree):
        toplevel_shape_rptrcls = self.context.current.toplevel_shape_rptrcls
        is_toplevel = self.context.current.stmt is None
        schema = self.context.current.schema

        with self.context.subquery():
            ctx = self.context.current

            stmt = ctx.stmt = irast.SelectStmt()
            self._visit_with_block(edgeql_tree)

            if edgeql_tree.op:  # UNION/INTERSECT/EXCEPT
                stmt.set_op = qlast.SetOperator(edgeql_tree.op)
                stmt.set_op_larg = self.visit(edgeql_tree.op_larg).expr
                stmt.set_op_rarg = self.visit(edgeql_tree.op_rarg).expr
            else:
                if (isinstance(edgeql_tree.result, qlast.Path) and
                        edgeql_tree.result.steps and
                        edgeql_tree.result.pathspec):
                    ctx.result_path_steps = edgeql_tree.result.steps

                stmt.where = self._process_select_where(edgeql_tree.where)

                stmt.groupby = self._process_groupby(edgeql_tree.groupby)
                if stmt.groupby is not None:
                    ctx.group_paths = set(extract_prefixes(stmt.groupby))

                stmt.result = self._process_stmt_result(
                    edgeql_tree.result, toplevel_shape_rptrcls)

            stmt.orderby = self._process_orderby(edgeql_tree.orderby)
            if edgeql_tree.offset:
                stmt.offset = self.visit(edgeql_tree.offset)
            if edgeql_tree.limit:
                stmt.limit = self.visit(edgeql_tree.limit)

            if stmt.groupby is None:
                # Check if query() or order() contain any aggregate
                # expressions and if so, add a sentinel group prefix
                # instructing the transformer that we are implicitly
                # grouping the whole set.
                def checker(n):
                    if isinstance(n, irast.FunctionCall):
                        return n.aggregate
                    elif isinstance(n, irast.Stmt):
                        # Make sure we don't dip into subqueries
                        raise ast.Skip()

                for node in itertools.chain(stmt.orderby or [],
                                            [stmt.offset],
                                            [stmt.limit],
                                            [stmt.result]):
                    if node is None:
                        continue
                    if ast.find_children(node, checker, force_traversal=True):
                        ctx.group_paths = {...}
                        break

            if is_toplevel:
                stmt.argument_types = self.context.current.arguments
                result = stmt
            else:
                restype = irutils.infer_type(stmt, schema)
                result = self._generated_set(stmt, restype, force=True)
                if isinstance(stmt.result, irast.Set):
                    result.path_id = stmt.result.path_id

        return result

    def visit_InsertQuery(self, edgeql_tree):
        toplevel_shape_rptrcls = self.context.current.toplevel_shape_rptrcls

        with self.context.subquery():
            ctx = self.context.current

            stmt = ctx.stmt = irast.InsertStmt()
            self._visit_with_block(edgeql_tree)

            with self.context.new():
                self.context.current.location = 'selector'
                subject = self.visit(edgeql_tree.subject)

                if edgeql_tree.result is not None:
                    stmt.result = self._process_stmt_result(
                        edgeql_tree.result, toplevel_shape_rptrcls)
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
                    default_expr = qlast.Constant(value=ptrcls.default)

                substmt = self.visit(default_expr)
                if not isinstance(substmt, irast.Stmt):
                    substmt = irast.SelectStmt(result=substmt)

                rt = irutils.infer_type(substmt, ctx.schema)
                el = irast.Set(
                    path_id=irast.PathId([rt]),
                    scls=rt,
                    expr=substmt,
                    rptr=targetstep.rptr
                )

                stmt.shape.elements.append(el)

            stmt.argument_types = self.context.current.arguments
            return stmt

    def visit_UpdateQuery(self, edgeql_tree):
        toplevel_shape_rptrcls = self.context.current.toplevel_shape_rptrcls

        with self.context.subquery():
            ctx = self.context.current

            stmt = ctx.stmt = irast.UpdateStmt()
            self._visit_with_block(edgeql_tree)

            with self.context.new():
                self.context.current.location = 'selector'
                subject = self.visit(edgeql_tree.subject)

            stmt.where = self._process_select_where(edgeql_tree.where)
            if edgeql_tree.result is not None:
                stmt.result = self._process_stmt_result(
                    edgeql_tree.result, toplevel_shape_rptrcls)
            else:
                stmt.result = self._process_shape(subject, None, [])

            stmt.shape = self._process_shape(
                subject, None, edgeql_tree.pathspec,
                require_expressions=True,
                include_implicit=False)

            stmt.argument_types = self.context.current.arguments
            return stmt

    def visit_DeleteQuery(self, edgeql_tree):
        toplevel_shape_rptrcls = self.context.current.toplevel_shape_rptrcls

        with self.context.subquery():
            ctx = self.context.current

            stmt = ctx.stmt = irast.DeleteStmt()
            self._visit_with_block(edgeql_tree)

            with self.context.new():
                self.context.current.location = 'selector'
                subject = self.visit(edgeql_tree.subject)

            stmt.where = self._process_select_where(edgeql_tree.where)
            if edgeql_tree.result is not None:
                stmt.result = self._process_stmt_result(
                    edgeql_tree.result, toplevel_shape_rptrcls)
            else:
                stmt.result = self._process_shape(subject, None, [])

            stmt.shape = self._process_shape(
                subject, None, [],
                require_expressions=True,
                include_implicit=False)

            stmt.argument_types = self.context.current.arguments
            return stmt

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

                if refnode is None and not step.module:
                    # Finally, check if the starting path label is
                    # a query defined in a WITH clause.
                    refnode = ctx.substmts.get(step.name)

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
                    path_tip = ctx.sets[path_id] = irast.Set()
                    path_tip.scls = scls
                    path_tip.path_id = path_id

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

                ptr_name = (ptr_expr.ptr.module, ptr_expr.ptr.name)

                if ptr_expr.type == 'property':
                    # Link property reference; the source is the
                    # link immediately preceding this step in the path.
                    source = path_tip.rptr.ptrcls
                else:
                    source = path_tip.scls

                path_tip, _ = self._path_step(
                    path_tip, source, ptr_name, direction, ptr_target)

            else:
                # Arbitrary expression
                if i > 0:
                    raise RuntimeError(
                        'unexpected expression as a non-first path item')

                expr = self.visit(step)
                result_type = irutils.infer_type(expr, ctx.schema)
                path_tip = self._generated_set(expr, result_type, force=True)

        if (ctx.group_paths and ctx.location in ('orderby', 'selector') and
                not ctx.in_aggregate and
                path_tip.path_id not in ctx.group_paths):
            raise errors.EdgeQLError(
                f'{path_tip.path_id!r} must appear in the '
                'GROUP BY expression or used in an aggregate function.')

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
        right = binop.right
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
        ctx = self.context.current

        left, right = self.visit((expr.left, expr.right))

        if isinstance(expr.op, ast.ops.TypeCheckOperator):
            right = self._process_type_ref_expr(right)

        binop = irast.BinOp(left=left, right=right, op=expr.op)
        folded = self._try_fold_binop(binop)
        if folded is not None:
            return folded

        result_type = irutils.infer_type(binop, ctx.schema)
        prefixes = get_common_prefixes([left, right])
        sources = set(itertools.chain.from_iterable(prefixes.values()))

        if sources:
            node = irast.Set(
                path_id=irast.PathId([]),
                scls=result_type,
                expr=binop,
                sources=sources
            )
        else:
            node = binop

        return node

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

    def visit_StructElement(self, expr):
        name = expr.name.name
        if expr.name.module:
            name = f'{expr.name.module}::{name}'

        val = self.visit(expr.val)

        if isinstance(val, irast.Set) and isinstance(val.expr, irast.Stmt):
            val = val.expr
        elif not isinstance(val, irast.Stmt):
            val = irast.SelectStmt(result=val)

        element = irast.StructElement(
            name=name,
            val=val
        )

        return element

    def visit_Struct(self, expr):
        elements = self.visit(expr.elements)
        return irast.Struct(elements=elements)

    def visit_Tuple(self, expr):
        elements = self.visit(expr.elements)
        return irast.Sequence(elements=elements)

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
            if funcs[0].aggregate:
                ctx.in_aggregate = True

            args = []
            kwargs = {}
            arg_types = []
            for ai, a in enumerate(expr.args):
                if isinstance(a, qlast.NamedArg):
                    kwargs[a.name] = arg = self.visit(a.arg)
                    aname = a.name
                else:
                    arg = self.visit(a)
                    args.append(arg)
                    aname = ai

                arg_type = irutils.infer_type(arg, ctx.schema)
                if arg_type is None:
                    raise errors.EdgeQLError(
                        f'could not resolve the type of argument '
                        f'${aname} of function {funcname}',
                        context=a.context)
                arg_types.append(arg_type)

            for funcobj in funcs:
                if self._check_function(funcobj, arg_types):
                    break
            else:
                raise errors.EdgeQLError(
                    f'could not find a function variant {funcname}',
                    context=expr.context)

            node = irast.FunctionCall(
                func=funcobj, args=args, kwargs=kwargs)

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

        return node

    def visit_IfElse(self, expr):
        return irast.IfElseExpr(
            condition=self.visit(expr.condition),
            if_expr=self.visit(expr.if_expr),
            else_expr=self.visit(expr.else_expr))

    def visit_UnaryOp(self, expr):
        ctx = self.context.current

        operand = self.visit(expr.operand)

        if isinstance(operand, irast.ExistPred) and expr.op == ast.ops.NOT:
            operand.negated = not operand.negated
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

        prefixes = get_common_prefixes([operand])
        sources = set(itertools.chain.from_iterable(prefixes.values()))

        if sources:
            node = irast.Set(
                path_id=irast.PathId([]),
                scls=result_type,
                expr=unop,
                sources=sources
            )
        else:
            node = unop

        return node

    def visit_ExistsPredicate(self, expr):
        operand = self.visit(expr.expr)
        if self._is_subquery_set(operand):
            operand = operand.expr
        return irast.ExistPred(expr=operand)

    def visit_Coalesce(self, expr):
        if all(isinstance(a, qlast.EmptySet) for a in expr.args):
            return irast.EmptySet()

        return irast.Coalesce(args=self.visit(expr.args))

    def visit_TypeCast(self, expr):
        maintype = expr.type.maintype
        subtypes = expr.type.subtypes

        if subtypes:
            typ = irast.TypeRef(
                maintype=maintype.name,
                subtypes=[]
            )

            for subtype in subtypes:
                if isinstance(subtype, qlast.Path):
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
        path_id = getattr(arg, 'path_id', None)
        if path_id is None:
            t = irutils.infer_type(arg, self.context.current.schema)
            path_id = irast.PathId([t])

        return irast.TypeFilter(
            path_id=path_id,
            expr=arg,
            type=irast.TypeRef(
                maintype=self._get_schema_object(expr.type.maintype).name
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
                step = irast.Set()
                step.scls = scls
                step.path_id = irast.PathId([step.scls])
                step.anchor = anchor
                step.show_as_anchor = anchor

            elif isinstance(scls, s_links.Link):
                if scls.source:
                    path = irast.Set()
                    path.scls = scls.source
                    path.path_id = irast.PathId([path.scls])
                    path = self._extend_path(
                        path, scls,
                        s_pointers.PointerDirection.Outbound,
                        scls.target)
                else:
                    path = irast.Set()
                    path.scls = ctx.schema.get('std::Object')
                    path.path_id = irast.PathId([path.scls])
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
                    path.path_id = irast.PathId([path.scls])
                    path = self._extend_path(
                        path, scls.source,
                        s_pointers.PointerDirection.Outbound,
                        scls.source.target)
                else:
                    path = irast.Set()
                    path.scls = ctx.schema.get('std::Object')
                    path.path_id = irast.PathId([path.scls])
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
            if isinstance(with_entry, qlast.NamespaceAliasDecl):
                ctx.namespaces[with_entry.alias] = with_entry.namespace

            elif isinstance(with_entry, qlast.CGE):
                substmt = self.visit(with_entry.expr).expr

                path_id = irast.PathId([
                    s_concepts.Concept(
                        name=sn.Name(
                            module='__cexpr__',
                            name=with_entry.alias
                        )
                    )
                ])

                result_type = irutils.infer_type(substmt, ctx.schema)

                if isinstance(substmt.result, irast.Set):
                    real_path_id = substmt.result.path_id
                elif isinstance(substmt.result, irast.Shape):
                    real_path_id = substmt.result.set.path_id
                else:
                    real_path_id = irast.PathId([result_type])

                substmt_set = irast.Set(
                    path_id=path_id,
                    real_path_id=real_path_id,
                    scls=result_type,
                    expr=substmt
                )

                ctx.sets[substmt_set.path_id] = substmt_set
                ctx.substmts[with_entry.alias] = substmt_set
                stmt.substmts.append(substmt_set)

            else:
                expr = self.visit(with_entry.expr)
                ctx.pathvars[with_entry.alias] = expr

    def _process_unlimited_recursion(self):
        type = s_types.normalize_type((0).__class__, self.schema)
        return irast.Constant(value=0, index=None, type=type)

    def _process_shape(self, source_expr, rptrcls, shapespec, *,
                       require_expressions=False, include_implicit=True,
                       _visited=None, _recurse=True):
        """Build a Shape node given shape spec."""
        if _visited is None:
            _visited = {}
        else:
            _visited = _visited.copy()

        scls = source_expr.scls

        elements = []

        shape = irast.Shape(elements=elements, scls=scls,
                            set=source_expr, rptr=source_expr.rptr)

        if isinstance(scls, s_concepts.Concept):
            _visited[scls] = shape

            if include_implicit:
                implicit_ptrs = (sn.Name('std::id'),)

                implicit_shape_els = []

                for pn in implicit_ptrs:
                    shape_el = qlast.SelectPathSpec(
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
            _visited[scls] = shape

            if include_implicit:
                implicit_ptrs = (sn.Name('std::target'),)

                implicit_shape_els = []

                for pn in implicit_ptrs:
                    shape_el = qlast.SelectPathSpec(
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
            el = self._process_shape_el(
                source_expr, rptrcls, shape_el, scls,
                require_expressions=require_expressions,
                include_implicit=include_implicit,
                _visited=_visited,
                _recurse=_recurse)

            # Record element may be none if ptrcls target is non-atomic
            # and recursion has been prohibited on this level to prevent
            # infinite looping.
            if el is not None:
                elements.append(el)

        return shape

    def _process_shape_el(self, source_expr, rptrcls, shape_el, scls, *,
                          require_expressions=False, include_implicit=True,
                          _visited=None, _recurse=True):

        with self.context.new():
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

            if lexpr.type == 'property':
                if rptrcls is None:
                    raise errors.EdgeQLError(
                        'invalid reference to link property '
                        'in top level shape')

                ptrsource = rptrcls
                ptr_metacls = s_lprops.LinkProperty
            else:
                ptr_metacls = s_links.Link

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

                with self.context.new():
                    # Put current pointer class in context, so
                    # that references to link properties in sub-SELECT
                    # can be resolved.  This is necessary for proper
                    # evaluation of link properties on computable links,
                    # most importantly, in INSERT/UPDATE context.
                    self.context.current.toplevel_shape_rptrcls = ptrcls
                    compexpr = self.visit(shape_el.compexpr)
                    if (isinstance(compexpr, irast.Set) and
                            isinstance(compexpr.expr, irast.Stmt)):
                        compexpr = compexpr.expr
                    elif not isinstance(compexpr, irast.Stmt):
                        compexpr = irast.SelectStmt(result=compexpr)

                target_class = irutils.infer_type(compexpr, schema)
                if target_class is None:
                    msg = 'cannot determine expression result type'
                    raise errors.EdgeQLError(msg, context=lexpr.context)

                if isinstance(compexpr.result, irast.Set):
                    path_id = compexpr.result.path_id
                else:
                    path_id = irast.PathId([target_class])

                targetstep = irast.Set(
                    path_id=path_id,
                    scls=target_class,
                    expr=compexpr
                )

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

                    if isinstance(shape_el.compexpr, qlast.Statement):
                        if shape_el.compexpr.single:
                            ptrcls.mapping = s_links.LinkMapping.ManyToOne
                        else:
                            ptrcls.mapping = s_links.LinkMapping.ManyToMany

                targetstep.rptr = irast.Pointer(
                    source=source_expr,
                    target=targetstep,
                    ptrcls=ptrcls,
                    direction=ptr_direction
                )

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
                if lexpr.target is not None:
                    ptr_target = self._get_schema_object(
                        module=lexpr.target.module, name=lexpr.target.name)
                else:
                    ptr_target = None

                targetstep, ptrcls = self._path_step(
                    source_expr, ptrsource, ptrname, ptr_direction, ptr_target)

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

            if _recurse and shape_el.pathspec:
                if (isinstance(ctx.stmt, irast.MutatingStmt) and
                        ctx.location != 'selector'):

                    mutation_pathspec = []
                    for subel in shape_el.pathspec or []:
                        is_prop = (
                            isinstance(subel.expr.steps[0], qlast.Ptr) and
                            subel.expr.steps[0].type == 'property'
                        )
                        if not is_prop:
                            mutation_pathspec.append(subel)

                    el = self._process_shape(
                        targetstep,
                        ptrcls,
                        mutation_pathspec,
                        _visited=_visited,
                        _recurse=True,
                        require_expressions=require_expressions,
                        include_implicit=include_implicit)

                    returning_pathspec = []
                    for subel in shape_el.pathspec or []:
                        is_prop = (
                            isinstance(subel.expr.steps[0], qlast.Ptr) and
                            subel.expr.steps[0].type == 'property'
                        )
                        if is_prop:
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

                    result = irutils.infer_type(substmt, ctx.schema)

                    # return early
                    return irast.Set(
                        path_id=irast.PathId([result]),
                        scls=result,
                        expr=substmt,
                        rptr=ptr_node
                    )

                else:
                    el = self._process_shape(
                        targetstep,
                        ptrcls,
                        shape_el.pathspec or [],
                        _visited=_visited,
                        _recurse=True,
                        require_expressions=require_expressions,
                        include_implicit=include_implicit)
            else:
                el = targetstep

            if ((not ptr_singular or recurse is not None) and
                    el is not None and shape_el.compexpr is None):
                substmt = irast.SelectStmt(
                    result=el,
                    where=where,
                    orderby=orderby,
                    offset=offset,
                    limit=limit
                )

                if recurse is not None:
                    substmt.recurse_ptr = ptr_node
                    substmt.recurse_depth = recurse

                result = irutils.infer_type(substmt, ctx.schema)
                el = irast.Set(
                    path_id=irast.PathId([result]),
                    scls=result,
                    expr=substmt,
                    rptr=ptr_node
                )

        return el

    def _path_step(self, path_tip, source, ptr_name, direction, ptr_target):
        ctx = self.context.current

        if isinstance(source, s_obj.Struct):
            if ptr_name[0] is not None:
                el_name = '::'.join(ptr_name)
            else:
                el_name = ptr_name[1]

            if el_name in source.element_types:
                expr = irast.StructIndirection(
                    expr=path_tip, name=el_name)
            else:
                raise errors.EdgeQLReferenceError(
                    f'{el_name} is not a member of a struct')

            field_type = irutils.infer_type(expr, ctx.schema)
            path_tip = self._generated_set(expr, field_type)

            return path_tip, None

        else:
            ptrcls = self._resolve_ptr(
                source, ptr_name, direction, target=ptr_target)

            target = ptrcls.get_far_endpoint(direction)

            path_tip = self._extend_path(
                path_tip, ptrcls, direction, target)

            if target.is_virtual and ptr_target is not None:
                pf = irast.TypeFilter(
                    path_id=path_tip.path_id,
                    expr=path_tip,
                    type=irast.TypeRef(maintype=ptr_target.name)
                )

                new_path_tip = self._generated_set(pf, ptr_target)
                new_path_tip.rptr = path_tip.rptr
                path_tip = new_path_tip

            return path_tip, ptrcls

    def _extend_path(self, source_set, ptrcls,
                     direction=s_pointers.PointerDirection.Outbound,
                     target=None):
        """Return a Set node representing the new path tip."""
        ctx = self.context.current

        if target is None:
            target = ptrcls.get_far_endpoint(direction)

        path_id = source_set.path_id.extend(ptrcls, direction, target)

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

    def _generated_set(self, expr, result_type, *, force=False):
        prefixes = get_common_prefixes([expr])
        sources = set(itertools.chain.from_iterable(prefixes.values()))

        if sources or force:
            if getattr(expr, 'path_id', None):
                path_id = expr.path_id
            else:
                path_id = irast.PathId([result_type])

            node = irast.Set(
                path_id=path_id,
                scls=result_type,
                expr=expr,
                sources=sources
            )
        else:
            node = expr

        return node

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

    def _get_schema_object(self, name, module=None):
        ctx = self.context.current

        if isinstance(name, qlast.ClassRef):
            module = name.module
            name = name.name

        if module:
            name = sn.Name(name=name, module=module)

        return ctx.schema.get(name=name, module_aliases=ctx.namespaces)

    def _process_stmt_result(self, result, toplevel_rptrcls):
        with self.context.new():
            self.context.current.location = 'selector'

            expr = self.visit(result)

            if (isinstance(expr, irast.Set) and
                    isinstance(expr.scls, s_concepts.Concept)):
                if expr.rptr is not None:
                    rptrcls = expr.rptr.ptrcls
                else:
                    rptrcls = toplevel_rptrcls

                expr = self._process_shape(
                    expr, rptrcls, result.pathspec or [])

        return expr

    def _process_select_where(self, where):
        with self.context.new():
            self.context.current.location = 'generator'

            if where is not None:
                return self.visit(where)
            else:
                return None

    def _process_orderby(self, sortexprs):

        result = []

        if not sortexprs:
            return result

        with self.context.new():
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
            with self.context.new():
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

            expr = irast.Array(elements=elems)

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
            isinstance(ir_expr.expr, irast.Stmt) and
            ir_expr.path_id[0].name.module != '__cexpr__'
        )
