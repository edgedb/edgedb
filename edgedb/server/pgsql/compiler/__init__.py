##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import itertools

from edgedb.lang.common import exceptions as edgedb_error

from edgedb.lang.ir import ast2 as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import utils as s_utils

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import codegen as pgcodegen
from edgedb.server.pgsql import common
from edgedb.server.pgsql import types as pg_types
from edgedb.server.pgsql import exceptions as pg_errors

from edgedb.lang.common import ast, markup
from edgedb.lang.common.debug import debug


from .context import TransformerContext


class IRCompilerError(pg_errors.BackendError):
    pass


class IRCompilerInternalError(IRCompilerError):
    pass


class IRCompilerErrorContext(markup.MarkupExceptionContext):
    title = 'EdgeDB PgSQL IR Compiler Error Context'

    def __init__(self, tree):
        super().__init__()
        self.tree = tree

    @classmethod
    def as_markup(cls, self, *, ctx):
        tree = markup.serialize(self.tree, ctx=ctx)
        return markup.elements.lang.ExceptionContext(
            title=self.title, body=[tree])


class IRCompiler(ast.visitor.NodeVisitor):
    @debug
    def transform(self, query, backend, schema, output_format=None):
        try:
            # Transform to sql tree
            self.context = TransformerContext()
            ctx = self.context.current
            ctx.backend = backend
            ctx.schema = schema
            ctx.output_format = output_format
            qtree = self.visit(query)
            argmap = ctx.argmap
            """LOG [edgeql.compile] SQL Tree
            from edgedb.lang.common import markup
            markup.dump(qtree)
            """

            # Generate query text
            codegen = self._run_codegen(qtree)
            qchunks = codegen.result
            arg_index = codegen.param_index
            """LOG [edgeql.compile]
            from edgedb.lang.common import markup
            qtext = ''.join(qchunks)
            markup.dump_code(qtext, lexer='sql', header='SQL Query')
            """

        except Exception as e:
            try:
                args = [e.args[0]]
            except (AttributeError, IndexError):
                args = []
            err = IRCompilerInternalError(*args)
            err_ctx = IRCompilerErrorContext(tree=query)
            edgedb_error.replace_context(err, err_ctx)
            raise err from e

        return qchunks, argmap, arg_index, type(qtree), tuple(
            ctx.record_info.values())

    def generic_visit(self, node, *, combine_results=None):
        raise NotImplementedError(
            'no IR compiler handler for {}'.format(node.__class__))

    def visit_Shape(self, expr):
        ctx = self.context.current

        my_elements = []
        attribute_map = []
        virtuals_map = {}
        testref = None

        for i, e in enumerate(expr.elements):
            element = self.visit(e)

            ptr_name = e.rptr.ptrcls.normal_name()
            ptr_direction = e.rptr.direction or \
                s_pointers.PointerDirection.Outbound
            if ptr_direction == s_pointers.PointerDirection.Outbound:
                ptr_target = e.rptr.ptrcls.target
            else:
                ptr_target = e.rptr.ptrcls.source

            if isinstance(element, pgast.SelectQueryNode):
                if not e.rptr.ptrcls.singular(ptr_direction):
                    # Aggregate subquery results to keep correct
                    # cardinality.
                    element.targets[0] = pgast.FunctionCallNode(
                        name='array_agg',
                        args=[element.targets[0]],
                        agg_sort=element.orderby
                    )
                    element.orderby = []

            if ptr_name == 'std::id':
                testref = element

            attr_name = s_pointers.PointerVector(
                name=ptr_name.name, module=ptr_name.module,
                direction=ptr_direction, target=ptr_target.name,
                is_linkprop=isinstance(e.rptr, s_lprops.LinkProperty))

            attribute_map.append(attr_name)
            my_elements.append(element)

        if ctx.output_format == 'json':
            keyvals = []
            for i, pgexpr in enumerate(my_elements):
                key = attribute_map[i]
                if isinstance(key, s_pointers.PointerVector):
                    if key.is_linkprop:
                        key = '@' + key.name
                    else:
                        key = key.name
                keyvals.append(pgast.ConstantNode(value=key))
                keyvals.append(pgexpr)

            result = pgast.FunctionCallNode(
                name='jsonb_build_object', args=keyvals)

        elif ctx.entityref_as_id:
            for i, a in enumerate(attribute_map):
                if (a.module, a.name) == ('std', 'id'):
                    result = my_elements[i]
                    break
            else:
                raise ValueError('cannot find id ptr in entitityref record')

        else:
            metaclass = expr.scls.get_canonical_class()
            metaclass_name = '{}.{}'.format(
                metaclass.__module__, metaclass.__name__)
            marker = common.RecordInfo(
                attribute_map=attribute_map, virtuals_map=virtuals_map,
                metaclass=metaclass_name, classname=expr.scls.name)

            ctx.record_info[marker.path_id] = marker
            ctx.backend._register_record_info(marker)

            marker = pgast.ConstantNode(value=marker.path_id)
            marker_type = pgast.TypeNode(
                name='edgedb.known_record_marker_t')
            marker = pgast.TypeCastNode(expr=marker, type=marker_type)

            my_elements.insert(0, marker)

            result = pgast.RowExprNode(args=my_elements)

        if testref is not None and ctx.filter_null_records:
            when_cond = pgast.NullTestNode(expr=testref)

            when_expr = pgast.CaseWhenNode(
                expr=when_cond, result=pgast.ConstantNode(value=None))
            result = pgast.CaseExprNode(
                args=[when_expr], default=result,
                filter_expr=pgast.UnaryOpNode(
                    operand=when_cond, op=ast.ops.NOT))

        return result

    def visit_FunctionCall(self, expr):
        ctx = self.context.current

        result = None
        agg_filter = None
        agg_sort = []

        if expr.aggregate:
            with self.context(self.context.NEW_TRANSPARENT):
                ctx.in_aggregate = True
                ctx.query.aggregates = True
                args = [
                    self.visit(a) for a in expr.args
                ]
                if expr.agg_filter:
                    agg_filter = self.visit(expr.agg_filter)
        else:
            args = [self.visit(a) for a in expr.args]

        if expr.agg_sort:
            for sortexpr in expr.agg_sort:
                _sortexpr = self.visit(sortexpr.expr)
                agg_sort.append(
                    pgast.SortExprNode(
                        expr=_sortexpr, direction=sortexpr.direction,
                        nulls_order=sortexpr.nones_order))

        partition = []
        if expr.partition:
            for partition_expr in expr.partition:
                _pexpr = self.visit(partition_expr)
                partition.append(_pexpr)

        funcname = expr.name
        if funcname[0] == 'std':
            funcname = funcname[1]

        if funcname == 'if':
            cond = self.visit(expr.args[0])
            pos = self.visit(expr.args[1])
            neg = self.visit(expr.args[2])
            when_expr = pgast.CaseWhenNode(expr=cond, result=pos)
            result = pgast.CaseExprNode(args=[when_expr], default=neg)

        elif funcname == 'join':
            name = 'string_agg'
            separator, ref = args[:2]
            try:
                ignore_nulls = args[2] and args[2].value
            except IndexError:
                ignore_nulls = False

            if not ignore_nulls:
                array_agg = pgast.FunctionCallNode(
                    name='array_agg', args=[ref], agg_sort=agg_sort)
                result = pgast.FunctionCallNode(
                    name='array_to_string', args=[array_agg, separator])
                result.args.append(pgast.ConstantNode(value=''))
            else:
                args = [ref, separator]

        elif funcname == 'count':
            name = 'count'

        elif funcname == 'current_time':
            result = pgast.FunctionCallNode(
                name='current_time', noparens=True)

        elif funcname == 'current_datetime':
            result = pgast.FunctionCallNode(
                name='current_timestamp', noparens=True)

        elif funcname == 'uuid_generate_v1mc':
            name = common.qname('edgedb', 'uuid_generate_v1mc')

        elif funcname == 'strlen':
            name = 'char_length'

        elif funcname == 'lpad':
            name = 'lpad'
            # lpad expects the second argument to be int, so force cast it
            args[1] = pgast.TypeCastNode(
                expr=args[1], type=pgast.TypeNode(name='int'))

        elif funcname == 'rpad':
            name = 'rpad'
            # rpad expects the second argument to be int, so force cast it
            args[1] = pgast.TypeCastNode(
                expr=args[1], type=pgast.TypeNode(name='int'))

        elif funcname == 'levenshtein':
            name = common.qname('edgedb', 'levenshtein')

        elif funcname == 're_match':
            subq = pgast.SelectQueryNode()

            flags = pgast.FunctionCallNode(
                name='coalesce',
                args=[args[2], pgast.ConstantNode(value='')])

            fargs = [args[1], args[0], flags]
            op = pgast.FunctionCallNode(
                name='regexp_matches', args=fargs)
            subq.targets.append(op)

            result = subq

        elif funcname == 'strpos':
            r = pgast.FunctionCallNode(name='strpos', args=args)
            result = pgast.BinOpNode(
                left=r, right=pgast.ConstantNode(value=1),
                op=ast.ops.SUB)

        elif funcname == 'substr':
            name = 'substr'
            args[1] = pgast.TypeCastNode(
                expr=args[1], type=pgast.TypeNode(name='int'))
            args[1] = pgast.BinOpNode(
                left=args[1], right=pgast.ConstantNode(value=1),
                op=ast.ops.ADD)
            if args[2] is not None:
                args[2] = pgast.TypeCastNode(
                    expr=args[2], type=pgast.TypeNode(name='int'))

        elif isinstance(funcname, tuple):
            assert False, 'unsupported function %s' % (funcname, )

        else:
            name = funcname

        if not result:
            if expr.window:
                window_sort = agg_sort
                agg_sort = None

            result = pgast.FunctionCallNode(
                name=name, args=args, aggregates=bool(expr.aggregate),
                agg_sort=agg_sort, agg_filter=agg_filter)

            if expr.window:
                result.over = pgast.WindowDefNode(
                    orderby=window_sort, partition=partition)

        return result

    def visit_Constant(self, expr):
        ctx = self.context.current

        if expr.type:
            const_type = self._schema_type_to_pg_type(expr.type)
        else:
            const_type = None

        if expr.expr:
            result = pgast.ConstantNode(
                expr=self.visit(expr.expr))
        else:
            value = expr.value
            const_expr = None
            index = None

            if expr.index is not None and not isinstance(expr.index, int):
                if expr.index in ctx.argmap:
                    index = list(ctx.argmap).index(expr.index)
                else:
                    ctx.argmap.add(expr.index)
                    index = len(ctx.argmap) - 1

            result = pgast.ConstantNode(
                value=value, expr=const_expr, index=index, type=const_type)

        if expr.substitute_for:
            result.origin_field = common.edgedb_name_to_pg_name(
                expr.substitute_for)

        return result

    def visit_TypeCast(self, expr):
        ctx = self.context.current

        if (isinstance(expr.expr, irast.BinOp)
                and isinstance(expr.expr.op,
                               (ast.ops.ComparisonOperator,
                                ast.ops.TypeCheckOperator))):
            expr_type = bool
        elif (
                isinstance(expr.expr, irast.BaseRefExpr) and
                isinstance(expr.expr.expr, irast.BinOp) and isinstance(
                    expr.expr.expr.op,
                    (ast.ops.ComparisonOperator, ast.ops.TypeCheckOperator))):
            expr_type = bool
        elif isinstance(expr.expr, irast.Constant):
            expr_type = expr.expr.type
        else:
            expr_type = None

        schema = ctx.schema

        pg_expr = self.visit(expr.expr)

        target_type = expr.type

        if target_type.subtypes:
            if target_type.maintype == 'list':
                elem_type = pg_types.pg_type_from_atom(
                    schema, schema.get(target_type.subtypes[0]), topbase=True)
                result = pgast.TypeCastNode(
                    expr=pg_expr,
                    type=pgast.TypeNode(name=elem_type, array_bounds=[-1]))
            else:
                raise NotImplementedError(
                    '{} composite type is not supported '
                    'yet'.format(target_type.maintype))

        else:
            int_class = schema.get('std::int')
            target_type = schema.get(target_type.maintype)

            if (expr_type and expr_type is bool and
                    target_type.issubclass(int_class)):
                when_expr = pgast.CaseWhenNode(
                    expr=pg_expr, result=pgast.ConstantNode(value=1))
                default = pgast.ConstantNode(value=0)
                result = pgast.CaseExprNode(
                    args=[when_expr], default=default)
            else:
                result = pgast.TypeCastNode(
                    expr=pg_expr,
                    type=pgast.TypeNode(
                        name=pg_types.pg_type_from_atom(
                            schema, target_type, topbase=True)
                    )
                )

        return result

    def visit_IndexIndirection(self, expr):
        # Handle Expr[Index], where Expr may be text or array

        ctx = self.context.current

        is_string = False
        arg_type = irutils.infer_type(expr.expr, ctx.schema)

        subj = self.visit(expr.expr)
        index = self.visit(expr.index)

        if isinstance(arg_type, s_atoms.Atom):
            b = arg_type.get_topmost_base()
            is_string = b.name == 'std::str'

        one = pgast.ConstantNode(value=1)
        zero = pgast.ConstantNode(value=0)

        when_cond = pgast.BinOpNode(
            left=index, right=zero, op=ast.ops.LT)

        index_plus_one = pgast.BinOpNode(
            left=index, op=ast.ops.ADD, right=one)

        if is_string:
            upper_bound = pgast.FunctionCallNode(
                name='char_length', args=[subj])
        else:
            upper_bound = pgast.FunctionCallNode(
                name='array_upper', args=[subj, one])

        neg_off = pgast.BinOpNode(
            left=upper_bound, right=index_plus_one, op=ast.ops.ADD)

        when_expr = pgast.CaseWhenNode(
            expr=when_cond, result=neg_off)

        index = pgast.CaseExprNode(
            args=[when_expr], default=index_plus_one)

        if is_string:
            result = pgast.FunctionCallNode(
                name='substr',
                args=[subj, index, one]
            )
        else:
            indirection = pgast.IndexIndirectionNode(upper=index)
            result = pgast.IndirectionNode(
                expr=subj, indirection=indirection)

        return result

    def visit_SliceIndirection(self, expr):
        # Handle Expr[Start:End], where Expr may be text or array

        ctx = self.context.current

        subj = self.visit(expr.expr)
        start = self.visit(expr.start)
        stop = self.visit(expr.stop)
        one = pgast.ConstantNode(value=1)
        zero = pgast.ConstantNode(value=0)

        is_string = False
        arg_type = irutils.infer_type(expr.expr, ctx.schema)

        if isinstance(arg_type, s_atoms.Atom):
            b = arg_type.get_topmost_base()
            is_string = b.name == 'std::str'

        if is_string:
            upper_bound = pgast.FunctionCallNode(
                name='char_length', args=[subj])
        else:
            upper_bound = pgast.FunctionCallNode(
                name='array_upper', args=[subj, one])

        if (
                isinstance(start, pgast.ConstantNode) and
                start.value is None and start.index is None and
                start.expr is None):
            lower = one
        else:
            lower = start

            when_cond = pgast.BinOpNode(
                left=lower, right=zero, op=ast.ops.LT)
            lower_plus_one = pgast.BinOpNode(
                left=lower, right=one, op=ast.ops.ADD)

            neg_off = pgast.BinOpNode(
                left=upper_bound, right=lower_plus_one, op=ast.ops.ADD)

            when_expr = pgast.CaseWhenNode(
                expr=when_cond, result=neg_off)
            lower = pgast.CaseExprNode(
                args=[when_expr], default=lower_plus_one)

        if (
                isinstance(stop, pgast.ConstantNode) and
                stop.value is None and stop.index is None and
                stop.expr is None):
            upper = upper_bound
        else:
            upper = stop

            when_cond = pgast.BinOpNode(
                left=upper, right=zero, op=ast.ops.LT)

            neg_off = pgast.BinOpNode(
                left=upper_bound, right=upper, op=ast.ops.ADD)

            when_expr = pgast.CaseWhenNode(
                expr=when_cond, result=neg_off)
            upper = pgast.CaseExprNode(
                args=[when_expr], default=upper)

        if is_string:
            args = [subj, lower]

            if upper is not upper_bound:
                for_length = pgast.BinOpNode(
                    left=upper, op=ast.ops.SUB, right=lower)
                for_length = pgast.BinOpNode(
                    left=for_length, op=ast.ops.ADD, right=one)
                args.append(for_length)

            result = pgast.FunctionCallNode(name='substr', args=args)

        else:
            indirection = pgast.IndexIndirectionNode(
                lower=lower, upper=upper)
            result = pgast.IndirectionNode(
                expr=subj, indirection=indirection)

        return result

    def visit_SubstmtRef(self, expr):
        return self.visit(expr.stmt)

    def visit_Set(self, expr):
        ctx = self.context.current

        result = None

        rel = ctx.rel
        parent_query = ctx.query

        source_cte = self._set_to_cte(expr)

        if (expr.as_set or
                (not isinstance(expr.scls, s_atoms.Atom) and
                 ctx.location == 'where')):

            wrapped_source_cte = pgast.SelectQueryNode(
                fromlist=[
                    pgast.FromExprNode(
                        expr=source_cte
                    )
                ]
            )

            self._pull_fieldrefs(wrapped_source_cte, source_cte)

            source_cte = wrapped_source_cte

        try:
            subrels = ctx.subquery_map[rel]
        except KeyError:
            subrels = ctx.subquery_map[rel] = set()

        if source_cte not in subrels:
            subrels.add(source_cte)

            if ctx.location != 'where':
                if isinstance(source_cte, pgast.CTENode):
                    parent_query.fromlist.append(
                        pgast.FromExprNode(expr=source_cte)
                    )
                self._pull_ir_map(parent_query, source_cte)

        if isinstance(expr.scls, s_atoms.Atom):
            if expr.as_set:
                result = pgast.ExistsNode(expr=source_cte)
            else:
                result = self._get_fieldref_for_set(expr)
        else:
            if ctx.location == 'where':
                result = pgast.ExistsNode(expr=source_cte)
            else:
                result = source_cte.bonds(source_cte.edgedbnode.path_id)[0]

        try:
            callbacks = ctx.node_callbacks.pop(expr)
        except KeyError:
            pass
        else:
            for callback in callbacks:
                callback(expr)

        return result

    def visit_BinOp(self, expr):
        ctx = self.context.current

        left = self.visit(expr.left)

        if expr.op in (ast.ops.IN, ast.ops.NOT_IN) \
                and isinstance(expr.right, irast.Constant) \
                and isinstance(expr.right.expr, irast.Sequence):
            with self.context(TransformerContext.NEW_TRANSPARENT):
                ctx.sequence_is_array = True
                right = self.visit(expr.right)
        else:
            right = self.visit(expr.right)

        if isinstance(expr.op, ast.ops.TypeCheckOperator):
            result = pgast.FunctionCallNode(
                name='edgedb.issubclass',
                args=[left, right])

            if expr.op == ast.ops.IS_NOT:
                result = pgast.UnaryOpNode(
                    op=ast.ops.NOT,
                    operand=result
                )

        else:
            ctx.append_graphs = False

            if (expr.op in (ast.ops.IN, ast.ops.NOT_IN) and
                    isinstance(expr.right, irast.Constant)):
                # "expr IN $CONST" translates into
                # "expr = any($CONST)" and
                # "expr NOT IN $CONST" translates into
                # "expr != all($CONST)"
                if expr.op == ast.ops.IN:
                    op = ast.ops.EQ
                    qual_func = 'any'
                else:
                    op = ast.ops.NE
                    qual_func = 'all'

                if isinstance(right.expr, pgast.SequenceNode):
                    right.expr = pgast.ArrayNode(
                        elements=right.expr.elements)
                elif right.type == 'text[]':
                    left_type = irutils.infer_type(
                        expr.left, ctx.schema)
                    if isinstance(left_type, s_obj.NodeClass):
                        if isinstance(left_type, s_concepts.Concept):
                            left_type = left_type.pointers[
                                'std::id'].target
                        left_type = pg_types.pg_type_from_atom(
                            ctx.schema, left_type,
                            topbase=True)
                        right.type = left_type + '[]'

                right = pgast.FunctionCallNode(
                    name=qual_func, args=[right])
            else:
                op = expr.op

            left_type = irutils.infer_type(
                expr.left, ctx.schema)
            right_type = irutils.infer_type(
                expr.right, ctx.schema)

            if left_type and right_type:
                if isinstance(left_type, s_obj.NodeClass):
                    if isinstance(left_type, s_concepts.Concept):
                        left_type = left_type.pointers[
                            'std::id'].target
                    left_type = pg_types.pg_type_from_atom(
                        ctx.schema, left_type,
                        topbase=True)
                elif (
                        not isinstance(
                            left_type, s_obj.Class) and
                        (not isinstance(left_type, tuple) or
                         not isinstance(
                            left_type[1], s_obj.Class))):
                    left_type = self._schema_type_to_pg_type(
                        left_type)

                if isinstance(right_type, s_obj.NodeClass):
                    if isinstance(right_type, s_concepts.Concept):
                        right_type = right_type.pointers[
                            'std::id'].target
                    right_type = pg_types.pg_type_from_atom(
                        ctx.schema, right_type,
                        topbase=True)
                elif (
                        not isinstance(
                            right_type, s_obj.Class) and
                        (not isinstance(right_type, tuple) or
                         not isinstance(
                            right_type[1], s_obj.Class))):
                    right_type = self._schema_type_to_pg_type(
                        right_type)

                if (
                        left_type in ('text', 'varchar') and
                        right_type in ('text', 'varchar') and
                        op == ast.ops.ADD):
                    op = '||'
                elif left_type != right_type:
                    if isinstance(
                            right, pgast.
                            ConstantNode) and right_type == 'text':
                        right.type = left_type
                    elif isinstance(
                            left, pgast.
                            ConstantNode) and left_type == 'text':
                        left.type = right_type

                if ((
                        isinstance(right, pgast.ConstantNode)
                        and op in {ast.ops.IS, ast.ops.IS_NOT})):
                    right.type = None

            result = pgast.BinOpNode(
                op=op, left=left, right=right)

        return result

    def visit_UnaryOp(self, expr):
        operand = self.visit(expr.expr)
        return pgast.UnaryOpNode(op=expr.op, operand=operand)

    def visit_Sequence(self, expr):
        ctx = self.context.current

        elements = [
            self.visit(e) for e in expr.elements
        ]
        if expr.is_array:
            result = pgast.ArrayNode(elements=elements)
        elif getattr(ctx, 'sequence_is_array', False):
            result = pgast.SequenceNode(elements=elements)
        else:
            if ctx.output_format == 'json':
                elements.insert(
                    0, pgast.ConstantNode(
                        value=common.FREEFORM_RECORD_ID))
            result = pgast.RowExprNode(args=elements)

        return result

    def visit_ExistPred(self, expr):
        ctx = self.context.current

        with self.context(TransformerContext.NEW_TRANSPARENT):
            ctx.direct_subquery_ref = True
            ctx.ignore_cardinality = 'recursive'
            expr = self.visit(expr.expr)

        return pgast.ExistsNode(expr=expr)

    def visit_TypeRef(self, expr):
        ctx = self.context.current

        data_backend = ctx.backend
        schema = ctx.schema

        if expr.subtypes:
            raise NotImplementedError()
        else:
            cls = schema.get(expr.maintype)
            concept_id = data_backend.get_concept_id(cls)
            result = pgast.ConstantNode(
                value=concept_id, type='uuid')

        return result

    def visit_SelectStmt(self, stmt):
        parent_ctx = self.context.current
        parent_rel = parent_ctx.rel or parent_ctx.query

        with self.context(TransformerContext.SUBQUERY):
            ctx = self.context.current
            try:
                subrels = parent_ctx.subquery_map[parent_rel]
            except KeyError:
                subrels = parent_ctx.subquery_map[parent_rel] = set()
            subrels.add(ctx.query)

            if stmt.substmts:
                for substmt in stmt.substmts:
                    with self.context(TransformerContext.SUBQUERY):
                        ctx.rel = ctx.query = pgast.CTENode()
                        cte = self.visit(substmt)
                        cte.alias = substmt.name
                    ctx.query.ctes.add(cte)
                    ctx.explicit_cte_map[substmt] = cte

            if stmt.set_op:
                with self.context(TransformerContext.NEW):
                    ctx.rel = ctx.query = pgast.SelectQueryNode()
                    larg = self.visit(stmt.set_op_larg)

                with self.context(TransformerContext.NEW):
                    ctx.rel = ctx.query = pgast.SelectQueryNode()
                    rarg = self.visit(stmt.set_op_rarg)

                set_op = pgast.PgSQLSetOperator(stmt.set_op)
                self._setop_from_list(ctx.query, [larg, rarg], set_op)

            if stmt.where:
                with self.context(TransformerContext.NEW_TRANSPARENT):
                    self.context.current.location = 'where'
                    where = self.visit(stmt.where)

                if isinstance(where, pgast.FieldRefNode):
                    where = pgast.UnaryOpNode(
                        operand=pgast.NullTestNode(expr=where),
                        op=ast.ops.NOT)

                ctx.query.where = where

            self._process_selector(stmt.result, transform_output=True)

            self._process_orderby(stmt.orderby)

            self._process_groupby(stmt.groupby)

            if stmt.offset:
                ctx.query.offset = self._process_constant(stmt.offset)

            if stmt.limit:
                ctx.query.limit = self._process_constant(stmt.limit)

            self._connect_subrels(ctx.query)

            return ctx.query

    def visit_InsertStmt(self, stmt):
        with self.context(TransformerContext.SUBQUERY):
            ctx = self.context.current
            ctx.query = pgast.InsertQueryNode()

            if stmt.substmts:
                for substmt in stmt.substmts:
                    with self.context(TransformerContext.SUBQUERY):
                        ctx.rel = ctx.query = pgast.CTENode()
                        cte = self.visit(substmt)
                        cte.alias = substmt.name
                    ctx.query.ctes.add(cte)
                    ctx.explicit_cte_map[substmt] = cte

            ctx.query.fromexpr = self._table_from_concept(
                stmt.shape.scls, stmt.shape, None)

            path_id = irutils.LinearPath([stmt.shape.scls])

            refexpr = pgast.FieldRefNode(
                table=ctx.query.fromexpr,
                field=common.edgedb_name_to_pg_name('std::id'))

            ctx.query.concept_node_map[path_id] = \
                pgast.SelectExprNode(expr=refexpr)

            if stmt.result is not None:
                # with self.context(TransformerContext.SUBQUERY):
                #     self._process_selector(
                #         stmt.result, transform_output=True)
                #     returning = self.context.current.query
                #
                # ctx.query.targets.append(
                #     pgast.SelectExprNode(
                #         expr=returning,
                #         alias='output'
                #     )
                # )
                #
                # ctx.subquery_map[ctx.query].add(returning)
                # self._connect_subrels(ctx.query)
                pass

            return self._process_insert_data(stmt)

    def visit_UpdateStmt(self, stmt):
        with self.context(TransformerContext.SUBQUERY):
            ctx = self.context.current
            update_range = ctx.query

            self._process_selector(stmt.result, transform_output=False)

            path_id = irutils.LinearPath([stmt.shape.scls])
            update_range.targets = [update_range.concept_node_map[path_id]]

            if stmt.substmts:
                for substmt in stmt.substmts:
                    with self.context(TransformerContext.SUBQUERY):
                        ctx.rel = ctx.query = pgast.CTENode()
                        cte = self.visit(substmt)
                        cte.alias = substmt.name
                    ctx.query.ctes.add(cte)
                    ctx.explicit_cte_map[substmt] = cte

            if stmt.where:
                with self.context(TransformerContext.NEW_TRANSPARENT):
                    self.context.current.location = 'where'
                    where = self.visit(stmt.where)

                if isinstance(where, pgast.FieldRefNode):
                    where = pgast.UnaryOpNode(
                        operand=pgast.NullTestNode(expr=where),
                        op=ast.ops.NOT)

                ctx.query.where = where

            self._connect_subrels(ctx.query)

            update_target = self._table_from_concept(
                stmt.shape.scls, stmt.shape, None)

            id_ref = pgast.FieldRefNode(
                table=update_target, field='std::id')

            ctx.query = pgast.UpdateQueryNode(
                fromexpr=update_target,
                where=pgast.BinOpNode(
                    left=id_ref, op='IN', right=update_range)
            )

            return self._process_update_data(stmt)

    def visit_DeleteStmt(self, stmt):
        with self.context(TransformerContext.SUBQUERY):
            ctx = self.context.current
            delete_range = ctx.query

            self._process_selector(stmt.result, transform_output=False)

            path_id = irutils.LinearPath([stmt.shape.scls])
            delete_range.targets = [delete_range.concept_node_map[path_id]]

            if stmt.substmts:
                for substmt in stmt.substmts:
                    with self.context(TransformerContext.SUBQUERY):
                        ctx.rel = ctx.query = pgast.CTENode()
                        cte = self.visit(substmt)
                        cte.alias = substmt.name
                    ctx.query.ctes.add(cte)
                    ctx.explicit_cte_map[substmt] = cte

            if stmt.where:
                with self.context(TransformerContext.NEW_TRANSPARENT):
                    self.context.current.location = 'where'
                    where = self.visit(stmt.where)

                if isinstance(where, pgast.FieldRefNode):
                    where = pgast.UnaryOpNode(
                        operand=pgast.NullTestNode(expr=where),
                        op=ast.ops.NOT)

                ctx.query.where = where

            self._connect_subrels(ctx.query)

            delete_target = self._table_from_concept(
                stmt.shape.scls, stmt.shape, None)

            id_ref = pgast.FieldRefNode(
                table=delete_target, field='std::id')

            ctx.query = pgast.DeleteQueryNode(
                fromexpr=delete_target,
                where=pgast.BinOpNode(
                    left=id_ref, op='IN', right=delete_range)
            )

            if ctx.output_format == 'json':
                keyvals = [pgast.ConstantNode(value='id'), id_ref]
                target = pgast.SelectExprNode(expr=pgast.FunctionCallNode(
                    name='jsonb_build_object', args=keyvals))
            else:
                target = pgast.SelectExprNode(expr=id_ref)

            ctx.query.targets.append(target)

            return ctx.query

    def _process_insert_data(self, stmt):
        """Generate SQL INSERTs from an Insert IR."""
        ctx = self.context.current

        cols = [pgast.FieldRefNode(field='std::__class__')]
        select = pgast.SelectQueryNode()
        values = pgast.SequenceNode()
        select.values = [values]

        query = ctx.query

        query.cols = cols
        query.select = select

        # Type reference is always inserted.
        values.elements.append(
            pgast.SelectQueryNode(
                targets=[
                    pgast.SelectExprNode(
                        expr=pgast.FieldRefNode(field='id'))
                ],
                fromlist=[
                    pgast.TableNode(name='concept', schema='edgedb')
                ],
                where=pgast.BinOpNode(
                    op=ast.ops.EQ,
                    left=pgast.FieldRefNode(field='name'),
                    right=pgast.ConstantNode(value=stmt.shape.scls.name)
                )
            )
        )

        if not stmt.shape.elements:
            return query

        external_inserts = []

        for expr in stmt.shape.elements:
            ptrcls = expr.rptr.ptrcls
            insvalue = expr.stmt.result

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, schema=ctx.schema, resolve_type=True,
                link_bias=False)

            props_only = False
            ins_props = None
            operation = None

            # First, process all local link inserts.
            if ptr_info.table_type == 'concept':
                props_only = True
                field = pgast.FieldRefNode(
                    field=ptr_info.column_name, table=None)
                cols.append(field)

                with self.context(TransformerContext.NEW_TRANSPARENT):
                    if self._is_composite_cast(insvalue):
                        insvalue, ins_props = self._extract_update_value(
                            insvalue, ptr_info.column_type)

                    else:
                        insvalue = pgast.TypeCastNode(
                            expr=self.visit(insvalue),
                            type=pgast.TypeNode(name=ptr_info.column_type))

                    values.elements.append(insvalue)

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, resolve_type=False, link_bias=True)

            if ptr_info and ptr_info.table_type == 'link':
                external_inserts.append((expr, props_only, operation))

        toplevel = None

        # Inserting externally-stored links requires repackaging everything
        # into a series of CTEs so that multiple statements can be executed
        # as a single query.
        #
        for expr, props_only, operation in external_inserts:
            if toplevel is None:
                toplevel = pgast.SelectQueryNode()
                toplevel.ctes.add(query)
                query.alias = ctx.genalias(hint='m')

                query.targets.append(
                    pgast.SelectExprNode(
                        expr=pgast.FieldRefNode(field='std::id'),
                        alias=None))

                ref = pgast.FieldRefNode(table=query, field='std::id')
                toplevel.fromlist.append(pgast.CTERefNode(cte=query))

                if ctx.output_format == 'json':
                    keyvals = [pgast.ConstantNode(value='id'), ref]
                    target = pgast.SelectExprNode(expr=pgast.FunctionCallNode(
                        name='jsonb_build_object', args=keyvals))
                else:
                    target = pgast.SelectExprNode(expr=ref)

                toplevel.targets.append(target)

            self._process_update_expr(
                expr, props_only, operation, toplevel, query)

        if toplevel is not None:
            query = toplevel

        return query

    def _process_update_values(
            self, updvalexpr, target_tab, tab_cols, col_data, sources,
            props_only, target_is_atom):
        """Unpack data from an update expression into a series of selects."""
        ctx = self.context.current

        # Recurse down to process update expressions like
        # col := col + val1 + val2
        #
        if (isinstance(updvalexpr, irast.BinOp) and
                isinstance(updvalexpr.left, irast.BinOp)):
            tranches = self._process_update_values(
                updvalexpr.left, target_tab, tab_cols, col_data,
                sources, props_only, target_is_atom)
        else:
            tranches = []

        if isinstance(updvalexpr, irast.BinOp):
            updval = updvalexpr.right
        else:
            updval = updvalexpr

        if isinstance(updval, irast.TypeCast):
            # Link property updates will have the data casted into
            # an appropriate selector shape which specifies which properties
            # are being updated.
            #
            data = updval.expr
            typ = updval.type

            if not isinstance(typ, tuple):
                raise ValueError(
                    'unexpected value type in update expr: {!r}'.format(typ))

            if not isinstance(typ[1], irast.CompositeType):
                raise ValueError(
                    'unexpected value type in update expr: {!r}'.format(typ))

            props = [p.ptr_class.normal_name() for p in typ[1].pathspec]

        else:
            # Target-only update
            #
            data = updval
            props = ['std::target']

        e = common.edgedb_name_to_pg_name

        spec_cols = {e(prop): i for i, prop in enumerate(props)}

        if (props == ['std::target'] and props_only and not target_is_atom):
            # No property upates and the target value is stored
            # in the source table, so we don't need to modify
            # any link tables.
            #
            return tranches

        with self.context(TransformerContext.NEW_TRANSPARENT):
            self.context.current.output_format = None
            input_data = self.visit(data)

        if (isinstance(input_data, pgast.ConstantNode) and
                input_data.type.endswith('[]')):
            data_is_json = input_data.type == 'json[]'
            input_data = pgast.FunctionCallNode(
                name='UNNEST', args=[input_data])
        else:
            data_is_json = False

        input_rel = pgast.FromExprNode(
            expr=input_data,
            alias=ctx.genalias('i')
        )

        unnested = pgast.SelectQueryNode(
            targets=[
                pgast.SelectExprNode(
                    expr=pgast.FieldRefNode(field='*', table=input_rel))
            ], fromlist=[input_rel], alias='j', coldef='(_)')

        row = pgast.SequenceNode()

        for col in tab_cols:
            if (col == 'std::target' and (props_only or target_is_atom)):
                expr = pgast.TypeCastNode(
                    expr=pgast.ConstantNode(value=None),
                    type=pgast.TypeNode(name='uuid'))
            else:
                if col == 'std::target@atom':
                    col = 'std::target'

                data_idx = spec_cols.get(col)
                if data_idx is None:
                    try:
                        expr = col_data[col]
                    except KeyError:
                        if tab_cols[col]['column_default'] is not None:
                            expr = pgast.LiteralExprNode(
                                expr=tab_cols[col]['column_default'])
                        else:
                            expr = pgast.ConstantNode(value=None)
                else:
                    expr = pgast.FieldRefNode(table=unnested, field='_')
                    if data_is_json:
                        expr = pgast.BinOpNode(
                            left=expr, op='->>',
                            right=pgast.ConstantNode(value=data_idx))

            row.elements.append(expr)

        tranch_data = pgast.SelectQueryNode(
            targets=[
                pgast.SelectExprNode(
                    expr=pgast.IndirectionNode(
                        expr=pgast.TypeCastNode(
                            expr=row,
                            type=pgast.TypeNode(
                                name=common.qname(*target_tab)
                            )
                        ),
                        indirection=pgast.StarIndirectionNode()
                    )
                )
            ],
            fromlist=[unnested],
            alias=ctx.genalias(hint='r')
        )

        tranch_data.fromlist.extend(sources)

        tranches.append((tab_cols, tranch_data))

        return tranches

    def _process_update_expr(self, updexpr, props_only, operation, query,
                             scope_cte):
        ctx = self.context.current

        edgedb_link = pgast.TableNode(
            schema='edgedb', name='link', alias=ctx.genalias(hint='l'))

        rptr = updexpr.rptr
        ptrcls = rptr.ptrcls
        target_is_atom = isinstance(rptr.target, s_atoms.Atom)

        lname_to_id = pgast.CTENode(
            fromlist=[
                edgedb_link
            ],
            targets=[
                pgast.SelectExprNode(
                    expr=pgast.FieldRefNode(table=edgedb_link, field='id'),
                    alias='id')
            ],
            where=pgast.BinOpNode(
                left=pgast.FieldRefNode(table=edgedb_link,
                                        field='name'), op=ast.ops.EQ,
                right=pgast.ConstantNode(value=ptrcls.name)
            ),
            alias=ctx.genalias(hint='lid')
        )

        query.ctes.add(lname_to_id)

        target_tab = self._table_from_ptrcls(ptrcls)

        if target_is_atom:
            target_tab_name = (target_tab.schema, target_tab.name)
        else:
            target_tab_name = common.link_name_to_table_name(
                ptrcls.normal_name(), catenate=False)

        tab_cols = \
            ctx.backend._type_mech.get_cached_table_columns(target_tab_name)

        assert tab_cols, "could not get cols for {!r}".format(target_tab_name)

        col_data = {
            'link_type_id': pgast.SelectExprNode(
                expr=pgast.FieldRefNode(table=lname_to_id, field='id')),
            'std::source': pgast.FieldRefNode(
                table=scope_cte, field='std::id')
        }

        if operation is None:
            # Drop previous entries first
            delcte = pgast.DeleteQueryNode(
                fromexpr=target_tab,
                where=pgast.BinOpNode(
                    left=col_data['std::source'],
                    op=ast.ops.EQ,
                    right=pgast.FieldRefNode(
                        table=target_tab,
                        field='std::source'
                    )
                ),
                alias=ctx.genalias(hint='d'),
                using=[scope_cte],
                targets=[
                    pgast.SelectExprNode(
                        expr=col_data['std::source'], alias='std::id')
                ])
            query.ctes.add(delcte)
            scope_cte = pgast.JoinNode(
                type='NATURAL LEFT', left=pgast.CTERefNode(cte=scope_cte),
                right=pgast.CTERefNode(cte=delcte))
        else:
            delcte = None

        tranches = self._process_update_values(
            updexpr, target_tab_name, tab_cols, col_data,
            [scope_cte, lname_to_id], props_only, target_is_atom)

        for cols, data in tranches:
            query.ctes.add(data)
            data = pgast.SelectQueryNode(
                targets=[
                    pgast.SelectExprNode(
                        expr=pgast.FieldRefNode(field='*', table=data))
                ], fromlist=[pgast.CTERefNode(cte=data)])

            if operation == ast.ops.SUB:
                # Removing links
                updcte = pgast.DeleteQueryNode(
                    alias=ctx.genalias(hint='d'),
                    targets=[
                        pgast.SelectExprNode(
                            expr=pgast.FieldRefNode(field='std::source'),
                            alias='std::id')
                    ]
                )

                updcte.fromexpr = target_tab
                data.alias = ctx.genalias(hint='q')
                updcte.where = pgast.BinOpNode(
                    left=pgast.FieldRefNode(field='std::linkid'),
                    op=ast.ops.IN,
                    right=pgast.SelectQueryNode(
                        targets=[
                            pgast.SelectExprNode(
                                expr=pgast.FieldRefNode(
                                    field='std::linkid'))
                        ],
                        fromlist=[data]
                    )
                )

            else:
                # Inserting links
                updcte = pgast.InsertQueryNode(
                    alias=ctx.genalias(hint='i'),
                    targets=[
                        pgast.SelectExprNode(
                            expr=pgast.FieldRefNode(field='std::source'),
                            alias='std::id'
                        )
                    ]
                )

                updcte.fromexpr = target_tab

                updcte.select = data
                updcte.cols = [
                    pgast.FieldRefNode(field=col) for col in cols
                ]

                update_clause = pgast.UpdateExprNode(
                    expr=pgast.SequenceNode(elements=updcte.cols),
                    value=data)

                updcte.on_conflict = pgast.OnConflictNode(
                    action='update',
                    infer=[pgast.FieldRefNode(field='std::linkid')],
                    targets=[update_clause])

            query.ctes.add(updcte)

    def _process_update_data(self, stmt):
        ctx = self.context.current
        query = ctx.query

        external_updates = []

        for expr in stmt.shape.elements:
            ptrcls = expr.rptr.ptrcls
            updvalue = expr.stmt.result

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, schema=ctx.schema, resolve_type=True, link_bias=False)

            props_only = False
            upd_props = None
            operation = None

            # First, process all internal link updates
            if ptr_info.table_type == 'concept':
                field = pgast.FieldRefNode(
                    field=ptr_info.column_name, table=None)
                props_only = True

                with self.context(TransformerContext.NEW_TRANSPARENT):
                    if self._is_composite_cast(updvalue):
                        updvalue, upd_props = self._extract_update_value(
                            updvalue, ptr_info.column_type)

                    else:
                        updvalue = pgast.TypeCastNode(
                            expr=self.visit(updvalue),
                            type=pgast.TypeNode(name=ptr_info.column_type))

                    query.values.append(
                        pgast.UpdateExprNode(expr=field, value=updvalue))

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, resolve_type=False, link_bias=True)

            if ptr_info and ptr_info.table_type == 'link':
                external_updates.append((expr, props_only, operation))

        if not query.values:
            # No atomic updates
            query = pgast.CTENode(
                ctes=query.ctes, targets=query.targets,
                fromlist=[query.fromexpr], where=query.where)

        if not external_updates:
            ref = pgast.FieldRefNode(field='std::id')

            if ctx.output_format == 'json':
                keyvals = [pgast.ConstantNode(value='id'), ref]
                target = pgast.SelectExprNode(expr=pgast.FunctionCallNode(
                    name='jsonb_build_object', args=keyvals))
            else:
                target = pgast.SelectExprNode(expr=ref)

            query.targets.append(target)

        toplevel = None

        # Updating externally-stored linksrequires repackaging everything into
        # a series of CTEs so that multiple statements can be executed as a
        # single query.
        #
        for expr, props_only, operation in external_updates:
            if toplevel is None:
                toplevel = pgast.SelectQueryNode()
                toplevel.ctes.update(query.ctes)
                toplevel.ctes.add(query)
                query.ctes.clear()
                query.alias = ctx.genalias(hint='m')

                query.targets.append(
                    pgast.SelectExprNode(
                        expr=pgast.FieldRefNode(field='std::id'),
                        alias=None))

                ref = pgast.FieldRefNode(table=query, field='std::id')

                if ctx.output_format == 'json':
                    keyvals = [pgast.ConstantNode(value='id'), ref]
                    target = pgast.SelectExprNode(expr=pgast.FunctionCallNode(
                        name='jsonb_build_object', args=keyvals))
                else:
                    target = pgast.SelectExprNode(expr=ref)

                toplevel.fromlist.append(pgast.CTERefNode(cte=query))
                toplevel.targets.append(target)

            self._process_update_expr(
                expr, props_only, operation, toplevel, query)

        if toplevel is not None:
            query = toplevel

        return query

    def _connect_subrels(self, query):
        ctx = self.context.current
        rels = ctx.subquery_map.get(query)

        if not rels:
            return

        field_map = query.concept_node_map

        for rel in rels:
            if isinstance(rel, pgast.CTENode):
                fromlist = {f.expr for f in query.fromlist}
                fromlist.add(query)

                for path_id, fieldref in rel._bonds.items():
                    if path_id not in field_map:
                        continue

                    lref = fieldref[0]
                    rref = field_map[path_id].expr

                    if (lref.table, lref.field) != (rref.table, rref.field):
                        bond_cond = pgast.BinOpNode(
                            op='=', left=lref, right=rref)
                        query.where = self._extend_binop(
                            query.where, bond_cond)

                        if lref.table not in fromlist:
                            query.fromlist.append(
                                pgast.FromExprNode(
                                    expr=lref.table
                                )
                            )
                            fromlist.add(lref.table)

            else:
                innerrel = rel.fromlist[0].expr
                fromlist = {f.expr for f in innerrel.fromlist}
                fromlist.add(innerrel)

                for path_id, fieldref in innerrel._bonds.items():
                    if path_id not in field_map:
                        continue

                    lref = fieldref[0]
                    rref = field_map[path_id].expr

                    if (lref.table, lref.field) != (rref.table, rref.field):
                        bond_cond = pgast.BinOpNode(
                            op='=', left=lref, right=rref)
                        rel.where = self._extend_binop(
                            rel.where, bond_cond)

                        if lref.table not in fromlist:
                            innerrel.fromlist.append(
                                pgast.FromExprNode(
                                    expr=lref.table
                                )
                            )
                            fromlist.add(lref.table)

    def _process_selector(self, result_expr, transform_output=True):
        ctx = self.context.current
        query = ctx.query

        with self.context(TransformerContext.NEW_TRANSPARENT):
            self.context.current.location = 'selector'
            pgexpr = self.visit(result_expr)
            selexprs = [(pgexpr, None)]

        if ctx.output_format == 'json' and transform_output:
            # Target list may be empty if selector is a set op product
            if selexprs:
                filter_expr = getattr(pgexpr, 'filter_expr', None)

                target = pgast.SelectExprNode(
                    expr=pgast.FunctionCallNode(
                        name='to_jsonb', args=[pgexpr]), alias=None,
                    filter_expr=filter_expr)
                query.targets.append(target)

        else:
            for pgexpr, alias in selexprs:
                target = pgast.SelectExprNode(expr=pgexpr, alias=alias)
                query.targets.append(target)

    def _process_orderby(self, sorter):
        ctx = self.context.current

        query = ctx.query
        ctx.location = 'orderby'

        for expr in sorter:
            sortexpr = pgast.SortExprNode(
                expr=self.visit(expr.expr),
                direction=expr.direction, nulls_order=expr.nones_order)
            query.orderby.append(sortexpr)

    def _process_groupby(self, grouper):
        ctx = self.context.current

        query = ctx.query
        ctx.location = 'groupby'

        for expr in grouper:
            sortexpr = self.visit(expr)
            query.groupby.append(sortexpr)

    def _get_fieldref_for_set(self, ir_set):
        """Return FieldRef node corresponding to the specified atomic Set.

        Arguments:
            - context: Current context
            - ir_set: IR Set

        Return:
            A pgast.FieldRef node representing a set of atom/schema
            values for the specified ir_set.
        """
        if not isinstance(ir_set.scls, s_atoms.Atom):
            raise ValueError('expecting atomic Set')

        ctx = self.context.current

        if ir_set.subset_of is not None:
            rptr = ir_set.subset_of.rptr
        else:
            rptr = ir_set.rptr

        ptr_name = rptr.ptrcls.normal_name()

        try:
            ref = ctx.ir_set_field_map[ir_set.path_id]
        except KeyError:
            raise LookupError('could not resolve {!r} as table field'.format(
                ir_set.path_id))

        if isinstance(ref, pgast.SelectExprNode):
            ref = ref.expr

        if ctx.in_aggregate:
            # Cast atom refs to the base type in aggregate expressions, since
            # PostgreSQL does not create array types for custom domains and
            # will fail to process a query with custom domains appearing as
            # array elements.
            #
            schema = ctx.schema
            link = ir_set.scls.resolve_pointer(
                schema, ptr_name, look_in_children=True)
            pgtype = pg_types.pg_type_from_atom(
                schema, link.target, topbase=True)
            pgtype = pgast.TypeNode(name=pgtype)
            ref = pgast.TypeCastNode(expr=ref, type=pgtype)

        return ref

    def _join_condition(self, left_refs, right_refs, op='='):
        if not isinstance(left_refs, tuple):
            left_refs = (left_refs, )
        if not isinstance(right_refs, tuple):
            right_refs = (right_refs, )

        condition = None
        for left_ref, right_ref in itertools.product(left_refs, right_refs):
            op = pgast.BinOpNode(op='=', left=left_ref, right=right_ref)
            condition = self._extend_binop(condition, op)

        return condition

    def _simple_join(self, left, right, key, type='inner', condition=None):
        if condition is None:
            left_refs = left.bonds(key)[-1]
            right_refs = right.bonds(key)[-1]
            condition = self._join_condition(left_refs, right_refs)

        join = pgast.JoinNode(
            type=type, left=left, right=right, condition=condition)

        join.updatebonds(left)
        join.updatebonds(right)

        return join

    def _pull_fieldrefs(self, target_rel, source_rel):
        for path_id, ref in source_rel.concept_node_map.items():
            refexpr = pgast.FieldRefNode(
                table=source_rel, field=ref.alias,
                origin=ref.expr.origin, origin_field=ref.expr.origin_field)

            fieldref = pgast.SelectExprNode(
                expr=refexpr, alias=ref.alias)

            target_rel.targets.append(fieldref)

            if path_id not in target_rel.concept_node_map:
                target_rel.concept_node_map[path_id] = fieldref

            if isinstance(path_id[-1], s_concepts.Concept):
                bondref = pgast.FieldRefNode(
                    table=target_rel, field=ref.alias,
                    origin=ref.expr.origin,
                    origin_field=ref.expr.origin_field)
                target_rel.addbond(path_id, bondref)

    def _pull_ir_map(self, target_rel, source_rel):
        for path_id, ref in source_rel.concept_node_map.items():
            refexpr = pgast.FieldRefNode(
                table=source_rel, field=ref.alias,
                origin=ref.expr.origin, origin_field=ref.expr.origin_field)

            fieldref = pgast.SelectExprNode(
                expr=refexpr, alias=ref.alias)

            if path_id not in target_rel.concept_node_map:
                target_rel.concept_node_map[path_id] = fieldref

    def _join_mapping_rel(self, *, ir_set, cte, join, scls_rel):
        id_field = common.edgedb_name_to_pg_name('std::id')
        map_join_type = 'inner'
        tip_pathvar = ir_set.pathvar if ir_set else None
        link = ir_set.rptr
        linkmap_key = link.ptrcls, link.direction, link.source, tip_pathvar

        try:
            # The same link map must not be joined more than once,
            # otherwise the cardinality of the result set will be wrong.
            #
            map_rel, map_join = cte.linkmap[linkmap_key]
        except KeyError:
            map_rel = self._relation_from_link(link)
            map_rel.concepts = frozenset((ir_set.scls,))
            map_join = None

        # Set up references according to link direction
        #
        src_col = common.edgedb_name_to_pg_name('std::source')
        source_ref = pgast.FieldRefNode(
            table=map_rel, field=src_col, origin=map_rel,
            origin_field=src_col)

        tgt_col = common.edgedb_name_to_pg_name('std::target')
        target_ref = pgast.FieldRefNode(
            table=map_rel, field=tgt_col, origin=map_rel,
            origin_field=tgt_col)

        valent_bond = join.bonds(link.source.path_id)[-1]
        forward_bond = self._join_condition(
            valent_bond, source_ref, op='=')
        backward_bond = self._join_condition(
            valent_bond, target_ref, op='=')

        if link.direction == s_pointers.PointerDirection.Inbound:
            map_join_cond = backward_bond
        else:
            map_join_cond = forward_bond

        if map_join is None:
            map_join = pgast.JoinNode(left=map_rel)
            map_join.updatebonds(map_rel)

            # Join link relation to source relation
            #
            join = self._simple_join(
                join, map_join, link.source.path_id,
                type=map_join_type, condition=map_join_cond)

            cte.linkmap[linkmap_key] = map_rel, map_join

        if scls_rel:
            # Join the target relation, if we have it
            target_id_field = pgast.FieldRefNode(
                table=scls_rel, field=id_field,
                origin=scls_rel, origin_field=id_field)

            if link.direction == s_pointers.PointerDirection.Inbound:
                map_tgt_ref = source_ref
            else:
                map_tgt_ref = target_ref

            cond_expr = pgast.BinOpNode(
                left=map_tgt_ref, op='=', right=target_id_field)

            prev_bonds = join.bonds(ir_set.path_id)

            # We use inner join for target relations to make sure this join
            # relation is not producing dangling links, either as a result
            # of partial data, or query constraints.
            #
            if map_join.right is None:
                map_join.right = scls_rel
                map_join.condition = cond_expr
                map_join.type = 'inner'
                map_join.updatebonds(scls_rel)

            else:
                pre_map_join = map_join.copy()
                new_map_join = self._simple_join(
                    pre_map_join, scls_rel,
                    ir_set.path_id, type='inner', condition=cond_expr)
                map_join.copyfrom(new_map_join)

            join.updatebonds(scls_rel)

            if prev_bonds:
                join.addbond(ir_set.path_id, prev_bonds[-1])

        return join, map_rel

    def _join_inline_rel(self, *, ir_set, cte, join, scls_rel):
        id_field = common.edgedb_name_to_pg_name('std::id')
        cls_field = common.edgedb_name_to_pg_name('std::__class__')
        source = join.bonds(ir_set.rptr.source.path_id)[-1]

        source_ref_field = pgast.FieldRefNode(
            table=source.table, field=cls_field,
            origin=source.table, origin_field=cls_field)

        target_ref_field = pgast.FieldRefNode(
            table=scls_rel, field=id_field,
            origin=scls_rel, origin_field=id_field)

        cond_expr = pgast.BinOpNode(
            left=source_ref_field, op='=', right=target_ref_field)

        prev_bonds = join.bonds(ir_set.path_id)

        new_join = self._simple_join(
            join, scls_rel,
            ir_set.path_id, type='inner', condition=cond_expr)

        join.updatebonds(scls_rel)
        if prev_bonds:
            join.addbond(ir_set.path_id, prev_bonds[-1])

        return new_join

    def _set_to_cte(self, ir_set):
        """Generate a Common Table Expression for a given IR Set.

        @param context: Compiler context.
        @param rel: Parent relation.
        @param ir_set: IR Set node.
        """
        ctx = self.context.current

        root_query = ctx.query

        cte = ctx.ctemap.get(ir_set)
        if cte is not None:
            # Already have a CTE for this Set.
            return cte

        fromnode = pgast.FromExprNode()
        if ir_set.rptr is not None:
            alias_hint = ir_set.rptr.ptrcls.normal_name().name
        elif (ir_set.subset_of is not None and
                ir_set.subset_of.rptr is not None):
            alias_hint = ir_set.subset_of.rptr.ptrcls.normal_name().name
        else:
            alias_hint = ir_set.scls.name.name

        cte = pgast.CTENode(
            concepts=frozenset({ir_set.scls}),
            alias=ctx.genalias(hint=str(alias_hint)),
            edgedbnode=ir_set,
            fromlist=[fromnode])

        ctx.ctemap[ir_set] = cte

        # Get the superset CTE if any.
        if ir_set.subset_of is not None:
            parent_cte = self._set_to_cte(ir_set.subset_of)

        elif ir_set.rptr is not None:
            parent_cte = self._set_to_cte(ir_set.rptr.source)

        else:
            parent_cte = None

            if isinstance(ir_set.scls, s_atoms.Atom):
                # Atomic Sets cannot appear without a source superset.
                raise RuntimeError('unexpected atomic set without a source')

        if parent_cte is not None:
            fromnode.expr = parent_cte
            self._pull_fieldrefs(cte, parent_cte)

        if isinstance(ir_set.scls, s_concepts.Concept):
            id_field = common.edgedb_name_to_pg_name('std::id')

            cte.scls_rel = scls_rel = self._relation_from_concepts(ir_set, cte)

            bond = pgast.FieldRefNode(
                table=scls_rel, field=id_field,
                origin=scls_rel, origin_field=id_field)

            scls_rel.addbond(ir_set.path_id, bond)

            id_set = self._get_id_set(ir_set)
            id_alias = self._add_inline_atom_ref(cte, id_set, ir_set.path_id)

            if not cte.bonds(ir_set.path_id):
                bond = pgast.FieldRefNode(table=cte, field=id_alias)
                cte.addbond(ir_set.path_id, bond)

        else:
            scls_rel = None

        if ir_set.rptr is not None:
            # This is the nth step in the path, where n > 1.
            # Translate pointer traversal into a join clause.

            ptrcls = ir_set.rptr.ptrcls

            join = fromnode.expr
            map_rel = None

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, resolve_type=False)

            if isinstance(ptrcls, s_lprops.LinkProperty):
                # Reference to singular atom.
                self._add_inline_atom_ref(parent_cte, ir_set)

                if not ir_set.criteria:
                    ctx.ctemap[ir_set] = parent_cte
                    return parent_cte

            elif ptr_info.table_type != 'concept':
                # This is a 1* or ** cardinality, join via a mapping relation.
                join, map_rel = self._join_mapping_rel(
                    ir_set=ir_set, cte=cte, join=join, scls_rel=scls_rel)

            elif isinstance(ir_set.scls, s_concepts.Concept):
                # Direct reference to another object.
                join = self._join_inline_rel(
                    ir_set=ir_set, cte=cte, join=join, scls_rel=scls_rel)

            else:
                # Reference to singular atom.
                self._add_inline_atom_ref(parent_cte, ir_set)

                if not ir_set.criteria:
                    ctx.ctemap[ir_set] = parent_cte
                    return parent_cte

            cte.rptr_rel = map_rel
            fromnode.expr = join

        if fromnode.expr is None:
            # This is the root set, select directly from class table.
            fromnode.expr = scls_rel

        if ir_set.criteria:
            # Turn set criteria into WHERE predicates
            for criterion in ir_set.criteria:
                with self.context(TransformerContext.NEW_TRANSPARENT):
                    self.context.current.rel = cte
                    filter_expr = self.visit(criterion)
                    self._connect_subrels(cte)

                cte.where = self._extend_binop(
                    cte.where, filter_expr)

        # Finally, attach the CTE to the parent query.
        root_query.ctes.add(cte)

        return cte

    def _get_id_set(self, source_set):
        ctx = self.context.current

        schema = ctx.schema
        scls = source_set.scls
        ptrcls = scls.resolve_pointer(schema, 'std::id')

        path_id = irutils.LinearPath(source_set.path_id)
        path_id.add(ptrcls, s_pointers.PointerDirection.Outbound,
                    ptrcls.target)

        target_set = irast.Set()
        target_set.scls = ptrcls.target
        target_set.path_id = path_id
        target_set.subset_of = None

        ptr = irast.Pointer(
            source=source_set,
            target=target_set,
            ptrcls=ptrcls,
            direction=s_pointers.PointerDirection.Outbound
        )

        target_set.rptr = ptr

        return target_set

    def _setop_from_list(self, parent_qry, oplist, op):
        nq = len(oplist)

        assert nq >= 2, 'set operation requires at least two arguments'

        parent_qry.op = op

        for i in range(nq):
            parent_qry.larg = oplist[i]
            if i == nq - 2:
                parent_qry.rarg = oplist[i + 1]
                break
            else:
                parent_qry.rarg = pgast.SelectQueryNode(op=op)
                parent_qry = parent_qry.rarg

    def _add_inline_atom_ref(self, rel, ir_set, path_id=None):
        ctx = self.context.current

        if path_id is None:
            path_id = ir_set.path_id

        try:
            return rel.concept_node_map[path_id].alias
        except KeyError:
            pass

        id_field = common.edgedb_name_to_pg_name('std::id')

        rptr = ir_set.rptr
        ptrcls = rptr.ptrcls
        ptrname = ptrcls.normal_name()

        if isinstance(ptrcls, s_lprops.LinkProperty):
            source = rptr.source.rptr.ptrcls
            scls_rel = rel.rptr_rel
        else:
            source = rptr.source.scls
            scls_rel = rel.scls_rel

        schema = ctx.schema

        fromnode = rel.fromlist[0]

        ref_map = {
            n: [scls_rel]
            for n, p in source.pointers.items() if p.atomic()
        }
        joined_atomref_sources = {source: scls_rel}

        try:
            atomref_tables = ref_map[ptrname]
        except KeyError:
            sources = source.get_ptr_sources(
                schema, ptrname, look_in_children=True,
                strict_ancestry=True)
            assert sources

            if source.is_virtual:
                # Atom refs to columns present in direct children of a
                # virtual concept are guaranteed to be included in the
                # relation representing the virtual concept.
                #
                schema = ctx.schema
                chain = itertools.chain.from_iterable
                child_ptrs = set(
                    chain(
                        c.pointers
                        for c in source.children(schema)))
                if ptrname in child_ptrs:
                    descendants = set(source.descendants(schema))
                    sources -= descendants
                    sources.add(source)

            for s in sources:
                if s not in joined_atomref_sources:
                    atomref_table = self._table_from_concept(
                        s, ir_set, rel)
                    joined_atomref_sources[s] = atomref_table
                    left = pgast.FieldRefNode(
                        table=scls_rel, field=id_field)
                    right = pgast.FieldRefNode(
                        table=atomref_table, field=id_field)
                    joincond = pgast.BinOpNode(
                        op='=', left=left, right=right)
                    fromnode.expr = self._simple_join(
                        fromnode.expr, atomref_table,
                        key=None, type='left', condition=joincond)
            ref_map[ptrname] = atomref_tables = [
                joined_atomref_sources[c] for c in sources
            ]

        colname = common.edgedb_name_to_pg_name(ptrname)

        fieldrefs = [
            pgast.FieldRefNode(
                table=atomref_table, field=colname,
                origin=atomref_table, origin_field=colname)
            for atomref_table in atomref_tables
        ]
        alias = rel.alias + (
            '_' + ctx.genalias(hint=str(ptrname)))

        # If the required atom column was defined in multiple
        # descendant tables and there is no common parent with
        # this column, we'll have to coalesce fieldrefs to all tables.
        #
        if len(fieldrefs) > 1:
            refexpr = pgast.FunctionCallNode(
                name='coalesce', args=fieldrefs)
        else:
            refexpr = fieldrefs[0]

        selectnode = pgast.SelectExprNode(expr=refexpr, alias=alias)
        rel.targets.append(selectnode)

        rel.concept_node_map[path_id] = selectnode

        # Record atom references in the global map in case they have to
        # be pulled up later
        #
        refexpr = pgast.FieldRefNode(
            table=rel, field=selectnode.alias,
            origin=atomref_tables, origin_field=colname)
        selectnode = pgast.SelectExprNode(
            expr=refexpr, alias=selectnode.alias)
        ctx.ir_set_field_map[path_id] = selectnode

        return alias

    def _run_codegen(self, qtree):
        codegen = pgcodegen.SQLSourceGenerator()
        try:
            codegen.visit(qtree)
        except pgcodegen.SQLSourceGeneratorError as e:
            ctx = pgcodegen.SQLSourceGeneratorContext(
                qtree, codegen.result)
            edgedb_error.add_context(e, ctx)
            raise
        except Exception as e:
            ctx = pgcodegen.SQLSourceGeneratorContext(
                qtree, codegen.result)
            err = pgcodegen.SQLSourceGeneratorError(
                'error while generating SQL source')
            edgedb_error.add_context(err, ctx)
            raise err from e

        return codegen

    def _schema_type_to_pg_type(self, schema_type):
        ctx = self.context.current

        if isinstance(schema_type, s_atoms.Atom):
            const_type = pg_types.pg_type_from_atom(
                ctx.schema, schema_type, topbase=True)
        elif isinstance(schema_type, (s_concepts.Concept, s_links.Link)):
            const_type = 'json'
        elif isinstance(schema_type, s_obj.MetaClass):
            const_type = 'int'
        elif isinstance(schema_type, tuple):
            item_type = schema_type[1]
            if isinstance(item_type, s_atoms.Atom):
                item_type = pg_types.pg_type_from_atom(
                    ctx.schema, item_type, topbase=True)
                const_type = '%s[]' % item_type
            elif isinstance(item_type, (s_concepts.Concept, s_links.Link)):
                item_type = 'json'
                const_type = '%s[]' % item_type
            elif isinstance(item_type, s_obj.MetaClass):
                item_type = 'int'
                const_type = '%s[]' % item_type
            else:
                raise ValueError('unexpected constant type: '
                                 '{!r}'.format(schema_type))
        else:
            raise ValueError('unexpected constant type: '
                             '{!r}'.format(schema_type))

        return const_type

    def _table_from_concept(self, concept, node, parent_cte):
        ctx = self.context.current

        if concept.is_virtual:
            # Virtual concepts are represented as a UNION of selects from their
            # children, which is, for most purposes, equivalent to SELECTing
            # from a parent table.
            #
            idptr = sn.Name('std::id')
            idcol = common.edgedb_name_to_pg_name(idptr)
            atomrefs = {idptr: irast.AtomicRefSimple(ref=node, name=idptr)}
            atomrefs.update({f.name: f for f in node.atomrefs})

            cols = [(aref, common.edgedb_name_to_pg_name(aref))
                    for aref in atomrefs]

            schema = ctx.schema

            union_list = []
            children = frozenset(concept.children(schema))

            inhmap = s_utils.get_full_inheritance_map(schema, children)

            coltypes = {}

            for c, cc in inhmap.items():
                table = self._table_from_concept(c, node, parent_cte)
                qry = pgast.SelectQueryNode()
                qry.fromlist.append(table)

                for aname, colname in cols:
                    if aname in c.pointers:
                        aref = atomrefs[aname]
                        if isinstance(aref, irast.AtomicRefSimple):
                            selexpr = pgast.FieldRefNode(
                                table=table, field=colname, origin=table,
                                origin_field=colname)

                        elif isinstance(aref, irast.SubgraphRef):
                            # Result of a rewrite

                            subquery = self.visit(aref.ref)

                            with self.context(
                                    TransformerContext.NEW_TRANSPARENT):
                                # Make sure subquery outerbonds are connected
                                # to the proper table, which is an element of
                                # this union.
                                for i, (outerref, innerref
                                        ) in enumerate(subquery.outerbonds):
                                    if outerref == node:
                                        fref = pgast.FieldRefNode(
                                            table=table, field=idcol,
                                            origin=table, origin_field=idcol)
                                        cmap = ctx.ir_set_field_map
                                        cmap[node] = {
                                            idcol: pgast.SelectExprNode(
                                                expr=fref)
                                        }

                                self._connect_subquery_outerbonds(
                                    subquery.outerbonds, subquery,
                                    inline=True)

                            selexpr = subquery

                            # Record this subquery in the computables map to
                            # signal that the value has been computed, which
                            # lets  all outer references to this subgraph to be
                            # pointed to a SelectExpr in parent_cte.
                            try:
                                computables = ctx.computable_map[
                                    node]
                            except KeyError:
                                computables = ctx.computable_map[
                                    node] = {}

                            computables[aref.name] = aref

                        else:
                            raise ValueError(
                                'unexpected node in atomrefs list: {!r}'.
                                format(aref))
                    else:
                        try:
                            coltype = coltypes[aname]
                        except KeyError:
                            target_ptr = concept.resolve_pointer(
                                schema, aname, look_in_children=True)
                            coltype = pg_types.pg_type_from_atom(
                                schema, target_ptr.target)
                            coltypes[aname] = coltype

                        selexpr = pgast.ConstantNode(value=None)
                        pgtype = pgast.TypeNode(name=coltype)
                        selexpr = pgast.TypeCastNode(
                            expr=selexpr, type=pgtype)

                    qry.targets.append(
                        pgast.SelectExprNode(expr=selexpr, alias=colname))

                selexpr = pgast.FieldRefNode(
                    table=table, field='std::__class__', origin=table,
                    origin_field='std::__class__')

                qry.targets.append(
                    pgast.SelectExprNode(
                        expr=selexpr, alias='std::__class__'))

                if cc:
                    # Make sure that all sets produced by each UNION member are
                    # disjoint so that there are no duplicates, and, most
                    # importantly, the shape of each row corresponds to the
                    # class.
                    get_concept_id = ctx.backend.get_concept_id
                    cc_ids = {get_concept_id(cls) for cls in cc}
                    cc_ids = [
                        pgast.ConstantNode(value=cc_id) for cc_id in cc_ids
                    ]
                    cc_ids = pgast.SequenceNode(elements=cc_ids)

                    qry.where = pgast.BinOpNode(
                        left=selexpr, right=cc_ids, op=ast.ops.NOT_IN)

                union_list.append(qry)

            if len(union_list) > 1:
                relation = pgast.SelectQueryNode(
                    edgedbnode=node, concepts=children, op=pgast.UNION)
                self._setop_from_list(relation, union_list, pgast.UNION)
            else:
                relation = union_list[0]

            relation.alias = ctx.genalias(hint=concept.name.name)

        else:
            table_schema_name, table_name = common.concept_name_to_table_name(
                concept.name, catenate=False)
            if concept.name.module == 'schema':
                # Redirect all queries to schema tables to edgedbss
                table_schema_name = 'edgedbss'

            relation = pgast.TableNode(
                name=table_name, schema=table_schema_name,
                concepts=frozenset({node.scls}),
                alias=ctx.genalias(hint=table_name),
                edgedbnode=node)
        return relation

    def _relation_from_concepts(self, node, parent_cte):
        return self._table_from_concept(node.scls, node, parent_cte)

    def _table_from_ptrcls(self, ptrcls):
        """Return a TableNode corresponding to a given Link."""
        table_schema_name, table_name = common.get_table_name(
            ptrcls, catenate=False)
        if ptrcls.normal_name().module == 'schema':
            # Redirect all queries to schema tables to edgedbss
            table_schema_name = 'edgedbss'
        return pgast.TableNode(
            name=table_name, schema=table_schema_name,
            alias=self.context.current.genalias(hint=table_name))

    def _relation_from_ptrcls(self, ptrcls, direction):
        """"Return a Relation subclass corresponding to a given ptr step.

        If `ptrcls` is a generic link, then a simple TableNode is returned,
        otherwise the return value may potentially be a UNION of all tables
        corresponding to a set of specialized links computed from the given
        `ptrcls` taking source inheritance into account.
        """
        ctx = self.context.current
        linkname = ptrcls.normal_name()
        endpoint = ptrcls.source

        if ptrcls.generic():
            # Generic links would capture the necessary set via inheritance.
            #
            relation = self._table_from_ptrcls(ptrcls)

        else:
            cols = []

            schema = ctx.schema

            union_list = []

            ptrclses = set()

            for source in {endpoint} | set(endpoint.descendants(schema)):
                # Sift through the descendants to see who has this link
                try:
                    src_ptrcls = source.pointers[linkname]
                except KeyError:
                    # This source has no such link, skip it
                    continue
                else:
                    if src_ptrcls in ptrclses:
                        # Seen this link already
                        continue
                    ptrclses.add(src_ptrcls)

                table = self._table_from_ptrcls(src_ptrcls)

                qry = pgast.SelectQueryNode()
                qry.fromlist.append(table)

                # Make sure all property references are pulled up properly
                for propname, colname in cols:
                    selexpr = pgast.FieldRefNode(
                        table=table, field=colname, origin=table,
                        origin_field=colname)
                    qry.targets.append(
                        pgast.SelectExprNode(expr=selexpr, alias=colname))

                union_list.append(qry)

            if len(union_list) == 0:
                # We've been given a generic link that none of the potential
                # sources contain directly, so fall back to general parent
                # table. #
                relation = self._table_from_ptrcls(ptrcls.bases[0])

            elif len(union_list) > 1:
                # More than one link table, generate a UNION clause.
                #
                relation = pgast.SelectQueryNode(op=pgast.UNION)
                self._setop_from_list(relation, union_list, pgast.UNION)

            else:
                # Just one link table, so returin it directly
                #
                relation = union_list[0].fromlist[0]

            relation.alias = ctx.genalias(hint=ptrcls.normal_name().name)

        return relation

    def _relation_from_link(self, link_node):
        ptrcls = link_node.ptrcls
        if ptrcls is None:
            ptrcls = self.context.current.schema.get('std::link')

        relation = self._relation_from_ptrcls(
            ptrcls, link_node.direction)
        relation.edgedbnode = link_node
        return relation

    def _extend_binop(self, binop, *exprs, op=ast.ops.AND, reversed=False):
        exprs = list(exprs)
        binop = binop or exprs.pop(0)

        for expr in exprs:
            if expr is not binop:
                if reversed:
                    binop = pgast.BinOpNode(right=binop, op=op, left=expr)
                else:
                    binop = pgast.BinOpNode(left=binop, op=op, right=expr)

        return binop

    def _is_composite_cast(self, expr):
        return (
            isinstance(expr, irast.TypeCast) and (
                isinstance(expr.type, irast.CompositeType) or (
                    isinstance(expr.type, tuple) and
                    isinstance(expr.type[1], irast.CompositeType))))
