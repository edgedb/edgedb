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
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import codegen as pgcodegen
from edgedb.server.pgsql import common
from edgedb.server.pgsql import types as pg_types
from edgedb.server.pgsql import exceptions as pg_errors

from edgedb.lang.common import ast, markup
from edgedb.lang.common.debug import debug


from .context import TransformerContext
from . import dbobj
from . import dml
from . import func


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


class IRCompiler(ast.visitor.NodeVisitor,
                 dbobj.IRCompilerDBObjects,
                 dml.IRCompilerDMLSupport,
                 func.IRCompilerFunctionSupport):
    def __init__(self, **kwargs):
        self.context = None
        super().__init__(**kwargs)

    @property
    def memo(self):
        if self.context is not None:
            return self.context.current.memo
        else:
            return self._memo

    @debug
    def transform(self, query, backend, schema, output_format=None):
        try:
            # Transform to sql tree
            self.context = TransformerContext()
            ctx = self.context.current
            ctx.memo = self._memo
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
                is_linkprop=isinstance(e.rptr.ptrcls, s_lprops.LinkProperty))

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

        return result

    def visit_TypeCast(self, expr):
        ctx = self.context.current

        if (isinstance(expr.expr, irast.BinOp)
                and isinstance(expr.expr.op,
                               (ast.ops.ComparisonOperator,
                                ast.ops.TypeCheckOperator))):
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
        arg_type = irutils.infer_type2(expr.expr, ctx.schema)

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
        arg_type = irutils.infer_type2(expr.expr, ctx.schema)

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
        inverted = False

        rel = ctx.rel

        source_cte = self._set_to_cte(expr)

        if ctx.location == 'where':
            wrapped_source_cte = pgast.SelectQueryNode(
                fromlist=[
                    pgast.FromExprNode(
                        expr=source_cte
                    )
                ]
            )

            if self._set_has_expr(expr):
                wrapped_source_cte.where = pgast.FieldRefNode(
                    table=source_cte,
                    field='v'
                )
            else:
                exists_set, inverted = self._is_exists_ir(expr.expr)

            self._pull_fieldrefs(wrapped_source_cte, source_cte)

            source_cte = wrapped_source_cte

        subrels = ctx.subquery_map[rel]
        if source_cte not in subrels:
            subrels[source_cte] = False

        if ctx.location == 'where':
            result = pgast.ExistsNode(expr=source_cte)
            if inverted:
                result = pgast.UnaryOpNode(
                    operand=result,
                    op=ast.ops.NOT
                )
        else:
            if isinstance(expr.scls, s_atoms.Atom):
                if expr.expr:
                    result = pgast.FieldRefNode(table=source_cte, field='v')
                else:
                    result = self._get_fieldref_for_set(expr)
            else:
                result = source_cte.bonds(source_cte.edgedbnode.path_id)[0]

        return result

    def visit_BinOp(self, expr):
        ctx = self.context.current

        if isinstance(expr.op, ast.ops.TypeCheckOperator):
            cl = self._get_ptr_set(expr.left, 'std::__class__')
            left = self.visit(self._get_ptr_set(cl, 'std::id'))
        else:
            left = self.visit(expr.left)

        if expr.op in (ast.ops.IN, ast.ops.NOT_IN) \
                and isinstance(expr.right, irast.Sequence):
            with self.context.new():
                self.context.current.sequence_is_array = True
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
                    left_type = irutils.infer_type2(
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

            left_type = irutils.infer_type2(
                expr.left, ctx.schema)
            right_type = irutils.infer_type2(
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

            if ctx.location == 'set_expr' and op in {ast.ops.AND, ast.ops.OR}:
                left = self._massage_binop_operand(expr.left, left)
                right = self._massage_binop_operand(expr.right, right)

            result = pgast.BinOpNode(
                op=op, left=left, right=right)

        return result

    def _massage_binop_operand(self, ir_expr, pg_expr):
        ctx = self.context.current

        exists_set, inverted = self._is_exists_ir(ir_expr)

        if isinstance(pg_expr, pgast.ExistsNode):
            result = pg_expr
        else:
            wrapper = pgast.SelectQueryNode(
                fromlist=[
                    pgast.FromExprNode(
                        expr=pg_expr.table
                    )
                ]
            )

            if self._set_has_expr(ir_expr):
                wrapper.where = pgast.FieldRefNode(
                    table=pg_expr.table,
                    field='v'
                )

            self._pull_fieldrefs(wrapper, pg_expr.table)
            subrels = ctx.subquery_map[ctx.rel]
            subrels[wrapper] = False
            subrels.pop(pg_expr.table, None)

            result = pgast.ExistsNode(
                expr=wrapper
            )

            if inverted:
                # EXISTS == NOT IS NULL
                result = pgast.UnaryOpNode(
                    operand=result,
                    op=ast.ops.NOT
                )

        return result

    def _set_has_expr(self, ir_set):
        return (
            ir_set.expr is not None and
            self._is_exists_ir(ir_set.expr)[0] is None
        )

    def _is_exists_ir(self, ir_expr):
        if isinstance(ir_expr, irast.Set):
            ir_expr = ir_expr.expr

        if isinstance(ir_expr, irast.ExistPred):
            return ir_expr.expr, False
        elif isinstance(ir_expr, irast.UnaryOp):
            ex_set, inverted = self._is_exists_ir(ir_expr.expr)
            if ex_set is not None:
                return ex_set, not inverted
            else:
                return None, None
        else:
            return None, None

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
            result = pgast.RowExprNode(args=elements)

        return result

    def visit_ExistPred(self, expr):
        ctx = self.context.current

        with self.context.new():
            result = self.visit(expr.expr)

        if not isinstance(result, pgast.ExistsNode):
            if isinstance(result, pgast.FieldRefNode):
                wrapper = pgast.SelectQueryNode(
                    fromlist=[
                        pgast.FromExprNode(
                            expr=result.table
                        )
                    ]
                )

                self._pull_fieldrefs(wrapper, result.table)
                subrels = ctx.subquery_map[ctx.rel]
                subrels[wrapper] = False
                subrels.pop(result.table, None)

                result = pgast.ExistsNode(
                    expr=wrapper
                )
            else:
                result = pgast.ExistsNode(expr=result)

        return result

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

        with self.context.subquery():
            ctx = self.context.current
            subrels = parent_ctx.subquery_map[parent_rel]
            subrels[ctx.query] = False

            if stmt.substmts:
                for substmt in stmt.substmts:
                    with self.context.subquery():
                        ctx.rel = ctx.query = pgast.CTENode()
                        cte = self.visit(substmt)
                        cte.alias = substmt.name
                    ctx.query.ctes.add(cte)
                    ctx.explicit_cte_map[substmt] = cte

            if stmt.set_op:
                with self.context.subquery():
                    ctx.rel = ctx.query = pgast.SelectQueryNode()
                    larg = self.visit(stmt.set_op_larg)

                with self.context.subquery():
                    ctx.rel = ctx.query = pgast.SelectQueryNode()
                    rarg = self.visit(stmt.set_op_rarg)

                set_op = pgast.PgSQLSetOperator(stmt.set_op)
                self._setop_from_list(ctx.query, [larg, rarg], set_op)

            self._process_selector(stmt.result, transform_output=True)

            if stmt.where:
                with self.context.new():
                    self.context.current.location = 'where'
                    where = self.visit(stmt.where)

                ctx.query.where = where

            self._process_orderby(stmt.orderby)

            self._process_groupby(stmt.groupby)

            if stmt.offset:
                ctx.query.offset = self.visit(stmt.offset)

            if stmt.limit:
                ctx.query.limit = self.visit(stmt.limit)

            self._connect_subrels(ctx.query)

            return ctx.query

    def _connect_subrels(self, query):
        # For any subquery or CTE referred to by the *query*
        # generate the appropriate JOIN condition.  This also
        # populates the FROM list in *query*.
        #
        ctx = self.context.current

        rels = [
            rel for rel, connected in ctx.subquery_map[query].items()
            if not connected
        ]
        if not rels:
            return

        # Mark all rels as "connected" so that subsequent calls
        # of this function on the same *query* work.
        for rel in rels:
            ctx.subquery_map[query][rel] = True

        if query.fromlist:
            fromexpr = query.fromlist[0].expr
        else:
            query.fromlist.append(pgast.FromExprNode(expr=None))
            fromexpr = None

        # Go through all CTE references and LEFT JOIN them
        # in *query* FROM.
        ctes = [rel for rel in rels if isinstance(rel, pgast.CTENode)]
        for rel in ctes:
            if fromexpr is None:
                fromexpr = rel
            else:
                fromexpr = self._rel_join(fromexpr, rel, type='left')

            # Make sure that all bonds received by joining the CTEs
            # are available for JOIN condition injection in the
            # subqueries below.
            # Make sure not to pollute the top-level query target list.
            self._pull_fieldrefs(
                query, rel, add_to_selector=query != ctx.query)

        query.fromlist[0].expr = fromexpr

        field_map = query.concept_node_map

        subq = [rel for rel in rels
                if not isinstance(rel, pgast.CTENode) and rel.fromlist]

        for rel in subq:
            innerrel = rel.fromlist[0].expr
            fromlist = {f.expr for f in innerrel.fromlist}
            fromlist.add(innerrel)

            for path_id, fieldref in innerrel._bonds.items():
                if path_id not in field_map:
                    continue

                lref = fieldref[0]
                rref = field_map[path_id].expr

                if isinstance(lref, pgast.FieldRefNode):
                    left_id = (lref.table, lref.field)
                else:
                    left_id = lref

                if isinstance(rref, pgast.FieldRefNode):
                    right_id = (rref.table, rref.field)
                else:
                    right_id = rref

                if left_id != right_id:
                    bond_cond = pgast.BinOpNode(
                        op='=', left=lref, right=rref)
                    rel.where = self._extend_binop(
                        rel.where, bond_cond)

                    if isinstance(lref, pgast.FieldRefNode):
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

        with self.context.new():
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
                direction=expr.direction,
                nulls_order=expr.nones_order)
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

    def _rel_join(self, left, right, type='inner'):
        condition = None

        for path_id, fieldref in left._bonds.items():
            lref = fieldref[-1]
            try:
                rref = right._bonds[path_id][-1]
            except KeyError:
                continue

            if (lref.table, lref.field) != (rref.table, rref.field):
                bond_cond = pgast.BinOpNode(
                    op='=', left=lref, right=rref)
                condition = self._extend_binop(condition, bond_cond)

        if condition is None:
            type = 'cross'

        join = pgast.JoinNode(
            type=type, left=left, right=right, condition=condition)

        join.updatebonds(left)
        join.updatebonds(right)

        return join

    def _pull_fieldrefs(self, target_rel, source_rel, add_to_selector=True):
        for path_id, ref in source_rel.concept_node_map.items():
            refexpr = pgast.FieldRefNode(table=source_rel, field=ref.alias)
            fieldref = pgast.SelectExprNode(expr=refexpr, alias=ref.alias)

            if path_id not in target_rel.concept_node_map:
                if add_to_selector:
                    target_rel.targets.append(fieldref)

                target_rel.concept_node_map[path_id] = fieldref

                if isinstance(path_id[-1], s_concepts.Concept):
                    bondref = pgast.FieldRefNode(
                        table=target_rel, field=ref.alias)
                    target_rel.addbond(path_id, bondref)

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
        source_ref = pgast.FieldRefNode(table=map_rel, field=src_col)

        tgt_col = common.edgedb_name_to_pg_name('std::target')
        target_ref = pgast.FieldRefNode(table=map_rel, field=tgt_col)

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
                table=scls_rel, field=id_field)

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

    def _join_inline_rel(self, *, ir_set, cte, join, scls_rel, join_field):
        id_field = common.edgedb_name_to_pg_name('std::id')
        source = join.bonds(ir_set.rptr.source.path_id)[-1]

        source_ref_field = pgast.FieldRefNode(
            table=source.table, field=join_field)

        target_ref_field = pgast.FieldRefNode(
            table=scls_rel, field=id_field)

        cond_expr = pgast.BinOpNode(
            left=source_ref_field, op='=', right=target_ref_field)

        new_join = self._simple_join(
            join, scls_rel,
            ir_set.path_id, type='inner', condition=cond_expr)

        return new_join

    def _set_to_cte(self, ir_set):
        """Generate a Common Table Expression for a given IR Set.

        @param ir_set: IR Set node.
        """
        ctx = self.context.current

        root_query = ctx.query

        cte = ctx.ctemap.get(ir_set)
        if cte is not None:
            # Already have a CTE for this Set.
            return cte

        fromlist = []
        if ir_set.rptr is not None:
            alias_hint = '{}_{}'.format(
                ir_set.rptr.source.scls.name.name,
                ir_set.rptr.ptrcls.normal_name().name
            )
        elif ir_set.expr is not None and len(ir_set.sources) == 1:
            src = list(ir_set.sources)[0]
            if src.rptr is not None:
                alias_hint = '{}_{}'.format(
                    src.rptr.source.scls.name.name,
                    src.rptr.ptrcls.normal_name().name
                )
            else:
                alias_hint = src.scls.name.name
            alias_hint += '_expr'
        else:
            alias_hint = ir_set.scls.name.name

        cte = pgast.CTENode(
            concepts=frozenset({ir_set.scls}),
            alias=ctx.genalias(hint=str(alias_hint)),
            edgedbnode=ir_set,
            fromlist=fromlist)

        ctx.ctemap[ir_set] = cte

        sources = [self._set_to_cte(s) for s in ir_set.sources]

        if not sources:
            if ir_set.rptr is not None:
                sources = [self._set_to_cte(ir_set.rptr.source)]

        if not sources:
            if isinstance(ir_set.scls, s_atoms.Atom):
                # Atomic Sets cannot appear without a source superset.
                raise RuntimeError('unexpected atomic set without sources')

        if sources:
            fromexpr = None

            subrels = ctx.subquery_map[cte]
            jtype = 'inner' if ir_set.source_conjunction else 'full'

            for source in sources:
                subrels[source] = True

                if fromexpr is None:
                    fromexpr = source
                else:
                    fromexpr = self._rel_join(fromexpr, source, type=jtype)

                self._pull_fieldrefs(cte, source)

            fromlist.append(pgast.FromExprNode(expr=fromexpr))

        if isinstance(ir_set.scls, s_concepts.Concept):
            id_field = common.edgedb_name_to_pg_name('std::id')

            cte.scls_rel = scls_rel = self._relation_from_concepts(ir_set, cte)

            if not fromlist:
                # This is the root set, select directly from class table.
                fromlist.append(pgast.FromExprNode(expr=scls_rel))

            bond = pgast.FieldRefNode(
                table=scls_rel, field=id_field)

            scls_rel.addbond(ir_set.path_id, bond)

            id_set = self._get_ptr_set(ir_set, 'std::id')
            id_alias = self._add_inline_atom_ref(cte, id_set, ir_set.path_id)

            bond = pgast.FieldRefNode(table=cte, field=id_alias)
            cte.addbond(ir_set.path_id, bond)

        else:
            scls_rel = None

        if ir_set.rptr is not None:
            # This is the nth step in the path, where n > 1.
            # Translate pointer traversal into a join clause.

            ptrcls = ir_set.rptr.ptrcls

            fromnode = fromlist[0]
            parent_cte = fromnode.expr
            join = parent_cte
            map_rel = None

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, resolve_type=False)

            if isinstance(ptrcls, s_lprops.LinkProperty):
                # Reference to singular atom.
                self._add_inline_atom_ref(parent_cte, ir_set)

                if not ir_set.expr:
                    ctx.ctemap[ir_set] = parent_cte
                    return parent_cte

            elif ptr_info.table_type != 'concept':
                # This is a 1* or ** cardinality, join via a mapping relation.
                join, map_rel = self._join_mapping_rel(
                    ir_set=ir_set, cte=cte, join=join, scls_rel=scls_rel)

            elif isinstance(ir_set.scls, s_concepts.Concept):
                lalias = self._add_inline_atom_ref(parent_cte, ir_set)

                # Direct reference to another object.
                join = self._join_inline_rel(
                    ir_set=ir_set, cte=cte, join=join, scls_rel=scls_rel,
                    join_field=lalias)

            else:
                # Reference to singular atom.
                self._add_inline_atom_ref(parent_cte, ir_set)

                if not ir_set.expr:
                    ctx.ctemap[ir_set] = parent_cte
                    return parent_cte

            cte.rptr_rel = map_rel
            fromnode.expr = join

        if ir_set.expr:
            exist_expr, _ = self._is_exists_ir(ir_set.expr)

            if exist_expr is None:
                with self.context.new():
                    self.context.current.location = 'set_expr'
                    self.context.current.rel = cte
                    set_expr = self.visit(ir_set.expr)
                    selectnode = pgast.SelectExprNode(
                        expr=set_expr,
                        alias='v')
                    self._connect_subrels(cte)

                cte.targets.append(selectnode)
            else:
                ctx.ctemap[ir_set] = fromlist[0].expr
                return fromlist[0].expr

        # Finally, attach the CTE to the parent query.
        root_query.ctes.add(cte)

        return cte

    def _get_ptr_set(self, source_set, ptr_name):
        ctx = self.context.current

        schema = ctx.schema
        scls = source_set.scls
        ptrcls = scls.resolve_pointer(schema, ptr_name)

        path_id = irutils.LinearPath(source_set.path_id)
        path_id.add(ptrcls, s_pointers.PointerDirection.Outbound,
                    ptrcls.target)

        target_set = irast.Set()
        target_set.scls = ptrcls.target
        target_set.path_id = path_id

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
            pgast.FieldRefNode(table=atomref_table, field=colname)
            for atomref_table in atomref_tables
        ]
        alias = rel.alias + ('_' + ctx.genalias(hint=str(ptrname)))

        # If the required atom column was defined in multiple
        # descendant tables and there is no common parent with
        # this column, we'll have to coalesce fieldrefs to all tables.
        #
        if len(fieldrefs) > 1:
            refexpr = pgast.FunctionCallNode(name='coalesce', args=fieldrefs)
        else:
            refexpr = fieldrefs[0]

        selectnode = pgast.SelectExprNode(expr=refexpr, alias=alias)
        rel.targets.append(selectnode)

        rel.concept_node_map[path_id] = selectnode

        # Record atom references in the global map in case they have to
        # be pulled up later
        #
        refexpr = pgast.FieldRefNode(table=rel, field=selectnode.alias)
        selectnode = pgast.SelectExprNode(expr=refexpr, alias=selectnode.alias)
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
