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

from edgedb.server.pgsql import ast2 as pgast
from edgedb.server.pgsql import codegen2 as pgcodegen
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

        return qchunks, argmap, arg_index, type(qtree), tuple()

    def generic_visit(self, node, *, combine_results=None):
        raise NotImplementedError(
            'no IR compiler handler for {}'.format(node.__class__))

    def visit_SelectStmt(self, stmt):
        parent_ctx = self.context.current
        parent_rel = parent_ctx.rel

        with self.context.substmt():
            ctx = self.context.current

            parent_ctx.subquery_map[parent_rel][ctx.query] = {
                'linked': False,
                'rvar': None
            }

            # Process any substatments in the WITH block.
            self._process_explicit_substmts(stmt)

            if stmt.set_op:
                # Process the UNION/EXCEPT/INTERSECT operation
                with self.context.substmt():
                    larg = self.visit(stmt.set_op_larg)

                with self.context.substmt():
                    rarg = self.visit(stmt.set_op_rarg)

                set_op = pgast.PgSQLSetOperator(stmt.set_op)
                self._setop_from_list(ctx.query, [larg, rarg], set_op)
            else:
                # Process the result expression;
                self._process_selector(stmt.result, transform_output=True)

                # The WHERE clause
                if stmt.where:
                    with self.context.new():
                        self.context.current.location = 'where'
                        where = self.visit(stmt.where)

                    ctx.query.where_clause = where

                # The GROUP BY clause
                self._process_groupby(stmt.groupby)

            # The ORDER BY clause
            self._process_orderby(stmt.orderby)

            # The OFFSET clause
            if stmt.offset:
                ctx.query.limit_offset = self.visit(stmt.offset)

            # The LIMIT clause
            if stmt.limit:
                ctx.query.limit_count = self.visit(stmt.limit)

            # Make sure all sub-selects are linked according
            # to path matching logic...
            self._connect_subrels(ctx.query)

            # ..and give the parent query the opportunity to do the same.
            for cte in ctx.query.ctes:
                parent_ctx.subquery_map[parent_rel][cte.query] = {
                    'linked': False,
                    'rvar': None
                }

            return ctx.query

    def visit_Shape(self, expr):
        ctx = self.context.current

        my_elements = []
        attribute_map = []
        testref = None

        for i, e in enumerate(expr.elements):
            element = self.visit(e)

            ptr_name = e.rptr.ptrcls.shortname

            ptr_direction = e.rptr.direction or \
                s_pointers.PointerDirection.Outbound

            if ptr_direction == s_pointers.PointerDirection.Outbound:
                ptr_target = e.rptr.ptrcls.target
            else:
                ptr_target = e.rptr.ptrcls.source

            if isinstance(element, pgast.SelectStmt):
                if not e.rptr.ptrcls.singular(ptr_direction):
                    # Aggregate subquery results to keep correct
                    # cardinality.
                    element.target_list[0].val = pgast.FuncCall(
                        name='array_agg',
                        args=[element.target_list[0].val],
                        agg_order=element.sort_clause
                    )
                    element.sort_clause = []

            if ptr_name == 'std::id':
                testref = element

            attr_name = s_pointers.PointerVector(
                name=ptr_name.name, module=ptr_name.module,
                direction=ptr_direction, target=ptr_target.name,
                is_linkprop=isinstance(e.rptr.ptrcls, s_lprops.LinkProperty))

            attribute_map.append(attr_name)
            my_elements.append(element)

        if ctx.clsref_as_id:
            # DML statements want ``SELECT Object`` to return the object
            # identity.
            for i, a in enumerate(attribute_map):
                if (a.module, a.name) == ('std', 'id'):
                    result = my_elements[i]
                    break
            else:
                raise ValueError('cannot find id ptr in entitityref record')

            testref = None

        elif ctx.output_format == 'json':
            # In JSON mode we simply produce a JSONB object of
            # the shape record...
            keyvals = []
            for i, pgexpr in enumerate(my_elements):
                key = attribute_map[i]
                if isinstance(key, s_pointers.PointerVector):
                    if key.is_linkprop:
                        key = '@' + key.name
                    else:
                        key = key.name
                keyvals.append(pgast.Constant(val=key))
                keyvals.append(pgexpr)

            result = pgast.FuncCall(
                name='jsonb_build_object', args=keyvals)
        else:
            # In non-JSON mode the result is an anonymous record.
            result = pgast.RowExpr(args=my_elements)

        if testref is not None:
            # In case the object reference is NULL we want the
            # entire result to be NULL rather than a record containing
            # a series of NULLs.
            when_cond = pgast.NullTest(arg=testref)

            when_expr = pgast.CaseWhen(
                expr=when_cond,
                result=pgast.Constant(val=None)
            )

            result = pgast.CaseExpr(
                args=[when_expr],
                defresult=result)

        return result

    def visit_Constant(self, expr):
        ctx = self.context.current

        if expr.type and expr.type.name != 'std::null':
            const_type = self._schema_type_to_pg_type(expr.type)
        else:
            const_type = None

        if expr.expr:
            result = self.visit(expr.expr)
        else:
            val = expr.value
            index = None

            if expr.index is not None and not isinstance(expr.index, int):
                if expr.index in ctx.argmap:
                    index = list(ctx.argmap).index(expr.index)
                else:
                    ctx.argmap.add(expr.index)
                    index = len(ctx.argmap) - 1

                result = pgast.ParamRef(number=index)
            else:
                result = pgast.Constant(val=val)

        if const_type not in {None, 'bigint'}:
            result = pgast.TypeCast(
                arg=result,
                type_name=pgast.TypeName(
                    name=const_type
                )
            )

        return result

    def visit_TypeCast(self, expr):
        ctx = self.context.current

        if (isinstance(expr.expr, irast.BinOp) and
                isinstance(expr.expr.op,
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
                result = pgast.TypeCast(
                    arg=pg_expr,
                    type_name=pgast.TypeName(
                        name=elem_type,
                        array_bounds=[-1]))
            else:
                raise NotImplementedError(
                    '{} composite type is not supported '
                    'yet'.format(target_type.maintype))

        else:
            int_class = schema.get('std::int')
            target_type = schema.get(target_type.maintype)

            if (expr_type and expr_type is bool and
                    target_type.issubclass(int_class)):
                when_expr = pgast.CaseWhen(
                    expr=pg_expr, result=pgast.Constant(val=1))
                default = pgast.Constant(val=0)
                result = pgast.CaseExpr(
                    args=[when_expr], defresult=default)
            else:
                result = pgast.TypeCast(
                    arg=pg_expr,
                    type_name=pgast.TypeName(
                        name=pg_types.pg_type_from_atom(
                            schema, target_type, topbase=True)
                    )
                )

        return result

    def visit_IndexIndirection(self, expr):
        # Handle Expr[Index], where Expr may be std::str or array<T>.
        # For strings we translate this into substr calls, whereas
        # for arrays the native slice syntax is used.
        ctx = self.context.current

        is_string = False
        arg_type = irutils.infer_type2(expr.expr, ctx.schema)

        subj = self.visit(expr.expr)
        index = self.visit(expr.index)

        if isinstance(arg_type, s_atoms.Atom):
            b = arg_type.get_topmost_base()
            is_string = b.name == 'std::str'

        one = pgast.Constant(val=1)
        zero = pgast.Constant(val=0)

        when_cond = self._new_binop(
            lexpr=index, rexpr=zero, op=ast.ops.LT)

        index_plus_one = self._new_binop(
            lexpr=index, op=ast.ops.ADD, rexpr=one)

        if is_string:
            upper_bound = pgast.FuncCall(
                name='char_length', args=[subj])
        else:
            upper_bound = pgast.FuncCall(
                name='array_upper', args=[subj, one])

        neg_off = self._new_binop(
            lexpr=upper_bound, rexpr=index_plus_one, op=ast.ops.ADD)

        when_expr = pgast.CaseWhen(
            expr=when_cond, result=neg_off)

        index = pgast.CaseExpr(
            args=[when_expr], defresult=index_plus_one)

        if is_string:
            result = pgast.FuncCall(
                name='substr',
                args=[subj, index, one]
            )
        else:
            indirection = pgast.Indices(ridx=index)
            result = pgast.Indirection(
                arg=subj, indirection=[indirection])

        return result

    def visit_SliceIndirection(self, expr):
        # Handle Expr[Start:End], where Expr may be std::str or array<T>.
        # For strings we translate this into substr calls, whereas
        # for arrays the native slice syntax is used.

        ctx = self.context.current

        subj = self.visit(expr.expr)
        start = self.visit(expr.start)
        stop = self.visit(expr.stop)
        one = pgast.Constant(val=1)
        zero = pgast.Constant(val=0)

        is_string = False
        arg_type = irutils.infer_type2(expr.expr, ctx.schema)

        if isinstance(arg_type, s_atoms.Atom):
            b = arg_type.get_topmost_base()
            is_string = b.name == 'std::str'

        if is_string:
            upper_bound = pgast.FuncCall(
                name='char_length', args=[subj])
        else:
            upper_bound = pgast.FuncCall(
                name='array_upper', args=[subj, one])

        if isinstance(start, pgast.Constant) and start.val is None:
            lower = one
        else:
            lower = start

            when_cond = self._new_binop(
                lexpr=lower, rexpr=zero, op=ast.ops.LT)
            lower_plus_one = self._new_binop(
                lexpr=lower, rexpr=one, op=ast.ops.ADD)

            neg_off = self._new_binop(
                lexpr=upper_bound, rexpr=lower_plus_one, op=ast.ops.ADD)

            when_expr = pgast.CaseWhen(
                expr=when_cond, result=neg_off)
            lower = pgast.CaseExpr(
                args=[when_expr], defresult=lower_plus_one)

        if isinstance(stop, pgast.Constant) and stop.val is None:
            upper = upper_bound
        else:
            upper = stop

            when_cond = self._new_binop(
                lexpr=upper, rexpr=zero, op=ast.ops.LT)

            neg_off = self._new_binop(
                lexpr=upper_bound, rexpr=upper, op=ast.ops.ADD)

            when_expr = pgast.CaseWhen(
                expr=when_cond, result=neg_off)
            upper = pgast.CaseExpr(
                args=[when_expr], defresult=upper)

        if is_string:
            args = [subj, lower]

            if upper is not upper_bound:
                for_length = self._new_binop(
                    lexpr=upper, op=ast.ops.SUB, rexpr=lower)
                for_length = self._new_binop(
                    lexpr=for_length, op=ast.ops.ADD, rexpr=one)
                args.append(for_length)

            result = pgast.FuncCall(name='substr', args=args)

        else:
            indirection = pgast.Indices(
                lidx=lower, ridx=upper)
            result = pgast.Indirection(
                arg=subj, indirection=[indirection])

        return result

    def visit_SubstmtRef(self, expr):
        return self.visit(expr.stmt)

    def visit_Set(self, expr):
        ctx = self.context.current

        # Get the CTE for this Set
        source_cte = self._set_to_cte(expr)

        if ctx.location in {'where', 'exists'}:
            # When referred to in WHERE or as an argument to EXISTS(),
            # we want to wrap the set CTE into
            #    EXISTS(SELECT * FROM SetCTE WHERE SetCTE.expr)
            result = self._wrap_set_rel(expr, source_cte)
        else:
            # Otherwise we join the CTE directly into the current rel
            # and make its refs available in the path namespace.
            source_rvar = self._include_range(source_cte)

            if expr.expr:
                # For expression sets the result is the result
                # of the expression.
                result = pgast.ColumnRef(
                    name=[source_rvar.alias.aliasname, 'v']
                )
            else:
                # Otherwise it is a regular link reference.
                result = self._get_fieldref_for_set(expr)

        return result

    def _include_range(self, cte):
        """Ensure the *cte* is present in the from_clause of current rel.

        :param cte:
            The CTE node to join.

        :return:
            RangeVar representing the *cte* in the context of current rel.
        """
        ctx = self.context.current

        subrel = ctx.subquery_map[ctx.rel].get(cte)

        if subrel is None:
            # The cte has not been recorded as a sub-relation of this rel,
            # so make it so.
            rvar = pgast.RangeVar(
                relation=cte,
                alias=pgast.Alias(
                    aliasname=ctx.genalias(hint=getattr(cte, 'name'))
                )
            )

            ctx.subquery_map[ctx.rel][cte] = {
                'rvar': rvar,
                'linked': False
            }
        else:
            rvar = subrel['rvar']

        # Make sure that the path namespace of *cte* is mapped
        # onto the path namespace of the current rel.
        self._pull_path_namespace(target=ctx.rel, source=rvar)

        return rvar

    def _set_as_exists_op(self, ir_expr, pg_expr):
        # Make sure *pg_expr* is an EXISTS() expression
        # Set references inside WHERE are transformed into
        # EXISTS expressions in visit_Set.  For other
        # occurrences we do it here.
        if (isinstance(pg_expr, pgast.SubLink) and
                pg_expr.type == pgast.SubLinkType.EXISTS):
            result = pg_expr
        else:
            result = pgast.SubLink(
                type=pgast.SubLinkType.EXISTS,
                subselect=pg_expr)

        return result

    def _set_has_expr(self, ir_set):
        # Returns True if *ir_set* is produced by any expression.
        # Literally this means that ``Set.expr`` is any expression
        # except [NOT] EXISTS.
        return (
            ir_set.expr is not None and
            self._is_exists_ir(ir_set.expr)[0] is None
        )

    def _is_exists_ir(self, ir_expr):
        # Returns True if *ir_expr* is a ([NOT] EXISTS Expr) expression.
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

    def _wrap_set_rel(self, ir_set, set_rel):
        # For the *set_rel* relation representing the *ir_set*
        # return the following:
        #     [NOT] EXISTS (
        #         SELECT
        #         FROM <set_rel>
        #         [WHERE <set_rel>.v]
        #     )
        #
        ctx = self.context.current

        rvar = pgast.RangeVar(
            relation=set_rel,
            alias=pgast.Alias(
                aliasname=ctx.genalias(hint=set_rel.name + '_w')
            )
        )

        wrapper = pgast.SelectStmt(
            from_clause=[rvar]
        )

        self._pull_path_namespace(target=wrapper, source=rvar)

        if self._set_has_expr(ir_set):
            wrapper.where_clause = pgast.ColumnRef(
                name=[rvar.alias.aliasname, 'v']
            )
            not_exists = False
        else:
            exists_target, not_exists = self._is_exists_ir(ir_set.expr)
            if exists_target is not None:
                wrapper.where_clause = self._new_unop(
                    op=ast.ops.NOT,
                    expr=pgast.NullTest(
                        arg=wrapper.path_namespace[exists_target.path_id]
                    )
                )
            else:
                wrapper.where_clause = self._new_unop(
                    op=ast.ops.NOT,
                    expr=pgast.NullTest(
                        arg=wrapper.path_namespace[ir_set.path_id]
                    )
                )

        subrels = ctx.subquery_map[ctx.rel]
        subrels[wrapper] = {
            'rvar': rvar,
            'linked': False
        }

        wrapper = pgast.SubLink(
            type=pgast.SubLinkType.EXISTS,
            subselect=wrapper
        )

        if not_exists:
            wrapper = pgast.Expr(
                rexpr=wrapper,
                name=ast.ops.NOT,
                kind=pgast.ExprKind.OP
            )

        return wrapper

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
                ir_set.rptr.ptrcls.shortname.name
            )
        elif ir_set.expr is not None and len(ir_set.sources) == 1:
            src = list(ir_set.sources)[0]
            if src.rptr is not None:
                alias_hint = '{}_{}'.format(
                    src.rptr.source.scls.name.name,
                    src.rptr.ptrcls.shortname.name
                )
            else:
                alias_hint = src.scls.name.name
            alias_hint += '_expr'
        else:
            alias_hint = ir_set.scls.name.name

        stmt = pgast.SelectStmt(from_clause=fromlist)
        stmt.path_id = ir_set.path_id

        cte = pgast.CommonTableExpr(
            query=stmt,
            name=ctx.genalias(hint=str(alias_hint)),
        )

        ctx.ctemap[ir_set] = cte

        ir_sources = list(ir_set.sources)
        if not ir_sources:
            # If there are no explicit sources, check if this Set
            # is a non-starting part of a path.
            if ir_set.rptr is not None:
                ir_sources.append(ir_set.rptr.source)

        if not ir_sources and isinstance(ir_set.scls, s_atoms.Atom):
            # Atomic Sets cannot appear without a source superset.
            raise RuntimeError('unexpected atomic set without sources')

        sources = []
        for ir_source in ir_sources:
            source_rel = self._set_to_cte(ir_source)
            sources.append(source_rel)

        if sources:
            # Generate a flat JOIN list from the gathered sources
            # using path bonds for conditions.
            subrels = ctx.subquery_map[stmt]
            jtype = 'inner' if ir_set.source_conjunction else 'left'

            for source in sources:
                src_rvar = pgast.RangeVar(
                    relation=source,
                    alias=pgast.Alias(
                        aliasname=ctx.genalias(hint=source.name)
                    )
                )

                subrels[source] = {
                    'rvar': src_rvar,
                    'linked': True
                }

                self._pull_path_namespace(target=stmt, source=src_rvar)

                self._rel_join(stmt, src_rvar, type=jtype)

        if isinstance(ir_set.scls, s_concepts.Concept):
            id_field = common.edgedb_name_to_pg_name('std::id')

            set_rvar = self._range_for_set(ir_set, stmt)
            stmt.scls_rvar = set_rvar

            if not fromlist:
                # This is the root set, select directly from class table.
                fromlist.append(set_rvar)

            path_id = ir_set.path_id
            set_rvar.query.path_vars[path_id] = id_field
            set_rvar.query.path_bonds[path_id] = id_field

            id_set = self._get_ptr_set(ir_set, 'std::id')
            self._add_path_var_reference(stmt, id_set, path_id=path_id)

            stmt.path_bonds[path_id] = stmt.path_vars[path_id]

        else:
            set_rvar = None

        return_parent = ir_set.rptr is not None and not ir_set.expr

        if ir_set.rptr is not None:
            # This is the nth step in the path, where n > 1.
            # Translate pointer traversal into a join clause.

            ptrcls = ir_set.rptr.ptrcls

            path_rvar = fromlist[0]
            source_rel = path_rvar.relation

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, resolve_type=False, link_bias=False)

            if isinstance(ptrcls, s_lprops.LinkProperty):
                # Reference to a link property.
                self._join_mapping_rel(
                    stmt=source_rel.query, set_rvar=set_rvar, ir_set=ir_set)
                self._add_path_var_reference(source_rel, ir_set)

            elif ptr_info.table_type != 'concept':
                # This is a 1* or ** cardinality, join via a mapping relation.
                map_rvar = self._join_mapping_rel(
                    stmt=stmt, set_rvar=set_rvar, ir_set=ir_set)

                stmt.rptr_rvar = map_rvar
                return_parent = False

            elif isinstance(ir_set.scls, s_concepts.Concept):
                # Direct reference to another object.
                self._add_path_var_reference(source_rel, ir_set)
                stmt.path_namespace[ir_set.path_id] = pgast.ColumnRef(
                    name=[
                        path_rvar.alias.aliasname,
                        source_rel.query.path_vars[ir_set.path_id]
                    ]
                )

                self._join_inline_rel(
                    stmt=stmt, set_rvar=set_rvar, ir_set=ir_set)

                return_parent = False

            elif ir_set.expr is None:
                # The path step target is stored in the source's table,
                # so we need to make sure that rel is returning the column
                # ref we need.
                self._add_path_var_reference(source_rel, ir_set)

        if ir_set.expr:
            exist_expr, _ = self._is_exists_ir(ir_set.expr)

            if exist_expr is None:
                with self.context.new():
                    self.context.current.location = 'set_expr'
                    self.context.current.rel = stmt
                    set_expr = self.visit(ir_set.expr)
                    restarget = pgast.ResTarget(
                        val=set_expr,
                        name='v')
                    self._connect_subrels(stmt)

                stmt.target_list.append(restarget)
            else:
                return_parent = True

        if return_parent:
            source_rel = fromlist[0].relation
            ctx.ctemap[ir_set] = source_rel
            return source_rel
        else:
            root_query.ctes.append(cte)
            return cte

    def visit_BinOp(self, expr):
        ctx = self.context.current

        with self.context.new():
            op = expr.op
            if ctx.location == 'set_expr' and op in {ast.ops.AND, ast.ops.OR}:
                self.context.current.location = 'exists'

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
            result = pgast.FuncCall(
                name='edgedb.issubclass',
                args=[left, right])

            if expr.op == ast.ops.IS_NOT:
                result = self._new_unop(ast.ops.NOT, result)

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

                if isinstance(right.expr, pgast.Sequence):
                    right.expr = pgast.ArrayExpr(
                        elements=right.expr.elements)
                elif right.type == 'text[]':
                    left_type = irutils.infer_type2(
                        expr.left, ctx.schema)
                    if isinstance(left_type, s_obj.Class):
                        if isinstance(left_type, s_concepts.Concept):
                            left_type = left_type.pointers[
                                'std::id'].target
                        left_type = pg_types.pg_type_from_atom(
                            ctx.schema, left_type,
                            topbase=True)
                        right.type = left_type + '[]'

                right = pgast.FuncCall(
                    name=qual_func, args=[right])
            else:
                op = expr.op

            left_type = irutils.infer_type2(
                expr.left, ctx.schema)
            right_type = irutils.infer_type2(
                expr.right, ctx.schema)

            if left_type and right_type:
                if isinstance(left_type, s_obj.Class):
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

                if isinstance(right_type, s_obj.Class):
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
                            Constant) and right_type == 'text':
                        right.type = left_type
                    elif isinstance(
                            left, pgast.
                            Constant) and left_type == 'text':
                        left.type = right_type

                if (isinstance(right, pgast.Constant) and
                        op in {ast.ops.IS, ast.ops.IS_NOT}):
                    right.type = None

            result = self._new_binop(left, right, op=op)

        return result

    def visit_UnaryOp(self, expr):
        operand = self.visit(expr.expr)
        return pgast.Expr(name=expr.op, rexpr=operand, kind=pgast.ExprKind.OP)

    def visit_Sequence(self, expr):
        ctx = self.context.current

        elements = [
            self.visit(e) for e in expr.elements
        ]
        if expr.is_array:
            result = pgast.ArrayExpr(elements=elements)
        elif getattr(ctx, 'sequence_is_array', False):
            result = pgast.ImplicitRowExpr(args=elements)
        else:
            result = pgast.RowExpr(args=elements)

        return result

    def visit_ExistPred(self, expr):
        with self.context.new():
            self.context.current.location = 'exists'
            return self._set_as_exists_op(expr.expr, self.visit(expr.expr))

    def visit_TypeRef(self, expr):
        ctx = self.context.current

        data_backend = ctx.backend
        schema = ctx.schema

        if expr.subtypes:
            raise NotImplementedError()
        else:
            cls = schema.get(expr.maintype)
            concept_id = data_backend.get_concept_id(cls)
            result = pgast.TypeCast(
                arg=pgast.Constant(val=concept_id),
                type_name=pgast.TypeName(
                    name='uuid'
                )
            )

        return result

    def _connect_subrels(self, query):
        # For any subquery or CTE referred to by the *query*
        # generate the appropriate JOIN condition.  This also
        # populates the FROM list in *query*.
        #
        ctx = self.context.current

        rels = [
            (rel, info) for rel, info in ctx.subquery_map[query].items()
            if not info['linked']
        ]
        if not rels:
            return

        # Mark all rels as "connected" so that subsequent calls
        # of this function on the same *query* work.
        for _, info in rels:
            info['linked'] = True

        # Go through all CTE references and LEFT JOIN them
        # in *query* FROM.
        ctes = [info['rvar'] for rel, info in rels
                if isinstance(rel, pgast.CommonTableExpr)]
        for rvar in ctes:
            self._rel_join(query, rvar, type='left')

        # Go through the remaining subqueries and inject join conditions.
        for rel, _ in rels:
            if not isinstance(rel, pgast.CommonTableExpr):
                self._connect_subquery(rel, query)

    def _connect_subquery(self, subquery, parentquery):
        # Inject a WHERE condition corresponding to the full inner bond join
        # between the outer query and the subquery.
        cond = self._full_inner_bond_condition(
            subquery, parentquery)

        if cond is not None:
            subquery.where_clause = \
                self._extend_binop(subquery.where_clause, cond)

    def _process_explicit_substmts(self, ir_stmt):
        ctx = self.context.current

        if ir_stmt.substmts:
            for substmt in ir_stmt.substmts:
                with self.context.substmt():
                    cte = pgast.CommonTableExpr(
                        query=self.visit(substmt),
                        name=substmt.name
                    )
                ctx.query.ctes.append(cte)

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
                target = pgast.ResTarget(
                    name=None,
                    val=pgast.FuncCall(name='to_jsonb', args=[pgexpr]),
                )

                query.target_list.append(target)

        else:
            for pgexpr, alias in selexprs:
                target = pgast.ResTarget(name=alias, val=pgexpr)
                query.target_list.append(target)

    def _process_orderby(self, sorter):
        ctx = self.context.current

        query = ctx.query
        ctx.location = 'orderby'

        for expr in sorter:
            sortexpr = pgast.SortBy(
                node=self.visit(expr.expr),
                dir=expr.direction,
                nulls=expr.nones_order)
            query.sort_clause.append(sortexpr)

    def _process_groupby(self, grouper):
        ctx = self.context.current

        query = ctx.query
        ctx.location = 'groupby'

        for expr in grouper:
            sortexpr = self.visit(expr)
            query.group_clause.append(sortexpr)

    def _get_fieldref_for_set(self, ir_set):
        """Return an expression node corresponding to the specified atomic Set.

        Arguments:
            - ir_set: IR Set

        Return:
            An expression node representing a set of atom/schema
            values for the specified ir_set.
        """
        ctx = self.context.current

        try:
            ref = ctx.rel.path_namespace[ir_set.path_id]
        except KeyError:
            raise LookupError(
                f'could not resolve {ir_set.path_id} as a column reference '
                f'in context of {ctx.rel!r}')

        if ctx.in_aggregate:
            rptr = ir_set.rptr
            ptr_name = rptr.ptrcls.shortname

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
            pgtype = pgast.TypeName(name=pgtype)
            ref = pgast.TypeCast(arg=ref, type_name=pgtype)

        return ref

    def _new_binop(self, lexpr, rexpr, op):
        return pgast.Expr(
            kind=pgast.ExprKind.OP,
            name=op,
            lexpr=lexpr,
            rexpr=rexpr
        )

    def _new_unop(self, op, expr):
        return pgast.Expr(
            kind=pgast.ExprKind.OP,
            name=op,
            rexpr=expr
        )

    def _join_condition(self, left_refs, right_refs, op='='):
        if not isinstance(left_refs, tuple):
            left_refs = (left_refs, )
        if not isinstance(right_refs, tuple):
            right_refs = (right_refs, )

        condition = None
        for left_ref, right_ref in itertools.product(left_refs, right_refs):
            op = self._new_binop(left_ref, right_ref, op=op)
            condition = self._extend_binop(condition)

        return condition

    def _simple_join(self, left, right, key, type='inner', condition=None):
        if condition is None:
            left_refs = left.bonds(key)[-1]
            right_refs = right.bonds(key)[-1]
            condition = self._join_condition(left_refs, right_refs)

        join = pgast.Join(
            type=type, left=left, right=right, condition=condition)

        join.updatebonds(left)
        join.updatebonds(right)

        return join

    def _full_inner_bond_condition(self, left, right):
        condition = None

        for path_id, lref in left.path_namespace.items():
            if not isinstance(path_id[-1], s_concepts.Concept):
                # Rather a hack.
                continue

            try:
                rref = right.path_namespace[path_id]
            except KeyError:
                continue

            path_cond = self._new_binop(lref, rref, op='=')
            condition = self._extend_binop(condition, path_cond)

        return condition

    def _full_outer_bond_condition(self, query, right_rvar):
        condition = None

        for path_id, rname in right_rvar.path_bonds.items():
            try:
                lref = query.path_namespace[path_id]
            except KeyError:
                continue

            rref = pgast.ColumnRef(
                name=[right_rvar.alias.aliasname, rname]
            )

            path_cond = self._new_binop(lref, rref, op='=')
            condition = self._extend_binop(condition, path_cond)

        return condition

    def _rel_join(self, query, right_rvar, type='inner'):
        condition = self._full_outer_bond_condition(query, right_rvar)

        if condition is None:
            type = 'cross'

        if query.from_clause:
            query.from_clause[0] = pgast.JoinExpr(
                type=type, larg=query.from_clause[0],
                rarg=right_rvar, quals=condition)
        else:
            query.from_clause.append(right_rvar)

    def _pull_path_namespace(self, *, target, source, add_to_target_list=None):
        ctx = self.context.current

        if add_to_target_list is None:
            add_to_target_list = ctx.stmt != target

        for path_id, name in source.path_vars.items():
            if path_id in target.path_namespace:
                continue

            ref = pgast.ColumnRef(
                name=[source.alias.aliasname, name]
            )

            target.path_namespace[path_id] = ref

            if add_to_target_list:
                alias = ctx.genalias(hint=name)

                if isinstance(target, pgast.DML):
                    if len(path_id) > 1:
                        lname = path_id[-2][0].shortname
                    else:
                        lname = 'std::id'

                    colname = common.edgedb_name_to_pg_name(lname)

                    ref = pgast.ColumnRef(
                        name=[target.relation.alias.aliasname, colname]
                    )

                    target.returning_list.append(
                        pgast.ResTarget(
                            name=alias,
                            val=ref
                        )
                    )
                else:
                    target.target_list.append(
                        pgast.ResTarget(
                            name=alias,
                            val=ref
                        )
                    )

                target.path_vars[path_id] = alias

                if isinstance(path_id[-1], s_concepts.Concept):
                    target.path_bonds[path_id] = alias

    def _join_mapping_rel(self, *, stmt, set_rvar, ir_set):
        fromexpr = stmt.from_clause[0]

        tip_pathvar = ir_set.pathvar if ir_set else None

        link = ir_set.rptr
        if isinstance(link.ptrcls, s_lprops.LinkProperty):
            link = link.source.rptr

        linkmap_key = link.ptrcls, link.direction, link.source, tip_pathvar

        try:
            # The same link map must not be joined more than once,
            # otherwise the cardinality of the result set will be wrong.
            #
            map_rvar, map_join = stmt.ptr_rvar_map[linkmap_key]
        except KeyError:
            map_rvar = self._range_for_pointer(link)
            map_join = None

        # Set up references according to link direction
        #
        src_col = common.edgedb_name_to_pg_name('std::source')
        source_ref = pgast.ColumnRef(
            name=[map_rvar.alias.aliasname, src_col])

        tgt_col = common.edgedb_name_to_pg_name('std::target')
        target_ref = pgast.ColumnRef(
            name=[map_rvar.alias.aliasname, tgt_col])

        valent_bond = stmt.path_namespace[link.source.path_id]
        forward_bond = self._new_binop(valent_bond, source_ref, op='=')
        backward_bond = self._new_binop(valent_bond, target_ref, op='=')

        if link.direction == s_pointers.PointerDirection.Inbound:
            map_join_cond = backward_bond
        else:
            map_join_cond = forward_bond

        if map_join is None:
            # Join link relation to source relation
            #
            map_join = pgast.JoinExpr(
                larg=fromexpr,
                rarg=map_rvar,
                type='inner',
                quals=map_join_cond
            )

            stmt.ptr_rvar_map[linkmap_key] = map_rvar, map_join

        if isinstance(ir_set.scls, s_concepts.Concept):
            # Join the target relation, if we have it
            target_range_bond = pgast.ColumnRef(
                name=[set_rvar.alias.aliasname,
                      set_rvar.path_vars[ir_set.path_id]]
            )

            if link.direction == s_pointers.PointerDirection.Inbound:
                map_tgt_ref = source_ref
            else:
                map_tgt_ref = target_ref

            cond_expr = self._new_binop(map_tgt_ref, target_range_bond, op='=')

            # We use inner join for target relations to make sure this join
            # relation is not producing dangling links, either as a result
            # of partial data, or query constraints.
            #
            if map_join.rarg is None:
                map_join.rarg = set_rvar
                map_join.quals = cond_expr
                map_join.type = 'inner'

            else:
                pre_map_join = map_join.copy()
                new_map_join = pgast.JoinExpr(
                    type='inner',
                    larg=pre_map_join,
                    rarg=set_rvar,
                    quals=cond_expr)
                map_join.copyfrom(new_map_join)

        stmt.from_clause[0] = map_join

        return map_rvar

    def _join_inline_rel(self, *, stmt, set_rvar, ir_set):
        id_col = common.edgedb_name_to_pg_name('std::id')
        src_ref = stmt.path_namespace[ir_set.path_id]
        tgt_ref = pgast.ColumnRef(
            name=[set_rvar.alias.aliasname, id_col]
        )

        fromexpr = stmt.from_clause[0]

        cond_expr = self._new_binop(src_ref, tgt_ref, op='=')

        stmt.from_clause[0] = pgast.JoinExpr(
            type='inner',
            larg=fromexpr,
            rarg=set_rvar,
            quals=cond_expr)

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

        if parent_qry is None:
            parent_qry = pgast.SelectStmt()

        parent_qry.op = op
        parent_qry.all = True

        for i in range(nq):
            parent_qry.larg = oplist[i]
            if i == nq - 2:
                parent_qry.rarg = oplist[i + 1]
                break
            else:
                parent_qry.rarg = pgast.SelectQuery(op=op, all=True)
                parent_qry = parent_qry.rarg

        return parent_qry

    def _add_path_var_reference(self, rel, ir_set, path_id=None, *,
                                add_to_target_list=None):
        ctx = self.context.current
        id_field = common.edgedb_name_to_pg_name('std::id')

        if isinstance(rel, pgast.CommonTableExpr):
            rel = rel.query

        if path_id is None:
            path_id = ir_set.path_id

        try:
            return rel.path_namespace[path_id]
        except KeyError:
            pass

        rptr = ir_set.rptr
        ptrcls = rptr.ptrcls
        ptrname = ptrcls.shortname

        if isinstance(ptrcls, s_lprops.LinkProperty):
            source = rptr.source.rptr.ptrcls
            rel_rvar = rel.rptr_rvar
        else:
            source = rptr.source.scls
            rel_rvar = rel.scls_rvar

        if rel_rvar is None:
            raise RuntimeError(
                f'{rel} is missing the relation context for {ir_set.path_id}')

        colname = None

        if isinstance(rel_rvar.relation, pgast.CommonTableExpr):
            # If this is another query, need to make sure the ref
            # is there too.
            source_rel = rel_rvar.relation.query
            self._add_path_var_reference(source_rel, ir_set, path_id)
            colname = source_rel.path_vars[path_id]

        if colname is None:
            colname = common.edgedb_name_to_pg_name(ptrname)

        schema = ctx.schema

        ref_map = {
            n: [rel_rvar]
            for n, p in source.pointers.items() if p.atomic()
        }
        joined_atomref_sources = {source: rel_rvar}

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
                if s in joined_atomref_sources:
                    continue

                src_rvar_pid = rel_rvar.query.path_id
                src_rvar = self._range_for_concept(s, rel)
                src_rvar.query.path_id = src_rvar_pid
                src_rvar.path_vars[src_rvar_pid] = id_field
                src_rvar.path_bonds[src_rvar_pid] = id_field

                self._rel_join(rel, src_rvar, type='left')

                joined_atomref_sources[s] = src_rvar

            ref_map[ptrname] = atomref_tables = [
                joined_atomref_sources[c] for c in sources
            ]

        fieldrefs = [
            pgast.ColumnRef(name=[atomref_table.alias.aliasname, colname])
            for atomref_table in atomref_tables
        ]

        alias = ctx.genalias(
            hint='{}_{}'.format(source.name.name, ptrname.name))

        # If the required atom column was defined in multiple
        # descendant tables and there is no common parent with
        # this column, we'll have to coalesce fieldrefs to all tables.
        #
        if len(fieldrefs) > 1:
            refexpr = pgast.CoalesceExpr(args=fieldrefs)
        else:
            refexpr = fieldrefs[0]

        restarget = pgast.ResTarget(name=alias, val=refexpr)

        if add_to_target_list is None:
            add_to_target_list = ctx.stmt != rel

        if add_to_target_list:
            if hasattr(rel, 'returning_list'):
                # This is a DML statement
                rel.returning_list.append(restarget)
            else:
                rel.target_list.append(restarget)

        rel.path_namespace[path_id] = refexpr
        rel.path_vars[path_id] = alias

        if ir_set.path_id != path_id:
            rel.path_namespace[ir_set.path_id] = refexpr
            rel.path_vars[ir_set.path_id] = alias

        return refexpr

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
                    binop = self._new_binop(rexpr=binop, op=op, lexpr=expr)
                else:
                    binop = self._new_binop(lexpr=binop, op=op, rexpr=expr)

        return binop

    def _is_composite_cast(self, expr):
        return (
            isinstance(expr, irast.TypeCast) and (
                isinstance(expr.type, irast.CompositeType) or (
                    isinstance(expr.type, tuple) and
                    isinstance(expr.type[1], irast.CompositeType))))
