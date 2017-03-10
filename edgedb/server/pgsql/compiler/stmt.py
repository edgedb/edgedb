##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details asyncpg.
##


import functools

from edgedb.lang.common import exceptions as edgedb_error

from edgedb.lang.ir import ast as irast
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
from edgedb.server.pgsql import optimizer as pg_opt

from edgedb.lang.common import ast
from edgedb.lang.common import debug

from . import context
from .context import CompilerContext
from . import expr as expr_compiler
from . import dml

from .expr import ResTargetList


class LazyPathVarRef:
    def __init__(self, getter, source, path_id):
        self.path_id = path_id
        self.source = source
        self.getter = getter
        self.grouped = False
        self._ref = None

    def get(self):
        if self._ref is None:
            ref = self.getter(self.source, self.path_id)
            if self.grouped:
                ref = pgast.ColumnRef(
                    name=ref.name,
                    nullable=ref.nullable,
                    grouped=self.grouped
                )
            self._ref = ref

        return self._ref

    def __repr__(self):
        return f'<LazyPathVarRef {self.path_id} source={self.source!r}>'


class IRCompiler(expr_compiler.IRCompilerBase,
                 dml.IRCompilerDMLSupport):
    def __init__(self, **kwargs):
        self.context = None
        super().__init__(**kwargs)

    def transform_to_sql_tree(self, ir_expr, *, schema, backend=None,
                              output_format=None, ignore_shapes=False):
        try:
            # Transform to sql tree
            self.context = CompilerContext()
            ctx = self.context.current
            ctx.memo = self._memo
            ctx.backend = backend
            ctx.schema = schema
            ctx.output_format = output_format
            if ignore_shapes:
                ctx.expr_exposed = False
            qtree = self.visit(ir_expr)

            if debug.flags.edgeql_compile:  # pragma: no cover
                debug.header('SQL Tree')
                debug.dump(qtree)

        except Exception as e:  # pragma: no cover
            try:
                args = [e.args[0]]
            except (AttributeError, IndexError):
                args = []
            err = expr_compiler.IRCompilerInternalError(*args)
            err_ctx = expr_compiler.IRCompilerErrorContext(tree=ir_expr)
            edgedb_error.replace_context(err, err_ctx)
            raise err from e

        return qtree

    def transform(self, ir_expr, *, schema, backend=None, output_format=None,
                  ignore_shapes=False, optimize=False):
        qtree = self.transform_to_sql_tree(
            ir_expr, schema=schema, backend=backend,
            output_format=output_format, ignore_shapes=ignore_shapes)

        if optimize:
            opt = pg_opt.Optimizer()
            qtree = opt.optimize(qtree)

        argmap = self.context.current.argmap

        # Generate query text
        codegen = self._run_codegen(qtree)
        qchunks = codegen.result
        arg_index = codegen.param_index

        if (debug.flags.edgeql_compile or
                debug.flags.edgeql_optimize):  # pragma: no cover
            debug.header('SQL')
            debug.dump_code(''.join(qchunks), lexer='sql')

        return qchunks, argmap, arg_index, type(qtree), tuple()

    def generic_visit(self, node, *, combine_results=None):
        raise NotImplementedError(
            'no IR compiler handler for {}'.format(node.__class__))

    def visit_SelectStmt(self, stmt):
        parent_ctx = self.context.current

        with self.context.substmt() as ctx:
            if ctx.toplevel_stmt is None:
                ctx.toplevel_stmt = ctx.stmt

            if stmt.aggregated_scope:
                ctx.aggregated_scope = stmt.aggregated_scope

            ctx.stmtmap[stmt] = ctx.stmt
            ctx.stmt_hierarchy[ctx.stmt] = parent_ctx.stmt

            query = ctx.query
            query.view_path_id_map = parent_ctx.view_path_id_map.copy()

            # Process the result expression;
            self._process_selector(stmt.result)

            # The WHERE clause
            if stmt.where:
                with self.context.new() as ctx1:
                    ctx1.clause = 'where'
                    ctx1.expr_exposed = False
                    query.where_clause = self.visit(stmt.where)

            simple_wrapper = irutils.is_simple_wrapper(stmt)

            if not simple_wrapper:
                self._enforce_path_scope(query, ctx.parent_path_bonds)

            if simple_wrapper and ctx.shape_format == context.ShapeFormat.FLAT:
                # This is a simple wrapper around a flat shape.
                # Make sure we pull out all target refs as-is
                subquery_rvar = query.from_clause[0]
                subquery = subquery_rvar.query
                query.path_outputs = subquery.path_outputs.copy()
                query.target_list = []
                for rt in subquery.target_list:
                    query.target_list.append(
                        pgast.ResTarget(
                            val=self._get_column(subquery_rvar, rt.name),
                            name=rt.name
                        )
                    )

            # The ORDER BY clause
            self._process_orderby(stmt.orderby)

            # The OFFSET clause
            if stmt.offset:
                with self.context.new() as ctx1:
                    ctx1.clause = 'offsetlimit'
                    ctx1.output_format = None
                    query.limit_offset = self.visit(stmt.offset)

            # The LIMIT clause
            if stmt.limit:
                with self.context.new() as ctx1:
                    ctx1.clause = 'offsetlimit'
                    ctx1.output_format = None
                    query.limit_count = self.visit(stmt.limit)

            if not parent_ctx.correct_set_assumed and not simple_wrapper:
                self._ensure_correct_set(stmt, query)

        return query

    def visit_GroupStmt(self, stmt):
        parent_ctx = self.context.current

        with self.context.substmt() as ctx:
            if ctx.toplevel_stmt is None:
                ctx.toplevel_stmt = ctx.stmt

            if stmt.aggregated_scope:
                ctx.aggregated_scope = stmt.aggregated_scope

            ctx.stmtmap[stmt] = ctx.stmt
            ctx.stmt_hierarchy[ctx.stmt] = parent_ctx.stmt

            query = ctx.query

            self.visit(stmt.subject)
            subjcte = self._get_set_cte(stmt.subject)
            subjrvar = self._include_range(query, subjcte)
            self._process_groupby(stmt.groupby)

            self._put_parent_range_scope(stmt.subject, subjrvar, grouped=True)

            # Process the result expression;
            with self.context.subquery() as selctx:
                for path_id, ref in selctx.path_bonds.items():
                    if path_id.startswith(stmt.subject.path_id):
                        ref.grouped = True
                self._pop_set_cte(stmt.subject)
                self._process_selector(stmt.result)
                self._enforce_path_scope(
                    selctx.query, selctx.parent_path_bonds)

                query.target_list = [
                    pgast.ResTarget(
                        val=selctx.query
                    )
                ]

            # The WHERE clause
            if stmt.where:
                with self.context.new() as ctx1:
                    ctx1.clause = 'where'
                    query.where_clause = self.visit(stmt.where)

            self._apply_path_scope()

            self._enforce_path_scope(query, ctx.parent_path_bonds)

            # The ORDER BY clause
            self._process_orderby(stmt.orderby)

            # The OFFSET clause
            if stmt.offset:
                with self.context.new() as ctx1:
                    ctx1.clause = 'offsetlimit'
                    ctx1.output_format = None
                    query.limit_offset = self.visit(stmt.offset)

            # The LIMIT clause
            if stmt.limit:
                with self.context.new() as ctx1:
                    ctx1.clause = 'offsetlimit'
                    ctx1.output_format = None
                    query.limit_count = self.visit(stmt.limit)

            for path_id in subjrvar.query.path_bonds:
                query.path_bonds.discard(path_id)

            if not parent_ctx.correct_set_assumed:
                self._ensure_correct_set(stmt, query)

        return query

    def _visit_shape(self, ir_set):
        ctx = self.context.current

        my_elements = []
        attribute_map = []
        path_id_aliases = {}
        idref = None

        source_is_view = irutils.is_view_set(ir_set)
        if source_is_view:
            source_shape = irutils.get_subquery_shape(ir_set)

            if source_shape is None:
                set_expr = ir_set.expr

                if isinstance(set_expr, irast.Stmt):
                    set_expr = set_expr.result

                if isinstance(set_expr, irast.TypeFilter):
                    set_expr = set_expr.expr

                if set_expr is not None:
                    target_ir_set = set_expr
                else:
                    target_ir_set = ir_set
            else:
                target_ir_set = source_shape

            if target_ir_set.path_id:
                path_id_aliases = {target_ir_set.path_id: ir_set.path_id}
                shape_source_cte = self._get_set_cte(ir_set)
                source_rvar = ctx.subquery_map[ctx.rel][shape_source_cte]
                self._put_parent_range_scope(target_ir_set, source_rvar)

        # The shape is ignored if the expression is not slated for output.
        ignore_shape = (
            not ctx.expr_exposed and
            ctx.shape_format != context.ShapeFormat.FLAT
        )

        for i, e in enumerate(ir_set.shape):
            rptr = e.rptr
            cset = irutils.get_canonical_set(e)
            ptrcls = rptr.ptrcls
            ptrdir = rptr.direction or s_pointers.PointerDirection.Outbound
            is_singleton = ptrcls.singular(ptrdir)
            ptrname = ptrcls.shortname

            # This shape is not slated for output, ignore it altogether.
            if ignore_shape and ptrname != 'std::id':
                continue

            with self.context.new() as newctx:
                newctx.scope_cutoff = True
                newctx.in_shape = True
                newctx.path_id_aliases.update(path_id_aliases)
                newctx.path_bonds = newctx.path_bonds.copy()

                if irutils.is_inner_view_reference(cset):
                    element = self.visit(irast.SelectStmt(result=e))
                elif irutils.is_subquery_set(cset):
                    element = self.visit(irast.SelectStmt(result=e))
                else:
                    element = self.visit(e)

            if not is_singleton:
                # Aggregate subquery results to keep correct
                # cardinality.
                element = self._aggregate_result(element)

            if ptrname == 'std::id':
                idref = element

            attr_name = s_pointers.PointerVector(
                name=ptrname.name, module=ptrname.module,
                direction=ptrdir, target=ptrcls.get_far_endpoint(ptrdir),
                is_linkprop=isinstance(ptrcls, s_lprops.LinkProperty))

            if isinstance(element, ResTargetList):
                attribute_map.extend(element.attmap)
                my_elements.extend(element.targets)
            else:
                attribute_map.append(attr_name)
                my_elements.append(element)

        if ignore_shape:
            result = idref
        else:
            result = ResTargetList(my_elements, attribute_map)

            if ctx.shape_format == context.ShapeFormat.SERIALIZED:
                if ctx.output_format == 'json':
                    # In JSON mode we simply produce a JSONB object of
                    # the shape record...
                    result = self._rtlist_as_json_object(result)
                else:
                    raise NotImplementedError(
                        f'unsupported output_format: {ctx.output_format}')

        return result

    def visit_Struct(self, expr):
        my_elements = []
        attribute_map = []

        for i, e in enumerate(expr.elements):
            with self.context.new() as newctx:
                newctx.scope_cutoff = True
                newctx.in_shape = True
                newctx.path_bonds = newctx.path_bonds.copy()
                val = e.val
                if (isinstance(val, irast.Set) and
                        isinstance(val.expr, irast.Stmt)):
                    element = self.visit(val.expr)
                else:
                    element = self.visit(val)

            attribute_map.append(e.name)
            my_elements.append(element)

        return ResTargetList(my_elements, attribute_map)

    def visit_StructIndirection(self, expr):
        with self.context.new() as ctx:
            # Make sure the struct doesn't get collapsed into a value.
            ctx.expr_exposed = False
            struct_vars = self.visit(expr.expr)

        if not isinstance(struct_vars, ResTargetList):
            raise RuntimeError(  # pragma: no cover
                'expecting struct ResTargetList')

        for i, rt in enumerate(struct_vars.attmap):
            if rt == expr.name:
                return struct_vars.targets[i]
        else:
            raise RuntimeError(  # pragma: no cover
                f'could not find {expr.name} in struct ResTargetList')

    def visit_Set(self, expr):
        ctx = self.context.current

        source_cte = self._set_to_cte(expr)

        if ctx.clause == 'where' and ctx.rel is ctx.stmt:
            # When referred to in WHERE
            # we want to wrap the set CTE into
            #    EXISTS(SELECT * FROM SetCTE WHERE SetCTE.expr)
            result = self._wrap_set_rel_as_bool_disjunction(expr, source_cte)

        elif ((ctx.clause == 'offsetlimit' and not ctx.in_set_expr) or
                ctx.in_member_test):
            # When referred to in OFFSET/LIMIT we want to wrap the
            # set CTE into
            #    SELECT v FROM SetCTE
            result = self._wrap_set_rel_as_value(expr, source_cte)

        else:
            # Otherwise we join the range directly into the current rel
            # and make its refs available in the path namespace.
            if ctx.clause == 'orderby':
                join_type = 'left'
            else:
                join_type = 'inner'
            source_rvar = self._include_range(
                ctx.rel, source_cte, join_type=join_type, lateral=True)
            result = self._get_var_for_set_expr(expr, source_rvar)

        if expr.shape:
            shape = self._visit_shape(expr)
            if shape is not None:
                result = shape

        return result

    def _aggregate_result(self, node):
        ctx = self.context.current

        if not isinstance(node, pgast.SelectStmt):
            node = pgast.SelectStmt(
                target_list=[
                    pgast.ResTarget(val=node)
                ]
            )

        rt = node.target_list[0]
        rt.name = ctx.genalias(hint='r')

        subrvar = pgast.RangeSubselect(
            subquery=node,
            alias=pgast.Alias(
                aliasname=ctx.genalias(hint='aggw')
            )
        )

        result = pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=pgast.FuncCall(
                        name=('array_agg',),
                        args=[
                            self._get_column(subrvar, rt.name)
                        ]
                    )
                )
            ],
            from_clause=[
                subrvar
            ]
        )

        return result

    def _enforce_path_scope(self, query, path_bonds):
        cond = self._full_inner_bond_condition(query, path_bonds)
        if cond is not None:
            query.where_clause = self._extend_binop(query.where_clause, cond)

    def _apply_path_scope(self):
        """Insert conditions to satisfy implicit set existence."""
        ctx = self.context.current
        query = ctx.query

        # EdgeQL semantics dictates that all sets produced by path
        # expresions must exist unless the non-existence is
        # either explicitly allowed or implicitly allowed by the
        # operation (if, ??).
        #
        # On the other hand, the semantics of most generated CTEs implies
        # set existence, so we only need to inject explicit EXISTS()
        # conditions when we know that the given SQL statement does not
        # imply set existence.
        #
        explicit_exists = []
        lax_prefixes = {s.path_id for s, lax in ctx.setscope.items() if lax}
        candidates = (ctx.forced_setscope - ctx.auto_setscope)
        for ir_set in candidates:
            lax = ctx.setscope.get(ir_set)
            if not lax:
                prefixes = set(ir_set.path_id.iter_prefixes())
                if not (prefixes & lax_prefixes):
                    explicit_exists.append(ir_set)

        explicit_exists.sort(key=lambda s: len(s.path_id), reverse=True)

        scoped_prefixes = set()
        for ir_set in explicit_exists:
            if ir_set.path_id not in scoped_prefixes:
                cte = ctx.ctemap[ir_set, False]
                scope_expr = self._set_as_exists_op(
                    self._wrap_set_rel(ir_set, cte))
                query.where_clause = self._extend_binop(
                    query.where_clause, scope_expr)
                scoped_prefixes.update(ir_set.path_id.iter_prefixes())

    def _include_range(self, stmt, rel, join_type='inner', lateral=False):
        """Ensure the *rel* is present in the from_clause of *stmt*.

        :param stmt:
            The statement to include *rel* in.

        :param rel:
            The relation node to join.

        :param join_type:
            JOIN type to use when including *rel*.

        :param lateral:
            Whether *rel* should be joined laterally.

        :return:
            RangeVar or RangeSubselect representing the *rel* in the
            context of current rel.
        """
        ctx = self.context.current

        rvar = ctx.subquery_map[stmt].get(rel)
        if rvar is None:
            # The rel has not been recorded as a sub-relation of this rel,
            # make it so.
            rvar = self._rvar_for_rel(rel, lateral=lateral)
            self._rel_join(stmt, rvar, type=join_type)

            ctx.subquery_map[stmt][rel] = rvar

        # Make sure that the path namespace of *cte* is mapped
        # onto the path namespace of *stmt*.
        self._pull_path_namespace(
            target=stmt, source=rvar, pull_bonds=not ctx.scope_cutoff)

        return rvar

    def _set_as_exists_op(self, pg_expr, negated=False):
        # Make sure *pg_expr* is an EXISTS() expression
        # Set references inside WHERE are transformed into
        # EXISTS expressions in visit_Set.  For other
        # occurrences we do it here.
        if isinstance(pg_expr, pgast.Query):
            result = pgast.SubLink(
                type=pgast.SubLinkType.EXISTS, expr=pg_expr)

        elif isinstance(pg_expr, (pgast.Constant, pgast.ParamRef)):
            result = pgast.NullTest(arg=pg_expr, negated=True)

        else:
            raise RuntimeError(  # pragma: no cover
                f'unexpected argument to _set_as_exists_op: {pg_expr!r}')

        if negated:
            result = self._new_unop(ast.ops.NOT, result)

        return result

    def _wrap_set_rel_as_bool_disjunction(self, ir_set, set_rel):
        # For the *set_rel* relation representing the *ir_set*
        # return the following:
        #     EXISTS (
        #         SELECT
        #         FROM <set_rel>
        #         [WHERE <set_rel>.v]
        #     )
        #
        ctx = self.context.current

        with self.context.subquery() as subctx:
            wrapper = subctx.query
            rvar = self._rvar_for_rel(set_rel)
            wrapper.from_clause = [rvar]
            self._put_path_rvar(wrapper, ir_set.path_id, rvar)
            self._pull_path_namespace(target=wrapper, source=rvar)
            wrapper.where_clause = self._get_var_for_set_expr(ir_set, rvar)
            self._enforce_path_scope(wrapper, ctx.path_bonds)

        return pgast.SubLink(
            type=pgast.SubLinkType.EXISTS,
            expr=wrapper
        )

    def _wrap_set_rel(self, ir_set, set_rel):
        # For the *set_rel* relation representing the *ir_set*
        # return the following:
        #     (
        #         SELECT
        #         FROM <set_rel>
        #         WHERE <set_rel>.v IS NOT NULL
        #     )
        #
        ctx = self.context.current

        with self.context.subquery() as subctx:
            wrapper = subctx.query
            rvar = self._rvar_for_rel(set_rel)
            wrapper.from_clause = [rvar]
            self._put_path_rvar(wrapper, ir_set.path_id, rvar)
            self._pull_path_namespace(target=wrapper, source=rvar)

            if ir_set.expr is not None:
                target = self._get_var_for_set_expr(ir_set, rvar)
            else:
                target = self._get_path_var(wrapper, ir_set.path_id)

            wrapper.where_clause = pgast.NullTest(
                arg=target,
                negated=True
            )

            self._enforce_path_scope(wrapper, ctx.path_bonds)

        return wrapper

    def _wrap_set_rel_as_value(self, ir_set, set_rel):
        # For the *set_rel* relation representing the *ir_set*
        # return the following:
        #     (
        #         SELECT <set_rel>.v
        #         FROM <set_rel>
        #     )
        #
        ctx = self.context.current

        with self.context.subquery() as subctx:
            wrapper = subctx.query
            rvar = self._rvar_for_rel(set_rel)
            wrapper.from_clause = [rvar]
            self._pull_path_namespace(target=wrapper, source=rvar)

            target = self._get_var_for_set_expr(ir_set, rvar)

            wrapper.target_list.append(
                pgast.ResTarget(
                    val=target
                )
            )

            # For expressions in OFFSET/LIMIT clauses we must
            # use the _parent_'s query scope, not the scope of
            # the query where the OFFSET/LIMIT clause is.
            if ctx.clause == 'offsetlimit':
                path_scope = ctx.parent_path_bonds
            else:
                path_scope = ctx.path_bonds
            self._enforce_path_scope(wrapper, path_scope)

        return wrapper

    def _put_parent_range_scope(self, ir_set, rvar, grouped=False):
        ctx = self.context.current
        ir_set = irutils.get_canonical_set(ir_set)
        if ir_set not in ctx.computed_node_rels:
            ctx.computed_node_rels[ir_set] = rvar, grouped

    def _get_parent_range_scope(self, ir_set):
        ctx = self.context.current
        ir_set = irutils.get_canonical_set(ir_set)
        return ctx.computed_node_rels.get(ir_set)

    def _put_set_cte(self, ir_set, cte, *, ctx=None):
        if ctx is None:
            ctx = self.context.current

        ir_set = irutils.get_canonical_set(ir_set)

        if ir_set.rptr is not None and ir_set.expr is None:
            key = (ir_set, ctx.lax_paths)
        else:
            key = (ir_set, False)

        ctx.ctemap[key] = cte
        ctx.ctemap_by_stmt[ctx.stmt][key] = cte

        if (ir_set.expr is None and
                ctx.clause in {'where', 'result'} and
                not ctx.in_shape):
            if ctx.lax_paths or not ctx.setscope.get(ir_set):
                ctx.setscope[ir_set] = ctx.lax_paths

        self._note_set_ref(ir_set)

        return cte

    def _pop_set_cte(self, ir_set, *, lax=None):

        ctx = self.context.current

        ir_set = irutils.get_canonical_set(ir_set)

        if lax is not None:
            key = (ir_set, lax)
        elif ir_set.rptr is not None and ir_set.expr is None:
            key = (ir_set, ctx.lax_paths)
        else:
            key = (ir_set, False)

        return ctx.ctemap.pop(key)

    def _get_set_cte(self, ir_set, *, lax=None):
        ctx = self.context.current

        ir_set = irutils.get_canonical_set(ir_set)

        if lax is not None:
            key = (ir_set, lax)
        elif ir_set.rptr is not None and ir_set.expr is None:
            key = (ir_set, ctx.lax_paths)
        else:
            key = (ir_set, False)

        cte = ctx.ctemap.get(key)

        if cte is not None:
            self._note_set_ref(ir_set)

        return cte

    def _note_set_ref(self, ir_set):
        ctx = self.context.current

        if (ir_set.expr is None and
                ctx.clause in {'where', 'result'} and
                not ctx.in_shape):
            # References to paths in SELECT/RETURNING and WHERE clauses
            # form a strict set existence condition for each path, unless
            # the existence predicate was used explicitly (either directly,
            # with [NOT] EXISTS, or through the coalescing operator.)
            if ctx.clause != 'result' or ctx.in_aggregate:
                ctx.auto_setscope.add(ir_set)
            elif len(ir_set.path_id) > 1:
                ctx.forced_setscope.add(ir_set)

    def _set_to_cte(self, ir_set):
        """Return a Common Table Expression for a given IR Set.

        @param ir_set: IR Set node.
        """
        cte = self._get_set_cte(ir_set)
        if cte is not None:
            # Already have a CTE for this Set.
            return cte

        ctx = self.context.current

        ir_set = irutils.get_canonical_set(ir_set)

        stmt = pgast.SelectStmt()
        stmt.path_id = ir_set.path_id

        cte_name = ctx.genalias(hint=self._get_set_cte_alias(ir_set))
        cte = pgast.CommonTableExpr(query=stmt, name=cte_name)

        self._put_set_cte(ir_set, cte)

        with self.context.new() as ctx:
            ctx.rel = stmt
            ctx.path_bonds = ctx.path_bonds.copy()

            if self._get_parent_range_scope(ir_set) is not None:
                # We are ranging over this set in the parent query,
                # while evaluating a view expression.
                self._process_set_as_parent_scope(ir_set, stmt)

            elif irutils.is_strictly_view_set(ir_set):
                self._process_set_as_view(ir_set, stmt)

            elif irutils.is_inner_view_reference(ir_set):
                self._process_set_as_view_inner_reference(ir_set, stmt)

            elif irutils.is_subquery_set(ir_set):
                self._process_set_as_subquery(ir_set, stmt)

            elif isinstance(ir_set.expr, irast.SetOp):
                # Set operation: UNION/INTERSECT/EXCEPT
                self._process_set_as_setop(ir_set, stmt)

            elif isinstance(ir_set.expr, irast.TypeFilter):
                # Expr[IS Type] expressions.
                self._process_set_as_typefilter(ir_set, stmt)

            elif ir_set.expr is not None:
                expr_result = irutils.infer_type(ir_set.expr, ctx.schema)
                if isinstance(expr_result, s_concepts.Concept):
                    # Expressions returning objects.
                    self._process_set_as_concept_expr(ir_set, stmt)

                elif (isinstance(ir_set.expr, irast.FunctionCall) and
                        ir_set.expr.func.aggregate):
                    # Call to an aggregate function.
                    self._process_set_as_agg_expr(ir_set, stmt)

                elif isinstance(ir_set.expr, irast.ExistPred):
                    # EXISTS(), which is a special kind of an aggregate.
                    self._process_set_as_exists_expr(ir_set, stmt)

                else:
                    # Other expressions.
                    self._process_set_as_expr(ir_set, stmt)

            elif ir_set.rptr is not None:
                self._process_set_as_path_step(ir_set, stmt)

            else:
                self._process_set_as_root(ir_set, stmt)

        return self._get_set_cte(ir_set)

    def _get_set_cte_alias(self, ir_set):
        if ir_set.rptr is not None and ir_set.rptr.source.scls is not None:
            alias_hint = '{}_{}'.format(
                ir_set.rptr.source.scls.name.name,
                ir_set.rptr.ptrcls.shortname.name
            )
        elif ir_set.expr is not None and ir_set.source is not None:
            src = ir_set.source
            if src.rptr is not None:
                alias_hint = '{}_{}'.format(
                    src.rptr.source.scls.name.name,
                    src.rptr.ptrcls.shortname.name
                )
            else:
                if isinstance(src.scls, s_obj.Collection):
                    alias_hint = src.scls.schema_name
                else:
                    alias_hint = src.scls.name.name
            alias_hint += '_expr'
        else:
            if isinstance(ir_set.scls, s_obj.Collection):
                alias_hint = ir_set.scls.schema_name
            else:
                alias_hint = ir_set.scls.name.name

        return alias_hint

    def _connect_set_sources(self, ir_set, stmt, sources, setscope=None):
        # Generate a flat JOIN list from the gathered sources
        # using path bonds for conditions.

        with self.context.new() as ctx:
            ctx.expr_exposed = False

            subrels = ctx.subquery_map[stmt]
            if setscope is None:
                setscope = ctx.setscope

            for source in sources:
                source_rel = self._set_to_cte(source)
                if source_rel in subrels:
                    continue

                lax_path = setscope.get(source)
                if lax_path:
                    lax_rel = self._get_set_cte(source, lax=True)
                    if lax_rel is not None:
                        source_rel = lax_rel

                self._include_range(
                    stmt, source_rel, join_type='inner', lateral=True)

    def _get_root_rvar(self, ir_set, stmt, nullable=False, set_rvar=None):
        if not isinstance(ir_set.scls, s_concepts.Concept):
            return None

        if set_rvar is None:
            set_rvar = self._range_for_set(ir_set)
            set_rvar.nullable = nullable
            set_rvar.path_bonds.add(ir_set.path_id)

        self._put_path_rvar(stmt, ir_set.path_id, set_rvar)
        self._put_path_bond(stmt, ir_set.path_id)

        return set_rvar

    def _process_set_as_root(self, ir_set, stmt):
        """Populate the CTE for a Set defined by a path root."""
        ctx = self.context.current

        set_rvar = self._get_root_rvar(ir_set, stmt)
        stmt.from_clause.append(set_rvar)
        self._enforce_path_scope(stmt, ctx.parent_path_bonds)
        ctx.query.ctes.append(self._get_set_cte(ir_set))

    def _process_set_as_parent_scope(self, ir_set, stmt):
        """Populate the CTE for a Set defined by parent range."""
        ctx = self.context.current

        parent_rvar, grouped = self._get_parent_range_scope(ir_set)
        if isinstance(parent_rvar, pgast.RangeVar):
            parent_scope_rel = parent_rvar.relation
        else:
            parent_scope_rel = parent_rvar

        if isinstance(parent_scope_rel, pgast.CommonTableExpr):
            set_rvar = pgast.RangeVar(
                relation=parent_scope_rel,
                alias=pgast.Alias(
                    aliasname=ctx.genalias(parent_scope_rel.name)
                )
            )
        else:
            set_rvar = pgast.RangeSubselect(
                subquery=parent_scope_rel,
                alias=pgast.Alias(
                    aliasname=ctx.genalias('scopew')
                )
            )

        self._get_root_rvar(ir_set, stmt, set_rvar=set_rvar)

        stmt.from_clause.append(set_rvar)
        ctx.query.ctes.append(self._get_set_cte(ir_set))

        if isinstance(parent_rvar, pgast.RangeVar):
            parent_scope = {}
            for path_id in parent_rvar.path_bonds:
                parent_scope[path_id] = LazyPathVarRef(
                    self._get_rvar_path_var, parent_rvar, path_id)
                parent_scope[path_id].grouped = grouped

            self._enforce_path_scope(stmt, parent_scope)

    def _process_set_as_path_step(self, ir_set, stmt):
        """Populate the CTE for Set defined by a single path step."""
        ctx = self.context.current

        rptr = ir_set.rptr
        ptrcls = rptr.ptrcls
        fromlist = stmt.from_clause

        # Path is a reference to Atom.__class__.
        is_atom_class_ref = (
            isinstance(rptr.source.scls, s_atoms.Atom) and
            ptrcls.shortname == 'std::__class__'
        )

        # Path is a reference to a link property.
        is_link_prop_ref = isinstance(ptrcls, s_lprops.LinkProperty)

        if not is_atom_class_ref and not is_link_prop_ref:
            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, resolve_type=False, link_bias=False)

            # Path is a reference to a relationship represented
            # in a mapping table.
            is_mapped_target_ref = ptr_info.table_type != 'concept'

            # Path target is a Concept class.
            is_concept_ref = isinstance(ir_set.scls, s_concepts.Concept)
        else:
            is_mapped_target_ref = False
            is_concept_ref = is_atom_class_ref

        # Check if the source CTE has all the data to resolve this path.
        return_parent = not (
            is_atom_class_ref or
            is_mapped_target_ref or
            is_concept_ref
        )

        with self.context.new() as newctx:
            newctx.path_bonds = ctx.parent_path_bonds.copy()

            if return_parent:
                source_cte = self._set_to_cte(ir_set.rptr.source)
                if isinstance(source_cte, pgast.CommonTableExpr):
                    source_query = source_cte.query
                else:
                    source_query = source_cte
            else:
                self._connect_set_sources(ir_set, stmt, [ir_set.rptr.source])
                path_rvar = fromlist[0]
                source_query = path_rvar.query

            set_rvar = self._get_root_rvar(
                ir_set, stmt, nullable=ctx.lax_paths > 0)

            if is_atom_class_ref:
                # Special case to support Path.atom.__class__ paths
                self._join_class_rel(
                    stmt=stmt, set_rvar=set_rvar, ir_set=ir_set)

            else:
                if is_link_prop_ref:
                    # Reference to a link property.
                    self._join_mapping_rel(
                        stmt=source_query, set_rvar=set_rvar,
                        ir_set=ir_set, map_join_type='left')

                elif is_mapped_target_ref:
                    map_join_type = 'left' if ctx.lax_paths else 'inner'

                    self._join_mapping_rel(
                        stmt=stmt, set_rvar=set_rvar, ir_set=ir_set,
                        map_join_type=map_join_type)

                elif is_concept_ref:
                    # Direct reference to another object.
                    self._join_inline_rel(
                        stmt=stmt, set_rvar=set_rvar, ir_set=ir_set,
                        back_id_col=ptr_info.column_name)

                else:
                    # The path step target is stored in the root relation.
                    # No need to do anything else here.
                    pass

        if return_parent:
            self._put_set_cte(ir_set, source_cte)
        else:
            cte = self._get_set_cte(ir_set)
            cte_parent = ctx.query

            cte_parent.ctes.append(cte)

    def _ensure_query_restarget_name(self, query, *, hint=None):
        ctx = self.context.current

        suggested_rt_name = ctx.genalias(hint=hint or 'v')
        rt_name = None

        def _get_restarget(q):
            nonlocal rt_name

            rt = q.target_list[0]
            if rt_name is not None:
                rt.name = rt_name
            elif rt.name is not None:
                rt_name = rt.name
            else:
                if isinstance(rt.val, pgast.ColumnRef):
                    rt.name = rt_name = rt.val.name[-1]
                else:
                    rt.name = rt_name = suggested_rt_name

        if query.op is not None:
            self._for_each_query_in_set(query, _get_restarget)
        else:
            _get_restarget(query)

        return rt_name

    def _process_set_as_view(self, ir_set, stmt):
        """Populate the CTE for Set defined by a subquery defining a view."""
        ctx = self.context.current
        cte = self._get_set_cte(ir_set)
        parent_stmt = ctx.stmtmap.get(ir_set.expr.parent_stmt)

        with self.context.new() as newctx:
            if parent_stmt is not None:
                newctx.path_bonds = ctx.path_bonds_by_stmt[parent_stmt].copy()
                newctx.ctemap = ctx.ctemap_by_stmt[parent_stmt].copy()
            else:
                newctx.path_bonds = {}
                newctx.ctemap = {}

            newctx.computed_node_rels = {}

            newctx.expr_exposed = False

            subquery = self.visit(ir_set.expr)

            if isinstance(ir_set.scls, s_concepts.Concept):
                s_rvar = pgast.RangeSubselect(
                    subquery=subquery,
                    alias=pgast.Alias(
                        aliasname=ctx.genalias(hint='vw')
                    )
                )

                subquery = self._wrap_view_ref(
                    ir_set.real_path_id, ir_set.path_id, s_rvar)
                self._get_path_output(subquery, ir_set.path_id)

            for path_id in list(subquery.path_bonds):
                if not path_id.startswith(ir_set.path_id):
                    subquery.path_bonds.discard(path_id)

        rt_name = self._ensure_query_restarget_name(subquery, hint=cte.name)
        self._put_path_output(subquery, ir_set, rt_name)

        cte.query = subquery
        ctx.toplevel_stmt.ctes.append(cte)

    def _process_set_as_subquery(self, ir_set, stmt):
        """Populate the CTE for Set defined by a subquery."""
        ctx = self.context.current
        cte = self._get_set_cte(ir_set)

        with self.context.new() as newctx:
            newctx.path_bonds = ctx.path_bonds.copy()
            newctx.path_scope = set(newctx.path_scope) | ir_set.path_scope

            if irutils.is_strictly_view_set(ir_set.expr.result):
                outer_id = ir_set.path_id
                inner_id = ir_set.expr.result.path_id

                newctx.view_path_id_map = {
                    outer_id: inner_id
                }

            subquery = self.visit(ir_set.expr)

            for path_id in list(subquery.path_bonds):
                if not path_id.startswith(ir_set.path_id):
                    subquery.path_bonds.discard(path_id)

        rt_name = self._ensure_query_restarget_name(subquery, hint=cte.name)
        self._put_path_output(subquery, ir_set, rt_name)

        self._put_set_cte(ir_set, subquery)

    def _process_set_as_view_inner_reference(self, ir_set, stmt):
        """Populate the CTE for Set for inner view references."""
        ctx = self.context.current
        cte = self._get_set_cte(ir_set)
        inner_set = ir_set.view_source

        with self.context.new() as newctx:
            newctx.path_bonds = ctx.path_bonds.copy()
            newctx.expr_exposed = False

            # rptr source is a view, so we need to make sure that all
            # references to source set in this subquery are properly
            # mapped to the view rel.
            src = ir_set.rptr.source
            # Naked source set.
            src_ir_set = irutils.get_subquery_shape(src)
            source_rvar = None

            if (irutils.is_strictly_view_set(src) or
                    (irutils.is_subquery_set(src) and
                     irutils.is_strictly_view_set(src.expr.result)) or
                    (irutils.is_inner_view_reference(src))):

                if src.path_id in newctx.path_bonds:
                    newctx.path_bonds[src_ir_set.path_id] = \
                        newctx.path_bonds[src.path_id]
                else:
                    source_cte = self._set_to_cte(src)
                    source_rvar = self._rvar_for_rel(source_cte)

                    # Wrap the view rel for proper path_id translation.
                    wrapper = self._wrap_view_ref(
                        src.path_id, src_ir_set.path_id, source_rvar)
                    # Finally, map the source Set to the wrapper.
                    self._put_parent_range_scope(src_ir_set, wrapper)

                    newctx.path_bonds = ctx.path_bonds.copy()

            # Prevent _ensure_correct_set from wrapping the subquery as we may
            # need to fiddle with it to ensure correct cardinality first.
            newctx.correct_set_assumed = True
            newctx.path_scope = set(newctx.path_scope) | ir_set.path_scope

            newctx.view_path_id_map = {
                ir_set.path_id: inner_set.expr.result.path_id
            }

            # We need to make sure that the target expression is computed at
            # least N times, where N is the cardinality of the ``rptr.source``
            # set.  However, we cannot simply inject ``source_rvar`` here, as
            # it might have already been injected if the expression has the
            # relevant path bond.
            # To determine whether the source_rvar JOIN is necessary, do a
            # deep search for the ``target_ir_set``.
            flt = lambda n: n is src_ir_set
            expr_refers_to_target = ast.find_children(
                inner_set.expr, flt, terminate_early=True)

            if not expr_refers_to_target:
                if source_rvar is None:
                    with self.context.new() as subctx:
                        subctx.path_bonds = ctx.path_bonds.copy()
                        source_cte = self._set_to_cte(src)
                        source_rvar = self._rvar_for_rel(source_cte)
                newctx.expr_injected_path_bond = {
                    'ref': self._get_rvar_path_var(source_rvar, src.path_id),
                    'path_id': src.path_id
                }

            subquery = self.visit(inner_set.expr)

            if not expr_refers_to_target:
                # Use a "where" join here to avoid mangling the canonical set
                # rvar in from_clause[0], as _pull_path_rvar will choke on a
                # JOIN there.
                self._rel_join(subquery, source_rvar,
                               type='where', front=True)

        # We inhibited _ensure_correct_set above.  Now that we are done with
        # the query, ensure set correctness explicitly.
        self._ensure_correct_set(inner_set.expr, subquery)

        rt_name = self._ensure_query_restarget_name(subquery, hint=cte.name)
        self._put_path_output(subquery, ir_set, rt_name)

        cte.query = subquery
        self._put_set_cte(ir_set, cte)
        ctx.query.ctes.append(cte)

    def _process_set_as_setop(self, ir_set, stmt):
        """Populate the CTE for Set defined by set operation."""
        ctx = self.context.current
        expr = ir_set.expr

        with self.context.new() as newctx:
            newctx.path_bonds = ctx.path_bonds.copy()
            larg = self.visit(irutils.ensure_stmt(expr.left))
            newctx.path_bonds = ctx.path_bonds.copy()
            rarg = self.visit(irutils.ensure_stmt(expr.right))

        stmt.op = pgast.PgSQLSetOperator(expr.op)
        stmt.all = True
        stmt.larg = larg
        stmt.rarg = rarg
        self._put_path_bond(stmt, ir_set.path_id)
        rt_name = self._ensure_query_restarget_name(stmt)
        self._put_path_output(stmt, ir_set, rt_name)
        self._put_path_rvar(stmt, ir_set, None)
        self._put_set_cte(ir_set, stmt)

    def _process_set_as_typefilter(self, ir_set, stmt):
        """Populate the CTE for Set defined by a Expr[IS Type] expression."""
        ctx = self.context.current

        root_rvar = self._get_root_rvar(ir_set, stmt)
        stmt.from_clause.append(root_rvar)
        self._put_path_rvar(stmt, ir_set.expr.expr.path_id, root_rvar)
        self.visit(ir_set.expr.expr)
        stmt.as_type = irast.PathId([ir_set.scls])

        ctx.query.ctes.append(self._get_set_cte(ir_set))

    def _process_set_as_concept_expr(self, ir_set, stmt):
        """Populate the CTE for Set defined by an object expression."""

        ctx = self.context.current

        root_rvar = self._get_root_rvar(ir_set, stmt)

        with self.context.new() as newctx:
            newctx.in_set_expr = True
            newctx.rel = innerqry = pgast.SelectStmt()
            set_expr = self.visit(ir_set.expr)

        innerqry.target_list.append(
            pgast.ResTarget(
                val=set_expr,
                name='v'
            )
        )

        self._put_path_output(innerqry, ir_set, 'v')
        self._put_path_var(innerqry, ir_set, pgast.ColumnRef(name=['v']))
        self._put_path_bond(innerqry, ir_set.path_id)

        qry = pgast.SelectStmt()
        self._include_range(qry, innerqry)
        # innerqry does not have path_rvar_map since the set
        # is derived from an expression, not a relation.
        self._put_path_rvar(qry, ir_set.path_id, qry.from_clause[0])

        stmt.from_clause.append(root_rvar)
        self._include_range(stmt, qry, join_type='inner', lateral=True)

        self._put_path_rvar(stmt, ir_set.path_id, root_rvar)
        self._put_path_bond(stmt, ir_set.path_id)

        ctx.query.ctes.append(self._get_set_cte(ir_set))

    def _process_set_as_expr(self, ir_set, stmt):
        """Populate the CTE for Set defined by an expression."""

        ctx = self.context.current

        with self.context.new() as newctx:
            newctx.in_set_expr = True
            newctx.rel = stmt
            newctx.path_scope = newctx.path_scope | ir_set.path_scope
            set_expr = self.visit(ir_set.expr)

        if isinstance(set_expr, ResTargetList):
            for i, rt in enumerate(set_expr.targets):
                stmt.target_list.append(
                    pgast.ResTarget(val=rt, name=set_expr.attmap[i])
                )
        else:
            restarget = pgast.ResTarget(val=set_expr, name='v')
            stmt.target_list.append(restarget)
            self._put_path_output(stmt, ir_set, restarget.name)

        if ctx.expr_injected_path_bond is not None:
            # Inject an explicitly provided path bond.  This is necessary
            # to ensure the correct output of rels that compute view
            # expressions that do not contain relevant path bonds themselves.
            alias = ctx.genalias(hint='b')
            bond_ref = ctx.expr_injected_path_bond['ref']
            bond_path_id = ctx.expr_injected_path_bond['path_id']
            stmt.target_list.append(
                pgast.ResTarget(val=bond_ref, name=alias)
            )
            # Register this bond as output just in case.
            # BUT, do not add it to path_bonds.
            self._put_path_output(stmt, bond_path_id, alias)
            # Due to injection this rel must not be a CTE.
            self._put_set_cte(ir_set, stmt)
        else:
            ctx.query.ctes.append(self._get_set_cte(ir_set))

    def _process_set_as_agg_expr(self, ir_set, stmt):
        """Populate the CTE for Set defined by an aggregate."""

        ctx = self.context.current

        with self.context.new() as newctx:
            newctx.in_set_expr = True
            newctx.rel = stmt

            path_scope = set(ctx.path_bonds) | ctx.path_scope
            set_expr = self.visit(ir_set.expr)

            # Add an explicit GROUP BY for each non-aggregated path bond.
            for path_id in list(stmt.path_bonds):
                if (not path_id.is_in_scope(path_scope) or
                        path_id in ctx.aggregated_scope):
                    stmt.path_bonds.discard(path_id)
                else:
                    path_var = self._get_path_var(stmt, path_id)
                    stmt.group_clause.append(path_var)

        restarget = pgast.ResTarget(val=set_expr, name='v')
        stmt.target_list.append(restarget)
        self._put_path_output(stmt, ir_set, restarget.name)
        ctx.query.ctes.append(self._get_set_cte(ir_set))

    def _process_set_as_exists_expr(self, ir_set, stmt):
        """Populate the CTE for Set defined by an EXISTS() expression."""

        if isinstance(ir_set.expr.expr, irast.Stmt):
            # Statement varant.
            return self._process_set_as_exists_stmt_expr(ir_set, stmt)

        ctx = self.context.current
        cte = self._get_set_cte(ir_set)

        with self.context.new() as newctx:
            newctx.in_set_expr = True
            newctx.lax_paths = 1
            newctx.rel = stmt
            path_scope = set(ctx.path_bonds) | ctx.path_scope

            ir_expr = ir_set.expr.expr
            set_ref = self.visit(ir_expr)

            for path_id in list(stmt.path_bonds):
                if not path_id.is_in_scope(path_scope):
                    stmt.path_bonds.discard(path_id)

            if not stmt.path_bonds and isinstance(set_ref, pgast.ColumnRef):
                set_expr = self._set_as_exists_op(
                    stmt, negated=ir_set.expr.negated)
                newctx.rel = cte.query = stmt = pgast.SelectStmt()
            else:
                for path_id in stmt.path_bonds:
                    var = self._get_path_var(stmt, path_id)
                    stmt.group_clause.append(var)

                set_expr = self._new_binop(
                    pgast.FuncCall(
                        name=('count',),
                        args=[set_ref],
                        agg_filter=pgast.NullTest(arg=set_ref, negated=True)
                    ),
                    pgast.Constant(
                        val=0
                    ),
                    op=ast.ops.EQ if ir_set.expr.negated else ast.ops.GT
                )

            restarget = pgast.ResTarget(val=set_expr, name='v')
            stmt.target_list.append(restarget)
            self._put_path_output(stmt, ir_set, restarget.name)
            ctx.query.ctes.append(self._get_set_cte(ir_set))

    def _process_set_as_exists_stmt_expr(self, ir_set, stmt):
        """Populate the CTE for Set defined by an EXISTS() expression."""

        ctx = self.context.current

        with self.context.new() as newctx:
            newctx.in_set_expr = True
            newctx.lax_paths = 2
            newctx.rel = stmt

            path_scope = set(ctx.path_bonds) | ctx.path_scope

            ir_expr = ir_set.expr.expr
            set_expr = self.visit(ir_expr)

            for path_id in list(set_expr.path_bonds):
                if path_id.is_in_scope(path_scope):
                    set_expr.path_bonds.discard(path_id)

        if not set_expr.path_bonds:
            set_expr = self._set_as_exists_op(
                set_expr, negated=ir_set.expr.negated)
        else:
            set_rvar = self._include_range(stmt, set_expr)
            set_ref = self._get_rvar_path_var(set_rvar, ir_expr.result.path_id)

            for path_id in stmt.path_bonds:
                var = self._get_path_var(stmt, path_id)
                stmt.group_clause.append(var)

            set_expr = self._new_binop(
                pgast.FuncCall(
                    name=('count',),
                    args=[set_ref],
                    agg_filter=pgast.NullTest(arg=set_ref, negated=True)
                ),
                pgast.Constant(
                    val=0
                ),
                op=ast.ops.EQ if ir_set.expr.negated else ast.ops.GT
            )

        restarget = pgast.ResTarget(val=set_expr, name='v')
        stmt.target_list.append(restarget)
        self._put_path_output(stmt, ir_set, restarget.name)
        self._put_set_cte(ir_set, stmt)

    def visit_Coalesce(self, expr):
        with self.context.new() as ctx:
            ctx.lax_paths = 1
            pg_args = self.visit(expr.args)
        return pgast.FuncCall(name=('coalesce',), args=pg_args)

    def _wrap_view_ref(self, inner_path_id, outer_path_id, view_rvar):
        wrapper = pgast.SelectStmt(
            from_clause=[view_rvar],
            view_path_id_map={
                outer_path_id: inner_path_id
            }
        )

        if isinstance(view_rvar, pgast.RangeSubselect):
            wrapper.ctes = view_rvar.subquery.ctes
            view_rvar.subquery.ctes = []

        self._pull_path_namespace(target=wrapper, source=view_rvar)
        return wrapper

    def _process_selector(self, result_expr):
        ctx = self.context.current
        query = ctx.query

        with self.context.new() as newctx:
            newctx.clause = 'result'
            if newctx.expr_exposed is None:
                newctx.expr_exposed = True
            pgexpr = self.visit(result_expr)

            if isinstance(pgexpr, ResTargetList):
                selexprs = []

                for i, rt in enumerate(pgexpr.targets):
                    att = pgexpr.attmap[i]

                    name = str(att)

                    selexprs.append(
                        (rt, common.edgedb_name_to_pg_name(name))
                    )
            else:
                selexprs = [(pgexpr, None)]

        if ((ctx.expr_exposed is None or ctx.expr_exposed) and
                ctx.output_format == 'json'):
            if isinstance(pgexpr, ResTargetList):
                val = self._rtlist_as_json_object(pgexpr)
            else:
                val = pgast.FuncCall(name=('to_jsonb',), args=[pgexpr])

            target = pgast.ResTarget(name=None, val=val)
            query.target_list.append(target)
        else:
            for pgexpr, alias in selexprs:
                target = pgast.ResTarget(name=alias, val=pgexpr)
                query.target_list.append(target)

    def _ensure_correct_set(self, stmt, query):
        ctx = self.context.current

        if stmt.result is None:
            return

        restype = irutils.infer_type(stmt.result, ctx.schema)
        if not isinstance(restype, s_atoms.Atom):
            return

        target_list = query.target_list

        wrapper = pgast.SelectStmt(
            target_list=target_list,
            from_clause=query.from_clause,
            where_clause=query.where_clause,
            group_clause=query.group_clause,
            sort_clause=query.sort_clause,
            path_outputs=query.path_outputs.copy(),
            path_bonds=query.path_bonds.copy(),
            path_namespace=query.path_namespace.copy(),
            path_rvar_map=query.path_rvar_map.copy(),
            view_path_id_map=query.view_path_id_map.copy(),
            ptr_join_map=query.ptr_join_map.copy(),
            distinct_clause=[pgast.Constant(val=1)]
        )

        orig_sort = list(wrapper.sort_clause)
        for i, sortby in enumerate(wrapper.sort_clause):
            target_list.append(
                pgast.ResTarget(val=sortby.node, name=f's{i}')
            )

        wrapper.sort_clause = [pgast.SortBy(node=pgast.Constant(val=1))]

        wrapper_rvar = pgast.RangeSubselect(
            subquery=wrapper,
            alias=pgast.Alias(
                aliasname=ctx.genalias('csw')
            )
        )

        val_alias = ctx.genalias('v')
        target_list[0].name = val_alias
        ref = pgast.ColumnRef(
            name=[wrapper_rvar.alias.aliasname, val_alias]
        )

        query.target_list = [
            pgast.ResTarget(
                val=ref
            )
        ]
        query.from_clause = [wrapper_rvar]
        query.where_clause = pgast.NullTest(arg=ref, negated=True)
        query.group_clause = []
        query.sort_clause = []
        for i, orig_sortby in enumerate(orig_sort):
            query.sort_clause.append(
                pgast.SortBy(
                    node=pgast.ColumnRef(
                        name=[wrapper_rvar.alias.aliasname, f's{i}']
                    ),
                    dir=orig_sortby.dir,
                    nulls=orig_sortby.nulls
                )
            )
        self._reset_path_namespace(query)
        query.path_rvar_map.clear()
        self._pull_path_namespace(
            source=wrapper_rvar, target=query, pull_bonds=True)

    def _process_orderby(self, sorter):
        with self.context.new() as ctx:
            ctx.clause = 'orderby'
            query = ctx.query

            for expr in sorter:
                sortexpr = pgast.SortBy(
                    node=self.visit(expr.expr),
                    dir=expr.direction,
                    nulls=expr.nones_order)
                query.sort_clause.append(sortexpr)

    def _process_groupby(self, grouper):
        with self.context.new() as ctx:
            ctx.clause = 'groupby'
            query = ctx.query

            for expr in grouper:
                groupexpr = self.visit(expr)
                query.group_clause.append(groupexpr)

    def _get_column(self, rvar, colspec, *, naked=False, name=None):
        if isinstance(colspec, pgast.ColumnRef):
            colname = colspec.name[-1]
            nullable = colspec.nullable
            grouped = colspec.grouped
        else:
            colname = colspec
            nullable = rvar.nullable
            grouped = False

        if name is not None:
            colname = name

        if naked:
            name = [colname]
        else:
            name = [rvar.alias.aliasname, colname]

        return pgast.ColumnRef(name=name, nullable=nullable, grouped=grouped)

    def _rtlist_as_json_object(self, rtlist):
        keyvals = []

        if hasattr(rtlist.attmap[0], 'is_linkprop'):
            # This is a shape attribute map, use a specialized version.
            for i, pgexpr in enumerate(rtlist.targets):
                key = rtlist.attmap[i]
                if key.is_linkprop:
                    key = '@' + key.name
                else:
                    key = key.name
                keyvals.append(pgast.Constant(val=key))
                keyvals.append(pgexpr)
        else:
            # Simple rtlist
            for i, pgexpr in enumerate(rtlist.targets):
                keyvals.append(pgast.Constant(val=rtlist.attmap[i]))
                keyvals.append(pgexpr)

        return pgast.FuncCall(
            name=('jsonb_build_object',), args=keyvals)

    def _varlist_as_json_object(self, varlist):
        keyvals = []
        for var in varlist.vars:
            keyvals.append(pgast.Constant(val=var.name[-1]))
            keyvals.append(var)

        return pgast.FuncCall(
            name=('jsonb_build_object',), args=keyvals)

    def _get_var_for_set_expr(self, ir_set, source_rvar):
        ctx = self.context.current

        if isinstance(ir_set.expr, irast.Stmt):
            expr = ir_set.expr.result
        else:
            expr = ir_set.expr

        if isinstance(expr, irast.Struct):
            ctx = self.context.current

            targets = []
            attmap = []

            for rt in source_rvar.query.target_list:
                val = self._get_column(source_rvar, rt.name)
                attmap.append(rt.name)
                targets.append(val)

            rtlist = ResTargetList(targets, attmap)

            if ctx.expr_exposed and ctx.output_format == 'json':
                return self._rtlist_as_json_object(rtlist)
            else:
                return rtlist

        return self._get_rvar_path_var(source_rvar, ir_set, raw=False)

    def _get_var_for_atomic_set(self, ir_set):
        """Return an expression node corresponding to the specified atomic Set.

        Arguments:
            - ir_set: IR Set

        Return:
            An expression node representing a set of atom/schema
            values for the specified ir_set.
        """
        ctx = self.context.current

        ref = self._get_path_var(ctx.rel, ir_set.path_id)

        if ctx.in_aggregate:
            if isinstance(ir_set.scls, s_atoms.Atom):
                # Cast atom refs to the base type in aggregate expressions,
                # since PostgreSQL does not create array types for custom
                # domains and will fail to process a query with custom domains
                # appearing as array elements.
                pgtype = pg_types.pg_type_from_atom(
                    ctx.schema, ir_set.scls, topbase=True)
                pgtype = pgast.TypeName(name=pgtype)
                ref = pgast.TypeCast(arg=ref, type_name=pgtype)

        return ref

    def _full_inner_bond_condition(self, query, parent_path_bonds):
        ctx = self.context.current

        condition = None

        for path_id in query.path_bonds:
            rptr = path_id.rptr(ctx.schema)
            if rptr and rptr.singular(path_id.rptr_dir()):
                continue

            rref = parent_path_bonds.get(path_id)
            if rref is None:
                aliased = ctx.path_id_aliases.get(path_id)
                if aliased is not None:
                    rref = parent_path_bonds.get(aliased)

            if rref is None:
                continue

            rref = rref.get()

            lref = self._get_path_var(query, path_id)

            if rref.grouped:
                op = '='
                rref = pgast.SubLink(
                    type=pgast.SubLinkType.ANY,
                    expr=pgast.FuncCall(
                        name=('array_agg',),
                        args=[rref]
                    )
                )
            else:
                if lref.nullable or rref.nullable:
                    op = 'IS NOT DISTINCT FROM'
                else:
                    op = '='

            path_cond = self._new_binop(lref, rref, op=op)
            condition = self._extend_binop(condition, path_cond)

        return condition

    def _full_outer_bond_condition(self, query, right_rvar):
        condition = None
        ctx = self.context.current

        for path_id in right_rvar.path_bonds:
            rptr = path_id.rptr(ctx.schema)
            if rptr and rptr.singular(path_id.rptr_dir()):
                continue

            lref = self._maybe_get_path_var(query, path_id)
            if lref is None:
                continue

            rref = self._get_rvar_path_var(right_rvar, path_id)

            if lref.nullable or rref.nullable:
                op = 'IS NOT DISTINCT FROM'
            else:
                op = '='

            path_cond = self._new_binop(lref, rref, op=op)
            condition = self._extend_binop(condition, path_cond)

        return condition

    def _rel_join(self, query, right_rvar, type='inner', front=False):
        if not query.from_clause:
            query.from_clause.append(right_rvar)
            return

        condition = self._full_outer_bond_condition(query, right_rvar)

        if type == 'where':
            # A "where" JOIN is equivalent to an INNER join with
            # its condition moved to a WHERE clause.
            if condition is not None:
                query.where_clause = self._extend_binop(
                    query.where_clause, condition)

            if front:
                query.from_clause.insert(0, right_rvar)
            else:
                query.from_clause.append(right_rvar)
        else:
            if condition is None:
                type = 'cross'

            if front:
                larg = right_rvar
                rarg = query.from_clause[0]
            else:
                larg = query.from_clause[0]
                rarg = right_rvar

            query.from_clause[0] = pgast.JoinExpr(
                type=type, larg=larg, rarg=rarg, quals=condition)
            if type == 'left':
                right_rvar.nullable = True

    def _join_mapping_rel(self, *, stmt, set_rvar, ir_set,
                          map_join_type='inner'):
        fromexpr = stmt.from_clause[0]

        link = ir_set.rptr
        if isinstance(link.ptrcls, s_lprops.LinkProperty):
            link = link.source.rptr
            link_path_id = ir_set.path_id[:-3]
        else:
            link_path_id = ir_set.path_id[:-1]

        try:
            # The same link map must not be joined more than once,
            # otherwise the cardinality of the result set will be wrong.
            #
            map_rvar = stmt.path_rvar_map[link_path_id]
            map_join = stmt.ptr_join_map[link_path_id]
        except KeyError:
            map_rvar = self._range_for_pointer(link)
            map_join = None
            if map_join_type == 'left':
                map_rvar.nullable = True

        # Set up references according to link direction
        #
        src_col = common.edgedb_name_to_pg_name('std::source')
        source_ref = self._get_column(map_rvar, src_col)

        tgt_col = common.edgedb_name_to_pg_name('std::target')
        target_ref = self._get_column(map_rvar, tgt_col)

        valent_bond = self._get_path_var(stmt, link.source.path_id)
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
                type=map_join_type,
                quals=map_join_cond
            )

            self._put_path_rvar(stmt, link_path_id, map_rvar)
            stmt.ptr_join_map[link_path_id] = map_join

        if isinstance(ir_set.scls, s_concepts.Concept):
            if map_join_type == 'left':
                set_rvar.nullable = True

            # Join the target relation, if we have it
            target_range_bond = self._get_rvar_path_var(
                set_rvar, ir_set.path_id)

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
                    type=map_join_type,
                    larg=pre_map_join,
                    rarg=set_rvar,
                    quals=cond_expr)
                map_join.copyfrom(new_map_join)

        stmt.from_clause[0] = map_join

        return map_rvar

    def _join_class_rel(self, *, stmt, set_rvar, ir_set):
        fromexpr = stmt.from_clause[0]

        nref = self._get_column(
            set_rvar, common.edgedb_name_to_pg_name('schema::name'))

        val = pgast.Constant(
            val=ir_set.rptr.source.scls.name
        )

        cond_expr = self._new_binop(nref, val, op='=')

        stmt.from_clause[0] = pgast.JoinExpr(
            type='inner',
            larg=fromexpr,
            rarg=set_rvar,
            quals=cond_expr)

    def _join_inline_rel(self, *, stmt, set_rvar, ir_set, back_id_col):
        if ir_set.rptr.direction == s_pointers.PointerDirection.Inbound:
            id_col = back_id_col
            src_ref = self._get_path_var(stmt, ir_set.rptr.source.path_id)
        else:
            id_col = common.edgedb_name_to_pg_name('std::id')
            src_ref = self._get_path_var(stmt, ir_set.path_id)

        tgt_ref = self._get_column(set_rvar, id_col)

        fromexpr = stmt.from_clause[0]

        cond_expr = self._new_binop(src_ref, tgt_ref, op='=')

        stmt.from_clause[0] = pgast.JoinExpr(
            type='inner',
            larg=fromexpr,
            rarg=set_rvar,
            quals=cond_expr)

    def _for_each_query_in_set(self, qry, cb):
        if qry.op:
            self._for_each_query_in_set(qry.larg, cb)
            self._for_each_query_in_set(qry.rarg, cb)
        else:
            cb(qry)

    def _reset_path_namespace(self, query):
        query.path_namespace.clear()
        query.path_outputs.clear()
        query.path_bonds.clear()

    def _remove_path_from_namespace(self, query, path_id):
        query.path_namespace.pop(path_id, None)
        query.path_outputs.pop(path_id, None)
        query.inner_path_bonds.pop(path_id, None)
        query.path_bonds.pop(path_id, None)

    def _map_path_id(self, path_id, path_id_map):
        for outer_id, inner_id in path_id_map.items():
            new_path_id = path_id.replace_prefix(outer_id, inner_id)
            if new_path_id != path_id:
                path_id = new_path_id
                break

        return path_id

    def _reverse_map_path_id(self, path_id, path_id_map):
        for outer_id, inner_id in path_id_map.items():
            new_path_id = path_id.replace_prefix(inner_id, outer_id)
            if new_path_id != path_id:
                path_id = new_path_id
                break

        return path_id

    def _pull_path_namespace(self, *, target, source, pull_bonds=True):
        ctx = self.context.current

        squery = source.query
        for path_id in squery.path_rvar_map:
            path_id = self._reverse_map_path_id(
                path_id, target.view_path_id_map)
            if path_id not in target.path_rvar_map:
                self._put_path_rvar(target, path_id, source)

        if pull_bonds:
            for path_id in squery.path_bonds:
                path_id = self._reverse_map_path_id(
                    path_id, target.view_path_id_map)
                self._put_path_bond(target, path_id)
                bond = LazyPathVarRef(self._get_rvar_path_var, source, path_id)
                ctx.path_bonds[path_id] = bond
                ctx.path_bonds_by_stmt[ctx.stmt][path_id] = bond

    def _get_path_var(self, rel, path_id):
        """Make sure the value of the *ir_set* path is present in namespace."""
        ctx = self.context.current

        if isinstance(rel, pgast.CommonTableExpr):
            rel = rel.query

        if isinstance(path_id[-1], s_concepts.Concept):
            path_id = self._get_id_path_id(path_id)

        if path_id in rel.path_namespace:
            return rel.path_namespace[path_id]

        if rel.as_type:
            # Relation represents the result of a type filter ([IS Type]).
            near_endpoint = rel.as_type[0]
        else:
            near_endpoint = None

        ptrcls = path_id.rptr(ctx.schema, near_endpoint)
        if ptrcls is not None:
            ptrname = ptrcls.shortname
            alias = common.edgedb_name_to_pg_name(ptrname)
        else:
            ptrname = None
            alias = common.edgedb_name_to_pg_name(path_id[-1].name)

        if getattr(rel, 'op', None) is not None:
            cb = functools.partial(
                self._get_path_output,
                path_id=path_id,
                alias=alias)

            self._for_each_query_in_set(rel, cb)
            self._put_path_output(rel, path_id, alias)
            return pgast.ColumnRef(name=[alias])

        ptr_info = parent_ptr_info = parent_ptrcls = None
        if ptrcls is not None:
            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, resolve_type=False, link_bias=False)

            parent_ptrcls = irast.PathId(path_id[:-2]).rptr(ctx.schema)
            if parent_ptrcls is not None:
                parent_ptr_info = pg_types.get_pointer_storage_info(
                    parent_ptrcls, resolve_type=False, link_bias=False)

        if isinstance(ptrcls, s_lprops.LinkProperty):
            if ptr_info.table_type == 'link':
                # This is a regular linkprop, step back to link rvar
                src_path_id = path_id[:-3]
            else:
                # This is a link prop that is stored in source rel,
                # step back to link source rvar.
                src_path_id = path_id[:-4]
        else:
            if ptrcls is not None:
                if (parent_ptr_info is not None and
                        parent_ptr_info.table_type == 'concept' and
                        ptrname == 'std::id'):
                    # Link to object with target stored directly in
                    # source table.
                    src_path_id = path_id[:-4]
                    ptr_info = parent_ptr_info
                else:
                    # Regular atomic link, step back to link source rvar.
                    src_path_id = path_id[:-2]
            else:
                if len(path_id) > 1:
                    # Couldn't resolve a specific pointer for path_id,
                    # assume this is a valid path with type filter.
                    src_path_id = path_id[:-2]
                else:
                    # This is an atomic set derived from an expression.
                    src_path_id = path_id

        rel_rvar = rel.path_rvar_map.get(src_path_id)
        if rel_rvar is None:
            raise LookupError(
                f'cannot find source range for path {src_path_id} in {rel}')

        colname = None

        if self._should_recurse_into_rvar(rel_rvar):
            source_rel = rel_rvar.query

            drilldown_path_id = self._map_path_id(
                path_id, rel.view_path_id_map)

            if source_rel in ctx.root_rels:
                assert len(source_rel.path_bonds) == 1
                if not drilldown_path_id.is_concept_path():
                    outer_path_id = drilldown_path_id.src_path()
                else:
                    outer_path_id = drilldown_path_id

                path_id_map = {
                    outer_path_id: next(iter(source_rel.path_bonds))
                }

                drilldown_path_id = self._map_path_id(
                    drilldown_path_id, path_id_map)

            colname = self._get_path_output(source_rel, drilldown_path_id)

        else:
            if not isinstance(ptrcls, s_lprops.LinkProperty):
                path_src = path_id[-3]
                ptr_src = ptrcls.source
                src_path_id = irast.PathId(path_id[:-2])
                if path_src != ptr_src and not path_src.issubclass(ptr_src):
                    poly_rvar = self._range_for_concept(ptr_src, src_path_id)
                    poly_rvar.nullable = True
                    poly_rvar.path_bonds.add(src_path_id)
                    self._rel_join(rel, poly_rvar, type='left')

                    rel_rvar = poly_rvar

            colname = ptr_info.column_name

        fieldref = self._get_column(rel_rvar, colname)

        self._put_path_var(rel, path_id, fieldref)

        return fieldref

    def _should_recurse_into_rvar(self, rvar):
        return (
            isinstance(rvar, pgast.RangeSubselect) or
            isinstance(rvar.relation, pgast.CommonTableExpr)
        )

    def _maybe_get_path_var(self, rel, path_id):
        try:
            return self._get_path_var(rel, path_id)
        except LookupError:
            return None

    def _proper_path_id(self, path_id):
        if isinstance(path_id, irast.Set):
            ir_set = path_id
            path_id = ir_set.path_id
            if not path_id:
                path_id = irast.PathId([ir_set.scls])

        if isinstance(path_id[-1], s_concepts.Concept):
            path_id = self._get_id_path_id(path_id)

        return path_id

    def _put_path_var(self, stmt, path_id, ref):
        path_id = self._proper_path_id(path_id)
        canonical_path_id = self._get_canonical_path_id(path_id)
        assert isinstance(ref, pgast.ColumnRef)
        stmt.path_namespace[path_id] = \
            stmt.path_namespace[canonical_path_id] = ref

    def _put_path_bond(self, stmt, path_id):
        if isinstance(path_id[-1], s_concepts.Concept):
            # Only Concept paths form bonds.
            stmt.path_bonds.add(path_id)

    def _get_path_output(self, rel, path_id, *, alias=None, raw=False):
        ctx = self.context.current

        path_id = self._proper_path_id(path_id)

        result = rel.path_outputs.get((path_id, raw))
        if result is None and not raw:
            result = rel.path_outputs.get((path_id, True))
        if result is not None:
            return result

        ref = self._get_path_var(rel, path_id)
        set_op = getattr(rel, 'op', None)
        if set_op is not None:
            alias = ref.name[0]

        if alias is None:
            rptr = path_id.rptr(ctx.schema)
            if rptr is not None:
                ptrname = rptr.shortname
                alias = ctx.genalias(hint=ptrname.name)
            else:
                alias = ctx.genalias(hint=path_id[-1].name.name)

        if set_op is None:
            restarget = pgast.ResTarget(name=alias, val=ref)
            if hasattr(rel, 'returning_list'):
                rel.returning_list.append(restarget)
            else:
                rel.target_list.append(restarget)

        self._put_path_output(rel, path_id, alias, raw=raw)

        return alias

    def _put_path_output(self, stmt, path_id, alias, *, raw=False):
        path_id = self._proper_path_id(path_id)
        canonical_path_id = self._get_canonical_path_id(path_id)
        stmt.path_outputs[path_id, raw] = \
            stmt.path_outputs[canonical_path_id, raw] = alias

    def _get_rvar_path_var(self, rvar, path_id, raw=True):
        ctx = self.context.current
        path_id = self._proper_path_id(path_id)
        if isinstance(rvar.query, pgast.Relation):
            if isinstance(path_id[-1], s_concepts.Concept):
                path_id = self._get_id_path_id(path_id)
            ptr = path_id.rptr(ctx.schema)
            name = common.edgedb_name_to_pg_name(ptr.shortname)
        else:
            name = self._get_path_output(rvar.query, path_id, raw=raw)

        return pgast.ColumnRef(name=[rvar.alias.aliasname, name])

    def _put_path_rvar(self, stmt, path_id, rvar):
        assert path_id
        path_id = self._get_canonical_path_id(self._proper_path_id(path_id))
        stmt.path_rvar_map[path_id] = rvar

    def _rvar_for_rel(self, rel, lateral=False):
        ctx = self.context.current

        if isinstance(rel, pgast.Query):
            rvar = pgast.RangeSubselect(
                subquery=rel,
                alias=pgast.Alias(
                    aliasname=ctx.genalias(hint='q')
                ),
                lateral=lateral
            )
        else:
            rvar = pgast.RangeVar(
                relation=rel,
                alias=pgast.Alias(
                    aliasname=ctx.genalias(hint=rel.name)
                )
            )

        return rvar

    def _run_codegen(self, qtree):
        codegen = pgcodegen.SQLSourceGenerator()
        try:
            codegen.visit(qtree)
        except pgcodegen.SQLSourceGeneratorError as e:  # pragma: no cover
            ctx = pgcodegen.SQLSourceGeneratorContext(
                qtree, codegen.result)
            edgedb_error.add_context(e, ctx)
            raise
        except Exception as e:  # pragma: no cover
            ctx = pgcodegen.SQLSourceGeneratorContext(
                qtree, codegen.result)
            err = pgcodegen.SQLSourceGeneratorError(
                'error while generating SQL source')
            edgedb_error.add_context(err, ctx)
            raise err from e

        return codegen
