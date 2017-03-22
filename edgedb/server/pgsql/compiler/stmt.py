##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import functools

from edgedb.lang.common import exceptions as edgedb_error

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import name as s_name
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
    def __init__(self, getter, source, path_id, *, grouped=False, weak=False):
        self.path_id = path_id
        self.source = source
        self.getter = getter
        self.grouped = grouped
        self.weak = weak
        self._ref = None

    def get(self):
        if self._ref is None:
            ref = self.getter(self.source, self.path_id)
            if self.grouped or self.weak:
                ref = pgast.ColumnRef(
                    name=ref.name,
                    nullable=ref.nullable,
                    grouped=self.grouped,
                    weak=self.weak
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
                  ignore_shapes=False, optimize=False, timer=None):

        if timer is None:
            qtree = self.transform_to_sql_tree(
                ir_expr, schema=schema, backend=backend,
                output_format=output_format, ignore_shapes=ignore_shapes)
        else:
            with timer.timeit('compile_ir_to_sql'):
                qtree = self.transform_to_sql_tree(
                    ir_expr, schema=schema, backend=backend,
                    output_format=output_format, ignore_shapes=ignore_shapes)

        if debug.flags.edgeql_compile:  # pragma: no cover
            debug.header('SQL Tree')
            debug.dump(qtree)

        if optimize:
            if debug.flags.edgeql_optimize:  # pragma: no cover
                debug.header('SQL Tree before optimization')
                debug.dump(qtree, _ast_include_meta=False)

                codegen = self._run_codegen(qtree)
                qchunks = codegen.result
                debug.header('SQL before optimization')
                debug.dump_code(''.join(qchunks), lexer='sql')

            opt = pg_opt.Optimizer()
            if timer is None:
                qtree = opt.optimize(qtree)
            else:
                with timer.timeit('optimize'):
                    qtree = opt.optimize(qtree)

        argmap = self.context.current.argmap

        # Generate query text
        if timer is None:
            codegen = self._run_codegen(qtree)
        else:
            with timer.timeit('compile_ir_to_sql'):
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

    def _init_stmt(self, stmt, ctx, parent_ctx):
        if ctx.toplevel_stmt is None:
            ctx.toplevel_stmt = ctx.stmt

        if stmt.aggregated_scope:
            ctx.aggregated_scope = stmt.aggregated_scope

        ctx.stmt_path_scope = stmt.path_scope.copy()

        ctx.stmt_specific_path_scope = \
            {s for s in stmt.specific_path_scope
             if s.path_id in ctx.stmt_path_scope}

        if stmt.parent_stmt is not None:
            ctx.parent_stmt_path_scope = stmt.parent_stmt.path_scope.copy()

        ctx.stmtmap[stmt] = ctx.stmt
        ctx.stmt_hierarchy[ctx.stmt] = parent_ctx.stmt

        ctx.stmt.view_path_id_map = parent_ctx.view_path_id_map.copy()

    def visit_SelectStmt(self, stmt):
        parent_ctx = self.context.current

        with self.context.substmt() as ctx:
            self._init_stmt(stmt, ctx, parent_ctx)

            query = ctx.query

            # Process the result expression;
            self._process_selector(stmt.result)

            if len(query.target_list) == 1:
                resalias = self._ensure_query_restarget_name(query)
                self._put_path_output(query, stmt.result.path_id, resalias)

            if query is not ctx.toplevel_stmt:
                specific_scope = {s for s in ctx.stmt_specific_path_scope
                                  if s.path_id in ctx.parent_path_bonds}

                for ir_set in specific_scope:
                    if (isinstance(ir_set.scls, s_concepts.Concept) and
                            ir_set.path_id not in query.path_bonds):
                        # The selector does not include this path explicitly,
                        # so we must do so here.
                        cte = self._set_to_cte(ir_set)
                        self._include_range(
                            ctx.rel, cte, join_type='left',
                            replace_bonds=False)

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
                query = self._ensure_correct_set(
                    stmt, query, query is ctx.toplevel_stmt)

        return query

    def visit_GroupStmt(self, stmt):
        parent_ctx = self.context.current

        with self.context.substmt() as ctx:
            self._init_stmt(stmt, ctx, parent_ctx)

            c = s_concepts.Concept(
                name=s_name.Name(
                    module='__group__', name=ctx.genalias('Group')),
                bases=[ctx.schema.get('std::Object')]
            )
            c.acquire_ancestor_inheritance(ctx.schema)

            group_path_id = irast.PathId([c])

            ctx.stmt_path_scope = ctx.stmt_path_scope.copy()
            ctx.stmt_path_scope[group_path_id] = 1
            ctx.parent_stmt_path_scope[group_path_id] = 1

            # Process the GROUP .. BY part into a subquery.
            with self.context.subquery() as gctx:
                gquery = gctx.query
                self.visit(stmt.subject)

                group_paths = set()

                part_clause = []

                for expr in stmt.groupby:
                    with self.context.new() as subctx:
                        subctx.path_bonds = gctx.parent_path_bonds.copy()
                        partexpr = self.visit(expr)

                    part_clause.append(partexpr)

                    if expr.expr is None:
                        group_paths.add(expr)

                # Since we will be computing arbitrary expressions
                # based on the grouped sets, it is more efficient
                # to compute the "group bond" as a small unique
                # value than it is to use GROUP BY and aggregate
                # actual id values into an array.
                #
                # To achieve this we use the first_value() window
                # function while using the GROUP BY clause as
                # a partition clause.  We use the id of the first
                # object in each partition if GROUP BY input is
                # a Concept, otherwise we generate the id using
                # row_number().
                if isinstance(stmt.subject.scls, s_concepts.Concept):
                    first_val = self._get_path_var(
                        gquery, stmt.subject.path_id)
                else:
                    pass

                group_id = pgast.FuncCall(
                    name=('first_value',),
                    args=[first_val],
                    over=pgast.WindowDef(
                        partition_clause=part_clause
                    )
                )

                gid_alias = ctx.genalias('gid')
                gquery.target_list.append(
                    pgast.ResTarget(
                        val=group_id,
                        name=gid_alias
                    )
                )

                self._put_path_output(gquery, group_path_id, gid_alias,
                                      raw=True)
                self._put_path_bond(gquery, group_path_id)

            group_cte = pgast.CommonTableExpr(
                query=gquery,
                name=ctx.genalias('g')
            )

            # Generate another subquery contaning distinct values of
            # path expressions in BY.
            with self.context.subquery() as gvctx:
                gvctx.stmt_path_scope = collections.defaultdict(int)
                gvctx.stmt_path_scope[group_path_id] = 1

                self._put_set_cte(stmt.subject, group_cte)

                for group_set in stmt.groupby:
                    if group_set.expr is None:
                        group_expr = self.visit(group_set)
                        path_id = group_set.path_id
                        alias = self._get_path_output_alias(path_id)
                        gvctx.query.target_list.append(
                            pgast.ResTarget(
                                val=group_expr,
                                name=alias
                            )
                        )
                        self._put_path_output(gvctx.query, path_id, alias,
                                              raw=True)
                        self._put_path_bond(gvctx.query, path_id)
                        gvctx.stmt_path_scope[path_id] = 1

                for path_id in list(gvctx.query.path_rvar_map):
                    c_path_id = self._get_canonical_path_id(path_id)
                    if c_path_id not in gvctx.stmt_path_scope:
                        gvctx.query.path_rvar_map.pop(path_id)

                gvctx.query.distinct_clause = [
                    self._get_path_var(gvctx.query, group_path_id)
                ]

            groupval_cte = pgast.CommonTableExpr(
                query=gvctx.query,
                name=ctx.genalias('gv')
            )

            o_stmt = stmt.result.expr

            # process the result expression;
            with self.context.subquery() as selctx:

                selctx.stmt_path_scope = o_stmt.path_scope.copy()
                selctx.stmt_path_scope[group_path_id] = 1

                selctx.stmt_specific_path_scope = \
                    {s for s in o_stmt.specific_path_scope
                     if s.path_id in selctx.stmt_path_scope}

                selctx.parent_stmt_path_scope = ctx.parent_stmt_path_scope

                selctx.query.ctes.append(group_cte)
                self._put_set_cte(stmt.subject, group_cte)
                # When GROUP subject appears in aggregates, which by
                # default use lax paths, we still want to use the group
                # CTE as the source.
                self._put_set_cte(stmt.subject, group_cte, lax=True)

                sortoutputs = []

                selctx.query.ctes.append(groupval_cte)
                for grouped_set in group_paths:
                    self._put_set_cte(grouped_set, groupval_cte)

                self._process_selector(o_stmt.result)

                self._enforce_path_scope(
                    selctx.query, selctx.parent_path_bonds)

                # The WHERE clause
                if o_stmt.where:
                    with self.context.new() as ctx1:
                        selctx.clause = 'where'
                        selctx.query.where_clause = self.visit(o_stmt.where)

                for ir_sortexpr in o_stmt.orderby:
                    alias = ctx.genalias('s')
                    sexpr = self.visit(ir_sortexpr.expr)
                    selctx.query.target_list.append(
                        pgast.ResTarget(
                            val=sexpr,
                            name=alias
                        )
                    )
                    sortoutputs.append(alias)

            query = ctx.query
            result_rvar = self._include_range(
                query, selctx.query, lateral=True)

            for rt in selctx.query.target_list:
                if rt.name is None:
                    rt.name = ctx.genalias('v')
                if rt.name not in sortoutputs:
                    query.target_list.append(
                        pgast.ResTarget(
                            val=self._get_column(result_rvar, rt.name),
                            name=rt.name
                        )
                    )

            if len(query.target_list) == 1:
                resalias = self._ensure_query_restarget_name(query)
                self._put_path_output(query, o_stmt.result.path_id, resalias)

            for i, expr in enumerate(o_stmt.orderby):
                sort_ref = self._get_column(result_rvar, sortoutputs[i])
                sortexpr = pgast.SortBy(
                    node=sort_ref,
                    dir=expr.direction,
                    nulls=expr.nones_order)
                query.sort_clause.append(sortexpr)

            # The OFFSET clause
            if o_stmt.offset:
                with self.context.new() as ctx1:
                    ctx1.clause = 'offsetlimit'
                    ctx1.output_format = None
                    query.limit_offset = self.visit(o_stmt.offset)

            # The LIMIT clause
            if o_stmt.limit:
                with self.context.new() as ctx1:
                    ctx1.clause = 'offsetlimit'
                    ctx1.output_format = None
                    query.limit_count = self.visit(o_stmt.limit)

            if not parent_ctx.correct_set_assumed:
                query = self._ensure_correct_set(
                    stmt, query, query is ctx.toplevel_stmt)

        return query

    def _visit_shape(self, ir_set):
        ctx = self.context.current

        my_elements = []
        attribute_map = []
        idref = None

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
                newctx.in_shape = True
                newctx.path_bonds = newctx.path_bonds.copy()

                if (not is_singleton or irutils.is_subquery_set(cset) or
                        irutils.is_inner_view_reference(cset) or
                        isinstance(cset.scls, s_concepts.Concept)):
                    with self.context.subquery() as qctx:
                        element = self.visit(e)
                        qctx.query.target_list = [
                            pgast.ResTarget(
                                val=element
                            )
                        ]

                    self._enforce_path_scope(qctx.query, newctx.path_bonds)

                    if not is_singleton:
                        # Auto-aggregate non-singleton computables.
                        element = self._aggregate_result(qctx.query)
                    else:
                        element = qctx.query
                else:
                    element = self.visit(e)

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

    def _include_range(self, stmt, rel, join_type='inner', lateral=False,
                       replace_bonds=True):
        """Ensure the *rel* is present in the from_clause of *stmt*.

        :param stmt:
            The statement to include *rel* in.

        :param rel:
            The relation node to join.

        :param join_type:
            JOIN type to use when including *rel*.

        :param lateral:
            Whether *rel* should be joined laterally.

        :param replace_bonds:
            Whether the path bonds in *stmt* should be replaced.

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
            target=stmt, source=rvar, replace_bonds=replace_bonds)

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
                    val=target,
                    name=ctx.genalias('v')
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

    def _put_parent_var_scope(self, ir_set, var):
        ctx = self.context.current
        ir_set = irutils.get_canonical_set(ir_set)
        if ir_set not in ctx.parent_var_scope:
            ctx.parent_var_scope[ir_set] = var

    def _get_parent_var_scope(self, ir_set):
        ctx = self.context.current
        ir_set = irutils.get_canonical_set(ir_set)
        return ctx.parent_var_scope.get(ir_set)

    def _put_set_cte(self, ir_set, cte, *, lax=None):
        ctx = self.context.current
        if lax is None:
            lax = ctx.lax_paths

        ir_set = irutils.get_canonical_set(ir_set)

        if ir_set.rptr is not None and ir_set.expr is None:
            key = (ir_set, lax)
        else:
            key = (ir_set, False)

        ctx.ctemap[key] = cte
        ctx.ctemap_by_stmt[ctx.stmt][key] = cte

        if (ir_set.expr is None and
                ctx.clause in {'where', 'result'} and
                not ctx.in_shape):
            if lax or not ctx.setscope.get(ir_set):
                ctx.setscope[ir_set] = lax

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

        return ctx.ctemap.get(key)

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

        cte_name = ctx.genalias(hint=self._get_set_cte_alias(ir_set))
        cte = pgast.CommonTableExpr(query=stmt, name=cte_name)

        self._put_set_cte(ir_set, cte)

        with self.context.new() as ctx:
            ctx.rel = stmt
            ctx.path_bonds = ctx.path_bonds.copy()

            if self._get_parent_var_scope(ir_set) is not None:
                self._process_set_as_parent_var_scope(ir_set, stmt)

            elif self._get_parent_range_scope(ir_set) is not None:
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

            elif isinstance(ir_set.expr, irast.Struct):
                # Named tuple
                self._process_set_as_named_tuple(ir_set, stmt)

            elif isinstance(ir_set.expr, irast.StructIndirection):
                # Named tuple indirection.
                self._process_set_as_named_tuple_indirection(ir_set, stmt)

            elif isinstance(ir_set.expr, irast.FunctionCall):
                if ir_set.expr.func.aggregate:
                    # Call to an aggregate function.
                    self._process_set_as_agg_expr(ir_set, stmt)
                else:
                    # Regular function call.
                    self._process_set_as_func_expr(ir_set, stmt)

            elif isinstance(ir_set.expr, irast.ExistPred):
                # EXISTS(), which is a special kind of an aggregate.
                self._process_set_as_exists_expr(ir_set, stmt)

            elif ir_set.expr is not None:
                # All other expressions.
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
        ctx = self.context.current

        if not isinstance(ir_set.scls, s_concepts.Concept):
            return None

        if set_rvar is None:
            set_rvar = self._range_for_set(ir_set)
            set_rvar.nullable = nullable
            set_rvar.path_bonds.add(ir_set.path_id)

        self._put_path_rvar(stmt, ir_set.path_id, set_rvar)

        if ir_set.path_id in ctx.stmt_path_scope:
            self._put_path_bond(stmt, ir_set.path_id)

        return set_rvar

    def _process_set_as_root(self, ir_set, stmt):
        """Populate the CTE for a Set defined by a path root."""
        ctx = self.context.current

        set_rvar = self._get_root_rvar(ir_set, stmt)
        stmt.from_clause.append(set_rvar)
        self._enforce_path_scope(stmt, ctx.parent_path_bonds)
        ctx.query.ctes.append(self._get_set_cte(ir_set))

    def _process_set_as_parent_var_scope(self, ir_set, stmt):
        ctx = self.context.current

        var = self._get_parent_var_scope(ir_set)
        alias = ctx.genalias('v')
        stmt.target_list.append(
            pgast.ResTarget(
                val=var,
                name=alias
            )
        )

        self._put_path_output(stmt, ir_set, alias)
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
        self._pull_path_namespace(target=stmt, source=set_rvar)

        if isinstance(parent_rvar, pgast.RangeVar):
            parent_scope = {}
            for path_id in parent_rvar.path_bonds:
                parent_scope[path_id] = LazyPathVarRef(
                    self._get_rvar_path_var, parent_rvar, path_id)
                parent_scope[path_id].grouped = grouped

            self._enforce_path_scope(stmt, parent_scope)

        ctx.query.ctes.append(self._get_set_cte(ir_set))

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
                    map_join_type = 'left' if ctx.lax_paths else 'inner'

                    self._join_inline_rel(
                        stmt=stmt, set_rvar=set_rvar, ir_set=ir_set,
                        back_id_col=ptr_info.column_name,
                        join_type=map_join_type)

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

        if not isinstance(ir_set.scls, s_obj.Struct):
            rt_name = self._ensure_query_restarget_name(
                subquery, hint=cte.name)
            self._put_path_output(subquery, ir_set, rt_name)

        cte.query = subquery
        ctx.toplevel_stmt.ctes.append(cte)

    def _process_set_as_subquery(self, ir_set, stmt):
        """Populate the CTE for Set defined by a subquery."""
        ctx = self.context.current
        cte = self._get_set_cte(ir_set)

        with self.context.new() as newctx:
            newctx.path_bonds = ctx.path_bonds.copy()

            if irutils.is_strictly_view_set(ir_set.expr.result):
                outer_id = ir_set.path_id
                inner_id = ir_set.expr.result.path_id

                newctx.view_path_id_map = {
                    outer_id: inner_id
                }

            subquery = self.visit(ir_set.expr)

        if not isinstance(ir_set.expr, irast.MutatingStmt):
            if not isinstance(ir_set.scls, s_obj.Struct):
                for path_id in list(subquery.path_bonds):
                    if not path_id.startswith(ir_set.path_id):
                        subquery.path_bonds.discard(path_id)

                rt_name = self._ensure_query_restarget_name(
                    subquery, hint=cte.name)
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
        subquery = self._ensure_correct_set(inner_set.expr, subquery)

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
            newctx.path_bonds = ctx.parent_path_bonds.copy()
            newctx.view_path_id_map = {
                ir_set.path_id: expr.left.result.path_id
            }
            larg = self.visit(expr.left)
            newctx.path_bonds = ctx.parent_path_bonds.copy()
            newctx.view_path_id_map = {
                ir_set.path_id: expr.right.result.path_id
            }
            rarg = self.visit(expr.right)

        with self.context.subquery() as subctx:
            subqry = subctx.query
            subqry.op = pgast.PgSQLSetOperator(expr.op)
            subqry.all = True
            subqry.larg = larg
            subqry.rarg = rarg

            rt_name = self._ensure_query_restarget_name(subqry)
            self._put_path_output(subqry, ir_set, rt_name)
            self._put_path_rvar(subqry, ir_set, None)

            sub_rvar = pgast.RangeSubselect(
                subquery=subqry,
                alias=pgast.Alias(
                    aliasname=ctx.genalias('u')
                )
            )

        self._pull_path_namespace(target=stmt, source=sub_rvar)
        stmt.from_clause = [sub_rvar]

        cte = self._get_set_cte(ir_set)
        cte.query = stmt
        ctx.query.ctes.append(cte)

    def _process_set_as_named_tuple(self, ir_set, stmt):
        """Populate the CTE for Set defined by a named tuple."""
        ctx = self.context.current

        expr = ir_set.expr

        with self.context.new() as subctx:
            for element in expr.elements:
                subctx.path_bonds = ctx.parent_path_bonds.copy()
                el_ref = self.visit(element.val)
                stmt.target_list.append(
                    pgast.ResTarget(
                        name=common.edgedb_name_to_pg_name(element.name),
                        val=el_ref
                    )
                )

        ctx.query.ctes.append(self._get_set_cte(ir_set))

    def _process_set_as_named_tuple_indirection(self, ir_set, stmt):
        """Populate the CTE for Set defined by a named tuple indirection."""

        expr = ir_set.expr

        with self.context.new() as ctx:
            ctx.expr_exposed = False
            self.visit(expr.expr)
            tuple_cte = self._get_set_cte(expr.expr)

        self._put_set_cte(ir_set, tuple_cte)

    def _process_set_as_typefilter(self, ir_set, stmt):
        """Populate the CTE for Set defined by a Expr[IS Type] expression."""
        ctx = self.context.current

        root_rvar = self._get_root_rvar(ir_set, stmt)
        stmt.from_clause.append(root_rvar)
        self._put_path_rvar(stmt, ir_set.expr.expr.path_id, root_rvar)
        self.visit(ir_set.expr.expr)
        stmt.as_type = irast.PathId([ir_set.scls])

        ctx.query.ctes.append(self._get_set_cte(ir_set))

    def _process_set_as_expr(self, ir_set, stmt):
        """Populate the CTE for Set defined by an expression."""

        ctx = self.context.current

        with self.context.new() as newctx:
            newctx.in_set_expr = True
            newctx.rel = stmt
            set_expr = self.visit(ir_set.expr)

        if isinstance(set_expr, ResTargetList):
            for i, rt in enumerate(set_expr.targets):
                stmt.target_list.append(
                    pgast.ResTarget(val=rt, name=set_expr.attmap[i])
                )
        else:
            self._ensure_correct_rvar_for_expr(ir_set, stmt, set_expr)

        if self._apply_path_bond_injections(stmt):
            # Due to injection this rel must not be a CTE.
            self._put_set_cte(ir_set, stmt)
        else:
            ctx.query.ctes.append(self._get_set_cte(ir_set))

    def _process_set_as_func_expr(self, ir_set, stmt):
        """Populate the CTE for Set defined by a function call."""
        ctx = self.context.current

        with self.context.new() as newctx:
            newctx.in_set_expr = True
            newctx.rel = stmt
            newctx.in_member_test = False
            newctx.expr_exposed = False

            expr = ir_set.expr
            funcobj = expr.func

            args = []

            for ir_arg in ir_set.expr.args:
                arg_ref = self.visit(ir_arg)
                args.append(arg_ref)

            if funcobj.from_function:
                name = (funcobj.from_function,)
            else:
                name = (
                    common.edgedb_module_name_to_schema_name(
                        funcobj.shortname.module),
                    common.edgedb_name_to_pg_name(
                        funcobj.shortname.name)
                )

            set_expr = pgast.FuncCall(name=name, args=args)

        self._ensure_correct_rvar_for_expr(ir_set, stmt, set_expr)

        if self._apply_path_bond_injections(stmt):
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
            newctx.in_member_test = False

            path_scope = set(ctx.stmt_path_scope)

            newctx.stmt_path_scope = newctx.stmt_path_scope.copy()
            newctx.stmt_path_scope.update(ir_set.path_scope)
            newctx.stmt_specific_path_scope = \
                newctx.stmt_specific_path_scope.copy()
            newctx.stmt_specific_path_scope.update(ir_set.stmt_path_scope)

            expr = ir_set.expr
            funcobj = expr.func
            agg_filter = None
            agg_sort = []

            with self.context.new() as argctx:
                argctx.in_aggregate = True
                argctx.lax_paths = True

                # We want array_agg() (and similar) to do the right
                # thing with respect to output format, so, barring
                # the (unacceptable) hardcoding of function names,
                # check if the aggregate accepts a single argument
                # of std::any to determine serialized input safety.
                serialization_safe = (
                    len(funcobj.paramtypes) == 1 and
                    funcobj.paramtypes[0].name == 'std::any'
                )

                if not serialization_safe:
                    argctx.expr_exposed = False

                args = []

                for ir_arg in ir_set.expr.args:
                    arg_ref = self.visit(ir_arg)

                    if (isinstance(ir_arg.scls, s_atoms.Atom) and
                            ir_arg.scls.bases):
                        # Cast atom refs to the base type in aggregate
                        # expressions, since PostgreSQL does not create array
                        # types for custom domains and will fail to process a
                        # query with custom domains appearing as array
                        # elements.
                        pgtype = pg_types.pg_type_from_atom(
                            ctx.schema, ir_arg.scls, topbase=True)
                        pgtype = pgast.TypeName(name=pgtype)
                        arg_ref = pgast.TypeCast(arg=arg_ref, type_name=pgtype)

                    args.append(arg_ref)

            if expr.agg_filter:
                agg_filter = self.visit(expr.agg_filter)

            for arg in args:
                agg_filter = self._extend_binop(
                    agg_filter, pgast.NullTest(arg=arg, negated=True))

            if expr.agg_sort:
                with self.context.new() as sortctx:
                    sortctx.lax_paths = True
                    for sortexpr in expr.agg_sort:
                        _sortexpr = self.visit(sortexpr.expr)
                        agg_sort.append(
                            pgast.SortBy(
                                node=_sortexpr, dir=sortexpr.direction,
                                nulls=sortexpr.nones_order))

            if funcobj.from_function:
                name = (funcobj.from_function,)
            else:
                name = (
                    common.edgedb_module_name_to_schema_name(
                        funcobj.shortname.module),
                    common.edgedb_name_to_pg_name(
                        funcobj.shortname.name)
                )

            set_expr = pgast.FuncCall(
                name=name, args=args,
                agg_order=agg_sort, agg_filter=agg_filter,
                agg_distinct=(
                    expr.agg_set_modifier == irast.SetModifier.DISTINCT))

            # Add an explicit GROUP BY for each non-aggregated path bond.
            for path_id in list(stmt.path_bonds):
                if path_id in path_scope:
                    path_var = self._get_path_var(stmt, path_id)
                    stmt.group_clause.append(path_var)
                else:
                    stmt.path_bonds.discard(path_id)
                    stmt.path_rvar_map.pop(path_id, None)

        if not stmt.group_clause and not stmt.having:
            # This is a sentinel HAVING clause so that the optimizer
            # knows how to inline the resulting query correctly.
            stmt.having = pgast.Constant(val=True)

        self._ensure_correct_rvar_for_expr(ir_set, stmt, set_expr)

        if self._apply_path_bond_injections(stmt):
            # Due to injection this rel must not be a CTE.
            self._put_set_cte(ir_set, stmt)
        else:
            ctx.query.ctes.append(self._get_set_cte(ir_set))

    def _process_set_as_exists_expr(self, ir_set, stmt):
        """Populate the CTE for Set defined by an EXISTS() expression."""

        if isinstance(ir_set.expr.expr, irast.Stmt):
            # Statement varant.
            return self._process_set_as_exists_stmt_expr(ir_set, stmt)

        ctx = self.context.current

        with self.context.new() as newctx:
            newctx.in_set_expr = True
            newctx.lax_paths = 1
            newctx.rel = stmt

            path_scope = set(ctx.stmt_path_scope)

            newctx.stmt_path_scope = newctx.stmt_path_scope.copy()
            newctx.stmt_path_scope.update(ir_set.path_scope)
            newctx.stmt_specific_path_scope = \
                newctx.stmt_specific_path_scope.copy()
            newctx.stmt_specific_path_scope.update(ir_set.stmt_path_scope)

            ir_expr = ir_set.expr.expr
            set_ref = self.visit(ir_expr)

            for path_id in list(stmt.path_bonds):
                if not path_id.starts_any_of(path_scope):
                    stmt.path_bonds.discard(path_id)
                else:
                    var = self._get_path_var(stmt, path_id)
                    stmt.group_clause.append(var)

            if not stmt.group_clause and not stmt.having:
                # This is a sentinel HAVING clause so that the optimizer
                # knows how to inline the resulting query correctly.
                stmt.having = pgast.Constant(val=True)

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

            path_scope = set(ctx.stmt_path_scope)

            newctx.stmt_path_scope = newctx.stmt_path_scope.copy()
            newctx.stmt_path_scope.update(ir_set.path_scope)
            newctx.stmt_specific_path_scope = \
                newctx.stmt_specific_path_scope.copy()
            newctx.stmt_specific_path_scope.update(ir_set.stmt_path_scope)

            ir_expr = ir_set.expr.expr
            set_expr = self.visit(ir_expr)

            for path_id in list(set_expr.path_bonds):
                if not path_id.starts_any_of(path_scope):
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

    def _ensure_correct_rvar_for_expr(self, ir_set, stmt, set_expr):
        restarget = pgast.ResTarget(val=set_expr, name='v')

        if isinstance(ir_set.scls, s_concepts.Concept):
            root_rvar = self._get_root_rvar(ir_set, stmt)
            subqry = pgast.SelectStmt(
                target_list=[restarget]
            )
            self._put_path_output(subqry, ir_set, restarget.name)
            self._include_range(stmt, subqry, lateral=True)
            self._rel_join(stmt, root_rvar)
            self._put_path_rvar(stmt, ir_set.path_id, root_rvar)
        else:
            stmt.target_list.append(restarget)
            self._put_path_output(stmt, ir_set, restarget.name)

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

    def _ensure_correct_set(self, stmt, query, enforce_uniqueness=False):
        # Make sure that the set returned by the *query* does not
        # contain NULL values.
        ctx = self.context.current

        restype = irutils.infer_type(stmt.result, ctx.schema)
        if not isinstance(restype, (s_atoms.Atom, s_obj.Array, s_obj.Map)):
            return query

        with self.context.new() as subctx:
            # This is a simple wrapper, make sure path bond
            # conditions do not get injected unnecessarily.
            subctx.path_bonds = {}
            wrapper = self._wrap_set_rel_as_value(stmt.result, query)

            if enforce_uniqueness:
                orig_sort = list(query.sort_clause)
                for i, sortby in enumerate(query.sort_clause):
                    query.target_list.append(
                        pgast.ResTarget(val=sortby.node, name=f's{i}')
                    )
                query.sort_clause = [pgast.SortBy(node=pgast.Constant(val=1))]

                query.distinct_clause = [pgast.Star()]

                for i, orig_sortby in enumerate(orig_sort):
                    wrapper.sort_clause.append(
                        pgast.SortBy(
                            node=self._get_column(
                                wrapper.from_clause[0], f's{i}'),
                            dir=orig_sortby.dir,
                            nulls=orig_sortby.nulls
                        )
                    )

        resref = self._get_path_var(wrapper, stmt.result.path_id)
        wrapper.where_clause = self._extend_binop(
            wrapper.where_clause, pgast.NullTest(arg=resref, negated=True))

        # Pull the CTEs up.
        wrapper.ctes = query.ctes
        query.ctes = []

        return wrapper

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

    def _get_column(self, rvar, colspec, *, grouped=False):
        if isinstance(colspec, pgast.ColumnRef):
            colname = colspec.name[-1]
            nullable = colspec.nullable
            grouped = colspec.grouped
        else:
            colname = colspec
            nullable = rvar.nullable if rvar is not None else False
            grouped = grouped

        if rvar is None:
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
            expr = ir_set.expr.result.expr
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

        elif isinstance(expr, irast.StructIndirection):
            return self._get_column(source_rvar, expr.name)

        else:
            return self._get_rvar_path_var(source_rvar, ir_set, raw=False)

    def _apply_path_bond_injections(self, stmt):
        ctx = self.context.current

        if ctx.expr_injected_path_bond is not None:
            # Inject an explicitly provided path bond.  This is necessary
            # to ensure the correct output of rels that compute view
            # expressions that do not contain relevant path bonds themselves.
            alias = ctx.genalias(hint='b')
            bond_ref = ctx.expr_injected_path_bond['ref']
            bond_path_id = ctx.expr_injected_path_bond['path_id']

            ex_ref = self._maybe_get_path_var(stmt, bond_path_id)
            if ex_ref is None:
                stmt.target_list.append(
                    pgast.ResTarget(val=bond_ref, name=alias)
                )
                # Register this bond as output just in case.
                # BUT, do not add it to path_bonds.
                self._put_path_output(stmt, bond_path_id, alias)
                self._put_path_bond(stmt, bond_path_id)

            return True
        else:
            return False

    def _full_inner_bond_condition(self, query, parent_path_bonds):
        ctx = self.context.current

        condition = None

        for path_id in query.path_bonds:
            rptr = path_id.rptr(ctx.schema)
            if rptr and rptr.singular(path_id.rptr_dir()):
                continue

            rref = parent_path_bonds.get(path_id)
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
                          map_join_type='inner', semi=False):
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
            far_ref = source_ref
        else:
            map_join_cond = forward_bond
            far_ref = target_ref

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

        if isinstance(ir_set.scls, s_concepts.Concept) and not semi:
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

        return map_rvar, far_ref

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

    def _join_inline_rel(self, *, stmt, set_rvar, ir_set, back_id_col,
                         join_type='inner'):
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
            type=join_type,
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

    def _pull_path_namespace(self, *, target, source, replace_bonds=True):
        ctx = self.context.current

        squery = source.query
        if self._is_set_op_query(squery):
            # Set op query
            source_qs = [squery, squery.larg, squery.rarg]
        else:
            source_qs = [squery]

        for source_q in source_qs:
            outputs = {o[0] for o in source_q.path_outputs}
            s_paths = set(source_q.path_rvar_map) | outputs
            for path_id in s_paths:
                path_id = self._reverse_map_path_id(
                    path_id, target.view_path_id_map)
                if path_id not in target.path_rvar_map or replace_bonds:
                    self._put_path_rvar(target, path_id, source)

            for path_id in source_q.path_bonds:
                if path_id in target.path_bonds and not replace_bonds:
                    continue

                orig_path_id = path_id
                path_id = self._reverse_map_path_id(
                    path_id, target.view_path_id_map)

                if (not path_id.is_in_scope(ctx.stmt_path_scope) and
                        not orig_path_id.is_in_scope(ctx.stmt_path_scope)):
                    continue

                self._put_path_bond(target, path_id)

                bond = LazyPathVarRef(
                    self._get_rvar_path_var, source, path_id)

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

        if self._is_set_op_query(rel):
            alias = ctx.genalias(alias)

            cb = functools.partial(
                self._get_path_output_or_null,
                path_id=path_id,
                alias=alias)

            self._for_each_query_in_set(rel, cb)
            self._put_path_output(rel, path_id, alias)
            return self._get_column(None, alias)

        ptr_info = parent_ptr_info = parent_ptrcls = parent_dir = None
        if ptrcls is not None:
            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, resolve_type=False, link_bias=False)

            parent_ptrcls = irast.PathId(path_id[:-2]).rptr(ctx.schema)
            if parent_ptrcls is not None:
                parent_ptr_info = pg_types.get_pointer_storage_info(
                    parent_ptrcls, resolve_type=False, link_bias=False)

                parent_dir = path_id[:-2].rptr_dir()

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
                        ptrname == 'std::id' and
                        parent_dir == s_pointers.PointerDirection.Outbound):
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

        rel_rvar = rel.path_rvar_map.get(path_id)
        if rel_rvar is None:
            rel_rvar = rel.path_rvar_map.get(src_path_id)
            if rel_rvar is None:
                raise LookupError(
                    f'cannot find source range for '
                    f'path {src_path_id} in {rel}')

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

    def _get_path_output_alias(self, path_id):
        ctx = self.context.current

        rptr = path_id.rptr(ctx.schema)
        if rptr is not None:
            ptrname = rptr.shortname
            alias = ctx.genalias(hint=ptrname.name)
        else:
            alias = ctx.genalias(hint=path_id[-1].name.name)

        return alias

    def _get_path_output(self, rel, path_id, *, alias=None, raw=False):
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
            alias = self._get_path_output_alias(path_id)

        if set_op is None:
            restarget = pgast.ResTarget(name=alias, val=ref)
            if hasattr(rel, 'returning_list'):
                rel.returning_list.append(restarget)
            else:
                rel.target_list.append(restarget)

        self._put_path_output(rel, path_id, alias, raw=raw)

        return alias

    def _get_path_output_or_null(self, rel, path_id, alias):
        try:
            alias = self._get_path_output(rel, path_id, alias=alias)
        except LookupError:
            restarget = pgast.ResTarget(
                name=alias,
                val=pgast.Constant(val=None))
            if hasattr(rel, 'returning_list'):
                rel.returning_list.append(restarget)
            else:
                rel.target_list.append(restarget)

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

        return self._get_column(rvar, name)

    def _put_path_rvar(self, stmt, path_id, rvar):
        assert path_id
        path_id = self._get_canonical_path_id(self._proper_path_id(path_id))
        stmt.path_rvar_map[path_id] = rvar

    def _get_set_rvar(self, stmt, ir_set):
        ctx = self.context.current

        cte = self._get_set_cte(ir_set)
        if cte is None:
            return None
        else:
            return ctx.subquery_map[stmt].get(cte)

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

    def _is_set_op_query(self, query):
        return getattr(query, 'op', None) is not None
