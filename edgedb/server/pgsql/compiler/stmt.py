##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import itertools
import functools

from edgedb.lang.common import exceptions as edgedb_error

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import pointers as s_pointers

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import codegen as pgcodegen
from edgedb.server.pgsql import common
from edgedb.server.pgsql import types as pg_types

from edgedb.lang.common import ast
from edgedb.lang.common import debug

from .context import TransformerContext
from . import expr as expr_compiler
from . import dml


ResTargetList = collections.namedtuple('ResTargetList', ['targets', 'attmap'])


class IRCompiler(expr_compiler.IRCompilerBase,
                 dml.IRCompilerDMLSupport):
    def __init__(self, **kwargs):
        self.context = None
        super().__init__(**kwargs)

    def transform_to_sql_tree(self, ir_expr, *, schema, backend=None,
                              output_format=None):
        try:
            # Transform to sql tree
            self.context = TransformerContext()
            ctx = self.context.current
            ctx.memo = self._memo
            ctx.backend = backend
            ctx.schema = schema
            ctx.output_format = output_format
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

    def transform(self, ir_expr, *, schema, backend=None, output_format=None):
        qtree = self.transform_to_sql_tree(
            ir_expr, schema=schema, backend=backend,
            output_format=output_format)

        argmap = self.context.current.argmap

        # Generate query text
        codegen = self._run_codegen(qtree)
        qchunks = codegen.result
        arg_index = codegen.param_index

        if debug.flags.edgeql_compile:  # pragma: no cover
            debug.header('SQL')
            debug.dump_code(''.join(qchunks), lexer='sql')

        return qchunks, argmap, arg_index, type(qtree), tuple()

    def generic_visit(self, node, *, combine_results=None):
        raise NotImplementedError(
            'no IR compiler handler for {}'.format(node.__class__))

    def visit_SelectStmt(self, stmt):
        parent_ctx = self.context.current
        parent_rel = parent_ctx.rel

        with self.context.substmt():
            ctx = self.context.current
            if ctx.toplevel_stmt is None:
                ctx.toplevel_stmt = ctx.stmt

            query = ctx.query

            parent_ctx.subquery_map[parent_rel][query] = {
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
                self._setop_from_list(query, [larg, rarg], set_op)
            else:
                # Process the result expression;
                self._process_selector(stmt.result)

                # The WHERE clause
                if stmt.where:
                    with self.context.new() as ctx1:
                        ctx1.clause = 'where'
                        query.where_clause = self.visit(stmt.where)

                self._apply_path_scope()

                # The GROUP BY clause
                self._process_groupby(stmt.groupby)

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

            # Make sure all sub-selects are linked according
            # to path matching logic...
            self._connect_subrels(query)

            # ..and give the parent query the opportunity to do the same.
            for cte in query.ctes:
                parent_ctx.subquery_map[parent_rel][cte.query] = {
                    'linked': False,
                    'rvar': None
                }

            return query

    def visit_Shape(self, expr):
        ctx = self.context.current

        my_elements = []
        attribute_map = []
        testref = None

        self.visit(expr.set)

        for i, e in enumerate(expr.elements):
            with self.context.new() as newctx:
                newctx.scope_cutoff = True
                newctx.in_shape = True
                if isinstance(e, irast.Set) and isinstance(e.expr, irast.Stmt):
                    element = self.visit(e.expr)
                else:
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
                    rt = element.target_list[0]
                    if not rt.name:
                        rt.name = ctx.genalias(hint='r')

                    subrvar = pgast.RangeSubselect(
                        subquery=element,
                        alias=pgast.Alias(
                            aliasname=ctx.genalias(hint='q')
                        )
                    )

                    result = pgast.SelectStmt(
                        target_list=[
                            pgast.ResTarget(
                                val=pgast.FuncCall(
                                    name=('array_agg',),
                                    args=[
                                        pgast.ColumnRef(
                                            name=[
                                                subrvar.alias.aliasname,
                                                rt.name
                                            ]
                                        )
                                    ]
                                )
                            )
                        ],
                        from_clause=[
                            subrvar
                        ]
                    )

                    element = result

            if ptr_name == 'std::id':
                testref = element

            attr_name = s_pointers.PointerVector(
                name=ptr_name.name, module=ptr_name.module,
                direction=ptr_direction, target=ptr_target.name,
                is_linkprop=isinstance(e.rptr.ptrcls, s_lprops.LinkProperty))

            attribute_map.append(attr_name)
            my_elements.append(element)

        if ctx.output_format == 'flat':
            # DML statements want ``SELECT Object`` to return the object
            # identity and properties in addressable column format.
            result = ResTargetList(my_elements, attribute_map)
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
                name=('jsonb_build_object',), args=keyvals)

        elif ctx.output_format == 'identity':
            result = testref
            testref = None

        else:
            raise NotImplementedError(
                f'unsupported output_format: {ctx.output_format}')

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

    def visit_Set(self, expr):
        ctx = self.context.current

        source_cte = self._set_to_cte(expr)

        if ctx.in_exists and not ctx.in_set_expr:
            # When referred to as an argument to EXISTS(),
            # we want to wrap the set CTE into
            #    EXISTS(SELECT FROM SetCTE)
            result = self._wrap_set_rel(expr, source_cte)

        elif ctx.clause == 'where' and not ctx.in_set_expr:
            # When referred to in WHERE
            # we want to wrap the set CTE into
            #    EXISTS(SELECT * FROM SetCTE WHERE SetCTE.expr)
            result = self._wrap_set_rel_as_bool_disjunction(expr, source_cte)

        elif ctx.clause == 'offsetlimit' and not ctx.in_set_expr:
            # When referred to in OFFSET/LIMIT we want to wrap the
            # set CTE into
            #    SELECT v FROM SetCTE
            result = self._wrap_set_rel_as_value(expr, source_cte)

        else:
            # Otherwise we join the range directly into the current rel
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

    def _apply_path_scope(self):
        ctx = self.context.current
        query = ctx.query

        scoped_prefixes = set()
        strict = []
        lax_prefixes = {s.path_id for s, lax in ctx.setscope.items() if lax}

        for ir_set, lax in ctx.setscope.items():
            if not lax:
                prefixes = set(ir_set.path_id.iter_prefixes())
                if not (prefixes & lax_prefixes):
                    strict.append(ir_set)

        strict.sort(key=lambda s: len(s.path_id), reverse=True)

        for ir_set in strict:
            if ir_set.path_id not in scoped_prefixes:
                cte = ctx.ctemap[ir_set, False]
                scope_expr = self._set_as_exists_op(
                    self._wrap_set_rel(ir_set, cte))
                query.where_clause = self._extend_binop(
                    query.where_clause, scope_expr)
                scoped_prefixes.update(ir_set.path_id.iter_prefixes())

    def _include_range(self, rel):
        """Ensure the *rel* is present in the from_clause of current rel.

        :param rel:
            The relation node to join.

        :return:
            RangeVar or RangeSubselect representing the *rel* in the
            context of current rel.
        """
        ctx = self.context.current

        subrel = ctx.subquery_map[ctx.rel].get(rel)

        if subrel is None or subrel['rvar'] is None:
            # The rel has not been recorded as a sub-relation of this rel,
            # so make it so.
            rvar = pgast.RangeVar(
                relation=rel,
                alias=pgast.Alias(
                    aliasname=ctx.genalias(hint=getattr(rel, 'name'))
                )
            )

            ctx.subquery_map[ctx.rel][rel] = {
                'rvar': rvar,
                'linked': False
            }
        else:
            rvar = subrel['rvar']

        # Make sure that the path namespace of *cte* is mapped
        # onto the path namespace of the current rel.
        self._pull_path_namespace(
            target=ctx.rel, source=rvar, pull_bonds=not ctx.scope_cutoff)

        return rvar

    def _set_as_exists_op(self, pg_expr, negated=False):
        # Make sure *pg_expr* is an EXISTS() expression
        # Set references inside WHERE are transformed into
        # EXISTS expressions in visit_Set.  For other
        # occurrences we do it here.
        if isinstance(pg_expr, pgast.Query):
            result = pgast.SubLink(
                type=pgast.SubLinkType.EXISTS, subselect=pg_expr)

        elif isinstance(pg_expr, pgast.CommonTableExpr):
            subselect = pgast.SelectStmt(
                from_clause=[
                    pgast.RangeVar(
                        relation=pg_expr
                    )
                ]
            )
            result = pgast.SubLink(
                type=pgast.SubLinkType.EXISTS, subselect=subselect)

        elif isinstance(pg_expr, (pgast.Constant, pgast.ParamRef)):
            result = pgast.NullTest(arg=pg_expr, negated=True)

        else:
            raise ValueError(
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

        if ir_set.expr is not None:
            v = pgast.ColumnRef(
                name=[rvar.alias.aliasname, 'v']
            )

            if ir_set.scls.name == 'std::bool':
                wrapper.where_clause = v
            else:
                wrapper.where_clause = pgast.NullTest(
                    arg=v,
                    negated=True
                )
        else:
            wrapper.where_clause = pgast.NullTest(
                arg=wrapper.path_namespace[ir_set.path_id],
                negated=True
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

        return wrapper

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

        if ir_set.expr is not None:
            target = pgast.ColumnRef(name=[rvar.alias.aliasname, 'v'])
        else:
            target = wrapper.path_namespace[ir_set.path_id]

        wrapper.where_clause = pgast.NullTest(
            arg=target,
            negated=True
        )

        subrels = ctx.subquery_map[ctx.rel]
        subrels[wrapper] = {
            'rvar': rvar,
            'linked': False
        }

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

        if ir_set.expr is not None:
            target = pgast.ColumnRef(name=[rvar.alias.aliasname, 'v'])
        else:
            target = wrapper.path_namespace[ir_set.path_id]

        wrapper.target_list.append(
            pgast.ResTarget(
                val=target
            )
        )

        subrels = ctx.subquery_map[ctx.rel]
        subrels[wrapper] = {
            'rvar': rvar,
            'linked': True
        }

        return wrapper

    def _put_set_cte(self, ir_set, cte, *, ctx=None):
        if ctx is None:
            ctx = self.context.current

        if ir_set.rptr is not None and ir_set.expr is None:
            key = (ir_set, ctx.lax_paths)
        else:
            key = (ir_set, False)

        ctx.ctemap[key] = cte

        if (ir_set.expr is None and ctx.clause in {'where', 'result'} and
                not ctx.in_shape):
            # References to paths in SELECT/RETURNING and WHERE clauses
            # form a strict set existence condition for each path, unless
            # the existence predicate was used explicitly (either directly,
            # with [NOT] EXISTS, or through the coalescing operator.)
            if ctx.lax_paths or not ctx.setscope.get(ir_set):
                ctx.setscope[ir_set] = ctx.lax_paths

        return cte

    def _get_set_cte(self, ir_set, *, lax=None):
        ctx = self.context.current

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

        stmt = pgast.SelectStmt()
        stmt.path_id = ir_set.path_id

        with self.context.new() as ctx:
            ctx.rel = stmt

            cte = pgast.CommonTableExpr(
                query=stmt,
                name=ctx.genalias(hint=self._get_set_cte_alias(ir_set))
            )

            self._put_set_cte(ir_set, cte)

            if ir_set.expr is not None:
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

        return alias_hint

    def _connect_set_sources(self, ir_set, stmt, sources):
        # Generate a flat JOIN list from the gathered sources
        # using path bonds for conditions.

        ctx = self.context.current

        subrels = ctx.subquery_map[stmt]

        for source in sources:
            source_rel = self._set_to_cte(source)
            if source_rel in subrels:
                continue

            lax_path = ctx.setscope.get(source)
            if lax_path:
                source_rel = self._get_set_cte(source, lax=True)

            src_rvar = pgast.RangeVar(
                relation=source_rel,
                alias=pgast.Alias(
                    aliasname=ctx.genalias(hint=source_rel.name)
                )
            )

            subrels[source_rel] = {
                'rvar': src_rvar,
                'linked': True
            }

            self._pull_path_namespace(target=stmt, source=src_rvar)
            self._rel_join(stmt, src_rvar, type='inner')

    def _get_set_rvar(self, ir_set, stmt):
        if not isinstance(ir_set.scls, s_concepts.Concept):
            return None

        id_field = common.edgedb_name_to_pg_name('std::id')
        id_set = self._get_ptr_set(ir_set, 'std::id')

        set_rvar = self._range_for_set(ir_set, stmt)
        stmt.scls_rvar = set_rvar

        path_id = ir_set.path_id

        if isinstance(set_rvar, pgast.RangeSubselect):
            rvar_rel = set_rvar.subquery
        else:
            rvar_rel = set_rvar.query

        rvar_rel.path_vars[path_id] = id_field
        rvar_rel.path_bonds[path_id] = id_field

        self._pull_path_var(stmt, id_set, path_id=path_id)
        stmt.inner_path_bonds[path_id] = stmt.path_namespace[path_id]

        return set_rvar

    def _process_set_as_root(self, ir_set, stmt):
        """Populate the CTE for a Set defined by a path root."""

        ctx = self.context.current
        set_rvar = self._get_set_rvar(ir_set, stmt)
        if set_rvar is not None:
            stmt.from_clause.append(set_rvar)
        ctx.query.ctes.append(self._get_set_cte(ir_set))

    def _process_set_as_path_step(self, ir_set, stmt):
        """Populate the CTE for Set defined by a single path step."""

        ctx = self.context.current

        return_parent = True

        rptr = ir_set.rptr
        ptrcls = rptr.ptrcls
        fromlist = stmt.from_clause

        self._connect_set_sources(ir_set, stmt, [ir_set.rptr.source])
        set_rvar = self._get_set_rvar(ir_set, stmt)

        if (isinstance(rptr.source.scls, s_atoms.Atom) and
                ptrcls.shortname == 'std::__class__'):
            # Special case to support Path.atom.__class__ paths
            self._join_class_rel(
                stmt=stmt, set_rvar=set_rvar, ir_set=ir_set)

            return_parent = False

        else:
            path_rvar = fromlist[0]
            source_rel = path_rvar.relation
            source_stmt = source_rel.query

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, resolve_type=False, link_bias=False)

            if isinstance(ptrcls, s_lprops.LinkProperty):
                # Reference to a link property.
                map_rvar = self._join_mapping_rel(
                    stmt=source_stmt, set_rvar=set_rvar, ir_set=ir_set,
                    map_join_type='left')

                source_stmt.rptr_rvar = map_rvar

                self._pull_path_var(source_stmt, ir_set)

            elif ptr_info.table_type != 'concept':
                map_join_type = 'left' if ctx.lax_paths else 'inner'

                map_rvar = self._join_mapping_rel(
                    stmt=stmt, set_rvar=set_rvar, ir_set=ir_set,
                    map_join_type=map_join_type)

                stmt.rptr_rvar = map_rvar

                return_parent = False

            elif isinstance(ir_set.scls, s_concepts.Concept):
                # Direct reference to another object.
                self._pull_path_var(source_rel, ir_set)
                stmt.path_namespace[ir_set.path_id] = pgast.ColumnRef(
                    name=[
                        path_rvar.alias.aliasname,
                        self._get_path_var(
                            source_rel.query, ir_set.path_id)
                    ]
                )

                self._join_inline_rel(
                    stmt=stmt, set_rvar=set_rvar, ir_set=ir_set,
                    back_id_col=ptr_info.column_name)

                return_parent = False

            elif ir_set.expr is None:
                # The path step target is stored in the source's table,
                # so we need to make sure that rel is returning the column
                # ref we need.
                self._pull_path_var(source_rel, ir_set)

        if return_parent:
            source_rel = stmt.from_clause[0].relation
            self._put_set_cte(ir_set, source_rel)
        else:
            ctx.query.ctes.append(self._get_set_cte(ir_set))

    def _process_set_as_expr(self, ir_set, stmt):
        """Populate the CTE for Set defined by an expression."""

        ctx = self.context.current

        if isinstance(ir_set.expr, irast.Stmt):
            set_expr = self.visit(ir_set.expr)
            res = ir_set.expr.result
            cte = self._get_set_cte(res)
            self._put_set_cte(ir_set, cte)

            return

        set_rvar = self._get_set_rvar(ir_set, stmt)

        if not ir_set.sources and set_rvar is not None:
            stmt.from_clause.append(set_rvar)

        if isinstance(ir_set.expr, irast.TypeFilter):
            self._rel_join(stmt, stmt.scls_rvar, type='left')

            valref = pgast.ColumnRef(
                name=[
                    stmt.scls_rvar.alias.aliasname,
                    stmt.scls_rvar.path_vars[ir_set.path_id]
                ]
            )

            restarget = pgast.ResTarget(val=valref, name='v')
            stmt.target_list.append(restarget)

            id_set = self._get_ptr_set(ir_set, 'std::id')

            stmt.path_namespace[ir_set.path_id] = valref
            stmt.path_vars[ir_set.path_id] = 'v'
            stmt.path_namespace[id_set.path_id] = valref
            stmt.path_vars[id_set.path_id] = 'v'

        else:
            with self.context.new() as newctx:
                newctx.in_set_expr = True
                newctx.rel = stmt
                set_expr = self.visit(ir_set.expr)

                if irutils.is_aggregated_expr(ir_set.expr):
                    # The expression includes calls to aggregates.
                    # Clear the namespace as it is no longer valid.
                    stmt.path_namespace.clear()
                    stmt.inner_path_bonds.clear()
                    stmt.target_list[:] = []

            expr_result = irutils.infer_type(ir_set.expr, ctx.schema)

            if isinstance(expr_result, s_concepts.Concept):
                innerqry = pgast.SelectStmt(
                    target_list=[
                        pgast.ResTarget(
                            val=set_expr,
                            name='v'
                        )
                    ]
                )

                valref = pgast.ColumnRef(
                    name=['q', 'v']
                )

                qry = pgast.SelectStmt(
                    target_list=[
                        pgast.ResTarget(
                            val=valref,
                            name='v'
                        )
                    ],
                    from_clause=[
                        pgast.RangeSubselect(
                            subquery=innerqry,
                            alias=pgast.Alias(
                                aliasname='q'
                            )
                        )
                    ]
                )

                qry.path_namespace[ir_set.path_id] = valref
                qry.path_vars[ir_set.path_id] = 'v'
                qry.inner_path_bonds[ir_set.path_id] = valref
                qry.path_bonds[ir_set.path_id] = 'v'

                expr_rvar = pgast.RangeSubselect(
                    lateral=True,
                    subquery=qry,
                    alias=pgast.Alias(
                        aliasname='q'
                    )
                )

                ctx.subquery_map[stmt][innerqry] = {
                    'rvar': expr_rvar,
                    'linked': True
                }

                self._rel_join(stmt, expr_rvar, type='inner')

                restarget = pgast.ResTarget(val=valref, name='v')
                stmt.target_list.append(restarget)
            else:
                restarget = pgast.ResTarget(val=set_expr, name='v')
                stmt.target_list.append(restarget)

        self._connect_subrels(stmt, connect_subqueries=False)

        if ir_set.sources:
            self._connect_set_sources(ir_set, stmt, ir_set.sources)

        self._connect_subrels(stmt)

        ctx.query.ctes.append(self._get_set_cte(ir_set))

    def visit_ExistPred(self, expr):
        with self.context.new() as ctx:
            ctx.in_exists = True
            ctx.lax_paths = True
            pg_expr = self.visit(expr.expr)
            return self._set_as_exists_op(pg_expr, expr.negated)

    def visit_Coalesce(self, expr):
        with self.context.new() as ctx:
            ctx.lax_paths = True
            pg_args = self.visit(expr.args)
        return pgast.FuncCall(name=('coalesce',), args=pg_args)

    def _connect_subrels(self, query, connect_subqueries=True):
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

        # Go through all CTE references and LEFT JOIN them
        # in *query* FROM.
        for rel, info in rels:
            if isinstance(rel, pgast.CommonTableExpr):
                self._rel_join(query, info['rvar'], type='left')
                info['linked'] = True

        # Go through the remaining subqueries and inject join conditions.
        if connect_subqueries:
            for rel, info in rels:
                if not isinstance(rel, pgast.CommonTableExpr):
                    self._connect_subquery(rel, query)
                    info['linked'] = True

    def _connect_subquery(self, subquery, parentquery):
        # Inject a WHERE condition corresponding to the full inner bond join
        # between the outer query and the subquery.
        cond = self._full_inner_bond_condition(subquery, parentquery)

        if cond is not None:
            subquery.where_clause = \
                self._extend_binop(subquery.where_clause, cond)

    def _process_explicit_substmts(self, ir_stmt):
        ctx = self.context.current

        for substmt in ir_stmt.substmts:
            with self.context.substmt():
                self.context.current.output_format = 'flat'
                subquery = self.visit(substmt.expr)
                cte = pgast.CommonTableExpr(
                    query=subquery,
                    name=ctx.genalias(hint=substmt.path_id[0].name.name)
                )
                # XXX: hack
                subquery.scls_rvar = subquery.from_clause[0]

                self._put_set_cte(substmt, cte, ctx=ctx)

                ref = subquery.path_namespace[substmt.real_path_id]

                self._reset_path_namespace(subquery)

                subquery.target_list.append(
                    pgast.ResTarget(
                        val=ref,
                        name='v'
                    )
                )

                subquery.path_namespace[substmt.path_id] = ref
                subquery.path_vars[substmt.path_id] = 'v'
                subquery.inner_path_bonds[substmt.path_id] = ref
                subquery.path_bonds[substmt.path_id] = 'v'

            ctx.query.ctes.append(cte)

    def _process_selector(self, result_expr):
        ctx = self.context.current
        query = ctx.query

        with self.context.new() as newctx:
            newctx.clause = 'result'
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

        if ctx.output_format == 'json':
            # Target list may be empty if selector is a set op product
            if selexprs:
                target = pgast.ResTarget(
                    name=None,
                    val=pgast.FuncCall(name=('to_jsonb',), args=[pgexpr]),
                )

                query.target_list.append(target)

        else:
            for pgexpr, alias in selexprs:
                target = pgast.ResTarget(name=alias, val=pgexpr)
                query.target_list.append(target)

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
        except KeyError:  # pragma: no cover
            raise expr_compiler.IRCompilerInternalError(
                f'could not resolve {ir_set.path_id} as a column '
                f'reference in context of {ctx.rel!r}')

        if ctx.in_aggregate and isinstance(ir_set.scls, s_atoms.Atom):
            # Cast atom refs to the base type in aggregate expressions, since
            # PostgreSQL does not create array types for custom domains and
            # will fail to process a query with custom domains appearing as
            # array elements.
            #
            pgtype = pg_types.pg_type_from_atom(
                ctx.schema, ir_set.scls, topbase=True)
            pgtype = pgast.TypeName(name=pgtype)
            ref = pgast.TypeCast(arg=ref, type_name=pgtype)

        return ref

    def _full_inner_bond_condition(self, left, right):
        condition = None

        for path_id, lref in left.inner_path_bonds.items():
            rptr = path_id.rptr()
            if rptr and rptr.singular(path_id.rptr_dir()):
                continue

            try:
                rref = right.inner_path_bonds[path_id]
            except KeyError:
                continue

            path_cond = self._new_binop(lref, rref, op='=')
            condition = self._extend_binop(condition, path_cond)

        return condition

    def _full_outer_bond_condition(self, query, right_rvar):
        condition = None

        for path_id in right_rvar.inner_path_bonds:
            rptr = path_id.rptr()
            if rptr and rptr.singular(path_id.rptr_dir()):
                continue

            rname = self._get_path_bond(right_rvar.query, path_id)

            try:
                lref = query.inner_path_bonds[path_id]
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

    def _join_mapping_rel(self, *, stmt, set_rvar, ir_set,
                          map_join_type='inner'):
        fromexpr = stmt.from_clause[0]

        link = ir_set.rptr
        if isinstance(link.ptrcls, s_lprops.LinkProperty):
            link = link.source.rptr

        linkmap_key = link.ptrcls, link.direction, link.source

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
                type=map_join_type,
                quals=map_join_cond
            )

            stmt.ptr_rvar_map[linkmap_key] = map_rvar, map_join

        if isinstance(ir_set.scls, s_concepts.Concept):
            # Join the target relation, if we have it
            target_range_bond = pgast.ColumnRef(
                name=[
                    set_rvar.alias.aliasname,
                    self._get_path_bond(set_rvar.query, ir_set.path_id)
                ]
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
                    type=map_join_type,
                    larg=pre_map_join,
                    rarg=set_rvar,
                    quals=cond_expr)
                map_join.copyfrom(new_map_join)

        stmt.from_clause[0] = map_join

        return map_rvar

    def _join_class_rel(self, *, stmt, set_rvar, ir_set):
        fromexpr = stmt.from_clause[0]

        nref = pgast.ColumnRef(
            name=[
                set_rvar.alias.aliasname,
                common.edgedb_name_to_pg_name('schema::name')
            ]
        )

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
            src_ref = stmt.path_namespace[ir_set.rptr.source.path_id]
        else:
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

    def _setop_from_list(self, parent_qry, oplist, op):
        nq = len(oplist)

        assert nq >= 2, 'set operation requires at least two arguments'

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

    def _for_each_query_in_set(self, qry, cb):
        if qry.op:
            self._for_each_query_in_set(qry.larg, cb)
            self._for_each_query_in_set(qry.rarg, cb)
        else:
            cb(qry)

    def _reset_path_namespace(self, query):
        query.path_namespace.clear()
        query.path_vars.clear()
        query.inner_path_bonds.clear()
        query.path_bonds.clear()

    def _pull_path_namespace(self, *, target, source, pull_bonds=True):
        squery = source.query
        for path_id in set(squery.path_namespace) - set(target.path_namespace):
            name = self._get_path_var(squery, path_id)
            ref = pgast.ColumnRef(name=[source.alias.aliasname, name])
            target.path_namespace[path_id] = ref

        if pull_bonds:
            for path_id in (set(squery.inner_path_bonds) -
                            set(target.inner_path_bonds)):
                name = self._get_path_bond(squery, path_id)
                ref = pgast.ColumnRef(name=[source.alias.aliasname, name])
                target.inner_path_bonds[path_id] = ref

    def _get_path_bond(self, stmt, path_id):
        alias = stmt.path_bonds.get(path_id)
        if alias is None:
            alias = self._get_outer_path_ref(
                stmt, path_id, stmt.inner_path_bonds)

            stmt.path_bonds[path_id] = alias

        return alias

    def _get_path_var(self, stmt, path_id):
        alias = stmt.path_vars.get(path_id)
        if alias is None:
            alias = self._get_outer_path_ref(
                stmt, path_id, stmt.path_namespace)
            stmt.path_vars[path_id] = alias

        return alias

    def _get_outer_path_ref(self, stmt, path_id, inner_ref_coll):
        ctx = self.context.current

        ref = inner_ref_coll[path_id]

        if isinstance(stmt, pgast.DML):
            rlist = stmt.returning_list

            if len(path_id) > 1:
                lname = path_id[-2][0].shortname
            else:
                lname = 'std::id'

            colname = common.edgedb_name_to_pg_name(lname)

            ref = pgast.ColumnRef(
                name=[stmt.relation.alias.aliasname, colname]
            )
        else:
            rlist = stmt.target_list

        # Check if the column is already in return list
        # (due to being resolved from a different path id).
        for rt in rlist:
            val = rt.val
            if isinstance(val, pgast.ColumnRef) and val.name == ref.name:
                alias = rt.name
                break
        else:
            alias = ctx.genalias(hint=ref.name[-1])
            rlist.append(pgast.ResTarget(name=alias, val=ref))

        return alias

    def _pull_path_var(self, rel, ir_set, path_id=None, *, alias=None):
        """Make sure the value of the *ir_set* path is present in namespace."""
        ctx = self.context.current

        if isinstance(rel, pgast.CommonTableExpr):
            rel = rel.query

        rptr = ir_set.rptr
        ptrcls = rptr.ptrcls
        ptrname = ptrcls.shortname

        if getattr(rel, 'op', None) is not None:
            cb = functools.partial(
                self._pull_path_var,
                ir_set=ir_set,
                path_id=path_id,
                alias=common.edgedb_name_to_pg_name(ptrname))

            self._for_each_query_in_set(rel, cb)
            return

        if path_id is None:
            path_id = ir_set.path_id

        try:
            return rel.path_namespace[path_id]
        except KeyError:
            pass

        ptr_info = pg_types.get_pointer_storage_info(
            ptrcls, resolve_type=False, link_bias=False)

        if ptr_info.table_type == 'link':
            source = rptr.source.rptr.ptrcls
            rel_rvar = rel.rptr_rvar
        else:
            if isinstance(ptrcls, s_lprops.LinkProperty):
                source = rptr.source.rptr.source.scls
                ptrcls = rptr.source.rptr.ptrcls
                ptrname = ptrcls.shortname
            else:
                source = rptr.source.scls

            rel_rvar = rel.scls_rvar

        if rel_rvar is None:
            raise expr_compiler.IRCompilerInternalError(
                f'{rel} is missing the relation context for {ir_set.path_id}')

        colname = None

        # If this is another query, need to make sure the ref
        # is there too.
        if isinstance(rel_rvar, pgast.RangeSubselect):
            source_rel = rel_rvar.subquery
            self._pull_path_var(source_rel, ir_set, path_id)
            colname = ptr_info.column_name
            source_rvars = [rel_rvar]

        elif isinstance(rel_rvar.relation, pgast.CommonTableExpr):
            source_rel = rel_rvar.relation.query
            self._pull_path_var(source_rel, ir_set, path_id)
            colname = self._get_path_var(source_rel, path_id)
            source_rvars = [rel_rvar]

        else:
            colname = ptr_info.column_name

            if ptrname in source.pointers:
                source_rvars = [rel_rvar]
            else:
                source_rvars = self._get_path_source_rvars(
                    rel, source, rel_rvar, ptrname, path_id)

        fieldrefs = [
            pgast.ColumnRef(name=[source_rvar.alias.aliasname, colname])
            for source_rvar in source_rvars
        ]

        if alias is None:
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

        rel.path_namespace[path_id] = refexpr
        rel.path_vars[path_id] = alias

        restarget = pgast.ResTarget(name=alias, val=refexpr)
        if hasattr(rel, 'returning_list'):
            rel.returning_list.append(restarget)
        else:
            rel.target_list.append(restarget)

        if ir_set.path_id != path_id:
            rel.path_namespace[ir_set.path_id] = refexpr
            rel.path_vars[ir_set.path_id] = alias

        return refexpr

    def _get_path_source_rvars(self, rel, source_cls, rel_rvar,
                               ptrname, path_id):
        ctx = self.context.current
        schema = ctx.schema
        id_field = common.edgedb_name_to_pg_name('std::id')

        sources = source_cls.get_ptr_sources(
            schema, ptrname, look_in_children=True,
            strict_ancestry=True)

        if not sources:
            raise RuntimeError(  # internal error
                f'cannot find column source for '
                f'({source_cls.name}).>({ptrname})')

        if getattr(source_cls, 'is_virtual', None):
            # Atom refs to columns present in direct children of a
            # virtual concept are guaranteed to be included in the
            # relation representing the virtual concept.
            #
            schema = ctx.schema
            child_ptrs = itertools.chain.from_iterable(
                c.pointers for c in source_cls.children(schema))
            if ptrname in set(child_ptrs):
                descendants = set(source_cls.descendants(schema))
                sources -= descendants
                sources.add(source_cls)

        rvars = {source_cls: rel_rvar}

        for s in sources:
            if s in rvars:
                continue

            src_rvar_pid = rel_rvar.query.path_id
            src_rvar = self._range_for_concept(s, rel)
            src_rvar.query.path_id = src_rvar_pid
            src_rvar.path_vars[src_rvar_pid] = id_field
            src_rvar.path_bonds[src_rvar_pid] = id_field

            self._rel_join(rel, src_rvar, type='left')

            rvars[s] = src_rvar

        source_rvars = [
            rvars[c] for c in sources
        ]

        return source_rvars

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
