##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

"""IR compiler support for INSERT/UPDATE/DELETE statements."""

#
# The processing of the DML statement is done in two parts.
#
# 1. The statement's *range* query is built: the relation representing
#    the statement's target Object with any WHERE quals taken into account.
#
# 2. The statement body is processed to generate a series of
#    SQL substatements to modify all relations touched by the statement
#    depending on the link layout.
#

import typing

from edgedb.lang.common import ast

from edgedb.lang.ir import ast as irast

from edgedb.lang.schema import scalars as s_scalars
from edgedb.lang.schema import lproperties as s_lprops

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import common
from edgedb.server.pgsql import types as pg_types

from . import astutils
from . import clauses
from . import context
from . import dbobj
from . import dispatch
from . import pathctx
from . import relctx
from . import typecomp


def init_dml_stmt(
        ir_stmt: irast.MutatingStmt, dml_stmt: pgast.DML, *,
        ctx: context.CompilerContextLevel,
        parent_ctx: context.CompilerContextLevel) \
        -> typing.Tuple[pgast.Query, pgast.CommonTableExpr,
                        pgast.CommonTableExpr]:
    """Prepare the common structure of the query representing a DML stmt.

    :param ir_stmt:
        IR of the statement.
    :param dml_stmt:
        SQL DML node instance.

    :return:
        A (*wrapper*, *dml_cte*, *range_cte*) tuple, where *wrapper* the
        the wrapping SQL statement, *dml_cte* is the CTE representing the
        SQL DML operation in the main relation of the Object, and
        *range_cte* is the CTE for the subset affected by the statement.
        *range_cte* is None for INSERT statmenets.
    """
    wrapper = ctx.rel

    clauses.init_stmt(ir_stmt, ctx, parent_ctx)

    target_ir_set = ir_stmt.subject

    dml_stmt.relation = dbobj.range_for_set(
        ir_stmt.subject, include_overlays=False, env=ctx.env)
    pathctx.put_path_value_rvar(
        dml_stmt, target_ir_set.path_id, dml_stmt.relation, env=ctx.env)
    dml_stmt.path_scope.add(target_ir_set.path_id)

    dml_cte = pgast.CommonTableExpr(
        query=dml_stmt,
        name=ctx.env.aliases.get(hint='m')
    )

    if isinstance(ir_stmt, (irast.UpdateStmt, irast.DeleteStmt)):
        # UPDATE and DELETE operate over a range, so generate
        # the corresponding CTE and connect it to the DML query.
        range_cte = get_dml_range(ir_stmt, dml_stmt, ctx=ctx)

        range_rvar = pgast.RangeVar(
            relation=range_cte,
            alias=pgast.Alias(
                aliasname=ctx.env.aliases.get(hint='range')
            )
        )

        relctx.pull_path_namespace(
            target=dml_stmt, source=range_rvar, ctx=ctx)

        # Auxillary relations are always joined via the WHERE
        # clause due to the structure of the UPDATE/DELETE SQL statments.
        id_col = common.edgedb_name_to_pg_name('std::id')
        dml_stmt.where_clause = astutils.new_binop(
            lexpr=pgast.ColumnRef(name=[
                dml_stmt.relation.alias.aliasname,
                id_col
            ]),
            op=ast.ops.EQ,
            rexpr=pathctx.get_rvar_path_identity_var(
                range_rvar, target_ir_set.path_id, env=ctx.env)
        )

        # UPDATE has "FROM", while DELETE has "USING".
        if hasattr(dml_stmt, 'from_clause'):
            dml_stmt.from_clause.append(range_rvar)
        else:
            dml_stmt.using_clause.append(range_rvar)

    else:
        range_cte = None

    # Due to the fact that DML statements are structured
    # as a flat list of CTEs instead of nested range vars,
    # the top level path scope must be empty.  The necessary
    # range vars will be injected explicitly in all rels that
    # need them.
    ctx.path_scope.clear()

    pathctx.put_path_value_rvar(
        dml_stmt, ir_stmt.subject.path_id, dml_stmt.relation, env=ctx.env)

    dml_rvar = pgast.RangeVar(
        relation=dml_cte,
        alias=pgast.Alias(aliasname=parent_ctx.env.aliases.get('d'))
    )

    relctx.include_rvar(wrapper, dml_rvar, ir_stmt.subject.path_id,
                        aspect='value', ctx=ctx)

    pathctx.put_path_bond(wrapper, ir_stmt.subject.path_id)

    return wrapper, dml_cte, dml_rvar, range_cte


def fini_dml_stmt(
        ir_stmt: irast.MutatingStmt, wrapper: pgast.Query,
        dml_cte: pgast.CommonTableExpr,
        dml_rvar: pgast.BaseRangeVar, *,
        parent_ctx: context.CompilerContextLevel,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    # Record the effect of this insertion in the relation overlay
    # context to ensure that the RETURNING clause potentially
    # referencing this class yields the expected results.
    if isinstance(ir_stmt, irast.InsertStmt):
        dbobj.add_rel_overlay(ir_stmt.subject.scls, 'union', dml_cte,
                              env=ctx.env)
    elif isinstance(ir_stmt, irast.DeleteStmt):
        dbobj.add_rel_overlay(ir_stmt.subject.scls, 'except', dml_cte,
                              env=ctx.env)

    if parent_ctx.toplevel_stmt is wrapper:
        ret_ref = pathctx.get_path_identity_var(
            wrapper, ir_stmt.subject.path_id, env=parent_ctx.env)
        count = pgast.FuncCall(name=('count',), args=[ret_ref])
        wrapper.target_list = [
            pgast.ResTarget(val=count)
        ]

    clauses.fini_stmt(wrapper, ctx, parent_ctx)

    return wrapper


def get_dml_range(
        ir_stmt: irast.MutatingStmt,
        dml_stmt: pgast.DML, *,
        ctx: context.CompilerContextLevel) -> pgast.CommonTableExpr:
    """Create a range CTE for the given DML statement.

    :param ir_stmt:
        IR of the statement.
    :param dml_stmt:
        SQL DML node instance.

    :return:
        A CommonTableExpr node representing the range affected
        by the DML statement.
    """
    target_ir_set = ir_stmt.subject
    ir_qual_expr = ir_stmt.where

    with ctx.newscope() as scopectx, scopectx.newrel() as subctx:
        subctx.expr_exposed = False
        range_stmt = subctx.rel

        # init_stmt() has associated all top-level paths with
        # the main query, which is at the very bottom.
        # Hoist that scope to the modification range statement
        # instead.
        for path_id, stmt in ctx.path_scope.items():
            if stmt is ctx.rel or path_id == ir_stmt.subject.path_id:
                scopectx.path_scope[path_id] = range_stmt

        if ir_stmt.parent_stmt is not None:
            iterator_set = ir_stmt.parent_stmt.iterator_stmt
        else:
            iterator_set = None

        if iterator_set is not None:
            scopectx.path_scope[iterator_set.path_id] = range_stmt
            relctx.update_scope(iterator_set, range_stmt, ctx=subctx)
            iterator_rvar = clauses.compile_iterator_expr(
                range_stmt, iterator_set, ctx=subctx)
            relctx.include_rvar(range_stmt, iterator_rvar,
                                iterator_set.path_id, ctx=subctx)

        dispatch.compile(target_ir_set, ctx=subctx)

        pathctx.get_path_identity_output(
            range_stmt, target_ir_set.path_id, env=subctx.env)

        if ir_qual_expr is not None:
            range_stmt.where_clause = astutils.extend_binop(
                range_stmt.where_clause,
                clauses.compile_filter_clause(ir_qual_expr, ctx=subctx))

        range_cte = pgast.CommonTableExpr(
            query=range_stmt,
            name=ctx.env.aliases.get('range')
        )

        return range_cte


def process_insert_body(
        ir_stmt: irast.MutatingStmt,
        wrapper: pgast.Query,
        insert_cte: pgast.CommonTableExpr,
        insert_rvar: pgast.BaseRangeVar, *,
        ctx: context.CompilerContextLevel) -> None:
    """Generate SQL DML CTEs from an InsertStmt IR.

    :param ir_stmt:
        IR of the statement.
    :param wrapper:
        Top-level SQL query.
    :param insert_cte:
        CTE representing the SQL INSERT to the main relation of the Object.
    """
    cols = [pgast.ColumnRef(name=['std::__type__'])]
    select = pgast.SelectStmt(target_list=[])
    values = select.target_list

    # The main INSERT query of this statement will always be
    # present to insert at least the std::id and std::__type__
    # links.
    insert_stmt = insert_cte.query

    insert_stmt.cols = cols
    insert_stmt.select_stmt = select

    if ir_stmt.parent_stmt is not None:
        iterator_set = ir_stmt.parent_stmt.iterator_stmt
    else:
        iterator_set = None

    if iterator_set is not None:
        with ctx.substmt() as ictx:
            ictx.path_scope = ictx.path_scope.new_child()
            ictx.path_scope[iterator_set.path_id] = ictx.rel
            clauses.compile_iterator_expr(ictx.rel, iterator_set, ctx=ictx)
            ictx.rel.path_id = iterator_set.path_id
            pathctx.put_path_bond(ictx.rel, iterator_set.path_id)
            iterator_cte = pgast.CommonTableExpr(
                query=ictx.rel,
                name=ctx.env.aliases.get('iter')
            )
            ictx.toplevel_stmt.ctes.append(iterator_cte)
        iterator_rvar = dbobj.rvar_for_rel(iterator_cte, env=ctx.env)
        relctx.include_rvar(select, iterator_rvar,
                            path_id=ictx.rel.path_id, ctx=ctx)
        iterator_id = pathctx.get_path_identity_var(
            select, iterator_set.path_id, env=ctx.env)
    else:
        iterator_cte = None
        iterator_id = None

    values.append(
        pgast.ResTarget(
            val=pgast.SelectStmt(
                target_list=[
                    pgast.ResTarget(
                        val=pgast.ColumnRef(name=['id']))
                ],
                from_clause=[
                    pgast.RangeVar(relation=pgast.Relation(
                        name='objecttype', schemaname='edgedb'))
                ],
                where_clause=astutils.new_binop(
                    op=ast.ops.EQ,
                    lexpr=pgast.ColumnRef(name=['name']),
                    rexpr=pgast.Constant(val=ir_stmt.subject.scls.shortname)
                )
            )
        )
    )

    external_inserts = []
    tuple_elements = []
    parent_link_props = []

    with ctx.newrel() as subctx:
        subctx.rel = select
        subctx.rel_hierarchy[select] = insert_stmt

        subctx.expr_exposed = False
        subctx.shape_format = context.ShapeFormat.FLAT

        if iterator_cte is not None:
            subctx.path_scope = ctx.path_scope.new_child()
            subctx.path_scope[iterator_cte.query.path_id] = select

        # Process the Insert IR and separate links that go
        # into the main table from links that are inserted into
        # a separate link table.
        for shape_el in ir_stmt.subject.shape:
            rptr = shape_el.rptr
            ptrcls = rptr.ptrcls.material_type()

            if (isinstance(ptrcls, s_lprops.LinkProperty) and
                    rptr.source.path_id != ir_stmt.subject.path_id):
                parent_link_props.append(shape_el)
                continue

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, schema=subctx.env.schema, resolve_type=True,
                link_bias=False)

            props_only = False

            # First, process all local link inserts.
            if ptr_info.table_type == 'ObjectType':
                props_only = True
                field = pgast.ColumnRef(name=[ptr_info.column_name])
                cols.append(field)

                insvalue = insert_value_for_shape_element(
                    insert_stmt, wrapper, ir_stmt, shape_el, iterator_id,
                    ptr_info=ptr_info, ctx=subctx)

                tuple_el = astutils.tuple_element_for_shape_el(shape_el, field)
                tuple_elements.append(tuple_el)
                values.append(pgast.ResTarget(val=insvalue))

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, resolve_type=False, link_bias=True)

            if ptr_info and ptr_info.table_type == 'link':
                external_inserts.append((shape_el, props_only))

        if iterator_cte is not None:
            cols.append(pgast.ColumnRef(name=['__edb_token']))

            values.append(pgast.ResTarget(val=iterator_id))

            pathctx.put_path_identity_var(
                insert_stmt, iterator_set.path_id,
                cols[-1], force=True, env=subctx.env
            )

            pathctx.put_path_bond(insert_stmt, iterator_set.path_id)

    toplevel = ctx.toplevel_stmt
    toplevel.ctes.append(insert_cte)

    # Process necessary updates to the link tables.
    for shape_el, props_only in external_inserts:
        process_link_update(
            ir_stmt, shape_el, props_only, wrapper,
            insert_cte, iterator_cte, ctx=ctx)

    if parent_link_props:
        prop_elements = []

        with ctx.newscope() as scopectx:
            scopectx.rel = wrapper

            for shape_el in parent_link_props:
                rptr = shape_el.rptr
                scopectx.path_scope[rptr.source.path_id] = wrapper
                pathctx.put_path_rvar_if_not_exists(
                    wrapper, rptr.source.path_id, insert_rvar,
                    aspect='value', env=scopectx.env)
                dispatch.compile(shape_el, ctx=scopectx)
                tuple_el = astutils.tuple_element_for_shape_el(shape_el, None)
                prop_elements.append(tuple_el)

        valtuple = pgast.TupleVar(elements=prop_elements, named=True)
        pathctx.put_path_value_var(
            wrapper, ir_stmt.subject.path_id,
            valtuple, force=True, env=ctx.env)


def compile_insert_shape_element(
        insert_stmt: pgast.InsertStmt,
        wrapper: pgast.Query,
        ir_stmt: irast.MutatingStmt,
        shape_el: irast.Set,
        iterator_id: pgast.OutputVar, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    with ctx.newscope() as insvalctx:
        if iterator_id is not None:
            insvalctx.volatility_ref = iterator_id
        else:
            # Single inserts have no need for forced
            # computable volatility, and, furthermore,
            # we do not have a valid identity reference
            # anyway.
            insvalctx.volatility_ref = context.NO_VOLATILITY

        insvalctx.path_scope[ir_stmt.subject.path_id] = insert_stmt
        dispatch.compile(shape_el, ctx=insvalctx)

    return insvalctx.rel


def insert_value_for_shape_element(
        insert_stmt: pgast.InsertStmt,
        wrapper: pgast.Query,
        ir_stmt: irast.MutatingStmt,
        shape_el: irast.Set,
        iterator_id: pgast.OutputVar, *,
        ptr_info: pg_types.PointerStorageInfo,
        ctx: context.CompilerContextLevel) -> pgast.OutputVar:

    rel = compile_insert_shape_element(
        insert_stmt, wrapper, ir_stmt, shape_el, iterator_id, ctx=ctx)

    insvalue = pathctx.get_path_value_var(
        rel, shape_el.path_id, env=ctx.env)

    if isinstance(insvalue, pgast.TupleVar):
        for element in insvalue.elements:
            name = element.path_id.rptr_name()
            if name == 'std::target':
                insvalue = pathctx.get_path_value_var(
                    rel, element.path_id,
                    env=ctx.env)
                break
        else:
            raise RuntimeError('could not find std::target in '
                               'insert computable')
    insvalue = pgast.TypeCast(
        arg=insvalue,
        type_name=typecomp.type_node(ptr_info.column_type))

    return insvalue


def process_update_body(
        ir_stmt: irast.MutatingStmt,
        wrapper: pgast.Query, update_cte: pgast.CommonTableExpr,
        range_cte: pgast.CommonTableExpr, *,
        ctx: context.CompilerContextLevel):
    """Generate SQL DML CTEs from an UpdateStmt IR.

    :param ir_stmt:
        IR of the statement.
    :param wrapper:
        Top-level SQL query.
    :param update_cte:
        CTE representing the SQL UPDATE to the main relation of the Object.
    :param range_cte:
        CTE representing the range affected by the statement.
    """
    update_stmt = update_cte.query

    external_updates = []

    toplevel = ctx.toplevel_stmt
    toplevel.ctes.append(range_cte)
    toplevel.ctes.append(update_cte)

    with ctx.newscope() as subctx:
        # It is necessary to process the expressions in
        # the UpdateStmt shape body in the context of the
        # UPDATE statement so that references to the current
        # values of the updated object are resolved correctly.
        subctx.path_scope[ir_stmt.subject.path_id] = update_stmt
        subctx.rel = update_stmt
        subctx.expr_exposed = False
        subctx.shape_format = context.ShapeFormat.FLAT

        for shape_el in ir_stmt.subject.shape:
            with subctx.newscope() as scopectx:
                ptrcls = shape_el.rptr.ptrcls
                updvalue = shape_el.expr

                ptr_info = pg_types.get_pointer_storage_info(
                    ptrcls, schema=scopectx.env.schema, resolve_type=True,
                    link_bias=False)

                # First, process all internal link updates
                if ptr_info.table_type == 'ObjectType':
                    updvalue = pgast.TypeCast(
                        arg=dispatch.compile(updvalue, ctx=scopectx),
                        type_name=typecomp.type_node(ptr_info.column_type))

                    update_stmt.targets.append(
                        pgast.UpdateTarget(
                            name=ptr_info.column_name,
                            val=updvalue))

            props_only = is_props_only_update(shape_el)

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, resolve_type=False, link_bias=True)

            if ptr_info and ptr_info.table_type == 'link':
                external_updates.append((shape_el, props_only))

    if not update_stmt.targets:
        # No updates directly to the set target table,
        # so convert the UPDATE statement into a SELECT.
        update_cte.query = pgast.SelectStmt(
            ctes=update_stmt.ctes,
            target_list=update_stmt.returning_list,
            from_clause=[update_stmt.relation] + update_stmt.from_clause,
            where_clause=update_stmt.where_clause,
            path_namespace=update_stmt.path_namespace,
            path_outputs=update_stmt.path_outputs,
            path_scope=update_stmt.path_scope,
            path_rvar_map=update_stmt.path_rvar_map.copy(),
            view_path_id_map=update_stmt.view_path_id_map.copy(),
            ptr_join_map=update_stmt.ptr_join_map.copy(),
        )

    # Process necessary updates to the link tables.
    for expr, props_only in external_updates:
        if props_only:
            process_linkprop_update(
                ir_stmt, expr, wrapper, update_cte, ctx=ctx)
        else:
            process_link_update(
                ir_stmt, expr, False, wrapper, update_cte, None, ctx=ctx)


def is_props_only_update(shape_el: irast.Set) -> bool:
    """Determine whether a link update is a property-only update.

    :param shape_el:
        IR of the shape element representing a link update.

    :return:
        `True` if *shape_el* represents a link property-only update.
    """
    return all(isinstance(el.rptr.ptrcls, s_lprops.LinkProperty)
               for el in shape_el.shape)


def process_link_update(
        ir_stmt: irast.MutatingStmt, ir_expr: irast.Base,
        props_only: bool, wrapper: pgast.Query,
        dml_cte: pgast.CommonTableExpr, iterator_cte: pgast.CommonTableExpr, *,
        ctx: context.CompilerContextLevel) -> typing.Optional[pgast.Query]:
    """Perform updates to a link relation as part of a DML statement.

    :param ir_stmt:
        IR of the statement.
    :param ir_expr:
        IR of the INSERT/UPDATE body element.
    :param props_only:
        Whether this link update only touches link properties.
    :param wrapper:
        Top-level SQL query.
    :param dml_cte:
        CTE representing the SQL INSERT or UPDATE to the main
        relation of the Object.
    :param iterator_cte:
        CTE representing the iterator range in the FOR clause of the
        EdgeQL DML statement.
    """
    toplevel = ctx.toplevel_stmt

    edgedb_link = pgast.RangeVar(
        relation=pgast.Relation(
            schemaname='edgedb', name='link'
        ),
        alias=pgast.Alias(aliasname=ctx.env.aliases.get(hint='l')))

    ltab_alias = edgedb_link.alias.aliasname

    rptr = ir_expr.rptr
    ptrcls = rptr.ptrcls
    target_is_scalar = isinstance(ptrcls.target, s_scalars.ScalarType)

    path_id = rptr.source.path_id.extend(
        ptrcls, rptr.direction, rptr.target.scls)

    # The links in the dml class shape have been derived,
    # but we must use the correct specialized link class for the
    # base material type.
    mptrcls = ptrcls.material_type()

    # Lookup link class id by link name.
    lname_to_id = pgast.CommonTableExpr(
        query=pgast.SelectStmt(
            from_clause=[
                edgedb_link
            ],
            target_list=[
                pgast.ResTarget(
                    val=pgast.ColumnRef(name=[ltab_alias, 'id']))
            ],
            where_clause=astutils.new_binop(
                lexpr=pgast.ColumnRef(name=[ltab_alias, 'name']),
                rexpr=pgast.Constant(val=mptrcls.name),
                op=ast.ops.EQ
            )
        ),
        name=ctx.env.aliases.get(hint='lid')
    )

    lname_to_id_rvar = pgast.RangeVar(relation=lname_to_id)
    toplevel.ctes.append(lname_to_id)

    target_rvar = dbobj.range_for_ptrcls(
        mptrcls, '>', include_overlays=False, env=ctx.env)
    target_alias = target_rvar.alias.aliasname

    if target_is_scalar:
        target_tab_name = (target_rvar.relation.schemaname,
                           target_rvar.relation.name)
    else:
        target_tab_name = common.link_name_to_table_name(
            mptrcls.shortname, catenate=False)

    tab_cols = \
        ctx.env.backend._type_mech.get_cached_table_columns(target_tab_name)

    assert tab_cols, "could not get cols for {!r}".format(target_tab_name)

    dml_cte_rvar = pgast.RangeVar(
        relation=dml_cte,
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get('m')
        )
    )

    col_data = {
        'link_type_id': pgast.ColumnRef(
            name=[
                lname_to_id.name,
                'id'
            ]
        ),
        'std::source': pathctx.get_rvar_path_identity_var(
            dml_cte_rvar, ir_stmt.subject.path_id, env=ctx.env)
    }

    # Drop all previous link records for this source.
    delcte = pgast.CommonTableExpr(
        query=pgast.DeleteStmt(
            relation=target_rvar,
            where_clause=astutils.new_binop(
                lexpr=col_data['std::source'],
                op=ast.ops.EQ,
                rexpr=pgast.ColumnRef(
                    name=[target_alias, 'std::source'])
            ),
            using_clause=[dml_cte_rvar],
            returning_list=[
                pgast.ResTarget(
                    val=pgast.ColumnRef(
                        name=[target_alias, pgast.Star()]))
            ]
        ),
        name=ctx.env.aliases.get(hint='d')
    )

    pathctx.put_path_value_rvar(
        delcte.query, path_id.ptr_path(), target_rvar, env=ctx.env)

    # Record the effect of this removal in the relation overlay
    # context to ensure that the RETURNING clause potentially
    # referencing this link yields the expected results.
    overlays = ctx.env.rel_overlays[ptrcls.shortname]
    overlays.append(('except', delcte))
    toplevel.ctes.append(delcte)

    # Turn the IR of the expression on the right side of :=
    # into a subquery returning records for the link table.
    data_cte = process_link_values(
        ir_stmt, ir_expr, target_tab_name, tab_cols, col_data,
        dml_cte_rvar, [lname_to_id_rvar],
        props_only, target_is_scalar, iterator_cte, ctx=ctx)

    toplevel.ctes.append(data_cte)

    data_select = pgast.SelectStmt(
        target_list=[
            pgast.ResTarget(
                val=pgast.ColumnRef(
                    name=[data_cte.name, pgast.Star()]))
        ],
        from_clause=[
            pgast.RangeVar(relation=data_cte)
        ]
    )

    # Inserting rows into the link table may produce cardinality
    # constraint violations, since the INSERT into the link table
    # is executed in the snapshot where the above DELETE from
    # the link table is not visible.  Hence, we need to use
    # the ON CONFLICT clause to resolve this.
    conflict_cols = ['std::source', 'std::target', 'link_type_id']
    conflict_inference = []
    conflict_exc_row = []

    for col in conflict_cols:
        conflict_inference.append(
            pgast.ColumnRef(name=[col])
        )
        conflict_exc_row.append(
            pgast.ColumnRef(name=['excluded', col])
        )

    conflict_data = pgast.SelectStmt(
        target_list=[
            pgast.ResTarget(
                val=pgast.ColumnRef(
                    name=[data_cte.name, pgast.Star()]))
        ],
        from_clause=[
            pgast.RangeVar(relation=data_cte)
        ],
        where_clause=astutils.new_binop(
            lexpr=pgast.ImplicitRowExpr(args=conflict_inference),
            rexpr=pgast.ImplicitRowExpr(args=conflict_exc_row),
            op='='
        )
    )

    cols = [pgast.ColumnRef(name=[col]) for col in tab_cols]
    updcte = pgast.CommonTableExpr(
        name=ctx.env.aliases.get(hint='i'),
        query=pgast.InsertStmt(
            relation=target_rvar,
            select_stmt=data_select,
            cols=cols,
            on_conflict=pgast.OnConflictClause(
                action='update',
                infer=pgast.InferClause(
                    index_elems=conflict_inference
                ),
                target_list=[
                    pgast.MultiAssignRef(
                        columns=cols,
                        source=conflict_data
                    )
                ]
            ),
            returning_list=[
                pgast.ResTarget(
                    val=pgast.ColumnRef(name=[pgast.Star()])
                )
            ]
        )
    )

    pathctx.put_path_value_rvar(
        updcte.query, path_id.ptr_path(), target_rvar, env=ctx.env)

    # Record the effect of this insertion in the relation overlay
    # context to ensure that the RETURNING clause potentially
    # referencing this link yields the expected results.
    overlays = ctx.env.rel_overlays[ptrcls.shortname]
    overlays.append(('union', updcte))

    toplevel.ctes.append(updcte)

    return data_cte


def process_linkprop_update(
        ir_stmt: irast.MutatingStmt, ir_expr: irast.Base,
        wrapper: pgast.Query, dml_cte: pgast.CommonTableExpr, *,
        ctx: context.CompilerContextLevel) -> None:
    """Perform link property updates to a link relation.

    :param ir_stmt:
        IR of the statement.
    :param ir_expr:
        IR of the UPDATE body element.
    :param wrapper:
        Top-level SQL query.
    :param dml_cte:
        CTE representing the SQL UPDATE to the main relation of the Object.
    """
    toplevel = ctx.toplevel_stmt

    rptr = ir_expr.rptr
    ptrcls = rptr.ptrcls
    target_is_scalar = isinstance(rptr.target, s_scalars.ScalarType)

    target_tab = dbobj.range_for_ptrcls(
        ptrcls, '>', include_overlays=False, env=ctx.env)

    if target_is_scalar:
        target_tab_name = (target_tab.schema, target_tab.name)
    else:
        target_tab_name = common.link_name_to_table_name(
            ptrcls.shortname, catenate=False)

    tab_cols = \
        ctx.env.backend._type_mech.get_cached_table_columns(target_tab_name)

    assert tab_cols, "could not get cols for {!r}".format(target_tab_name)

    dml_cte_rvar = pgast.RangeVar(
        relation=dml_cte,
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get('m')
        )
    )

    cond = astutils.new_binop(
        pathctx.get_rvar_path_identity_var(
            dml_cte_rvar, ir_stmt.subject.path_id, env=ctx.env),
        dbobj.get_column(target_tab, 'std::source'),
        ast.ops.EQ
    )

    targets = []
    for prop_el in ir_expr.shape:
        ptrname = prop_el.rptr.ptrcls.shortname
        with ctx.new() as input_rel_ctx:
            input_rel_ctx.expr_exposed = False
            input_rel = dispatch.compile(prop_el.expr, ctx=input_rel_ctx)
            targets.append(
                pgast.UpdateTarget(
                    name=common.edgedb_name_to_pg_name(ptrname),
                    val=input_rel
                )
            )

    updstmt = pgast.UpdateStmt(
        relation=target_tab,
        where_clause=cond,
        targets=targets,
        from_clause=[dml_cte_rvar]
    )

    updcte = pgast.CommonTableExpr(
        query=updstmt,
        name=ctx.env.aliases.get(ptrcls.shortname.name)
    )

    toplevel.ctes.append(updcte)


def process_link_values(
        ir_stmt, ir_expr, target_tab, tab_cols, col_data,
        dml_rvar, sources, props_only, target_is_scalar, iterator_cte, *,
        ctx=context.CompilerContext) -> pgast.CommonTableExpr:
    """Unpack data from an update expression into a series of selects.

    :param ir_expr:
        IR of the INSERT/UPDATE body element.
    :param target_tab:
        The link table being updated.
    :param tab_cols:
        A sequence of columns in the table being updated.
    :param col_data:
        Expressions used to populate well-known columns of the link
        table such as std::source and std::__type__.
    :param sources:
        A list of relations which must be joined into the data query
        to resolve expressions in *col_data*.
    :param props_only:
        Whether this link update only touches link properties.
    :param target_is_scalar:
        Whether the link target is an ScalarType.
    :param iterator_cte:
        CTE representing the iterator range in the FOR clause of the
        EdgeQL DML statement.
    """
    with ctx.newscope() as newscope, newscope.newrel() as subrelctx:
        row_query = subrelctx.rel

        relctx.include_rvar(row_query, dml_rvar, ctx=subrelctx)
        subrelctx.path_scope[ir_stmt.subject.path_id] = row_query

        if iterator_cte is not None:
            iterator_rvar = dbobj.rvar_for_rel(
                iterator_cte, lateral=True, env=subrelctx.env)
            relctx.include_rvar(row_query, iterator_rvar,
                                iterator_cte.query.path_id,
                                aspect='value', ctx=subrelctx)

        with subrelctx.newscope() as sctx, sctx.subrel() as input_rel_ctx:
            input_rel = input_rel_ctx.rel
            if iterator_cte is not None:
                input_rel_ctx.path_scope[iterator_cte.query.path_id] = \
                    row_query
            input_rel_ctx.expr_exposed = False
            input_rel_ctx.shape_format = context.ShapeFormat.FLAT
            input_rel_ctx.volatility_ref = pathctx.get_path_identity_var(
                row_query, ir_stmt.subject.path_id, env=input_rel_ctx.env)
            dispatch.compile(ir_expr, ctx=input_rel_ctx)

    input_stmt = input_rel

    input_rvar = pgast.RangeSubselect(
        subquery=input_rel,
        lateral=True,
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get('val')
        )
    )

    row = pgast.ImplicitRowExpr()

    source_data = {}

    if input_stmt.op is not None:
        # UNION
        input_stmt = input_stmt.rarg

    path_id = ir_expr.path_id

    output = pathctx.get_path_value_output(
        input_stmt, path_id, env=ctx.env)

    if isinstance(output, pgast.TupleVar):
        for element in output.elements:
            name = element.path_id.rptr_name()
            if name is None:
                name = element.path_id[-1].name
            colname = common.edgedb_name_to_pg_name(name)
            if target_is_scalar and colname == 'std::target':
                colname = 'std::target@inline'

            source_data.setdefault(
                colname, dbobj.get_column(input_rvar, element.name))
    else:
        if target_is_scalar:
            target_ref = pathctx.get_rvar_path_value_var(
                input_rvar, path_id, env=ctx.env)
            source_data['std::target@inline'] = target_ref
        else:
            target_ref = pathctx.get_rvar_path_identity_var(
                input_rvar, path_id, env=ctx.env)
            source_data['std::target'] = target_ref

    if not target_is_scalar and 'std::target' not in source_data:
        target_ref = pathctx.get_rvar_path_identity_var(
            input_rvar, path_id, env=ctx.env)
        source_data['std::target'] = target_ref

    for col in tab_cols:
        expr = col_data.get(col)
        if expr is None:
            expr = source_data.get(col)

        if expr is None:
            if tab_cols[col]['column_default'] is not None:
                expr = pgast.LiteralExpr(
                    expr=tab_cols[col]['column_default'])
            else:
                expr = pgast.Constant(val=None)

        row.args.append(expr)

    row_query.target_list = [
        pgast.ResTarget(
            val=pgast.Indirection(
                arg=pgast.TypeCast(
                    arg=row,
                    type_name=pgast.TypeName(
                        name=target_tab
                    )
                ),
                indirection=[pgast.Star()]
            )
        )
    ]

    row_query.from_clause += list(sources) + [input_rvar]

    link_rows = pgast.CommonTableExpr(
        query=row_query,
        name=ctx.env.aliases.get(hint='r')
    )

    return link_rows
