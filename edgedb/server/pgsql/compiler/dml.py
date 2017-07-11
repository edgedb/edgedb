##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

"""IR compiler support for INSERT/UPDATE/DELETE statements."""

#
# The processing of the DML statement is done in three parts.
#
# 1. The statement's *range* query is built: the relation representing
#    the statement's target Class with any WHERE quals taken into account.
#
# 2. The statement body is processed to generate a series of
#    SQL substatements to modify all relations touched by the statement
#    depending on the link layout.
#
# 3. The RETURNING statement is processed to and generates a SELECT statement
#    producing the result rows.  Note that the SQL's RETURNING is not used
#    on the top level, as need to be able to compute an arbitrary expression
#    in EdgeQL RETURNING clause.
#

import typing

from edgedb.lang.common import ast

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import lproperties as s_lprops

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import common
from edgedb.server.pgsql import types as pg_types

from . import astutils
from . import boilerplate
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
        SQL DML operation in the main relation of the Class, and
        *range_cte* is the CTE for the subset affected by the statement.
        *range_cte* is None for INSERT statmenets.
    """
    ctx.stmt = ctx.query = ctx.rel = wrapper = pgast.SelectStmt()

    boilerplate.init_stmt(ir_stmt, ctx, parent_ctx)

    target_ir_set = ir_stmt.subject

    dml_stmt.relation = dbobj.range_for_set(
        ctx.env, ir_stmt.subject, include_overlays=False)
    pathctx.put_path_rvar(
        ctx.env, dml_stmt, target_ir_set.path_id, dml_stmt.relation)
    dml_stmt.path_bonds.add(target_ir_set.path_id)

    dml_cte = pgast.CommonTableExpr(
        query=dml_stmt,
        name=ctx.genalias(hint='m')
    )

    # Mark the DML statemetn as a "root" relation so that the
    # compiler knows how to recurse into it while resolving
    # path vars.
    ctx.env.root_rels.add(dml_stmt)

    if isinstance(ir_stmt, (irast.UpdateStmt, irast.DeleteStmt)):
        # UPDATE and DELETE operate over a range, so generate
        # the corresponding CTE and connect it to the DML query.
        range_cte = get_dml_range(ir_stmt, dml_stmt, ctx=ctx)

        range_rvar = pgast.RangeVar(
            relation=range_cte,
            alias=pgast.Alias(
                aliasname=ctx.genalias(hint='range')
            )
        )

        ctx.subquery_map[dml_stmt][range_cte] = range_rvar

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
        # No range CTE for INSERT statements, however we need
        # to make sure it RETURNs the inserted entity id, which
        # we will require when updating the link relations as
        # a result of INSERT body processing.
        range_cte = None

    pathctx.put_path_rvar(
        ctx.env, dml_stmt, ir_stmt.subject.path_id, dml_stmt.relation)

    return wrapper, dml_cte, range_cte


def fini_dml_stmt(
        ir_stmt: irast.MutatingStmt, wrapper: pgast.Query,
        dml_cte: pgast.CommonTableExpr, *,
        parent_ctx: context.CompilerContextLevel,
        ctx: context.CompilerContextLevel) -> pgast.Query:
    dml_rvar = pgast.RangeVar(
        relation=dml_cte,
        alias=pgast.Alias(aliasname=parent_ctx.genalias('d'))
    )

    # Record the effect of this insertion in the relation overlay
    # context to ensure that the RETURNING clause potentially
    # referencing this class yields the expected results.
    if isinstance(ir_stmt, irast.InsertStmt):
        dbobj.add_rel_overlay(ir_stmt.subject.scls, 'union', dml_cte,
                              env=ctx.env)
    elif isinstance(ir_stmt, irast.DeleteStmt):
        dbobj.add_rel_overlay(ir_stmt.subject.scls, 'except', dml_cte,
                              env=ctx.env)

    if parent_ctx.toplevel_stmt is None:
        ret_ref = pathctx.get_rvar_path_identity_var(
            dml_rvar, ir_stmt.subject.path_id, env=parent_ctx.env)
        count = pgast.FuncCall(name=('count',), args=[ret_ref])
        wrapper.target_list = [
            pgast.ResTarget(val=count)
        ]
        wrapper.from_clause.append(dml_rvar)
    else:
        wrapper.from_clause.append(dml_rvar)
        relctx.pull_path_namespace(
            target=wrapper, source=dml_rvar, ctx=ctx)

    boilerplate.fini_stmt(wrapper, ctx, parent_ctx)

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

    with ctx.subquery() as subctx:
        subctx.expr_exposed = False
        range_stmt = subctx.query

        id_set = irutils.extend_path(ctx.env.schema, target_ir_set, 'std::id')
        dispatch.compile(id_set, ctx=subctx)

        target_cte = relctx.get_set_cte(target_ir_set, ctx=subctx)

        pathctx.put_path_rvar(
            subctx.env, range_stmt, target_ir_set.path_id,
            subctx.subquery_map[range_stmt][target_cte])

        pathctx.get_path_identity_output(
            range_stmt, id_set.path_id, env=subctx.env)

        if ir_qual_expr is not None:
            with subctx.new() as newctx:
                newctx.clause = 'where'
                range_stmt.where_clause = dispatch.compile(
                    ir_qual_expr, ctx=newctx)

        relctx.enforce_path_scope(range_stmt, ctx.path_bonds, ctx=subctx)

        range_cte = pgast.CommonTableExpr(
            query=range_stmt,
            name=ctx.env.aliases.get('range')
        )

        return range_cte


def process_insert_body(
        ir_stmt: irast.MutatingStmt, wrapper: pgast.Query,
        insert_cte: pgast.CommonTableExpr, *,
        ctx: context.CompilerContextLevel) -> None:
    """Generate SQL DML CTEs from an InsertStmt IR.

    :param ir_stmt:
        IR of the statement.
    :param wrapper:
        Top-level SQL query.
    :param insert_cte:
        CTE representing the SQL INSERT to the main relation of the Class.
    """
    cols = [pgast.ColumnRef(name=['std::__class__'])]
    select = pgast.SelectStmt(target_list=[])
    values = select.target_list

    # The main INSERT query of this statement will always be
    # present to insert at least the std::id and std::__class__
    # links.
    insert_stmt = insert_cte.query

    insert_stmt.cols = cols
    insert_stmt.select_stmt = select

    iterator_cte = boilerplate.compile_iterator_expr(select, ir_stmt, ctx=ctx)

    values.append(
        pgast.ResTarget(
            val=pgast.SelectStmt(
                target_list=[
                    pgast.ResTarget(
                        val=pgast.ColumnRef(name=['id']))
                ],
                from_clause=[
                    pgast.RangeVar(relation=pgast.Relation(
                        relname='concept', schemaname='edgedb'))
                ],
                where_clause=astutils.new_binop(
                    op=ast.ops.EQ,
                    lexpr=pgast.ColumnRef(name=['name']),
                    rexpr=pgast.Constant(val=ir_stmt.subject.scls.name)
                )
            )
        )
    )

    external_inserts = []
    tuple_elements = []
    resolved_pointers = set()

    with ctx.subquery() as subctx:
        # It is necessary to process the expressions in
        # the UpdateStmt shape body in the context of the
        # UPDATE statement so that references to the current
        # values of the updated object are resolved correctly.
        subctx.rel = subctx.query = select
        subctx.expr_exposed = False
        subctx.shape_format = context.ShapeFormat.FLAT

        # Process the Insert IR and separate links that go
        # into the main table from links that are inserted into
        # a separate link table.
        for shape_el in ir_stmt.subject.shape:
            ptrcls = shape_el.rptr.ptrcls
            insvalue = shape_el.expr

            resolved_pointers.add(ptrcls)

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, schema=subctx.env.schema, resolve_type=True,
                link_bias=False)

            props_only = False

            # First, process all local link inserts.
            if ptr_info.table_type == 'concept':
                props_only = True
                field = pgast.ColumnRef(name=[ptr_info.column_name])
                cols.append(field)

                with subctx.new() as insvalctx:
                    insexpr = dispatch.compile(insvalue, ctx=insvalctx)

                    insvalue = pgast.TypeCast(
                        arg=insexpr,
                        type_name=typecomp.type_node(ptr_info.column_type))

                    tuple_elements.append(
                        astutils.tuple_element_for_shape_el(
                            shape_el, field))

                    values.append(pgast.ResTarget(val=insvalue))

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, resolve_type=False, link_bias=True)

            if ptr_info and ptr_info.table_type == 'link':
                external_inserts.append((shape_el, props_only))

        for shape_el in ir_stmt.result.shape:
            ptrcls = shape_el.rptr.ptrcls
            insvalue = shape_el.expr

            if ptrcls in resolved_pointers or insvalue is None:
                continue

            with subctx.new() as insvalctx:
                value = dispatch.compile(insvalue, ctx=insvalctx)

                tuple_elements.append(
                    astutils.tuple_element_for_shape_el(
                        shape_el, value))

        if iterator_cte is not None:
            cols.append(pgast.ColumnRef(name=['__edb_token']))

            values.append(
                pgast.ResTarget(val=pathctx.get_path_identity_var(
                    select,  ir_stmt.iterator_stmt.path_id, env=subctx.env
                ))
            )

            pathctx.put_path_identity_var(
                insert_stmt, ir_stmt.iterator_stmt.path_id,
                cols[-1], force=True, env=subctx.env
            )

            pathctx.put_path_bond(insert_stmt, ir_stmt.iterator_stmt.path_id)

    toplevel = ctx.toplevel_stmt
    toplevel.ctes.append(insert_cte)

    # Set the DML CTE as the source for paths originating
    # in its relation.
    relctx.put_set_cte(ir_stmt.subject, insert_cte, ctx=ctx)

    # Process necessary updates to the link tables.
    for shape_el, props_only in external_inserts:
        process_link_update(
            ir_stmt, shape_el, props_only, wrapper,
            insert_cte, iterator_cte, ctx=ctx)

        resolved_pointers.add(shape_el.rptr.ptrcls)

    result = astutils.TupleVar(elements=tuple_elements, named=True)
    pathctx.put_path_value_var(
        insert_stmt, ir_stmt.result.path_id, result, force=True, env=ctx.env)

    for element in tuple_elements:
        pathctx.put_path_value_var_if_not_exists(
            insert_stmt, element.path_id, element.val, env=ctx.env)


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
        CTE representing the SQL UPDATE to the main relation of the Class.
    :param range_cte:
        CTE representing the range affected by the statement.
    """
    update_stmt = update_cte.query

    external_updates = []

    boilerplate.compile_iterator_expr(
        update_stmt, ir_stmt, ctx=ctx)

    toplevel = ctx.toplevel_stmt
    toplevel.ctes.append(range_cte)
    toplevel.ctes.append(update_cte)

    with ctx.subquery() as subctx:
        # It is necessary to process the expressions in
        # the UpdateStmt shape body in the context of the
        # UPDATE statement so that references to the current
        # values of the updated object are resolved correctly.
        subctx.rel = subctx.query = update_stmt
        subctx.expr_exposed = False
        subctx.shape_format = context.ShapeFormat.FLAT
        relctx.put_set_cte(ir_stmt.subject, range_cte, ctx=subctx)

        for shape_el in ir_stmt.subject.shape:
            ptrcls = shape_el.rptr.ptrcls
            updvalue = shape_el.expr

            ptr_info = pg_types.get_pointer_storage_info(
                ptrcls, schema=subctx.env.schema, resolve_type=True,
                link_bias=False)

            # First, process all internal link updates
            if ptr_info.table_type == 'concept':
                updvalue = pgast.TypeCast(
                    arg=dispatch.compile(updvalue, ctx=subctx),
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

        relctx.enforce_path_scope(
            update_stmt, ctx.path_bonds, ctx=subctx)

    # Set the DML CTE as the source for paths originating
    # in its relation.
    relctx.put_set_cte(ir_stmt.subject, update_cte, ctx=ctx)

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
            path_bonds=update_stmt.path_bonds,
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
    if not irutils.is_subquery_set(shape_el):
        return False

    ir_set = shape_el.expr.result
    target = shape_el.rptr.target

    ir_cset = irutils.get_canonical_set(ir_set)

    if ir_cset != target:
        return False

    return all(isinstance(el.rptr.ptrcls, s_lprops.LinkProperty)
               for el in ir_set.shape)


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
        relation of the Class.
    :param iterator_cte:
        CTE representing the iterator range in the FOR clause of the
        EdgeQL DML statement.
    """
    toplevel = ctx.toplevel_stmt

    edgedb_link = pgast.RangeVar(
        relation=pgast.Relation(
            schemaname='edgedb', relname='link'
        ),
        alias=pgast.Alias(aliasname=ctx.genalias(hint='l')))

    ltab_alias = edgedb_link.alias.aliasname

    rptr = ir_expr.rptr
    ptrcls = rptr.ptrcls
    target_is_atom = isinstance(ptrcls.target, s_atoms.Atom)

    path_id = rptr.source.path_id.extend(
        ptrcls, rptr.direction, rptr.target.scls)

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
                rexpr=pgast.Constant(val=ptrcls.name),
                op=ast.ops.EQ
            )
        ),
        name=ctx.genalias(hint='lid')
    )

    lname_to_id_rvar = pgast.RangeVar(relation=lname_to_id)
    toplevel.ctes.append(lname_to_id)

    target_rvar = dbobj.range_for_ptrcls(
        ctx.env, ptrcls, '>', include_overlays=False)
    target_alias = target_rvar.alias.aliasname

    if target_is_atom:
        target_tab_name = (target_rvar.relation.schemaname,
                           target_rvar.relation.relname)
    else:
        target_tab_name = common.link_name_to_table_name(
            ptrcls.shortname, catenate=False)

    tab_cols = \
        ctx.backend._type_mech.get_cached_table_columns(target_tab_name)

    assert tab_cols, "could not get cols for {!r}".format(target_tab_name)

    dml_cte_rvar = pgast.RangeVar(
        relation=dml_cte,
        alias=pgast.Alias(
            aliasname=ctx.genalias('m')
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
        name=ctx.genalias(hint='d')
    )

    pathctx.put_path_rvar(ctx.env, delcte.query, path_id[:-1], target_rvar)

    # Record the effect of this removal in the relation overlay
    # context to ensure that the RETURNING clause potentially
    # referencing this link yields the expected results.
    overlays = ctx.env.rel_overlays[ptrcls]
    overlays.append(('except', delcte))
    toplevel.ctes.append(delcte)

    # Turn the IR of the expression on the right side of :=
    # into a subquery returning records for the link table.
    data_cte = process_link_values(
        ir_stmt, ir_expr, target_tab_name, tab_cols, col_data,
        dml_cte_rvar, [lname_to_id_rvar],
        props_only, target_is_atom, iterator_cte, ctx=ctx)

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
        name=ctx.genalias(hint='i'),
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

    pathctx.put_path_rvar(
        ctx.env, updcte.query, path_id[:-1], target_rvar)

    # Record the effect of this insertion in the relation overlay
    # context to ensure that the RETURNING clause potentially
    # referencing this link yields the expected results.
    overlays = ctx.env.rel_overlays[ptrcls]
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
        CTE representing the SQL UPDATE to the main relation of the Class.
    """
    toplevel = ctx.toplevel_stmt

    rptr = ir_expr.rptr
    ptrcls = rptr.ptrcls
    target_is_atom = isinstance(rptr.target, s_atoms.Atom)

    target_tab = dbobj.range_for_ptrcls(
        ctx.env, ptrcls, '>', include_overlays=False)

    if target_is_atom:
        target_tab_name = (target_tab.schema, target_tab.name)
    else:
        target_tab_name = common.link_name_to_table_name(
            ptrcls.shortname, catenate=False)

    tab_cols = \
        ctx.backend._type_mech.get_cached_table_columns(target_tab_name)

    assert tab_cols, "could not get cols for {!r}".format(target_tab_name)

    dml_cte_rvar = pgast.RangeVar(
        relation=dml_cte,
        alias=pgast.Alias(
            aliasname=ctx.genalias('m')
        )
    )

    cond = astutils.new_binop(
        pathctx.get_rvar_path_identity_var(
            dml_cte_rvar, ir_stmt.subject.path_id, env=ctx.env),
        dbobj.get_column(target_tab, 'std::source'),
        ast.ops.EQ
    )

    targets = []
    shape_set = irutils.get_subquery_shape(ir_expr)
    for prop_el in shape_set.shape:
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
        name=ctx.genalias(ptrcls.shortname.name)
    )

    toplevel.ctes.append(updcte)


def process_link_values(
        ir_stmt, ir_expr, target_tab, tab_cols, col_data,
        dml_rvar, sources, props_only, target_is_atom, iterator_cte, *,
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
        table such as std::source and std::__class__.
    :param sources:
        A list of relations which must be joined into the data query
        to resolve expressions in *col_data*.
    :param props_only:
        Whether this link update only touches link properties.
    :param target_is_atom:
        Whether the link target is an Atom.
    :param iterator_cte:
        CTE representing the iterator range in the FOR clause of the
        EdgeQL DML statement.
    """
    data = ir_expr.expr
    row_query = pgast.SelectStmt()

    relctx.include_rvar(row_query, dml_rvar, ctx=ctx)

    if iterator_cte is not None:
        iterator_rvar = relctx.include_range(row_query, iterator_cte, ctx=ctx)
        relctx.put_parent_range_scope(
            ir_stmt.iterator_stmt, iterator_rvar, force=True, ctx=ctx)

    with ctx.new() as input_rel_ctx:
        input_rel_ctx.expr_exposed = False
        input_rel_ctx.shape_format = context.ShapeFormat.FLAT
        input_rel_ctx.path_bonds.pop(ir_stmt.subject.path_id, None)
        input_rel = dispatch.compile(data, ctx=input_rel_ctx)

    input_stmt = input_rel

    input_rvar = pgast.RangeSubselect(
        subquery=input_rel,
        lateral=True,
        alias=pgast.Alias(
            aliasname=ctx.genalias('val')
        )
    )

    row = pgast.ImplicitRowExpr()

    source_data = {}

    if input_stmt.op is not None:
        # UNION
        input_stmt = input_stmt.rarg

    path_id = data.result.path_id

    if target_is_atom:
        target_ref = pathctx.get_rvar_path_value_var(
            input_rvar, path_id, env=ctx.env)
        source_data['std::target@atom'] = target_ref
    else:
        target_ref = pathctx.get_rvar_path_identity_var(
            input_rvar, path_id, env=ctx.env)
        source_data['std::target'] = target_ref

    output = pathctx.get_path_value_output(
        input_stmt, path_id, env=ctx.env)

    if isinstance(output, astutils.TupleVar):
        for element in output.elements:
            name = element.path_id.rptr_name()
            if name is None:
                name = element.path_id[-1].name
            colname = common.edgedb_name_to_pg_name(name)
            source_data.setdefault(
                colname, dbobj.get_column(input_rvar, element.name))

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
        name=ctx.genalias(hint='r')
    )

    return link_rows
