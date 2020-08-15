#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


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

from __future__ import annotations

import collections
from typing import *

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils

from edb.pgsql import ast as pgast
from edb.pgsql import types as pg_types

from . import astutils
from . import clauses
from . import context
from . import dispatch
from . import output
from . import pathctx
from . import relctx
from . import relgen
from . import shapecomp


class DMLParts(NamedTuple):

    dml_ctes: Mapping[
        irast.TypeRef,
        Tuple[pgast.CommonTableExpr, pgast.PathRangeVar],
    ]

    union_cte: pgast.CommonTableExpr

    range_cte: Optional[pgast.CommonTableExpr]


def init_dml_stmt(
    ir_stmt: irast.MutatingStmt,
    *,
    ctx: context.CompilerContextLevel,
    parent_ctx: context.CompilerContextLevel,
) -> DMLParts:
    """Prepare the common structure of the query representing a DML stmt.

    :param ir_stmt:
        IR of the DML statement.

    :return:
        A DMLParts tuple containing a map of DML CTEs as well as the
        common range CTE for UPDATE/DELETE statements.
    """
    clauses.init_stmt(ir_stmt, ctx, parent_ctx)

    range_cte: Optional[pgast.CommonTableExpr]
    range_rvar: Optional[pgast.RelRangeVar]

    if isinstance(ir_stmt, (irast.UpdateStmt, irast.DeleteStmt)):
        # UPDATE and DELETE operate over a range, so generate
        # the corresponding CTE and connect it to the DML stetements.
        range_cte = get_dml_range(ir_stmt, ctx=ctx)
        range_rvar = pgast.RelRangeVar(
            relation=range_cte,
            alias=pgast.Alias(
                aliasname=ctx.env.aliases.get(hint='range')
            )
        )
    else:
        range_cte = None
        range_rvar = None

    top_typeref = ir_stmt.subject.typeref
    if top_typeref.material_type:
        top_typeref = top_typeref.material_type

    typerefs = [top_typeref]

    if isinstance(ir_stmt, (irast.UpdateStmt, irast.DeleteStmt)):
        if top_typeref.union:
            for component in top_typeref.union:
                if component.material_type:
                    component = component.material_type

                typerefs.append(component)
                if component.descendants:
                    typerefs.extend(component.descendants)

        if top_typeref.descendants:
            typerefs.extend(top_typeref.descendants)

    dml_map = {}

    for typeref in typerefs:
        dml_cte, dml_rvar = gen_dml_cte(
            ir_stmt,
            range_rvar=range_rvar,
            typeref=typeref,
            ctx=ctx,
        )

        dml_map[typeref] = (dml_cte, dml_rvar)

    if len(dml_map) == 1:
        union_cte, union_rvar = next(iter(dml_map.values()))
    else:
        union_components = []
        for _, dml_rvar in dml_map.values():
            union_component = pgast.SelectStmt()
            relctx.include_rvar(
                union_component,
                dml_rvar,
                ir_stmt.subject.path_id,
                ctx=ctx,
            )
            union_components.append(union_component)

        qry = pgast.SelectStmt(
            all=True,
            larg=union_components[0],
        )

        for union_component in union_components[1:]:
            qry.op = 'UNION'
            qry.rarg = union_component
            qry = pgast.SelectStmt(
                all=True,
                larg=qry,
            )

        union_cte = pgast.CommonTableExpr(
            query=qry.larg,
            name=ctx.env.aliases.get(hint='ma')
        )

        union_rvar = relctx.rvar_for_rel(
            union_cte,
            typeref=ir_stmt.subject.typeref,
            ctx=ctx,
        )

    relctx.include_rvar(ctx.rel, union_rvar, ir_stmt.subject.path_id, ctx=ctx)
    pathctx.put_path_bond(ctx.rel, ir_stmt.subject.path_id)

    ctx.dml_stmts[ir_stmt] = union_cte

    return DMLParts(dml_ctes=dml_map, range_cte=range_cte, union_cte=union_cte)


def gen_dml_cte(
    ir_stmt: irast.MutatingStmt,
    *,
    range_rvar: Optional[pgast.RelRangeVar],
    typeref: irast.TypeRef,
    ctx: context.CompilerContextLevel,
) -> Tuple[pgast.CommonTableExpr, pgast.PathRangeVar]:

    target_ir_set = ir_stmt.subject
    target_path_id = target_ir_set.path_id

    dml_stmt: pgast.Query
    if isinstance(ir_stmt, irast.InsertStmt):
        dml_stmt = pgast.InsertStmt()
    elif isinstance(ir_stmt, irast.UpdateStmt):
        dml_stmt = pgast.UpdateStmt()
    elif isinstance(ir_stmt, irast.DeleteStmt):
        dml_stmt = pgast.DeleteStmt()
    else:
        raise AssertionError(f'unexpected DML IR: {ir_stmt!r}')

    dml_stmt.relation = relctx.range_for_typeref(
        typeref,
        target_path_id,
        for_mutation=True,
        common_parent=True,
        ctx=ctx,
    )
    pathctx.put_path_value_rvar(
        dml_stmt, target_path_id, dml_stmt.relation, env=ctx.env)
    pathctx.put_path_source_rvar(
        dml_stmt, target_path_id, dml_stmt.relation, env=ctx.env)
    pathctx.put_path_bond(
        dml_stmt, target_path_id)

    dml_cte = pgast.CommonTableExpr(
        query=dml_stmt,
        name=ctx.env.aliases.get(hint='m')
    )

    if range_rvar is not None:
        relctx.pull_path_namespace(
            target=dml_stmt, source=range_rvar, ctx=ctx)

        # Auxiliary relations are always joined via the WHERE
        # clause due to the structure of the UPDATE/DELETE SQL statements.
        dml_stmt.where_clause = astutils.new_binop(
            lexpr=pgast.ColumnRef(name=[
                dml_stmt.relation.alias.aliasname,
                'id'
            ]),
            op='=',
            rexpr=pathctx.get_rvar_path_identity_var(
                range_rvar, target_ir_set.path_id, env=ctx.env)
        )

        # UPDATE has "FROM", while DELETE has "USING".
        if isinstance(dml_stmt, pgast.UpdateStmt):
            dml_stmt.from_clause.append(range_rvar)
        elif isinstance(dml_stmt, pgast.DeleteStmt):
            dml_stmt.using_clause.append(range_rvar)

    # Due to the fact that DML statements are structured
    # as a flat list of CTEs instead of nested range vars,
    # the top level path scope must be empty.  The necessary
    # range vars will be injected explicitly in all rels that
    # need them.
    ctx.path_scope.clear()

    pathctx.put_path_value_rvar(
        dml_stmt, target_path_id, dml_stmt.relation, env=ctx.env)

    pathctx.put_path_source_rvar(
        dml_stmt, target_path_id, dml_stmt.relation, env=ctx.env)

    dml_rvar = relctx.rvar_for_rel(dml_cte, typeref=typeref, ctx=ctx)

    return dml_cte, dml_rvar


def wrap_dml_cte(
    ir_stmt: irast.MutatingStmt,
    dml_cte: pgast.CommonTableExpr,
    *,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:

    wrapper = ctx.rel
    dml_rvar = relctx.rvar_for_rel(
        dml_cte,
        typeref=ir_stmt.subject.typeref,
        ctx=ctx,
    )
    relctx.include_rvar(wrapper, dml_rvar, ir_stmt.subject.path_id, ctx=ctx)
    pathctx.put_path_bond(wrapper, ir_stmt.subject.path_id)

    return dml_rvar


def fini_dml_stmt(
    ir_stmt: irast.MutatingStmt,
    wrapper: pgast.Query,
    parts: DMLParts,
    *,
    parent_ctx: context.CompilerContextLevel,
    ctx: context.CompilerContextLevel,
) -> pgast.Query:

    # Record the effect of this insertion in the relation overlay
    # context to ensure that the RETURNING clause potentially
    # referencing this class yields the expected results.
    dml_stack = get_dml_stmt_stack(ir_stmt, ctx=ctx)
    if isinstance(ir_stmt, irast.InsertStmt):
        relctx.add_type_rel_overlay(
            ir_stmt.subject.typeref, 'union', parts.union_cte,
            dml_stmts=dml_stack, path_id=ir_stmt.subject.path_id, ctx=ctx)
    elif isinstance(ir_stmt, irast.DeleteStmt):
        relctx.add_type_rel_overlay(
            ir_stmt.subject.typeref, 'except', parts.union_cte,
            dml_stmts=dml_stack, path_id=ir_stmt.subject.path_id, ctx=ctx)

    clauses.compile_output(ir_stmt.result, ctx=ctx)
    clauses.fini_stmt(wrapper, ctx, parent_ctx)

    return wrapper


def get_dml_stmt_stack(
        ir_stmt: irast.MutatingStmt, *,
        ctx: context.CompilerContextLevel) -> List[irast.MutatingStmt]:
    stack = []
    stmt: Optional[irast.Stmt] = ir_stmt
    while stmt is not None:
        if isinstance(stmt, irast.MutatingStmt):
            stack.append(stmt)
        stmt = stmt.parent_stmt

    return stack


def get_dml_range(
    ir_stmt: Union[irast.UpdateStmt, irast.DeleteStmt],
    *,
    ctx: context.CompilerContextLevel,
) -> pgast.CommonTableExpr:
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
    ir_qual_card = ir_stmt.where_card

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
                                path_id=iterator_set.path_id, ctx=subctx)

        dispatch.visit(target_ir_set, ctx=subctx)

        pathctx.get_path_identity_output(
            range_stmt, target_ir_set.path_id, env=subctx.env)

        if ir_qual_expr is not None:
            range_stmt.where_clause = astutils.extend_binop(
                range_stmt.where_clause,
                clauses.compile_filter_clause(
                    ir_qual_expr, ir_qual_card, ctx=subctx))

        range_cte = pgast.CommonTableExpr(
            query=range_stmt,
            name=ctx.env.aliases.get('range')
        )

        return range_cte


def process_insert_body(
        ir_stmt: irast.MutatingStmt,
        wrapper: pgast.SelectStmt,
        insert_cte: pgast.CommonTableExpr,
        insert_rvar: pgast.PathRangeVar, *,
        ctx: context.CompilerContextLevel) -> None:
    """Generate SQL DML CTEs from an InsertStmt IR.

    :param ir_stmt:
        IR of the statement.
    :param wrapper:
        Top-level SQL query.
    :param insert_cte:
        CTE representing the SQL INSERT to the main relation of the Object.
    """
    cols = [pgast.ColumnRef(name=['__type__'])]
    select = pgast.SelectStmt(target_list=[])
    values = select.target_list

    # The main INSERT query of this statement will always be
    # present to insert at least the `id` and `__type__`
    # properties.
    insert_stmt = insert_cte.query
    assert isinstance(insert_stmt, pgast.InsertStmt)

    insert_stmt.cols = cols
    insert_stmt.select_stmt = select

    if ir_stmt.parent_stmt is not None:
        iterator_set = ir_stmt.parent_stmt.iterator_stmt
    else:
        iterator_set = None

    pseudo_iterator_set: Optional[irast.Set]
    pseudo_iterator_cte: Optional[pgast.CommonTableExpr]
    if ctx.enclosing_dml:
        dml_expr, pseudo_iterator_cte = ctx.enclosing_dml
        pseudo_iterator_set = dml_expr.subject
    else:
        pseudo_iterator_cte = None
        pseudo_iterator_set = None

    iterator_cte: Optional[pgast.CommonTableExpr]
    iterator_id: Optional[pgast.BaseExpr]

    if iterator_set is not None:
        with ctx.substmt() as ictx:
            # This has been lifted up to the top level, so we can't
            # have it inheriting anything from where it would have
            # been nested.
            # TODO: Inherit from the nearest iterator instead?
            del ictx.rel_hierarchy[ictx.rel]
            ictx.path_scope = ictx.path_scope.new_child()
            ictx.path_scope[iterator_set.path_id] = ictx.rel

            # If there is an enclosing insert *and* an explicit iterator,
            # we need to correlate the enclosing insert with the iterator.
            if pseudo_iterator_set is not None:
                assert pseudo_iterator_cte
                pseudo_iterator_rvar = relctx.rvar_for_rel(
                    pseudo_iterator_cte, ctx=ictx)

                pathctx.put_path_bond(ictx.rel, pseudo_iterator_set.path_id)
                relctx.include_rvar(
                    ictx.rel, pseudo_iterator_rvar,
                    path_id=pseudo_iterator_set.path_id,
                    overwrite_path_rvar=True,
                    ctx=ictx)

                ictx.volatility_ref = pathctx.get_path_identity_var(
                    ictx.rel,
                    pseudo_iterator_set.path_id,
                    env=ictx.env)

            clauses.compile_iterator_expr(ictx.rel, iterator_set, ctx=ictx)
            ictx.rel.path_id = iterator_set.path_id
            pathctx.put_path_bond(ictx.rel, iterator_set.path_id)
            iterator_cte = pgast.CommonTableExpr(
                query=ictx.rel,
                name=ctx.env.aliases.get('iter')
            )
            ictx.toplevel_stmt.ctes.append(iterator_cte)
        iterator_rvar = relctx.rvar_for_rel(iterator_cte, ctx=ctx)
        relctx.include_rvar(select, iterator_rvar,
                            path_id=ictx.rel.path_id, ctx=ctx)
        iterator_id = pathctx.get_path_identity_var(
            select, iterator_set.path_id, env=ctx.env)

    elif pseudo_iterator_set is not None:
        assert pseudo_iterator_cte is not None
        iterator_rvar = relctx.rvar_for_rel(pseudo_iterator_cte, ctx=ctx)
        relctx.include_rvar(select, iterator_rvar,
                            path_id=pseudo_iterator_set.path_id, ctx=ctx)

        iterator_set = pseudo_iterator_set
        iterator_cte = pseudo_iterator_cte
        iterator_id = relctx.get_path_var(
            select, pseudo_iterator_set.path_id,
            aspect='identity', ctx=ctx)

    else:
        iterator_cte = None
        iterator_id = None

    typeref = ir_stmt.subject.typeref
    if typeref.material_type is not None:
        typeref = typeref.material_type

    values.append(
        pgast.ResTarget(
            val=pgast.TypeCast(
                arg=pgast.StringConstant(val=str(typeref.id)),
                type_name=pgast.TypeName(name=('uuid',))
            ),
        )
    )

    external_inserts = []

    with ctx.newrel() as subctx:
        subctx.rel = select
        subctx.rel_hierarchy[select] = insert_stmt

        subctx.expr_exposed = False

        if iterator_cte is not None:
            subctx.path_scope = ctx.path_scope.new_child()
            subctx.path_scope[iterator_cte.query.path_id] = select

        # Process the Insert IR and separate links that go
        # into the main table from links that are inserted into
        # a separate link table.
        for shape_el, shape_op in ir_stmt.subject.shape:
            assert shape_op is qlast.ShapeOp.ASSIGN

            rptr = shape_el.rptr
            ptrref = rptr.ptrref
            if ptrref.material_ptr is not None:
                ptrref = ptrref.material_ptr

            if (ptrref.source_ptr is not None and
                    rptr.source.path_id != ir_stmt.subject.path_id):
                continue

            ptr_info = pg_types.get_ptrref_storage_info(
                ptrref, resolve_type=True, link_bias=False)

            props_only = False

            # First, process all local link inserts.
            if ptr_info.table_type == 'ObjectType':
                props_only = True
                field = pgast.ColumnRef(name=[ptr_info.column_name])
                cols.append(field)

                rel = compile_insert_shape_element(
                    insert_stmt, wrapper, ir_stmt, shape_el, iterator_id,
                    ctx=ctx)

                insvalue = pathctx.get_path_value_var(
                    rel, shape_el.path_id, env=ctx.env)

                if irtyputils.is_tuple(shape_el.typeref):
                    # Tuples require an explicit cast.
                    insvalue = pgast.TypeCast(
                        arg=output.output_as_value(insvalue, env=ctx.env),
                        type_name=pgast.TypeName(
                            name=ptr_info.column_type,
                        ),
                    )

                values.append(pgast.ResTarget(val=insvalue))

            ptr_info = pg_types.get_ptrref_storage_info(
                ptrref, resolve_type=False, link_bias=True)

            if ptr_info and ptr_info.table_type == 'link':
                external_inserts.append((shape_el, props_only))

        if iterator_set is not None:
            cols.append(pgast.ColumnRef(name=['__edb_token']))

            values.append(pgast.ResTarget(val=iterator_id))

            pathctx.put_path_identity_var(
                insert_stmt, iterator_set.path_id,
                cols[-1], force=True, env=subctx.env
            )

            pathctx.put_path_bond(insert_stmt, iterator_set.path_id)
            pathctx.put_path_rvar(
                wrapper,
                path_id=iterator_set.path_id,
                rvar=insert_rvar,
                aspect='identity',
                env=subctx.env,
            )

    if isinstance(ir_stmt, irast.InsertStmt) and ir_stmt.on_conflict:
        assert not insert_stmt.on_conflict

        infer = None
        if isinstance(ir_stmt.on_conflict, irast.ConstraintRef):
            constraint_name = f'"{ir_stmt.on_conflict.id};schemaconstr"'
            infer = pgast.InferClause(conname=constraint_name)

        insert_stmt.on_conflict = pgast.OnConflictClause(
            action='nothing',
            infer=infer,
        )

    toplevel = ctx.toplevel_stmt
    toplevel.ctes.append(insert_cte)

    iterator = None
    if iterator_set:
        assert iterator_cte
        iterator = iterator_set, iterator_cte

    # Process necessary updates to the link tables.
    for shape_el, props_only in external_inserts:
        process_link_update(
            ir_stmt=ir_stmt,
            ir_set=shape_el,
            props_only=props_only,
            wrapper=wrapper,
            dml_cte=insert_cte,
            source_typeref=typeref,
            iterator=iterator,
            is_insert=True,
            ctx=ctx,
        )


def compile_insert_shape_element(
        insert_stmt: pgast.InsertStmt,
        wrapper: pgast.Query,
        ir_stmt: irast.MutatingStmt,
        shape_el: irast.Set,
        iterator_id: Optional[pgast.BaseExpr], *,
        ctx: context.CompilerContextLevel) -> pgast.Query:

    with ctx.newscope() as insvalctx:
        # This method is only called if the upper cardinality of
        # the expression is one, so we check for AT_MOST_ONE
        # to determine nullability.
        if (shape_el.rptr.ptrref.dir_cardinality
                is qltypes.Cardinality.AT_MOST_ONE):
            insvalctx.force_optional.add(shape_el.path_id)

        if iterator_id is not None:
            insvalctx.volatility_ref = iterator_id
        else:
            # Single inserts have no need for forced
            # computable volatility, and, furthermore,
            # we do not have a valid identity reference
            # anyway.
            insvalctx.volatility_ref = context.NO_VOLATILITY

        dispatch.visit(shape_el, ctx=insvalctx)

    return insvalctx.rel


def process_update_body(
    ir_stmt: irast.MutatingStmt,
    wrapper: pgast.Query,
    update_cte: pgast.CommonTableExpr,
    typeref: irast.TypeRef,
    *,
    ctx: context.CompilerContextLevel,
) -> None:
    """Generate SQL DML CTEs from an UpdateStmt IR.

    :param ir_stmt:
        IR of the statement.
    :param wrapper:
        Top-level SQL query.
    :param update_cte:
        CTE representing the SQL UPDATE to the main relation of the Object.
    :param typeref:
        The specific TypeRef of a set being updated.
    """
    update_stmt = update_cte.query
    assert isinstance(update_stmt, pgast.UpdateStmt)

    external_updates = []

    with ctx.newscope() as subctx:
        # It is necessary to process the expressions in
        # the UpdateStmt shape body in the context of the
        # UPDATE statement so that references to the current
        # values of the updated object are resolved correctly.
        subctx.parent_rel = update_stmt
        subctx.expr_exposed = False

        for shape_el, shape_op in ir_stmt.subject.shape:
            ptrref = shape_el.rptr.ptrref
            updvalue = shape_el.expr
            ptr_info = pg_types.get_ptrref_storage_info(
                ptrref, resolve_type=True, link_bias=False)

            if ptr_info.table_type == 'ObjectType' and updvalue is not None:
                with subctx.newscope() as scopectx:
                    val: pgast.BaseExpr

                    if irtyputils.is_tuple(shape_el.typeref):
                        # When target is a tuple type, make sure
                        # the expression is compiled into a subquery
                        # returning a single column that is explicitly
                        # cast into the appropriate composite type.
                        val = relgen.set_as_subquery(
                            shape_el,
                            as_value=True,
                            explicit_cast=ptr_info.column_type,
                            ctx=scopectx,
                        )
                    else:
                        if (
                            isinstance(updvalue, irast.MutatingStmt)
                            and updvalue in ctx.dml_stmts
                        ):
                            with scopectx.substmt() as relctx:
                                dml_cte = ctx.dml_stmts[updvalue]
                                wrap_dml_cte(updvalue, dml_cte, ctx=relctx)
                                pathctx.get_path_identity_output(
                                    relctx.rel,
                                    updvalue.subject.path_id,
                                    env=relctx.env,
                                )
                                val = relctx.rel
                        else:
                            val = dispatch.compile(updvalue, ctx=scopectx)

                        val = pgast.TypeCast(
                            arg=val,
                            type_name=pgast.TypeName(name=ptr_info.column_type)
                        )

                    if shape_op is qlast.ShapeOp.SUBTRACT:
                        val = pgast.FuncCall(
                            name=('nullif',),
                            args=[
                                pgast.ColumnRef(name=(ptr_info.column_name,)),
                                val,
                            ],
                        )

                    updtarget = pgast.UpdateTarget(
                        name=ptr_info.column_name,
                        val=val,
                    )

                    update_stmt.targets.append(updtarget)

            props_only = is_props_only_update(shape_el, ctx=subctx)

            ptr_info = pg_types.get_ptrref_storage_info(
                ptrref, resolve_type=False, link_bias=True)

            if ptr_info and ptr_info.table_type == 'link':
                external_updates.append((shape_el, shape_op, props_only))

    if not update_stmt.targets:
        # No updates directly to the set target table,
        # so convert the UPDATE statement into a SELECT.
        from_clause: List[pgast.BaseRangeVar] = [update_stmt.relation]
        from_clause.extend(update_stmt.from_clause)
        update_cte.query = pgast.SelectStmt(
            ctes=update_stmt.ctes,
            target_list=update_stmt.returning_list,
            from_clause=from_clause,
            where_clause=update_stmt.where_clause,
            path_namespace=update_stmt.path_namespace,
            path_outputs=update_stmt.path_outputs,
            path_scope=update_stmt.path_scope,
            path_rvar_map=update_stmt.path_rvar_map.copy(),
            view_path_id_map=update_stmt.view_path_id_map.copy(),
            ptr_join_map=update_stmt.ptr_join_map.copy(),
        )

    toplevel = ctx.toplevel_stmt
    toplevel.ctes.append(update_cte)

    # Process necessary updates to the link tables.
    for expr, shape_op, _ in external_updates:
        process_link_update(
            ir_stmt=ir_stmt,
            ir_set=expr,
            props_only=False,
            wrapper=wrapper,
            dml_cte=update_cte,
            iterator=None,
            is_insert=False,
            shape_op=shape_op,
            source_typeref=typeref,
            ctx=ctx,
        )


def is_props_only_update(shape_el: irast.Set, *,
                         ctx: context.CompilerContextLevel) -> bool:
    """Determine whether a link update is a property-only update.

    :param shape_el:
        IR of the shape element representing a link update.

    :return:
        `True` if *shape_el* represents a link property-only update.
    """
    return (
        bool(shape_el.shape) and
        all(el.rptr.ptrref.source_ptr is not None for el, _ in shape_el.shape)
    )


def process_link_update(
    *,
    ir_stmt: irast.MutatingStmt,
    ir_set: irast.Set,
    props_only: bool,
    is_insert: bool,
    shape_op: qlast.ShapeOp = qlast.ShapeOp.ASSIGN,
    source_typeref: irast.TypeRef,
    wrapper: pgast.Query,
    dml_cte: pgast.CommonTableExpr,
    iterator: Optional[Tuple[irast.Set, pgast.CommonTableExpr]],
    ctx: context.CompilerContextLevel,
) -> pgast.CommonTableExpr:
    """Perform updates to a link relation as part of a DML statement.

    :param ir_stmt:
        IR of the statement.
    :param ir_set:
        IR of the INSERT/UPDATE body element.
    :param props_only:
        Whether this link update only touches link properties.
    :param wrapper:
        Top-level SQL query.
    :param dml_cte:
        CTE representing the SQL INSERT or UPDATE to the main
        relation of the Object.
    :param iterator:
        IR and CTE representing the iterator range in the FOR clause of the
        EdgeQL DML statement.
    """
    toplevel = ctx.toplevel_stmt

    rptr = ir_set.rptr
    ptrref = rptr.ptrref
    assert isinstance(ptrref, irast.PointerRef)
    target_is_scalar = irtyputils.is_scalar(ir_set.typeref)
    path_id = ir_set.path_id

    # The links in the dml class shape have been derived,
    # but we must use the correct specialized link class for the
    # base material type.
    if ptrref.material_ptr is not None:
        mptrref = ptrref.material_ptr
    else:
        mptrref = ptrref

    if mptrref.out_source.id != source_typeref.id:
        for descendant in mptrref.descendants:
            if descendant.out_source.id == source_typeref.id:
                mptrref = descendant
                break
        else:
            raise errors.InternalServerError(
                'missing PointerRef descriptor for source typeref')

    assert isinstance(mptrref, irast.PointerRef)

    target_rvar = relctx.range_for_ptrref(
        mptrref, for_mutation=True, only_self=True, ctx=ctx)
    assert isinstance(target_rvar, pgast.RelRangeVar)
    assert isinstance(target_rvar.relation, pgast.Relation)
    target_alias = target_rvar.alias.aliasname

    target_tab_name = (target_rvar.relation.schemaname,
                       target_rvar.relation.name)

    dml_cte_rvar = pgast.RelRangeVar(
        relation=dml_cte,
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get('m')
        )
    )

    col_data = {
        'ptr_item_id': pgast.TypeCast(
            arg=pgast.StringConstant(val=str(mptrref.id)),
            type_name=pgast.TypeName(name=('uuid',))
        ),
        'source': pathctx.get_rvar_path_identity_var(
            dml_cte_rvar, ir_stmt.subject.path_id, env=ctx.env)
    }

    # Turn the IR of the expression on the right side of :=
    # into a subquery returning records for the link table.
    data_cte, specified_cols = process_link_values(
        ir_stmt=ir_stmt,
        ir_expr=ir_set,
        target_tab=target_tab_name,
        col_data=col_data,
        dml_rvar=dml_cte_rvar,
        sources=[],
        props_only=props_only,
        target_is_scalar=target_is_scalar,
        dml_cte=dml_cte,
        iterator=iterator,
        ctx=ctx,
    )

    toplevel.ctes.append(data_cte)

    delqry: Optional[pgast.DeleteStmt]

    data_select = pgast.SelectStmt(
        target_list=[
            pgast.ResTarget(
                val=pgast.ColumnRef(
                    name=[data_cte.name, pgast.Star()]
                ),
            ),
        ],
        from_clause=[
            pgast.RelRangeVar(relation=data_cte),
        ],
    )

    if not is_insert and shape_op is not qlast.ShapeOp.APPEND:
        if shape_op is qlast.ShapeOp.SUBTRACT:
            data_rvar = relctx.rvar_for_rel(data_select, ctx=ctx)

            # Drop requested link records.
            delqry = pgast.DeleteStmt(
                relation=target_rvar,
                where_clause=astutils.new_binop(
                    lexpr=astutils.new_binop(
                        lexpr=col_data['source'],
                        op='=',
                        rexpr=pgast.ColumnRef(
                            name=[target_alias, 'source'],
                        ),
                    ),
                    op='AND',
                    rexpr=astutils.new_binop(
                        lexpr=pgast.ColumnRef(
                            name=[target_alias, 'target'],
                        ),
                        op='=',
                        rexpr=pgast.ColumnRef(
                            name=[data_rvar.alias.aliasname, 'target'],
                        ),
                    ),
                ),
                using_clause=[
                    dml_cte_rvar,
                    data_rvar,
                ],
                returning_list=[
                    pgast.ResTarget(
                        val=pgast.ColumnRef(
                            name=[target_alias, pgast.Star()],
                        ),
                    )
                ]
            )
        else:
            # Drop all previous link records for this source.
            delqry = pgast.DeleteStmt(
                relation=target_rvar,
                where_clause=astutils.new_binop(
                    lexpr=col_data['source'],
                    op='=',
                    rexpr=pgast.ColumnRef(
                        name=[target_alias, 'source'],
                    ),
                ),
                using_clause=[dml_cte_rvar],
                returning_list=[
                    pgast.ResTarget(
                        val=pgast.ColumnRef(
                            name=[target_alias, pgast.Star()],
                        ),
                    )
                ]
            )

        delcte = pgast.CommonTableExpr(
            name=ctx.env.aliases.get(hint='d'),
            query=delqry,
        )

        pathctx.put_path_value_rvar(
            delcte.query, path_id.ptr_path(), target_rvar, env=ctx.env)

        # Record the effect of this removal in the relation overlay
        # context to ensure that references to the link in the result
        # of this DML statement yield the expected results.
        dml_stack = get_dml_stmt_stack(ir_stmt, ctx=ctx)
        relctx.add_ptr_rel_overlay(
            ptrref, 'except', delcte, dml_stmts=dml_stack, ctx=ctx)
        toplevel.ctes.append(delcte)
    else:
        delqry = None

    if shape_op is qlast.ShapeOp.SUBTRACT:
        return data_cte

    cols = [pgast.ColumnRef(name=[col]) for col in specified_cols]
    conflict_cols = ['source', 'target', 'ptr_item_id']

    if is_insert:
        conflict_clause = None
    elif len(cols) == len(conflict_cols) and delqry is not None:
        # There are no link properties, so we can optimize the
        # link replacement operation by omitting the overlapping
        # link rows from deletion.
        filter_select = pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=pgast.ColumnRef(name=['source']),
                ),
                pgast.ResTarget(
                    val=pgast.ColumnRef(name=['target']),
                ),
            ],
            from_clause=[pgast.RelRangeVar(relation=data_cte)],
        )

        delqry.where_clause = astutils.extend_binop(
            delqry.where_clause,
            astutils.new_binop(
                lexpr=pgast.ImplicitRowExpr(
                    args=[
                        pgast.ColumnRef(name=['source']),
                        pgast.ColumnRef(name=['target']),
                    ],
                ),
                rexpr=pgast.SubLink(
                    type=pgast.SubLinkType.ALL,
                    expr=filter_select,
                ),
                op='!=',
            )
        )

        conflict_clause = pgast.OnConflictClause(
            action='nothing',
            infer=pgast.InferClause(
                index_elems=[
                    pgast.ColumnRef(name=[col]) for col in conflict_cols
                ]
            ),
        )
    else:
        # Inserting rows into the link table may produce cardinality
        # constraint violations, since the INSERT into the link table
        # is executed in the snapshot where the above DELETE from
        # the link table is not visible.  Hence, we need to use
        # the ON CONFLICT clause to resolve this.
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
                pgast.RelRangeVar(relation=data_cte)
            ],
            where_clause=astutils.new_binop(
                lexpr=pgast.ImplicitRowExpr(args=conflict_inference),
                rexpr=pgast.ImplicitRowExpr(args=conflict_exc_row),
                op='='
            )
        )

        conflict_clause = pgast.OnConflictClause(
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
        )

    updcte = pgast.CommonTableExpr(
        name=ctx.env.aliases.get(hint='i'),
        query=pgast.InsertStmt(
            relation=target_rvar,
            select_stmt=data_select,
            cols=cols,
            on_conflict=conflict_clause,
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
    # context to ensure that references to the link in the result
    # of this DML statement yield the expected results.
    dml_stack = get_dml_stmt_stack(ir_stmt, ctx=ctx)
    relctx.add_ptr_rel_overlay(
        ptrref, 'union', updcte, dml_stmts=dml_stack, ctx=ctx)
    toplevel.ctes.append(updcte)

    return data_cte


def process_linkprop_update(
        ir_stmt: irast.MutatingStmt, ir_expr: irast.Set,
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
    ptrref = rptr.ptrref

    if ptrref.material_ptr:
        ptrref = ptrref.material_ptr

    target_tab = relctx.range_for_ptrref(
        ptrref, for_mutation=True, ctx=ctx)

    dml_cte_rvar = pgast.RelRangeVar(
        relation=dml_cte,
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get('m')
        )
    )

    cond = astutils.new_binop(
        pathctx.get_rvar_path_identity_var(
            dml_cte_rvar, ir_stmt.subject.path_id, env=ctx.env),
        astutils.get_column(target_tab, 'source', nullable=False),
        op='=',
    )

    targets = []
    for prop_el, shape_op in ir_expr.shape:
        assert shape_op is qlast.ShapeOp.ASSIGN
        ptrname = prop_el.rptr.ptrref.shortname
        with ctx.new() as input_rel_ctx:
            input_rel_ctx.expr_exposed = False
            input_rel = dispatch.compile(prop_el.expr, ctx=input_rel_ctx)
            targets.append(
                pgast.UpdateTarget(
                    name=ptrname.name,
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
        name=ctx.env.aliases.get(ptrref.shortname.name)
    )

    toplevel.ctes.append(updcte)


def process_link_values(
    *,
    ir_stmt: irast.MutatingStmt,
    ir_expr: irast.Set,
    target_tab: Tuple[str, ...],
    col_data: Mapping[str, pgast.BaseExpr],
    dml_rvar: pgast.PathRangeVar,
    sources: Iterable[pgast.BaseRangeVar],
    props_only: bool,
    target_is_scalar: bool,
    dml_cte: pgast.CommonTableExpr,
    iterator: Optional[Tuple[irast.Set, pgast.CommonTableExpr]],
    ctx: context.CompilerContextLevel,
) -> Tuple[pgast.CommonTableExpr, List[str]]:
    """Unpack data from an update expression into a series of selects.

    :param ir_expr:
        IR of the INSERT/UPDATE body element.
    :param target_tab:
        The link table being updated.
    :param col_data:
        Expressions used to populate well-known columns of the link
        table such as `source` and `__type__`.
    :param sources:
        A list of relations which must be joined into the data query
        to resolve expressions in *col_data*.
    :param props_only:
        Whether this link update only touches link properties.
    :param target_is_scalar:
        Whether the link target is an ScalarType.
    :param iterator:
        IR and CTE representing the iterator range in the FOR clause of the
        EdgeQL DML statement.
    """
    old_cte_count = len(ctx.toplevel_stmt.ctes)
    with ctx.newscope() as newscope, newscope.newrel() as subrelctx:
        subrelctx.enclosing_dml = (ir_stmt, dml_cte)
        row_query = subrelctx.rel

        relctx.include_rvar(row_query, dml_rvar,
                            path_id=ir_stmt.subject.path_id, ctx=subrelctx)
        subrelctx.path_scope[ir_stmt.subject.path_id] = row_query

        if iterator is not None:
            iterator_set, iterator_cte = iterator
            iterator_rvar = relctx.rvar_for_rel(
                iterator_cte, lateral=True, ctx=subrelctx)
            relctx.include_rvar(row_query, iterator_rvar,
                                path_id=iterator_set.path_id,
                                ctx=subrelctx)

        with subrelctx.newscope() as sctx, sctx.subrel() as input_rel_ctx:
            input_rel = input_rel_ctx.rel
            if iterator is not None:
                input_rel_ctx.path_scope[iterator[0].path_id] = \
                    row_query
            input_rel_ctx.expr_exposed = False
            input_rel_ctx.volatility_ref = pathctx.get_path_identity_var(
                row_query, ir_stmt.subject.path_id, env=input_rel_ctx.env)
            dispatch.visit(ir_expr, ctx=input_rel_ctx)

            if (
                isinstance(ir_expr.expr, irast.Stmt)
                and ir_expr.expr.iterator_stmt is not None
            ):
                # The link value is computaed by a FOR expression,
                # check if the statement is a DML statement, and if so,
                # pull the iterator scope so that link property expressions
                # have the correct context.
                inner_iterator_cte = None
                inner_iterator_path_id = ir_expr.expr.iterator_stmt.path_id
                for cte in input_rel_ctx.toplevel_stmt.ctes:
                    if cte.query.path_id == inner_iterator_path_id:
                        inner_iterator_cte = cte
                        break
                if inner_iterator_cte is not None:
                    inner_iterator_rvar = relctx.rvar_for_rel(
                        inner_iterator_cte, lateral=True, ctx=subrelctx)

                    relctx.include_rvar(
                        input_rel,
                        inner_iterator_rvar,
                        path_id=inner_iterator_path_id,
                        ctx=subrelctx,
                    )

                    input_rel_ctx.path_scope[inner_iterator_path_id] = (
                        input_rel
                    )

            shape_tuple = None
            if ir_expr.shape:
                shape_tuple = shapecomp.compile_shape(
                    ir_expr,
                    [expr for expr, _ in ir_expr.shape],
                    ctx=input_rel_ctx,
                )

                for element in shape_tuple.elements:
                    pathctx.put_path_var_if_not_exists(
                        input_rel_ctx.rel, element.path_id, element.val,
                        aspect='value', env=input_rel_ctx.env)

    input_stmt: pgast.Query = input_rel

    input_rvar = pgast.RangeSubselect(
        subquery=input_rel,
        lateral=True,
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get('val')
        )
    )

    if len(ctx.toplevel_stmt.ctes) > old_cte_count:
        # If there were any nested inserts, we need to join them in.
        pathctx.put_rvar_path_bond(input_rvar, ir_stmt.subject.path_id)
    relctx.include_rvar(row_query, input_rvar,
                        path_id=ir_stmt.subject.path_id,
                        ctx=ctx)

    source_data: Dict[str, pgast.BaseExpr] = {}

    if isinstance(input_stmt, pgast.SelectStmt) and input_stmt.op is not None:
        # UNION
        input_stmt = input_stmt.rarg

    path_id = ir_expr.path_id

    if shape_tuple is not None:
        for element in shape_tuple.elements:
            if not element.path_id.is_linkprop_path():
                continue
            rptr_name = element.path_id.rptr_name()
            assert rptr_name is not None
            colname = rptr_name.name
            val = pathctx.get_rvar_path_value_var(
                input_rvar, element.path_id, env=ctx.env)
            source_data.setdefault(colname, val)
    else:
        if target_is_scalar:
            target_ref = pathctx.get_rvar_path_value_var(
                input_rvar, path_id, env=ctx.env)
        else:
            target_ref = pathctx.get_rvar_path_identity_var(
                input_rvar, path_id, env=ctx.env)

        source_data['target'] = target_ref

    if not target_is_scalar and 'target' not in source_data:
        target_ref = pathctx.get_rvar_path_identity_var(
            input_rvar, path_id, env=ctx.env)
        source_data['target'] = target_ref

    specified_cols = []
    for col, expr in collections.ChainMap(col_data, source_data).items():
        row_query.target_list.append(pgast.ResTarget(
            val=expr,
            name=col
        ))
        specified_cols.append(col)

    row_query.from_clause += list(sources)

    link_rows = pgast.CommonTableExpr(
        query=row_query,
        name=ctx.env.aliases.get(hint='r')
    )

    return link_rows, specified_cols
