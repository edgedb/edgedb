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

from typing import *

from edb.common import uuidgen
from edb.common.typeutils import downcast, not_none

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb.schema import name as sn

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from edb.pgsql import ast as pgast
from edb.pgsql import types as pg_types
from edb.pgsql import common

from . import astutils
from . import clauses
from . import context
from . import dispatch
from . import output
from . import pathctx
from . import relctx
from . import relgen


class DMLParts(NamedTuple):

    dml_ctes: Mapping[
        irast.TypeRef,
        Tuple[pgast.CommonTableExpr, pgast.PathRangeVar],
    ]

    else_cte: Optional[Tuple[pgast.CommonTableExpr, pgast.PathRangeVar]]

    range_cte: Optional[pgast.CommonTableExpr]


def init_dml_stmt(
    ir_stmt: irast.MutatingStmt,
    *,
    ctx: context.CompilerContextLevel,
    parent_ctx: context.CompilerContextLevel,
) -> DMLParts:
    """Prepare the common structure of the query representing a DML stmt.

    Args:
        ir_stmt:
            IR of the DML statement.

    Returns:
        A ``DMLParts`` tuple containing a map of DML CTEs as well as the
        common range CTE for UPDATE/DELETE statements.
    """
    range_cte: Optional[pgast.CommonTableExpr]
    range_rvar: Optional[pgast.RelRangeVar]

    clauses.compile_dml_bindings(ir_stmt, ctx=ctx)

    if isinstance(ir_stmt, (irast.UpdateStmt, irast.DeleteStmt)):
        # UPDATE and DELETE operate over a range, so generate
        # the corresponding CTE and connect it to the DML statements.
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

    top_typeref = ir_stmt.material_type

    typerefs = [top_typeref]

    if isinstance(ir_stmt, (irast.UpdateStmt, irast.DeleteStmt)):
        if top_typeref.union:
            for component in top_typeref.union:
                if component.material_type:
                    component = component.material_type

                typerefs.append(component)
                typerefs.extend(irtyputils.get_typeref_descendants(component))

        typerefs.extend(irtyputils.get_typeref_descendants(top_typeref))

    dml_map = {}

    for typeref in typerefs:
        if typeref.union:
            continue
        dml_cte, dml_rvar = gen_dml_cte(
            ir_stmt,
            range_rvar=range_rvar,
            typeref=typeref,
            ctx=ctx,
        )

        dml_map[typeref] = (dml_cte, dml_rvar)

    else_cte = None
    if (
        isinstance(ir_stmt, irast.InsertStmt)
        and ir_stmt.on_conflict and ir_stmt.on_conflict.else_ir is not None
    ):
        dml_cte = pgast.CommonTableExpr(
            query=pgast.SelectStmt(),
            name=ctx.env.aliases.get(hint='melse')
        )
        dml_rvar = relctx.rvar_for_rel(dml_cte, ctx=ctx)
        else_cte = (dml_cte, dml_rvar)

    if ctx.enclosing_cte_iterator:
        pathctx.put_path_bond(ctx.rel, ctx.enclosing_cte_iterator.path_id)

    ctx.dml_stmt_stack.append(ir_stmt)

    return DMLParts(
        dml_ctes=dml_map,
        range_cte=range_cte,
        else_cte=else_cte,
    )


def gen_dml_union(
    ir_stmt: irast.MutatingStmt,
    parts: DMLParts,
    *,
    ctx: context.CompilerContextLevel
) -> Tuple[pgast.CommonTableExpr, pgast.PathRangeVar]:
    dml_entries = list(parts.dml_ctes.values())
    if parts.else_cte:
        dml_entries.append(parts.else_cte)

    if len(dml_entries) == 1:
        union_cte, union_rvar = dml_entries[0]
    else:
        union_components = []
        for _, dml_rvar in dml_entries:
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

        assert qry.larg
        if ctx.enclosing_cte_iterator:
            pathctx.put_path_bond(qry.larg, ctx.enclosing_cte_iterator.path_id)

        union_cte = pgast.CommonTableExpr(
            query=qry.larg,
            name=ctx.env.aliases.get(hint='ma')
        )

        union_rvar = relctx.rvar_for_rel(
            union_cte,
            typeref=ir_stmt.subject.typeref,
            ctx=ctx,
        )

    ctx.dml_stmts[ir_stmt] = union_cte

    return union_cte, union_rvar


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
        # We generate a Select as the initial statement for an update,
        # since the contents select is the query that needs to join
        # the range and include policy filters and because we
        # sometimes end up not needing an UPDATE anyway (if it only
        # touches link tables).
        dml_stmt = pgast.SelectStmt()
    elif isinstance(ir_stmt, irast.DeleteStmt):
        dml_stmt = pgast.DeleteStmt()
    else:
        raise AssertionError(f'unexpected DML IR: {ir_stmt!r}')

    relation = relctx.range_for_typeref(
        typeref,
        target_path_id,
        for_mutation=True,
        ctx=ctx,
    )
    if isinstance(dml_stmt, pgast.DMLQuery):
        dml_stmt.relation = relation
    pathctx.put_path_value_rvar(
        dml_stmt, target_path_id, relation, env=ctx.env)
    pathctx.put_path_source_rvar(
        dml_stmt, target_path_id, relation, env=ctx.env)
    # Skip the path bond for inserts, since it doesn't help and
    # interferes when inserting in an UNLESS CONFLICT ELSE
    if not isinstance(ir_stmt, irast.InsertStmt):
        pathctx.put_path_bond(dml_stmt, target_path_id)

    dml_cte = pgast.CommonTableExpr(
        query=dml_stmt,
        name=ctx.env.aliases.get(hint='m')
    )

    # Due to the fact that DML statements are structured
    # as a flat list of CTEs instead of nested range vars,
    # the top level path scope must be empty.  The necessary
    # range vars will be injected explicitly in all rels that
    # need them.
    ctx.path_scope.maps.clear()

    if range_rvar is not None:
        relctx.pull_path_namespace(
            target=dml_stmt, source=range_rvar, ctx=ctx)

        # Auxiliary relations are always joined via the WHERE
        # clause due to the structure of the UPDATE/DELETE SQL statements.
        assert isinstance(dml_stmt, (pgast.SelectStmt, pgast.DeleteStmt))
        dml_stmt.where_clause = astutils.new_binop(
            lexpr=pgast.ColumnRef(name=[
                relation.alias.aliasname, 'id'
            ]),
            op='=',
            rexpr=pathctx.get_rvar_path_identity_var(
                range_rvar, target_ir_set.path_id, env=ctx.env)
        )
        # Do any read-side filtering
        if pol_expr := ir_stmt.read_policies.get(typeref.id):
            with ctx.newrel() as sctx:
                pathctx.put_path_value_rvar(
                    sctx.rel, target_path_id, relation, env=ctx.env)
                pathctx.put_path_source_rvar(
                    sctx.rel, target_path_id, relation, env=ctx.env)

                val = clauses.compile_filter_clause(
                    pol_expr.expr, pol_expr.cardinality, ctx=sctx)
            sctx.rel.target_list.append(pgast.ResTarget(val=val))

            dml_stmt.where_clause = astutils.extend_binop(
                dml_stmt.where_clause, sctx.rel
            )

        # SELECT has "FROM", while DELETE has "USING".
        if isinstance(dml_stmt, pgast.SelectStmt):
            dml_stmt.from_clause.append(relation)
            dml_stmt.from_clause.append(range_rvar)
        elif isinstance(dml_stmt, pgast.DeleteStmt):
            dml_stmt.using_clause.append(range_rvar)

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

    if ctx.dml_stmt_stack:
        relctx.reuse_type_rel_overlays(
            dml_source=ir_stmt, dml_stmts=ctx.dml_stmt_stack, ctx=ctx)

    return dml_rvar


def merge_iterator_scope(
    iterator: Optional[pgast.IteratorCTE],
    select: pgast.SelectStmt,
    *,
    ctx: context.CompilerContextLevel
) -> None:
    while iterator:
        ctx.path_scope[iterator.path_id] = select
        iterator = iterator.parent


def merge_iterator(
    iterator: Optional[pgast.IteratorCTE],
    select: pgast.SelectStmt,
    *,
    ctx: context.CompilerContextLevel
) -> None:
    merge_iterator_scope(iterator, select, ctx=ctx)

    if iterator:
        iterator_rvar = relctx.rvar_for_rel(iterator.cte, ctx=ctx)

        pathctx.put_path_bond(select, iterator.path_id)
        relctx.include_rvar(
            select, iterator_rvar,
            path_id=iterator.path_id,
            overwrite_path_rvar=True,
            ctx=ctx)
        # We need nested iterators to re-export their enclosing
        # iterators in some cases that the path_id_mask blocks
        # otherwise.
        select.path_id_mask.discard(iterator.path_id)


def fini_dml_stmt(
    ir_stmt: irast.MutatingStmt,
    wrapper: pgast.Query,
    parts: DMLParts,
    *,
    parent_ctx: context.CompilerContextLevel,
    ctx: context.CompilerContextLevel,
) -> pgast.Query:

    union_cte, union_rvar = gen_dml_union(ir_stmt, parts, ctx=ctx)

    if len(parts.dml_ctes) > 1 or parts.else_cte:
        ctx.toplevel_stmt.append_cte(union_cte)

    relctx.include_rvar(ctx.rel, union_rvar, ir_stmt.subject.path_id, ctx=ctx)

    # Record the effect of this insertion in the relation overlay
    # context to ensure that the RETURNING clause potentially
    # referencing this class yields the expected results.
    dml_stack = ctx.dml_stmt_stack
    if isinstance(ir_stmt, irast.InsertStmt):
        # The union CTE might have a SELECT from an ELSE clause, which
        # we don't actually want to include.
        assert len(parts.dml_ctes) == 1
        cte = next(iter(parts.dml_ctes.values()))[0]
        relctx.add_type_rel_overlay(
            ir_stmt.subject.typeref, 'union', cte,
            dml_stmts=dml_stack, path_id=ir_stmt.subject.path_id, ctx=ctx)
    elif isinstance(ir_stmt, irast.UpdateStmt):
        base_typeref = ir_stmt.subject.typeref.real_material_type

        for typeref, (cte, _) in parts.dml_ctes.items():
            # Because we have a nice union_cte for the base type,
            # we don't need to propagate the children overlays to
            # that type or its ancestors, hence the stop_ref argument.
            if typeref.id == base_typeref.id:
                cte = union_cte
                stop_ref = None
            else:
                stop_ref = base_typeref

            # The overlay for update is in two parts:
            # First, filter out objects that have been updated, then union them
            # back in. (If we just did union, we'd see the old values also.)
            relctx.add_type_rel_overlay(
                typeref, 'filter', cte,
                stop_ref=stop_ref,
                dml_stmts=dml_stack, path_id=ir_stmt.subject.path_id, ctx=ctx)
            relctx.add_type_rel_overlay(
                typeref, 'union', cte,
                stop_ref=stop_ref,
                dml_stmts=dml_stack, path_id=ir_stmt.subject.path_id, ctx=ctx)

        process_update_conflicts(
            ir_stmt=ir_stmt, update_cte=union_cte, dml_parts=parts, ctx=ctx)
    elif isinstance(ir_stmt, irast.DeleteStmt):
        relctx.add_type_rel_overlay(
            ir_stmt.subject.typeref, 'except', union_cte,
            dml_stmts=dml_stack, path_id=ir_stmt.subject.path_id, ctx=ctx)

    clauses.compile_output(ir_stmt.result, ctx=ctx)

    ctx.dml_stmt_stack.pop()

    return wrapper


def get_dml_range(
    ir_stmt: Union[irast.UpdateStmt, irast.DeleteStmt],
    *,
    ctx: context.CompilerContextLevel,
) -> pgast.CommonTableExpr:
    """Create a range CTE for the given DML statement.

    Args:
        ir_stmt:
            IR of the DML statement.

    Returns:
        A CommonTableExpr node representing the range affected
        by the DML statement.
    """
    target_ir_set = ir_stmt.subject
    ir_qual_expr = ir_stmt.where
    ir_qual_card = ir_stmt.where_card

    with ctx.newrel() as subctx:
        subctx.expr_exposed = False
        range_stmt = subctx.rel

        merge_iterator(ctx.enclosing_cte_iterator, range_stmt, ctx=subctx)

        dispatch.visit(target_ir_set, ctx=subctx)

        pathctx.get_path_identity_output(
            range_stmt, target_ir_set.path_id, env=subctx.env)

        if ir_qual_expr is not None:
            with subctx.new() as wctx:
                clauses.setup_iterator_volatility(target_ir_set,
                                                  is_cte=True, ctx=wctx)
                range_stmt.where_clause = astutils.extend_binop(
                    range_stmt.where_clause,
                    clauses.compile_filter_clause(
                        ir_qual_expr, ir_qual_card, ctx=wctx))

        range_stmt.path_id_mask.discard(target_ir_set.path_id)
        pathctx.put_path_bond(range_stmt, target_ir_set.path_id)

        range_cte = pgast.CommonTableExpr(
            query=range_stmt,
            name=ctx.env.aliases.get('range')
        )

        return range_cte


def compile_iterator_cte(
    iterator_set: irast.Set,
    *,
    ctx: context.CompilerContextLevel
) -> Optional[pgast.IteratorCTE]:

    last_iterator = ctx.enclosing_cte_iterator

    # If this iterator has already been compiled to a CTE, use
    # that CTE instead of recompiling. (This will happen when
    # a DML-containing FOR loop is WITH bound, for example.)
    if iterator_set in ctx.dml_stmts:
        iterator_cte = ctx.dml_stmts[iterator_set]
        return pgast.IteratorCTE(
            path_id=iterator_set.path_id, cte=iterator_cte,
            parent=last_iterator)

    with ctx.newrel() as ictx:
        ictx.path_scope[iterator_set.path_id] = ictx.rel

        # Correlate with enclosing iterators
        merge_iterator(last_iterator, ictx.rel, ctx=ictx)
        clauses.setup_iterator_volatility(last_iterator, is_cte=True,
                                          ctx=ictx)

        clauses.compile_iterator_expr(ictx.rel, iterator_set, ctx=ictx)
        if iterator_set.path_id.is_objtype_path():
            relgen.ensure_source_rvar(iterator_set, ictx.rel, ctx=ictx)
        ictx.rel.path_id = iterator_set.path_id
        pathctx.put_path_bond(ictx.rel, iterator_set.path_id)
        iterator_cte = pgast.CommonTableExpr(
            query=ictx.rel,
            name=ctx.env.aliases.get('iter')
        )
        ictx.toplevel_stmt.append_cte(iterator_cte)

    ctx.dml_stmts[iterator_set] = iterator_cte

    return pgast.IteratorCTE(
        path_id=iterator_set.path_id, cte=iterator_cte,
        parent=last_iterator)


def process_insert_body(
    *,
    ir_stmt: irast.InsertStmt,
    insert_cte: pgast.CommonTableExpr,
    dml_parts: DMLParts,
    ctx: context.CompilerContextLevel,
) -> None:
    """Generate SQL DML CTEs from an InsertStmt IR.

    Args:
        ir_stmt:
            IR of the DML statement.
        insert_cte:
            A CommonTableExpr node representing the SQL INSERT into
            the main relation of the DML subject.
        else_cte_rvar:
            If present, a tuple containing a CommonTableExpr and
            a RangeVar for it, which represent the body of an
            ELSE clause in an UNLESS CONFLICT construct.
        dml_parts:
            A DMLParts tuple returned by init_dml_stmt().
    """

    # We build the tuples to insert in a select we put into a CTE
    select = pgast.SelectStmt(target_list=[])
    values = select.target_list

    # The main INSERT query of this statement will always be
    # present to insert at least the `id` and `__type__`
    # properties.
    insert_stmt = insert_cte.query
    assert isinstance(insert_stmt, pgast.InsertStmt)

    typeref = ir_stmt.subject.typeref
    if typeref.material_type is not None:
        typeref = typeref.material_type

    values.append(
        pgast.ResTarget(
            name='__type__',
            val=pgast.TypeCast(
                arg=pgast.StringConstant(val=str(typeref.id)),
                type_name=pgast.TypeName(name=('uuid',))
            ),
        )
    )

    # Handle an UNLESS CONFLICT if we need it

    # If there is an UNLESS CONFLICT, we need to know that there is a
    # conflict *before* we execute DML for fields stored in the object
    # itself, so we can prevent that execution from happening. If that
    # is necessary, compile_insert_else_body will generate an iterator
    # CTE with a row for each non-conflicting insert we want to do. We
    # then use that as the iterator for any DML in inline fields.
    #
    # (For DML in the definition of pointers stored in link tables, we
    # don't need to worry about this, because we can run that DML
    # after the enclosing INSERT, using the enclosing INSERT as the
    # iterator.)
    on_conflict_fake_iterator = None
    if ir_stmt.on_conflict:
        assert not insert_stmt.on_conflict

        on_conflict_fake_iterator = compile_insert_else_body(
            insert_stmt,
            ir_stmt,
            ir_stmt.on_conflict,
            ctx.enclosing_cte_iterator,
            dml_parts.else_cte,
            dml_parts,
            ctx=ctx,
        )

    iterator = ctx.enclosing_cte_iterator
    inner_iterator = on_conflict_fake_iterator or iterator

    # Compile the shape
    external_inserts = []

    ptr_map: Dict[irast.BasePointerRef, pgast.BaseExpr] = {}

    # Use a dynamic rvar to return values out of the select purely
    # based on material rptr, as if it was a base relation.
    def dynamic_get_path(
        rel: pgast.Query, path_id: irast.PathId, *,
        flavor: str,
        aspect: str, env: context.Environment
    ) -> Optional[pgast.BaseExpr | pgast.PathRangeVar]:
        if flavor != 'normal' or aspect not in ('value', 'identity'):
            return None
        if not (rptr := path_id.rptr()):
            return None
        if ret := ptr_map.get(rptr.real_material_ptr):
            return ret
        # Properties that aren't specified are {}
        return pgast.NullConstant()

    fallback_rvar = pgast.DynamicRangeVar(dynamic_get_path=dynamic_get_path)
    pathctx.put_path_source_rvar(
        select, ir_stmt.subject.path_id, fallback_rvar, env=ctx.env)
    pathctx.put_path_value_rvar(
        select, ir_stmt.subject.path_id, fallback_rvar, env=ctx.env)

    with ctx.newrel() as subctx:
        subctx.enclosing_cte_iterator = inner_iterator

        subctx.rel = select
        subctx.expr_exposed = False

        inner_iterator_id = None
        if inner_iterator is not None:
            subctx.path_scope = ctx.path_scope.new_child()
            merge_iterator(inner_iterator, select, ctx=subctx)
            inner_iterator_id = relctx.get_path_var(
                select, inner_iterator.path_id, aspect='identity', ctx=ctx)

        # Process the Insert IR and separate links that go
        # into the main table from links that are inserted into
        # a separate link table.
        for shape_el, shape_op in ir_stmt.subject.shape:
            assert shape_op is qlast.ShapeOp.ASSIGN

            # If the shape element is a linkprop, we do nothing.
            # It will be picked up by the enclosing DML.
            if shape_el.path_id.is_linkprop_path():
                continue

            rptr = shape_el.rptr
            assert rptr is not None
            ptrref = rptr.ptrref
            if ptrref.material_ptr is not None:
                ptrref = ptrref.material_ptr

            ptr_info = pg_types.get_ptrref_storage_info(
                ptrref, resolve_type=True, link_bias=False)

            # First, process all local link inserts.
            if ptr_info.table_type == 'ObjectType':
                rel = compile_insert_shape_element(
                    ir_stmt=ir_stmt,
                    shape_el=shape_el,
                    iterator_id=inner_iterator_id,
                    ctx=ctx,
                )

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

                ptr_map[ptrref] = insvalue
                values.append(pgast.ResTarget(
                    name=ptr_info.column_name, val=insvalue))

            # Register all link table inserts to be run after the main
            # insert.  Note that single links with link properties are
            # processed both as a local link insert (for the inline
            # pointer) and as a link table insert (because lprops are
            # stored in link tables).
            link_ptr_info = pg_types.get_ptrref_storage_info(
                ptrref, resolve_type=False, link_bias=True)

            if link_ptr_info and link_ptr_info.table_type == 'link':
                external_inserts.append(shape_el)

        if iterator is not None:
            pathctx.put_path_bond(select, iterator.path_id)

    for aspect in ('value', 'identity'):
        pathctx._put_path_output_var(
            select, ir_stmt.subject.path_id, aspect=aspect,
            var=pgast.ColumnRef(name=['id']), env=ctx.env,
        )

    # Put the select that builds the tuples to insert into its own CTE.
    # We do this for two reasons:
    # 1. Generating the object ids outside of the actual SQL insert allows
    #    us to join any enclosing iterators into any nested external inserts.
    # 2. We can use the contents CTE to evaluate insert access policies
    #    before we actually try the insert. This is important because
    #    otherwise an exclusive constraint could be raised first,
    #    which leaks information.
    pathctx.put_path_bond(select, ir_stmt.subject.path_id)
    contents_cte = pgast.CommonTableExpr(
        query=select,
        name=ctx.env.aliases.get('ins_contents')
    )
    ctx.toplevel_stmt.append_cte(contents_cte)
    contents_rvar = relctx.rvar_for_rel(contents_cte, ctx=ctx)

    # Populate the real insert statement based on the select we generated
    insert_stmt.cols = [
        pgast.InsertTarget(name=not_none(value.name)) for value in values
    ]
    insert_stmt.select_stmt = pgast.SelectStmt(
        target_list=[
            pgast.ResTarget(val=col) for col in insert_stmt.cols
        ],
        from_clause=[contents_rvar],
    )
    pathctx.put_path_bond(insert_stmt, ir_stmt.subject.path_id)

    real_insert_cte = pgast.CommonTableExpr(
        query=insert_stmt,
        name=ctx.env.aliases.get('ins')
    )

    # Create the final CTE for the insert that joins the insert
    # and the select together.
    with ctx.newrel() as ictx:
        merge_iterator(iterator, ictx.rel, ctx=ictx)
        insert_rvar = relctx.rvar_for_rel(real_insert_cte, ctx=ctx)
        relctx.include_rvar(
            ictx.rel, insert_rvar, ir_stmt.subject.path_id, ctx=ictx)
        relctx.include_rvar(
            ictx.rel, contents_rvar, ir_stmt.subject.path_id, ctx=ictx)
    # TODO: set up dml_parts with a SelectStmt for inserts always?
    insert_cte.query = ictx.rel

    needs_insert_on_conflict = bool(
        ir_stmt.on_conflict and not on_conflict_fake_iterator)

    if needs_insert_on_conflict:
        ctx.toplevel_stmt.append_cte(real_insert_cte)
        ctx.toplevel_stmt.append_cte(insert_cte)

    dml_cte = contents_cte if not needs_insert_on_conflict else insert_cte

    pol_expr = ir_stmt.write_policies.get(typeref.id)
    pol_ctx = None
    if pol_expr:
        with ctx.new() as pol_ctx:
            pass

    # Process necessary updates to the link tables.
    link_ctes = []
    for shape_el in external_inserts:
        link_cte, check_cte = process_link_update(
            ir_stmt=ir_stmt,
            ir_set=shape_el,
            dml_cte=dml_cte,
            source_typeref=typeref,
            iterator=iterator,
            policy_ctx=pol_ctx,
            ctx=ctx,
        )
        if link_cte:
            link_ctes.append(link_cte)
        if check_cte is not None:
            ctx.env.check_ctes.append(check_cte)

    if pol_expr:
        assert pol_ctx
        assert not needs_insert_on_conflict
        policy_cte = compile_policy_check(
            contents_cte, ir_stmt, pol_expr, typeref=typeref, ctx=pol_ctx
        )
        force_policy_checks(
            policy_cte,
            (insert_stmt,) + tuple(cte.query for cte in link_ctes),
            ctx=ctx)

    for link_cte in link_ctes:
        ctx.toplevel_stmt.append_cte(link_cte)

    if not needs_insert_on_conflict:
        ctx.toplevel_stmt.append_cte(real_insert_cte)
        ctx.toplevel_stmt.append_cte(insert_cte)

    for extra_conflict in (ir_stmt.conflict_checks or ()):
        compile_insert_else_body(
            insert_stmt,
            ir_stmt,
            extra_conflict,
            inner_iterator,
            None,
            dml_parts,
            ctx=ctx,
        )


def compile_policy_check(
    dml_cte: pgast.CommonTableExpr,
    ir_stmt: irast.MutatingStmt,
    access_policies: irast.WritePolicies,
    typeref: irast.TypeRef,
    *,
    ctx: context.CompilerContextLevel,
) -> pgast.CommonTableExpr:
    subject_id = ir_stmt.subject.path_id

    with ctx.newrel() as ictx:
        # Pull in ptr rel overlays, so we can see the pointers
        ictx.ptr_rel_overlays = ctx.ptr_rel_overlays.copy()
        ictx.ptr_rel_overlays[None] = ictx.ptr_rel_overlays[None].copy()
        ictx.ptr_rel_overlays[None].update(
            ictx.ptr_rel_overlays[ir_stmt])

        ictx.type_rel_overlays = ctx.type_rel_overlays.copy()
        ictx.type_rel_overlays[None] = ictx.type_rel_overlays[None].copy()
        ictx.type_rel_overlays[None].update(
            ictx.type_rel_overlays[ir_stmt])

        dml_rvar = relctx.rvar_for_rel(dml_cte, ctx=ctx)
        relctx.include_rvar(ictx.rel, dml_rvar, path_id=subject_id, ctx=ictx)

        # split and compile
        allow, deny = [], []
        for policy in access_policies.policies:
            cond_ref = clauses.compile_filter_clause(
                policy.expr, policy.cardinality, ctx=ictx
            )

            if policy.action == qltypes.AccessPolicyAction.Allow:
                allow.append((policy, cond_ref))
            else:
                deny.append((policy, cond_ref))

        def raise_if(a: pgast.BaseExpr, msg: pgast.BaseExpr) -> pgast.BaseExpr:
            return pgast.FuncCall(
                name=('edgedb', 'raise_on_null'),
                args=[
                    pgast.FuncCall(
                        name=('nullif',),
                        args=[a, pgast.BooleanConstant(val='TRUE')],
                    ),
                    pgast.StringConstant(val='insufficient_privilege'),
                    pgast.NamedFuncArg(
                        name='msg',
                        val=msg,
                    ),
                    pgast.NamedFuncArg(
                        name='table',
                        val=pgast.StringConstant(val=str(typeref.id)),
                    ),
                ],
            )

        # allow
        if allow:
            allow_conds = (cond for _, cond in allow)
            no_allow_expr: pgast.BaseExpr = astutils.new_unop(
                'NOT', astutils.extend_binop(None, *allow_conds, op='OR')
            )
        else:
            no_allow_expr = pgast.BooleanConstant(val='TRUE')

        # deny
        deny_exprs = (cond for _, cond in deny)

        # message
        if isinstance(ir_stmt, irast.InsertStmt):
            op = 'insert'
        else:
            op = 'update'
        msg = f'access policy violation on {op} of {typeref.name_hint}'

        allow_hints = (pol.error_msg for pol, _ in allow if pol.error_msg)
        allow_hint = ', '.join(allow_hints)

        hints = [(allow_hint, no_allow_expr)] + [
            (pol.error_msg, cond) for pol, cond in deny if pol.error_msg
        ]

        hint = _conditional_string_agg(hints)
        if hint:
            hint = astutils.new_coalesce(
                astutils.extend_concat(' (', hint, ')'),
                pgast.StringConstant(val=''),
            )
            message = astutils.extend_concat(msg, hint)
        else:
            message = astutils.extend_concat(msg)

        ictx.rel.target_list.append(
            pgast.ResTarget(
                name=f'error',
                val=raise_if(
                    astutils.extend_binop(no_allow_expr, *deny_exprs, op='OR'),
                    msg=message,
                ),
            )
        )

        policy_cte = pgast.CommonTableExpr(
            query=ictx.rel,
            name=ctx.env.aliases.get('policy'),
            materialized=True,
        )
        ictx.toplevel_stmt.append_cte(policy_cte)
        return policy_cte


def _conditional_string_agg(
    pairs: Sequence[Tuple[Optional[str], pgast.BaseExpr]],
) -> Optional[pgast.BaseExpr]:

    selects = [
        pgast.SelectStmt(
            target_list=[
                pgast.ResTarget(
                    val=pgast.StringConstant(val=str)
                    if str
                    else pgast.NullConstant()
                )
            ],
            where_clause=cond,
        )
        for str, cond in pairs
    ]
    union = astutils.extend_select_op(None, *selects)

    if not union:
        return None

    return pgast.SelectStmt(
        target_list=[
            pgast.ResTarget(
                val=pgast.FuncCall(
                    name=('string_agg',),
                    args=[
                        pgast.ColumnRef(name=('error_msg',)),
                        pgast.StringConstant(val=', '),
                    ],
                ),
            )
        ],
        from_clause=[
            pgast.RangeSubselect(
                subquery=union,
                alias=pgast.Alias(aliasname='t', colnames=['error_msg']),
            )
        ],
    )


def force_policy_checks(
        policy_cte: pgast.CommonTableExpr,
        queries: Sequence[pgast.Query],
        *,
        ctx: context.CompilerContextLevel) -> None:
    # The actual DML statements need to be made dependent on the
    # policy CTE, to ensure that it is evaluated before any
    # modifications are done.

    scan = pgast.Expr(
        kind=pgast.ExprKind.OP, name='>',
        lexpr=clauses.make_check_scan(policy_cte, ctx=ctx),
        rexpr=pgast.NumericConstant(val="-1"),
    )
    stmt: Optional[pgast.Query]
    for stmt in queries:
        if isinstance(stmt, pgast.InsertStmt):
            stmt = stmt.select_stmt
        if isinstance(stmt, (pgast.SelectStmt, pgast.UpdateStmt)):
            stmt.where_clause = astutils.extend_binop(
                stmt.where_clause, scan
            )

    # If there aren't any update/insert queries to put it into
    # (because it is just an update with a -=, probably), make it a
    # normal check CTE.
    if not queries:
        ctx.env.check_ctes.append(policy_cte)


def insert_needs_conflict_cte(
    ir_stmt: irast.MutatingStmt,
    on_conflict: irast.OnConflictClause,
    *,
    ctx: context.CompilerContextLevel,
) -> bool:
    # We need to generate a conflict CTE if it is possible that
    # the query might generate two conflicting objects.
    # For now, we calculate that conservatively and generate a conflict
    # CTE if there are iterators or other DML statements that we have
    # seen already.
    # A more fine-grained scheme would check if there are enclosing
    # iterators or INSERT/UPDATEs to types that could conflict.
    if on_conflict.else_fail:
        return False

    if ctx.dml_stmts:
        return True

    if on_conflict.always_check or ir_stmt.conflict_checks:
        return True

    type_id = ir_stmt.subject.typeref.real_material_type.id
    if (
        (type_id, True) in ctx.env.type_rewrites
        or (type_id, False) in ctx.env.type_rewrites
    ):
        return True

    for shape_el, _ in ir_stmt.subject.shape:
        assert shape_el.rptr is not None
        ptrref = shape_el.rptr.ptrref
        ptr_info = pg_types.get_ptrref_storage_info(
            ptrref, resolve_type=True, link_bias=False)

        # We need to generate a conflict CTE if we have a DML containing
        # pointer stored in the object itself
        if (
            ptr_info.table_type == 'ObjectType'
            and shape_el.expr
            and irutils.contains_dml(shape_el.expr, skip_bindings=True)
        ):
            return True

    return False


def compile_insert_else_body(
        stmt: Optional[pgast.InsertStmt],
        ir_stmt: irast.MutatingStmt,
        on_conflict: irast.OnConflictClause,
        enclosing_cte_iterator: Optional[pgast.IteratorCTE],
        else_cte_rvar: Optional[
            Tuple[pgast.CommonTableExpr, pgast.PathRangeVar]],
        dml_parts: DMLParts,
        *,
        ctx: context.CompilerContextLevel) -> Optional[pgast.IteratorCTE]:

    else_select = on_conflict.select_ir
    else_branch = on_conflict.else_ir
    else_fail = on_conflict.else_fail

    # We need to generate a "conflict CTE" that filters out
    # objects-to-insert that would conflict with existing objects in
    # three scenarios:
    #  1) When there is a nested DML operation as part of the value
    #     of a pointer that is stored inline with the object.
    #     This is because we need to prevent that DML from executing
    #     before we have a chance to see what ON CONFLICT does.
    #  2) When there could be a conflict with an object INSERT/UPDATEd
    #     in this same query. (Either because of FOR or other DML statements.)
    #     This is because we need that to raise a ConstraintError,
    #     which means we can't use ON CONFLICT, and so we need to prevent
    #     the insertion of objects that conflict with existing ones ourselves.
    #  3) When the type to insert has rewrite rules on it that could
    #     prevent seeing the existing objects, we use conflict ctes
    #     instead of setting ON CONFLICT so that we raise ConstraintError
    #     instead of succeeding. This is partially for compatibility with
    #     cases that have access rules and fall into case 1, where we
    #     must do this, and partly because we would not be able to return
    #     the objects in the ELSE anyway.
    #
    # When we need a conflict CTE, we don't use SQL ON CONFLICT. In
    # cases 2 & 3, that is the whole point, while in case 1 it would
    # just be superfluous to do so.
    #
    # When none of these cases obtain, we use ON CONFLICT because it
    # ought to be more performant.
    needs_conflict_cte = insert_needs_conflict_cte(
        ir_stmt, on_conflict, ctx=ctx)
    if not needs_conflict_cte and not else_fail:
        infer = None
        if on_conflict.constraint:
            constraint_name = common.get_constraint_raw_name(
                on_conflict.constraint.id)
            infer = pgast.InferClause(conname=f'"{constraint_name}"')

        assert isinstance(stmt, pgast.InsertStmt)
        stmt.on_conflict = pgast.OnConflictClause(
            action='nothing',
            infer=infer,
        )

    if not else_branch and not needs_conflict_cte and not else_fail:
        return None

    subject_id = ir_stmt.subject.path_id

    # Compile the query CTE that selects out the existing rows
    # that we would conflict with
    with ctx.newrel() as ictx:
        ictx.expr_exposed = False
        ictx.path_scope[subject_id] = ictx.rel

        compile_insert_else_body_failure_check(on_conflict, ctx=ictx)

        merge_iterator(enclosing_cte_iterator, ictx.rel, ctx=ictx)
        clauses.setup_iterator_volatility(enclosing_cte_iterator,
                                          is_cte=True, ctx=ictx)

        dispatch.compile(else_select, ctx=ictx)
        pathctx.put_path_id_map(ictx.rel, subject_id, else_select.path_id)
        # Discard else_branch from the path_id_mask to prevent subject_id
        # from being masked.
        ictx.rel.path_id_mask.discard(else_select.path_id)

        else_select_cte = pgast.CommonTableExpr(
            query=ictx.rel,
            name=ctx.env.aliases.get('else')
        )
        if else_fail:
            ctx.env.check_ctes.append(else_select_cte)

        ictx.toplevel_stmt.append_cte(else_select_cte)

    else_select_rvar = relctx.rvar_for_rel(else_select_cte, ctx=ctx)

    if else_branch:
        # Compile the body of the ELSE query
        with ctx.newrel() as ictx:
            ictx.path_scope[subject_id] = ictx.rel

            relctx.include_rvar(ictx.rel, else_select_rvar,
                                path_id=else_select.path_id, ctx=ictx)

            ictx.enclosing_cte_iterator = pgast.IteratorCTE(
                path_id=else_select.path_id, cte=else_select_cte,
                parent=enclosing_cte_iterator)
            ictx.volatility_ref = ()
            dispatch.compile(else_branch, ctx=ictx)
            pathctx.put_path_id_map(ictx.rel, subject_id, else_branch.path_id)
            # Discard else_branch from the path_id_mask to prevent subject_id
            # from being masked.
            ictx.rel.path_id_mask.discard(else_branch.path_id)

            assert else_cte_rvar
            else_branch_cte = else_cte_rvar[0]
            else_branch_cte.query = ictx.rel
            ictx.toplevel_stmt.append_cte(else_branch_cte)

    anti_cte_iterator = None
    if needs_conflict_cte:
        # Compile a CTE that matches rows that didn't appear in the
        # ELSE query of conflicting rows.
        with ctx.newrel() as ictx:
            merge_iterator(enclosing_cte_iterator, ictx.rel, ctx=ictx)
            clauses.setup_iterator_volatility(enclosing_cte_iterator,
                                              is_cte=True, ctx=ictx)

            # Set up a dummy path to represent all of the rows
            # that *aren't* being filtered out
            dummy_pathid = irast.PathId.from_typeref(
                typeref=irast.TypeRef(
                    id=uuidgen.uuid1mc(),
                    name_hint=sn.QualName(
                        module='__derived__',
                        name=ctx.env.aliases.get('dummy'))))
            with ctx.subrel() as dctx:
                dummy_q = dctx.rel
                relctx.ensure_transient_identity_for_path(
                    dummy_pathid, dummy_q, ctx=dctx)
            dummy_rvar = relctx.rvar_for_rel(
                dummy_q, lateral=True, ctx=ictx)
            relctx.include_rvar(ictx.rel, dummy_rvar,
                                path_id=dummy_pathid, ctx=ictx)

            with ctx.subrel() as subrelctx:
                subrel = subrelctx.rel
                relctx.include_rvar(subrel, else_select_rvar,
                                    path_id=subject_id, ctx=ictx)

            # Do the anti-join
            iter_path_id = (
                enclosing_cte_iterator.path_id if
                enclosing_cte_iterator else None)
            relctx.anti_join(ictx.rel, subrel, iter_path_id, ctx=ctx)

            # Package it up as a CTE
            anti_cte = pgast.CommonTableExpr(
                query=ictx.rel,
                name=ctx.env.aliases.get('non_conflict')
            )
            ictx.toplevel_stmt.append_cte(anti_cte)
            anti_cte_iterator = pgast.IteratorCTE(
                path_id=dummy_pathid, cte=anti_cte,
                parent=ictx.enclosing_cte_iterator)

    return anti_cte_iterator


def compile_insert_else_body_failure_check(
        on_conflict: irast.OnConflictClause,
        *,
        ctx: context.CompilerContextLevel) -> None:
    else_fail = on_conflict.else_fail
    if not else_fail:
        return

    # Copy the type rels from the possibly conflicting earlier DML
    # into the None overlays so it gets picked up.
    ctx.type_rel_overlays = ctx.type_rel_overlays.copy()
    overlays_map = ctx.type_rel_overlays[None].copy()
    ctx.type_rel_overlays[None] = overlays_map
    overlays_map.update(ctx.type_rel_overlays[else_fail])

    # Do some work so that we aren't looking at the existing on-disk
    # data, just newly data created data.
    for k, overlays in overlays_map.items():
        # Strip out filters, which we don't care about in this context
        overlays = [(k, r, p) for k, r, p in overlays if k != 'filter']
        # Drop the initial set
        if overlays and overlays[0][0] == 'union':
            overlays[0] = ('replace', *overlays[0][1:])
        overlays_map[k] = overlays

    ctx.ptr_rel_overlays = ctx.ptr_rel_overlays.copy()
    ctx.ptr_rel_overlays[None] = ctx.ptr_rel_overlays[None].copy()
    ctx.ptr_rel_overlays[None].update(
        ctx.ptr_rel_overlays[else_fail])

    assert on_conflict.constraint
    cid = common.get_constraint_raw_name(on_conflict.constraint.id)
    maybe_raise = pgast.FuncCall(
        name=('edgedb', 'raise'),
        args=[
            pgast.TypeCast(
                arg=pgast.NullConstant(),
                type_name=pgast.TypeName(name=('text',))),
            pgast.StringConstant(val='exclusion_violation'),
            pgast.NamedFuncArg(
                name='msg',
                val=pgast.StringConstant(
                    val=(
                        f'duplicate key value violates unique '
                        f'constraint "{cid}"'
                    )
                ),
            ),
            pgast.NamedFuncArg(
                name='constraint',
                val=pgast.StringConstant(val=f"{cid}")
            ),
        ],
    )
    ctx.rel.target_list.append(
        pgast.ResTarget(name='error', val=maybe_raise)
    )


def compile_insert_shape_element(
    *,
    ir_stmt: irast.MutatingStmt,
    shape_el: irast.Set,
    iterator_id: Optional[pgast.BaseExpr],
    ctx: context.CompilerContextLevel,
) -> pgast.Query:

    with ctx.newscope() as insvalctx:
        # This method is only called if the upper cardinality of
        # the expression is one, so we check for AT_MOST_ONE
        # to determine nullability.
        assert shape_el.rptr is not None
        if (shape_el.rptr.dir_cardinality
                is qltypes.Cardinality.AT_MOST_ONE):
            insvalctx.force_optional |= {shape_el.path_id}

        if iterator_id is not None:
            id = iterator_id
            insvalctx.volatility_ref = (lambda _stmt, _ctx: id,)
        else:
            # Single inserts have no need for forced
            # computable volatility, and, furthermore,
            # we do not have a valid identity reference
            # anyway.
            insvalctx.volatility_ref = ()

        insvalctx.current_insert_path_id = ir_stmt.subject.path_id

        dispatch.visit(shape_el, ctx=insvalctx)

    return insvalctx.rel


def process_update_body(
    *,
    ir_stmt: irast.UpdateStmt,
    update_cte: pgast.CommonTableExpr,
    dml_parts: DMLParts,
    typeref: irast.TypeRef,
    ctx: context.CompilerContextLevel,
) -> None:
    """Generate SQL DML CTEs from an UpdateStmt IR.

    Args:
        ir_stmt:
            IR of the DML statement.
        update_cte:
            CTE representing the SQL UPDATE to the main relation of the UPDATE
            subject.
        dml_parts:
            A DMLParts tuple returned by init_dml_stmt().
        typeref:
            A TypeRef corresponding the the type of a subject being updated
            by the update_cte.
    """
    assert isinstance(update_cte.query, pgast.SelectStmt)
    contents_select = update_cte.query

    values = []

    if ctx.enclosing_cte_iterator:
        pathctx.put_path_bond(
            contents_select, ctx.enclosing_cte_iterator.path_id)

    external_updates = []

    assert dml_parts.range_cte
    iterator = pgast.IteratorCTE(
        path_id=ir_stmt.subject.path_id,
        cte=dml_parts.range_cte,
        parent=ctx.enclosing_cte_iterator)

    ptr_map: Dict[irast.BasePointerRef, pgast.BaseExpr] = {}

    with ctx.newscope() as subctx:
        # It is necessary to process the expressions in
        # the UpdateStmt shape body in the context of the
        # UPDATE statement so that references to the current
        # values of the updated object are resolved correctly.
        subctx.parent_rel = contents_select
        subctx.expr_exposed = False
        subctx.enclosing_cte_iterator = iterator

        for shape_el, shape_op in ir_stmt.subject.shape:
            if shape_op == qlast.ShapeOp.MATERIALIZE:
                continue

            assert shape_el.rptr is not None
            ptrref = shape_el.rptr.ptrref
            actual_ptrref = irtyputils.find_actual_ptrref(typeref, ptrref)
            updvalue = shape_el.expr
            ptr_info = pg_types.get_ptrref_storage_info(
                actual_ptrref, resolve_type=True, link_bias=False)

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
                            with scopectx.substmt() as srelctx:
                                dml_cte = ctx.dml_stmts[updvalue]
                                wrap_dml_cte(updvalue, dml_cte, ctx=srelctx)
                                pathctx.get_path_identity_output(
                                    srelctx.rel,
                                    updvalue.subject.path_id,
                                    env=srelctx.env,
                                )
                                val = srelctx.rel
                        else:
                            val = dispatch.compile(updvalue, ctx=scopectx)

                        assert isinstance(updvalue, irast.Stmt)
                        val = check_update_type(
                            val,
                            val,
                            is_subquery=True,
                            ir_stmt=ir_stmt,
                            ir_set=updvalue.result,
                            subject_typeref=typeref,
                            shape_ptrref=ptrref,
                            actual_ptrref=actual_ptrref,
                            ctx=scopectx,
                        )

                        val = pgast.TypeCast(
                            arg=val,
                            type_name=pgast.TypeName(name=ptr_info.column_type)
                        )

                    if shape_op is qlast.ShapeOp.SUBTRACT:
                        val = pgast.FuncCall(
                            name=('nullif',),
                            args=[
                                pgast.ColumnRef(name=[ptr_info.column_name]),
                                val,
                            ],
                        )

                    ptr_map[actual_ptrref] = val
                    updtarget = pgast.ResTarget(
                        name=ptr_info.column_name,
                        val=val,
                    )
                    values.append(updtarget)

            # Register all link table inserts to be run after the main
            # insert.  Note that single links with link properties are
            # processed both as a local link update (for the inline
            # pointer) and as a link table update (because lprops are
            # stored in link tables).
            link_ptr_info = pg_types.get_ptrref_storage_info(
                actual_ptrref, resolve_type=False, link_bias=True)

            if link_ptr_info and link_ptr_info.table_type == 'link':
                external_updates.append((shape_el, shape_op))

    contents_select.target_list.extend(values)

    relation = contents_select.from_clause[0]
    assert isinstance(relation, pgast.PathRangeVar)

    # Use a dynamic rvar to return values out of the select purely
    # based on material rptr, as if it was a base relation (and to
    # fall back to the base relation if the value wasn't updated.)
    def dynamic_get_path(
        rel: pgast.Query, path_id: irast.PathId, *,
        flavor: str,
        aspect: str, env: context.Environment
    ) -> Optional[pgast.BaseExpr | pgast.PathRangeVar]:
        if flavor != 'normal' or aspect not in ('value', 'identity'):
            return None
        if (
            (rptr := path_id.rptr())
            and (var := ptr_map.get(rptr.real_material_ptr))
        ):
            return var
        return relation

    fallback_rvar = pgast.DynamicRangeVar(dynamic_get_path=dynamic_get_path)
    pathctx.put_path_source_rvar(
        contents_select, ir_stmt.subject.path_id, fallback_rvar, env=ctx.env)
    pathctx.put_path_value_rvar(
        contents_select, ir_stmt.subject.path_id, fallback_rvar, env=ctx.env)

    toplevel = ctx.toplevel_stmt

    update_stmt = None
    if not values:
        # No updates directly to the set target table,
        # so convert the UPDATE statement into a SELECT.
        update_cte.query = contents_select
        contents_cte = update_cte

        toplevel.append_cte(update_cte)

    else:
        contents_cte = pgast.CommonTableExpr(
            query=contents_select,
            name=ctx.env.aliases.get('upd_contents')
        )

        toplevel.append_cte(contents_cte)
        contents_rvar = relctx.rvar_for_rel(contents_cte, ctx=ctx)

        target_path_id = ir_stmt.subject.path_id
        update_stmt = pgast.UpdateStmt(
            relation=relation,
            where_clause=astutils.new_binop(
                lexpr=pgast.ColumnRef(name=[relation.alias.aliasname, 'id']),
                op='=',
                rexpr=pathctx.get_rvar_path_identity_var(
                    contents_rvar, target_path_id, env=ctx.env)
            ),
            from_clause=[contents_rvar],
            targets=[pgast.UpdateTarget(
                name=[not_none(value.name) for value in values],
                val=pgast.SelectStmt(
                    target_list=[
                        pgast.ResTarget(
                            val=pgast.ColumnRef(name=[
                                contents_rvar.alias.aliasname,
                                not_none(value.name),
                            ]))
                        for value in values
                    ],
                )
            )],
        )
        relctx.pull_path_namespace(
            target=update_stmt, source=contents_rvar, ctx=ctx)
        pathctx.put_path_value_rvar(
            update_stmt, target_path_id, relation, env=ctx.env)
        pathctx.put_path_source_rvar(
            update_stmt, target_path_id, relation, env=ctx.env)

        update_cte.query = update_stmt

    pol_expr = ir_stmt.write_policies.get(typeref.id)
    pol_ctx = None
    if pol_expr:
        with ctx.new() as pol_ctx:
            pass

    # Process necessary updates to the link tables.
    link_ctes = []
    for expr, shape_op in external_updates:
        link_cte, check_cte = process_link_update(
            ir_stmt=ir_stmt,
            ir_set=expr,
            dml_cte=contents_cte,
            iterator=iterator,
            shape_op=shape_op,
            source_typeref=typeref,
            ctx=ctx,
            policy_ctx=pol_ctx,
        )
        if link_cte:
            link_ctes.append(link_cte)

        if check_cte is not None:
            ctx.env.check_ctes.append(check_cte)

    if pol_expr:
        assert pol_ctx
        policy_cte = compile_policy_check(
            contents_cte, ir_stmt, pol_expr, typeref=typeref, ctx=pol_ctx
        )
        force_policy_checks(
            policy_cte,
            ((update_stmt,) if update_stmt else ()) +
            tuple(cte.query for cte in link_ctes),
            ctx=ctx)

    if values:
        toplevel.append_cte(update_cte)

    for link_cte in link_ctes:
        toplevel.append_cte(link_cte)


def process_update_conflicts(
    *,
    ir_stmt: irast.UpdateStmt,
    update_cte: pgast.CommonTableExpr,
    dml_parts: DMLParts,
    ctx: context.CompilerContextLevel,
) -> None:
    if not ir_stmt.conflict_checks:
        return

    for extra_conflict in ir_stmt.conflict_checks:
        q_set = extra_conflict.update_query_set
        assert q_set
        typeref = q_set.path_id.target.real_material_type
        cte, _ = dml_parts.dml_ctes[typeref]

        pathctx.put_path_id_map(
            cte.query, q_set.path_id, ir_stmt.subject.path_id)

        conflict_iterator = pgast.IteratorCTE(
            path_id=q_set.path_id, cte=cte,
            parent=ctx.enclosing_cte_iterator)

        compile_insert_else_body(
            None,
            ir_stmt,
            extra_conflict,
            conflict_iterator,
            None,
            dml_parts,
            ctx=ctx,
        )


def check_update_type(
    val: pgast.BaseExpr,
    rel_or_rvar: Union[pgast.BaseExpr, pgast.PathRangeVar],
    *,
    is_subquery: bool,
    ir_stmt: irast.UpdateStmt,
    ir_set: irast.Set,
    subject_typeref: irast.TypeRef,
    shape_ptrref: irast.BasePointerRef,
    actual_ptrref: irast.BasePointerRef,
    ctx: context.CompilerContextLevel,
) -> pgast.BaseExpr:
    """Possibly insert a type check on an UPDATE to a link

    Because edgedb allows subtypes to covariantly override the target
    types of links, we need to insert runtime type checks when
    the target in a base type being UPDATEd does not match the
    target type for this concrete subtype being handled.
    """

    base_ptrref = irtyputils.find_actual_ptrref(
        ir_stmt.material_type, shape_ptrref)
    # We skip the check if either the base type matches exactly
    # or the shape type matches exactly. FIXME: *Really* we want to do
    # a subtype check, here, though, since this could do a needless
    # check if we have multiple levels of overloading, but we don't
    # have the infrastructure here.
    if (
        not irtyputils.is_object(ir_set.typeref)
        or base_ptrref.out_target.id == actual_ptrref.out_target.id
        or shape_ptrref.out_target.id == actual_ptrref.out_target.id
    ):
        return val

    if isinstance(rel_or_rvar, pgast.PathRangeVar):
        rvar = rel_or_rvar
    else:
        assert isinstance(rel_or_rvar, pgast.BaseRelation)
        rvar = relctx.rvar_for_rel(rel_or_rvar, ctx=ctx)

    # Find the ptrref for the __type__ link on our actual target type
    # and make up a new path_id to access it
    assert isinstance(actual_ptrref, irast.PointerRef)
    actual_type_ptrref = irtyputils.find_actual_ptrref(
        actual_ptrref.out_target, ir_stmt.dunder_type_ptrref)
    type_pathid = ir_set.path_id.extend(ptrref=actual_type_ptrref)

    # Grab the actual value we have inserted and pull the __type__ out
    rval = pathctx.get_rvar_path_identity_var(
        rvar, ir_set.path_id, env=ctx.env)
    typ = pathctx.get_rvar_path_identity_var(rvar, type_pathid, env=ctx.env)

    typeref_val = dispatch.compile(actual_ptrref.out_target, ctx=ctx)

    # Do the check! Include the ptrref for this concrete class and
    # also the (dynamic) type of the argument, so that we can produce
    # a good error message.
    check_result = pgast.FuncCall(
        name=('edgedb', 'issubclass'),
        args=[typ, typeref_val],
    )
    maybe_null = pgast.CaseExpr(
        args=[pgast.CaseWhen(expr=check_result, result=rval)])
    maybe_raise = pgast.FuncCall(
        name=('edgedb', 'raise_on_null'),
        args=[
            maybe_null,
            pgast.StringConstant(val='wrong_object_type'),
            pgast.NamedFuncArg(
                name='msg',
                val=pgast.StringConstant(val='covariance error')
            ),
            pgast.NamedFuncArg(
                name='column',
                val=pgast.StringConstant(val=str(actual_ptrref.id)),
            ),
            pgast.NamedFuncArg(
                name='table',
                val=pgast.TypeCast(
                    arg=typ, type_name=pgast.TypeName(name=('text',))
                ),
            ),
        ],
    )

    if is_subquery:
        # If this is supposed to be a subquery (because it is an
        # update of a single link), wrap the result query in a new one,
        # since we need to access two outputs from it and produce just one
        # from this query
        return pgast.SelectStmt(
            from_clause=[rvar],
            target_list=[pgast.ResTarget(val=maybe_raise)],
        )
    else:
        return maybe_raise


def process_link_update(
    *,
    ir_stmt: irast.MutatingStmt,
    ir_set: irast.Set,
    shape_op: qlast.ShapeOp = qlast.ShapeOp.ASSIGN,
    source_typeref: irast.TypeRef,
    dml_cte: pgast.CommonTableExpr,
    iterator: Optional[pgast.IteratorCTE] = None,
    ctx: context.CompilerContextLevel,
    policy_ctx: Optional[context.CompilerContextLevel],
) -> Tuple[Optional[pgast.CommonTableExpr], Optional[pgast.CommonTableExpr]]:
    """Perform updates to a link relation as part of a DML statement.

    Args:
        ir_stmt:
            IR of the DML statement.
        ir_set:
            IR of the INSERT/UPDATE body element.
        shape_op:
            The operation of the UPDATE body element (:=, +=, -=).  For
            INSERT this should always be :=.
        source_typeref:
            An ir.TypeRef instance representing the specific type of an object
            being updated.
        dml_cte:
            CTE representing the SQL INSERT or UPDATE to the main
            relation of the DML subject.
        iterator:
            IR and CTE representing the iterator range in the FOR clause
            of the EdgeQL DML statement (if present).
        policy_ctx:
            Optionally, a context in which to populate overlays that
            use the select CTE for overlays instead of the
            actual insert CTE. This is needed if an access policy is to
            be applied, and requires disabling a potential optimization.

            We need separate overlay contexts because default values for
            link properties don't currently get populated in our IR, so we
            need to do actual SQL DML to get their values. (And so we disallow
            their use in policies.)
    """
    toplevel = ctx.toplevel_stmt
    is_insert = isinstance(ir_stmt, irast.InsertStmt)

    rptr = ir_set.rptr
    assert rptr is not None
    ptrref = rptr.ptrref
    assert isinstance(ptrref, irast.PointerRef)
    target_is_scalar = not irtyputils.is_object(ir_set.typeref)
    path_id = ir_set.path_id

    # The links in the dml class shape have been derived,
    # but we must use the correct specialized link class for the
    # base material type.
    mptrref = irtyputils.find_actual_ptrref(source_typeref, ptrref)
    assert isinstance(mptrref, irast.PointerRef)

    target_rvar = relctx.range_for_ptrref(
        mptrref, for_mutation=True, only_self=True, ctx=ctx)
    assert isinstance(target_rvar, pgast.RelRangeVar)
    assert isinstance(target_rvar.relation, pgast.Relation)
    target_alias = target_rvar.alias.aliasname

    dml_cte_rvar = pgast.RelRangeVar(
        relation=dml_cte,
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get('m')
        )
    )

    # Turn the IR of the expression on the right side of :=
    # into a subquery returning records for the link table.
    data_cte, specified_cols = process_link_values(
        ir_stmt=ir_stmt,
        ir_expr=ir_set,
        dml_rvar=dml_cte_rvar,
        source_typeref=source_typeref,
        target_is_scalar=target_is_scalar,
        enforce_cardinality=(shape_op is qlast.ShapeOp.ASSIGN),
        dml_cte=dml_cte,
        iterator=iterator,
        ctx=ctx,
    )

    toplevel.append_cte(data_cte)

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

        source_ref = pathctx.get_rvar_path_identity_var(
            dml_cte_rvar,
            ir_stmt.subject.path_id,
            env=ctx.env,
        )

        if shape_op is qlast.ShapeOp.SUBTRACT:
            data_rvar = relctx.rvar_for_rel(data_select, ctx=ctx)

            if target_is_scalar:
                # MULTI properties are not distinct, and since `-=` must
                # be a proper inverse of `+=` we cannot simply DELETE
                # all property values matching the `-=` expression, and
                # instead have to resort to careful deletion of no more
                # than the number of tuples returned by the expression.
                # Here, we rely on the "ctid" system column to refer to
                # specific tuples.
                #
                # DELETE
                #   FROM <link-tab>
                # WHERE
                #   ctid IN (
                #     SELECT
                #       shortlist.ctid
                #     FROM
                #       (SELECT
                #         source,
                #         target,
                #         count(target) AS cnt
                #        FROM
                #         <data-expr>
                #        GROUP BY source, target
                #       ) AS counts,
                #       LATERAL (
                #         SELECT
                #           candidates.ctid
                #         FROM
                #           (SELECT
                #             ctid,
                #             row_number() OVER (
                #               PARTITION BY data
                #               ORDER BY data
                #             ) AS rn
                #           FROM
                #             <link-tab>
                #           WHERE
                #             source = counts.source
                #             AND target = counts.target
                #           ) AS candidates
                #         WHERE
                #           candidates.rn <= counts.cnt
                #       ) AS shortlist
                #   );

                val_src_ref = pgast.ColumnRef(
                    name=[data_rvar.alias.aliasname, 'source'],
                )
                val_tgt_ref = pgast.ColumnRef(
                    name=[data_rvar.alias.aliasname, 'target'],
                )
                counts_select = pgast.SelectStmt(
                    target_list=[
                        pgast.ResTarget(name='source', val=val_src_ref),
                        pgast.ResTarget(name='target', val=val_tgt_ref),
                        pgast.ResTarget(
                            name='cnt',
                            val=pgast.FuncCall(
                                name=('count',),
                                args=[val_tgt_ref],
                            ),
                        ),
                    ],
                    from_clause=[data_rvar],
                    group_clause=[val_src_ref, val_tgt_ref],
                )

                counts_rvar = relctx.rvar_for_rel(counts_select, ctx=ctx)
                counts_alias = counts_rvar.alias.aliasname

                target_ref = pgast.ColumnRef(name=[target_alias, 'target'])

                candidates_select = pgast.SelectStmt(
                    target_list=[
                        pgast.ResTarget(
                            name='ctid',
                            val=pgast.ColumnRef(
                                name=[target_alias, 'ctid'],
                            ),
                        ),
                        pgast.ResTarget(
                            name='rn',
                            val=pgast.FuncCall(
                                name=('row_number',),
                                args=[],
                                over=pgast.WindowDef(
                                    partition_clause=[target_ref],
                                    order_clause=[
                                        pgast.SortBy(node=target_ref),
                                    ],
                                ),
                            ),
                        ),
                    ],
                    from_clause=[target_rvar],
                    where_clause=astutils.new_binop(
                        lexpr=astutils.new_binop(
                            lexpr=pgast.ColumnRef(
                                name=[counts_alias, 'source'],
                            ),
                            op='=',
                            rexpr=pgast.ColumnRef(
                                name=[target_alias, 'source'],
                            ),
                        ),
                        op='AND',
                        rexpr=astutils.new_binop(
                            lexpr=target_ref,
                            op='=',
                            rexpr=pgast.ColumnRef(
                                name=[counts_alias, 'target']),
                        ),
                    ),
                )

                candidates_rvar = relctx.rvar_for_rel(
                    candidates_select, ctx=ctx)

                candidates_alias = candidates_rvar.alias.aliasname

                shortlist_select = pgast.SelectStmt(
                    target_list=[
                        pgast.ResTarget(
                            name='ctid',
                            val=pgast.ColumnRef(
                                name=[candidates_alias, 'ctid'],
                            ),
                        ),
                    ],
                    from_clause=[candidates_rvar],
                    where_clause=astutils.new_binop(
                        lexpr=pgast.ColumnRef(name=[candidates_alias, 'rn']),
                        op='<=',
                        rexpr=pgast.ColumnRef(name=[counts_alias, 'cnt']),
                    ),
                )

                shortlist_rvar = relctx.rvar_for_rel(
                    shortlist_select, lateral=True, ctx=ctx)
                shortlist_alias = shortlist_rvar.alias.aliasname

                ctid_select = pgast.SelectStmt(
                    target_list=[
                        pgast.ResTarget(
                            name='ctid',
                            val=pgast.ColumnRef(name=[shortlist_alias, 'ctid'])
                        ),
                    ],
                    from_clause=[
                        counts_rvar,
                        shortlist_rvar,
                    ],
                )

                delqry = pgast.DeleteStmt(
                    relation=target_rvar,
                    where_clause=astutils.new_binop(
                        lexpr=pgast.ColumnRef(
                            name=[target_alias, 'ctid'],
                        ),
                        op='=',
                        rexpr=pgast.SubLink(
                            type=pgast.SubLinkType.ANY,
                            expr=ctid_select,
                        ),
                    ),
                    returning_list=[
                        pgast.ResTarget(
                            val=pgast.ColumnRef(
                                name=[target_alias, pgast.Star()],
                            ),
                        )
                    ]
                )
            else:
                # Links are always distinct, so we can simply
                # DELETE the tuples matching the `-=` expression.
                delqry = pgast.DeleteStmt(
                    relation=target_rvar,
                    where_clause=astutils.new_binop(
                        lexpr=astutils.new_binop(
                            lexpr=source_ref,
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
                    lexpr=source_ref,
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

        if shape_op is not qlast.ShapeOp.SUBTRACT:
            # Correlate the deletion with INSERT to make sure
            # link properties get erased properly and we aren't
            # just ON CONFLICT UPDATE-ing the link rows.
            # This basically just tacks on a
            #    WHERE (SELECT count(*) FROM delcte) IS NOT NULL)
            del_select = pgast.SelectStmt(
                target_list=[
                    pgast.ResTarget(
                        val=pgast.FuncCall(
                            name=['count'],
                            args=[pgast.ColumnRef(name=[pgast.Star()])],
                        ),
                    ),
                ],
                from_clause=[
                    pgast.RelRangeVar(relation=delcte),
                ],
            )

            data_select.where_clause = astutils.extend_binop(
                data_select.where_clause,
                pgast.NullTest(arg=del_select, negated=True),
            )

        pathctx.put_path_value_rvar(
            delcte.query, path_id.ptr_path(), target_rvar, env=ctx.env)

        # Record the effect of this removal in the relation overlay
        # context to ensure that references to the link in the result
        # of this DML statement yield the expected results.
        relctx.add_ptr_rel_overlay(
            mptrref, 'except', delcte, path_id=path_id,
            dml_stmts=ctx.dml_stmt_stack, ctx=ctx)
        toplevel.append_cte(delcte)
    else:
        delqry = None

    if shape_op is qlast.ShapeOp.SUBTRACT:
        if mptrref.dir_cardinality(rptr.direction).can_be_zero():
            # The pointer is OPTIONAL, no checks or further processing
            # is needed.
            return None, None
        else:
            # The pointer is REQUIRED, so we must take the result of
            # the subtraction produced by the "delcte" above, apply it
            # as a subtracting overlay, and re-compute the pointer relation
            # to see if there are any newly created empty sets.
            #
            # The actual work is done via raise_on_null injection performed
            # by "process_link_values()" below (hence "enforce_cardinality").
            #
            # The other part of this enforcement is in doing it when a
            # target is deleted and the link policy is ALLOW. This is
            # handled in _get_outline_link_trigger_proc_text in
            # pgsql/delta.py.

            # Turn `foo := <expr>` into just `foo`.
            ptr_ref_set = irast.Set(
                path_id=ir_set.path_id,
                path_scope_id=ir_set.path_scope_id,
                typeref=ir_set.typeref,
                rptr=ir_set.rptr,
            )

            with ctx.new() as subctx:
                subctx.ptr_rel_overlays = ctx.ptr_rel_overlays.copy()
                relctx.add_ptr_rel_overlay(
                    ptrref, 'except', delcte, path_id=path_id, ctx=subctx)

                check_cte, _ = process_link_values(
                    ir_stmt=ir_stmt,
                    ir_expr=ptr_ref_set,
                    dml_rvar=dml_cte_rvar,
                    source_typeref=source_typeref,
                    target_is_scalar=target_is_scalar,
                    enforce_cardinality=True,
                    dml_cte=dml_cte,
                    iterator=iterator,
                    ctx=subctx,
                )

                toplevel.append_cte(check_cte)

            return None, check_cte

    cols = [pgast.ColumnRef(name=[col]) for col in specified_cols]
    conflict_cols = ['source', 'target']

    if is_insert or target_is_scalar:
        conflict_clause = None
    elif (
        len(cols) == len(conflict_cols)
        and delqry is not None
        and not policy_ctx
    ):
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
            cols=[
                pgast.InsertTarget(name=downcast(col.name[0], str))
                for col in cols
            ],
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

    def register_overlays(
        overlay_cte: pgast.CommonTableExpr, octx: context.CompilerContextLevel
    ) -> None:
        assert isinstance(mptrref, irast.PointerRef)
        # Record the effect of this insertion in the relation overlay
        # context to ensure that references to the link in the result
        # of this DML statement yield the expected results.
        if shape_op is qlast.ShapeOp.APPEND and not target_is_scalar:
            # When doing an UPDATE with +=, we need to do an anti-join
            # based filter to filter out links that were already present
            # and have been re-added.
            relctx.add_ptr_rel_overlay(
                mptrref, 'filter', overlay_cte, dml_stmts=ctx.dml_stmt_stack,
                path_id=path_id.ptr_path(),
                ctx=octx)

        relctx.add_ptr_rel_overlay(
            mptrref, 'union', overlay_cte, dml_stmts=ctx.dml_stmt_stack,
            path_id=path_id.ptr_path(),
            ctx=octx)

    if policy_ctx:
        relctx.clone_ptr_rel_overlays(ctx=policy_ctx)
        register_overlays(data_cte, policy_ctx)

    register_overlays(updcte, ctx)

    return updcte, None


def process_link_values(
    *,
    ir_stmt: irast.MutatingStmt,
    ir_expr: irast.Set,
    dml_rvar: pgast.PathRangeVar,
    dml_cte: pgast.CommonTableExpr,
    source_typeref: irast.TypeRef,
    target_is_scalar: bool,
    enforce_cardinality: bool,
    iterator: Optional[pgast.IteratorCTE],
    ctx: context.CompilerContextLevel,
) -> Tuple[pgast.CommonTableExpr, List[str]]:
    """Produce a pointer relation for a given body element of an INSERT/UPDATE.

    Given an INSERT/UPDATE body shape element that mutates a MULTI pointer,
    produce a (source, target [, link properties]) relation as a CTE and
    return it along with a list of relation attribute names.

    Args:
        ir_stmt:
            IR of the DML statement.
        ir_set:
            IR of the INSERT/UPDATE body element.
        dml_rvar:
            The RangeVar over the SQL INSERT/UPDATE of the main relation
            of the object being updated.
        dml_cte:
            CTE representing the SQL INSERT or UPDATE to the main
            relation of the DML subject.
        source_typeref:
            An ir.TypeRef instance representing the specific type of an object
            being updated.
        target_is_scalar:
            True, if mutating a property, False if a link.
        enforce_cardinality:
            Whether an explicit empty set check should be generated.
            Used for REQUIRED pointers.
        iterator:
            IR and CTE representing the iterator range in the FOR clause
            of the EdgeQL DML statement (if present).

    Returns:
        A tuple containing the pointer relation CTE and a list of attribute
        names in it.
    """
    old_dml_count = len(ctx.dml_stmts)
    with ctx.newrel() as subrelctx:
        # For inserts, we need to use the main DML statement as the
        # iterator, while for updates, we need to use the DML range
        # CTE as the iterator (and so arrange for it to be passed in).
        # This is because, for updates, we need to execute any nested
        # DML once for each row in the range over all types, while
        # dml_cte contains just one subtype.
        if isinstance(ir_stmt, irast.InsertStmt):
            subrelctx.enclosing_cte_iterator = pgast.IteratorCTE(
                path_id=ir_stmt.subject.path_id, cte=dml_cte,
                parent=iterator)
        else:
            subrelctx.enclosing_cte_iterator = iterator
        row_query = subrelctx.rel

        merge_iterator(iterator, row_query, ctx=subrelctx)

        relctx.include_rvar(row_query, dml_rvar, pull_namespace=False,
                            path_id=ir_stmt.subject.path_id, ctx=subrelctx)
        subrelctx.path_scope[ir_stmt.subject.path_id] = row_query

        ir_rptr = ir_expr.rptr
        assert ir_rptr is not None
        ptrref = ir_rptr.ptrref
        if ptrref.material_ptr is not None:
            ptrref = ptrref.material_ptr
        assert isinstance(ptrref, irast.PointerRef)
        ptr_is_required = (
            not ptrref.dir_cardinality(ir_rptr.direction).can_be_zero()
        )

        with subrelctx.newscope() as sctx, sctx.subrel() as input_rel_ctx:
            input_rel = input_rel_ctx.rel
            input_rel_ctx.expr_exposed = False
            input_rel_ctx.volatility_ref = (
                lambda _stmt, _ctx: pathctx.get_path_identity_var(
                    row_query, ir_stmt.subject.path_id,
                    env=input_rel_ctx.env),)

            # Check if some nested Set provides a shape that is
            # visible here.
            shape_expr = ir_expr.shape_source or ir_expr
            # Register that this shape needs to be compiled for use by DML,
            # so that the values will be there for us to grab later.
            input_rel_ctx.shapes_needed_by_dml.add(shape_expr)

            if ptr_is_required and enforce_cardinality:
                input_rel_ctx.force_optional |= {ir_expr.path_id}

            dispatch.visit(ir_expr, ctx=input_rel_ctx)

    input_stmt: pgast.Query = input_rel

    input_rvar = pgast.RangeSubselect(
        subquery=input_rel,
        lateral=True,
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get('val')
        )
    )

    if len(ctx.dml_stmts) > old_dml_count:
        # If there were any nested inserts, we need to join them in.
        pathctx.put_rvar_path_bond(input_rvar, ir_stmt.subject.path_id)
    relctx.include_rvar(row_query, input_rvar,
                        path_id=ir_stmt.subject.path_id,
                        ctx=ctx)

    source_data: Dict[str, Tuple[irast.PathId, pgast.BaseExpr]] = {}

    if isinstance(input_stmt, pgast.SelectStmt) and input_stmt.op is not None:
        # UNION
        assert input_stmt.rarg
        input_stmt = input_stmt.rarg

    path_id = ir_expr.path_id

    target_ref: pgast.BaseExpr
    if shape_expr.shape:
        for element, _ in shape_expr.shape:
            if not element.path_id.is_linkprop_path():
                continue
            val = pathctx.get_rvar_path_value_var(
                input_rvar, element.path_id, env=ctx.env)
            rptr = element.path_id.rptr()
            assert isinstance(rptr, irast.PointerRef)
            actual_rptr = irtyputils.find_actual_ptrref(source_typeref, rptr)
            ptr_info = pg_types.get_ptrref_storage_info(actual_rptr)
            real_path_id = path_id.ptr_path().extend(ptrref=actual_rptr)
            source_data.setdefault(
                ptr_info.column_name, (real_path_id, val))

        if not target_is_scalar and 'target' not in source_data:
            target_ref = pathctx.get_rvar_path_identity_var(
                input_rvar, path_id, env=ctx.env)

    else:
        if target_is_scalar:
            target_ref = pathctx.get_rvar_path_value_var(
                input_rvar, path_id, env=ctx.env)
            target_ref = output.output_as_value(target_ref, env=ctx.env)
        else:
            target_ref = pathctx.get_rvar_path_identity_var(
                input_rvar, path_id, env=ctx.env)

    if isinstance(ir_stmt, irast.UpdateStmt) and not target_is_scalar:
        actual_ptrref = irtyputils.find_actual_ptrref(source_typeref, ptrref)
        target_ref = check_update_type(
            target_ref,
            input_rvar,
            is_subquery=False,
            ir_stmt=ir_stmt,
            ir_set=ir_expr,
            subject_typeref=source_typeref,
            shape_ptrref=ptrref,
            actual_ptrref=actual_ptrref,
            ctx=ctx,
        )

    if ptr_is_required and enforce_cardinality:
        target_ref = pgast.FuncCall(
            name=('edgedb', 'raise_on_null'),
            args=[
                target_ref,
                pgast.StringConstant(val='not_null_violation'),
                pgast.NamedFuncArg(
                    name='msg',
                    val=pgast.StringConstant(val='missing value'),
                ),
                pgast.NamedFuncArg(
                    name='column',
                    val=pgast.StringConstant(val=str(ptrref.id)),
                ),
            ],
        )

    source_data['target'] = (path_id, target_ref)

    row_query.target_list.append(
        pgast.ResTarget(
            name='source',
            val=pathctx.get_rvar_path_identity_var(
                dml_rvar,
                ir_stmt.subject.path_id,
                env=ctx.env,
            ),
        ),
    )

    specified_cols = ['source']
    for col, (col_path_id, expr) in source_data.items():
        row_query.target_list.append(
            pgast.ResTarget(
                val=expr,
                name=col,
            ),
        )
        specified_cols.append(col)
        # XXX: This is dodgy. Do we need to do the dynamic rvar thing?
        # XXX: And can we make defaults work?
        pathctx._put_path_output_var(
            row_query, col_path_id, aspect='value',
            var=pgast.ColumnRef(name=[col]),
            env=ctx.env,
        )

    link_rows = pgast.CommonTableExpr(
        query=row_query,
        name=ctx.env.aliases.get(hint='r'),
    )

    return link_rows, specified_cols
