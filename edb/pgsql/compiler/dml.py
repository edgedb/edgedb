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

from typing import (
    Optional,
    Tuple,
    Union,
    Mapping,
    Sequence,
    Collection,
    Dict,
    List,
    NamedTuple,
)

import immutables as immu

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
from . import enums as pgce
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

    clauses.compile_volatile_bindings(ir_stmt, ctx=ctx)

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

        # Only update/delete concrete types. (Except in the degenerate
        # corner case where there are none, in which case keep using
        # everything so as to avoid needing a more complex special case.)
        concrete_typerefs = [t for t in typerefs if not t.is_abstract]
        if concrete_typerefs:
            typerefs = concrete_typerefs

    dml_map = {}

    for typeref in typerefs:
        if typeref.union:
            continue
        if (
            isinstance(typeref.name_hint, sn.QualName)
            and typeref.name_hint.module in ('sys', 'cfg')
        ):
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
            name=ctx.env.aliases.get(hint='melse'),
            for_dml_stmt=ctx.get_current_dml_stmt(),
        )
        dml_rvar = relctx.rvar_for_rel(dml_cte, ctx=ctx)
        else_cte = (dml_cte, dml_rvar)

    put_iterator_bond(ctx.enclosing_cte_iterator, ctx.rel)

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
        put_iterator_bond(ctx.enclosing_cte_iterator, qry.larg)

        union_cte = pgast.CommonTableExpr(
            query=qry.larg,
            name=ctx.env.aliases.get(hint='ma'),
            for_dml_stmt=ctx.get_current_dml_stmt(),
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

    relation = relctx.range_for_typeref(
        typeref,
        target_path_id,
        for_mutation=True,
        ctx=ctx,
    )
    assert isinstance(relation, pgast.RelRangeVar), (
        "spurious overlay on DML target"
    )

    dml_stmt: pgast.InsertStmt | pgast.SelectStmt | pgast.DeleteStmt
    if isinstance(ir_stmt, irast.InsertStmt):
        dml_stmt = pgast.InsertStmt(relation=relation)
    elif isinstance(ir_stmt, irast.UpdateStmt):
        # We generate a Select as the initial statement for an update,
        # since the contents select is the query that needs to join
        # the range and include policy filters and because we
        # sometimes end up not needing an UPDATE anyway (if it only
        # touches link tables).
        dml_stmt = pgast.SelectStmt()
    elif isinstance(ir_stmt, irast.DeleteStmt):
        dml_stmt = pgast.DeleteStmt(relation=relation)
    else:
        raise AssertionError(f'unexpected DML IR: {ir_stmt!r}')

    pathctx.put_path_value_rvar(dml_stmt, target_path_id, relation)
    pathctx.put_path_source_rvar(dml_stmt, target_path_id, relation)
    # Skip the path bond for inserts, since it doesn't help and
    # interferes when inserting in an UNLESS CONFLICT ELSE
    if not isinstance(ir_stmt, irast.InsertStmt):
        pathctx.put_path_bond(dml_stmt, target_path_id)

    dml_cte = pgast.CommonTableExpr(
        query=dml_stmt,
        name=ctx.env.aliases.get(hint='m'),
        for_dml_stmt=ir_stmt,
    )

    # Due to the fact that DML statements are structured
    # as a flat list of CTEs instead of nested range vars,
    # the top level path scope must be empty.  The necessary
    # range vars will be injected explicitly in all rels that
    # need them.
    ctx.path_scope.maps.clear()

    skip_rel = (
        isinstance(ir_stmt, irast.UpdateStmt) and ir_stmt.sql_mode_link_only
    )

    if range_rvar is not None:
        relctx.pull_path_namespace(
            target=dml_stmt, source=range_rvar, ctx=ctx)

        # Auxiliary relations are always joined via the WHERE
        # clause due to the structure of the UPDATE/DELETE SQL statements.
        assert isinstance(dml_stmt, (pgast.SelectStmt, pgast.DeleteStmt))
        if not skip_rel:
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
                pathctx.put_path_value_rvar(sctx.rel, target_path_id, relation)
                pathctx.put_path_source_rvar(
                    sctx.rel, target_path_id, relation
                )

                val = clauses.compile_filter_clause(
                    pol_expr.expr, pol_expr.cardinality, ctx=sctx)
            sctx.rel.target_list.append(pgast.ResTarget(val=val))

            dml_stmt.where_clause = astutils.extend_binop(
                dml_stmt.where_clause, sctx.rel
            )

        # SELECT has "FROM", while DELETE has "USING".
        if isinstance(dml_stmt, pgast.SelectStmt):
            if not skip_rel:
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


def put_iterator_bond(
    iterator: Optional[pgast.IteratorCTE],
    select: pgast.Query,
) -> None:
    if iterator:
        pathctx.put_path_bond(
            select, iterator.path_id, iterator=iterator.iterator_bond)


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

        put_iterator_bond(iterator, select)
        relctx.include_rvar(
            select, iterator_rvar,
            aspects=(pgce.PathAspect.VALUE, iterator.aspect) + (
                (pgce.PathAspect.SOURCE,)
                if iterator.path_id.is_objtype_path() else
                ()
            ),
            path_id=iterator.path_id,
            overwrite_path_rvar=True,
            ctx=ctx)
        # We need nested iterators to re-export their enclosing
        # iterators in some cases that the path_id_mask blocks
        # otherwise.
        select.path_id_mask.discard(iterator.path_id)

        # HACK: This is a hack for triggers, to stick __old__ in
        # as a reference to __new__'s identity for updates/deletes
        for other_path, aspect in iterator.other_paths:
            pathctx.put_path_rvar(
                select, other_path, iterator_rvar, aspect=aspect
            )


def fini_dml_stmt(
    ir_stmt: irast.MutatingStmt,
    wrapper: pgast.Query,
    parts: DMLParts,
    *,
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
            ir_stmt.subject.typeref, context.OverlayOp.UNION, cte,
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

            # When the base type is abstract, there will be no CTE for it,
            # so the overlays of children types have to apply to the whole
            # ancestry tree.
            if base_typeref.is_abstract:
                stop_ref = None

            # The overlay for update is in two parts:
            # First, filter out objects that have been updated, then union them
            # back in. (If we just did union, we'd see the old values also.)
            relctx.add_type_rel_overlay(
                typeref, context.OverlayOp.FILTER, cte,
                stop_ref=stop_ref,
                dml_stmts=dml_stack, path_id=ir_stmt.subject.path_id, ctx=ctx)
            relctx.add_type_rel_overlay(
                typeref, context.OverlayOp.UNION, cte,
                stop_ref=stop_ref,
                dml_stmts=dml_stack, path_id=ir_stmt.subject.path_id, ctx=ctx)

        process_update_conflicts(ir_stmt=ir_stmt, dml_parts=parts, ctx=ctx)
    elif isinstance(ir_stmt, irast.DeleteStmt):
        base_typeref = ir_stmt.subject.typeref.real_material_type

        for typeref, (cte, _) in parts.dml_ctes.items():
            # see above, re: stop_ref
            if typeref.id == base_typeref.id:
                cte = union_cte
                stop_ref = None
            else:
                stop_ref = base_typeref

            relctx.add_type_rel_overlay(
                typeref, context.OverlayOp.EXCEPT, cte,
                stop_ref=stop_ref,
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
                clauses.setup_iterator_volatility(target_ir_set, ctx=wctx)
                range_stmt.where_clause = astutils.extend_binop(
                    range_stmt.where_clause,
                    clauses.compile_filter_clause(
                        ir_qual_expr, ir_qual_card, ctx=wctx))

        range_stmt.path_id_mask.discard(target_ir_set.path_id)
        pathctx.put_path_bond(range_stmt, target_ir_set.path_id)

        range_cte = pgast.CommonTableExpr(
            query=range_stmt,
            name=ctx.env.aliases.get('range'),
            for_dml_stmt=ctx.get_current_dml_stmt(),
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
            parent=last_iterator, iterator_bond=True)

    with ctx.newrel() as ictx:
        ictx.scope_tree = ctx.scope_tree
        ictx.path_scope[iterator_set.path_id] = ictx.rel

        # Correlate with enclosing iterators
        merge_iterator(last_iterator, ictx.rel, ctx=ictx)
        clauses.setup_iterator_volatility(last_iterator, ctx=ictx)

        clauses.compile_iterator_expr(
            ictx.rel, iterator_set, is_dml=True, ctx=ictx)
        if iterator_set.path_id.is_objtype_path():
            relgen.ensure_source_rvar(iterator_set, ictx.rel, ctx=ictx)
        ictx.rel.path_id = iterator_set.path_id
        pathctx.put_path_bond(ictx.rel, iterator_set.path_id, iterator=True)
        iterator_cte = pgast.CommonTableExpr(
            query=ictx.rel,
            name=ctx.env.aliases.get('iter'),
            for_dml_stmt=ctx.get_current_dml_stmt(),
        )
        ictx.toplevel_stmt.append_cte(iterator_cte)

    ctx.dml_stmts[iterator_set] = iterator_cte

    return pgast.IteratorCTE(
        path_id=iterator_set.path_id,
        cte=iterator_cte,
        parent=last_iterator,
        iterator_bond=True,
    )


def _mk_dynamic_get_path(
    ptr_map: Dict[sn.Name, pgast.BaseExpr],
    typeref: irast.TypeRef,
    fallback_rvar: Optional[pgast.PathRangeVar] = None,
) -> pgast.DynamicRangeVarFunc:
    """A dynamic rvar function for insert/update.

    It returns values out of a select purely based on material rptr,
    as if it was a base relation. This is to make it easy for access
    policies to operate on the results.
    """

    def dynamic_get_path(
        rel: pgast.Query, path_id: irast.PathId, *,
        flavor: str,
        aspect: str, env: context.Environment
    ) -> Optional[pgast.BaseExpr | pgast.PathRangeVar]:
        if (
            flavor != 'normal'
            or aspect not in (
                pgce.PathAspect.VALUE, pgce.PathAspect.IDENTITY
            )
        ):
            return None
        if rptr := path_id.rptr():
            if ret := ptr_map.get(rptr.real_material_ptr.name):
                return ret
            if rptr.real_material_ptr.shortname.name == '__type__':
                return astutils.compile_typeref(typeref)
        # If a fallback rvar is specified, defer to that.
        # This is used in rewrites to go back to the original
        if fallback_rvar:
            return fallback_rvar
        if not rptr:
            raise LookupError('only pointers appear in insert fallback')
        # Properties that aren't specified are {}
        return pgast.NullConstant()
    return dynamic_get_path


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

    # The main INSERT query of this statement will always be
    # present to insert at least the `id` property.
    insert_stmt = insert_cte.query
    assert isinstance(insert_stmt, pgast.InsertStmt)

    typeref = ir_stmt.subject.typeref.real_material_type

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
            ctx=ctx,
        )

    iterator = ctx.enclosing_cte_iterator
    inner_iterator = on_conflict_fake_iterator or iterator

    # ptr_map needs to be set up in advance of compiling the shape
    # because defaults might reference earlier pointers.
    ptr_map: Dict[sn.Name, pgast.BaseExpr] = {}

    # Use a dynamic rvar to return values out of the select purely
    # based on material rptr, as if it was a base relation.
    # This is to make it easy for access policies to operate on the result
    # of the INSERT.
    fallback_rvar = pgast.DynamicRangeVar(
        dynamic_get_path=_mk_dynamic_get_path(ptr_map, typeref))
    pathctx.put_path_source_rvar(
        select, ir_stmt.subject.path_id, fallback_rvar
    )
    pathctx.put_path_value_rvar(select, ir_stmt.subject.path_id, fallback_rvar)

    # compile contents CTE
    elements: List[Tuple[irast.SetE[irast.Pointer], irast.BasePointerRef]] = []
    for shape_el, shape_op in ir_stmt.subject.shape:
        assert shape_op is qlast.ShapeOp.ASSIGN

        # If the shape element is a linkprop, we do nothing.
        # It will be picked up by the enclosing DML.
        if shape_el.path_id.is_linkprop_path():
            continue

        ptrref = shape_el.expr.ptrref
        if ptrref.material_ptr is not None:
            ptrref = ptrref.material_ptr
        assert shape_el.expr.expr
        elements.append((shape_el, ptrref))

    external_inserts = process_insert_shape(
        ir_stmt, select, ptr_map, elements, iterator, inner_iterator, ctx
    )
    single_external = [
        ir for ir in external_inserts
        if ir.expr.dir_cardinality.is_single()
    ]

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
        name=ctx.env.aliases.get('ins_contents'),
        for_dml_stmt=ctx.get_current_dml_stmt(),
    )
    ctx.toplevel_stmt.append_cte(contents_cte)
    contents_rvar = relctx.rvar_for_rel(contents_cte, ctx=ctx)

    rewrites = ir_stmt.rewrites and ir_stmt.rewrites.by_type.get(typeref)

    pol_expr = ir_stmt.write_policies.get(typeref.id)
    pol_ctx = None
    if pol_expr or rewrites or single_external:
        # Create a context for handling policies/rewrites that we will
        # use later. We do this in advance so that the link update code
        # can populate overlay fields in it.
        with ctx.new() as pol_ctx:
            pass

    needs_insert_on_conflict = bool(
        ir_stmt.on_conflict and not on_conflict_fake_iterator)

    # The first serious bit of trickiness: if there are rewrites, the link
    # table updates need to be done *before* we compute the rewrites, since
    # the rewrites might refer to them.
    #
    # However, we can't unconditionally do it like this, because we
    # want to be able to use ON CONFLICT to implement UNLESS CONFLICT
    # ON when possible, and in that case the link table operations
    # need to be done after the *actual insert*, because it is the actual
    # insert that filters out conflicting rows. (This also means that we
    # can't use ON CONFLICT if there are rewrites.)
    #
    # Similar issues obtain with access policies: we can't use ON
    # CONFLICT if there are access policies, since we can't "see" all
    # possible conflicting objects.
    #
    # We *also* need link tables to go first if there are any single links
    # with link properties. We do the actual computation for those in a link
    # table and then join it in to the main table, where it is duplicated.
    link_ctes = []

    def _update_link_tables(inp_cte: pgast.CommonTableExpr) -> None:
        # Process necessary updates to the link tables.
        for shape_el in external_inserts:
            link_cte, check_cte = process_link_update(
                ir_stmt=ir_stmt,
                ir_set=shape_el,
                dml_cte=inp_cte,
                source_typeref=typeref,
                iterator=iterator,
                policy_ctx=pol_ctx,
                ctx=ctx,
            )
            if link_cte:
                link_ctes.append(link_cte)
            if check_cte is not None:
                ctx.env.check_ctes.append(check_cte)

    if not needs_insert_on_conflict:
        _update_link_tables(contents_cte)

    # compile rewrites CTE
    if rewrites or single_external:
        rewrites = rewrites or {}
        assert not needs_insert_on_conflict

        assert pol_ctx
        with pol_ctx.reenter(), pol_ctx.newrel() as rctx:
            # Pull in ptr rel overlays, so we can see the pointers
            merge_overlays_globally((ir_stmt,), ctx=rctx)

            contents_cte, contents_rvar = process_insert_rewrites(
                ir_stmt,
                contents_cte=contents_cte,
                iterator=iterator,
                inner_iterator=inner_iterator,
                rewrites=rewrites,
                single_external=single_external,
                elements=elements,
                ctx=rctx,
            )

    # Populate the real insert statement based on the select we generated
    insert_stmt.cols = [
        pgast.InsertTarget(name=name)
        for value in contents_cte.query.target_list
        # Filter out generated columns; only keep concrete ones
        if '~' not in (name := not_none(value.name))
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
        name=ctx.env.aliases.get('ins'),
        for_dml_stmt=ctx.get_current_dml_stmt(),
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

    # If there is an ON CONFLICT clause, insert the CTEs now so that the
    # link inserts can depend on it. Otherwise we have the link updates
    # depend on the contents cte so that policies can operate before
    # doing any actual INSERTs.
    if needs_insert_on_conflict:
        ctx.toplevel_stmt.append_cte(real_insert_cte)
        ctx.toplevel_stmt.append_cte(insert_cte)
        link_op_cte = insert_cte
    else:
        link_op_cte = contents_cte

    if needs_insert_on_conflict:
        _update_link_tables(link_op_cte)

    if pol_expr:
        assert pol_ctx
        assert not needs_insert_on_conflict
        with pol_ctx.reenter():
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
            ctx=ctx,
        )


def process_insert_rewrites(
    ir_stmt: irast.InsertStmt,
    *,
    contents_cte: pgast.CommonTableExpr,
    iterator: Optional[pgast.IteratorCTE],
    inner_iterator: Optional[pgast.IteratorCTE],
    rewrites: irast.RewritesOfType,
    single_external: List[irast.SetE[irast.Pointer]],
    elements: Sequence[Tuple[irast.SetE[irast.Pointer], irast.BasePointerRef]],
    ctx: context.CompilerContextLevel,
) -> tuple[pgast.CommonTableExpr, pgast.PathRangeVar]:
    typeref = ir_stmt.subject.typeref.real_material_type

    subject_path_id = ir_stmt.subject.path_id
    rew_stmt = ctx.rel

    # Use the original contents as the iterator.
    inner_iterator = pgast.IteratorCTE(
        path_id=subject_path_id,
        cte=contents_cte,
        parent=inner_iterator,
        other_paths=(
            (subject_path_id, pgce.PathAspect.IDENTITY),
            (subject_path_id, pgce.PathAspect.VALUE),
            (subject_path_id, pgce.PathAspect.SOURCE),
        ),
    )

    # compile rewrite shape
    rewrite_elements = list(rewrites.values())
    nptr_map: Dict[sn.Name, pgast.BaseExpr] = {}
    process_insert_shape(
        ir_stmt,
        rew_stmt,
        nptr_map,
        rewrite_elements,
        iterator,
        inner_iterator,
        ctx,
        force_optional=True,
    )

    iterator_rvar = pathctx.get_path_rvar(
        rew_stmt, path_id=subject_path_id, aspect=pgce.PathAspect.VALUE
    )
    fallback_rvar = pgast.DynamicRangeVar(
        dynamic_get_path=_mk_dynamic_get_path(nptr_map, typeref, iterator_rvar)
    )
    pathctx.put_path_source_rvar(rew_stmt, subject_path_id, fallback_rvar)
    pathctx.put_path_value_rvar(rew_stmt, subject_path_id, fallback_rvar)

    # If there are any single links that were compiled externally,
    # populate the field from the link overlays.
    handled = set(rewrites)
    for ext_ir in single_external:
        handled.add(ext_ir.expr.ptrref.shortname.name)
        with ctx.subrel() as ectx:
            ext_rvar = relctx.new_pointer_rvar(
                ext_ir, link_bias=True, src_rvar=iterator_rvar, ctx=ectx)
            relctx.include_rvar(ectx.rel, ext_rvar, ext_ir.path_id, ctx=ectx)
            # Make the subquery output the target
            pathctx.get_path_value_output(
                ectx.rel, ext_ir.path_id, env=ctx.env)

        ptr_info = pg_types.get_ptrref_storage_info(
            ext_ir.expr.ptrref, resolve_type=True, link_bias=False)
        rew_stmt.target_list.append(pgast.ResTarget(
            name=ptr_info.column_name, val=ectx.rel))
        nptr_map[ext_ir.expr.ptrref.real_material_ptr.name] = ectx.rel

    # Pull in pointers that were not rewritten
    not_rewritten = {
        (e, ptrref) for e, ptrref in elements
        if ptrref.shortname.name not in handled
    }
    for e, ptrref in not_rewritten:
        # FIXME: Duplicates some with process_insert_shape
        ptr_info = pg_types.get_ptrref_storage_info(
            ptrref, resolve_type=True, link_bias=False)
        if ptr_info.table_type == 'ObjectType':
            val = pathctx.get_path_var(
                rew_stmt,
                e.path_id,
                aspect=pgce.PathAspect.VALUE,
                env=ctx.env,
            )
            val = output.output_as_value(val, env=ctx.env)
            rew_stmt.target_list.append(pgast.ResTarget(
                name=ptr_info.column_name, val=val))

    # construct the CTE
    pathctx.put_path_bond(rew_stmt, ir_stmt.subject.path_id)
    rewrites_cte = pgast.CommonTableExpr(
        query=rew_stmt,
        name=ctx.env.aliases.get('ins_rewrites'),
        for_dml_stmt=ctx.get_current_dml_stmt(),
    )
    ctx.toplevel_stmt.append_cte(rewrites_cte)
    rewrites_rvar = relctx.rvar_for_rel(rewrites_cte, ctx=ctx)

    return rewrites_cte, rewrites_rvar


def process_insert_shape(
    ir_stmt: irast.InsertStmt,
    select: pgast.SelectStmt,
    ptr_map: Dict[sn.Name, pgast.BaseExpr],
    elements: Sequence[Tuple[irast.SetE[irast.Pointer], irast.BasePointerRef]],
    iterator: Optional[pgast.IteratorCTE],
    inner_iterator: Optional[pgast.IteratorCTE],
    ctx: context.CompilerContextLevel,
    force_optional: bool=False,
) -> List[irast.SetE[irast.Pointer]]:
    # Compile the shape
    external_inserts = []

    with ctx.newrel() as subctx:
        subctx.enclosing_cte_iterator = inner_iterator

        subctx.rel = select
        subctx.expr_exposed = False

        inner_iterator_id = None
        if inner_iterator is not None:
            subctx.path_scope = ctx.path_scope.new_child()
            merge_iterator(inner_iterator, select, ctx=subctx)
            inner_iterator_id = relctx.get_path_var(
                select, inner_iterator.path_id,
                aspect=inner_iterator.aspect,
                ctx=ctx)

        # Process the Insert IR and separate links that go
        # into the main table from links that are inserted into
        # a separate link table.
        for element, ptrref in elements:

            ptr_info = pg_types.get_ptrref_storage_info(
                ptrref, resolve_type=True, link_bias=False)
            link_ptr_info = pg_types.get_ptrref_storage_info(
                ptrref, resolve_type=False, link_bias=True)

            # First, process all local link inserts. Single link with
            # link properties are not processed here; we compile those
            # in link tables and then select those back into the main
            # table as a rewrite.
            if not link_ptr_info and ptr_info.table_type == 'ObjectType':
                compile_insert_shape_element(
                    element,
                    ir_stmt=ir_stmt,
                    iterator_id=inner_iterator_id,
                    force_optional=force_optional,
                    ctx=subctx,
                )

                insvalue = pathctx.get_path_value_var(
                    subctx.rel, element.path_id, env=ctx.env)

                if irtyputils.is_tuple(element.typeref):
                    # Tuples require an explicit cast.
                    insvalue = pgast.TypeCast(
                        arg=output.output_as_value(insvalue, env=ctx.env),
                        type_name=pgast.TypeName(
                            name=ptr_info.column_type,
                        ),
                    )

                ptr_map[ptrref.name] = insvalue
                select.target_list.append(pgast.ResTarget(
                    name=ptr_info.column_name, val=insvalue))

            if link_ptr_info and link_ptr_info.table_type == 'link':
                external_inserts.append(element)

        put_iterator_bond(iterator, select)

    for aspect in (pgce.PathAspect.VALUE, pgce.PathAspect.IDENTITY):
        pathctx._put_path_output_var(
            select, ir_stmt.subject.path_id, aspect=aspect,
            var=pgast.ColumnRef(name=['id']),
        )

    return external_inserts


def compile_insert_shape_element(
    shape_el: irast.SetE[irast.Pointer],
    *,
    ir_stmt: irast.MutatingStmt,
    iterator_id: Optional[pgast.BaseExpr],
    force_optional: bool,
    ctx: context.CompilerContextLevel,
) -> None:

    with ctx.new() as insvalctx:
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

        if shape_el.expr.dir_cardinality.can_be_zero() or force_optional:
            # If the element can be empty, compile it in a subquery to force it
            # to be NULL.
            value = relgen.set_as_subquery(
                shape_el, as_value=True, ctx=insvalctx)
            pathctx.put_path_value_var(insvalctx.rel, shape_el.path_id, value)
        else:
            dispatch.visit(shape_el, ctx=insvalctx)


def merge_overlays_globally(
    ir_stmts: Collection[irast.MutatingLikeStmt | None],
    *,
    ctx: context.CompilerContextLevel,
) -> None:
    ctx.rel_overlays = ctx.rel_overlays.copy()

    type_overlay = ctx.rel_overlays.type.get(None, immu.Map())
    ptr_overlay = ctx.rel_overlays.ptr.get(None, immu.Map())

    for ir_stmt in ir_stmts:
        if not ir_stmt:
            continue
        for k, v in ctx.rel_overlays.type.get(ir_stmt, immu.Map()).items():
            els = set(type_overlay.get(k, ()))
            n_els = (
                type_overlay.get(k, ()) + tuple(e for e in v if e not in els)
            )
            type_overlay = type_overlay.set(k, n_els)
        for k2, v2 in ctx.rel_overlays.ptr.get(ir_stmt, immu.Map()).items():
            els = set(ptr_overlay.get(k2, ()))
            n_els = (
                ptr_overlay.get(k2, ()) + tuple(e for e in v2 if e not in els)
            )
            ptr_overlay = ptr_overlay.set(k2, n_els)

    ctx.rel_overlays.type = ctx.rel_overlays.type.set(None, type_overlay)
    ctx.rel_overlays.ptr = ctx.rel_overlays.ptr.set(None, ptr_overlay)


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
        merge_overlays_globally((ir_stmt,), ctx=ictx)

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
                name=astutils.edgedb_func('raise_on_null', ctx=ctx),
                args=[
                    pgast.FuncCall(
                        name=('nullif',),
                        args=[a, pgast.BooleanConstant(val=True)],
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
            no_allow_expr = pgast.BooleanConstant(val=True)

        # deny
        deny_exprs = (cond for _, cond in deny)

        # message
        if isinstance(ir_stmt, irast.InsertStmt):
            op = 'insert'
        else:
            op = 'update'
        msg = f'access policy violation on {op} of {typeref.name_hint}'

        allow_hints = (pol.error_msg for pol, _ in allow if pol.error_msg)
        allow_hint = '; '.join(allow_hints)

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
            for_dml_stmt=ctx.get_current_dml_stmt(),
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
                        pgast.StringConstant(val='; '),
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
        name=">",
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
    if on_conflict.else_fail:
        return False

    if on_conflict.always_check or ir_stmt.conflict_checks:
        return True

    # We can't use ON CONFLICT if there are access policies
    # on the type, since UNLESS CONFLICT only should avoid
    # conflicts with objects that are visible.
    type_id = ir_stmt.subject.typeref.real_material_type.id
    if (
        (type_id, True) in ctx.env.type_rewrites
        or (type_id, False) in ctx.env.type_rewrites
    ):
        return True

    # We can't use ON CONFLICT if there are rewrites on the type
    # because rewrites might reference multi pointers, which means
    # we need to execute link operations before the final INSERT.
    if ir_stmt.rewrites and ir_stmt.rewrites.by_type:
        return True

    for shape_el, _ in ir_stmt.subject.shape:
        ptrref = shape_el.expr.ptrref
        ptr_info = pg_types.get_ptrref_storage_info(
            ptrref, resolve_type=True, link_bias=False)

        # We need to generate a conflict CTE if we have a DML containing
        # pointer stored in the object itself
        if (
            ptr_info.table_type == 'ObjectType'
            and shape_el.expr.expr
            and irutils.contains_dml(
                shape_el.expr.expr,
                skip_bindings=True,
                skip_nodes=(ir_stmt.subject,),
            )
        ):
            return True

        # If there are any single links with link properties, we need
        # a conflict CTE, since the link tables have to go before the
        # insert.
        if ptr_info.table_type == 'ObjectType':
            link_ptr_info = pg_types.get_ptrref_storage_info(
                ptrref, resolve_type=True, link_bias=True)
            if link_ptr_info:
                return True

    return False


def compile_insert_else_body(
        stmt: Optional[pgast.InsertStmt],
        ir_stmt: irast.MutatingStmt,
        on_conflict: irast.OnConflictClause,
        enclosing_cte_iterator: Optional[pgast.IteratorCTE],
        else_cte_rvar: Optional[
            Tuple[pgast.CommonTableExpr, pgast.PathRangeVar]],
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
        clauses.setup_iterator_volatility(enclosing_cte_iterator, ctx=ictx)

        dispatch.compile(else_select, ctx=ictx)
        pathctx.put_path_id_map(ictx.rel, subject_id, else_select.path_id)
        # Discard else_branch from the path_id_mask to prevent subject_id
        # from being masked.
        ictx.rel.path_id_mask.discard(else_select.path_id)

        else_select_cte = pgast.CommonTableExpr(
            query=ictx.rel,
            name=ctx.env.aliases.get('else'),
            for_dml_stmt=ctx.get_current_dml_stmt(),
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
            clauses.setup_iterator_volatility(enclosing_cte_iterator, ctx=ictx)

            # Set up a dummy path to represent all of the rows
            # that *aren't* being filtered out
            dummy_pathid = irast.PathId.new_dummy(ctx.env.aliases.get('dummy'))
            with ictx.subrel() as dctx:
                dummy_q = dctx.rel
                relctx.create_iterator_identity_for_path(
                    dummy_pathid, dummy_q, ctx=dctx)
            dummy_rvar = relctx.rvar_for_rel(
                dummy_q, lateral=True, ctx=ictx)
            relctx.include_rvar(ictx.rel, dummy_rvar,
                                path_id=dummy_pathid, ctx=ictx)

            with ictx.subrel() as subrelctx:
                subrel = subrelctx.rel
                relctx.include_rvar(subrel, else_select_rvar,
                                    path_id=subject_id, ctx=ictx)

            # Do the anti-join
            iter_path_id = (
                enclosing_cte_iterator.path_id if
                enclosing_cte_iterator else None)
            aspect = (
                enclosing_cte_iterator.aspect if enclosing_cte_iterator
                else pgce.PathAspect.IDENTITY
            )
            relctx.anti_join(ictx.rel, subrel, iter_path_id,
                             aspect=aspect, ctx=ctx)

            # Package it up as a CTE
            anti_cte = pgast.CommonTableExpr(
                query=ictx.rel,
                name=ctx.env.aliases.get('non_conflict'),
                for_dml_stmt=ctx.get_current_dml_stmt(),
            )
            ictx.toplevel_stmt.append_cte(anti_cte)
            anti_cte_iterator = pgast.IteratorCTE(
                path_id=dummy_pathid, cte=anti_cte,
                parent=ictx.enclosing_cte_iterator,
                iterator_bond=True
            )

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
    merge_overlays_globally((else_fail,), ctx=ctx)

    # Do some work so that we aren't looking at the existing on-disk
    # data, just newly data created data.
    overlays_map = ctx.rel_overlays.type.get(None, immu.Map())
    for k, overlays in overlays_map.items():
        # Strip out filters, which we don't care about in this context
        overlays = tuple([
            (k, r, p)
            for k, r, p in overlays
            if k != context.OverlayOp.FILTER
        ])
        # Drop the initial set
        if overlays and overlays[0][0] == context.OverlayOp.UNION:
            overlays = (
                (context.OverlayOp.REPLACE, *overlays[0][1:]),
                *overlays[1:]
            )
        overlays_map = overlays_map.set(k, overlays)

    ctx.rel_overlays.type = ctx.rel_overlays.type.set(None, overlays_map)

    assert on_conflict.constraint
    cid = common.get_constraint_raw_name(on_conflict.constraint.id)
    maybe_raise = pgast.FuncCall(
        name=astutils.edgedb_func('raise', ctx=ctx),
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
    toplevel = ctx.toplevel_stmt

    put_iterator_bond(ctx.enclosing_cte_iterator, contents_select)

    assert dml_parts.range_cte
    iterator = pgast.IteratorCTE(
        path_id=ir_stmt.subject.path_id,
        cte=dml_parts.range_cte,
        parent=ctx.enclosing_cte_iterator,
    )

    with ctx.newscope() as subctx:
        # It is necessary to process the expressions in
        # the UpdateStmt shape body in the context of the
        # UPDATE statement so that references to the current
        # values of the updated object are resolved correctly.
        subctx.parent_rel = contents_select
        subctx.expr_exposed = False
        subctx.enclosing_cte_iterator = iterator

        clauses.setup_iterator_volatility(iterator, ctx=subctx)

        # compile contents CTE
        elements = [
            (shape_el, shape_el.expr.ptrref, shape_op)
            for shape_el, shape_op in ir_stmt.subject.shape
            if shape_op != qlast.ShapeOp.MATERIALIZE
        ]

        values, external_updates, ptr_map = process_update_shape(
            ir_stmt, contents_select, elements, typeref, subctx
        )

        relation = contents_select.from_clause[0]
        assert isinstance(relation, pgast.PathRangeVar)

        # Use a dynamic rvar to return values out of the select purely
        # based on material rptr, as if it was a base relation (and to
        # fall back to the base relation if the value wasn't updated.)
        fallback_rvar = pgast.DynamicRangeVar(
            dynamic_get_path=_mk_dynamic_get_path(ptr_map, typeref, relation),
        )
        pathctx.put_path_source_rvar(
            contents_select,
            ir_stmt.subject.path_id,
            fallback_rvar,
        )
        pathctx.put_path_value_rvar(
            contents_select,
            ir_stmt.subject.path_id,
            fallback_rvar,
        )

    update_stmt = None

    single_external = [
        ir for ir, _ in external_updates
        if ir.expr.dir_cardinality.is_single()
    ]

    rewrites = ir_stmt.rewrites and ir_stmt.rewrites.by_type.get(typeref)

    pol_expr = ir_stmt.write_policies.get(typeref.id)
    pol_ctx = None
    if pol_expr or rewrites or single_external:
        # Create a context for handling policies/rewrites that we will
        # use later. We do this in advance so that the link update code
        # can populate overlay fields in it.
        with ctx.new() as pol_ctx:
            pass

    no_update = not values and not rewrites and not single_external
    if no_update:
        # No updates directly to the set target table,
        # so convert the UPDATE statement into a SELECT.
        update_cte.query = contents_select
        contents_cte = update_cte
    else:
        contents_cte = pgast.CommonTableExpr(
            query=contents_select,
            name=ctx.env.aliases.get("upd_contents"),
            for_dml_stmt=ctx.get_current_dml_stmt(),
        )
    toplevel.append_cte(contents_cte)

    # Process necessary updates to the link tables.
    # We do link tables before we do the main update so that
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

    if not no_update:
        table_relation = contents_select.from_clause[0]
        assert isinstance(table_relation, pgast.RelRangeVar)
        range_relation = contents_select.from_clause[1]
        assert isinstance(range_relation, pgast.PathRangeVar)

        contents_rvar = relctx.rvar_for_rel(contents_cte, ctx=ctx)
        subject_path_id = ir_stmt.subject.path_id

        # Compile rewrites CTE
        if rewrites or single_external:
            rewrites = rewrites or {}
            assert pol_ctx
            with pol_ctx.reenter(), pol_ctx.new() as rctx:
                merge_overlays_globally((ir_stmt,), ctx=rctx)
                contents_cte, contents_rvar, values = process_update_rewrites(
                    ir_stmt,
                    typeref=typeref,
                    contents_cte=contents_cte,
                    contents_rvar=contents_rvar,
                    iterator=iterator,
                    contents_select=contents_select,
                    table_relation=table_relation,
                    range_relation=range_relation,
                    single_external=single_external,
                    rewrites=rewrites,
                    elements=elements,
                    ctx=rctx,
                )

        update_stmt = pgast.UpdateStmt(
            relation=table_relation,
            where_clause=astutils.new_binop(
                lexpr=pgast.ColumnRef(
                    name=[table_relation.alias.aliasname, "id"]
                ),
                op="=",
                rexpr=pathctx.get_rvar_path_identity_var(
                    contents_rvar, subject_path_id, env=ctx.env
                ),
            ),
            from_clause=[contents_rvar],
            targets=[
                pgast.MultiAssignRef(
                    columns=[not_none(value.name) for value, _ in values],
                    source=pgast.SelectStmt(
                        target_list=[
                            pgast.ResTarget(
                                val=pgast.ColumnRef(
                                    name=[
                                        contents_rvar.alias.aliasname,
                                        not_none(value.name),
                                    ]
                                )
                            )
                            for value, _ in values
                        ],
                    ),
                )
            ],
        )
        relctx.pull_path_namespace(
            target=update_stmt, source=contents_rvar, ctx=ctx
        )
        pathctx.put_path_value_rvar(
            update_stmt, subject_path_id, table_relation
        )
        pathctx.put_path_source_rvar(
            update_stmt, subject_path_id, table_relation
        )

        update_cte.query = update_stmt

    if pol_expr:
        assert pol_ctx
        with pol_ctx.reenter():
            policy_cte = compile_policy_check(
                contents_cte, ir_stmt, pol_expr, typeref=typeref, ctx=pol_ctx
            )
        force_policy_checks(
            policy_cte,
            ((update_stmt,) if update_stmt else ())
            + tuple(cte.query for cte in link_ctes),
            ctx=ctx,
        )

    if values:
        toplevel.append_cte(update_cte)

    for link_cte in link_ctes:
        toplevel.append_cte(link_cte)


def process_update_rewrites(
    ir_stmt: irast.UpdateStmt,
    *,
    typeref: irast.TypeRef,
    contents_cte: pgast.CommonTableExpr,
    contents_rvar: pgast.PathRangeVar,
    iterator: Optional[pgast.IteratorCTE],
    contents_select: pgast.SelectStmt,
    table_relation: pgast.RelRangeVar,
    range_relation: pgast.PathRangeVar,
    single_external: List[irast.SetE[irast.Pointer]],
    rewrites: irast.RewritesOfType,
    elements: Sequence[
        Tuple[irast.SetE[irast.Pointer], irast.BasePointerRef, qlast.ShapeOp]],
    ctx: context.CompilerContextLevel,
) -> tuple[
    pgast.CommonTableExpr,
    pgast.PathRangeVar,
    list[tuple[pgast.ResTarget, irast.PathId]],
]:
    # assert ir_stmt.rewrites
    subject_path_id = ir_stmt.subject.path_id
    if ir_stmt.rewrites:
        old_path_id = ir_stmt.rewrites.old_path_id
    else:
        # Need values for the single external link case
        old_path_id = subject_path_id
    assert old_path_id

    table_rel = table_relation.relation
    assert isinstance(table_rel, pgast.Relation)

    # Need to set up an iterator for any internal DML.
    iterator = pgast.IteratorCTE(
        path_id=subject_path_id,
        cte=contents_cte,
        parent=iterator,
        # __old__
        other_paths=(
            ((old_path_id, pgce.PathAspect.IDENTITY),)
        ),
    )

    with ctx.newrel() as rctx:
        rewrites_stmt = rctx.rel
        clauses.setup_iterator_volatility(iterator, ctx=rctx)
        rctx.enclosing_cte_iterator = iterator

        # pruned down version of gen_dml_cte
        rewrites_stmt.from_clause.append(range_relation)

        # pull in contents_select for __subject__
        relctx.include_rvar(
            rewrites_stmt, contents_rvar, subject_path_id, ctx=ctx
        )
        rewrites_stmt.where_clause = astutils.new_binop(
            lexpr=pathctx.get_rvar_path_identity_var(
                contents_rvar, subject_path_id, env=ctx.env
            ),
            op="=",
            rexpr=pathctx.get_rvar_path_identity_var(
                range_relation, subject_path_id, env=ctx.env
            ),
        )

        # pull in table_relation for __old__
        table_rel.path_outputs[
            (old_path_id, pgce.PathAspect.VALUE)
        ] = table_rel.path_outputs[(subject_path_id, pgce.PathAspect.VALUE)]
        relctx.include_rvar(
            rewrites_stmt, table_relation, old_path_id, ctx=ctx
        )
        rewrites_stmt.where_clause = astutils.extend_binop(
            rewrites_stmt.where_clause,
            astutils.new_binop(
                lexpr=pgast.ColumnRef(
                    name=[table_relation.alias.aliasname, "id"]
                ),
                op="=",
                rexpr=pathctx.get_rvar_path_identity_var(
                    range_relation, subject_path_id, env=ctx.env
                ),
            ),
        )

        relctx.pull_path_namespace(
            target=rewrites_stmt, source=table_relation, ctx=ctx
        )

        rewrite_elements = [
            (el, ptrref, qlast.ShapeOp.ASSIGN)
            for el, ptrref in rewrites.values()
        ]
        values, _, nptr_map = process_update_shape(
            ir_stmt, rewrites_stmt, rewrite_elements, typeref, rctx,
        )

        # If there are any single links that were compiled externally,
        # populate the field from the link overlays.
        handled = set(rewrites)
        for ext_ir in single_external:
            handled.add(ext_ir.expr.ptrref.shortname.name)
            actual_ptrref = irtyputils.find_actual_ptrref(
                typeref, ext_ir.expr.ptrref)
            with rctx.subrel() as ectx:
                ext_rvar = relctx.new_pointer_rvar(
                    ext_ir, link_bias=True, src_rvar=contents_rvar,
                    ctx=ectx)
                relctx.include_rvar(
                    ectx.rel, ext_rvar, ext_ir.path_id, ctx=ectx)
                # Make the subquery output the target
                pathctx.get_path_value_output(
                    ectx.rel, ext_ir.path_id, env=ctx.env)

            ptr_info = pg_types.get_ptrref_storage_info(
                actual_ptrref, resolve_type=True, link_bias=False)
            updval = pgast.ResTarget(
                name=ptr_info.column_name, val=ectx.rel)
            rewrites_stmt.target_list.append(updval)
            values.append((updval, ext_ir.path_id))
            nptr_map[actual_ptrref.name] = ectx.rel

        # Pull in pointers that were not rewritten
        not_rewritten = {
            (e, ptrref) for e, ptrref, _ in elements
            if ptrref.shortname.name not in handled
        }
        for e, ptrref in not_rewritten:
            # FIXME: Duplicates some with process_update_shape
            actual_ptrref = irtyputils.find_actual_ptrref(typeref, ptrref)
            ptr_info = pg_types.get_ptrref_storage_info(
                actual_ptrref, resolve_type=True, link_bias=False)
            if ptr_info.table_type == 'ObjectType':
                val = pathctx.get_path_var(
                    rewrites_stmt,
                    e.path_id,
                    aspect=pgce.PathAspect.VALUE,
                    env=ctx.env,
                )
                updval = pgast.ResTarget(
                    name=ptr_info.column_name, val=val)
                values.append((updval, e.path_id))
                rewrites_stmt.target_list.append(updval)

        fallback_rvar = pgast.DynamicRangeVar(
            dynamic_get_path=_mk_dynamic_get_path(
                nptr_map, typeref, contents_rvar),
        )
        pathctx.put_path_source_rvar(rctx.rel, subject_path_id, fallback_rvar)
        pathctx.put_path_value_rvar(rctx.rel, subject_path_id, fallback_rvar)

        rewrites_cte = pgast.CommonTableExpr(
            query=rctx.rel,
            name=ctx.env.aliases.get("upd_rewrites"),
            for_dml_stmt=ctx.get_current_dml_stmt(),
        )
        ctx.toplevel_stmt.append_cte(rewrites_cte)
        rewrites_rvar = relctx.rvar_for_rel(rewrites_cte, ctx=ctx)

    return rewrites_cte, rewrites_rvar, values


def process_update_shape(
    ir_stmt: irast.UpdateStmt,
    rel: pgast.SelectStmt,
    elements: Sequence[
        Tuple[irast.SetE[irast.Pointer], irast.BasePointerRef, qlast.ShapeOp]],
    typeref: irast.TypeRef,
    ctx: context.CompilerContextLevel,
) -> Tuple[
    List[Tuple[pgast.ResTarget, irast.PathId]],
    List[Tuple[irast.SetE[irast.Pointer], qlast.ShapeOp]],
    Dict[sn.Name, pgast.BaseExpr],
]:
    values: List[Tuple[pgast.ResTarget, irast.PathId]] = []
    external_updates: List[Tuple[irast.SetE[irast.Pointer], qlast.ShapeOp]] = []
    ptr_map: Dict[sn.Name, pgast.BaseExpr] = {}

    for element, shape_ptrref, shape_op in elements:
        actual_ptrref = irtyputils.find_actual_ptrref(typeref, shape_ptrref)
        ptr_info = pg_types.get_ptrref_storage_info(
            actual_ptrref, resolve_type=True, link_bias=False
        )
        link_ptr_info = pg_types.get_ptrref_storage_info(
            actual_ptrref, resolve_type=False, link_bias=True
        )
        # XXX: Slightly nervous about this.
        assert isinstance(element.expr, irast.Pointer)
        updvalue = element.expr.expr

        if (
            ptr_info.table_type == "ObjectType"
            and not link_ptr_info
            and updvalue is not None
        ):
            with ctx.newscope() as scopectx:
                scopectx.expr_exposed = False
                val: pgast.BaseExpr

                if irtyputils.is_tuple(element.typeref):
                    # When target is a tuple type, make sure
                    # the expression is compiled into a subquery
                    # returning a single column that is explicitly
                    # cast into the appropriate composite type.
                    val = relgen.set_as_subquery(
                        element,
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
                        # base case
                        val = dispatch.compile(updvalue, ctx=scopectx)

                    assert isinstance(updvalue, irast.Stmt)

                    val = check_update_type(
                        val,
                        val,
                        is_subquery=True,
                        ir_stmt=ir_stmt,
                        ir_set=updvalue.result,
                        shape_ptrref=shape_ptrref,
                        actual_ptrref=actual_ptrref,
                        ctx=scopectx,
                    )

                    val = pgast.TypeCast(
                        arg=val,
                        type_name=pgast.TypeName(name=ptr_info.column_type),
                    )

                if shape_op is qlast.ShapeOp.SUBTRACT:
                    val = pgast.FuncCall(
                        name=("nullif",),
                        args=[
                            pgast.ColumnRef(name=[ptr_info.column_name]),
                            val,
                        ],
                    )

                ptr_map[actual_ptrref.name] = val
                updtarget = pgast.ResTarget(
                    name=ptr_info.column_name,
                    val=val,
                )
                values.append((updtarget, element.path_id))

                # Register the output as both a var and an output
                # so that if it is referenced in a policy or
                # rewrite, the find_path_output optimization fires
                # and we reuse the output instead of duplicating
                # it.
                # XXX: Maybe this suggests a rework of the
                # DynamicRangeVar mechanism would be a good idea.
                pathctx.put_path_var(
                    rel,
                    element.path_id,
                    aspect=pgce.PathAspect.VALUE,
                    var=val,
                )
                pathctx._put_path_output_var(
                    rel,
                    element.path_id,
                    aspect=pgce.PathAspect.VALUE,
                    var=pgast.ColumnRef(name=[ptr_info.column_name]),
                )

        if link_ptr_info and link_ptr_info.table_type == "link":
            external_updates.append((element, shape_op))

    rel.target_list.extend(v for v, _ in values)

    return (values, external_updates, ptr_map)


def process_update_conflicts(
    *,
    ir_stmt: irast.UpdateStmt,
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
            ctx=ctx,
        )


def check_update_type(
    val: pgast.BaseExpr,
    rel_or_rvar: Union[pgast.BaseExpr, pgast.PathRangeVar],
    *,
    is_subquery: bool,
    ir_stmt: irast.UpdateStmt,
    ir_set: irast.Set,
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

    assert isinstance(actual_ptrref, irast.PointerRef)
    base_ptrref = shape_ptrref.real_material_ptr

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

    # Make up a ptrref for the __type__ link on our actual target type
    # and make up a new path_id to access it. Relies on __type__ always
    # being named __type__, but that's fine.
    # (Arranging to actually get a legit pointer ref is pointlessly expensive.)
    el_name = sn.QualName('__', '__type__')
    actual_type_ptrref = irast.SpecialPointerRef(
        name=el_name,
        shortname=el_name,
        out_source=actual_ptrref.out_target,
        # HACK: This is obviously not the right target type, but we don't
        # need it for anything and the pathid never escapes this function.
        out_target=actual_ptrref.out_target,
        out_cardinality=qltypes.Cardinality.AT_MOST_ONE,
    )
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
        name=astutils.edgedb_func('issubclass', ctx=ctx),
        args=[typ, typeref_val],
    )
    maybe_null = pgast.CaseExpr(
        args=[pgast.CaseWhen(expr=check_result, result=rval)])
    maybe_raise = pgast.FuncCall(
        name=astutils.edgedb_func('raise_on_null', ctx=ctx),
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
    ir_set: irast.SetE[irast.Pointer],
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

    rptr = ir_set.expr
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
                            operator="ANY",
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
            for_dml_stmt=ctx.get_current_dml_stmt(),
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
            delcte.query, path_id.ptr_path(), target_rvar
        )

        # Record the effect of this removal in the relation overlay
        # context to ensure that references to the link in the result
        # of this DML statement yield the expected results.
        relctx.add_ptr_rel_overlay(
            mptrref,
            context.OverlayOp.EXCEPT,
            delcte,
            path_id=path_id.ptr_path(),
            dml_stmts=ctx.dml_stmt_stack,
            ctx=ctx
        )
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
                expr=ir_set.expr.replace(expr=None),
            )
            assert irutils.is_set_instance(ptr_ref_set, irast.Pointer)

            with ctx.new() as subctx:
                # TODO: Do we really need a copy here? things /seem/
                # to work without it
                subctx.rel_overlays = subctx.rel_overlays.copy()
                relctx.add_ptr_rel_overlay(
                    ptrref,
                    context.OverlayOp.EXCEPT,
                    delcte,
                    path_id=path_id.ptr_path(),
                    ctx=subctx
                )

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
                    operator="ALL",
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
        conflict_inference = [
            pgast.ColumnRef(name=[col])
            for col in conflict_cols
        ]

        target_cols = [
            col.name[0]
            for col in cols
            if isinstance(col.name[0], str) and col.name[0] not in conflict_cols
        ]

        if len(target_cols) == 0:
            conflict_clause = pgast.OnConflictClause(
                action='nothing',
                infer=pgast.InferClause(
                    index_elems=conflict_inference
                )
            )
        else:
            conflict_data = pgast.RowExpr(
                args=[
                    pgast.ColumnRef(name=['excluded', col])
                    for col in target_cols
                ],
            )
            conflict_clause = pgast.OnConflictClause(
                action='update',
                infer=pgast.InferClause(
                    index_elems=conflict_inference
                ),
                target_list=[
                    pgast.MultiAssignRef(
                        columns=target_cols,
                        source=conflict_data
                    )
                ]
            )

    update = pgast.CommonTableExpr(
        name=ctx.env.aliases.get(hint='i'),
        query=pgast.InsertStmt(
            relation=target_rvar,
            select_stmt=data_select,
            cols=[
                pgast.InsertTarget(name=downcast(str, col.name[0]))
                for col in cols
            ],
            on_conflict=conflict_clause,
            returning_list=[
                pgast.ResTarget(
                    val=pgast.ColumnRef(name=[pgast.Star()])
                )
            ]
        ),
        for_dml_stmt=ctx.get_current_dml_stmt(),
    )

    pathctx.put_path_value_rvar(update.query, path_id.ptr_path(), target_rvar)

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
                mptrref,
                context.OverlayOp.FILTER,
                overlay_cte,
                dml_stmts=ctx.dml_stmt_stack,
                path_id=path_id.ptr_path(),
                ctx=octx
            )

        relctx.add_ptr_rel_overlay(
            mptrref,
            context.OverlayOp.UNION,
            overlay_cte,
            dml_stmts=ctx.dml_stmt_stack,
            path_id=path_id.ptr_path(),
            ctx=octx
        )

    if policy_ctx:
        policy_ctx.rel_overlays = policy_ctx.rel_overlays.copy()
        register_overlays(data_cte, policy_ctx)

    register_overlays(update, ctx)

    return update, None


def process_link_values(
    *,
    ir_stmt: irast.MutatingStmt,
    ir_expr: irast.SetE[irast.Pointer],
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

        ir_rptr = ir_expr.expr
        ptrref = ir_rptr.ptrref
        if ptrref.material_ptr is not None:
            ptrref = ptrref.material_ptr
        assert isinstance(ptrref, irast.PointerRef)
        ptr_is_multi_required = (
            ptrref.out_cardinality == qltypes.Cardinality.AT_LEAST_ONE
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

            if ptr_is_multi_required and enforce_cardinality:
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
            shape_ptrref=ptrref,
            actual_ptrref=actual_ptrref,
            ctx=ctx,
        )

    if ptr_is_multi_required and enforce_cardinality:
        target_ref = pgast.FuncCall(
            name=astutils.edgedb_func('raise_on_null', ctx=ctx),
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
            row_query, col_path_id, aspect=pgce.PathAspect.VALUE,
            var=pgast.ColumnRef(name=[col]),
        )

    link_rows = pgast.CommonTableExpr(
        query=row_query,
        name=ctx.env.aliases.get(hint='r'),
        for_dml_stmt=ctx.get_current_dml_stmt(),
    )

    return link_rows, specified_cols


def process_delete_body(
    *,
    ir_stmt: irast.DeleteStmt,
    delete_cte: pgast.CommonTableExpr,
    typeref: irast.TypeRef,
    ctx: context.CompilerContextLevel,
) -> None:
    """Finalize DELETE on an object.

    The actual DELETE was generated in gen_dml_cte, so we only
    have work to do here if there are link tables to clean up.
    """
    ctx.toplevel_stmt.append_cte(delete_cte)

    pointers = ir_stmt.links_to_delete[typeref.id]

    for ptrref in pointers:
        target_rvar = relctx.range_for_ptrref(
            ptrref, for_mutation=True, only_self=True, ctx=ctx)
        assert isinstance(target_rvar, pgast.RelRangeVar)

        range_rvar = pgast.RelRangeVar(
            relation=delete_cte,
            alias=pgast.Alias(
                aliasname=ctx.env.aliases.get(hint='range')
            )
        )

        where_clause = astutils.new_binop(
            lexpr=pgast.ColumnRef(name=[
                target_rvar.alias.aliasname, 'source'
            ]),
            op='=',
            rexpr=pathctx.get_rvar_path_identity_var(
                range_rvar, ir_stmt.result.path_id, env=ctx.env)
        )
        del_query = pgast.DeleteStmt(
            relation=target_rvar,
            where_clause=where_clause,
            using_clause=[range_rvar],
        )
        ctx.toplevel_stmt.append_cte(pgast.CommonTableExpr(
            query=del_query,
            name=ctx.env.aliases.get(hint='mlink'),
            for_dml_stmt=ctx.get_current_dml_stmt(),
        ))


# Trigger compilation
def compile_triggers(
    triggers: tuple[tuple[irast.Trigger, ...], ...],
    stmt: pgast.Base,
    *,
    ctx: context.CompilerContextLevel,
) -> None:
    if not triggers:
        return
    assert isinstance(stmt, pgast.Query)

    if stmt.ctes is None:
        stmt.ctes = []
    start_ctes = len(stmt.ctes)

    with ctx.new() as ictx:
        # Clear out type_ctes so that we will recompile them all with
        # our overlays baked in (trigger_mode = True causes the
        # overlays to be included), so that access policies still
        # apply to our "new view" of the database.
        # FIXME: I think we actually need to keep the old type_ctes
        # available for pointers off of __old__ to use.
        ictx.trigger_mode = True
        ictx.type_rewrite_ctes = {}
        ictx.type_inheritance_ctes = {}
        ictx.ordered_type_ctes = []
        ictx.toplevel_stmt = stmt

        for stage in triggers:
            new_overlays = []
            for trigger in stage:
                ictx.path_scope = ctx.path_scope.new_child()
                new_overlays.append(compile_trigger(trigger, ctx=ictx))

            for overlay in new_overlays:
                ictx.rel_overlays.type = (
                    ictx.rel_overlays.type.update(overlay.type))
                ictx.rel_overlays.ptr = (
                    ictx.rel_overlays.ptr.update(overlay.ptr))

    # Install any newly created type CTEs before the CTEs created from
    # this trigger compilation but after anything compiled before.
    stmt.ctes[start_ctes:start_ctes] = ictx.ordered_type_ctes


def compile_trigger(
    trigger: irast.Trigger,
    *,
    ctx: context.CompilerContextLevel,
) -> context.RelOverlays:
    # N.B: The *base type* overlays have the whole union, while subtypes
    # just have subtype things.
    # The things we produce for `affected` take this into account.

    new_path = trigger.new_set.path_id
    old_path = trigger.old_set.path_id if trigger.old_set else None

    # We use overlays to drive the trigger, since with a bit of
    # tweaking, they contain all the relevant information.
    overlays: list[context.OverlayEntry] = []
    for typeref, dml in trigger.affected:
        toverlays = ctx.rel_overlays.type[dml]
        if ov := toverlays.get(typeref.id):
            overlays.extend(ov)

    # Handle deletions by turning except into union
    # Drop FILTER, which is included by update but doesn't help us here
    overlays = [
        (
            (context.OverlayOp.UNION, *x[1:])
            if x[0] == context.OverlayOp.EXCEPT
            else x
        )
        for x in overlays
        if x[0] != context.OverlayOp.FILTER
    ]
    # Replace an initial union with REPLACE, since we *don't* want whatever
    # already existed
    assert overlays and overlays[0][0] == context.OverlayOp.UNION
    overlays[0] = (context.OverlayOp.REPLACE, *overlays[0][1:])

    # Produce a CTE containing all of the affected objects for this trigger
    with ctx.newrel() as ictx:
        ictx.rel_overlays = context.RelOverlays()
        ictx.rel_overlays.type = immu.Map({
            None: immu.Map({trigger.source_type.id: tuple(overlays)})
        })

        # The range produced here will be driven just by the overlays
        rvar = relctx.range_for_material_objtype(
            trigger.source_type,
            new_path,
            include_overlays=True,
            ignore_rewrites=True,
            ctx=ictx,
        )
        relctx.include_rvar(
            ictx.rel, rvar, path_id=new_path, ctx=ictx
        )

        # If __old__ is available, we register its identity/value,
        # but *not* its source.
        if old_path:
            new_ident = pathctx.get_path_identity_var(
                ictx.rel, new_path, env=ctx.env
            )
            pathctx.put_path_identity_var(ictx.rel, old_path, new_ident)
            pathctx.put_path_value_var(ictx.rel, old_path, new_ident)

        contents_cte = pgast.CommonTableExpr(
            query=ictx.rel,
            name=ctx.env.aliases.get('trig_contents'),
            materialized=True,  # XXX: or not?
            for_dml_stmt=ctx.get_current_dml_stmt(),
        )
        ictx.toplevel_stmt.append_cte(contents_cte)

    # Actually compile the trigger
    with ctx.newrel() as tctx:
        # With FOR EACH, we use the iterator machinery to iterate over
        # all of the objects
        if trigger.scope == qltypes.TriggerScope.Each:
            tctx.enclosing_cte_iterator = pgast.IteratorCTE(
                path_id=new_path,
                cte=contents_cte,
                parent=None,
                # old_path gets registered as also appearing in the
                # iterator cte, and so will get included whenever
                # merged
                other_paths=(
                    ((old_path, pgce.PathAspect.IDENTITY),)
                    if old_path else
                    ()
                ),
            )
            merge_iterator(tctx.enclosing_cte_iterator, tctx.rel, ctx=ctx)

        # While with FOR ALL, we register the sets as external rels
        else:
            tctx.external_rels = dict(tctx.external_rels)
            # new_path is just the contents_cte
            tctx.external_rels[new_path] = (
                contents_cte,
                (pgce.PathAspect.VALUE, pgce.PathAspect.SOURCE)
            )
            if old_path:
                # old_path is *also* the contents_cte, but without a source
                # aspect, so we need to include the real database back in.
                tctx.external_rels[old_path] = (
                    contents_cte,
                    (pgce.PathAspect.VALUE, pgce.PathAspect.IDENTITY,)
                )

        # This is somewhat subtle: we merge *every* DML into
        # the "None" overlay, so that all the new database state shows
        # up everywhere...  but __old__ has a TriggerAnchor set up in
        # it, which acts like a dml statement, and *diverts* __old__
        # away from the new data!

        # We grab the list of DML out of dml_stmts instead of just
        # from the overlays for determinism reasons; it effects the
        # order overlays appear in
        all_dml = [
            x for x in ctx.dml_stmts if isinstance(x, irast.MutatingStmt)]
        merge_overlays_globally(all_dml, ctx=tctx)

        # Strip out everything but None. This tidies things up and makes
        # it easy to detect new additions.
        tctx.rel_overlays.type = immu.Map({None: tctx.rel_overlays.type[None]})
        tctx.rel_overlays.ptr = immu.Map({None: tctx.rel_overlays.ptr[None]})

        # Copy over the global overlay to __new__, since it should see
        # the new data also.
        # TODO: We should consider building a dedicated __new__overlay
        # in order to reduce overlay sizes in common cases
        assert isinstance(trigger.new_set.expr, irast.TriggerAnchor)
        tctx.rel_overlays.type = tctx.rel_overlays.type.set(
            trigger.new_set.expr, tctx.rel_overlays.type[None])
        tctx.rel_overlays.ptr = tctx.rel_overlays.ptr.set(
            trigger.new_set.expr, tctx.rel_overlays.ptr[None])

        # N.B: Any DML in the trigger will have the "global" overlay (None)
        # as its starting point.
        dispatch.compile(trigger.expr, ctx=tctx)
        # Force the value to get output so that if it might error
        # it will be forced up by check_ctes
        pathctx.get_path_value_output(
            tctx.rel, trigger.expr.path_id, env=ctx.env)
        pathctx.get_path_serialized_output(
            tctx.rel, trigger.expr.path_id, env=ctx.env)

        # If the expression is *just* DML, as an optimization, skip
        # generating a CTE for the expression and forcing its evaluation
        # with check_ctes. The actual work is all in a DML CTE so we
        # don't need to worry about anything more.
        if (
            not isinstance(trigger.expr.expr, irast.MutatingStmt)
            and not trigger.expr.shape
        ):
            trigger_cte = pgast.CommonTableExpr(
                query=tctx.rel,
                name=ctx.env.aliases.get('trig_body'),
                materialized=True,  # XXX: or not?
                for_dml_stmt=ctx.get_current_dml_stmt(),
            )
            tctx.toplevel_stmt.append_cte(trigger_cte)
            tctx.env.check_ctes.append(trigger_cte)

    saved_overlays = tctx.rel_overlays.copy()
    saved_overlays.type = saved_overlays.type.delete(trigger.new_set.expr)
    saved_overlays.ptr = saved_overlays.ptr.delete(trigger.new_set.expr)
    return saved_overlays
