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


"""Compiler functions to generate SQL relations for IR sets."""


import contextlib
import typing

from edb import errors

from edb.edgeql import qltypes

from edb.schema import objects as s_obj

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from edb.pgsql import ast as pgast
from edb.pgsql import common
from edb.pgsql import types as pg_types

from . import astutils
from . import context
from . import dbobj
from . import dispatch
from . import output
from . import pathctx
from . import relctx


class SetRVar:
    __slots__ = ('rvar', 'path_id', 'aspects')

    def __init__(self, rvar: pgast.BaseRangeVar,
                 path_id: irast.PathId,
                 aspects: typing.Iterable[str]=('value',)) -> None:
        self.aspects = aspects
        self.path_id = path_id
        self.rvar = rvar


class SetRVars:
    __slots__ = ('main', 'new')

    def __init__(self, main: SetRVar, new: typing.List[SetRVar]) -> None:
        self.main = main
        self.new = new


def new_simple_set_rvar(
        ir_set: irast.Set, rvar: pgast.BaseRangeVar,
        aspects: typing.Optional[typing.Iterable[str]]=None) -> SetRVars:

    if aspects is None:
        if ir_set.path_id.is_objtype_path():
            aspects = ('source', 'value')
        else:
            aspects = ('value',)

    rvar = SetRVar(rvar=rvar, path_id=ir_set.path_id, aspects=aspects)
    return SetRVars(main=rvar, new=[rvar])


def new_source_set_rvar(
        ir_set: irast.Set, rvar: pgast.BaseRangeVar) -> SetRVars:

    aspects = ['value']
    if ir_set.path_id.is_objtype_path():
        aspects.append('source')

    return new_simple_set_rvar(ir_set, rvar, aspects)


class OptionalRel:
    def __init__(self, scope_rel, target_rel, emptyrel,
                 unionrel, wrapper, container, marker):
        self.scope_rel = scope_rel
        self.target_rel = target_rel
        self.emptyrel = emptyrel
        self.unionrel = unionrel
        self.wrapper = wrapper
        self.container = container
        self.marker = marker


def get_set_rvar(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:
    """Return a BaseRangeVar for a given IR Set.

    @param ir_set: IR Set node.
    """
    path_id = ir_set.path_id

    scope_stmt = relctx.maybe_get_scope_stmt(path_id, ctx=ctx)
    rvar = relctx.find_rvar(ctx.rel, source_stmt=scope_stmt,
                            path_id=path_id, ctx=ctx)

    if rvar is not None:
        return rvar

    if ctx.toplevel_stmt is None:
        # Top level query
        return _process_toplevel_query(ir_set, ctx=ctx)

    with contextlib.ExitStack() as cstack:

        if scope_stmt is not None:
            newctx = cstack.enter_context(ctx.new())
            newctx.rel = scope_stmt
        else:
            newctx = ctx
            scope_stmt = newctx.rel

        subctx = cstack.enter_context(newctx.subrel())
        # *stmt* here is a tentative container for the relation generated
        # by processing the *ir_set*.  However, the actual compilation
        # is free to return something else instead of a range var over
        # stmt.
        stmt = subctx.rel
        stmt.name = ctx.env.aliases.get(get_set_rel_alias(ir_set, ctx=ctx))

        # If ir.Set compilation needs to produce a subquery,
        # make sure it uses the current subrel.  This makes it
        # possible to set up the path scope here and don't worry
        # about it later.
        subctx.pending_query = stmt

        is_empty_set = isinstance(ir_set, irast.EmptySet)

        is_optional = (
            subctx.scope_tree.is_optional(path_id) or
            path_id in subctx.force_optional
        )

        optional_wrapping = is_optional and not is_empty_set

        if optional_wrapping:
            stmt, optrel = prepare_optional_rel(
                ir_set=ir_set, stmt=stmt, ctx=subctx)
            subctx.pending_query = subctx.rel = stmt

        path_scope = relctx.get_scope(ir_set, ctx=subctx)
        if path_scope:
            if path_scope.is_visible(path_id):
                subctx.path_scope[path_id] = scope_stmt
            relctx.update_scope(ir_set, stmt, ctx=subctx)

        rvars = _get_set_rvar(ir_set, ctx=subctx)

        if optional_wrapping:
            rvars = finalize_optional_rel(ir_set, optrel=optrel,
                                          rvars=rvars, ctx=subctx)
        elif not is_optional and is_empty_set:
            null_query = rvars.main.rvar.query
            null_query.where_clause = pgast.BooleanConstant(val='FALSE')

        for set_rvar in rvars.new:
            # overwrite_path_rvar is needed because we want
            # the outermost Set with the given path_id to
            # represent the path.  Nested Sets with the
            # same path_id but different expression are
            # possible when there is a computable pointer
            # that refers to itself in its expression.
            relctx.include_specific_rvar(
                scope_stmt, set_rvar.rvar,
                path_id=set_rvar.path_id,
                overwrite_path_rvar=True,
                aspects=set_rvar.aspects,
                ctx=subctx)

        rvar = rvars.main.rvar

        for aspect in rvars.main.aspects:
            pathctx.put_path_rvar_if_not_exists(
                ctx.rel, path_id, rvar,
                aspect=aspect, env=subctx.env)

    return rvar


def _process_toplevel_query(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:

    relctx.init_toplevel_query(ir_set, ctx=ctx)
    rvars = _get_set_rvar(ir_set, ctx=ctx)
    return rvars.main.rvar


def _get_set_rvar(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseRangeVar:

    stmt = ctx.rel

    if irutils.is_subquery_set(ir_set):
        # Sub-statement (explicit or implicit), most computables
        # go here.
        rvars = process_set_as_subquery(ir_set, stmt, ctx=ctx)

    elif irutils.is_set_membership_expr(ir_set.expr):
        # A [NOT] IN B expression.
        rvars = process_set_as_membership_expr(ir_set, stmt, ctx=ctx)

    elif isinstance(ir_set, irast.EmptySet):
        # {}
        rvars = process_set_as_empty(ir_set, stmt, ctx=ctx)

    elif irutils.is_union_expr(ir_set.expr):
        # Set operation: UNION
        rvars = process_set_as_setop(ir_set, stmt, ctx=ctx)

    elif irutils.is_distinct_expr(ir_set.expr):
        # DISTINCT Expr
        rvars = process_set_as_distinct(ir_set, stmt, ctx=ctx)

    elif irutils.is_ifelse_expr(ir_set.expr):
        # Expr IF Cond ELSE Expr
        rvars = process_set_as_ifelse(ir_set, stmt, ctx=ctx)

    elif irutils.is_coalesce_expr(ir_set.expr):
        # Expr ?? Expr
        rvars = process_set_as_coalesce(ir_set, stmt, ctx=ctx)

    elif isinstance(ir_set.expr, irast.Tuple):
        # Named tuple
        rvars = process_set_as_tuple(ir_set, stmt, ctx=ctx)

    elif isinstance(ir_set.expr, irast.TupleIndirection):
        # Named tuple indirection.
        rvars = process_set_as_tuple_indirection(ir_set, stmt, ctx=ctx)

    elif isinstance(ir_set.expr, irast.FunctionCall):
        if ir_set.expr.func_shortname == 'std::enumerate':
            if isinstance(irutils.unwrap_set(ir_set.expr.args[0].expr).expr,
                          irast.FunctionCall):
                # Enumeration of a SET-returning function
                rvars = process_set_as_func_enumerate(ir_set, stmt, ctx=ctx)
            else:
                rvars = process_set_as_enumerate(ir_set, stmt, ctx=ctx)
        elif any(pm is qltypes.TypeModifier.SET_OF
                 for pm in ir_set.expr.params_typemods):
            # Call to an aggregate function.
            rvars = process_set_as_agg_expr(ir_set, stmt, ctx=ctx)
        else:
            # Regular function call.
            rvars = process_set_as_func_expr(ir_set, stmt, ctx=ctx)

    elif irutils.is_exists_expr(ir_set.expr):
        # EXISTS(), which is a special kind of an aggregate.
        rvars = process_set_as_exists_expr(ir_set, stmt, ctx=ctx)

    elif isinstance(ir_set.expr, irast.Array):
        # Array literal: "[" expr ... "]"
        rvars = process_set_as_array_expr(ir_set, stmt, ctx=ctx)

    elif isinstance(ir_set.expr, irast.TypeCast):
        # Type cast: <foo>expr
        rvars = process_set_as_type_cast(ir_set, stmt, ctx=ctx)

    elif isinstance(ir_set.expr, irast.TypeIntrospection):
        # INTROSPECT <type-expr>
        rvars = process_set_as_type_introspection(ir_set, stmt, ctx=ctx)

    elif isinstance(ir_set.expr, irast.ConstantSet):
        # {<const>[, <const> ...]}
        rvars = process_set_as_const_set(ir_set, stmt, ctx=ctx)

    elif ir_set.expr is not None:
        # All other expressions.
        rvars = process_set_as_expr(ir_set, stmt, ctx=ctx)

    elif ir_set.rptr is not None:
        # Regular non-computable path step.
        rvars = process_set_as_path(ir_set, stmt, ctx=ctx)

    else:
        # Regular non-computable path start.
        rvars = process_set_as_root(ir_set, stmt, ctx=ctx)

    return rvars


def set_as_subquery(
        ir_set: irast.Set, *,
        as_value: bool=False,
        ctx: context.CompilerContextLevel) -> pgast.Query:
    # Compile *ir_set* into a subquery as follows:
    #     (
    #         SELECT <set_rel>.v
    #         FROM <set_rel>
    #     )
    # If *aggregate* is True, then the return value will
    # be aggregated into an array.
    with ctx.subrel() as subctx:
        wrapper = subctx.rel
        dispatch.visit(ir_set, ctx=subctx)

        if as_value:

            if output.in_serialization_ctx(ctx):
                pathctx.get_path_serialized_output(
                    rel=wrapper, path_id=ir_set.path_id, env=ctx.env)
            else:
                pathctx.get_path_value_output(
                    rel=wrapper, path_id=ir_set.path_id, env=ctx.env)

                var = pathctx.get_path_value_var(
                    rel=wrapper, path_id=ir_set.path_id, env=ctx.env)
                value = output.output_as_value(var, env=ctx.env)

                wrapper.target_list = [
                    pgast.ResTarget(val=value)
                ]
        else:
            pathctx.get_path_value_output(
                rel=wrapper, path_id=ir_set.path_id, env=ctx.env)

    return wrapper


def set_to_array(
        ir_set: irast.Set, query: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:
    """Collapse a set into an array."""
    subrvar = pgast.RangeSubselect(
        subquery=query,
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get('aggw')
        )
    )

    result = pgast.SelectStmt()
    relctx.include_rvar(result, subrvar, path_id=ir_set.path_id, ctx=ctx)

    val = pathctx.maybe_get_path_serialized_var(
        result, ir_set.path_id, env=ctx.env)

    if val is None:
        value_var = pathctx.get_path_value_var(
            result, ir_set.path_id, env=ctx.env)
        val = output.serialize_expr(
            value_var, path_id=ir_set.path_id, env=ctx.env)
        pathctx.put_path_serialized_var(
            result, ir_set.path_id, val, force=True, env=ctx.env)

    pg_type = output.get_pg_type(ir_set.typeref, ctx=ctx)
    orig_val = val

    if (ir_set.path_id.is_array_path()
            and ctx.env.output_format is context.OutputFormat.NATIVE):
        # We cannot aggregate arrays straight away, as
        # they be of different length, so we have to
        # encase each element into a record.
        val = pgast.RowExpr(args=[val], ser_safe=val.ser_safe)
        pg_type = ('record',)

    array_agg = pgast.FuncCall(
        name=('array_agg',),
        args=[val],
        agg_filter=(
            astutils.new_binop(orig_val, pgast.NullConstant(),
                               'IS DISTINCT FROM')
            if orig_val.nullable else None
        ),
        ser_safe=val.ser_safe,
    )

    agg_expr = pgast.CoalesceExpr(
        args=[
            array_agg,
            pgast.TypeCast(
                arg=pgast.ArrayExpr(elements=[]),
                type_name=pgast.TypeName(name=pg_type, array_bounds=[-1])
            )
        ],
        ser_safe=array_agg.ser_safe,
        nullable=False,
    )

    result.target_list = [
        pgast.ResTarget(
            val=agg_expr,
            ser_safe=array_agg.ser_safe,
        )
    ]

    return result


def prepare_optional_rel(
        *, ir_set: irast.Set, stmt: pgast.Query,
        ctx: context.CompilerContextLevel) \
        -> typing.Tuple[pgast.Query, OptionalRel]:

    # For OPTIONAL sets we compute a UNION of both sides and annotate
    # each side with a marker.  We then select only rows that match
    # the marker of the first row:
    #
    #     SELECT
    #         q.*
    #     FROM
    #         (SELECT
    #             marker = first_value(marker) OVER () AS marker,
    #             ...
    #          FROM
    #             (SELECT 1 AS marker, * FROM left
    #              UNION ALL
    #              SELECT 2 AS marker, * FROM right) AS u
    #         ) AS q
    #     WHERE marker

    with ctx.new() as subctx:
        subctx.rel = stmt

        with subctx.subrel() as wrapctx:
            wrapper = wrapctx.rel

            with wrapctx.subrel() as unionctx:

                with unionctx.subrel() as scopectx:
                    scope_rel = scopectx.rel

                    with scopectx.subrel() as targetctx:
                        target_rel = targetctx.rel

                with unionctx.subrel() as scopectx:
                    emptyrel = scopectx.rel
                    emptyrvar = relctx.new_empty_rvar(
                        irast.EmptySet(path_id=ir_set.path_id,
                                       typeref=ir_set.typeref),
                        ctx=scopectx)

                    relctx.include_rvar(
                        emptyrel, emptyrvar, path_id=ir_set.path_id,
                        ctx=scopectx)

                marker = unionctx.env.aliases.get('m')

                scope_rel.target_list.insert(
                    0,
                    pgast.ResTarget(val=pgast.NumericConstant(val='1'),
                                    name=marker))
                emptyrel.target_list.insert(
                    0,
                    pgast.ResTarget(val=pgast.NumericConstant(val='2'),
                                    name=marker))

                unionqry = unionctx.rel
                unionqry.op = 'UNION'
                unionqry.all = True
                unionqry.larg = scope_rel
                unionqry.rarg = emptyrel

            lagged_marker = pgast.FuncCall(
                name=('first_value',),
                args=[pgast.ColumnRef(name=[marker])],
                over=pgast.WindowDef()
            )

            marker_ok = astutils.new_binop(
                pgast.ColumnRef(name=[marker]),
                lagged_marker,
                op='=',
            )

            wrapper.target_list.append(
                pgast.ResTarget(
                    name=marker,
                    val=marker_ok
                )
            )

    return (
        target_rel,
        OptionalRel(scope_rel=scope_rel, target_rel=target_rel,
                    emptyrel=emptyrel, unionrel=unionqry,
                    wrapper=wrapper, container=stmt, marker=marker)
    )


def finalize_optional_rel(
        ir_set: irast.Set, optrel: OptionalRel, rvars: SetRVars,
        ctx: context.CompilerContextLevel) -> SetRVars:

    with ctx.new() as subctx:
        subctx.rel = setrel = optrel.scope_rel

        for set_rvar in rvars.new:
            relctx.include_specific_rvar(
                setrel, set_rvar.rvar, path_id=set_rvar.path_id,
                aspects=set_rvar.aspects, ctx=subctx)

        for aspect in rvars.main.aspects:
            pathctx.put_path_rvar_if_not_exists(
                setrel, ir_set.path_id, rvars.main.rvar,
                aspect=aspect, env=subctx.env)

        lvar = pathctx.get_path_value_var(
            setrel, path_id=ir_set.path_id, env=subctx.env)

        if lvar.nullable:
            # The left var is still nullable, which may be the
            # case for non-required singleton scalar links.
            # Filter out NULLs.
            setrel.where_clause = astutils.extend_binop(
                setrel.where_clause,
                pgast.NullTest(
                    arg=lvar, negated=True
                )
            )

        if isinstance(lvar, pgast.TupleVar):
            # Make sure we have a correct TupleVar in place for the empty rvar,
            # otherwise _get_rel_path_output() will try to inject a NULL in its
            # place, breaking the UNION balance.
            null_rvar = pathctx.get_path_rvar(
                optrel.emptyrel, ir_set.path_id,
                aspect='value', env=subctx.env)

            def _ensure_empty_tvar(tvar: pgast.TupleVar,
                                   path_id: irast.PathId) -> pgast.TupleVar:
                tvarels = []
                for element in tvar.elements:
                    if isinstance(element.val, pgast.TupleVar):
                        _ensure_empty_tvar(element.val, element.path_id)
                    tvarels.append(pgast.TupleElement(path_id=element.path_id))
                pathctx.put_path_value_var(
                    optrel.emptyrel, path_id,
                    pgast.TupleVar(elements=tvarels), env=subctx.env)
                pathctx.put_path_source_rvar(
                    optrel.emptyrel, path_id, null_rvar, env=subctx.env)

            _ensure_empty_tvar(lvar, ir_set.path_id)

    unionrel = optrel.unionrel
    union_rvar = dbobj.rvar_for_rel(unionrel, lateral=True, env=ctx.env)

    with ctx.new() as subctx:
        subctx.rel = wrapper = optrel.wrapper
        relctx.include_rvar(wrapper, union_rvar, ir_set.path_id, ctx=subctx)

    with ctx.new() as subctx:
        subctx.rel = stmt = optrel.container
        wrapper_rvar = dbobj.rvar_for_rel(
            wrapper, lateral=True, env=subctx.env)

        relctx.include_rvar(stmt, wrapper_rvar, ir_set.path_id, ctx=subctx)

        stmt.where_clause = astutils.extend_binop(
            stmt.where_clause,
            dbobj.get_column(wrapper_rvar, optrel.marker, nullable=False))

        stmt.nullable = True

    sub_rvar = SetRVar(rvar=relctx.new_rel_rvar(ir_set, stmt, ctx=ctx),
                       path_id=ir_set.path_id,
                       aspects=rvars.main.aspects)

    return SetRVars(main=sub_rvar, new=[sub_rvar])


def get_set_rel_alias(ir_set: irast.Set, *,
                      ctx: context.CompilerContextLevel) -> str:
    if ir_set.rptr is not None and ir_set.rptr.source.typeref is not None:
        alias_hint = '{}_{}'.format(
            ir_set.rptr.source.typeref.name_hint.name,
            ir_set.rptr.ptrref.shortname.name
        )
    else:
        _, _, dname = ir_set.path_id.target_name_hint.rpartition('::')
        alias_hint = dname.replace('~', '-')

    return alias_hint


def process_set_as_root(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:

    rvar = relctx.new_root_rvar(ir_set, ctx=ctx)
    return new_source_set_rvar(ir_set, rvar)


def process_set_as_empty(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:

    rvar = relctx.new_empty_rvar(ir_set, ctx=ctx)
    return new_source_set_rvar(ir_set, rvar)


def process_set_as_link_property_ref(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    ir_source = ir_set.rptr.source
    src_rvar = get_set_rvar(ir_source, ctx=ctx)
    rvars = []

    lpropref = ir_set.rptr.ptrref
    ptr_info = pg_types.get_ptrref_storage_info(
        lpropref, resolve_type=False, link_bias=False)

    if (ptr_info.table_type == 'ObjectType' or
            lpropref.shortname == 'std::target'):
        # This is a singleton link property stored in source rel,
        # e.g. @target
        val = pathctx.get_rvar_path_var(
            src_rvar, ir_source.path_id, aspect='value', env=ctx.env)

        pathctx.put_rvar_path_output(
            src_rvar, ir_set.path_id, aspect='value', var=val, env=ctx.env)

        return SetRVars(
            main=SetRVar(rvar=src_rvar, path_id=ir_set.path_id), new=[])

    with ctx.new() as newctx:
        link_path_id = ir_set.path_id.src_path()
        source_scope_stmt = relctx.get_scope_stmt(
            ir_source.path_id, ctx=newctx)

        link_rvar = pathctx.maybe_get_path_rvar(
            source_scope_stmt, link_path_id, aspect='source', env=ctx.env)

        if link_rvar is None:
            link_rvar = relctx.new_pointer_rvar(
                ir_source.rptr, src_rvar=src_rvar, link_bias=True, ctx=newctx)

        rvars.append(SetRVar(
            link_rvar, link_path_id, aspects=['value', 'source']))

        target_rvar = pathctx.maybe_get_path_rvar(
            source_scope_stmt, link_path_id.tgt_path(),
            aspect='value', env=ctx.env)

        if target_rvar is None:
            target_rvar = relctx.new_root_rvar(ir_source, ctx=newctx)

        rvars.append(SetRVar(target_rvar, link_path_id.tgt_path()))

    return SetRVars(main=SetRVar(link_rvar, ir_set.path_id), new=rvars)


def process_set_as_path(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    rptr = ir_set.rptr
    ptrref = rptr.ptrref
    ir_source = rptr.source
    source_is_visible = ctx.scope_tree.is_visible(ir_source.path_id)
    is_type_indirection = ir_set.path_id.is_type_indirection_path()

    rvars = []

    if is_type_indirection:
        if ptrref.ancestral:
            # This is an ancestral type indirection, i.e. the
            # target type is an ancestor of the current type,
            # which means we don't have to restrict the set
            # and can simply forward to the source rvar.
            source_rvar = get_set_rvar(ir_source, ctx=ctx)
            stmt.view_path_id_map[ir_set.path_id] = ir_source.path_id
            relctx.include_rvar(stmt, source_rvar, ir_set.path_id, ctx=ctx)

        elif (not source_is_visible and ir_source.rptr is not None
                and not ir_source.path_id.is_type_indirection_path()
                and pg_types.get_ptrref_storage_info(
                    ir_source.rptr.ptrref).table_type != 'ObjectType'):
            # Otherwise, if the source link path is not visible
            # and is not inline, we have an opportunity to opmimize
            # the target join by directly replacing the target type.
            with ctx.new() as subctx:
                subctx.join_target_type_filter = (
                    subctx.join_target_type_filter.copy())
                subctx.join_target_type_filter[ir_source] = ir_set.typeref
                source_rvar = get_set_rvar(ir_source, ctx=subctx)

            stmt.view_path_id_map[ir_set.path_id] = ir_source.path_id
            relctx.include_rvar(stmt, source_rvar, ir_set.path_id, ctx=ctx)

        else:
            source_rvar = get_set_rvar(ir_source, ctx=ctx)
            poly_rvar = relctx.new_poly_rvar(ir_set, ctx=ctx)
            relctx.include_rvar(stmt, poly_rvar, ir_set.path_id, ctx=ctx)

        sub_rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
        return new_simple_set_rvar(ir_set, sub_rvar, ['value', 'source'])

    ptr_info = pg_types.get_ptrref_storage_info(
        ptrref, resolve_type=False, link_bias=False)

    # Path is a link property.
    is_linkprop = ptrref.parent_ptr is not None
    # Path is a reference to a relationship stored in the source table.
    is_inline_ref = ptr_info.table_type == 'ObjectType'
    is_primitive_ref = not irtyputils.is_object(ptrref.out_target)
    is_inline_primitive_ref = is_inline_ref and is_primitive_ref
    is_id_ref_to_inline_source = False

    semi_join = (
        not source_is_visible and
        ir_source.path_id not in ctx.disable_semi_join and
        not (is_linkprop or is_primitive_ref)
    )

    source_rptr = ir_source.rptr
    if (irtyputils.is_id_ptrref(ptrref) and source_rptr is not None
            and not irtyputils.is_inbound_ptrref(source_rptr.ptrref)
            and not irtyputils.is_computable_ptrref(source_rptr.ptrref)
            and not irutils.is_type_indirection_reference(ir_set)):

        src_src_is_visible = ctx.scope_tree.is_visible(
            source_rptr.source.path_id)

        # Record the ptrref visibility in a way that get_path_var
        # can access, to properly apply the second part of this
        # optimization.
        ctx.env.ptrref_source_visibility[source_rptr.ptrref] = (
            src_src_is_visible)

        if src_src_is_visible:
            # When there is a reference to the id property of
            # an object which is linked to by a link stored
            # inline, we want to route the reference to the
            # inline attribute.  For example,
            # Foo.__type__.id gets resolved to the Foo.__type__
            # column.  However, this optimization must not be
            # applied if the source is a type indirection, e.g
            # __type__[IS Array].id, or if Foo is not visible in
            # this scope.
            source_ptr_info = pg_types.get_ptrref_storage_info(
                source_rptr.ptrref, resolve_type=False, link_bias=False)
            is_id_ref_to_inline_source = (
                source_ptr_info.table_type == 'ObjectType')

    if semi_join:
        with ctx.subrel() as srcctx:
            srcctx.expr_exposed = False
            src_rvar = get_set_rvar(ir_source, ctx=srcctx)
            set_rvar = relctx.semi_join(stmt, ir_set, src_rvar, ctx=srcctx)
            rvars.append(SetRVar(set_rvar, ir_set.path_id,
                                 ['value', 'source']))

    elif is_id_ref_to_inline_source:
        ir_source = ir_source.rptr.source
        src_rvar = get_set_rvar(ir_source, ctx=ctx)

    elif not source_is_visible:
        with ctx.subrel() as srcctx:
            if is_linkprop:
                srcctx.disable_semi_join.add(ir_source.path_id)
                srcctx.unique_paths.add(ir_source.path_id)

            get_set_rvar(ir_source, ctx=srcctx)

            if is_inline_primitive_ref:
                # Semi-join variant for inline scalar links,
                # which is, essentially, just filtering out NULLs.
                relctx.ensure_source_rvar(ir_source, srcctx.rel, ctx=srcctx)

                var = pathctx.get_path_value_var(
                    srcctx.rel, path_id=ir_set.path_id, env=ctx.env)
                if var.nullable:
                    srcctx.rel.where_clause = astutils.extend_binop(
                        srcctx.rel.where_clause,
                        pgast.NullTest(arg=var, negated=True))

        srcrel = srcctx.rel
        src_rvar = dbobj.rvar_for_rel(
            srcrel, lateral=True, env=srcctx.env)
        relctx.include_rvar(stmt, src_rvar, path_id=ir_source.path_id, ctx=ctx)
        stmt.path_id_mask.add(ir_source.path_id)

    else:
        src_rvar = get_set_rvar(ir_source, ctx=ctx)

    # Path is a reference to a link property.
    if is_linkprop:
        srvars = process_set_as_link_property_ref(ir_set, stmt, ctx=ctx)
        main_rvar = srvars.main
        rvars.extend(srvars.new)

    elif is_id_ref_to_inline_source:
        main_rvar = SetRVar(
            relctx.ensure_source_rvar(ir_source, stmt, ctx=ctx),
            path_id=ir_set.path_id,
            aspects=['value']
        )

    elif is_inline_primitive_ref:
        # There is an opportunity to also expose the "source" aspect
        # for tuple refs here, but that requires teaching pathctx about
        # complex field indirections, so rely on the row_getattr_by_num
        # fallback for tuple properties for now.
        main_rvar = SetRVar(
            relctx.ensure_source_rvar(ir_source, stmt, ctx=ctx),
            path_id=ir_set.path_id,
            aspects=['value']
        )

    elif not semi_join:
        # Link range.
        if is_inline_ref:
            aspects = ['value']
        else:
            aspects = ['value', 'source']

        map_rvar = SetRVar(
            relctx.new_pointer_rvar(ir_set.rptr, src_rvar=src_rvar, ctx=ctx),
            path_id=ir_set.path_id.ptr_path(),
            aspects=aspects
        )

        rvars.append(map_rvar)

        # Target set range.
        if irtyputils.is_object(ir_set.typeref):
            typeref = ctx.join_target_type_filter.get(ir_set, ir_set.typeref)
            target_rvar = relctx.new_root_rvar(
                ir_set, typeref=typeref, ctx=ctx)

            main_rvar = SetRVar(
                target_rvar,
                path_id=ir_set.path_id,
                aspects=['value', 'source']
            )

            rvars.append(main_rvar)
        else:
            main_rvar = map_rvar

    if not source_is_visible:
        # If the source path is not visible in the current scope,
        # it means that there are no other paths sharing this path prefix
        # in this scope.  In such cases the path is represented by a subquery
        # rather than a simple set of ranges.
        for set_rvar in rvars:
            relctx.include_specific_rvar(
                stmt, set_rvar.rvar, path_id=set_rvar.path_id,
                aspects=set_rvar.aspects, ctx=ctx)

        if is_primitive_ref:
            aspects = ['value']
        else:
            aspects = ['value', 'source']

        main_rvar = SetRVar(
            relctx.new_rel_rvar(ir_set, stmt, ctx=ctx),
            path_id=ir_set.path_id,
            aspects=aspects,
        )

        rvars = [main_rvar]

    return SetRVars(main=main_rvar, new=rvars)


def process_set_as_subquery(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    is_scalar_path = ir_set.path_id.is_scalar_path()

    if ir_set.rptr is not None:
        ir_source = ir_set.rptr.source
        if is_scalar_path:
            source_is_visible = True
        else:
            # Non-scalar computable pointer.  Check if path source is
            # visible in the outer scope.
            outer_fence = ctx.scope_tree.parent_fence
            source_is_visible = outer_fence.is_visible(ir_source.path_id)

        if source_is_visible:
            get_set_rvar(ir_set.rptr.source, ctx=ctx)
    else:
        ir_source = None
        source_is_visible = False

    with ctx.new() as newctx:
        inner_set = ir_set.expr.result
        outer_id = ir_set.path_id
        inner_id = inner_set.path_id
        semi_join = False

        if inner_id != outer_id:
            ctx.rel.view_path_id_map[outer_id] = inner_id

        if ir_source is not None:
            if is_scalar_path and newctx.volatility_ref is None:
                # This is a computable pointer.  In order to ensure that
                # the volatile functions in the pointer expression are called
                # the necessary number of times, we must inject a
                # "volatility reference" into function expressions.
                # The volatility_ref is the identity of the pointer source.
                newctx.volatility_ref = relctx.maybe_get_path_var(
                    stmt, path_id=ir_source.path_id, aspect='identity',
                    ctx=ctx)
            elif not is_scalar_path and not source_is_visible:
                path_scope = relctx.get_scope(ir_set, ctx=newctx)
                if (path_scope is None or
                        path_scope.find_descendant(ir_source.path_id) is None):
                    # Non-scalar computable semi-join.
                    semi_join = True

                    with newctx.subrel() as _, _.newscope() as subctx:
                        get_set_rvar(ir_source, ctx=subctx)
                        subrel = subctx.rel

                    pathctx.get_path_identity_output(
                        subrel, path_id=ir_source.path_id, env=ctx.env)

        dispatch.visit(ir_set.expr, ctx=newctx)

        if semi_join:
            src_ref = pathctx.maybe_get_path_identity_var(
                stmt, path_id=ir_source.path_id, env=ctx.env)

            if src_ref is not None:
                cond_expr = astutils.new_binop(src_ref, subrel, 'IN')
            else:
                # The link expression does not refer to the source,
                # so simply check it's not empty.
                cond_expr = pgast.SubLink(
                    type=pgast.SubLinkType.EXISTS,
                    expr=subrel
                )

            stmt.where_clause = astutils.extend_binop(
                stmt.where_clause, cond_expr)

    if inner_id.is_tuple_path():
        # If we are wrapping a tuple expression, make sure not to
        # over-represent it in terms of the exposed aspects.
        aspects = pathctx.list_path_rvar_aspects(stmt, inner_id, env=ctx.env)
        aspects -= {'serialized'}
    else:
        aspects = ['value', 'source']

    sub_rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return new_simple_set_rvar(ir_set, sub_rvar, aspects)


def process_set_as_membership_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    expr = ir_set.expr

    with ctx.new() as newctx:
        left_arg, right_arg = (a.expr for a in expr.args)

        newctx.expr_exposed = False
        left_expr = dispatch.compile(left_arg, ctx=newctx)

        right_arg = irutils.unwrap_set(right_arg)
        # If the right operand of [NOT] IN is an array_unpack call,
        # then use the ANY/ALL array comparison operator directly,
        # since that has a higher chance of using the indexes.
        is_array_unpack = (
            isinstance(right_arg.expr, irast.FunctionCall)
            and right_arg.expr.func_shortname == 'std::array_unpack'
        )
        if is_array_unpack:
            right_arg = right_arg.expr.args[0].expr

        with newctx.subrel() as _, _.newscope() as subctx:
            dispatch.compile(right_arg, ctx=subctx)
            pathctx.get_path_value_output(
                subctx.rel, right_arg.path_id,
                env=subctx.env)

            right_rel = subctx.rel

            if is_array_unpack:
                right_rel = pgast.TypeCast(
                    arg=right_rel,
                    type_name=pgast.TypeName(
                        name=pg_types.pg_type_from_ir_typeref(
                            right_arg.typeref)
                    )
                )

    negated = expr.func_shortname == 'std::NOT IN'
    op = '!=' if negated else '='
    sublink_type = pgast.SubLinkType.ALL if negated else pgast.SubLinkType.ANY

    set_expr = astutils.new_binop(
        lexpr=left_expr,
        rexpr=pgast.SubLink(
            type=sublink_type,
            expr=right_rel,
        ),
        op=op,
    )

    pathctx.put_path_value_var_if_not_exists(
        stmt, ir_set.path_id, set_expr, env=ctx.env)

    rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return new_simple_set_rvar(ir_set, rvar)


def process_set_as_setop(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    expr = ir_set.expr

    with ctx.new() as newctx:
        newctx.expr_exposed = False

        left, right = (a.expr for a in expr.args)

        with newctx.subrel() as _, _.newscope() as scopectx:
            larg = scopectx.rel
            larg.view_path_id_map[ir_set.path_id] = left.path_id
            dispatch.visit(left, ctx=scopectx)

        with newctx.subrel() as _, _.newscope() as scopectx:
            rarg = scopectx.rel
            rarg.view_path_id_map[ir_set.path_id] = right.path_id
            dispatch.visit(right, ctx=scopectx)

    with ctx.subrel() as subctx:
        subqry = subctx.rel
        # There is only one binary set operators possible coming from IR:
        # UNION
        subqry.op = 'UNION'
        subqry.all = True
        subqry.larg = larg
        subqry.rarg = rarg

        union_rvar = dbobj.rvar_for_rel(subqry, lateral=True, env=subctx.env)
        relctx.include_rvar(stmt, union_rvar, ir_set.path_id, ctx=subctx)

    rvar = dbobj.rvar_for_rel(stmt, lateral=True, env=ctx.env)
    return new_simple_set_rvar(ir_set, rvar)


def process_set_as_distinct(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    expr = ir_set.expr

    with ctx.subrel() as subctx:
        subqry = subctx.rel
        arg = expr.args[0].expr
        subqry.view_path_id_map[ir_set.path_id] = arg.path_id
        dispatch.visit(arg, ctx=subctx)
        subrvar = dbobj.rvar_for_rel(subqry, lateral=True, env=subctx.env)

    relctx.include_rvar(stmt, subrvar, ir_set.path_id, ctx=ctx)

    value_var = pathctx.get_rvar_path_var(
        subrvar, ir_set.path_id, aspect='value', env=ctx.env)

    stmt.distinct_clause = pathctx.get_rvar_output_var_as_col_list(
        subrvar, value_var, aspect='value', env=ctx.env)

    rvar = dbobj.rvar_for_rel(stmt, lateral=True, env=ctx.env)
    return new_simple_set_rvar(ir_set, rvar)


def process_set_as_ifelse(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    # A IF Cond ELSE B is transformed into:
    # SELECT A WHERE Cond UNION ALL SELECT B WHERE NOT Cond
    expr = ir_set.expr

    condition, if_expr, else_expr = (a.expr for a in expr.args)
    _, if_expr_card, else_expr_card = (a.cardinality for a in expr.args)

    with ctx.new() as newctx:
        newctx.expr_exposed = False
        dispatch.visit(condition, ctx=newctx)
        condref = relctx.get_path_var(
            stmt, path_id=condition.path_id,
            aspect='value', ctx=newctx)

    if (if_expr_card is qltypes.Cardinality.ONE
            and else_expr_card is qltypes.Cardinality.ONE
            and irtyputils.is_scalar(expr.typeref)):
        # For a simple case of singleton scalars on both ends of IF,
        # use a CASE WHEN construct, since it's normally faster than
        # a UNION ALL with filters.  The reason why we limit this
        # optimization to scalars is because CASE WHEN can only yield
        # a single value, hence no other aspects can be supported
        # by this rvar.
        with ctx.new() as newctx:
            newctx.expr_exposed = False
            # Values still need to be encased in subqueries to guard
            # against empty sets.
            if_val = set_as_subquery(if_expr, as_value=True, ctx=newctx)
            else_val = set_as_subquery(else_expr, as_value=True, ctx=newctx)

        set_expr = pgast.CaseExpr(
            args=[pgast.CaseWhen(expr=condref, result=if_val)],
            defresult=else_val,
        )

        pathctx.put_path_value_var_if_not_exists(
            stmt, ir_set.path_id, set_expr, env=ctx.env)

    else:
        with ctx.subrel() as _, _.newscope() as subctx:
            larg = subctx.rel
            larg.view_path_id_map[ir_set.path_id] = if_expr.path_id
            dispatch.visit(if_expr, ctx=subctx)

            larg.where_clause = astutils.extend_binop(
                larg.where_clause,
                condref
            )

        with ctx.subrel() as _, _.newscope() as subctx:
            rarg = subctx.rel
            rarg.view_path_id_map[ir_set.path_id] = else_expr.path_id
            dispatch.visit(else_expr, ctx=subctx)

            rarg.where_clause = astutils.extend_binop(
                rarg.where_clause,
                astutils.new_unop('NOT', condref)
            )

        with ctx.subrel() as subctx:
            subqry = subctx.rel
            subqry.op = 'UNION'
            subqry.all = True
            subqry.larg = larg
            subqry.rarg = rarg

            union_rvar = dbobj.rvar_for_rel(
                subqry, lateral=True, env=subctx.env)
            relctx.include_rvar(stmt, union_rvar, ir_set.path_id, ctx=subctx)

    rvar = dbobj.rvar_for_rel(stmt, lateral=True, env=ctx.env)
    return new_simple_set_rvar(ir_set, rvar)


def process_set_as_coalesce(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    expr = ir_set.expr

    with ctx.new() as newctx:
        newctx.expr_exposed = False
        left_ir, right_ir = (a.expr for a in expr.args)
        left_card, right_card = (a.cardinality for a in expr.args)

        if right_card == qltypes.Cardinality.ONE:
            # Singleton RHS, simply use scalar COALESCE.
            left = dispatch.compile(left_ir, ctx=newctx)

            with newctx.new() as rightctx:
                rightctx.force_optional.add(right_ir.path_id)
                right = dispatch.compile(right_ir, ctx=rightctx)

            set_expr = pgast.CoalesceExpr(args=[left, right])

            pathctx.put_path_value_var_if_not_exists(
                stmt, ir_set.path_id, set_expr, env=ctx.env)
        else:
            # Things become tricky in cases where the RHS is a non-singleton.
            # We cannot use the regular scalar COALESCE over a JOIN,
            # as that'll blow up the result cardinality. Instead, we
            # compute a UNION of both sides and annotate each side with
            # a marker.  We then select only rows that match the marker
            # of the first row:
            #
            #     SELECT
            #         q.*
            #     FROM
            #         (SELECT
            #             marker = first_value(marker) OVER () AS marker,
            #             ...
            #          FROM
            #             (SELECT 1 AS marker, * FROM left
            #              UNION ALL
            #              SELECT 2 AS marker, * FROM right) AS u
            #         ) AS q
            #     WHERE marker
            with newctx.subrel() as subctx:
                subqry = subctx.rel

                with ctx.subrel() as sub2ctx:

                    with sub2ctx.subrel() as scopectx:
                        larg = scopectx.rel
                        larg.view_path_id_map[ir_set.path_id] = left_ir.path_id
                        dispatch.visit(left_ir, ctx=scopectx)

                        lvar = pathctx.get_path_value_var(
                            larg, path_id=left_ir.path_id, env=scopectx.env)

                        if lvar.nullable:
                            # The left var is still nullable, which may be the
                            # case for non-required singleton scalar links.
                            # Filter out NULLs.
                            larg.where_clause = astutils.extend_binop(
                                larg.where_clause,
                                pgast.NullTest(
                                    arg=lvar, negated=True
                                )
                            )

                    with sub2ctx.subrel() as scopectx:
                        rarg = scopectx.rel
                        rarg.view_path_id_map[ir_set.path_id] = \
                            right_ir.path_id
                        dispatch.visit(right_ir, ctx=scopectx)

                    marker = sub2ctx.env.aliases.get('m')

                    larg.target_list.insert(
                        0,
                        pgast.ResTarget(val=pgast.NumericConstant(val='1'),
                                        name=marker))
                    rarg.target_list.insert(
                        0,
                        pgast.ResTarget(val=pgast.NumericConstant(val='2'),
                                        name=marker))

                    unionqry = sub2ctx.rel
                    unionqry.op = 'UNION'
                    unionqry.all = True
                    unionqry.larg = larg
                    unionqry.rarg = rarg

                union_rvar = dbobj.rvar_for_rel(
                    unionqry, lateral=True, env=subctx.env)

                relctx.include_rvar(
                    subqry, union_rvar, path_id=ir_set.path_id, ctx=subctx)

                lagged_marker = pgast.FuncCall(
                    name=('first_value',),
                    args=[pgast.ColumnRef(name=[marker])],
                    over=pgast.WindowDef()
                )

                marker_ok = astutils.new_binop(
                    pgast.ColumnRef(name=[marker]),
                    lagged_marker,
                    op='=',
                )

                subqry.target_list.append(
                    pgast.ResTarget(
                        name=marker,
                        val=marker_ok
                    )
                )

            subrvar = dbobj.rvar_for_rel(
                subqry, lateral=True, env=newctx.env)

            relctx.include_rvar(
                stmt, subrvar, path_id=ir_set.path_id, ctx=newctx)

            stmt.where_clause = astutils.extend_binop(
                stmt.where_clause,
                dbobj.get_column(subrvar, marker, nullable=False))

    rvar = dbobj.rvar_for_rel(stmt, lateral=True, env=ctx.env)
    return new_simple_set_rvar(ir_set, rvar)


def process_set_as_tuple(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    expr = ir_set.expr

    with ctx.new() as subctx:
        elements = []

        ttypes = {}
        for i, st in enumerate(ir_set.typeref.subtypes):
            if st.element_name:
                ttypes[st.element_name] = st
            else:
                ttypes[str(i)] = st

        for element in expr.elements:
            path_id = element.path_id
            if path_id != element.val.path_id:
                stmt.view_path_id_map[path_id] = element.val.path_id

            dispatch.visit(element.val, ctx=subctx)
            elements.append(pgast.TupleElement(path_id=path_id))

            var = pathctx.maybe_get_path_var(
                stmt, element.val.path_id,
                aspect='serialized', env=subctx.env)
            if var is not None:
                pathctx.put_path_var(stmt, path_id, var,
                                     aspect='serialized', env=subctx.env)

        set_expr = pgast.TupleVar(elements=elements, named=expr.named)

    relctx.ensure_bond_for_expr(ir_set, stmt, ctx=ctx)
    pathctx.put_path_value_var(stmt, ir_set.path_id, set_expr, env=ctx.env)

    rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return new_simple_set_rvar(ir_set, rvar, ['value', 'source'])


def process_set_as_tuple_indirection(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> typing.List[pgast.BaseRangeVar]:
    tuple_set = ir_set.expr.expr

    with ctx.new() as subctx:
        subctx.expr_exposed = False
        rvar = get_set_rvar(tuple_set, ctx=subctx)

        if not ir_set.path_id.startswith(tuple_set.path_id):
            # Tuple indirection set is fenced, so we need to
            # wrap the reference in a subquery to ensure path_id
            # remapping.
            stmt.view_path_id_map[ir_set.path_id] = ir_set.expr.path_id
            rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=subctx)

        source_rvar = pathctx.maybe_get_path_rvar(
            stmt, tuple_set.path_id, aspect='source', env=subctx.env)

        if source_rvar is None:
            # Lack of visible tuple source means we are
            # an indirection over an opaque tuple, e.g. in
            # `SELECT [(1,)][0].0`.  This means we must
            # use an explicit row attribute dereference.
            tuple_val = pathctx.get_path_value_var(
                stmt, path_id=tuple_set.path_id, env=subctx.env)

            type_sentinel = pgast.TypeCast(
                arg=pgast.NullConstant(),
                type_name=pgast.TypeName(
                    name=pg_types.pg_type_from_ir_typeref(ir_set.typeref)
                )
            )

            ttypes = []
            for i, st in enumerate(tuple_set.typeref.subtypes):
                if st.element_name:
                    ttypes.append(st.element_name)
                else:
                    ttypes.append(str(i))

            index = ttypes.index(ir_set.expr.name)
            att_idx = pgast.NumericConstant(val=str(index + 1))

            set_expr = pgast.FuncCall(
                name=('edgedb', 'row_getattr_by_num'),
                args=[tuple_val, att_idx, type_sentinel]
            )

            pathctx.put_path_var_if_not_exists(
                stmt, ir_set.path_id, set_expr, aspect='value', env=subctx.env)

            rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=subctx)

    return new_simple_set_rvar(ir_set, rvar)


def process_set_as_type_cast(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:

    inner_set = ir_set.expr.expr
    is_json_cast = (
        ir_set.expr.to_type.id == s_obj.get_known_type_id('std::json'))

    with ctx.new() as subctx:
        ctx.rel.view_path_id_map[ir_set.path_id] = inner_set.path_id

        if (is_json_cast
                and (irtyputils.is_collection(inner_set.typeref)
                     or irtyputils.is_object(inner_set.typeref))):
            subctx.expr_exposed = True
            # XXX: this is necessary until pathctx is converted
            #      to use context levels instead of using env
            #      directly.
            orig_output_format = subctx.env.output_format
            subctx.env.output_format = context.OutputFormat.JSONB
            implicit_cast = True
        else:
            implicit_cast = False

        if implicit_cast:
            set_expr = dispatch.compile(inner_set, ctx=subctx)

            serialized = pathctx.maybe_get_path_serialized_var(
                stmt, inner_set.path_id, env=subctx.env)

            if serialized is not None:
                if irtyputils.is_collection(inner_set.typeref):
                    serialized = output.serialize_expr_to_json(
                        serialized, path_id=inner_set.path_id,
                        env=subctx.env)

                pathctx.put_path_value_var(
                    stmt, inner_set.path_id, serialized,
                    force=True, env=subctx.env)

                pathctx.put_path_serialized_var(
                    stmt, inner_set.path_id, serialized,
                    force=True, env=subctx.env)

            subctx.env.output_format = orig_output_format
        else:
            set_expr = dispatch.compile(ir_set.expr, ctx=ctx)

            # A proper path var mapping way would be to wrap
            # the inner expression in a subquery, but that
            # seems excessive for a type cast, so we cover
            # our tracks here by removing the mapping and
            # relying on the value and serialized vars
            # populated above.
            ctx.rel.view_path_id_map.pop(ir_set.path_id)

    pathctx.put_path_value_var_if_not_exists(
        stmt, ir_set.path_id, set_expr, env=ctx.env)

    rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return new_simple_set_rvar(ir_set, rvar)


def process_set_as_type_introspection(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:

    typeref = ir_set.expr.typeref
    type_rvar = dbobj.range_for_typeref(ir_set.typeref,
                                        ir_set.path_id, env=ctx.env)
    pathctx.put_rvar_path_bond(type_rvar, ir_set.path_id)
    type_rvar.value_scope.add(ir_set.path_id)
    clsname = pgast.StringConstant(val=str(typeref.id))
    nameref = dbobj.get_column(type_rvar, 'id', nullable=False)

    condition = astutils.new_binop(nameref, clsname, op='=')
    substmt = pgast.SelectStmt()
    relctx.include_rvar(substmt, type_rvar, ir_set.path_id, ctx=ctx)
    substmt.where_clause = astutils.extend_binop(
        substmt.where_clause, condition)
    set_rvar = relctx.new_rel_rvar(ir_set, substmt, ctx=ctx)

    return new_simple_set_rvar(ir_set, set_rvar, ['value', 'source'])


def process_set_as_const_set(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:

    with ctx.subrel() as subctx:
        vals = [dispatch.compile(v, ctx=subctx) for v in ir_set.expr.elements]
        vals_rel = subctx.rel
        vals_rel.values = [pgast.ImplicitRowExpr(args=[v]) for v in vals]

    vals_rvar = relctx.new_rel_rvar(ir_set, vals_rel, ctx=ctx)
    relctx.include_rvar(stmt, vals_rvar, ir_set.path_id, ctx=ctx)

    rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return new_simple_set_rvar(ir_set, rvar)


def process_set_as_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    with ctx.new() as newctx:
        newctx.expr_exposed = False
        set_expr = dispatch.compile(ir_set.expr, ctx=newctx)

    pathctx.put_path_value_var_if_not_exists(
        stmt, ir_set.path_id, set_expr, env=ctx.env)

    rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return new_simple_set_rvar(ir_set, rvar)


def process_set_as_enumerate(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:

    with ctx.subrel() as newctx:
        newctx.expr_exposed = False

        expr = ir_set.expr
        ir_arg = ir_set.expr.args[0].expr

        arg_ref = dispatch.compile(ir_arg, ctx=newctx)
        arg_val = output.output_as_value(arg_ref, env=newctx.env)
        rtype = expr.typeref
        named_tuple = any(st.element_name for st in rtype.subtypes)

        num_expr = pgast.Expr(
            kind=pgast.ExprKind.OP,
            name='-',
            lexpr=pgast.FuncCall(
                name=('row_number',),
                args=[],
                over=pgast.WindowDef()
            ),
            rexpr=pgast.NumericConstant(val='1'),
            nullable=False,
        )

        set_expr = pgast.TupleVar(
            elements=[
                pgast.TupleElement(
                    path_id=expr.tuple_path_ids[0],
                    name=rtype.subtypes[0].element_name or '0',
                    val=num_expr,
                ),
                pgast.TupleElement(
                    path_id=expr.tuple_path_ids[1],
                    name=rtype.subtypes[1].element_name or '1',
                    val=arg_val,
                ),
            ],
            named=named_tuple
        )

        for element in set_expr.elements:
            pathctx.put_path_value_var(
                newctx.rel, element.path_id, element.val, env=newctx.env)

        pathctx.put_path_var_if_not_exists(
            newctx.rel, ir_set.path_id, set_expr, aspect='value', env=ctx.env)

    aspects = ('value',)

    func_rvar = relctx.new_rel_rvar(ir_set, newctx.rel, ctx=ctx)
    relctx.include_rvar(stmt, func_rvar, ir_set.path_id,
                        pull_namespace=False, aspects=aspects, ctx=ctx)

    rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)

    return new_simple_set_rvar(ir_set, rvar, aspects=aspects)


def _process_set_func_with_ordinality(
        ir_set: irast.Set, *,
        outer_func_set: irast.Set,
        func_name: typing.Tuple[str, ...],
        args: typing.List[pgast.Base],
        ctx: context.CompilerContextLevel) -> None:

    rtype = outer_func_set.typeref
    named_tuple = any(st.element_name for st in rtype.subtypes)
    inner_rtype = ir_set.typeref

    coldeflist = []
    arg_is_tuple = irtyputils.is_tuple(inner_rtype)

    if arg_is_tuple:
        subtypes = {}
        for i, st in enumerate(inner_rtype.subtypes):
            colname = st.element_name or str(i)
            subtypes[colname] = st
            coldeflist.append(
                pgast.ColumnDef(
                    name=colname,
                    typename=pgast.TypeName(
                        name=pg_types.pg_type_from_ir_typeref(st)
                    )
                )
            )

        colnames = list(subtypes)

    else:
        colnames = [ctx.env.aliases.get('v')]
        coldeflist = []

    set_expr = pgast.FuncCall(name=func_name, args=args, coldeflist=coldeflist)

    colnames.append(
        rtype.subtypes[0].element_name or '0'
    )

    func_rvar = pgast.RangeFunction(
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get('f'),
            colnames=colnames),
        lateral=True,
        is_rowsfrom=True,
        with_ordinality=True,
        functions=[set_expr])

    ctx.rel.from_clause.append(func_rvar)

    if arg_is_tuple:
        inner_named_tuple = any(st.element_name for st in inner_rtype.subtypes)
        inner_expr = pgast.TupleVar(
            elements=[
                pgast.TupleElement(
                    path_id=outer_func_set.expr.tuple_path_ids[
                        len(rtype.subtypes) + i],
                    name=n,
                    val=dbobj.get_column(
                        func_rvar, n, nullable=set_expr.nullable)
                )
                for i, n in enumerate(colnames[:-1])
            ],
            named=inner_named_tuple
        )
    else:
        inner_expr = dbobj.get_column(
            func_rvar, colnames[0], nullable=set_expr.nullable)

    set_expr = pgast.TupleVar(
        elements=[
            pgast.TupleElement(
                path_id=outer_func_set.expr.tuple_path_ids[0],
                name=colnames[0],
                val=pgast.Expr(
                    kind=pgast.ExprKind.OP,
                    name='-',
                    lexpr=dbobj.get_column(
                        func_rvar, colnames[-1], nullable=set_expr.nullable,
                    ),
                    rexpr=pgast.NumericConstant(val='1')
                )
            ),
            pgast.TupleElement(
                path_id=outer_func_set.expr.tuple_path_ids[1],
                name=rtype.subtypes[1].element_name or '1',
                val=inner_expr,
            ),
        ],
        named=named_tuple
    )

    for element in set_expr.elements:
        pathctx.put_path_value_var(
            ctx.rel, element.path_id, element.val, env=ctx.env)

    if arg_is_tuple:
        for element in set_expr.elements[1].val.elements:
            pathctx.put_path_value_var(
                ctx.rel, element.path_id, element.val, env=ctx.env)

    return set_expr


def _process_set_func(
        ir_set: irast.Set, *,
        func_name: typing.Tuple[str, ...],
        args: typing.List[pgast.Base],
        ctx: context.CompilerContextLevel) -> None:

    expr = ir_set.expr
    rtype = expr.typeref
    named_tuple = any(st.element_name for st in rtype.subtypes)
    coldeflist = []
    is_tuple = irtyputils.is_tuple(rtype)

    if is_tuple:
        subtypes = {}
        for i, st in enumerate(rtype.subtypes):
            colname = st.element_name or str(i)
            subtypes[colname] = st
            coldeflist.append(
                pgast.ColumnDef(
                    name=colname,
                    typename=pgast.TypeName(
                        name=pg_types.pg_type_from_ir_typeref(st)
                    )
                )
            )

        colnames = list(subtypes)
    else:
        colnames = [ctx.env.aliases.get('v')]
        coldeflist = []

    if expr.sql_func_has_out_params:
        # SQL functions declared with OUT params reject column definitions.
        coldeflist = []

    set_expr = pgast.FuncCall(name=func_name, args=args, coldeflist=coldeflist)

    func_rvar = pgast.RangeFunction(
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get('f'),
            colnames=colnames),
        lateral=True,
        is_rowsfrom=True,
        functions=[set_expr])

    ctx.rel.from_clause.append(func_rvar)

    if not is_tuple:
        set_expr = dbobj.get_column(
            func_rvar, colnames[0], nullable=set_expr.nullable)
    else:
        set_expr = pgast.TupleVar(
            elements=[
                pgast.TupleElement(
                    path_id=expr.tuple_path_ids[i],
                    name=n,
                    val=dbobj.get_column(
                        func_rvar, n, nullable=set_expr.nullable)
                )
                for i, n in enumerate(colnames)
            ],
            named=named_tuple
        )

        for element in set_expr.elements:
            pathctx.put_path_value_var(
                ctx.rel, element.path_id, element.val, env=ctx.env)

    return set_expr


def _compile_func_epilogue(
        ir_set: irast.Set, *,
        set_expr: pgast.Base,
        func_rel: pgast.Query,
        stmt: pgast.Query,
        ctx: context.CompilerContextLevel) -> None:

    if (ctx.volatility_ref is not None and
            ctx.volatility_ref is not context.NO_VOLATILITY and
            ir_set.expr.volatility is qltypes.Volatility.VOLATILE):
        # Apply the volatility reference.
        # See the comment in process_set_as_subquery().
        volatility_source = pgast.SelectStmt(
            values=[pgast.ImplicitRowExpr(args=[ctx.volatility_ref])]
        )
        volatility_rvar = dbobj.rvar_for_rel(volatility_source, env=ctx.env)
        relctx.rel_join(func_rel, volatility_rvar, ctx=ctx)

    pathctx.put_path_var_if_not_exists(
        func_rel, ir_set.path_id, set_expr, aspect='value', env=ctx.env)

    aspects = ('value',)

    func_rvar = relctx.new_rel_rvar(ir_set, func_rel, ctx=ctx)
    relctx.include_rvar(stmt, func_rvar, ir_set.path_id,
                        pull_namespace=False, aspects=aspects, ctx=ctx)

    rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)

    if (ir_set.path_id.is_tuple_path()
            and ir_set.expr.typemod is qltypes.TypeModifier.SET_OF):
        # Functions returning a set of tuples are compiled with an
        # explicit coldeflist, so the result is represented as a
        # TupleVar as opposed to an opaque record datum, so
        # we can access the elements directly without calling
        # `row_getattr_by_num`.
        aspects += ('source',)

    return new_simple_set_rvar(ir_set, rvar, aspects=aspects)


def _compile_func_args(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> None:

    expr = ir_set.expr

    args = []

    for ir_arg in ir_set.expr.args:
        arg_ref = dispatch.compile(ir_arg.expr, ctx=ctx)
        args.append(output.output_as_value(arg_ref, env=ctx.env))

    if expr.has_empty_variadic:
        var = pgast.TypeCast(
            arg=pgast.ArrayExpr(elements=[]),
            type_name=pgast.TypeName(
                name=pg_types.pg_type_from_ir_typeref(
                    expr.variadic_param_type)
            )
        )

        args.append(pgast.VariadicArgument(expr=var))

    return args


def process_set_as_func_enumerate(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:

    expr = ir_set.expr
    inner_func_set = irutils.unwrap_set(expr.args[0].expr)
    inner_func = inner_func_set.expr

    with ctx.subrel() as newctx:
        newctx.expr_exposed = False
        args = _compile_func_args(inner_func_set, ctx=newctx)

        if inner_func.func_sql_function:
            func_name = (inner_func.func_sql_function,)
        else:
            func_name = common.get_function_backend_name(
                inner_func.func_shortname, inner_func.func_module_id)

        set_expr = _process_set_func_with_ordinality(
            ir_set=inner_func_set,
            outer_func_set=ir_set,
            func_name=func_name,
            args=args,
            ctx=newctx)

        func_rel = newctx.rel

    return _compile_func_epilogue(
        ir_set, set_expr=set_expr, func_rel=func_rel, stmt=stmt, ctx=ctx)


def process_set_as_func_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:

    expr = ir_set.expr

    with ctx.subrel() as newctx:
        newctx.expr_exposed = False
        args = _compile_func_args(ir_set, ctx=newctx)

        if expr.func_sql_function:
            name = (expr.func_sql_function,)
        else:
            name = common.get_function_backend_name(
                expr.func_shortname, expr.func_module_id)

        if expr.typemod is qltypes.TypeModifier.SET_OF:
            set_expr = _process_set_func(
                ir_set, func_name=name, args=args, ctx=newctx)
        else:
            set_expr = pgast.FuncCall(name=name, args=args)

        if expr.error_on_null_result:
            set_expr = pgast.FuncCall(
                name=('edgedb', '_raise_exception_on_null'),
                args=[
                    set_expr,
                    pgast.StringConstant(
                        val='invalid_parameter_value',
                    ),
                    pgast.StringConstant(
                        val=expr.error_on_null_result,
                    ),
                    pgast.StringConstant(
                        val=irutils.get_source_context_as_json(
                            expr, errors.InvalidValueError),
                    ),
                ]
            )

        func_rel = newctx.rel

    return _compile_func_epilogue(
        ir_set, set_expr=set_expr, func_rel=func_rel, stmt=stmt, ctx=ctx)


def process_set_as_agg_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    with ctx.newscope() as newctx:
        expr = ir_set.expr
        agg_filter = None
        agg_sort = []

        if ctx.group_by_rels:
            for (path_id, s_path_id), group_rel in ctx.group_by_rels.items():
                group_rvar = dbobj.rvar_for_rel(group_rel, env=ctx.env)
                relctx.include_rvar(stmt, group_rvar, path_id=path_id, ctx=ctx)
                ref = pathctx.get_path_identity_var(stmt, path_id, env=ctx.env)
                stmt.group_clause.append(ref)
                newctx.path_scope[s_path_id] = stmt

        with newctx.new() as argctx:
            # We want array_agg() (and similar) to do the right
            # thing with respect to output format, so, barring
            # the (unacceptable) hardcoding of function names,
            # check if the aggregate accepts a single argument
            # of "any" to determine serialized input safety.
            serialization_safe = expr.func_polymorphic

            if not serialization_safe:
                argctx.expr_exposed = False

            args = []

            for i, ir_call_arg in enumerate(ir_set.expr.args):
                ir_arg = ir_call_arg.expr
                dispatch.visit(ir_arg, ctx=argctx)

                if output.in_serialization_ctx(ctx=argctx):
                    arg_ref = pathctx.get_path_serialized_or_value_var(
                        argctx.rel, ir_arg.path_id, env=argctx.env)

                    if isinstance(arg_ref, pgast.TupleVar):
                        arg_ref = output.serialize_expr(
                            arg_ref, path_id=ir_arg.path_id, env=argctx.env)
                else:
                    arg_ref = pathctx.get_path_value_var(
                        argctx.rel, ir_arg.path_id, env=argctx.env)

                    if isinstance(arg_ref, pgast.TupleVar):
                        arg_ref = output.output_as_value(
                            arg_ref, env=argctx.env)

                path_scope = relctx.get_scope(ir_arg, ctx=argctx)
                arg_is_visible = (
                    path_scope is not None and
                    path_scope.parent.is_any_prefix_visible(ir_arg.path_id))

                if arg_is_visible:
                    # If the argument set is visible above us, we
                    # are aggregating a singleton set, potentially on
                    # the same query level, as the source set.
                    # Postgres doesn't like aggregates on the same query
                    # level, so wrap the arg ref into a VALUES range.
                    wrapper = pgast.SelectStmt(
                        values=[pgast.ImplicitRowExpr(args=[arg_ref])]
                    )
                    colname = argctx.env.aliases.get('a')
                    wrapper_rvar = dbobj.rvar_for_rel(
                        wrapper, lateral=True, colnames=[colname],
                        env=argctx.env)
                    relctx.include_rvar(stmt, wrapper_rvar,
                                        path_id=ir_arg.path_id, ctx=argctx)
                    arg_ref = dbobj.get_column(wrapper_rvar, colname)

                if i == 0 and irutils.is_subquery_set(ir_arg):
                    # If the first argument of the aggregate
                    # is a SELECT or GROUP with an ORDER BY clause,
                    # we move the ordering conditions to the aggregate
                    # call to make sure the ordering is as expected.
                    substmt = ir_arg.expr
                    if isinstance(substmt, irast.GroupStmt):
                        substmt = substmt.result.expr

                    if (isinstance(substmt, irast.SelectStmt) and
                            substmt.orderby):
                        qrvar = pathctx.get_path_rvar(
                            stmt, ir_arg.path_id,
                            aspect='value', env=argctx.env)
                        query = qrvar.query

                        for i, sortref in enumerate(query.sort_clause):
                            alias = argctx.env.aliases.get(f's{i}')

                            query.target_list.append(
                                pgast.ResTarget(
                                    val=sortref.node,
                                    name=alias
                                )
                            )

                            agg_sort.append(
                                pgast.SortBy(
                                    node=dbobj.get_column(qrvar, alias),
                                    dir=sortref.dir,
                                    nulls=sortref.nulls))

                        query.sort_clause = []

                if (irtyputils.is_scalar(ir_arg.typeref)
                        and ir_arg.typeref.base_type is not None):
                    # Cast scalar refs to the base type in aggregate
                    # expressions, since PostgreSQL does not create array
                    # types for custom domains and will fail to process a
                    # query with custom domains appearing as array
                    # elements.
                    #
                    # XXX: Remove this once we switch to PostgreSQL 11,
                    #      which supports domain type arrays.
                    pgtype = pg_types.pg_type_from_ir_typeref(
                        ir_arg.typeref.base_type)
                    pgtype = pgast.TypeName(name=pgtype)
                    arg_ref = pgast.TypeCast(arg=arg_ref, type_name=pgtype)

                args.append(arg_ref)

        if expr.func_sql_function:
            name = (expr.func_sql_function,)
        else:
            name = common.get_function_backend_name(expr.func_shortname,
                                                    expr.func_module_id)

        set_expr = pgast.FuncCall(
            name=name, args=args, agg_order=agg_sort, agg_filter=agg_filter,
            ser_safe=serialization_safe)

        if expr.error_on_null_result:
            set_expr = pgast.FuncCall(
                name=('edgedb', '_raise_exception_on_null'),
                args=[
                    set_expr,
                    pgast.StringConstant(
                        val='invalid_parameter_value',
                    ),
                    pgast.StringConstant(
                        val=expr.error_on_null_result,
                    ),
                    pgast.StringConstant(
                        val=irutils.get_source_context_as_json(
                            expr, errors.InvalidValueError),
                    ),
                ]
            )

        if expr.force_return_cast:
            # The underlying function has a return value type
            # different from that of the EdgeQL function declaration,
            # so we need to make an explicit cast here.
            set_expr = pgast.TypeCast(
                arg=set_expr,
                type_name=pgast.TypeName(
                    name=pg_types.pg_type_from_ir_typeref(expr.typeref)
                )
            )

    if expr.func_initial_value is not None:
        iv_ir = expr.func_initial_value.expr

        if newctx.expr_exposed and serialization_safe:
            # Serialization has changed the output type.
            with newctx.new() as ivctx:
                ivctx.expr_exposed = True
                iv = dispatch.compile(iv_ir, ctx=ivctx)
                iv = output.serialize_expr_if_needed(
                    iv, path_id=ir_set.path_id, ctx=ctx)
                set_expr = output.serialize_expr_if_needed(
                    set_expr, path_id=ir_set.path_id, ctx=ctx)
        else:
            iv = dispatch.compile(iv_ir, ctx=newctx)

        pathctx.put_path_value_var(stmt, ir_set.path_id, set_expr, env=ctx.env)
        pathctx.get_path_value_output(stmt, ir_set.path_id, env=ctx.env)

        with ctx.subrel() as subctx:
            wrapper = subctx.rel
            set_expr = pgast.CoalesceExpr(
                args=[stmt, iv], ser_safe=serialization_safe)

            pathctx.put_path_value_var(
                wrapper, ir_set.path_id, set_expr, env=ctx.env)
            stmt = wrapper

    pathctx.put_path_value_var_if_not_exists(
        stmt, ir_set.path_id, set_expr, env=ctx.env)

    rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return new_simple_set_rvar(ir_set, rvar)


def process_set_as_exists_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    with ctx.subrel() as subctx:
        wrapper = subctx.rel
        subctx.expr_exposed = False
        ir_expr = ir_set.expr.args[0].expr
        set_ref = dispatch.compile(ir_expr, ctx=subctx)

        pathctx.put_path_value_var(
            wrapper, ir_set.path_id, set_ref, env=ctx.env)
        pathctx.get_path_value_output(
            wrapper, ir_set.path_id, env=ctx.env)

        wrapper.where_clause = astutils.extend_binop(
            wrapper.where_clause, pgast.NullTest(arg=set_ref, negated=True))

        set_expr = pgast.SubLink(
            type=pgast.SubLinkType.EXISTS,
            expr=wrapper
        )

    pathctx.put_path_value_var(stmt, ir_set.path_id, set_expr, env=ctx.env)
    rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return new_simple_set_rvar(ir_set, rvar)


def build_array_expr(
        ir_expr: irast.Base,
        elements: typing.List[pgast.BaseExpr], *,
        ctx: context.CompilerContextLevel) -> pgast.Base:

    array = astutils.safe_array_expr(elements)

    if irutils.is_empty_array_expr(ir_expr):
        typeref = ir_expr.typeref

        if irtyputils.is_any(typeref.subtypes[0]):
            # The type of the input is not determined, which means that
            # the result of this expression is passed as an argument
            # to a generic function, e.g. `count(array_agg({}))`.  In this
            # case, amend the array type to a concrete type,
            # since Postgres balks at `[]::anyarray`.
            pg_type = ('text[]',)
        else:
            serialized = output.in_serialization_ctx(ctx=ctx)
            pg_type = pg_types.pg_type_from_ir_typeref(
                typeref, serialized=serialized)

        return pgast.TypeCast(
            arg=array,
            type_name=pgast.TypeName(
                name=pg_type,
            ),
        )
    else:
        return array


def process_set_as_array_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    elements = []
    s_elements = []
    serializing = output.in_serialization_ctx(ctx=ctx)

    for ir_element in ir_set.expr.elements:
        element = dispatch.compile(ir_element, ctx=ctx)
        elements.append(element)

        if serializing:
            s_var = pathctx.maybe_get_path_serialized_var(
                stmt, ir_element.path_id, env=ctx.env)

            if s_var is None:
                v_var = pathctx.get_path_value_var(
                    stmt, ir_element.path_id, env=ctx.env)
                s_var = output.serialize_expr(
                    v_var, path_id=ir_element.path_id, env=ctx.env)
            elif isinstance(s_var, pgast.TupleVar):
                s_var = output.serialize_expr(
                    s_var, path_id=ir_element.path_id, env=ctx.env)

            s_elements.append(s_var)

    set_expr = build_array_expr(ir_set.expr, elements, ctx=ctx)

    pathctx.put_path_value_var_if_not_exists(
        stmt, ir_set.path_id, set_expr, env=ctx.env)

    if serializing:
        s_set_expr = astutils.safe_array_expr(s_elements, ser_safe=True)

        if irutils.is_empty_array_expr(ir_set.expr):
            s_set_expr = pgast.TypeCast(
                arg=s_set_expr,
                type_name=pgast.TypeName(
                    name=pg_types.pg_type_from_ir_typeref(ir_set.expr.typeref)
                )
            )

        pathctx.put_path_serialized_var(
            stmt, ir_set.path_id, s_set_expr, env=ctx.env)

    rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return new_simple_set_rvar(ir_set, rvar)
