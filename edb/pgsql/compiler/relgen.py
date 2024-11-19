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


from __future__ import annotations
from typing import (
    Callable,
    Optional,
    Protocol,
    Tuple,
    Iterable,
    Collection,
    List,
    Set,
    NamedTuple,
    Generic,
    TypeVar,
    Type,
    cast,
)

import contextlib
import functools

from edb import errors

from edb.edgeql import qltypes

from edb.schema import objects as s_obj
from edb.schema import name as sn

from edb.edgeql import ast as qlast

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from edb.pgsql import ast as pgast
from edb.pgsql import common
from edb.pgsql import types as pg_types

from edb.common.typeutils import not_none

from . import astutils
from . import clauses
from . import context
from . import dispatch
from . import dml
from . import enums as pgce
from . import expr as exprcomp
from . import output
from . import pathctx
from . import relctx


class SetRVar:
    __slots__ = ('rvar', 'path_id', 'aspects')

    def __init__(
        self,
        rvar: pgast.PathRangeVar,
        path_id: irast.PathId,
        aspects: Iterable[pgce.PathAspect]=(pgce.PathAspect.VALUE,),
    ) -> None:
        self.aspects = aspects
        self.path_id = path_id
        self.rvar = rvar


class SetRVars:
    __slots__ = ('main', 'new')

    def __init__(self, main: SetRVar, new: List[SetRVar]) -> None:
        self.main = main
        self.new = new


def new_simple_set_rvar(
    ir_set: irast.Set,
    rvar: pgast.PathRangeVar,
    aspects: Iterable[pgce.PathAspect],
) -> SetRVars:
    srvar = SetRVar(rvar=rvar, path_id=ir_set.path_id, aspects=aspects)
    return SetRVars(main=srvar, new=[srvar])


def new_source_set_rvar(
    ir_set: irast.Set,
    rvar: pgast.PathRangeVar,
) -> SetRVars:
    aspects = [pgce.PathAspect.VALUE]
    if ir_set.path_id.is_objtype_path():
        aspects.append(pgce.PathAspect.SOURCE)

    return new_simple_set_rvar(ir_set, rvar, aspects)


def new_stmt_set_rvar(
    ir_set: irast.Set,
    stmt: pgast.Query,
    *,
    aspects: Optional[Iterable[pgce.PathAspect]]=None,
    ctx: context.CompilerContextLevel,
) -> SetRVars:
    rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    if aspects is not None:
        aspects = tuple(aspects)
    else:
        aspects = pathctx.list_path_aspects(stmt, ir_set.path_id)
    return new_simple_set_rvar(ir_set, rvar, aspects=aspects)


class OptionalRel(NamedTuple):

    scope_rel: pgast.SelectStmt
    target_rel: pgast.SelectStmt
    emptyrel: pgast.SelectStmt
    unionrel: pgast.SelectStmt
    wrapper: pgast.SelectStmt
    container: pgast.SelectStmt
    marker: str


def _lookup_set_rvar(
        ir_set: irast.Set, *,
        scope_stmt: Optional[pgast.SelectStmt]=None,
        ctx: context.CompilerContextLevel) -> Optional[pgast.PathRangeVar]:
    path_id = ir_set.path_id

    rvar = relctx.find_rvar(ctx.rel, source_stmt=scope_stmt,
                            path_id=path_id, ctx=ctx)

    if rvar is not None:
        return rvar

    # We couldn't find a regular rvar, but maybe we can find a packed one?
    packed_rvar = relctx.find_rvar(ctx.rel, flavor='packed',
                                   source_stmt=scope_stmt,
                                   path_id=path_id, ctx=ctx)

    if packed_rvar is not None:
        rvar = relctx.unpack_rvar(
            scope_stmt or ctx.rel,
            path_id, packed_rvar=packed_rvar, ctx=ctx)

        return rvar

    return None


def get_set_rvar(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> pgast.PathRangeVar:
    """Return a PathRangeVar for a given IR Set.

    Basically all of compilation comes through here for each set.

    @param ir_set: IR Set node.
    """
    path_id = ir_set.path_id

    scope_stmt = relctx.maybe_get_scope_stmt(path_id, ctx=ctx)
    if rvar := _lookup_set_rvar(ir_set, scope_stmt=scope_stmt, ctx=ctx):
        return rvar

    if ctx.toplevel_stmt is context.NO_STMT:
        # Top level query
        return _process_toplevel_query(ir_set, ctx=ctx)

    with contextlib.ExitStack() as cstack:

        # If there was a scope_stmt registered for our path, we compile
        # as a subrel of that scope_stmt. Otherwise we use whatever the
        # current rel was.
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

        is_empty_set = isinstance(ir_set.expr, irast.EmptySet)

        path_scope = relctx.get_scope(ir_set, ctx=subctx)
        new_scope = path_scope or subctx.scope_tree
        is_optional = (
            subctx.scope_tree.is_optional(path_id) or
            new_scope.is_optional(path_id) or
            path_id in subctx.force_optional
        ) and not can_omit_optional_wrapper(ir_set, new_scope, ctx=ctx)

        optional_wrapping = is_optional and not is_empty_set

        if optional_wrapping:
            stmt, optrel = prepare_optional_rel(
                ir_set=ir_set, stmt=stmt, ctx=subctx)
            subctx.pending_query = subctx.rel = stmt

        # XXX: This is pretty dodgy, because it updates the path_scope
        # *before* we call new_child() on it. Removing it only breaks two
        # tests of lprops on backlinks.
        if path_scope and path_scope.is_visible(path_id):
            subctx.path_scope[path_id] = scope_stmt

        # If this set has a scope in the scope tree associated with it,
        # register paths in that scope to be compiled with this stmt
        # as their scope_stmt.
        if path_scope:
            relctx.update_scope(ir_set, stmt, ctx=subctx)

        # Actually compile the set
        rvars = _get_expr_set_rvar(ir_set.expr, ir_set, ctx=subctx)
        relctx.update_scope_masks(ir_set, rvars.main.rvar, ctx=subctx)

        if ctx.env.is_explain:
            for srvar in rvars.new:
                if not srvar.rvar.ir_origins:
                    srvar.rvar.ir_origins = []
                srvar.rvar.ir_origins.append(ir_set)

        if optional_wrapping:
            rvars = finalize_optional_rel(ir_set, optrel=optrel,
                                          rvars=rvars, ctx=subctx)
            relctx.update_scope_masks(ir_set, rvars.main.rvar, ctx=subctx)
        elif not is_optional and is_empty_set:
            # In most cases it is totally fine for us to represent an
            # empty set as an empty relation.
            # (except when it needs to be fed to an optional argument)
            null_query = rvars.main.rvar.query
            assert isinstance(
                null_query, (pgast.SelectStmt, pgast.NullRelation))
            null_query.where_clause = pgast.BooleanConstant(val=False)

        result_rvar = _include_rvars(rvars, scope_stmt=scope_stmt, ctx=subctx)
        for aspect in rvars.main.aspects:
            pathctx.put_path_rvar_if_not_exists(
                ctx.rel,
                path_id,
                result_rvar,
                aspect=aspect,
            )

    return result_rvar


def _include_rvars(
    rvars: SetRVars,
    *,
    scope_stmt: pgast.SelectStmt,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:
    for set_rvar in rvars.new:
        # overwrite_path_rvar is needed because we want
        # the outermost Set with the given path_id to
        # represent the path.  Nested Sets with the
        # same path_id but different expression are
        # possible when there is a computable pointer
        # that refers to itself in its expression.
        relctx.include_specific_rvar(
            scope_stmt,
            set_rvar.rvar,
            path_id=set_rvar.path_id,
            overwrite_path_rvar=True,
            aspects=set_rvar.aspects,
            ctx=ctx,
        )

    return rvars.main.rvar


def _process_toplevel_query(
    ir_set: irast.Set,
    *,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:

    relctx.init_toplevel_query(ir_set, ctx=ctx)
    rvars = _get_expr_set_rvar(ir_set.expr, ir_set, ctx=ctx)
    if isinstance(ir_set.expr, irast.EmptySet):
        # In cases where the top-level expression is an empty set
        # as opposed to a Set wrapping some expression or path, make
        # sure the generated empty rel gets selected in the toplevel
        # SelectStmt.
        result_rvar = _include_rvars(rvars, scope_stmt=ctx.rel, ctx=ctx)
        for aspect in rvars.main.aspects:
            pathctx.put_path_rvar_if_not_exists(
                ctx.rel,
                ir_set.path_id,
                result_rvar,
                aspect=aspect,
            )
    else:
        result_rvar = rvars.main.rvar

    return result_rvar


class _SpecialCaseFunc(Protocol):
    def __call__(
        self, ir_set: irast.SetE[irast.Call],
        *,
        ctx: context.CompilerContextLevel,
    ) -> SetRVars:
        pass


class _FunctionSpecialCase(NamedTuple):
    func: _SpecialCaseFunc
    only_as_fallback: bool


_SPECIAL_FUNCTIONS: dict[str, _FunctionSpecialCase] = {}


def _special_case(name: str, only_as_fallback: bool = False) -> Callable[
    [_SpecialCaseFunc], _SpecialCaseFunc
]:
    def func(f: _SpecialCaseFunc) -> _SpecialCaseFunc:
        _SPECIAL_FUNCTIONS[name] = _FunctionSpecialCase(f, only_as_fallback)
        return f

    return func


class _SimpleSpecialCaseFunc(Protocol):
    def __call__(
        self, expr: irast.FunctionCall, *, ctx: context.CompilerContextLevel
    ) -> pgast.BaseExpr:
        pass


_SIMPLE_SPECIAL_FUNCTIONS: dict[str, _SimpleSpecialCaseFunc] = {}


def simple_special_case(
    name: str,
) -> Callable[[_SimpleSpecialCaseFunc], _SimpleSpecialCaseFunc]:
    def func(f: _SimpleSpecialCaseFunc) -> _SimpleSpecialCaseFunc:
        _SIMPLE_SPECIAL_FUNCTIONS[name] = f
        return f

    return func


# Dispatcher for _get_set_rvar implementations for different expressions.
# The implementations just take a SetE[T] for some T, so register_get_rvar
# needs to do some wrapping.
@functools.singledispatch
def _get_expr_set_rvar(
    expr: irast.Expr,
    ir: irast.Set,
    *,
    ctx: context.CompilerContextLevel,
) -> SetRVars:
    raise NotImplementedError(f'no relgen handler for {ir.__class__}')


T_expr = TypeVar('T_expr', contravariant=True, bound=irast.Expr)


class _GetExprRvarFunc(Protocol, Generic[T_expr]):
    def __call__(
        self, __ir_set: irast.SetE[T_expr], *, ctx: context.CompilerContextLevel
    ) -> SetRVars:
        pass


def register_get_rvar(
    typ: Type[T_expr],
) -> Callable[[_GetExprRvarFunc[T_expr]], _GetExprRvarFunc[T_expr]]:
    def func(f: _GetExprRvarFunc[T_expr]) -> _GetExprRvarFunc[T_expr]:
        _get_expr_set_rvar.register(typ)(
            lambda _, ir, *, ctx: f(ir, ctx=ctx))
        return f

    return func


def _get_source_rvar(
    ir_set: irast.Set,
    scope_stmt: pgast.SelectStmt,
    *,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:
    is_optional = (
        ctx.scope_tree.is_optional(ir_set.path_id) or
        ir_set.path_id in ctx.force_optional
    )

    if not is_optional:
        rvar = relctx.new_root_rvar(ir_set, lateral=True, ctx=ctx)
        relctx.include_rvar(
            scope_stmt, rvar, path_id=ir_set.path_id, ctx=ctx
        )
    else:
        # If the path is optional in the context we are in, then we
        # need to put optional wrapping around the join with the base table.
        with ctx.subrel() as subctx:
            stmt, optrel = prepare_optional_rel(
                ir_set=ir_set, stmt=subctx.rel, ctx=subctx)
            subctx.pending_query = subctx.rel = stmt

            rvar = relctx.new_root_rvar(ir_set, lateral=True, ctx=subctx)
            rvars = new_source_set_rvar(ir_set, rvar)
            rvars = finalize_optional_rel(
                ir_set, optrel=optrel, rvars=rvars, ctx=subctx)

            rvar = _include_rvars(rvars, scope_stmt=scope_stmt, ctx=ctx)

    return rvar


def ensure_source_rvar(
    ir_set: irast.Set,
    stmt: pgast.Query,
    *,
    ctx: context.CompilerContextLevel,
) -> pgast.PathRangeVar:
    """Make sure that a source aspect is available for ir_set.

    If no aspect is available, compile it. If value/identity is available
    but source is not, select from the base relation and join it in.
    """

    rvar = relctx.maybe_get_path_rvar(
        stmt, ir_set.path_id, aspect=pgce.PathAspect.SOURCE, ctx=ctx)
    if rvar is None:
        get_set_rvar(ir_set, ctx=ctx)

    rvar = relctx.maybe_get_path_rvar(
        stmt, ir_set.path_id, aspect=pgce.PathAspect.SOURCE, ctx=ctx)
    if rvar is None:
        scope_stmt = relctx.maybe_get_scope_stmt(ir_set.path_id, ctx=ctx)
        if scope_stmt is None:
            scope_stmt = ctx.rel
        rvar = relctx.maybe_get_path_rvar(
            scope_stmt,
            ir_set.path_id,
            aspect=pgce.PathAspect.SOURCE,
            ctx=ctx,
        )
        if rvar is None:
            if irtyputils.is_free_object(ir_set.path_id.target):
                # Free objects don't have a real source, and
                # generating a new fake source doesn't work because
                # the ids don't match, so instead we call the existing
                # value rvar a source.
                rvar = relctx.get_path_rvar(
                    scope_stmt,
                    ir_set.path_id,
                    aspect=pgce.PathAspect.VALUE,
                    ctx=ctx,
                )
            else:
                rvar = _get_source_rvar(ir_set, scope_stmt, ctx=ctx)
            pathctx.put_path_rvar(
                stmt, ir_set.path_id, rvar, aspect=pgce.PathAspect.SOURCE,
            )

    return rvar


def set_as_subquery(
        ir_set: irast.Set, *,
        as_value: bool=False,
        explicit_cast: Optional[Tuple[str, ...]] = None,
        ctx: context.CompilerContextLevel) -> pgast.Query:
    # Compile *ir_set* into a subquery as follows:
    #     (
    #         SELECT <set_rel>.v
    #         FROM <set_rel>
    #     )
    with ctx.subrel() as subctx:
        wrapper = subctx.rel
        wrapper.name = ctx.env.aliases.get('set_as_subquery')
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

                if explicit_cast is not None:
                    value = pgast.TypeCast(
                        arg=value,
                        type_name=pgast.TypeName(name=explicit_cast),
                    )

                wrapper.target_list = [
                    pgast.ResTarget(val=value)
                ]
        else:
            pathctx.get_path_value_output(
                rel=wrapper, path_id=ir_set.path_id, env=ctx.env)

    return wrapper


def can_omit_optional_wrapper(
        ir_set: irast.Set, new_scope: irast.ScopeTreeNode, *,
        ctx: context.CompilerContextLevel) -> bool:
    """Determine whether it is safe to omit the optional wrapper.

    Doing so is safe when the expression is guarenteed to result in
    a NULL and not an empty set.

    The main such case implemented is a path `foo.bar` where foo
    is visible and bar is a single non-computed property, which we know
    will be stored as NULL in the database.

    We also handle trivial SELECTs wrapping such an expression.
    """
    if ir_set.expr and irutils.is_trivial_select(ir_set.expr):
        return can_omit_optional_wrapper(
            ir_set.expr.result,
            relctx.get_scope(ir_set.expr.result, ctx=ctx) or new_scope,
            ctx=ctx,
        )

    if isinstance(ir_set.expr, irast.Parameter):
        return True

    # Our base json casts should all preserve nullity (instead of
    # turning it into an empty set), so allow passing through those
    # cases. This is mainly an optimization for passing globals to
    # functions, where we need to convert a bunch of optional params
    # to json, and for casting out of json there and in schema updates.
    if (
        isinstance(ir_set.expr, irast.TypeCast)
        and ((
            irtyputils.is_scalar(ir_set.expr.expr.typeref)
            and irtyputils.is_json(ir_set.expr.to_type)
        ) or (
            irtyputils.is_json(ir_set.expr.expr.typeref)
            and irtyputils.is_scalar(ir_set.expr.to_type)
        ))
    ):
        return can_omit_optional_wrapper(
            ir_set.expr.expr,
            relctx.get_scope(ir_set.expr.expr, ctx=ctx) or new_scope,
            ctx=ctx,
        )

    if isinstance(ir_set.expr, irast.TupleIndirectionPointer):
        return can_omit_optional_wrapper(ir_set.expr.source, new_scope, ctx=ctx)

    return bool(
        isinstance(ir_set.expr, irast.Pointer)
        and (rptr := ir_set.expr)
        and rptr.expr is None
        and not ir_set.path_id.is_objtype_path()
        and not ir_set.path_id.is_type_intersection_path()
        and new_scope.is_visible(rptr.source.path_id)
        and not rptr.is_inbound
        and rptr.ptrref.out_cardinality.is_single()
        and not rptr.ptrref.is_computable
    )


def prepare_optional_rel(
        *, ir_set: irast.Set, stmt: pgast.SelectStmt,
        ctx: context.CompilerContextLevel) \
        -> Tuple[pgast.SelectStmt, OptionalRel]:

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
                    empty_ir = irast.Set(
                        path_id=ir_set.path_id,
                        typeref=ir_set.typeref,
                        expr=irast.EmptySet(typeref=ir_set.typeref),
                    )

                    emptyrvar = relctx.new_empty_rvar(
                        cast('irast.SetE[irast.EmptySet]', empty_ir),
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
                setrel, ir_set.path_id, rvars.main.rvar, aspect=aspect
            )

        lvar = pathctx.get_path_value_var(
            setrel, path_id=ir_set.path_id, env=subctx.env)

        if lvar.nullable:
            # The left var is still nullable, which may be the
            # case for non-required singleton properties.
            # Filter out NULLs.
            setrel.where_clause = astutils.extend_binop(
                setrel.where_clause,
                pgast.NullTest(
                    arg=lvar, negated=True
                )
            )

    unionrel = optrel.unionrel
    union_rvar = relctx.rvar_for_rel(unionrel, lateral=True, ctx=ctx)

    with ctx.new() as subctx:
        subctx.rel = wrapper = optrel.wrapper
        relctx.include_rvar(wrapper, union_rvar, ir_set.path_id, ctx=subctx)

    with ctx.new() as subctx:
        subctx.rel = stmt = optrel.container
        wrapper_rvar = relctx.rvar_for_rel(wrapper, lateral=True, ctx=subctx)

        relctx.include_rvar(stmt, wrapper_rvar, ir_set.path_id, ctx=subctx)

        stmt.where_clause = astutils.extend_binop(
            stmt.where_clause,
            astutils.get_column(wrapper_rvar, optrel.marker, nullable=False))

        stmt.nullable = True

    sub_rvar = SetRVar(rvar=relctx.new_rel_rvar(ir_set, stmt, ctx=ctx),
                       path_id=ir_set.path_id,
                       aspects=rvars.main.aspects)

    return SetRVars(main=sub_rvar, new=[sub_rvar])


def get_set_rel_alias(ir_set: irast.Set, *,
                      ctx: context.CompilerContextLevel) -> str:
    dname = ir_set.path_id.target_name_hint.name
    if (
        isinstance(ir_set.expr, irast.Pointer)
        and ir_set.expr.source.typeref is not None
    ):
        alias_hint = '{}_{}'.format(
            dname,
            ir_set.expr.ptrref.shortname.name
        )
    else:
        alias_hint = dname.replace('~', '-')

    return alias_hint


# N.B: registered for get_rvar for TypeRoot below
def process_set_as_root(
    ir_set: irast.Set, *, ctx: context.CompilerContextLevel
) -> SetRVars:

    # TODO(ir): Represent these as something other than TypeRoot?
    if ir_set.path_id in ctx.external_rels:
        return process_external_rel(ir_set, ctx=ctx)

    assert not ir_set.is_visible_binding_ref, (
        f"Can't compile ref to visible binding root {ir_set.path_id}"
    )

    rvar = relctx.new_root_rvar(ir_set, ctx=ctx)
    return new_source_set_rvar(ir_set, rvar)


register_get_rvar(irast.TypeRoot)(process_set_as_root)


@register_get_rvar(irast.VisibleBindingExpr)
def process_set_as_visible_binding(
    ir_set: irast.SetE[irast.VisibleBindingExpr],
    *, ctx: context.CompilerContextLevel
) -> SetRVars:
    raise AssertionError(
        f"Can't compile ref to visible binding {ir_set.path_id}"
    )


@register_get_rvar(irast.InlinedParameterExpr)
def process_set_as_inlined_parameter(
    ir_set: irast.SetE[irast.InlinedParameterExpr],
    *, ctx: context.CompilerContextLevel
) -> SetRVars:
    raise AssertionError(
        f"Can't compile ref to inline parameter {ir_set.path_id}"
    )


@register_get_rvar(irast.EmptySet)
def process_set_as_empty(
    ir_set: irast.SetE[irast.EmptySet], *, ctx: context.CompilerContextLevel
) -> SetRVars:

    rvar = relctx.new_empty_rvar(ir_set, ctx=ctx)
    return new_source_set_rvar(ir_set, rvar)


def process_external_rel(
    ir_set: irast.Set, *, ctx: context.CompilerContextLevel
) -> SetRVars:
    rel, aspects = ctx.external_rels[ir_set.path_id]
    for a in aspects:
        assert isinstance(a, str)

    rvar = relctx.rvar_for_rel(rel, ctx=ctx)
    return new_simple_set_rvar(ir_set, rvar, aspects)


def process_set_as_link_property_ref(
    ir_set: irast.SetE[irast.Pointer], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    rptr = ir_set.expr
    ir_source = rptr.source
    rvars = []

    lpropref = rptr.ptrref
    ptr_info = pg_types.get_ptrref_storage_info(
        lpropref, resolve_type=False, link_bias=False)

    if (ptr_info.table_type == 'ObjectType' or
            str(lpropref.std_parent_name) == 'std::target'):
        # This is a singleton link property stored in source rel,
        # e.g. @target
        src_rvar = get_set_rvar(ir_source, ctx=ctx)

        val = pathctx.get_rvar_path_var(
            src_rvar,
            ir_source.path_id,
            aspect=pgce.PathAspect.VALUE,
            env=ctx.env,
        )

        pathctx.put_rvar_path_output(
            src_rvar, ir_set.path_id, aspect=pgce.PathAspect.VALUE, var=val
        )

        return SetRVars(
            main=SetRVar(rvar=src_rvar, path_id=ir_set.path_id), new=[])

    with ctx.new() as newctx:
        link_path_id = ir_set.path_id.src_path()
        assert link_path_id is not None

        rptr_specialization: Optional[Set[irast.PointerRef]] = None

        if link_path_id.is_type_intersection_path():
            rptr_specialization = set()
            link_prefix, ind_ptrs = (
                irutils.collapse_type_intersection(ir_source))
            for ind_ptr in ind_ptrs:
                rptr_specialization.update(ind_ptr.ptrref.rptr_specialization)
        else:
            link_prefix = ir_source

        source_scope_stmt = relctx.maybe_get_scope_stmt(
            ir_source.path_id, ctx=ctx
        ) or ctx.rel
        link_rvar = pathctx.maybe_get_path_rvar(
            source_scope_stmt, link_path_id, aspect=pgce.PathAspect.SOURCE
        )

        if link_rvar is None:
            src_rvar = get_set_rvar(ir_source, ctx=newctx)
            assert irutils.is_set_instance(link_prefix, irast.Pointer)
            link_rvar = relctx.new_pointer_rvar(
                link_prefix, src_rvar=src_rvar,
                link_bias=True, ctx=newctx)
            # Make sure the link rvar understands the path_id we are using.
            # (FIXME: Would it be better to pass this in to new_pointer_rvar?)
            pathctx.put_path_bond(link_rvar.query, link_path_id.tgt_path())
            var = pathctx.get_rvar_path_identity_var(
                link_rvar, link_prefix.path_id, env=ctx.env)
            pathctx.put_rvar_path_output(
                link_rvar,
                link_path_id.tgt_path(),
                pgce.PathAspect.IDENTITY,
                var,
            )

        if astutils.is_set_op_query(link_rvar.query):
            # If we have an rptr_specialization, then this is a link
            # property reference to a link union narrowed by a type
            # intersection.  We already know which union components
            # match the indirection expression, and can route the link
            # property references to correct UNION subqueries.
            ptr_ids = (
                {spec.id for spec in rptr_specialization}
                if rptr_specialization is not None else None
            )
            if ptr_ids and rptr_specialization:
                ptr_ids.update(
                    x.id for spec in rptr_specialization
                    for x in spec.descendants()
                    if isinstance(x, irast.PointerRef)
                )

            for subquery in astutils.each_query_in_set(link_rvar.query):
                if isinstance(subquery, pgast.SelectStmt):
                    rvar = subquery.from_clause[0]
                    assert isinstance(rvar, pgast.PathRangeVar)
                    if ptr_ids is None or rvar.schema_object_id in ptr_ids:
                        pathctx.put_path_source_rvar(
                            subquery, link_path_id, rvar
                        )
                        continue
                # Spare get_path_var() from attempting to rebalance
                # the UNION by recording an explicit NULL as as the
                # link property var.
                pathctx.put_path_value_var(
                    subquery,
                    ir_set.path_id,
                    pgast.TypeCast(
                        arg=pgast.NullConstant(),
                        type_name=pgast.TypeName(
                            name=pg_types.pg_type_from_ir_typeref(
                                ir_set.typeref),
                        ),
                    ),
                )
        elif isinstance(link_rvar.query, pgast.SelectStmt):
            # When processing link properties into a CTE, map the current link
            # path id into the form used by the CTE.

            for from_rvar in link_rvar.query.from_clause:
                if (
                    isinstance(from_rvar, pgast.RelRangeVar)
                    and isinstance(from_rvar.relation, pgast.CommonTableExpr)
                    and from_rvar.relation.query.path_id is not None
                ):
                    pathctx.put_path_id_map(
                        link_rvar.query,
                        link_path_id,
                        from_rvar.relation.query.path_id
                    )

        rvars.append(SetRVar(
            link_rvar,
            link_path_id,
            aspects=[pgce.PathAspect.VALUE, pgce.PathAspect.SOURCE],
        ))

    return SetRVars(main=SetRVar(link_rvar, ir_set.path_id), new=rvars)


@register_get_rvar(irast.TypeIntersectionPointer)
def process_set_as_path_type_intersection(
    ir_set: irast.SetE[irast.TypeIntersectionPointer],
    *,
    ctx: context.CompilerContextLevel,
) -> SetRVars:
    rptr = ir_set.expr
    ir_source = rptr.source
    source_is_visible = ctx.scope_tree.is_visible(ir_source.path_id)
    stmt = ctx.rel

    assert not rptr.expr, 'type intersection pointer with expr??'

    if irtyputils.is_empty_typeref(ir_set.typeref):
        # If the typeref was a type expression which resolves to no actual
        # types, just return an empty set.
        empty_ir = irast.Set(
            path_id=ir_set.path_id,
            typeref=ir_set.typeref,
            expr=irast.EmptySet(typeref=ir_set.typeref),
        )
        source_rvar = relctx.new_empty_rvar(
            cast('irast.SetE[irast.EmptySet]', empty_ir),
            ctx=ctx)
        relctx.include_rvar(stmt, source_rvar, ir_set.path_id, ctx=ctx)

    elif (not source_is_visible
            and isinstance(ir_source.expr, irast.Pointer)
            and not ir_source.path_id.is_type_intersection_path()
            and not ir_source.expr.expr
            and (
                rptr.ptrref.is_subtype
                or pg_types.get_ptrref_storage_info(
                    ir_source.expr.ptrref).table_type != 'ObjectType'
            )):
        # Otherwise, if the source link path is not visible,
        # and this is a subtype intersection, or the pointer is not inline,
        # we have an opportunity to opmimize the target join by
        # directly replacing the target type.
        with ctx.new() as subctx:
            subctx.intersection_narrowing = (
                subctx.intersection_narrowing.copy())
            subctx.intersection_narrowing[ir_source] = ir_set
            source_rvar = get_set_rvar(ir_source, ctx=subctx)

        pathctx.put_path_id_map(stmt, ir_set.path_id, ir_source.path_id)
        relctx.include_rvar(stmt, source_rvar, ir_set.path_id, ctx=ctx)

    else:
        source_rvar = get_set_rvar(ir_source, ctx=ctx)

        poly_rvar = relctx.range_for_typeref(
            rptr.ptrref.out_target,
            path_id=ir_set.path_id,
            dml_source=irutils.get_dml_sources(ir_set),
            lateral=True,
            ctx=ctx,
        )

        prefix_path_id = ir_set.path_id.src_path()
        assert prefix_path_id is not None, 'expected a path'

        relctx.deep_copy_primitive_rvar_path_var(
            ir_set.path_id, prefix_path_id, poly_rvar, env=ctx.env)
        pathctx.put_rvar_path_bond(poly_rvar, prefix_path_id)
        relctx.include_rvar(stmt, poly_rvar, ir_set.path_id, ctx=ctx)
        int_rvar = pgast.IntersectionRangeVar(
            component_rvars=[
                source_rvar,
                poly_rvar,
            ]
        )

        if isinstance(source_rvar.query, pgast.Query):
            pathctx.put_path_id_map(
                source_rvar.query, ir_set.path_id, ir_source.path_id)

        for aspect in (pgce.PathAspect.SOURCE, pgce.PathAspect.VALUE):
            pathctx.put_path_rvar(
                stmt,
                ir_source.path_id,
                source_rvar,
                aspect=aspect,
            )

            pathctx.put_path_rvar(
                stmt,
                ir_set.path_id,
                int_rvar,
                aspect=aspect,
            )

    rvars = new_stmt_set_rvar(
        ir_set,
        stmt,
        aspects=[pgce.PathAspect.VALUE, pgce.PathAspect.SOURCE],
        ctx=ctx,
    )
    # If the inner set also exposes a pointer path source, we need to
    # also expose a pointer path source. See tests like
    # test_edgeql_for_lprop_02, where it is needed to to make FOR binding
    # of backlinks work.
    if pathctx.maybe_get_path_rvar(
        stmt,
        ir_source.path_id.ptr_path(),
        aspect=pgce.PathAspect.SOURCE,
    ):
        rvars.new.append(
            SetRVar(
                rvars.main.rvar,
                ir_set.path_id.ptr_path(),
                aspects=(pgce.PathAspect.SOURCE,),
            )
        )

    return rvars


def _source_path_needs_semi_join(
        ir_source: irast.Set,
        ctx: context.CompilerContextLevel) -> bool:
    """Check if the path might need a semi-join

    It does not need one if it has a visible prefix followed by single
    pointers. Otherwise it might.

    This is an optimization that allows us to avoid doing a semi-join
    when there is a chain of single links referenced (probably in a filter
    or a computable).

    """
    if ctx.scope_tree.is_visible(ir_source.path_id):
        return False

    while (
        isinstance(ir_source.expr, irast.Pointer)
        and ir_source.expr.dir_cardinality.is_single()
        and not ir_source.expr.expr
    ):
        ir_source = ir_source.expr.source

        if ctx.scope_tree.is_visible(ir_source.path_id):
            return False

    return True


@register_get_rvar(irast.Pointer)
def process_set_as_path(
    ir_set: irast.SetE[irast.Pointer],
    *, ctx: context.CompilerContextLevel
) -> SetRVars:
    if ir_set.expr.expr:
        return process_set_as_subquery(ir_set, ctx=ctx)

    rptr = ir_set.expr
    ptrref = rptr.ptrref
    ir_source = rptr.source
    stmt = ctx.rel

    source_is_visible = ctx.scope_tree.is_visible(ir_source.path_id)
    rvars = []

    ptr_info = pg_types.get_ptrref_storage_info(
        ptrref, resolve_type=False, link_bias=False, allow_missing=True)

    # Path is a link property.
    is_linkprop = ptrref.source_ptr is not None
    is_primitive_ref = not irtyputils.is_object(ptrref.out_target)
    # Path is a reference to a relationship stored in the source table.
    is_inline_ref = bool(ptr_info and ptr_info.table_type == 'ObjectType')
    is_inline_primitive_ref = is_inline_ref and is_primitive_ref
    is_id_ref_to_inline_source = False

    semi_join = (
        ir_set.path_id not in ctx.disable_semi_join and
        not (is_linkprop or is_primitive_ref) and
        _source_path_needs_semi_join(ir_source, ctx=ctx) and
        # This is an optimization for when we are inside of a semi-join on
        # a computable: process_set_as_subquery will have included an
        # rvar for the computable source, and we want to join on it
        # instead of semi-joining.
        not relctx.find_rvar(stmt, path_id=ir_source.path_id, ctx=ctx)
    )

    if irtyputils.is_empty_typeref(ir_source.typeref):
        # If the source is an empty type intersection, just produce an empty set

        if is_primitive_ref:
            aspects = [pgce.PathAspect.VALUE]
        else:
            aspects = [pgce.PathAspect.VALUE, pgce.PathAspect.SOURCE]

        empty_ir = irast.Set(
            path_id=ir_set.path_id,
            typeref=ir_set.typeref,
            expr=irast.EmptySet(typeref=ir_set.typeref),
        )
        empty_rvar = SetRVar(
            relctx.new_empty_rvar(
                cast('irast.SetE[irast.EmptySet]', empty_ir),
                ctx=ctx
            ),
            path_id=ir_set.path_id,
            aspects=aspects,
        )
        return SetRVars(main=empty_rvar, new=[empty_rvar])

    main_rvar = None
    source_rptr = (
        ir_source.expr if isinstance(ir_source.expr, irast.Pointer) else None)
    if (irtyputils.is_id_ptrref(ptrref) and source_rptr is not None
            and isinstance(source_rptr.ptrref, irast.PointerRef)
            and not source_rptr.is_inbound
            and not irtyputils.is_computable_ptrref(source_rptr.ptrref)
            and not irutils.is_type_intersection_reference(ir_set)
            and not pathctx.link_needs_type_rewrite(
                ir_source.typeref, env=ctx.env)):

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
            # applied if the source is a type intersection, e.g
            # __type__[IS Array].id, or if Foo is not visible in
            # this scope.
            source_ptr_info = pg_types.get_ptrref_storage_info(
                source_rptr.ptrref, resolve_type=False, link_bias=False,
                allow_missing=True)
            is_id_ref_to_inline_source = bool(
                source_ptr_info and source_ptr_info.table_type == 'ObjectType')

    if semi_join:
        with ctx.subrel() as srcctx:
            srcctx.expr_exposed = False
            src_rvar = get_set_rvar(ir_source, ctx=srcctx)
            # semi_join needs a source rvar, so make sure we have one.
            # (The returned one won't be a source rvar if it comes
            # from a function, for example)
            if not ir_source.path_id.is_type_intersection_path():
                src_rvar = ensure_source_rvar(ir_source, stmt, ctx=srcctx)
            set_rvar = relctx.semi_join(stmt, ir_set, src_rvar, ctx=srcctx)
            rvars.append(SetRVar(
                set_rvar,
                ir_set.path_id,
                [pgce.PathAspect.VALUE, pgce.PathAspect.SOURCE]
            ))

    elif is_id_ref_to_inline_source:
        assert source_rptr is not None
        ir_source = source_rptr.source
        src_rvar = get_set_rvar(ir_source, ctx=ctx)

    elif not source_is_visible:
        with ctx.subrel() as srcctx:
            srcctx.expr_exposed = False

            get_set_rvar(ir_source, ctx=srcctx)

            if is_inline_primitive_ref:
                # Semi-join variant for inline scalar links,
                # which is, essentially, just filtering out NULLs.
                ensure_source_rvar(ir_source, srcctx.rel, ctx=srcctx)

                var = pathctx.get_path_value_var(
                    srcctx.rel, path_id=ir_set.path_id, env=ctx.env)
                if var.nullable:
                    srcctx.rel.where_clause = astutils.extend_binop(
                        srcctx.rel.where_clause,
                        pgast.NullTest(arg=var, negated=True))

        srcrel = srcctx.rel
        src_rvar = relctx.rvar_for_rel(srcrel, lateral=True, ctx=srcctx)
        relctx.include_rvar(stmt, src_rvar, path_id=ir_source.path_id, ctx=ctx)
        pathctx.put_path_id_mask(stmt, ir_source.path_id)

    # Path is a reference to a link property.
    if is_linkprop:
        srvars = process_set_as_link_property_ref(ir_set, ctx=ctx)
        main_rvar = srvars.main
        rvars.extend(srvars.new)

    elif is_id_ref_to_inline_source:
        main_rvar = SetRVar(
            ensure_source_rvar(ir_source, stmt, ctx=ctx),
            path_id=ir_set.path_id,
            aspects=[pgce.PathAspect.VALUE]
        )

    elif is_inline_primitive_ref:
        # There is an opportunity to also expose the "source" aspect
        # for tuple refs here, but that requires teaching pathctx about
        # complex field indirections, so rely on tuple_getattr()
        # fallback for tuple properties for now.
        main_rvar = SetRVar(
            ensure_source_rvar(ir_source, stmt, ctx=ctx),
            path_id=ir_set.path_id,
            aspects=[pgce.PathAspect.VALUE]
        )
        rvars = [main_rvar]

    elif not semi_join:
        # Link range.
        if is_inline_ref:
            aspects = [pgce.PathAspect.VALUE]
            # If this is a link that is stored inline, make sure
            # the source aspect is actually accessible (not just value).
            src_rvar = ensure_source_rvar(ir_source, stmt, ctx=ctx)
            # In case the source is visible (so the codepath below
            # that uses the current statement doesn't trigger) but the
            # source aspect wasn't available, make sure we include it
            # in our return. This can come up with __old__ in triggers.
            if source_is_visible:
                rvars.append(SetRVar(
                    src_rvar,
                    path_id=ir_source.path_id,
                    aspects=[pgce.PathAspect.SOURCE]
                ))
        else:
            aspects = [pgce.PathAspect.VALUE, pgce.PathAspect.SOURCE]
            src_rvar = get_set_rvar(ir_source, ctx=ctx)

        map_rvar = SetRVar(
            relctx.new_pointer_rvar(ir_set, src_rvar=src_rvar, ctx=ctx),
            path_id=ir_set.path_id.ptr_path(),
            aspects=aspects
        )

        rvars.append(map_rvar)

        # Target set range.
        if irtyputils.is_object(ir_set.typeref):
            target_rvar = relctx.new_root_rvar(ir_set, lateral=True, ctx=ctx)

            main_rvar = SetRVar(
                target_rvar,
                path_id=ir_set.path_id,
                aspects=[pgce.PathAspect.VALUE, pgce.PathAspect.SOURCE]
            )

            rvars.append(main_rvar)
        else:
            main_rvar = SetRVar(
                map_rvar.rvar,
                path_id=ir_set.path_id,
                aspects=[pgce.PathAspect.VALUE],
            )
            rvars.append(main_rvar)

    if not source_is_visible:
        # If the source path is not visible in the current scope,
        # it means that there are no other paths sharing this path prefix
        # in this scope.  In such cases the path is represented by a subquery
        # rather than a simple set of ranges.
        for srvar in rvars:
            relctx.include_specific_rvar(
                stmt, srvar.rvar, path_id=srvar.path_id,
                aspects=srvar.aspects, ctx=ctx)

        if is_primitive_ref:
            aspects = [pgce.PathAspect.VALUE]
        else:
            aspects = [pgce.PathAspect.VALUE, pgce.PathAspect.SOURCE]

        main_rvar = SetRVar(
            relctx.new_rel_rvar(ir_set, stmt, ctx=ctx),
            path_id=ir_set.path_id,
            aspects=aspects,
        )

        rvars = [main_rvar]

    assert main_rvar

    return SetRVars(main=main_rvar, new=rvars)


def _new_subquery_stmt_set_rvar(
    ir_set: irast.Set,
    stmt: pgast.Query,
    *,
    ctx: context.CompilerContextLevel,
) -> SetRVars:
    aspects = pathctx.list_path_aspects(stmt, ir_set.path_id)
    if ir_set.path_id.is_tuple_path():
        # If we are wrapping a tuple expression, make sure not to
        # over-represent it in terms of the exposed aspects.
        aspects -= {pgce.PathAspect.SERIALIZED}

    return new_stmt_set_rvar(
        ir_set, stmt, aspects=aspects, ctx=ctx)


def _lookup_set_rvar_in_source(
        ir_set: irast.Set,
        src_rvar: Optional[pgast.PathRangeVar], *,
        ctx: context.CompilerContextLevel) -> Optional[pgast.PathRangeVar]:
    if not (
        ir_set.is_materialized_ref
        and isinstance(src_rvar, pgast.RangeSubselect)
    ):
        return None

    if pathctx.maybe_get_path_value_var(
        src_rvar.subquery, ir_set.path_id, env=ctx.env
    ):
        return src_rvar

    # When looking for an packed value in our source rvar, we need to
    # account for the fact that unpack_rvar names all of its outputs
    # based solely on the source--that is, if any of the pointer paths
    # have extra namespaces on them, they won't appear. Rebuild the
    # path_id without any namespaces that aren't on the src_path.
    path_id = ir_set.path_id
    path_id = not_none(path_id.src_path()).extend(
        ptrref=not_none(path_id.rptr()),
        direction=not_none(path_id.rptr_dir()),
    )
    if packed_ref := pathctx.maybe_get_rvar_path_var(
        src_rvar,
        pathctx.map_path_id(
            path_id,
            src_rvar.subquery.view_path_id_map,
        ),
        aspect=pgce.PathAspect.VALUE,
        flavor='packed',
        env=ctx.env,
    ):
        return relctx.unpack_var(
            ctx.rel, ir_set.path_id, ref=packed_ref, ctx=ctx)
    return None


# N.B: registered for get_rvar for Stmt and MaterializedExpr below
# Also, called explicitly for Pointer when expr is not None
# TODO: This is a tangled mess that handles several cases.
# Most of the code is for computed pointers.
def process_set_as_subquery(
    ir_set: irast.Set, *, ctx: context.CompilerContextLevel
) -> SetRVars:
    is_objtype_path = ir_set.path_id.is_objtype_path()

    stmt = ctx.rel
    if isinstance(ir_set.expr, irast.Pointer):
        rptr = ir_set.expr
        expr = rptr.expr
    else:
        expr = ir_set.expr
        rptr = None

    ir_source: Optional[irast.Set]

    source_set_rvar = None
    if rptr is not None:
        ir_source = rptr.source

        if not is_objtype_path:
            source_is_visible = True
        else:
            # Non-scalar computable pointer.  Check if path source is
            # visible in the outer scope.
            outer_fence = ctx.scope_tree.parent_branch
            assert outer_fence is not None
            source_is_visible = outer_fence.is_visible(ir_source.path_id)

        if source_is_visible and (
            ir_source.path_id not in ctx.skippable_sources
        ):
            with ctx.new() as sctx:
                sctx.expr_exposed = False
                source_set_rvar = get_set_rvar(ir_source, ctx=sctx)
                # Force a source rvar so that trivial computed pointers
                # on erroneous objects (like a bad array deref) fail.
                # (Most sensible computables will end up requiring the
                # source rvar anyway.)
                ensure_source_rvar(ir_source, stmt, ctx=sctx)
    else:
        ir_source = None
        source_is_visible = False

    with ctx.new() as newctx:
        # Suppress volatility refs while compiling schema
        # aliases/globals.  While they might try to apply volatility
        # refs due to FOR/free objects, it shouldn't be semantically
        # necessary that they actually are attached to the enclosing
        # location. This turns out to be an important optimization for
        # ext::auth::ClientTokenIdentity.
        if ir_set.is_schema_alias:
            newctx.volatility_ref = ()

        outer_id = ir_set.path_id
        semi_join = False

        if ir_source is not None:
            if (
                ir_source.path_id != ctx.current_insert_path_id
                and not irutils.is_trivial_free_object(ir_source)
            ):
                # This is a computable pointer.  In order to ensure that
                # the volatile functions in the pointer expression are called
                # the necessary number of times, we must inject a
                # "volatility reference" into function expressions.
                # The volatility_ref is the identity of the pointer source.

                # If the source is an insert that we are in the middle
                # of doing, we don't have a volatility ref to add, so
                # skip it based on the current_insert_path_id check.

                # Note also that we skip this when the source is a
                # trivial free object reference. A trivial free object
                # reference is always executed exactly once (if there
                # is an outer iterator of some kind, we'll pick up
                # *that* volatility ref) and, unlike other shapes, may
                # contain DML. We disable the volatility ref for
                # trival free objects then both as a minor
                # optimization and to avoid it interfering with DML in
                # the object (since the volatility ref would not be
                # visible in DML CTEs).
                path_id = ir_source.path_id
                newctx.volatility_ref += (
                    lambda _stmt, xctx: relctx.maybe_get_path_var(
                        stmt,
                        path_id=path_id,
                        aspect=pgce.PathAspect.IDENTITY,
                        ctx=xctx,
                    ),
                )

            if is_objtype_path and not source_is_visible:
                # Non-scalar computable semi-join.

                # TODO: The basic path case has a more sophisticated
                # understanding of when to do semi-joins. Using that
                # naively here doesn't work, but perhaps it could be
                # adapted?
                # Don't semi-join on free objects, since they are all unique
                # (but don't *actually* have unique ids...)
                semi_join = not irtyputils.is_free_object(ir_set.typeref)

                # We need to compile the source and include it in,
                # since we need to do the semi-join deduplication here
                # on the outside, and not when the source is used in a
                # path inside the computable.
                # (See test_edgeql_scope_computables_09 for an example.)
                with newctx.subrel() as _, _.newscope() as subctx:
                    get_set_rvar(ir_source, ctx=subctx)
                    subrvar = relctx.rvar_for_rel(subctx.rel, ctx=subctx)
                    # Force a source rvar. See above.
                    ensure_source_rvar(ir_source, subctx.rel, ctx=subctx)

                relctx.include_rvar(
                    stmt, subrvar, ir_source.path_id, ctx=newctx)

        # If we are looking at a materialized computable, running
        # get_set_rvar on the source above may have made it show
        # up. So try to lookup the rvar again, and try to look it up
        # in the source_rvar itself, and if we find it, skip compiling
        # the computable.
        if ir_source and (new_rvar := (
            _lookup_set_rvar(ir_set, ctx=newctx)
            or _lookup_set_rvar_in_source(ir_set, source_set_rvar, ctx=newctx)
        )):
            if semi_join:
                # We need to use DISTINCT, instead of doing an actual
                # semi-join, unfortunately: we need to extract data
                # out from stmt, which we can't do with a semi-join.
                value_var = pathctx.get_rvar_path_var(
                    new_rvar,
                    outer_id,
                    aspect=pgce.PathAspect.VALUE,
                    env=ctx.env,
                )
                stmt.distinct_clause = (
                    pathctx.get_rvar_output_var_as_col_list(
                        subrvar,
                        value_var,
                        aspect=pgce.PathAspect.VALUE,
                        env=ctx.env,
                    )
                )

            return _new_subquery_stmt_set_rvar(ir_set, stmt, ctx=newctx)

        # materialized refs should always get picked up by now
        assert not isinstance(expr, irast.MaterializedExpr), (
            f"Can't find materialized set {ir_set.path_id}"
        )
        assert isinstance(expr, irast.Stmt)

        inner_set = expr.result
        inner_id = inner_set.path_id

        if inner_id != outer_id:
            pathctx.put_path_id_map(stmt, outer_id, inner_id)

        if isinstance(expr, irast.MutatingStmt) and expr in ctx.dml_stmts:
            # The DML table-routing logic may result in the same
            # DML subquery to be visited twice, such as in the case
            # of a nested INSERT declaring link properties, so guard
            # against generating a duplicate DML CTE.
            with newctx.substmt() as subrelctx:
                dml_cte = ctx.dml_stmts[expr]
                dml.wrap_dml_cte(expr, dml_cte, ctx=subrelctx)
        else:
            dispatch.visit(expr, ctx=newctx)

        if semi_join:
            set_rvar = relctx.new_root_rvar(ir_set, ctx=newctx)
            tgt_ref = pathctx.get_rvar_path_identity_var(
                set_rvar, ir_set.path_id, env=ctx.env)

            pathctx.get_path_identity_output(
                stmt, ir_set.path_id, env=ctx.env)
            cond_expr = astutils.new_binop(tgt_ref, stmt, 'IN')

            # Make a new stmt, join in the new root, and semi join on
            # the original statement.
            stmt = pgast.SelectStmt()
            relctx.include_rvar(stmt, set_rvar, ir_set.path_id, ctx=newctx)
            stmt.where_clause = astutils.extend_binop(
                stmt.where_clause, cond_expr)

    rvars = _new_subquery_stmt_set_rvar(ir_set, stmt, ctx=ctx)
    # If the inner set also exposes a pointer path source, we need to
    # also expose a pointer path source. See tests like
    # test_edgeql_select_linkprop_rebind_01
    if pathctx.maybe_get_path_rvar(
        stmt,
        inner_id.ptr_path(),
        aspect=pgce.PathAspect.SOURCE,
    ):
        rvars.new.append(
            SetRVar(
                rvars.main.rvar,
                outer_id.ptr_path(),
                aspects=(pgce.PathAspect.SOURCE,),
            )
        )

    return rvars


register_get_rvar(irast.Stmt)(process_set_as_subquery)
register_get_rvar(irast.MaterializedExpr)(process_set_as_subquery)


@_special_case('std::IN')
@_special_case('std::NOT IN')
def process_set_as_membership_expr(
    ir_set: irast.SetE[irast.Call], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    expr = ir_set.expr
    assert isinstance(expr, irast.OperatorCall)

    with ctx.new() as newctx:
        left_arg, right_arg = (a.expr for a in expr.args.values())

        newctx.expr_exposed = False
        left_out = dispatch.compile(left_arg, ctx=newctx)

        orig_right_arg = right_arg
        unwrapped_right_arg = irutils.unwrap_set(right_arg)
        # If the right operand of [NOT] IN is an array_unpack call,
        # then use the ANY/ALL array comparison operator directly,
        # since that has a higher chance of using the indexes.
        right_expr = unwrapped_right_arg.expr
        needs_coalesce = False

        if (
            isinstance(right_expr, irast.FunctionCall)
            and str(right_expr.func_shortname) == 'std::array_unpack'
            and not right_expr.args[0].cardinality.is_multi()
            and (not expr.sql_operator or len(expr.sql_operator) <= 1)
        ):
            is_array_unpack = True
            right_arg = right_expr.args[0].expr
            needs_coalesce = right_expr.args[0].cardinality.can_be_zero()
        else:
            is_array_unpack = False

        left_is_row_expr = astutils.is_row_expr(left_out)

        with newctx.subrel() as _, _.newscope() as subctx:
            if is_array_unpack:
                relctx.update_scope(orig_right_arg, subctx.rel, ctx=subctx)
                relctx.update_scope(
                    unwrapped_right_arg, subctx.rel, ctx=subctx)

            dispatch.compile(right_arg, ctx=subctx)
            right_rel = subctx.rel
            right_out = pathctx.get_path_value_var(
                right_rel, right_arg.path_id, env=subctx.env)
            right_out = output.output_as_value(right_out, env=ctx.env)

            if (
                left_is_row_expr
                and right_arg.path_id.is_tuple_path()
            ):
                # When the RHS is an opaque tuple, we must unpack
                # it using the (...).* indirection syntax, otherwise
                # we get "subquery has too few columns".
                right_out = pgast.Indirection(
                    arg=right_out,
                    indirection=[pgast.Star()],
                )

            right_rel.target_list = [pgast.ResTarget(val=right_out)]

            if is_array_unpack:
                right_rel = pgast.TypeCast(
                    arg=right_rel,
                    type_name=pgast.TypeName(
                        name=pg_types.pg_type_from_ir_typeref(
                            right_arg.typeref)
                    )
                )

            negated = str(expr.func_shortname) == 'std::NOT IN'

            set_expr = exprcomp.compile_operator(
                expr,
                [
                    left_out,
                    pgast.SubLink(
                        operator="ALL" if negated else "ANY",
                        expr=right_rel,
                    ),
                ],
                ctx=ctx,
            )

            # A NULL argument to the array variant will produce NULL, so we
            # need to coalesce if that is possible.
            if needs_coalesce:
                empty_val = negated
                set_expr = pgast.CoalesceExpr(args=[
                    set_expr, pgast.BooleanConstant(val=empty_val)])

            pathctx.put_path_value_var_if_not_exists(
                ctx.rel, ir_set.path_id, set_expr
            )

    return new_stmt_set_rvar(ir_set, ctx.rel, ctx=ctx)


@_special_case('std::UNION')
@_special_case('std::EXCEPT')
@_special_case('std::INTERSECT')
def process_set_as_setop(
    ir_set: irast.SetE[irast.Call], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    expr = ir_set.expr

    with ctx.new() as newctx:
        newctx.expr_exposed = False

        left, right = (a.expr for a in expr.args.values())

        with newctx.subrel() as _, _.newscope() as scopectx:
            larg = scopectx.rel
            pathctx.put_path_id_map(larg, ir_set.path_id, left.path_id)
            dispatch.visit(left, ctx=scopectx)

        with newctx.subrel() as _, _.newscope() as scopectx:
            rarg = scopectx.rel
            pathctx.put_path_id_map(rarg, ir_set.path_id, right.path_id)
            dispatch.visit(right, ctx=scopectx)

    aspects = pathctx.list_path_aspects(
        larg, left.path_id
    ) & pathctx.list_path_aspects(rarg, right.path_id)

    with ctx.subrel() as subctx:
        subqry = subctx.rel
        # There are three possible binary set operators coming from IR:
        # UNION, EXCEPT, and INTERSECT
        subqry.op = expr.func_shortname.name
        subqry.all = True
        subqry.larg = larg
        subqry.rarg = rarg

        setop_rvar = relctx.rvar_for_rel(subqry, lateral=True, ctx=subctx)
        # No pull_namespace because we don't want the union arguments to
        # escape, just the final result.
        relctx.include_rvar(
            ctx.rel,
            setop_rvar,
            ir_set.path_id,
            aspects=aspects,
            pull_namespace=False,
            ctx=subctx,
        )

    return new_stmt_set_rvar(ir_set, ctx.rel, ctx=ctx)


@_special_case('std::DISTINCT')
def process_set_as_distinct(
    ir_set: irast.SetE[irast.Call], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    expr = ir_set.expr
    stmt = ctx.rel

    with ctx.subrel() as subctx:
        subqry = subctx.rel
        arg = expr.args[0].expr
        pathctx.put_path_id_map(subqry, ir_set.path_id, arg.path_id)
        dispatch.visit(arg, ctx=subctx)
        subrvar = relctx.rvar_for_rel(
            subqry, typeref=arg.typeref, lateral=True, ctx=subctx)

    relctx.include_rvar(stmt, subrvar, ir_set.path_id, ctx=ctx)

    value_var = pathctx.get_rvar_path_var(
        subrvar,
        ir_set.path_id,
        aspect=pgce.PathAspect.VALUE,
        env=ctx.env,
    )

    stmt.distinct_clause = pathctx.get_rvar_output_var_as_col_list(
        subrvar,
        value_var,
        aspect=pgce.PathAspect.VALUE,
        env=ctx.env,
    )
    # If there aren't any columns, we are doing DISTINCT on empty
    # tuples. All empty tuples are equivalent, so we can just compile
    # this by adding a LIMIT 1.
    if not stmt.distinct_clause:
        stmt.limit_count = pgast.NumericConstant(val="1")

    return new_stmt_set_rvar(ir_set, stmt, ctx=ctx)


@_special_case('std::IF')
def process_set_as_ifelse(
    ir_set: irast.SetE[irast.Call], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    # A IF Cond ELSE B is transformed into:
    # SELECT A WHERE Cond UNION ALL SELECT B WHERE NOT Cond
    expr = ir_set.expr
    stmt = ctx.rel

    if_expr, condition, else_expr = (a.expr for a in expr.args.values())
    if_expr_card, _, else_expr_card = (
        a.cardinality for a in expr.args.values()
    )

    with ctx.new() as newctx:
        newctx.expr_exposed = False
        dispatch.visit(condition, ctx=newctx)
        condref = relctx.get_path_var(
            stmt,
            path_id=condition.path_id,
            aspect=pgce.PathAspect.VALUE,
            ctx=newctx,
        )

    if (if_expr_card.is_single() and else_expr_card.is_single()
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

        with ctx.subrel() as subctx:
            pathctx.put_path_value_var_if_not_exists(
                subctx.rel,
                ir_set.path_id,
                set_expr,
            )
            sub_rvar = relctx.rvar_for_rel(
                subctx.rel,
                lateral=True,
                ctx=subctx,
            )
            relctx.include_rvar(stmt, sub_rvar, ir_set.path_id, ctx=subctx)

        rvar = pathctx.get_path_value_var(
            stmt, path_id=ir_set.path_id, env=ctx.env)
        # We need to NULL filter both the result and the input condition
        for var in [rvar, condref]:
            stmt.where_clause = astutils.extend_binop(
                stmt.where_clause,
                pgast.NullTest(
                    arg=var, negated=True
                )
            )

    else:
        with ctx.subrel() as _, _.newscope() as subctx:
            subctx.expr_exposed = False
            larg = subctx.rel
            pathctx.put_path_id_map(larg, ir_set.path_id, if_expr.path_id)
            dispatch.visit(if_expr, ctx=subctx)

            larg.where_clause = astutils.extend_binop(
                larg.where_clause,
                condref
            )

        with ctx.subrel() as _, _.newscope() as subctx:
            subctx.expr_exposed = False
            rarg = subctx.rel
            pathctx.put_path_id_map(rarg, ir_set.path_id, else_expr.path_id)
            dispatch.visit(else_expr, ctx=subctx)

            rarg.where_clause = astutils.extend_binop(
                rarg.where_clause,
                astutils.new_unop('NOT', condref)
            )

        aspects = pathctx.list_path_aspects(
            larg, if_expr.path_id
        ) & pathctx.list_path_aspects(rarg, else_expr.path_id)

        with ctx.subrel() as subctx:
            subqry = subctx.rel
            subqry.op = 'UNION'
            subqry.all = True
            subqry.larg = larg
            subqry.rarg = rarg

            union_rvar = relctx.rvar_for_rel(subqry, lateral=True, ctx=subctx)
            relctx.include_rvar(
                stmt,
                union_rvar,
                ir_set.path_id,
                pull_namespace=False,
                aspects=aspects,
                ctx=subctx,
            )

    return new_stmt_set_rvar(ir_set, stmt, ctx=ctx)


@_special_case('std::??')
def process_set_as_coalesce(
    ir_set: irast.SetE[irast.Call], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    expr = ir_set.expr

    with ctx.new() as newctx:
        newctx.expr_exposed = False
        left_ir, right_ir = (a.expr for a in expr.args.values())
        _left_card, right_card = (a.cardinality for a in expr.args.values())
        is_object = (
            ir_set.path_id.is_objtype_path()
            or ir_set.path_id.is_tuple_path()
        )

        # The cardinality optimization below applies only to
        # non-object/non-tuple expressions, because we don't want to
        # have to deal with the complexity of resolving coalesced
        # sources for potential link or property references.
        if right_card.is_single() and not is_object:
            left = dispatch.compile(left_ir, ctx=newctx)

            # If the RHS is optional, we compile it in a subquery so that
            # it becomes NULL instead of potentially joining in zero rows.
            # If not, just compile it without any protection.
            right = (
                set_as_subquery(right_ir, ctx=newctx)
                if right_card.can_be_zero()
                else dispatch.compile(right_ir, ctx=newctx)
            )

            # Just use scalar COALESCE now
            set_expr = pgast.CoalesceExpr(args=[left, right])
            pathctx.put_path_value_var(ctx.rel, ir_set.path_id, set_expr)

        else:
            # Things become tricky in cases where the RHS is a non-singleton
            # or where we need to worry about source aspects.
            # We cannot use the regular scalar COALESCE over a JOIN,
            # as that'll blow up the result cardinality. Instead, we do
            # something like:
            #
            #     CROSS JOIN LATERAL
            #       (<left>) as lhs
            #     CROSS JOIN LATERAL (
            #       SELECT * FROM lhs WHERE lhs.value IS NOT NULL
            #       UNION ALL
            #       SELECT * FROM <right> as rhs WHERE lhs.value IS NULL
            #     ) as q
            #
            # Note that <left> will be compiled with optional wrapping,
            # so it shouldn't ever produce zero rows.
            lhs_rvar = get_set_rvar(left_ir, ctx=newctx)
            lvar = pathctx.get_rvar_path_var(
                lhs_rvar,
                left_ir.path_id,
                aspect=pgce.PathAspect.VALUE,
                env=ctx.env,
            )
            lval = output.output_as_value(lvar, env=ctx.env)

            with newctx.subrel() as lctx:
                larg = lctx.rel
                pathctx.put_path_id_map(larg, ir_set.path_id, left_ir.path_id)

                relctx.include_rvar(
                    larg,
                    lhs_rvar,
                    path_id=left_ir.path_id,
                    ctx=lctx,
                    # Only include the aspects that got included
                    # into our rel; it's possible some were explicitly
                    # left out, and we should respect that.
                    aspects=pathctx.list_path_aspects(
                        newctx.rel, left_ir.path_id
                    ),
                )

                # Include the LHS when it is not NULL.
                larg.where_clause = astutils.extend_binop(
                    larg.where_clause,
                    pgast.NullTest(
                        arg=lval, negated=True
                    )
                )

            with newctx.subrel() as rctx:
                rarg = rctx.rel
                pathctx.put_path_id_map(rarg, ir_set.path_id, right_ir.path_id)
                rvar = dispatch.compile(right_ir, ctx=rctx)

                # Include the RHS when the LHS is NULL.
                # Note that an important precondition of this is that
                # there are not "stray" NULLs in the LHS input set.
                #
                # HACK: We need to use IS NOT DISTINCT FROM
                # because ROW() IS NULL is true, and that
                # breaks some things.
                rarg.where_clause = astutils.extend_binop(
                    rarg.where_clause,
                    astutils.new_binop(
                        lval, pgast.NullConstant(),
                        'IS NOT DISTINCT FROM',
                    ),
                )

                if rvar.nullable:
                    rarg.where_clause = astutils.extend_binop(
                        rarg.where_clause,
                        pgast.NullTest(arg=rvar, negated=True)
                    )

            union_rvar = relctx.rvar_for_rel(
                pgast.SelectStmt(op='UNION', all=True, larg=larg, rarg=rarg),
                lateral=True,
                ctx=newctx,
            )

            aspects = (
                pathctx.list_path_aspects(larg, left_ir.path_id)
                & pathctx.list_path_aspects(rarg, right_ir.path_id)
            )

            # No pull_namespace because we don't want the coalesce arguments to
            # escape, just the final result.
            relctx.include_rvar(
                ctx.rel,
                union_rvar,
                path_id=ir_set.path_id,
                aspects=aspects,
                pull_namespace=False,
                ctx=newctx,
            )

    return new_stmt_set_rvar(ir_set, ctx.rel, ctx=ctx)


@register_get_rvar(irast.Tuple)
def process_set_as_tuple(
    ir_set: irast.SetE[irast.Tuple], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    expr = ir_set.expr
    stmt = ctx.rel

    with ctx.new() as subctx:
        subctx.expr_exposed_tuple_cheat = None
        elements = []

        ttypes = {}
        for i, st in enumerate(ir_set.typeref.subtypes):
            if st.element_name:
                ttypes[st.element_name] = st
            else:
                ttypes[str(i)] = st

        for element in expr.elements:
            assert element.path_id
            path_id = element.path_id

            # We compile in a subrel *solely* so that we can map
            # each element individually. It would be nice to have
            # a way to do this that doesn't actually affect the output!
            with subctx.subrel() as newctx:
                if element is ctx.expr_exposed_tuple_cheat:
                    newctx.expr_exposed = True

                if path_id != element.val.path_id:
                    pathctx.put_path_id_map(
                        newctx.rel, path_id, element.val.path_id)
                dispatch.visit(element.val, ctx=newctx)

            el_rvar = relctx.new_rel_rvar(ir_set, newctx.rel, ctx=ctx)
            aspects = pathctx.list_path_aspects(
                newctx.rel, element.val.path_id
            )
            # update_mask=False because we are doing this solely to remap
            # elements individually and don't want to affect the mask.
            relctx.include_rvar(
                stmt,
                el_rvar,
                path_id,
                update_mask=False,
                aspects=aspects,
                ctx=ctx,
            )
            tvar = pathctx.get_path_value_var(stmt, path_id, env=subctx.env)

            elements.append(pgast.TupleElementBase(path_id=path_id))

            # We need to filter out NULLs at tuple creation time, to
            # prevent having tuples that are part-NULL.
            if tvar.nullable:
                stmt.where_clause = astutils.extend_binop(
                    stmt.where_clause,
                    pgast.NullTest(arg=tvar, negated=True)
                )

            var = pathctx.maybe_get_path_var(
                stmt,
                element.val.path_id,
                aspect=pgce.PathAspect.SERIALIZED,
                env=subctx.env,
            )
            if var is not None:
                pathctx.put_path_var(
                    stmt,
                    path_id,
                    var,
                    aspect=pgce.PathAspect.SERIALIZED,
                )

        set_expr = pgast.TupleVarBase(
            elements=elements,
            named=expr.named,
            typeref=ir_set.typeref,
        )

    pathctx.put_path_value_var(stmt, ir_set.path_id, set_expr)

    # This is an unfortunate hack. If any of those types that we
    # contain are an object, then force the computation of the
    # serialized output now. This avoids issues where there may be
    # references to tuple elements with the same path id but different
    # shapes, and the delaying induced by a TupleBaseVar can cause the
    # wrong one to be output. (See test_edgeql_scope_shape_03 for an example
    # where this can come up.)
    # (We only do it for objects as an optimization.)
    if (
        output.in_serialization_ctx(ctx)
        and any(irtyputils.is_object(x) for x in ir_set.typeref.subtypes)
    ):
        pathctx.get_path_serialized_output(stmt, ir_set.path_id, env=ctx.env)

    return new_stmt_set_rvar(
        ir_set,
        stmt,
        aspects=[pgce.PathAspect.VALUE, pgce.PathAspect.SOURCE],
        ctx=ctx,
    )


@register_get_rvar(irast.TupleIndirectionPointer)
def process_set_as_tuple_indirection(
    ir_set: irast.SetE[irast.TupleIndirectionPointer],
    *, ctx: context.CompilerContextLevel
) -> SetRVars:
    rptr = ir_set.expr
    tuple_set = rptr.source
    stmt = ctx.rel

    assert not rptr.expr, 'tuple indirection pointer with expr??'

    with ctx.new() as subctx:
        # Usually the LHS is is not exposed, but when we are directly
        # projecting from an explicit tuple, and the result is a
        # collection, arrange to have the element we are projecting
        # treated as exposed. This behavior is needed for our
        # eta-expansion of arrays to work, since it generates that
        # idiom in a place where it needs the output to be exposed.
        subctx.expr_exposed = False
        if (
            ctx.expr_exposed
            and not tuple_set.is_binding
            and isinstance(tuple_set.expr, irast.Tuple)
            and ir_set.path_id.is_collection_path()
        ):
            for el in tuple_set.expr.elements:
                if el.name == rptr.ptrref.shortname.name:
                    subctx.expr_exposed_tuple_cheat = el
                    break
        rvar = get_set_rvar(tuple_set, ctx=subctx)

        source_rvar = relctx.maybe_get_path_rvar(
            stmt,
            tuple_set.path_id,
            aspect=pgce.PathAspect.SOURCE,
            ctx=subctx,
        )

        if source_rvar is None:
            # Lack of visible tuple source means we are
            # an indirection over an opaque tuple, e.g. in
            # `SELECT [(1,)][0].0`.  This means we must
            # use an explicit row attribute dereference.
            tuple_val = pathctx.get_path_value_var(
                stmt, path_id=tuple_set.path_id, env=subctx.env)

            set_expr = astutils.tuple_getattr(
                tuple_val,
                tuple_set.typeref,
                rptr.ptrref.shortname.name,
            )

            pathctx.put_path_var_if_not_exists(
                stmt,
                ir_set.path_id,
                set_expr,
                aspect=pgce.PathAspect.VALUE,
            )

            rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=subctx)

    return new_simple_set_rvar(
        ir_set,
        rvar,
        aspects=(pgce.PathAspect.VALUE,),
    )


@register_get_rvar(irast.TypeCast)
def process_set_as_type_cast(
    ir_set: irast.SetE[irast.TypeCast], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    expr = ir_set.expr
    stmt = ctx.rel

    inner_set = expr.expr
    is_json_cast = expr.to_type.id == s_obj.get_known_type_id('std::json')

    # Are we casting by compiling the innards in json mode?
    implicit_cast = (
        is_json_cast
        and not irtyputils.is_range(inner_set.typeref)
        and (irtyputils.is_collection(inner_set.typeref)
             or irtyputils.is_object(inner_set.typeref))
    )
    fmt_ctx = (
        context.output_format(ctx, context.OutputFormat.JSONB) if implicit_cast
        else contextlib.nullcontext()
    )

    with fmt_ctx, ctx.new() as subctx:
        pathctx.put_path_id_map(ctx.rel, ir_set.path_id, inner_set.path_id)

        if implicit_cast:
            subctx.expr_exposed = True

            set_expr = dispatch.compile(inner_set, ctx=subctx)

            serialized: Optional[pgast.BaseExpr] = (
                pathctx.maybe_get_path_serialized_var(
                    stmt, inner_set.path_id, env=subctx.env)
            )

            if serialized is not None:
                if irtyputils.is_collection(inner_set.typeref):
                    serialized = output.serialize_expr_to_json(
                        serialized, styperef=inner_set.path_id.target,
                        env=subctx.env)

                pathctx.put_path_value_var(
                    stmt, inner_set.path_id, serialized, force=True
                )

                pathctx.put_path_serialized_var(
                    stmt, inner_set.path_id, serialized, force=True
                )
        else:
            # Rely on the simple implementation of TypeCast
            set_expr = dispatch.compile(expr, ctx=subctx)

            # A proper path var mapping way would be to wrap
            # the inner expression in a subquery, but that
            # seems excessive for a type cast, so we cover
            # our tracks here by removing the mapping and
            # relying on the value and serialized vars
            # populated above.
            stmt.view_path_id_map.pop(ir_set.path_id)

    pathctx.put_path_value_var_if_not_exists(stmt, ir_set.path_id, set_expr)

    return new_stmt_set_rvar(ir_set, stmt, ctx=ctx)


@register_get_rvar(irast.ConstantSet)
def process_set_as_const_set(
    ir_set: irast.SetE[irast.ConstantSet], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    with ctx.subrel() as subctx:
        vals = [dispatch.compile(v, ctx=subctx) for v in ir_set.expr.elements]
        vals_rel = subctx.rel
        vals_rel.values = [pgast.ImplicitRowExpr(args=[v]) for v in vals]
        vals_rel.nullable = any(v.nullable for v in vals)

    vals_rvar = relctx.new_rel_rvar(ir_set, vals_rel, ctx=ctx)
    relctx.include_rvar(ctx.rel, vals_rvar, ir_set.path_id, ctx=ctx)

    return new_stmt_set_rvar(ir_set, ctx.rel, ctx=ctx)


def process_set_as_oper_expr(
    ir_set: irast.SetE[irast.OperatorCall], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    # XXX: do we need a subrel?
    with ctx.new() as newctx:
        newctx.expr_exposed = False
        args = _compile_call_args(ir_set, ctx=newctx)
        oper_expr = exprcomp.compile_operator(ir_set.expr, args, ctx=newctx)

    pathctx.put_path_value_var_if_not_exists(
        ctx.rel, ir_set.path_id, oper_expr
    )

    return new_stmt_set_rvar(ir_set, ctx.rel, ctx=ctx)


@register_get_rvar(irast.TriggerAnchor)
def process_set_as_trigger_anchor(
    ir_set: irast.Set, *, ctx: context.CompilerContextLevel
) -> SetRVars:
    # XXX: This will need to grow more things
    if ir_set.path_id in ctx.external_rels:
        return process_external_rel(ir_set, ctx=ctx)

    return process_set_as_root(ir_set, ctx=ctx)


@register_get_rvar(irast.Expr)
def process_set_as_expr(
    ir_set: irast.SetE[irast.Expr], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    with ctx.new() as newctx:
        newctx.expr_exposed = False
        set_expr = dispatch.compile(ir_set.expr, ctx=newctx)

    pathctx.put_path_value_var_if_not_exists(ctx.rel, ir_set.path_id, set_expr)

    return new_stmt_set_rvar(ir_set, ctx.rel, ctx=ctx)


@_special_case('std::assert_single')
def process_set_as_singleton_assertion(
    ir_set: irast.SetE[irast.Call],
    *,
    ctx: context.CompilerContextLevel,
) -> SetRVars:
    expr = ir_set.expr
    stmt = ctx.rel

    msg_arg = expr.args['message']
    ir_arg = expr.args[0]
    ir_arg_set = ir_arg.expr

    if (
        ir_arg.cardinality.is_single()
        and not msg_arg.cardinality.is_multi()
    ):
        # If the argument has been statically proven to be a singleton,
        # elide the entire assertion.
        arg_ref = dispatch.compile(ir_arg_set, ctx=ctx)
        pathctx.put_path_value_var(stmt, ir_set.path_id, arg_ref)
        pathctx.put_path_id_map(stmt, ir_set.path_id, ir_arg_set.path_id)
        return new_stmt_set_rvar(ir_set, stmt, ctx=ctx)

    with ctx.subrel() as newctx:
        arg_ref = dispatch.compile(ir_arg_set, ctx=newctx)
        arg_val = output.output_as_value(arg_ref, env=newctx.env)

        msg = dispatch.compile(msg_arg.expr, ctx=newctx)

        # Generate a singleton set assertion as the following SQL:
        #
        #    SELECT
        #        <target_set>,
        #        raise_on_null(NULLIF(row_number() OVER (), 2)) AS _sentinel
        #    ORDER BY
        #        _sentinel
        #
        # This effectively raises an error whenever the row counter reaches 2.
        check_expr = pgast.FuncCall(
            name=('nullif',),
            args=[
                pgast.FuncCall(
                    name=('row_number',),
                    args=[],
                    over=pgast.WindowDef()
                ),
                pgast.NumericConstant(
                    val='2',
                ),
            ],
        )

        maybe_raise = pgast.FuncCall(
            name=astutils.edgedb_func('raise_on_null', ctx=ctx),
            args=[
                check_expr,
                pgast.StringConstant(val='cardinality_violation'),
                pgast.NamedFuncArg(
                    name='msg',
                    val=pgast.CoalesceExpr(
                        args=[
                            msg,
                            pgast.StringConstant(
                                val='assert_single violation: more than one '
                                    'element returned by an expression',
                            ),
                        ],
                    ),
                ),
                pgast.NamedFuncArg(
                    name='constraint',
                    val=pgast.StringConstant(val='std::assert_single'),
                ),
            ],
        )

        output.add_null_test(arg_ref, newctx.rel)

        # Force Postgres to actually evaluate the result target
        # by putting it into an ORDER BY.
        newctx.rel.target_list.append(
            pgast.ResTarget(
                name="_sentinel",
                val=maybe_raise,
            ),
        )

        if newctx.rel.sort_clause is None:
            newctx.rel.sort_clause = []
        newctx.rel.sort_clause.append(
            pgast.SortBy(node=pgast.ColumnRef(name=["_sentinel"])),
        )

        pathctx.put_path_var_if_not_exists(
            newctx.rel, ir_set.path_id, arg_val, aspect=pgce.PathAspect.VALUE
        )

        pathctx.put_path_id_map(newctx.rel, ir_set.path_id, ir_arg_set.path_id)

    aspects = pathctx.list_path_aspects(newctx.rel, ir_arg_set.path_id)
    func_rvar = relctx.new_rel_rvar(ir_set, newctx.rel, ctx=ctx)
    relctx.include_rvar(stmt, func_rvar, ir_set.path_id,
                        aspects=aspects, ctx=ctx)

    return new_stmt_set_rvar(ir_set, stmt, aspects=aspects, ctx=ctx)


@_special_case('std::assert_exists')
def process_set_as_existence_assertion(
    ir_set: irast.SetE[irast.Call],
    *,
    ctx: context.CompilerContextLevel,
) -> SetRVars:
    """Implementation of std::assert_exists"""
    expr = ir_set.expr
    stmt = ctx.rel

    msg_arg = expr.args['message']
    ir_arg = expr.args[0]
    ir_arg_set = ir_arg.expr

    if (
        not ir_arg.cardinality.can_be_zero()
        and not msg_arg.cardinality.is_multi()
    ):
        # If the argument has been statically proven to be non empty,
        # elide the entire assertion.
        arg_ref = dispatch.compile(ir_arg_set, ctx=ctx)
        pathctx.put_path_value_var(stmt, ir_set.path_id, arg_ref)
        pathctx.put_path_id_map(stmt, ir_set.path_id, ir_arg_set.path_id)
        return new_stmt_set_rvar(ir_set, stmt, ctx=ctx)

    with ctx.subrel() as newctx:
        # The solution to assert_exists() is as simple as
        # calling raise_on_null().
        newctx.expr_exposed = False
        newctx.force_optional |= {ir_arg_set.path_id}
        pathctx.put_path_id_map(newctx.rel, ir_set.path_id, ir_arg_set.path_id)
        arg_ref = dispatch.compile(ir_arg_set, ctx=newctx)
        arg_val = output.output_as_value(arg_ref, env=newctx.env)

        msg = dispatch.compile(msg_arg.expr, ctx=newctx)

        set_expr = pgast.FuncCall(
            name=astutils.edgedb_func('raise_on_null', ctx=ctx),
            args=[
                arg_val,
                pgast.StringConstant(val='cardinality_violation'),
                pgast.NamedFuncArg(
                    name='msg',
                    val=pgast.CoalesceExpr(
                        args=[
                            msg,
                            pgast.StringConstant(
                                val='assert_exists violation: expression '
                                    'returned an empty set',
                            ),
                        ]
                    ),
                ),
                pgast.NamedFuncArg(
                    name='constraint',
                    val=pgast.StringConstant(val='std::assert_exists'),
                ),
            ],
        )

        pathctx.put_path_value_var(
            newctx.rel,
            ir_arg_set.path_id,
            set_expr,
            force=True,
        )
        other_aspect = (
            pgce.PathAspect.IDENTITY
            if ir_set.path_id.is_objtype_path() else
            pgce.PathAspect.SERIALIZED
        )
        pathctx.put_path_var(
            newctx.rel,
            ir_arg_set.path_id,
            set_expr,
            force=True,
            aspect=other_aspect,
        )

    # It is important that we do not provide source, which could allow
    # fields on the object to be accessed without triggering the
    # raise_on_null. Not providing source means another join is
    # needed, which will trigger it.
    func_rvar = relctx.new_rel_rvar(ir_set, newctx.rel, ctx=ctx)
    relctx.include_rvar(
        stmt,
        func_rvar,
        ir_set.path_id,
        aspects=(pgce.PathAspect.VALUE,),
        ctx=ctx,
    )

    return new_stmt_set_rvar(
        ir_set,
        stmt,
        aspects=(pgce.PathAspect.VALUE,),
        ctx=ctx,
    )


@_special_case('std::assert_distinct')
def process_set_as_multiplicity_assertion(
    ir_set: irast.SetE[irast.Call],
    *,
    ctx: context.CompilerContextLevel,
) -> SetRVars:
    """Implementation of std::assert_distinct"""
    expr = ir_set.expr
    msg_arg = expr.args['message']
    ir_arg = expr.args[0]
    ir_arg_set = ir_arg.expr

    if (
        not ir_arg.multiplicity.is_duplicate()
        and not msg_arg.cardinality.is_multi()
    ):
        # If the argument has been statically proven to be distinct,
        # elide the entire assertion.
        arg_ref = dispatch.compile(ir_arg_set, ctx=ctx)
        pathctx.put_path_value_var(ctx.rel, ir_set.path_id, arg_ref)
        pathctx.put_path_id_map(ctx.rel, ir_set.path_id, ir_arg_set.path_id)
        return new_stmt_set_rvar(ir_set, ctx.rel, ctx=ctx)

    # Generate a distinct set assertion as the following SQL:
    #
    #    SELECT
    #        <target_set>,
    #        (CASE WHEN
    #            <target_set>
    #            IS DISTINCT FROM
    #            lag(<target_set>) OVER (ORDER BY <target_set>)
    #        THEN <target_set>
    #        ELSE edgedb.raise(ConstraintViolationError)) AS check_expr
    #    FROM
    #        (SELECT <target_set>, row_number() OVER () AS i) AS q
    #    ORDER BY
    #        q.i, check_expr
    #
    # NOTE: sorting over original row_number() is necessary to preserve
    #       order, as assert_distinct() must be completely transparent for
    #       compliant sets.
    with ctx.subrel() as newctx:
        with newctx.subrel() as subctx:
            dispatch.visit(ir_arg_set, ctx=subctx)
            arg_ref = pathctx.get_path_output(
                subctx.rel,
                ir_arg_set.path_id,
                aspect=pgce.PathAspect.VALUE,
                env=subctx.env,
            )
            arg_val = output.output_as_value(arg_ref, env=newctx.env)
            sub_rvar = relctx.new_rel_rvar(ir_arg_set, subctx.rel, ctx=subctx)

            aspects = pathctx.list_path_aspects(subctx.rel, ir_arg_set.path_id)
            relctx.include_rvar(
                newctx.rel, sub_rvar, ir_arg_set.path_id,
                aspects=aspects, ctx=subctx,
            )
            alias = ctx.env.aliases.get('i')
            subctx.rel.target_list.append(
                pgast.ResTarget(
                    name=alias,
                    val=pgast.FuncCall(
                        name=('row_number',),
                        args=[],
                        over=pgast.WindowDef(),
                    )
                )
            )

        msg = dispatch.compile(msg_arg.expr, ctx=newctx)

        do_raise = pgast.FuncCall(
            name=astutils.edgedb_func('raise', ctx=ctx),
            args=[
                pgast.TypeCast(
                    arg=pgast.NullConstant(),
                    type_name=pgast.TypeName(
                        name=pg_types.pg_type_from_ir_typeref(
                            ir_arg_set.typeref),
                    ),
                ),
                pgast.StringConstant(val='cardinality_violation'),
                pgast.NamedFuncArg(
                    name='msg',
                    val=pgast.CoalesceExpr(
                        args=[
                            msg,
                            pgast.StringConstant(
                                val='assert_distinct violation: expression '
                                    'returned a set with duplicate elements',
                            ),
                        ],
                    ),
                ),
                pgast.NamedFuncArg(
                    name='constraint',
                    val=pgast.StringConstant(val='std::assert_distinct'),
                ),
            ],
        )

        check_expr = pgast.CaseExpr(
            args=[
                pgast.CaseWhen(
                    expr=astutils.new_binop(
                        lexpr=arg_val,
                        op='IS DISTINCT FROM',
                        rexpr=pgast.FuncCall(
                            name=('lag',),
                            args=[arg_val],
                            over=pgast.WindowDef(
                                order_clause=[pgast.SortBy(node=arg_val)],
                            ),
                        ),
                    ),
                    result=arg_val,
                ),
            ],
            defresult=do_raise,
        )

        alias2 = ctx.env.aliases.get('v')
        newctx.rel.target_list.append(
            pgast.ResTarget(
                val=check_expr,
                name=alias2,
            )
        )

        pathctx.put_path_var(
            newctx.rel,
            ir_set.path_id,
            check_expr,
            aspect=pgce.PathAspect.VALUE,
        )

        if newctx.rel.sort_clause is None:
            newctx.rel.sort_clause = []
        newctx.rel.sort_clause.extend([
            pgast.SortBy(
                node=pgast.ColumnRef(name=[sub_rvar.alias.aliasname, alias]),
            ),
            pgast.SortBy(
                node=pgast.ColumnRef(name=[alias2]),
            ),
        ])

        pathctx.put_path_id_map(newctx.rel, ir_set.path_id, ir_arg_set.path_id)

    func_rvar = relctx.new_rel_rvar(ir_set, newctx.rel, ctx=ctx)
    relctx.include_rvar(
        ctx.rel, func_rvar, ir_set.path_id, aspects=aspects, ctx=ctx
    )

    return new_stmt_set_rvar(ir_set, ctx.rel, aspects=aspects, ctx=ctx)


@_special_case('std::materialized')
def process_set_as_materialized_call(
    ir_set: irast.SetE[irast.Call],
    *,
    ctx: context.CompilerContextLevel,
) -> SetRVars:
    # It's a pure pass-through. Just an identity function marked volatile.
    stmt = ctx.rel

    ir_arg_set = ir_set.expr.args[0].expr

    arg_ref = dispatch.compile(ir_arg_set, ctx=ctx)
    pathctx.put_path_value_var(stmt, ir_set.path_id, arg_ref)
    pathctx.put_path_id_map(stmt, ir_set.path_id, ir_arg_set.path_id)
    return new_stmt_set_rvar(ir_set, stmt, ctx=ctx)


def process_set_as_simple_enumerate(
    ir_set: irast.Set, *, ctx: context.CompilerContextLevel
) -> SetRVars:
    expr = ir_set.expr
    assert isinstance(expr, irast.FunctionCall)

    with ctx.subrel() as newctx:
        ir_call_arg = expr.args[0]
        ir_arg = ir_call_arg.expr
        arg_ref = dispatch.compile(ir_arg, ctx=newctx)
        arg_val = output.output_as_value(arg_ref, env=newctx.env)

        if arg_ref.nullable:
            newctx.rel.where_clause = astutils.extend_binop(
                newctx.rel.where_clause,
                pgast.NullTest(arg=arg_ref, negated=True)
            )

        rtype = expr.typeref
        named_tuple = any(st.element_name for st in rtype.subtypes)

        num_expr = pgast.Expr(
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
            named=named_tuple,
            typeref=ir_set.typeref,
        )

        for element in set_expr.elements:
            pathctx.put_path_value_var(
                newctx.rel, element.path_id, element.val
            )

        var = pathctx.maybe_get_path_var(
            newctx.rel,
            ir_arg.path_id,
            aspect=pgce.PathAspect.SERIALIZED,
            env=newctx.env,
        )
        if var is not None:
            pathctx.put_path_var(
                newctx.rel,
                set_expr.elements[1].path_id,
                var,
                aspect=pgce.PathAspect.SERIALIZED,
            )

        pathctx.put_path_var_if_not_exists(
            newctx.rel,
            ir_set.path_id,
            set_expr,
            aspect=pgce.PathAspect.VALUE,
        )

    aspects = pathctx.list_path_aspects(newctx.rel, ir_arg.path_id) | {
        pgce.PathAspect.SOURCE
    }

    pathctx.put_path_id_map(newctx.rel, expr.tuple_path_ids[1], ir_arg.path_id)

    func_rvar = relctx.new_rel_rvar(ir_set, newctx.rel, ctx=ctx)
    relctx.include_rvar(
        ctx.rel, func_rvar, ir_set.path_id, aspects=aspects, ctx=ctx
    )

    return new_stmt_set_rvar(ir_set, ctx.rel, aspects=aspects, ctx=ctx)


@_special_case('std::enumerate')
def process_set_as_enumerate(
    ir_set: irast.SetE[irast.Call], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    expr = ir_set.expr
    arg_set = expr.args[0].expr
    arg_expr = arg_set.expr
    arg_subj = irutils.unwrap_set(arg_set).expr
    if (
        isinstance(arg_subj, irast.FunctionCall)
        and not arg_subj.func_sql_expr
        and not (
            isinstance(arg_expr, irast.SelectStmt)
            and (
                arg_expr.where
                or arg_expr.orderby
                or arg_expr.limit
                or arg_expr.offset
            )
        ) and not any(
            f_arg.param_typemod == qltypes.TypeModifier.SetOfType
            for _, f_arg in arg_subj.args.items()
        )
    ):
        # Enumeration of a non-aggregate function
        rvars = process_set_as_func_enumerate(ir_set, ctx=ctx)
    else:
        rvars = process_set_as_simple_enumerate(ir_set, ctx=ctx)

    return rvars


@_special_case('std::max', only_as_fallback=True)
@_special_case('std::min', only_as_fallback=True)
def process_set_as_std_min_max(
    ir_set: irast.SetE[irast.Call],
    *,
    ctx: context.CompilerContextLevel,
) -> SetRVars:
    # Postgres implements min/max aggregates for only a specific
    # subset of scalars and their respective arrays. However, in
    # EdgeDB every type is orderable (supports < and >) and so to
    # accommodate that we must choose between the native Postgres
    # aggregate and the generic fallback implementation (the native
    # implementation being faster).
    #
    # Since the fallback implementation is not mapped onto the same
    # polymorphic function in Postgres as the implementation for
    # supported types, we cannot rely on Postgres to always correctly
    # pick the polymorphic function to call, instead we use static
    # type inference to determine whether we'll delegate this to
    # Postgres (e.g. for anyreal) or we'll use the slower
    # one-size-fits-all fallback which then gets compiled differently.
    # In particular this means that when used inside a body of another
    # polymorphic (anytype) function, the slower generic version of
    # min/max will be used regardless of the actual concrete input
    # type.

    expr = ir_set.expr
    with ctx.subrel() as newctx:
        ir_arg = expr.args[0].expr
        dispatch.visit(ir_arg, ctx=newctx)

        arg_ref = pathctx.get_path_value_var(
            newctx.rel, ir_arg.path_id, env=newctx.env)

        arg_val = output.output_as_value(arg_ref, env=newctx.env)

        if newctx.rel.sort_clause is None:
            newctx.rel.sort_clause = []
        newctx.rel.sort_clause.append(
            pgast.SortBy(
                node=arg_val,
                dir=(
                    pgast.SortAsc
                    if str(expr.func_shortname) == 'std::min'
                    else pgast.SortDesc
                ),
            ),
        )
        newctx.rel.limit_count = pgast.NumericConstant(val='1')

        pathctx.put_path_id_map(newctx.rel, ir_set.path_id, ir_arg.path_id)

    func_rvar = relctx.new_rel_rvar(ir_set, newctx.rel, ctx=ctx)
    relctx.include_rvar(
        ctx.rel, func_rvar, ir_set.path_id, pull_namespace=False, ctx=ctx
    )

    return new_stmt_set_rvar(ir_set, ctx.rel, ctx=ctx)


@simple_special_case('std::range')
def process_set_as_std_range(
    expr: irast.FunctionCall,
    *,
    ctx: context.CompilerContextLevel,
) -> pgast.BaseExpr:
    # Generic range constructor implementation
    #
    #   std::range(
    #     lower,
    #     upper,
    #     named only inc_lower,
    #     named only inc_upper,
    #     named only empty,
    #   )
    #
    #     into
    #
    #   case when empty then
    #     'empty'::<pg_range_type>
    #   else
    #     <pg_range_type>(
    #       lower,
    #       upper,
    #       (array[['()', '(]'], ['[)', '[]']])
    #         [inc_lower::int + 1][inc_upper::int + 1]
    #     )
    #   end

    empty = dispatch.compile(expr.args['empty'].expr, ctx=ctx)
    inc_lower = dispatch.compile(expr.args['inc_lower'].expr, ctx=ctx)
    inc_upper = dispatch.compile(expr.args['inc_upper'].expr, ctx=ctx)
    lower = dispatch.compile(expr.args[0].expr, ctx=ctx)
    upper = dispatch.compile(expr.args[1].expr, ctx=ctx)

    lb = pgast.Index(
        idx=astutils.new_binop(
            lexpr=pgast.TypeCast(
                arg=inc_lower,
                type_name=pgast.TypeName(name=('int4',)),
            ),
            op='+',
            rexpr=pgast.NumericConstant(val='1'),
        ),
    )

    rb = pgast.Index(
        idx=astutils.new_binop(
            lexpr=pgast.TypeCast(
                arg=inc_upper,
                type_name=pgast.TypeName(name=('int4',)),
            ),
            op='+',
            rexpr=pgast.NumericConstant(val='1'),
        ),
    )

    bounds_matrix = pgast.ArrayExpr(
        elements=[
            pgast.ArrayDimension(
                elements=[
                    pgast.StringConstant(val="()"),
                    pgast.StringConstant(val="(]"),
                ],
            ),
            pgast.ArrayDimension(
                elements=[
                    pgast.StringConstant(val="[)"),
                    pgast.StringConstant(val="[]"),
                ],
            ),
        ]
    )

    bounds = pgast.Indirection(arg=bounds_matrix, indirection=[lb, rb])
    pg_type = pg_types.pg_type_from_ir_typeref(expr.typeref)
    non_empty_range = pgast.FuncCall(name=pg_type, args=[lower, upper, bounds])
    empty_range = pgast.TypeCast(
        arg=pgast.StringConstant(val='empty'),
        type_name=pgast.TypeName(name=pg_type),
    )

    # If any of the non-optional arguments are nullable, add an explicit
    # null check for them.
    null_checks = [
        pgast.NullTest(arg=e) for e in [empty, inc_upper, inc_lower]
        if e.nullable
    ]
    if null_checks:
        null_case = [
            pgast.CaseWhen(
                expr=astutils.extend_binop(None, *null_checks, op='OR'),
                result=pgast.NullConstant(),
            )
        ]
    else:
        null_case = []

    set_expr = pgast.CaseExpr(
        args=[
            *null_case,
            pgast.CaseWhen(
                expr=pgast.FuncCall(
                    name=astutils.edgedb_func('range_validate', ctx=ctx),
                    args=[lower, upper, inc_lower, inc_upper, empty],
                ),
                result=empty_range,
            ),
        ],
        defresult=non_empty_range,
    )

    return set_expr


@simple_special_case('std::_is_exclusive')
def process_set_as_std_is_exclusive(
    expr: irast.FunctionCall,
    *,
    ctx: context.CompilerContextLevel,
) -> pgast.BaseExpr:
    # `std::_is_exclusive` is a helper function used in the implementation of
    # exclusive constraints. It is removed before (ir->sql) compilation and will
    # never be executed by the server.
    #
    # However, during the (ql->ir) compilation, an additional (ir->sql)
    # compilation takes place in order to catch any potential downstream errors,
    # such as set returning functions.
    #
    # This simple special case is used to prevent unwanted errors during this
    # additional (ir->sql) compilation.
    return pgast.BooleanConstant(val=False)


@_special_case('std::multirange', only_as_fallback=True)
def process_set_as_std_multirange(
    ir_set: irast.SetE[irast.Call],
    *,
    ctx: context.CompilerContextLevel,
) -> SetRVars:
    # Generic multirange constructor implementation
    #
    #   std::multirange(
    #     ranges: array<range<anypoint>>,
    #   )
    #
    #     into
    #
    #   <pg_range_type>(variadic ranges)
    expr = ir_set.expr

    ranges = dispatch.compile(expr.args[0].expr, ctx=ctx)
    pg_type = pg_types.pg_type_from_ir_typeref(expr.typeref)
    set_expr = pgast.FuncCall(
        name=pg_type,
        args=[pgast.VariadicArgument(expr=ranges)]
    )

    pathctx.put_path_value_var(ctx.rel, ir_set.path_id, set_expr)

    return new_stmt_set_rvar(ir_set, ctx.rel, ctx=ctx)


@register_get_rvar(irast.Call)
def process_set_as_call(
    ir_set: irast.SetE[irast.Call], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    fname = str(ir_set.expr.func_shortname)
    if (func := _SPECIAL_FUNCTIONS.get(fname)) and (
        not func.only_as_fallback or ir_set.expr.func_sql_expr
    ):
        return func.func(ir_set, ctx=ctx)

    # Route simple special functions through expr compilation
    if fname in _SIMPLE_SPECIAL_FUNCTIONS:
        return process_set_as_expr(ir_set, ctx=ctx)

    if irutils.is_set_instance(ir_set, irast.OperatorCall):
        # Operator call
        return process_set_as_oper_expr(ir_set, ctx=ctx)

    if any(
        arg.param_typemod is qltypes.TypeModifier.SetOfType
        for key, arg in ir_set.expr.args.items()
    ):
        # Call to an aggregate function.
        assert irutils.is_set_instance(ir_set, irast.FunctionCall)
        return process_set_as_agg_expr(ir_set, ctx=ctx)

    # Regular function call.
    return process_set_as_func_expr(ir_set, ctx=ctx)


def _process_set_func_with_ordinality(
        ir_set: irast.Set, *,
        outer_func_set: irast.Set,
        func_name: Tuple[str, ...],
        args: List[pgast.BaseExpr],
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    expr = ir_set.expr
    assert isinstance(expr, irast.FunctionCall)
    rtype = outer_func_set.typeref
    outer_func_expr = outer_func_set.expr
    assert isinstance(outer_func_expr, irast.FunctionCall)

    named_tuple = any(st.element_name for st in rtype.subtypes)
    inner_rtype = ir_set.typeref

    coldeflist = []
    arg_is_tuple = irtyputils.is_tuple(inner_rtype)

    if arg_is_tuple:
        subtypes = {}
        for i, st in enumerate(inner_rtype.subtypes):
            colname = st.element_name or f'_t{i + 1}'
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

    if (expr.sql_func_has_out_params
            or irtyputils.is_persistent_tuple(inner_rtype)):
        # SQL functions declared with OUT params reject column definitions.
        # Also persistent tuple types
        coldeflist = []

    fexpr = pgast.FuncCall(name=func_name, args=args, coldeflist=coldeflist)

    colnames.append(
        rtype.subtypes[0].element_name or '_i'
    )

    func_rvar = pgast.RangeFunction(
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get('f'),
            colnames=colnames),
        lateral=True,
        is_rowsfrom=True,
        with_ordinality=True,
        functions=[fexpr])

    ctx.rel.from_clause.append(func_rvar)

    inner_expr: pgast.OutputVar

    if arg_is_tuple:
        inner_named_tuple = any(st.element_name for st in inner_rtype.subtypes)
        inner_expr = pgast.TupleVar(
            elements=[
                pgast.TupleElement(
                    path_id=outer_func_expr.tuple_path_ids[
                        len(rtype.subtypes) + i],
                    name=n,
                    val=astutils.get_column(
                        func_rvar, n, nullable=fexpr.nullable)
                )
                for i, n in enumerate(colnames[:-1])
            ],
            named=inner_named_tuple,
            typeref=inner_rtype,
        )
    else:
        inner_expr = astutils.get_column(
            func_rvar, colnames[0], nullable=fexpr.nullable)

    set_expr = pgast.TupleVar(
        elements=[
            pgast.TupleElement(
                path_id=outer_func_expr.tuple_path_ids[0],
                name=colnames[0],
                val=pgast.Expr(
                    name='-',
                    lexpr=astutils.get_column(
                        func_rvar, colnames[-1], nullable=fexpr.nullable,
                    ),
                    rexpr=pgast.NumericConstant(val='1')
                )
            ),
            pgast.TupleElement(
                path_id=outer_func_expr.tuple_path_ids[1],
                name=rtype.subtypes[1].element_name or '1',
                val=inner_expr,
            ),
        ],
        named=named_tuple,
        typeref=outer_func_set.typeref,
    )

    for element in set_expr.elements:
        pathctx.put_path_value_var(ctx.rel, element.path_id, element.val)

    if arg_is_tuple:
        arg_tuple = set_expr.elements[1].val
        assert isinstance(arg_tuple, pgast.TupleVar)
        for element in arg_tuple.elements:
            pathctx.put_path_value_var(ctx.rel, element.path_id, element.val)

    # If there is a shape specified on the argument to enumerate, we need
    # to compile it here manually, since we are skipping the normal
    # code path for it.
    if (output.in_serialization_ctx(ctx) and ir_set.shape
            and not ctx.env.ignore_object_shapes):
        ensure_source_rvar(ir_set, ctx.rel, ctx=ctx)
        exprcomp._compile_shape(ir_set, ir_set.shape, ctx=ctx)

    var = pathctx.maybe_get_path_var(
        ctx.rel,
        ir_set.path_id,
        aspect=pgce.PathAspect.SERIALIZED,
        env=ctx.env,
    )
    if var is not None:
        pathctx.put_path_var(
            ctx.rel,
            set_expr.elements[1].path_id,
            var,
            aspect=pgce.PathAspect.SERIALIZED,
        )

    return set_expr


def _process_set_func(
        ir_set: irast.Set, *,
        func_name: Tuple[str, ...],
        args: List[pgast.BaseExpr],
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:
    expr = ir_set.expr
    assert isinstance(expr, irast.FunctionCall)

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

    if (
        # SQL functions declared with OUT params or returning
        # named composite types reject column definitions.
        irtyputils.is_persistent_tuple(rtype)
        or expr.sql_func_has_out_params
    ):
        coldeflist = []

    fexpr = pgast.FuncCall(name=func_name, args=args, coldeflist=coldeflist)

    func_rvar = pgast.RangeFunction(
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get('f'),
            colnames=colnames),
        lateral=True,
        is_rowsfrom=True,
        functions=[fexpr])

    ctx.rel.from_clause.append(func_rvar)

    set_expr: pgast.BaseExpr

    if not is_tuple:
        set_expr = astutils.get_column(
            func_rvar, colnames[0], nullable=fexpr.nullable)
    else:
        set_expr = pgast.TupleVar(
            elements=[
                pgast.TupleElement(
                    path_id=expr.tuple_path_ids[i],
                    name=n,
                    val=astutils.get_column(
                        func_rvar, n, nullable=fexpr.nullable)
                )
                for i, n in enumerate(colnames)
            ],
            named=named_tuple,
            typeref=rtype,
        )

        for element in set_expr.elements:
            pathctx.put_path_value_var_if_not_exists(
                ctx.rel, element.path_id, element.val
            )

    return set_expr


def _compile_func_epilogue(
        ir_set: irast.Set, *,
        set_expr: pgast.BaseExpr,
        func_rel: pgast.SelectStmt,
        ctx: context.CompilerContextLevel) -> SetRVars:
    expr = ir_set.expr
    assert isinstance(expr, irast.FunctionCall)

    if expr.volatility.is_volatile():
        relctx.apply_volatility_ref(func_rel, ctx=ctx)

    pathctx.put_path_var_if_not_exists(
        func_rel,
        ir_set.path_id,
        set_expr,
        aspect=pgce.PathAspect.VALUE,
    )

    aspects: Tuple[pgce.PathAspect, ...]
    if expr.body:
        # For inlined functions, we want all of the aspects provided.
        aspects = tuple(pathctx.list_path_aspects(func_rel, ir_set.path_id))
    else:
        # Otherwise we just know we have value.
        aspects = (pgce.PathAspect.VALUE,)

    func_rvar = relctx.new_rel_rvar(ir_set, func_rel, ctx=ctx)
    relctx.include_rvar(
        ctx.rel,
        func_rvar,
        ir_set.path_id,
        pull_namespace=False,
        aspects=aspects,
        ctx=ctx,
    )

    if (ir_set.path_id.is_tuple_path()
            and expr.typemod is qltypes.TypeModifier.SetOfType):
        # Functions returning a set of tuples are compiled with an
        # explicit coldeflist, so the result is represented as a
        # TupleVar as opposed to an opaque record datum, so
        # we can access the elements directly without using
        # `tuple_getattr()`.
        aspects += (pgce.PathAspect.SOURCE,)

    return new_stmt_set_rvar(ir_set, ctx.rel, aspects=aspects, ctx=ctx)


def _needs_arg_null_check(
    call_expr: irast.Call, ir_arg: irast.CallArg,
    typemod: qltypes.TypeModifier, *,
    ctx: context.CompilerContextLevel
) -> bool:
    return (
        not call_expr.impl_is_strict
        and not ir_arg.is_default
        and (
            (
                typemod == qltypes.TypeModifier.SingletonType
                and ir_arg.cardinality.can_be_zero()
            ) or typemod == qltypes.TypeModifier.SetOfType
        )
    )


def _compile_arg_null_check(
    call_expr: irast.Call, ir_arg: irast.CallArg, arg_ref: pgast.BaseExpr,
    typemod: qltypes.TypeModifier, *,
    ctx: context.CompilerContextLevel
) -> None:
    if (
        _needs_arg_null_check(call_expr, ir_arg, typemod, ctx=ctx)
        and arg_ref.nullable
    ):
        ctx.rel.where_clause = astutils.extend_binop(
            ctx.rel.where_clause,
            pgast.NullTest(arg=arg_ref, negated=True)
        )


def _compile_call_args(
    ir_set: irast.Set,
    *,
    skip: Collection[int] = (),
    no_subquery_args: bool = False,
    ctx: context.CompilerContextLevel,
) -> list[pgast.BaseExpr]:
    """
    Compiles function call arguments, whose index is not in `skip`.
    """

    expr = ir_set.expr
    assert isinstance(expr, irast.Call)

    args = []

    if isinstance(expr, irast.FunctionCall) and expr.global_args:
        for glob_arg in expr.global_args:
            arg_ref = dispatch.compile(glob_arg, ctx=ctx)
            args.append(output.output_as_value(arg_ref, env=ctx.env))

    for ir_key, ir_arg in expr.args.items():
        if ir_key in skip:
            continue
        assert ir_arg.multiplicity != qltypes.Multiplicity.UNKNOWN

        typemod = ir_arg.param_typemod

        # Support a mode where we try to compile arguments as pure
        # subqueries. This is occasionally valuable as it lets us
        # "push down" the subqueries from the top level, which is
        # important for things like hitting pgvector indexes in an
        # ORDER BY.
        arg_typeref = ir_arg.expr.typeref
        make_subquery = (
            expr.prefer_subquery_args
            and typemod != qltypes.TypeModifier.SetOfType
            and ir_arg.cardinality.is_single()
            and (arg_typeref.is_scalar or arg_typeref.collection)
            and not _needs_arg_null_check(expr, ir_arg, typemod, ctx=ctx)
            and not no_subquery_args
        )

        if make_subquery:
            arg_ref = set_as_subquery(ir_arg.expr, as_value=True, ctx=ctx)
            arg_ref.nullable = ir_arg.cardinality.can_be_zero()
            arg_ref = astutils.collapse_query(arg_ref)
        else:
            arg_ref = dispatch.compile(ir_arg.expr, ctx=ctx)
            arg_ref = output.output_as_value(arg_ref, env=ctx.env)
        args.append(arg_ref)
        _compile_arg_null_check(expr, ir_arg, arg_ref, typemod, ctx=ctx)

        if (
            isinstance(expr, irast.FunctionCall)
            and ir_arg.expr_type_path_id is not None
        ):
            # Object type arguments are represented by two
            # SQL arguments: object id and object type id.
            # The latter is needed for proper overload
            # dispatch.
            ensure_source_rvar(ir_arg.expr, ctx.rel, ctx=ctx)
            type_ref = relctx.get_path_var(
                ctx.rel,
                ir_arg.expr_type_path_id,
                aspect=pgce.PathAspect.IDENTITY,
                ctx=ctx,
            )
            args.append(type_ref)

    if (
        isinstance(expr, irast.FunctionCall)
        and expr.has_empty_variadic
        and expr.variadic_param_type is not None
    ):
        var = pgast.TypeCast(
            arg=pgast.ArrayExpr(elements=[]),
            type_name=pgast.TypeName(
                name=pg_types.pg_type_from_ir_typeref(
                    expr.variadic_param_type)
            )
        )

        args.append(pgast.VariadicArgument(expr=var))

    return args


def _compile_inlined_call_args(
    ir_set: irast.Set, *, ctx: context.CompilerContextLevel
) -> None:
    expr = ir_set.expr
    assert isinstance(expr, irast.FunctionCall)
    assert expr.body is not None

    if irutils.contains_dml(expr.body):
        last_iterator = ctx.enclosing_cte_iterator

        # Compile args into an iterator CTE
        with ctx.newrel() as arg_ctx:
            dml.merge_iterator(last_iterator, arg_ctx.rel, ctx=arg_ctx)
            clauses.setup_iterator_volatility(last_iterator, ctx=arg_ctx)

            _compile_call_args(ir_set, ctx=arg_ctx)

            # Add iterator identity
            args_pathid = irast.PathId.new_dummy(ctx.env.aliases.get('args'))
            with arg_ctx.subrel() as args_pathid_ctx:
                relctx.create_iterator_identity_for_path(
                    args_pathid, args_pathid_ctx.rel, ctx=args_pathid_ctx
                )
            args_id_rvar = relctx.rvar_for_rel(
                args_pathid_ctx.rel, lateral=True, ctx=arg_ctx
            )
            relctx.include_rvar(
                arg_ctx.rel, args_id_rvar, path_id=args_pathid, ctx=arg_ctx
            )

            for ir_arg in expr.args.values():
                arg_path_id = ir_arg.expr.path_id
                # Ensure args appear in arg CTE
                pathctx.get_path_output(
                    arg_ctx.rel,
                    arg_path_id,
                    aspect=pgce.PathAspect.VALUE,
                    env=arg_ctx.env,
                )
                pathctx.put_path_bond(arg_ctx.rel, arg_path_id, iterator=True)

        arg_cte = pgast.CommonTableExpr(
            name=ctx.env.aliases.get('args'),
            query=arg_ctx.rel,
            materialized=False,
        )

        arg_iterator = pgast.IteratorCTE(
            path_id=args_pathid,
            cte=arg_cte,
            parent=last_iterator,
            other_paths=tuple(
                (ir_arg.expr.path_id, pgce.PathAspect.VALUE)
                for ir_arg in expr.args.values()
            ),
            iterator_bond=True,
        )
        ctx.toplevel_stmt.append_cte(arg_cte)

        # Merge the new iterator
        ctx.path_scope = ctx.path_scope.new_child()
        dml.merge_iterator(arg_iterator, ctx.rel, ctx=ctx)
        clauses.setup_iterator_volatility(arg_iterator, ctx=ctx)

        ctx.enclosing_cte_iterator = arg_iterator

    else:
        with ctx.subrel() as arg_ctx:
            # Compile the call args but don't do anything with the resulting
            # exprs. Their rvar will be found when compiling the inlined body.
            _compile_call_args(ir_set, ctx=arg_ctx)

            arg_rvar = relctx.rvar_for_rel(arg_ctx.rel, ctx=ctx)

        for ir_arg in expr.args.values():
            arg_path_id = ir_arg.expr.path_id
            relctx.include_rvar(
                ctx.rel,
                arg_rvar,
                arg_path_id,
                ctx=ctx,
            )
            if arg_scope_stmt := relctx.maybe_get_scope_stmt(
                arg_path_id, ctx=ctx
            ):
                # The rvar is joined to ctx.rel, but other sets may
                # look for it in the scope statement. Make sure it's
                # available.
                pathctx.put_path_value_rvar(
                    arg_scope_stmt,
                    arg_path_id,
                    arg_rvar,
                )


def process_set_as_func_enumerate(
    ir_set: irast.Set, *, ctx: context.CompilerContextLevel
) -> SetRVars:
    expr = ir_set.expr
    assert isinstance(expr, irast.FunctionCall)

    inner_func_set = irutils.unwrap_set(expr.args[0].expr)
    inner_func = inner_func_set.expr
    assert isinstance(inner_func, irast.FunctionCall)

    with ctx.subrel() as newctx:
        with newctx.new() as newctx2:
            newctx2.expr_exposed = False
            args = _compile_call_args(inner_func_set, ctx=newctx2)
        func_name = exprcomp.get_func_call_backend_name(inner_func, ctx=newctx)

        set_expr = _process_set_func_with_ordinality(
            ir_set=inner_func_set,
            outer_func_set=ir_set,
            func_name=func_name,
            args=args,
            ctx=newctx)

        func_rel = newctx.rel

    return _compile_func_epilogue(
        ir_set, set_expr=set_expr, func_rel=func_rel, ctx=ctx
    )


def process_set_as_func_expr(
    ir_set: irast.Set, *, ctx: context.CompilerContextLevel
) -> SetRVars:
    expr = ir_set.expr
    assert isinstance(expr, irast.FunctionCall)

    with ctx.subrel() as newctx:
        newctx.expr_exposed = False

        if expr.body is not None:
            _compile_inlined_call_args(ir_set, ctx=newctx)

            set_expr = dispatch.compile(expr.body, ctx=newctx)
            # Map the path id so that we can extract source aspects
            # from it, which we want so that we can directly select
            # from an INSERT instead of using overlays.
            pathctx.put_path_id_map(
                newctx.rel, ir_set.path_id, expr.body.path_id
            )

        else:
            args = _compile_call_args(ir_set, ctx=newctx)

            name = exprcomp.get_func_call_backend_name(expr, ctx=newctx)

            if expr.typemod is qltypes.TypeModifier.SetOfType:
                set_expr = _process_set_func(
                    ir_set, func_name=name, args=args, ctx=newctx)
            else:
                set_expr = pgast.FuncCall(name=name, args=args)

        if expr.error_on_null_result:
            set_expr = pgast.FuncCall(
                name=astutils.edgedb_func('raise_on_null', ctx=ctx),
                args=[
                    set_expr,
                    pgast.StringConstant(
                        val='invalid_parameter_value',
                    ),
                    pgast.StringConstant(
                        val=expr.error_on_null_result,
                    ),
                    pgast.StringConstant(
                        val=irutils.get_span_as_json(
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

        func_rel = newctx.rel

    return _compile_func_epilogue(
        ir_set, set_expr=set_expr, func_rel=func_rel, ctx=ctx
    )


def process_set_as_agg_expr_inner(
    ir_set: irast.SetE[irast.FunctionCall],
    *,
    aspect: pgce.PathAspect,
    wrapper: Optional[pgast.SelectStmt],
    for_group_by: bool = False,
    ctx: context.CompilerContextLevel,
) -> SetRVars:
    expr = ir_set.expr
    assert isinstance(expr, irast.FunctionCall)
    stmt = ctx.rel

    set_expr: pgast.BaseExpr

    with ctx.newscope() as newctx:
        agg_filter = None
        agg_sort = []

        with newctx.new() as argctx:
            # We want array_agg() (and similar) to do the right
            # thing with respect to output format, so, barring
            # the (unacceptable) hardcoding of function names,
            # check if the aggregate accepts a single argument
            # of "any" to determine serialized input safety.
            serialization_safe = (
                expr.func_polymorphic
                and aspect == pgce.PathAspect.SERIALIZED
            )

            if not serialization_safe:
                argctx.expr_exposed = False

            args = []

            for ir_call_key, ir_call_arg in expr.args.items():
                ir_arg = ir_call_arg.expr

                arg_ref: pgast.BaseExpr
                if for_group_by:
                    arg_ref = set_as_subquery(
                        ir_arg, as_value=True, ctx=argctx)
                    arg_ref.nullable = False
                elif aspect == pgce.PathAspect.SERIALIZED:
                    dispatch.visit(ir_arg, ctx=argctx)

                    arg_ref = pathctx.get_path_serialized_or_value_var(
                        argctx.rel, ir_arg.path_id, env=argctx.env)

                    if isinstance(arg_ref, pgast.TupleVar):
                        arg_ref = output.serialize_expr(
                            arg_ref, path_id=ir_arg.path_id, env=argctx.env)
                else:
                    dispatch.visit(ir_arg, ctx=argctx)

                    arg_ref = pathctx.get_path_value_var(
                        argctx.rel, ir_arg.path_id, env=argctx.env)

                    if isinstance(arg_ref, pgast.TupleVar):
                        arg_ref = output.output_as_value(
                            arg_ref, env=argctx.env)

                _compile_arg_null_check(
                    expr,
                    ir_call_arg,
                    arg_ref,
                    ir_call_arg.param_typemod,
                    ctx=argctx
                )

                path_scope = relctx.get_scope(ir_arg, ctx=argctx)
                if path_scope is not None and path_scope.parent is not None:
                    arg_is_visible = path_scope.parent.is_any_prefix_visible(
                        ir_arg.path_id)
                else:
                    arg_is_visible = False

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
                    wrapper_rvar = relctx.rvar_for_rel(
                        wrapper, lateral=True, colnames=[colname], ctx=argctx)
                    relctx.include_rvar(
                        ctx.rel,
                        wrapper_rvar,
                        path_id=ir_arg.path_id,
                        ctx=argctx,
                    )
                    arg_ref = astutils.get_column(wrapper_rvar, colname)

                if ir_call_key == 0 and irutils.is_subquery_set(ir_arg):
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
                            ctx.rel,
                            ir_arg.path_id,
                            aspect=pgce.PathAspect.VALUE,
                        )
                        query = qrvar.query
                        assert isinstance(query, pgast.SelectStmt)

                        for i, sortref in enumerate(query.sort_clause or ()):
                            alias = argctx.env.aliases.get(f's{i}')

                            query.target_list.append(
                                pgast.ResTarget(
                                    val=sortref.node,
                                    name=alias
                                )
                            )

                            agg_sort.append(
                                pgast.SortBy(
                                    node=astutils.get_column(qrvar, alias),
                                    dir=sortref.dir,
                                    nulls=sortref.nulls))

                        query.sort_clause = []

                args.append(arg_ref)

        name = exprcomp.get_func_call_backend_name(expr, ctx=newctx)

        set_expr = pgast.FuncCall(
            name=name, args=args, agg_order=agg_sort, agg_filter=agg_filter,
            ser_safe=serialization_safe and all(x.ser_safe for x in args))

        if for_group_by and not expr.impl_is_strict:
            # If we are doing this for a GROUP BY, and the function is not
            # strict in its arguments, we are in trouble!

            # The problem is that we don't have a way to filter the NULLs
            # out in the subquery in general. The value could be
            # computed *inside* the subquery, so we can't use an agg_filter,
            # and we can't filter it inside the subquery because it gets
            # executed separately for each row and collapses to NULL when
            # it is empty!

            # Fortunately I think that only array_agg has this property,
            # so we can just handle that by popping the NULLs out.
            # If other cases turn up, we could handle it by falling
            # back to aggregate grouping.

            # TODO: only do this when there might really be a null?
            assert str(expr.func_shortname) == 'std::array_agg'
            set_expr = pgast.FuncCall(
                name=('array_remove',),
                args=[set_expr, pgast.NullConstant()]
            )

        if expr.error_on_null_result:
            set_expr = pgast.FuncCall(
                name=astutils.edgedb_func('raise_on_null', ctx=ctx),
                args=[
                    set_expr,
                    pgast.StringConstant(
                        val='invalid_parameter_value',
                    ),
                    pgast.StringConstant(
                        val=expr.error_on_null_result,
                    ),
                    pgast.StringConstant(
                        val=irutils.get_span_as_json(
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

    if expr.func_initial_value is not None and wrapper:
        iv_ir = expr.func_initial_value.expr
        assert iv_ir is not None

        if serialization_safe and aspect == pgce.PathAspect.SERIALIZED:
            # Serialization has changed the output type.
            with ctx.new() as ivctx:
                iv = dispatch.compile(iv_ir, ctx=ivctx)

                iv = output.serialize_expr_if_needed(
                    iv, path_id=ir_set.path_id, ctx=ctx)
                set_expr = output.serialize_expr_if_needed(
                    set_expr, path_id=ir_set.path_id, ctx=ctx)
        else:
            with ctx.new() as ivctx:
                iv = dispatch.compile(iv_ir, ctx=ivctx)

        pathctx.put_path_var(stmt, ir_set.path_id, set_expr, aspect=aspect)
        out = pathctx.get_path_output(
            stmt, ir_set.path_id, aspect=aspect, env=ctx.env
        )
        assert isinstance(out, pgast.ColumnRef)

        # HACK: We select join in the inner statement instead of just
        # using it as a subquery to work around a postgres bug that
        # occurs when something defined with a subquery is used as an
        # argument to `grouping`. See #3844.
        stmt_rvar = relctx.rvar_for_rel(stmt, ctx=ctx)
        wrapper.from_clause.append(stmt_rvar)
        val = astutils.get_column(stmt_rvar, out)

        assert wrapper
        set_expr = pgast.CoalesceExpr(
            args=[val, iv], ser_safe=serialization_safe)

        pathctx.put_path_var(wrapper, ir_set.path_id, set_expr, aspect=aspect)
        stmt = wrapper

    pathctx.put_path_var_if_not_exists(
        stmt, ir_set.path_id, set_expr, aspect=aspect
    )
    # Cheat a little bit: as discussed above, pretend the serialized
    # value is also really a value. Eta-expansion should ensure this
    # only happens when we don't really need the value again.
    if aspect == pgce.PathAspect.SERIALIZED:
        pathctx.put_path_var_if_not_exists(
            stmt, ir_set.path_id, set_expr, aspect=pgce.PathAspect.VALUE
        )

    return new_stmt_set_rvar(ir_set, stmt, ctx=ctx)


def process_set_as_agg_expr(
    ir_set: irast.SetE[irast.FunctionCall], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    # If the func has an initial val, we need to do the interesting
    # work in subrels and provide a wrapper to put the coalesces in
    wrapper = None
    if ir_set.expr.func_initial_value is not None:
        wrapper = ctx.rel

    # In a serialization context that produces something containing an object,
    # we produce *only* a serialized value, and we claim it is the value too.
    # For this to be correct, we need to only have serialized agg expr results
    # in cases where value can't be used anymore. Our eta-expansion pass
    # make sure this happens.
    # (... the only such *function* currently is array_agg.)

    # Though if the result type contains no objects, the value should be good
    # enough, so don't generate a bunch of unnecessary code to produce
    # a serialized value when we can use value.
    serialized = (
        output.in_serialization_ctx(ctx=ctx)
        and irtyputils.contains_object(ir_set.typeref)
    )

    cctx = ctx.subrel() if wrapper else ctx.new()
    with cctx as xctx:
        xctx.expr_exposed = serialized
        aspect = (
            pgce.PathAspect.SERIALIZED
            if serialized else
            pgce.PathAspect.VALUE
        )
        process_set_as_agg_expr_inner(
            ir_set, aspect=aspect, wrapper=wrapper, ctx=xctx
        )

    return new_stmt_set_rvar(ir_set, ctx.rel, ctx=ctx)


@_special_case('std::EXISTS')
def process_set_as_exists_expr(
    ir_set: irast.SetE[irast.Call], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    expr = ir_set.expr
    with ctx.subrel() as subctx:
        wrapper = subctx.rel
        subctx.expr_exposed = False
        ir_expr = expr.args[0].expr
        set_ref = dispatch.compile(ir_expr, ctx=subctx)

        pathctx.put_path_value_var(wrapper, ir_set.path_id, set_ref)
        pathctx.get_path_value_output(wrapper, ir_set.path_id, env=ctx.env)

        wrapper.where_clause = astutils.extend_binop(
            wrapper.where_clause, pgast.NullTest(arg=set_ref, negated=True))

        set_expr = pgast.SubLink(operator="EXISTS", expr=wrapper)

    pathctx.put_path_value_var(ctx.rel, ir_set.path_id, set_expr)
    return new_stmt_set_rvar(ir_set, ctx.rel, ctx=ctx)


@_special_case('std::json_object_pack')
def process_set_as_json_object_pack(
    ir_set: irast.SetE[irast.Call], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    ir_arg = ir_set.expr.args[0].expr

    # compile IR to pg AST
    dispatch.visit(ir_arg, ctx=ctx)
    arg_val = pathctx.get_path_value_var(ctx.rel, ir_arg.path_id, env=ctx.env)

    # get first and the second fields of the tuple
    if isinstance(arg_val, pgast.TupleVar):
        keys = arg_val.elements[0].val
        values = arg_val.elements[1].val
    else:
        keys = astutils.tuple_getattr(arg_val, ir_arg.typeref, "0")
        values = astutils.tuple_getattr(arg_val, ir_arg.typeref, "1")

    # construct the function call
    set_expr = pgast.FuncCall(
        name=("coalesce",),
        args=[
            pgast.FuncCall(name=("jsonb_object_agg",), args=[keys, values]),
            pgast.TypeCast(
                arg=pgast.StringConstant(val="{}"),
                type_name=pgast.TypeName(name=('jsonb',)),
            ),
        ],
    )

    # declare that the 'aspect=value' of ir_set (original set)
    # can be found by in ctx.rel, by using set_expr
    pathctx.put_path_value_var_if_not_exists(ctx.rel, ir_set.path_id, set_expr)

    # return subquery as set_rvar
    return new_stmt_set_rvar(ir_set, ctx.rel, ctx=ctx)


def build_array_expr(
        ir_expr: irast.Base,
        elements: List[pgast.BaseExpr], *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    array = astutils.safe_array_expr(elements, ctx=ctx)

    if irutils.is_empty_array_expr(ir_expr):
        assert isinstance(ir_expr, irast.Array)
        typeref = ir_expr.typeref

        if irtyputils.is_any(typeref.subtypes[0]):
            # The type of the input is not determined, which means that
            # the result of this expression is passed as an argument
            # to a generic function, e.g. `count(array_agg({}))`.  In this
            # case, amend the array type to a concrete type,
            # since Postgres balks at `[]::anyarray`.
            pg_type: Tuple[str, ...] = ('text[]',)
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


@register_get_rvar(irast.Array)
def process_set_as_array_expr(
    ir_set: irast.SetE[irast.Array], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    expr = ir_set.expr

    elements = []
    s_elements = []
    serializing = (
        output.in_serialization_ctx(ctx=ctx)
        and irtyputils.contains_object(ir_set.typeref)
    )

    for ir_element in expr.elements:
        element = dispatch.compile(ir_element, ctx=ctx)
        elements.append(element)

        if serializing:
            s_var: Optional[pgast.BaseExpr]

            s_var = pathctx.maybe_get_path_serialized_var(
                ctx.rel, ir_element.path_id, env=ctx.env
            )

            if s_var is None:
                v_var = pathctx.get_path_value_var(
                    ctx.rel, ir_element.path_id, env=ctx.env
                )
                s_var = output.serialize_expr(
                    v_var, path_id=ir_element.path_id, env=ctx.env)
            elif isinstance(s_var, pgast.TupleVar):
                s_var = output.serialize_expr(
                    s_var, path_id=ir_element.path_id, env=ctx.env)

            s_elements.append(s_var)

    if serializing:
        set_expr = astutils.safe_array_expr(
            s_elements, ser_safe=all(x.ser_safe for x in s_elements), ctx=ctx)

        if irutils.is_empty_array_expr(expr):
            set_expr = pgast.TypeCast(
                arg=set_expr,
                type_name=pgast.TypeName(
                    name=pg_types.pg_type_from_ir_typeref(expr.typeref)
                )
            )

        pathctx.put_path_serialized_var(ctx.rel, ir_set.path_id, set_expr)
    else:
        set_expr = build_array_expr(expr, elements, ctx=ctx)

    pathctx.put_path_value_var_if_not_exists(ctx.rel, ir_set.path_id, set_expr)

    return new_stmt_set_rvar(ir_set, ctx.rel, ctx=ctx)


def process_encoded_param(
        param: irast.Param, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseExpr:

    assert param.sub_params
    decoder = param.sub_params.decoder_ir
    assert decoder

    if (param_cte := ctx.param_ctes.get(param.name)) is None:
        with ctx.newrel() as sctx:
            sctx.pending_query = sctx.rel
            sctx.rel_overlays = context.RelOverlays()
            arg_ref = dispatch.compile(decoder, ctx=sctx)

            # Force it into a real tuple so we can just always grab it
            # from a subquery below.
            arg_val = output.output_as_value(arg_ref, env=sctx.env)
            pathctx.put_path_value_var(
                sctx.rel, decoder.path_id, arg_val, force=True
            )

            param_cte = pgast.CommonTableExpr(
                name=ctx.env.aliases.get('p'),
                query=sctx.rel,
                materialized=False,
            )
            ctx.param_ctes[param.name] = param_cte

    with ctx.subrel() as sctx:
        cte_rvar = pgast.RelRangeVar(
            relation=param_cte,
            typeref=decoder.typeref,
            alias=pgast.Alias(aliasname=ctx.env.aliases.get('t'))
        )
        relctx.include_rvar(
            sctx.rel, cte_rvar, decoder.path_id, pull_namespace=False,
            aspects=(pgce.PathAspect.VALUE,), ctx=sctx,
        )
        pathctx.get_path_value_output(sctx.rel, decoder.path_id, env=ctx.env)
        if not param.required:
            sctx.rel.nullable = True

    return sctx.rel


_ObjectSearchInnerCallback = Callable[
    [
        irast.Call,
        irast.PathId,
        list[pgast.BaseExpr],
        context.CompilerContextLevel,
        context.CompilerContextLevel,
        context.CompilerContextLevel,
    ],
    tuple[pgast.BaseExpr, Optional[pgast.BaseExpr]],
]


@_special_case('std::fts::search')
def process_set_as_fts_search(
    ir_set: irast.SetE[irast.Call], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    from edb.common import debug

    cb: _ObjectSearchInnerCallback
    if debug.flags.zombodb:
        cb = _fts_search_inner_zombo
    else:
        cb = _fts_search_inner_pg

    return _process_set_as_object_search(
        ir_set, inner_cb=cb, ctx=ctx)


@_special_case('ext::ai::search')
def process_set_as_ext_ai_search(
    ir_set: irast.SetE[irast.Call], *, ctx: context.CompilerContextLevel
) -> SetRVars:
    cb = _ext_ai_search_inner_pgvector
    return _process_set_as_object_search(
        ir_set, inner_cb=cb, ctx=ctx)


def _ext_ai_search_inner_pgvector(
    call: irast.Call,
    obj_id: irast.PathId,
    args_pg: list[pgast.BaseExpr],
    _ctx: context.CompilerContextLevel,
    newctx: context.CompilerContextLevel,
    _inner_ctx: context.CompilerContextLevel,
) -> Tuple[pgast.BaseExpr, Optional[pgast.BaseExpr]]:
    assert isinstance(call, irast.FunctionCall)
    if call.extras is None:
        raise AssertionError(
            "missing expected index metadata in FunctionCall.extras")
    index_metadata = call.extras.get("index_metadata")
    if index_metadata is None:
        raise AssertionError(
            "missing expected index metadata in FunctionCall.extras")
    tgt = obj_id.target
    if tgt.material_type is not None:
        tgt = tgt.material_type
    target_index_metadata = index_metadata.get(tgt)
    if target_index_metadata is None:
        raise AssertionError(
            "missing expected index metadata in FunctionCall.extras")
    index_id = target_index_metadata.get("id")
    if index_id is None:
        raise AssertionError(
            "missing expected index metadata in FunctionCall.extras")
    dimensions = target_index_metadata.get("dimensions")
    if dimensions is None:
        raise AssertionError(
            "missing expected index metadata in FunctionCall.extras")
    df = target_index_metadata.get("distance_function")
    if index_id is None:
        raise AssertionError(
            "missing expected index metadata in FunctionCall.extras")

    query, = args_pg
    el_name = sn.QualName(
        '__object__',
        f'__ext_ai_{index_id}_embedding__',
    )
    embedding_ptrref = irast.SpecialPointerRef(
        name=el_name,
        shortname=el_name,
        out_source=obj_id.target,
        out_target=pg_types.pg_tsvector_typeref,
        out_cardinality=qltypes.Cardinality.AT_MOST_ONE,
    )
    embedding_id = obj_id.extend(ptrref=embedding_ptrref)
    embedding = relctx.get_path_var(
        newctx.rel,
        embedding_id,
        aspect=pgce.PathAspect.VALUE,
        ctx=newctx,
    )

    similarity = pgast.FuncCall(
        name=common.get_function_backend_name(*df),
        args=[
            embedding,
            pgast.TypeCast(
                arg=query,
                type_name=pgast.TypeName(
                    name=('edgedb', f'vector({dimensions})'),
                ),
            ),
        ],
    )

    # Install the filter directly in newctx.rel. We could return it
    # and have it put in inner_ctx.rel, and that does seem to work,
    # but seems weirder.
    valid = pgast.NullTest(arg=embedding, negated=True)
    newctx.rel.where_clause = astutils.extend_binop(
        newctx.rel.where_clause, valid
    )

    # Do an integrated sort. This ensures we can hit the index, and is
    # more ergonomic anyway. Having the ORDER BY operate directly on
    # the function call is not the *only* way to have it work, but it
    # is the most reliable.
    sort_by = pgast.SortBy(
        node=similarity,
        dir=qlast.SortOrder.Asc,
        nulls=qlast.NonesOrder.Last,
    )
    if newctx.rel.sort_clause is None:
        newctx.rel.sort_clause = []
    newctx.rel.sort_clause.append(sort_by)

    return similarity, None


def _process_set_as_object_search(
    ir_set: irast.SetE[irast.Call],
    *,
    inner_cb: _ObjectSearchInnerCallback,
    ctx: context.CompilerContextLevel,
) -> SetRVars:
    func_call = ir_set.expr

    # We skip the object, as it has to be compiled as rvar source.
    #
    # Also, disable subquery args. ai::search needs it for its
    # scoping effects, but we don't need to use it here, since
    # it can cause the ai search to duplicate arguments.
    args_pg = _compile_call_args(
        ir_set, skip={0}, no_subquery_args=True, ctx=ctx)

    with ctx.subrel() as newctx:
        newctx.expr_exposed = False

        obj_ir = func_call.args[0].expr
        obj_id = obj_ir.path_id
        obj_rvar = ensure_source_rvar(obj_ir, newctx.rel, ctx=newctx)

        out_obj_id, out_score_id = func_call.tuple_path_ids

        with newctx.subrel() as inner_ctx:
            # inner_ctx generates the `SELECT score WHERE test` relation
            score_pg, where_clause = inner_cb(
                func_call,
                obj_id,
                args_pg,
                ctx,
                newctx,
                inner_ctx,
            )

            pathctx.put_path_var(
                inner_ctx.rel,
                out_score_id,
                score_pg,
                aspect=pgce.PathAspect.VALUE,
            )

            if where_clause is not None:
                inner_ctx.rel.where_clause = astutils.extend_binop(
                    inner_ctx.rel.where_clause, where_clause
                )

            in_rvar = relctx.new_rel_rvar(ir_set, inner_ctx.rel, ctx=newctx)
            relctx.include_rvar(
                newctx.rel,
                in_rvar,
                out_score_id,
                aspects={pgce.PathAspect.VALUE},
                ctx=newctx,
            )

        obj_id_pg_ref = pathctx.get_rvar_path_var(
            obj_rvar,
            obj_id,
            aspect=pgce.PathAspect.VALUE,
            env=newctx.env,
        )
        score_pg_ref = pathctx.get_path_var(
            newctx.rel,
            out_score_id,
            aspect=pgce.PathAspect.VALUE,
            env=newctx.env,
        )

        tuple_expr = pgast.TupleVar(
            elements=[
                pgast.TupleElement(
                    path_id=out_obj_id,
                    name='object',
                    val=obj_id_pg_ref,
                ),
                pgast.TupleElement(
                    path_id=out_score_id,
                    name='score',
                    val=score_pg_ref,
                ),
            ],
            named=True,
            typeref=ir_set.typeref,
        )

        pathctx.put_path_var(
            newctx.rel,
            ir_set.path_id,
            tuple_expr,
            aspect=pgce.PathAspect.VALUE,
        )

        var = pathctx.maybe_get_path_var(
            newctx.rel,
            obj_id,
            aspect=pgce.PathAspect.SERIALIZED,
            env=newctx.env,
        )
        if var is not None:
            pathctx.put_path_var(
                newctx.rel,
                out_obj_id,
                var,
                aspect=pgce.PathAspect.SERIALIZED,
            )

    pathctx.put_path_id_map(newctx.rel, out_obj_id, obj_id)

    aspects = {pgce.PathAspect.VALUE, pgce.PathAspect.SOURCE}

    func_rvar = relctx.new_rel_rvar(ir_set, newctx.rel, ctx=ctx)
    relctx.include_rvar(
        ctx.rel, func_rvar, ir_set.path_id, aspects=aspects, ctx=ctx
    )

    pathctx.put_path_rvar(
        ctx.rel,
        out_obj_id,
        func_rvar,
        aspect=pgce.PathAspect.SOURCE,
    )

    return new_stmt_set_rvar(ir_set, ctx.rel, aspects=aspects, ctx=ctx)


def _fts_search_inner_pg(
    _call: irast.Call,
    obj_id: irast.PathId,
    args_pg: list[pgast.BaseExpr],
    ctx: context.CompilerContextLevel,
    newctx: context.CompilerContextLevel,
    inner_ctx: context.CompilerContextLevel,
) -> Tuple[pgast.BaseExpr, pgast.BaseExpr]:
    lang, weights, query = args_pg
    el_name = sn.QualName('__object__', '__fts_document__')
    fts_document_ptrref = irast.SpecialPointerRef(
        name=el_name,
        shortname=el_name,
        out_source=obj_id.target,
        out_target=pg_types.pg_tsvector_typeref,
        out_cardinality=qltypes.Cardinality.AT_MOST_ONE,
    )
    fts_document_id = obj_id.extend(ptrref=fts_document_ptrref)
    fts_document = relctx.get_path_var(
        newctx.rel,
        fts_document_id,
        aspect=pgce.PathAspect.VALUE,
        ctx=newctx,
    )

    lang = pgast.FuncCall(
        name=astutils.edgedb_func('fts_to_regconfig', ctx=ctx),
        args=[lang],
    )

    parsed_query: pgast.BaseExpr = pgast.FuncCall(
        name=astutils.edgedb_func('fts_parse_query', ctx=ctx),
        args=[query, lang]
    )
    parsed_query_id = create_subrel_for_expr(parsed_query, ctx=inner_ctx)
    parsed_query = pathctx.get_path_var(
        inner_ctx.rel,
        parsed_query_id,
        aspect=pgce.PathAspect.VALUE,
        env=ctx.env,
    )

    weights = _fts_prepare_weights(weights, ctx=inner_ctx)

    score_pg = pgast.FuncCall(
        name=('pg_catalog', 'ts_rank'),
        args=[weights, fts_document, parsed_query],
    )
    where_clause = pgast.Expr(lexpr=fts_document, name='@@', rexpr=parsed_query)

    return score_pg, where_clause


def _fts_prepare_weights(
    weights: pgast.BaseExpr,
    ctx: context.CompilerContextLevel,
) -> pgast.BaseExpr:
    # default value
    default_weights = pgast.ArrayExpr(
        elements=[
            pgast.NumericConstant(val='1.0'),
            pgast.NumericConstant(val='0.5'),
            pgast.NumericConstant(val='0.25'),
            pgast.NumericConstant(val='0.125'),
        ]
    )
    weights = pgast.CoalesceExpr(args=[weights, default_weights])

    # cast to reals
    weights = pgast.TypeCast(
        arg=weights, type_name=pgast.TypeName(name=('real',), array_bounds=[-1])
    )

    # pad with zeros
    zero = pgast.NumericConstant(val='0.0')
    padding_weights = pgast.ArrayExpr(elements=[zero, zero, zero, zero])
    weights = pgast.Expr(
        lexpr=weights,
        name='||',
        rexpr=padding_weights,
    )

    # put the whole expression into subrel,
    # so it can be referenced mutiple times
    weights_id = create_subrel_for_expr(weights, ctx=ctx)
    weights = pathctx.get_path_var(
        ctx.rel,
        weights_id,
        aspect=pgce.PathAspect.VALUE,
        env=ctx.env,
    )

    # return array of first 4 values, reversed
    return pgast.ArrayExpr(
        elements=[
            pgast.Indirection(
                arg=weights,
                indirection=[
                    pgast.Index(idx=pgast.NumericConstant(val=str(i)))
                ],
            )
            for i in range(4, 0, -1)
        ]
    )


def _fts_search_inner_zombo(
    _call: irast.Call,
    obj_id: irast.PathId,
    args_pg: list[pgast.BaseExpr],
    _ctx: context.CompilerContextLevel,
    newctx: context.CompilerContextLevel,
    _inner_ctx: context.CompilerContextLevel,
) -> Tuple[pgast.BaseExpr, pgast.BaseExpr]:
    _, _, query = args_pg
    el_name = sn.QualName('__object__', 'ctid')
    ctid_ptrref = irast.SpecialPointerRef(
        name=el_name,
        shortname=el_name,
        out_source=obj_id.target,
        out_target=pg_types.pg_oid_typeref,
        out_cardinality=qltypes.Cardinality.AT_MOST_ONE,
    )
    ctid_id = obj_id.extend(ptrref=ctid_ptrref)
    ctid = relctx.get_path_var(
        newctx.rel,
        ctid_id,
        aspect=pgce.PathAspect.VALUE,
        ctx=newctx,
    )

    score_pg = pgast.FuncCall(name=('zdb', 'score'), args=[ctid])
    where_clause = pgast.Expr(
        lexpr=ctid,
        name='==>',
        rexpr=query,
    )
    return score_pg, where_clause


def create_subrel_for_expr(
    expr: pgast.BaseExpr, *, ctx: context.CompilerContextLevel
) -> irast.PathId:
    """
    Creates a sub query relation that contains the given expression.
    """

    # create a dummy path id for a dummy object
    expr_id = irast.PathId.new_dummy(ctx.env.aliases.get('d'))

    with ctx.subrel() as newctx:

        # register the expression
        pathctx.put_path_var(
            newctx.rel,
            expr_id,
            expr,
            aspect=pgce.PathAspect.VALUE,
        )

        # include the subrel in the parent
        new_rvar = relctx.rvar_for_rel(newctx.rel, ctx=ctx)
        relctx.include_rvar(
            ctx.rel,
            new_rvar,
            expr_id,
            aspects=(pgce.PathAspect.VALUE,),
            ctx=ctx,
        )

    return expr_id
