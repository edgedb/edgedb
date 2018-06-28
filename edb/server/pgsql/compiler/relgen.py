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

from edb.lang.common import ast

from edb.lang.ir import ast as irast
from edb.lang.ir import inference as irinference
from edb.lang.ir import utils as irutils

from edb.lang.schema import scalars as s_scalars
from edb.lang.schema import objtypes as s_objtypes
from edb.lang.schema import types as s_types

from edb.server.pgsql import ast as pgast
from edb.server.pgsql import common
from edb.server.pgsql import types as pg_types

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
        aspects: typing.Iterable[str]=('value',)) -> SetRVars:

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
        stmt.name = ctx.env.aliases.get(get_set_rel_alias(ir_set))

        # If ir.Set compilation needs to produce a subquery,
        # make sure it uses the current subrel.  This makes it
        # possible to set up the path scope here and don't worry
        # about it later.
        subctx.pending_query = stmt

        is_optional = subctx.scope_tree.is_optional(path_id)
        if is_optional:
            stmt, optrel = prepare_optional_rel(
                ir_set=ir_set, stmt=stmt, ctx=subctx)
            subctx.pending_query = subctx.rel = stmt

        path_scope = relctx.get_scope(ir_set, ctx=subctx)
        if path_scope:
            if path_scope.is_visible(path_id):
                subctx.path_scope[path_id] = scope_stmt
            relctx.update_scope(ir_set, stmt, ctx=subctx)

        rvars = _get_set_rvar(ir_set, ctx=subctx)

        if is_optional:
            rvars = finalize_optional_rel(ir_set, optrel=optrel,
                                          rvars=rvars, ctx=subctx)

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

    ctx.toplevel_stmt = ctx.stmt = ctx.rel = pgast.SelectStmt()
    relctx.update_scope(ir_set, ctx.rel, ctx=ctx)
    ctx.pending_query = ctx.rel
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

    elif isinstance(ir_set.expr, irast.SetOp):
        # Set operation: UNION
        rvars = process_set_as_setop(ir_set, stmt, ctx=ctx)

    elif isinstance(ir_set.expr, irast.DistinctOp):
        # DISTINCT Expr
        rvars = process_set_as_distinct(ir_set, stmt, ctx=ctx)

    elif isinstance(ir_set.expr, irast.IfElseExpr):
        # Expr IF Cond ELSE Expr
        rvars = process_set_as_ifelse(ir_set, stmt, ctx=ctx)

    elif isinstance(ir_set.expr, irast.Coalesce):
        # Expr ?? Expr
        rvars = process_set_as_coalesce(ir_set, stmt, ctx=ctx)

    elif isinstance(ir_set.expr, irast.EquivalenceOp):
        # Expr ?= Expr
        rvars = process_set_as_equivalence(ir_set, stmt, ctx=ctx)

    elif isinstance(ir_set.expr, irast.Tuple):
        # Named tuple
        rvars = process_set_as_tuple(ir_set, stmt, ctx=ctx)

    elif isinstance(ir_set.expr, irast.TupleIndirection):
        # Named tuple indirection.
        rvars = process_set_as_tuple_indirection(
            ir_set, stmt, ctx=ctx)

    elif isinstance(ir_set.expr, irast.FunctionCall):
        if any(k == irast.SetQualifier.SET_OF
               for k in ir_set.expr.func.paramkinds):
            # Call to an aggregate function.
            rvars = process_set_as_agg_expr(ir_set, stmt, ctx=ctx)
        else:
            # Regular function call.
            rvars = process_set_as_func_expr(ir_set, stmt, ctx=ctx)

    elif isinstance(ir_set.expr, irast.ExistPred):
        # EXISTS(), which is a special kind of an aggregate.
        rvars = process_set_as_exists_expr(ir_set, stmt, ctx=ctx)

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

    if output.in_serialization_ctx(ctx):
        val = pathctx.maybe_get_path_serialized_var(
            result, ir_set.path_id, env=ctx.env)

        if val is None:
            val = pathctx.get_path_value_var(
                result, ir_set.path_id, env=ctx.env)
            val = output.serialize_expr(val, env=ctx.env)
            pathctx.put_path_serialized_var(
                result, ir_set.path_id, val, force=True, env=ctx.env)
    else:
        val = pathctx.get_path_value_var(result, ir_set.path_id, env=ctx.env)

    result.target_list = [
        pgast.ResTarget(
            val=pgast.FuncCall(
                name=('array_agg',),
                args=[val],
            )
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
                                       scls=ir_set.scls),
                        ctx=scopectx)

                    relctx.include_rvar(
                        emptyrel, emptyrvar, path_id=ir_set.path_id,
                        ctx=scopectx)

                marker = unionctx.env.aliases.get('m')

                scope_rel.target_list.insert(
                    0,
                    pgast.ResTarget(val=pgast.Constant(val=1),
                                    name=marker))
                emptyrel.target_list.insert(
                    0,
                    pgast.ResTarget(val=pgast.Constant(val=2),
                                    name=marker))

                unionqry = unionctx.rel
                unionqry.op = pgast.UNION
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
                op=ast.ops.EQ,
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
            stmt.where_clause, dbobj.get_column(wrapper_rvar, optrel.marker))

        stmt.nullable = True

    sub_rvar = SetRVar(rvar=relctx.new_rel_rvar(ir_set, stmt, ctx=ctx),
                       path_id=ir_set.path_id,
                       aspects=rvars.main.aspects)

    return SetRVars(main=sub_rvar, new=[sub_rvar])


def get_set_rel_alias(ir_set: irast.Set) -> str:
    if ir_set.rptr is not None and ir_set.rptr.source.scls is not None:
        alias_hint = '{}_{}'.format(
            ir_set.rptr.source.scls.name.name,
            ir_set.rptr.ptrcls.shortname.name
        )
    else:
        if isinstance(ir_set.scls, s_types.Collection):
            alias_hint = ir_set.scls.schema_name
        else:
            alias_hint = ir_set.path_id[-1].name.name.replace('~', '-')

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

    lprop = ir_set.rptr.ptrcls
    ptr_info = pg_types.get_pointer_storage_info(
        lprop, resolve_type=False, link_bias=False)

    if ptr_info.table_type == 'ObjectType' or lprop.shortname == 'std::target':
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
            source_scope_stmt, link_path_id, aspect='value', env=ctx.env)

        if link_rvar is None:
            link_rvar = relctx.new_pointer_rvar(
                ir_source.rptr, src_rvar=src_rvar, link_bias=True, ctx=newctx)

        rvars.append(SetRVar(link_rvar, link_path_id))

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
    ptrcls = rptr.ptrcls
    ir_source = rptr.source

    rvars = []

    # Path is a __type__ reference of a homogeneous set,
    # e.g {1, 2}.__type__.
    is_static_clsref = (isinstance(ir_source.scls, s_scalars.ScalarType) and
                        ptrcls.shortname == 'std::__type__')
    if is_static_clsref:
        rvar = relctx.new_static_class_rvar(ir_set, ctx=ctx)
        return new_simple_set_rvar(ir_set, rvar, ['value', 'source'])

    if ir_set.path_id.is_type_indirection_path():
        get_set_rvar(ir_source, ctx=ctx)
        poly_rvar = relctx.new_poly_rvar(ir_set, nullable=True, ctx=ctx)
        relctx.include_rvar(stmt, poly_rvar, ir_set.path_id, ctx=ctx)

        sub_rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
        return new_simple_set_rvar(ir_set, sub_rvar, ['value', 'source'])

    ptr_info = pg_types.get_pointer_storage_info(
        ptrcls, resolve_type=False, link_bias=False)

    # Path is a link property.
    is_linkprop = ptrcls.is_link_property()
    # Path is a reference to a relationship stored in the source table.
    is_inline_ref = ptr_info.table_type == 'ObjectType'
    is_scalar_ref = not isinstance(ptrcls.target, s_objtypes.ObjectType)
    is_inline_scalar_ref = is_inline_ref and is_scalar_ref
    source_is_visible = ctx.scope_tree.is_visible(ir_source.path_id)
    semi_join = (
        not source_is_visible and
        ir_source.path_id not in ctx.disable_semi_join and
        not (is_linkprop or is_scalar_ref)
    )

    if semi_join:
        with ctx.subrel() as srcctx:
            srcctx.expr_exposed = False
            src_rvar = get_set_rvar(ir_source, ctx=srcctx)
            set_rvar = relctx.semi_join(stmt, ir_set, src_rvar, ctx=srcctx)
            rvars.append(SetRVar(set_rvar, ir_set.path_id,
                                 ['value', 'source']))

    elif not source_is_visible:
        with ctx.subrel() as srcctx:
            if is_linkprop:
                srcctx.disable_semi_join.add(ir_source.path_id)
                srcctx.unique_paths.add(ir_source.path_id)

            get_set_rvar(ir_source, ctx=srcctx)

            if is_inline_scalar_ref:
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

    elif is_inline_scalar_ref:
        main_rvar = SetRVar(
            relctx.ensure_source_rvar(ir_source, stmt, ctx=ctx),
            path_id=ir_set.path_id,
            aspects=['value', 'source']
        )

    elif not semi_join:
        # Link range.
        map_rvar = SetRVar(
            relctx.new_pointer_rvar(ir_set.rptr, src_rvar=src_rvar, ctx=ctx),
            path_id=ir_set.path_id.ptr_path(),
            aspects=['value', 'source']
        )

        rvars.append(map_rvar)

        # Target set range.
        if isinstance(ir_set.scls, s_objtypes.ObjectType):
            target_rvar = relctx.new_root_rvar(ir_set, ctx=ctx)
            if ir_source.path_id not in ctx.unique_paths:
                target_rvar.query.is_distinct = False

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

        main_rvar = SetRVar(
            relctx.new_rel_rvar(ir_set, stmt, ctx=ctx),
            path_id=ir_set.path_id,
            aspects=['value', 'source']
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
            # Non-scalar computable pointer.  Theck if path source is
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

    sub_rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return new_simple_set_rvar(ir_set, sub_rvar, ['value', 'source'])


def process_set_as_membership_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    # A [NOT] IN B is transformed into
    # SELECT [NOT] bool_or(val(A) = val(B)) FOR A CROSS JOIN B
    # bool_or is used instead of an IN sublink because it is necessary
    # to partition `B` properly considering the path scope.
    expr = ir_set.expr

    with ctx.new() as newctx:
        newctx.expr_exposed = False
        left_expr = dispatch.compile(expr.left, ctx=newctx)

        with newctx.subrel() as _, _.newscope() as subctx:
            right_rvar = get_set_rvar(expr.right, ctx=subctx)
            right_expr = pathctx.get_rvar_path_var(
                right_rvar, expr.right.path_id,
                aspect='value', env=subctx.env)

            if right_expr.nullable:
                op = 'IS NOT DISTINCT FROM'
            else:
                op = ast.ops.EQ

            check_expr = astutils.new_binop(left_expr, right_expr, op=op)
            check_expr = pgast.FuncCall(
                name=('bool_or',), args=[check_expr])

            if expr.op == ast.ops.NOT_IN:
                check_expr = astutils.new_unop(
                    ast.ops.NOT, check_expr)

            wrapper = subctx.rel
            pathctx.put_path_value_var(
                wrapper, ir_set.path_id, check_expr, env=ctx.env)
            pathctx.get_path_value_output(
                wrapper, ir_set.path_id, env=ctx.env)

            if expr.op == ast.ops.NOT_IN:
                with subctx.subrel() as subsubctx:
                    coalesce = pgast.CoalesceExpr(
                        args=[wrapper, pgast.Constant(val=True)])

                    wrapper = subsubctx.rel
                    pathctx.put_path_value_var(
                        wrapper, ir_set.path_id, coalesce, env=subsubctx.env)

            sub_rvar = relctx.new_rel_rvar(ir_set, wrapper, ctx=subctx)

    relctx.include_rvar(stmt, sub_rvar, path_id=ir_set.path_id, ctx=ctx)
    sub_rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return new_simple_set_rvar(ir_set, sub_rvar)


def process_set_as_setop(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    expr = ir_set.expr

    with ctx.new() as newctx:
        newctx.expr_exposed = False

        with newctx.subrel() as _, _.newscope() as scopectx:
            larg = scopectx.rel
            larg.view_path_id_map[ir_set.path_id] = expr.left.path_id
            dispatch.visit(expr.left, ctx=scopectx)

        with newctx.subrel() as _, _.newscope() as scopectx:
            rarg = scopectx.rel
            rarg.view_path_id_map[ir_set.path_id] = expr.right.path_id
            dispatch.visit(expr.right, ctx=scopectx)

    with ctx.subrel() as subctx:
        subqry = subctx.rel
        # There is only one binary set operators possible coming from IR:
        # UNION
        subqry.op = pgast.UNION
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
        subqry.view_path_id_map[ir_set.path_id] = expr.expr.path_id
        dispatch.visit(expr.expr, ctx=subctx)
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

    with ctx.new() as newctx:
        newctx.expr_exposed = False
        dispatch.visit(expr.condition, ctx=newctx)
        condref = relctx.get_path_var(
            stmt, path_id=expr.condition.path_id, aspect='value', ctx=newctx)

    with ctx.subrel() as _, _.newscope() as subctx:
        larg = subctx.rel
        larg.view_path_id_map[ir_set.path_id] = expr.if_expr.path_id
        dispatch.visit(expr.if_expr, ctx=subctx)

        larg.where_clause = astutils.extend_binop(
            larg.where_clause,
            condref
        )

    with ctx.subrel() as _, _.newscope() as subctx:
        rarg = subctx.rel
        rarg.view_path_id_map[ir_set.path_id] = expr.else_expr.path_id
        dispatch.visit(expr.else_expr, ctx=subctx)

        rarg.where_clause = astutils.extend_binop(
            rarg.where_clause,
            astutils.new_unop(ast.ops.NOT, condref)
        )

    with ctx.subrel() as subctx:
        subqry = subctx.rel
        subqry.op = pgast.UNION
        subqry.all = True
        subqry.larg = larg
        subqry.rarg = rarg

        union_rvar = dbobj.rvar_for_rel(subqry, lateral=True, env=subctx.env)
        relctx.include_rvar(stmt, union_rvar, ir_set.path_id, ctx=subctx)

    rvar = dbobj.rvar_for_rel(stmt, lateral=True, env=ctx.env)
    return new_simple_set_rvar(ir_set, rvar)


def process_set_as_coalesce(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    expr = ir_set.expr

    with ctx.new() as newctx:
        newctx.expr_exposed = False
        rcard = irinference.infer_cardinality(
            expr.right, scope_tree=ctx.scope_tree, schema=newctx.env.schema)

        if rcard == irast.Cardinality.ONE:
            # Singleton RHS, simply use scalar COALESCE.
            dispatch.visit(expr.left, ctx=newctx)
            dispatch.visit(expr.right, ctx=newctx)

            set_expr = pgast.CoalesceExpr(args=[
                pathctx.get_path_value_var(
                    stmt, expr.left.path_id, env=newctx.env),
                pathctx.get_path_value_var(
                    stmt, expr.right.path_id, env=newctx.env),
            ])

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
                        larg.view_path_id_map[ir_set.path_id] = \
                            expr.left.path_id
                        dispatch.visit(expr.left, ctx=scopectx)

                        lvar = pathctx.get_path_value_var(
                            larg, path_id=expr.left.path_id, env=scopectx.env)

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
                            expr.right.path_id
                        dispatch.visit(expr.right, ctx=scopectx)

                    marker = sub2ctx.env.aliases.get('m')

                    larg.target_list.insert(
                        0,
                        pgast.ResTarget(val=pgast.Constant(val=1),
                                        name=marker))
                    rarg.target_list.insert(
                        0,
                        pgast.ResTarget(val=pgast.Constant(val=2),
                                        name=marker))

                    unionqry = sub2ctx.rel
                    unionqry.op = pgast.UNION
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
                    op=ast.ops.EQ,
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
                stmt.where_clause, dbobj.get_column(subrvar, marker))

    rvar = dbobj.rvar_for_rel(stmt, lateral=True, env=ctx.env)
    return new_simple_set_rvar(ir_set, rvar)


def process_set_as_equivalence(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    expr = ir_set.expr

    dispatch.visit(expr.left, ctx=ctx)
    dispatch.visit(expr.right, ctx=ctx)

    if expr.op == irast.NEQUIVALENT:
        op = 'IS DISTINCT FROM'
    else:
        op = 'IS NOT DISTINCT FROM'

    set_expr = astutils.new_binop(
        lexpr=pathctx.get_path_value_var(
            stmt, expr.left.path_id, env=ctx.env),
        rexpr=pathctx.get_path_value_var(
            stmt, expr.right.path_id, env=ctx.env),
        op=op
    )

    pathctx.put_path_value_var_if_not_exists(
        stmt, ir_set.path_id, set_expr, env=ctx.env)

    rvar = dbobj.rvar_for_rel(stmt, lateral=True, env=ctx.env)
    return new_simple_set_rvar(ir_set, rvar)


def process_set_as_tuple(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    expr = ir_set.expr

    with ctx.new() as subctx:
        elements = []

        for element in expr.elements:
            path_id = irutils.tuple_indirection_path_id(
                ir_set.path_id, element.name,
                ir_set.scls.element_types[element.name]
            )
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

    return new_simple_set_rvar(ir_set, rvar, ['value', 'source'])


def process_set_as_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> SetRVars:
    set_expr = dispatch.compile(ir_set.expr, ctx=ctx)

    pathctx.put_path_value_var_if_not_exists(
        stmt, ir_set.path_id, set_expr, env=ctx.env)

    rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return new_simple_set_rvar(ir_set, rvar)


def process_set_as_func_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    with ctx.new() as newctx:
        newctx.rel = stmt
        newctx.expr_exposed = False

        expr = ir_set.expr
        funcobj = expr.func

        args = []

        for ir_arg in ir_set.expr.args:
            arg_ref = dispatch.compile(ir_arg, ctx=newctx)
            args.append(output.output_as_value(arg_ref, env=newctx.env))

        with_ordinality = False

        if funcobj.shortname == 'std::array_unpack':
            name = ('unnest',)
        elif funcobj.shortname == 'std::array_enumerate':
            name = ('unnest',)
            with_ordinality = True
        elif funcobj.from_function:
            name = (funcobj.from_function,)
        else:
            name = (
                common.edgedb_module_name_to_schema_name(
                    funcobj.shortname.module),
                common.edgedb_name_to_pg_name(
                    funcobj.shortname.name)
            )

        set_expr = pgast.FuncCall(
            name=name, args=args, with_ordinality=with_ordinality)

    if funcobj.set_returning:
        rtype = funcobj.returntype

        if isinstance(funcobj.returntype, s_types.Tuple):
            colnames = [name for name in rtype.element_types]
        else:
            colnames = [ctx.env.aliases.get('v')]

        func_rvar = pgast.RangeFunction(
            alias=pgast.Alias(
                aliasname=ctx.env.aliases.get('f'),
                colnames=colnames),
            lateral=True,
            functions=[set_expr])

        stmt.from_clause.append(func_rvar)

        if len(colnames) == 1:
            set_expr = dbobj.get_column(func_rvar, colnames[0])
        else:
            set_expr = pgast.TupleVar(
                elements=[
                    pgast.TupleElement(
                        path_id=irutils.tuple_indirection_path_id(
                            ir_set.path_id, n, rtype.element_types[n],
                        ),
                        name=n,
                        val=dbobj.get_column(func_rvar, n)
                    )
                    for n in colnames
                ],
                named=rtype.named
            )

            if funcobj.shortname == 'std::array_enumerate':
                # Patch index colref to (colref - 1) to make
                # enumerate indexes start from 0.
                set_expr.elements[1].val = pgast.Expr(
                    kind=pgast.ExprKind.OP,
                    name='-',
                    lexpr=set_expr.elements[1].val,
                    rexpr=pgast.Constant(val=1))

            for element in set_expr.elements:
                pathctx.put_path_value_var(
                    stmt, element.path_id, element.val, env=ctx.env)

    if (ctx.volatility_ref is not None and
            ctx.volatility_ref is not context.NO_VOLATILITY):
        # Apply the volatility reference.
        # See the comment in process_set_as_subquery().
        # XXX: check if the function is actually volatile.
        volatility_source = pgast.SelectStmt(
            values=[pgast.ImplicitRowExpr(args=[ctx.volatility_ref])]
        )
        volatility_rvar = dbobj.rvar_for_rel(volatility_source, env=ctx.env)
        relctx.rel_join(stmt, volatility_rvar, ctx=ctx)

    pathctx.put_path_var_if_not_exists(
        stmt, ir_set.path_id, set_expr, aspect='value', env=ctx.env)

    rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return new_simple_set_rvar(ir_set, rvar)


def process_set_as_agg_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    with ctx.newscope() as newctx:
        expr = ir_set.expr
        funcobj = expr.func
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
            # of std::any to determine serialized input safety.
            serialization_safe = (
                any(irutils.is_polymorphic_type(p)
                    for p in funcobj.paramtypes) and
                irutils.is_polymorphic_type(funcobj.returntype)
            )

            if not serialization_safe:
                argctx.expr_exposed = False

            args = []

            for i, ir_arg in enumerate(ir_set.expr.args):
                dispatch.visit(ir_arg, ctx=argctx)

                if output.in_serialization_ctx(ctx=argctx):
                    arg_ref = pathctx.get_path_serialized_or_value_var(
                        argctx.rel, ir_arg.path_id, env=argctx.env)

                    if isinstance(arg_ref, pgast.TupleVar):
                        arg_ref = output.serialize_expr(
                            arg_ref, env=argctx.env)
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

                if (not expr.agg_sort and i == 0 and
                        irutils.is_subquery_set(ir_arg)):
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

                if (isinstance(ir_arg.scls, s_scalars.ScalarType) and
                        ir_arg.scls.bases):
                    # Cast scalar refs to the base type in aggregate
                    # expressions, since PostgreSQL does not create array
                    # types for custom domains and will fail to process a
                    # query with custom domains appearing as array
                    # elements.
                    #
                    # XXX: Remove this once we switch to PostgreSQL 11,
                    #      which supports domain type arrays.
                    pgtype = pg_types.pg_type_from_object(
                        ctx.env.schema, ir_arg.scls, topbase=True)
                    pgtype = pgast.TypeName(name=pgtype)
                    arg_ref = pgast.TypeCast(arg=arg_ref, type_name=pgtype)

                args.append(arg_ref)

        if expr.agg_filter:
            agg_filter = dispatch.compile(expr.agg_filter, ctx=newctx)

        for arg in args:
            if arg.nullable:
                agg_filter = astutils.extend_binop(
                    agg_filter, pgast.NullTest(arg=arg, negated=True))

        if expr.agg_sort:
            with newctx.new() as sortctx:
                for sortexpr in expr.agg_sort:
                    _sortexpr = dispatch.compile(sortexpr.expr, ctx=sortctx)
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

    if expr.initial_value is not None:
        if newctx.expr_exposed and serialization_safe:
            # Serialization has changed the output type.
            with newctx.new() as ivctx:
                ivctx.expr_exposed = True
                iv = dispatch.compile(expr.initial_value.expr, ctx=ivctx)
                iv = output.serialize_expr_if_needed(iv, ctx=ctx)
                set_expr = output.serialize_expr_if_needed(set_expr, ctx=ctx)
        else:
            iv = dispatch.compile(expr.initial_value.expr, ctx=newctx)

        pathctx.put_path_value_var(stmt, ir_set.path_id, set_expr, env=ctx.env)
        pathctx.get_path_value_output(stmt, ir_set.path_id, env=ctx.env)

        with ctx.subrel() as subctx:
            wrapper = subctx.rel
            set_expr = pgast.CoalesceExpr(args=[stmt, iv])

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
        ir_expr = ir_set.expr.expr
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

        if ir_set.expr.negated:
            set_expr = astutils.new_unop(ast.ops.NOT, set_expr)

    pathctx.put_path_value_var(stmt, ir_set.path_id, set_expr, env=ctx.env)
    rvar = relctx.new_rel_rvar(ir_set, stmt, ctx=ctx)
    return new_simple_set_rvar(ir_set, rvar)
