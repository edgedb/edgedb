##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""Compiler functions to generate SQL relations for IR sets."""


import typing

from edgedb.lang.common import ast

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers

from edgedb.server.pgsql import ast as pgast
from edgedb.server.pgsql import common
from edgedb.server.pgsql import types as pg_types

from . import astutils
from . import context
from . import dbobj
from . import dispatch
from . import output
from . import pathctx
from . import relctx


def set_to_cte(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> pgast.BaseRelation:
    """Return a BaseRelation for a given IR Set.

    @param ir_set: IR Set node.
    """
    cte = relctx.get_set_cte(ir_set, ctx=ctx)
    if cte is not None:
        # Already have a CTE for this Set.
        return cte

    ir_set = irutils.get_canonical_set(ir_set)

    stmt = pgast.SelectStmt()

    cte_name = ctx.env.aliases.get(get_set_cte_alias(ir_set))
    cte = pgast.CommonTableExpr(query=stmt, name=cte_name)

    relctx.put_set_cte(ir_set, cte, ctx=ctx)

    with ctx.new() as subctx:
        subctx.rel = stmt
        subctx.path_scope_refs = ctx.path_scope_refs.copy()

        if relctx.get_parent_range_scope(ir_set, ctx=subctx) is not None:
            # We are ranging over this set in the parent query,
            # while evaluating a view expression.
            process_set_as_parent_scope(ir_set, stmt, ctx=subctx)

        elif irutils.is_strictly_view_set(ir_set):
            process_set_as_view(ir_set, stmt, ctx=subctx)

        elif irutils.is_inner_view_reference(ir_set):
            process_set_as_view_inner_reference(ir_set, stmt, ctx=subctx)

        elif irutils.is_subquery_set(ir_set):
            process_set_as_subquery(ir_set, stmt, ctx=subctx)

        elif irutils.is_set_membership_expr(ir_set.expr):
            # A [NOT] IN B expression.
            process_set_as_membership_expr(ir_set, stmt, ctx=subctx)

        elif isinstance(ir_set.expr, irast.SetOp):
            # Set operation: UNION
            process_set_as_setop(ir_set, stmt, ctx=subctx)

        elif isinstance(ir_set.expr, irast.TypeFilter):
            # Expr[IS Type] expressions.
            process_set_as_typefilter(ir_set, stmt, ctx=subctx)

        elif isinstance(ir_set.expr, irast.Tuple):
            # Named tuple
            process_set_as_named_tuple(ir_set, stmt, ctx=subctx)

        elif isinstance(ir_set.expr, irast.TupleIndirection):
            # Named tuple indirection.
            process_set_as_named_tuple_indirection(ir_set, stmt, ctx=subctx)

        elif isinstance(ir_set.expr, irast.FunctionCall):
            if ir_set.expr.func.aggregate:
                # Call to an aggregate function.
                process_set_as_agg_expr(ir_set, stmt, ctx=subctx)
            else:
                # Regular function call.
                process_set_as_func_expr(ir_set, stmt, ctx=subctx)

        elif isinstance(ir_set.expr, irast.ExistPred):
            # EXISTS(), which is a special kind of an aggregate.
            process_set_as_exists_expr(ir_set, stmt, ctx=subctx)

        elif ir_set.expr is not None:
            # All other expressions.
            process_set_as_expr(ir_set, stmt, ctx=subctx)

        elif ir_set.rptr is not None:
            process_set_as_path_step(ir_set, stmt, ctx=subctx)

        else:
            process_set_as_root(ir_set, stmt, ctx=subctx)

    rel = relctx.get_set_cte(ir_set, ctx=ctx)
    if irutils.is_aliased_set(ir_set):
        if isinstance(rel, pgast.CommonTableExpr):
            query = rel.query
        else:
            query = rel
        relctx.ensure_bond_for_expr(ir_set, query, ctx=ctx)

    return rel


def ensure_correct_set(
        stmt: irast.Stmt, query: pgast.Query,
        enforce_uniqueness: bool=False, *,
        path_id: irast.PathId=None,
        ctx: context.CompilerContextLevel) -> pgast.Query:
    # Make sure that the set returned by the *query* does not
    # contain NULL values.
    restype = irutils.infer_type(stmt.result, ctx.schema)
    if not isinstance(restype, (s_concepts.Concept, s_atoms.Atom,
                                s_obj.Array, s_obj.Map)):
        return query

    if not enforce_uniqueness and isinstance(restype, s_concepts.Concept):
        return query

    if path_id is None:
        path_id = stmt.result.path_id

    path_id = pathctx.reverse_map_path_id(path_id, query.view_path_id_map)

    with ctx.new() as subctx:
        # This is a simple wrapper, make sure path bond
        # conditions do not get injected unnecessarily.
        subctx.path_scope_refs = {}
        wrapper = wrap_set_rel(stmt.result, query, ctx=subctx)

        if enforce_uniqueness:
            orig_sort = list(query.sort_clause)
            for i, sortby in enumerate(query.sort_clause):
                query.target_list.append(
                    pgast.ResTarget(val=sortby.node, name=f's{i}')
                )

            ref = pathctx.get_path_identity_var(
                query, path_id, env=ctx.env)

            query.distinct_clause = [ref]
            query.sort_clause = [ref]

            wrapper.limit_offset = query.limit_offset
            wrapper.limit_count = query.limit_count
            query.limit_offset = None
            query.limit_count = None

            for i, orig_sortby in enumerate(orig_sort):
                wrapper.sort_clause.append(
                    pgast.SortBy(
                        node=dbobj.get_column(
                            wrapper.from_clause[0], f's{i}'),
                        dir=orig_sortby.dir,
                        nulls=orig_sortby.nulls
                    )
                )

    if isinstance(path_id[-1], s_concepts.Concept):
        resref = pathctx.get_path_identity_var(wrapper, path_id, env=ctx.env)
    else:
        resref = pathctx.get_path_value_var(wrapper, path_id, env=ctx.env)
    wrapper.where_clause = astutils.extend_binop(
        wrapper.where_clause, pgast.NullTest(arg=resref, negated=True))

    # Pull the CTEs up.
    wrapper.ctes = query.ctes
    query.ctes = []

    return wrapper


def wrap_set_rel(
        ir_set: irast.Set, set_rel: pgast.Query, *,
        as_value: bool=False,
        ctx: context.CompilerContextLevel) -> pgast.Query:
    # For the *set_rel* relation representing the *ir_set*
    # return the following:
    #     (
    #         SELECT <set_rel>.v
    #         FROM <set_rel>
    #     )
    #
    path_id = pathctx.reverse_map_path_id(
        ir_set.path_id, set_rel.view_path_id_map)

    with ctx.subquery() as subctx:
        wrapper = subctx.query
        rvar = dbobj.rvar_for_rel(ctx.env, set_rel)
        wrapper.from_clause = [rvar]
        relctx.pull_path_namespace(
            target=wrapper, source=rvar, ctx=subctx)

        if output.in_serialization_ctx(ctx):
            pathctx.get_path_serialized_output(
                rel=wrapper, path_id=path_id, env=ctx.env)
        else:
            pathctx.get_path_value_output(
                rel=wrapper, path_id=path_id, env=ctx.env)

        # For expressions in OFFSET/LIMIT clauses we must
        # use the _parent_'s query scope, not the scope of
        # the query where the OFFSET/LIMIT clause is.
        if ctx.clause == 'offsetlimit':
            path_scope = ctx.parent_path_scope_refs
        else:
            path_scope = ctx.path_scope_refs
        relctx.enforce_path_scope(wrapper, path_scope, ctx=subctx)

    result = wrapper

    if as_value:
        rptr = ir_set.rptr
        if not rptr.ptrcls.singular(rptr.direction):
            result = set_to_array(
                ir_set=ir_set, query=result, ctx=ctx)

    return result


def set_to_array(
        ir_set: irast.Set, query: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:
    """Convert a set query into an array."""
    if output.in_serialization_ctx(ctx):
        rt_name = pathctx.get_path_serialized_output(
            rel=query, path_id=ir_set.path_id, env=ctx.env)
    else:
        rt_name = pathctx.get_path_value_output(
            rel=query, path_id=ir_set.path_id, env=ctx.env)

    subrvar = pgast.RangeSubselect(
        subquery=query,
        alias=pgast.Alias(
            aliasname=ctx.env.aliases.get('aggw')
        )
    )

    val = dbobj.get_rvar_fieldref(subrvar, rt_name)
    val = output.serialize_expr_if_needed(ctx, val)

    result = pgast.SelectStmt(
        target_list=[
            pgast.ResTarget(
                val=pgast.FuncCall(
                    name=('array_agg',),
                    args=[val],
                )
            )
        ],
        from_clause=[
            subrvar
        ]
    )

    return result


def wrap_set_rel_as_bool_disjunction(
        ir_set: irast.Set, set_rel: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:
    # For the *set_rel* relation representing the *ir_set*
    # return the following:
    #     EXISTS (
    #         SELECT
    #         FROM <set_rel>
    #         [WHERE <set_rel>.v]
    #     )
    #
    with ctx.subquery() as subctx:
        wrapper = subctx.query
        rvar = dbobj.rvar_for_rel(ctx.env, set_rel)
        wrapper.from_clause = [rvar]
        pathctx.put_path_rvar(ctx.env, wrapper, ir_set.path_id, rvar)
        relctx.pull_path_namespace(
            target=wrapper, source=rvar, ctx=subctx)
        wrapper.where_clause = pathctx.get_rvar_path_value_var(
            rvar, ir_set.path_id, env=subctx.env)
        relctx.enforce_path_scope(wrapper, ctx.path_scope_refs, ctx=subctx)

    return pgast.SubLink(
        type=pgast.SubLinkType.EXISTS,
        expr=wrapper
    )


def get_set_cte_alias(ir_set: irast.Set) -> str:
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


def process_set_as_root(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    """Populate the CTE for a Set defined by a path root."""
    set_rvar = relctx.get_root_rvar(ir_set, stmt, ctx=ctx)
    stmt.from_clause.append(set_rvar)
    relctx.enforce_path_scope(stmt, ctx.parent_path_scope_refs, ctx=ctx)
    ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=ctx))


def process_set_as_parent_scope(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    """Populate the CTE for a Set defined by parent range."""
    parent_rvar, grouped = relctx.get_parent_range_scope(ir_set, ctx=ctx)
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

    relctx.get_root_rvar(ir_set, stmt, set_rvar=set_rvar, ctx=ctx)

    stmt.from_clause.append(set_rvar)
    relctx.pull_path_namespace(target=stmt, source=set_rvar, ctx=ctx)

    if isinstance(parent_rvar, pgast.RangeVar):
        parent_scope = {}
        for path_id in parent_rvar.path_scope:
            parent_scope[path_id] = pathctx.LazyPathVarRef(
                pathctx.get_rvar_path_identity_var,
                ctx.env, parent_rvar, path_id)
            parent_scope[path_id].grouped = grouped

        relctx.enforce_path_scope(stmt, parent_scope, ctx=ctx)

    ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=ctx))


def process_set_as_atom_class_ref(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:

    with ctx.new() as newctx:
        newctx.path_scope_refs = ctx.parent_path_scope_refs.copy()
        set_rvar = relctx.get_root_rvar(
            ir_set, stmt, nullable=ctx.lax_paths > 0, ctx=newctx)
        pathctx.join_class_rel(
            ctx.env, stmt=stmt, set_rvar=set_rvar, ir_set=ir_set)

    cte = relctx.get_set_cte(ir_set, ctx=ctx)
    ctx.query.ctes.append(cte)


def process_set_as_link_property_ref(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    ir_source = ir_set.rptr.source

    with ctx.new() as newctx:
        newctx.path_scope_refs = ctx.parent_path_scope_refs.copy()

        source_rel = set_to_cte(ir_source, ctx=newctx)
        if isinstance(source_rel, pgast.CommonTableExpr):
            source_query = source_rel.query
        else:
            source_query = source_rel

        pvar = pathctx.maybe_get_path_identity_var(
            source_query, ir_set.path_id, env=ctx.env)

        if pvar is None:
            # Reference to a link property.
            pathctx.join_mapping_rel(
                ctx.env,
                stmt=source_query, set_rvar=None,
                ir_set=ir_set, map_join_type='left')

    relctx.put_set_cte(ir_set, source_rel, ctx=ctx)


def process_set_as_path_step(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    """Populate the CTE for Set defined by a single path step."""
    rptr = ir_set.rptr
    ptrcls = rptr.ptrcls
    ir_source = rptr.source

    # Path is a reference to Atom.__class__.
    is_atom_class_ref = (
        isinstance(ir_source.scls, s_atoms.Atom) and
        ptrcls.shortname == 'std::__class__'
    )
    if is_atom_class_ref:
        return process_set_as_atom_class_ref(ir_set, stmt, ctx=ctx)

    # Path is a reference to a link property.
    if isinstance(ptrcls, s_lprops.LinkProperty):
        return process_set_as_link_property_ref(ir_set, stmt, ctx=ctx)

    ptr_info = pg_types.get_pointer_storage_info(
        ptrcls, resolve_type=False, link_bias=False)

    source_rptr = ir_source.rptr
    if (source_rptr is not None and
            isinstance(source_rptr.source.scls, s_concepts.Concept)):
        source_ptr_info = pg_types.get_pointer_storage_info(
            source_rptr.ptrcls, resolve_type=False, link_bias=False)
        source_is_inline = (
            source_ptr_info.table_type == 'concept' and
            source_rptr.direction == s_pointers.PointerDirection.Outbound)

        if source_is_inline:
            with ctx.new() as newctx:
                newctx.path_scope_refs = ctx.parent_path_scope_refs.copy()
                source_rel = set_to_cte(ir_source, ctx=newctx)

            if isinstance(source_rel, pgast.CommonTableExpr):
                source_query = source_rel.query
            else:
                source_query = source_rel

            path_id = pathctx.get_id_path_id(
                ctx.env.schema, ir_source.path_id)

            pvar = pathctx.maybe_get_path_rvar(ctx.env, source_query, path_id)

            if pvar is None:
                set_rvar = relctx.get_root_rvar(
                    ir_source, source_query,
                    nullable=ctx.lax_paths > 0, ctx=ctx)

                src_ref = pathctx.get_path_identity_var(
                    source_query, ir_source.path_id,
                    env=ctx.env)
                id_col = common.edgedb_name_to_pg_name('std::id')
                tgt_ref = dbobj.get_column(set_rvar, id_col)

                map_join_type = 'left' if ctx.lax_paths else 'inner'
                pathctx.join_inline_rel(
                    ctx.env,
                    stmt=source_query, set_rvar=set_rvar, src_ref=src_ref,
                    tgt_ref=tgt_ref, join_type=map_join_type)

                pathctx.put_path_rvar(
                    ctx.env, source_query, path_id, set_rvar)

    # Path is a reference to a relationship stored in the source table.
    is_inline_ref = ptr_info.table_type == 'concept'

    if (is_inline_ref and
            rptr.direction == s_pointers.PointerDirection.Outbound):
        with ctx.new() as newctx:
            source_rel = set_to_cte(ir_source, ctx=newctx)
            ptr_src = ptrcls.source
            path_src = ir_source.scls

            if ptr_src != path_src and not path_src.issubclass(ptr_src):
                # This is a polymorphic path element of a shape.
                src_path_id = ir_set.path_id[:-2]
                relctx.include_range(
                    stmt, source_rel, join_type='inner',
                    lateral=True, ctx=newctx)
                poly_rvar = dbobj.range_for_concept(
                    ctx.env, ptr_src, src_path_id)
                poly_rvar.path_scope.add(src_path_id)
                poly_rvar.nullable = True
                pathctx.rel_join(ctx.env, stmt, poly_rvar, type='left',
                                 allow_implicit_bond=False)
                pathctx.put_path_rvar(ctx.env, stmt, src_path_id, poly_rvar)

                cte = relctx.get_set_cte(ir_set, ctx=ctx)
                ctx.query.ctes.append(cte)
            else:
                relctx.put_set_cte(ir_set, source_rel, ctx=ctx)
        return

    with ctx.new() as newctx:
        newctx.path_scope_refs = ctx.parent_path_scope_refs.copy()

        with newctx.new() as srcctx:
            srcctx.expr_exposed = False
            source_rel = set_to_cte(ir_source, ctx=srcctx)

        relctx.include_range(
            stmt, source_rel, join_type='inner',
            lateral=True, ctx=srcctx)

        set_rvar = relctx.get_root_rvar(
            ir_set, stmt, nullable=ctx.lax_paths > 0, ctx=newctx)

        map_join_type = 'left' if ctx.lax_paths else 'inner'

        if is_inline_ref:
            # Inbound inline link.
            src_ref = pathctx.get_path_identity_var(
                stmt, ir_set.rptr.source.path_id, env=ctx.env)
            tgt_ref = dbobj.get_column(set_rvar, ptr_info.column_name)

            pathctx.join_inline_rel(
                ctx.env,
                stmt=stmt, set_rvar=set_rvar, src_ref=src_ref,
                tgt_ref=tgt_ref, join_type=map_join_type)
        else:
            pathctx.join_mapping_rel(
                ctx.env,
                stmt=stmt, set_rvar=set_rvar, ir_set=ir_set,
                map_join_type=map_join_type)

    relctx.ensure_bond_for_expr(ir_set, stmt, ctx=ctx)
    cte = relctx.get_set_cte(ir_set, ctx=ctx)
    ctx.query.ctes.append(cte)


def process_set_as_view(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    """Populate the CTE for Set defined by a subquery defining a view."""
    cte = relctx.get_set_cte(ir_set, ctx=ctx)
    parent_stmt = ctx.stmtmap.get(ir_set.expr.parent_stmt)

    with ctx.new() as newctx:
        if parent_stmt is not None:
            newctx.path_scope_refs = \
                ctx.path_scope_refs_by_stmt[parent_stmt].copy()
            newctx.ctemap = ctx.ctemap_by_stmt[parent_stmt].copy()
        else:
            newctx.path_scope_refs = {}
            newctx.ctemap = {}

        newctx.computed_node_rels = {}

        newctx.expr_exposed = False

        subquery = dispatch.compile(ir_set.expr, ctx=newctx)

        s_rvar = pgast.RangeSubselect(
            subquery=subquery,
            alias=pgast.Alias(
                aliasname=ctx.genalias(hint='vw')
            )
        )

        subquery = wrap_view_ref(
            ir_set, ir_set.real_path_id, ir_set.path_id,
            s_rvar, ctx=newctx)

        for path_id in list(subquery.path_scope):
            if not path_id.startswith(ir_set.path_id):
                subquery.path_scope.discard(path_id)

    relctx.ensure_bond_for_expr(ir_set, subquery, ctx=ctx)

    cte.query = subquery
    ctx.toplevel_stmt.ctes.append(cte)

    if ctx.stmt is not ctx.toplevel_stmt:
        with ctx.new() as c1:
            c1.stmt = c1.toplevel_stmt
            c1.ctemap = c1.ctemap_by_stmt[c1.stmt]
            relctx.put_set_cte(ir_set, cte, ctx=c1)
            ctx.viewmap[ir_set] = cte


def process_set_as_subquery(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    """Populate the CTE for Set defined by a subquery."""
    with ctx.new() as newctx:
        newctx.path_scope_refs = ctx.path_scope_refs.copy()

        inner_set = ir_set.expr.result

        outer_id = ir_set.path_id
        inner_id = inner_set.path_id

        newctx.view_path_id_map = {
            outer_id: inner_id
        }

        if ir_set.rptr and ir_set.rptr.ptrcls.is_pure_computable():
            subquery = process_computable_subquery(
                ir_set, ir_set.expr, ctx=newctx)
        else:
            subquery = dispatch.compile(ir_set.expr, ctx=newctx)

    relctx.put_set_cte(ir_set, subquery, ctx=ctx)


def process_set_as_membership_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    """Populate the CTE for Set defined by a `A [NOT] IN B` expression."""

    # A [NOT] IN B is transformed into
    # SELECT [NOT] bool_or(val(A) = val(B)) FOR A CROSS JOIN B
    # bool_or is used instead of an IN sublink because it is necessary
    # to partition `B` properly considering the path scope.
    expr = ir_set.expr

    with ctx.new() as newctx:
        newctx.expr_exposed = False

        left_expr = dispatch.compile(expr.left, ctx=newctx)

        with newctx.subquery() as rightctx:
            rightctx.unique_set_assumed = True
            right_expr = dispatch.compile(expr.right, ctx=rightctx)

            check_expr = astutils.new_binop(
                left_expr, right_expr, op=ast.ops.EQ)
            check_expr = pgast.FuncCall(
                name=('bool_or',), args=[check_expr])

            if expr.op == ast.ops.NOT_IN:
                check_expr = astutils.new_unop(
                    ast.ops.NOT, check_expr)

        subquery = rightctx.query

        path_scope = set(rightctx.rel.path_scope)
        bond_path_id = fini_agg_expr_stmt(
            ir_set, check_expr, subquery, path_scope=path_scope, ctx=rightctx)

    relctx.include_range(stmt, subquery, lateral=True, ctx=ctx)

    if bond_path_id:
        # Due to injection this rel must not be a CTE.
        relctx.put_set_cte(ir_set, stmt, ctx=ctx)
    else:
        ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=ctx))


def process_set_as_view_inner_reference(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    """Populate the CTE for Set for inner view references."""
    cte = relctx.get_set_cte(ir_set, ctx=ctx)
    inner_set = ir_set.view_source

    with ctx.new() as newctx:
        newctx.path_scope_refs = ctx.parent_path_scope_refs.copy()
        newctx.expr_exposed = False

        # rptr source is a view, so we need to make sure that all
        # references to source set in this subquery are properly
        # mapped to the view rel.
        src = ir_set.rptr.source
        # Naked source set.
        src_ir_set = irutils.get_subquery_shape(src)
        source_rvar = None

        if src.path_id in newctx.path_scope_refs:
            newctx.path_scope_refs[src_ir_set.path_id] = \
                newctx.path_scope_refs[src.path_id]
        else:
            source_cte = set_to_cte(src, ctx=newctx)
            source_rvar = dbobj.rvar_for_rel(ctx.env, source_cte)

            # Wrap the view rel for proper path_id translation.
            wrapper = wrap_view_ref(
                src_ir_set, src.path_id, src_ir_set.path_id,
                source_rvar, ctx=newctx)
            # Finally, map the source Set to the wrapper.
            relctx.put_parent_range_scope(
                src_ir_set, wrapper, ctx=newctx)

            newctx.path_scope_refs = ctx.parent_path_scope_refs.copy()

        # Prevent ensure_correct_set from wrapping the subquery as we may
        # need to fiddle with it to ensure correct cardinality first.
        newctx.correct_set_assumed = True

        newctx.view_path_id_map = {
            ir_set.path_id: inner_set.expr.result.path_id,
            src.path_id: src_ir_set.path_id
        }

        subquery = process_computable_subquery(
            ir_set, inner_set.expr, src_ir_set, source_rvar, ctx=newctx)

    # We inhibited ensure_correct_set above.  Now that we are done with
    # the query, ensure set correctness explicitly.
    enforce_uniqueness = isinstance(inner_set.scls, s_concepts.Concept)
    subquery = ensure_correct_set(inner_set.expr, subquery,
                                  enforce_uniqueness=enforce_uniqueness,
                                  path_id=ir_set.path_id,
                                  ctx=ctx)

    cte.query = subquery
    relctx.put_set_cte(ir_set, cte, ctx=ctx)
    ctx.query.ctes.append(cte)


def process_set_as_setop(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    """Populate the CTE for Set defined by set operation."""
    expr = ir_set.expr

    with ctx.new() as newctx:
        newctx.expr_exposed = False
        newctx.correct_set_assumed = True
        newctx.path_scope_refs = ctx.parent_path_scope_refs.copy()
        newctx.view_path_id_map = {
            ir_set.path_id: expr.left.result.path_id
        }
        larg = dispatch.compile(expr.left, ctx=newctx)
        newctx.path_scope_refs = ctx.parent_path_scope_refs.copy()
        newctx.view_path_id_map = {
            ir_set.path_id: expr.right.result.path_id
        }
        rarg = dispatch.compile(expr.right, ctx=newctx)

    with ctx.subquery() as subctx:
        subqry = subctx.query
        subqry.op = pgast.PgSQLSetOperator(expr.op)
        subqry.all = True
        subqry.larg = larg
        subqry.rarg = rarg

        sub_rvar = pgast.RangeSubselect(
            subquery=subqry,
            alias=pgast.Alias(
                aliasname=ctx.genalias('u')
            )
        )

    relctx.pull_path_namespace(target=stmt, source=sub_rvar, ctx=ctx)
    stmt.from_clause = [sub_rvar]

    cte = relctx.get_set_cte(ir_set, ctx=ctx)
    cte.query = stmt
    ctx.query.ctes.append(cte)


def process_set_as_named_tuple(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    """Populate the CTE for Set defined by a named tuple."""
    expr = ir_set.expr

    with ctx.new() as subctx:
        elements = []

        for element in expr.elements:
            subctx.path_scope_refs = ctx.parent_path_scope_refs.copy()
            el_ref = dispatch.compile(element.val, ctx=subctx)
            path_id = irutils.tuple_indirection_path_id(
                ir_set.path_id, element.name,
                ir_set.scls.element_types[element.name]
            )
            elements.append(astutils.TupleElement(path_id=path_id))
            pathctx.put_path_value_var(stmt, path_id, el_ref, env=ctx.env)

        set_expr = astutils.TupleVar(elements=elements, named=expr.named)

    relctx.ensure_bond_for_expr(ir_set, stmt, ctx=ctx)
    pathctx.put_path_value_var(stmt, ir_set.path_id, set_expr, env=ctx.env)
    ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=ctx))


def process_set_as_named_tuple_indirection(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    """Populate the CTE for Set defined by tuple indirection."""
    expr = ir_set.expr

    with ctx.new() as subctx:
        subctx.expr_exposed = False
        dispatch.compile(expr.expr, ctx=subctx)
        tuple_var = pathctx.get_path_value_var(stmt, ir_set.path_id,
                                               env=ctx.env)

    relctx.ensure_correct_rvar_for_expr(ir_set, stmt, tuple_var, ctx=ctx)
    ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=ctx))


def process_set_as_typefilter(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    """Populate the CTE for Set defined by a Expr[IS Type] expression."""
    root_rvar = relctx.get_root_rvar(ir_set, stmt, ctx=ctx)
    stmt.from_clause.append(root_rvar)
    pathctx.put_path_rvar(
        ctx.env, stmt, ir_set.expr.expr.path_id, root_rvar)
    dispatch.compile(ir_set.expr.expr, ctx=ctx)
    pathctx.put_path_rvar(
        ctx.env, stmt, ir_set.expr.expr.path_id, root_rvar)

    ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=ctx))


def process_set_as_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    """Populate the CTE for Set defined by an expression."""

    with ctx.new() as newctx:
        newctx.rel = stmt
        set_expr = dispatch.compile(ir_set.expr, ctx=newctx)

    relctx.ensure_correct_rvar_for_expr(ir_set, stmt, set_expr, ctx=ctx)

    if relctx.apply_path_bond_injections(stmt, ctx=ctx):
        # Due to injection this rel must not be a CTE.
        relctx.put_set_cte(ir_set, stmt, ctx=ctx)
    else:
        ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=ctx))


def process_set_as_func_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    """Populate the CTE for Set defined by a function call."""
    with ctx.new() as newctx:
        newctx.rel = stmt
        newctx.expr_as_isolated_set = False
        newctx.expr_exposed = False

        expr = ir_set.expr
        funcobj = expr.func

        args = []

        for ir_arg in ir_set.expr.args:
            arg_ref = dispatch.compile(ir_arg, ctx=newctx)
            args.append(arg_ref)

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

        if isinstance(funcobj.returntype, s_obj.Tuple):
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
            set_expr = astutils.TupleVar(
                elements=[
                    astutils.TupleElement(
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

    relctx.ensure_correct_rvar_for_expr(ir_set, stmt, set_expr, ctx=ctx)

    if relctx.apply_path_bond_injections(stmt, ctx=ctx):
        # Due to injection this rel must not be a CTE.
        relctx.put_set_cte(ir_set, stmt, ctx=ctx)
    else:
        ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=ctx))


def process_set_as_agg_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    """Populate the CTE for Set defined by an aggregate."""

    with ctx.new() as newctx:
        init_agg_expr_stmt(ir_set, ctx=newctx)

        expr = ir_set.expr
        funcobj = expr.func
        agg_filter = None
        agg_sort = []

        with newctx.new() as argctx:
            argctx.lax_paths = True
            argctx.unique_set_assumed = True

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
                arg_ref = dispatch.compile(ir_arg, ctx=argctx)
                if isinstance(arg_ref, astutils.TupleVar):
                    # tuple
                    arg_ref = output.serialize_expr_if_needed(argctx, arg_ref)

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
                        query = relctx.get_set_cte(ir_arg, ctx=argctx)
                        qrvar = argctx.subquery_map[stmt][query]

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

                if (isinstance(ir_arg.scls, s_atoms.Atom) and
                        ir_arg.scls.bases):
                    # Cast atom refs to the base type in aggregate
                    # expressions, since PostgreSQL does not create array
                    # types for custom domains and will fail to process a
                    # query with custom domains appearing as array
                    # elements.
                    pgtype = pg_types.pg_type_from_object(
                        ctx.schema, ir_arg.scls, topbase=True)
                    pgtype = pgast.TypeName(name=pgtype)
                    arg_ref = pgast.TypeCast(arg=arg_ref, type_name=pgtype)

                args.append(arg_ref)

        if expr.agg_filter:
            agg_filter = dispatch.compile(expr.agg_filter, ctx=newctx)

        for arg in args:
            if astutils.is_nullable(arg):
                agg_filter = astutils.extend_binop(
                    agg_filter, pgast.NullTest(arg=arg, negated=True))

        if expr.agg_sort:
            with newctx.new() as sortctx:
                sortctx.lax_paths = True
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
                    iv = dispatch.compile(expr.initial_value, ctx=ivctx)
                    iv = output.serialize_expr_if_needed(ctx, iv)
                    set_expr = output.serialize_expr_if_needed(ctx, set_expr)
            else:
                iv = dispatch.compile(expr.initial_value, ctx=newctx)

            set_expr = pgast.CoalesceExpr(args=[set_expr, iv])

    bond_path_id = fini_agg_expr_stmt(
        ir_set, set_expr, stmt, path_scope=set(stmt.path_scope), ctx=ctx)

    if bond_path_id:
        # Due to injection this rel must not be a CTE.
        relctx.put_set_cte(ir_set, stmt, ctx=ctx)
    else:
        ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=ctx))


def process_set_as_exists_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    """Populate the CTE for Set defined by an EXISTS() expression."""

    if isinstance(ir_set.expr.expr, irast.Stmt):
        # Statement varant.
        return process_set_as_exists_stmt_expr(ir_set, stmt, ctx=ctx)

    with ctx.new() as newctx:
        init_agg_expr_stmt(ir_set, ctx=newctx)
        newctx.lax_paths = 1

        ir_expr = ir_set.expr.expr
        set_ref = dispatch.compile(ir_expr, ctx=newctx)

        set_expr = astutils.new_binop(
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

    bond_path_id = fini_agg_expr_stmt(
        ir_set, set_expr, stmt, path_scope=set(stmt.path_scope), ctx=ctx)

    if bond_path_id:
        # Due to injection this rel must not be a CTE.
        relctx.put_set_cte(ir_set, stmt, ctx=ctx)
    else:
        ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=ctx))


def process_set_as_exists_stmt_expr(
        ir_set: irast.Set, stmt: pgast.Query, *,
        ctx: context.CompilerContextLevel) -> None:
    """Populate the CTE for Set defined by an EXISTS() expression."""
    with ctx.new() as newctx:
        init_agg_expr_stmt(ir_set, ctx=newctx)
        newctx.lax_paths = 2

        ir_expr = ir_set.expr.expr
        set_expr = dispatch.compile(ir_expr, ctx=newctx)

        for path_id in list(set_expr.path_scope):
            if not path_id.starts_any_of(ctx.path_scope):
                set_expr.path_scope.discard(path_id)

    if not set_expr.path_scope:
        set_expr = astutils.set_as_exists_op(
            set_expr, negated=ir_set.expr.negated)
    else:
        set_rvar = relctx.include_range(stmt, set_expr, ctx=ctx)
        set_ref = pathctx.get_rvar_path_identity_var(
            set_rvar, ir_expr.result.path_id, env=ctx.env)

        for path_id in stmt.path_scope:
            var = pathctx.get_path_identity_var(stmt, path_id, env=ctx.env)
            stmt.group_clause.append(var)

        set_expr = astutils.new_binop(
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

    pathctx.put_path_value_var(stmt, ir_set.path_id, set_expr, env=ctx.env)
    relctx.put_set_cte(ir_set, stmt, ctx=ctx)


def init_agg_expr_stmt(
        ir_set: irast.Set, *,
        ctx: context.CompilerContextLevel) -> None:
    ctx.expr_as_isolated_set = False
    ctx.path_scope = ctx.path_scope.copy()
    ctx.path_scope.update(ir_set.path_scope)
    ctx.local_scope_sets = ctx.local_scope_sets.copy()
    ctx.local_scope_sets.update(ir_set.local_scope_sets)


def fini_agg_expr_stmt(
        ir_set: irast.Set, set_expr: pgast.Base, stmt: pgast.Query, *,
        path_scope: typing.Set[irast.PathId],
        ctx: context.CompilerContextLevel) -> typing.Optional[irast.PathId]:
    stmt_path_scope = ctx.path_scope

    # Add an explicit GROUP BY for each non-aggregated path bond.
    for path_id in path_scope:
        if path_id in stmt_path_scope:
            path_var = pathctx.get_path_identity_var(
                stmt, path_id, env=ctx.env)
            stmt.group_clause.append(path_var)
        else:
            stmt.path_scope.discard(path_id)
            stmt.path_rvar_map.pop(path_id, None)

    bond_path_id = relctx.apply_path_bond_injections(stmt, ctx=ctx)
    if bond_path_id:
        # An explicit bond has been injected
        # (we are in the view inner reference),
        # it must be put into GROUP BY.
        path_var = pathctx.get_path_identity_var(
            stmt, bond_path_id, env=ctx.env)
        stmt.group_clause.append(path_var)

    if not stmt.group_clause and not stmt.having:
        # This is a sentinel HAVING clause so that the optimizer
        # knows how to inline the resulting query correctly.
        stmt.having = pgast.Constant(val=True)

    relctx.ensure_correct_rvar_for_expr(ir_set, stmt, set_expr, ctx=ctx)

    return bond_path_id


def wrap_view_ref(
        ir_set: irast.Set,
        inner_path_id: irast.PathId, outer_path_id: irast.PathId,
        view_rvar: pgast.BaseRangeVar, *,
        ctx: context.CompilerContextLevel) -> pgast.SelectStmt:
    wrapper = pgast.SelectStmt(
        from_clause=[view_rvar],
        view_path_id_map={
            outer_path_id: inner_path_id
        }
    )

    relctx.pull_path_namespace(target=wrapper, source=view_rvar, ctx=ctx)
    return wrapper


def process_computable_subquery(
        ir_set: irast.Set, expr: irast.Base,
        src_ir_set: typing.Optional[irast.Set] = None,
        source_rvar: typing.Optional[pgast.BaseRangeVar] = None, *,
        ctx: context.CompilerContextLevel) -> pgast.Query:
    # We need to make sure that the target expression is computed at
    # least N times, where N is the cardinality of the ``rptr.source``
    # set.  However, we cannot simply inject ``source_rvar`` here, as
    # it might have already been injected if the expression has the
    # relevant path bond.
    # To determine whether the source_rvar JOIN is necessary, do a
    # deep search for the ``src_ir_set``.
    src = ir_set.rptr.source
    if src_ir_set is None:
        src_ir_set = src

    canonical_src_ir_set = irutils.get_canonical_set(src_ir_set)
    flt = lambda n: n is canonical_src_ir_set
    expr_refers_to_target = ast.find_children(
        expr, flt, terminate_early=True)

    if not expr_refers_to_target:
        if source_rvar is None:
            with ctx.new() as subctx:
                subctx.path_scope_refs = ctx.parent_path_scope_refs.copy()
                source_cte = set_to_cte(src, ctx=subctx)
                source_rvar = dbobj.rvar_for_rel(ctx.env, source_cte)

        ctx.expr_injected_path_bond = {
            'ref': pathctx.get_rvar_path_identity_var(
                source_rvar, src.path_id, env=ctx.env),
            'path_id': src.path_id
        }

    subquery = dispatch.compile(expr, ctx=ctx)

    if not expr_refers_to_target:
        # Use a "where" join here to avoid mangling the canonical set
        # rvar in from_clause[0], as _pull_path_rvar will choke on a
        # JOIN there.
        pathctx.rel_join(ctx.env, subquery, source_rvar,
                         type='where', front=True)

    return subquery
