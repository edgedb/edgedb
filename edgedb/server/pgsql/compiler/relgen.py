##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##
"""Compiler functions to generate SQL relations for IR sets."""


from edgedb.lang.common import ast

from edgedb.lang.ir import ast as irast
from edgedb.lang.ir import utils as irutils

from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import objects as s_obj

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
        ctx: context.CompilerContext) -> pgast.BaseRelation:
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
        subctx.path_bonds = ctx.path_bonds.copy()

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

        elif isinstance(ir_set.expr, irast.SetOp):
            # Set operation: UNION/INTERSECT/EXCEPT
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

    return relctx.get_set_cte(ir_set, ctx=ctx)


def ensure_correct_set(
        stmt: irast.Stmt, query: pgast.Query,
        enforce_uniqueness: bool=False, *,
        path_id: irast.PathId=None,
        ctx: context.CompilerContext) -> pgast.Query:
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

    with ctx.new() as subctx:
        # This is a simple wrapper, make sure path bond
        # conditions do not get injected unnecessarily.
        subctx.path_bonds = {}
        wrapper = wrap_set_rel(stmt.result, query, ctx=subctx)

        if enforce_uniqueness:
            orig_sort = list(query.sort_clause)
            for i, sortby in enumerate(query.sort_clause):
                query.target_list.append(
                    pgast.ResTarget(val=sortby.node, name=f's{i}')
                )

            if isinstance(stmt.result.expr, irast.TupleIndirection):
                ref = query.target_list[0].val
            else:
                ref = pathctx.get_path_var(ctx.env, query, path_id)

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

    resref = pathctx.get_path_var(ctx.env, wrapper, path_id)
    wrapper.where_clause = astutils.extend_binop(
        wrapper.where_clause, pgast.NullTest(arg=resref, negated=True))

    # Pull the CTEs up.
    wrapper.ctes = query.ctes
    query.ctes = []

    return wrapper


def wrap_set_rel(
        ir_set: irast.Set, set_rel: pgast.Query, *,
        as_value: bool=False, ctx: context.CompilerContext) -> pgast.Query:
    # For the *set_rel* relation representing the *ir_set*
    # return the following:
    #     (
    #         SELECT <set_rel>.v
    #         FROM <set_rel>
    #     )
    #
    with ctx.subquery() as subctx:
        wrapper = subctx.query
        rvar = dbobj.rvar_for_rel(ctx.env, set_rel)
        wrapper.from_clause = [rvar]
        relctx.pull_path_namespace(
            target=wrapper, source=rvar, ctx=subctx)

        target = pathctx.get_rvar_path_var(
            ctx.env, rvar, ir_set.path_id, raw=False)

        wrapper.target_list.append(
            pgast.ResTarget(
                val=target,
                name=ctx.genalias('v')
            )
        )

        # For expressions in OFFSET/LIMIT clauses we must
        # use the _parent_'s query scope, not the scope of
        # the query where the OFFSET/LIMIT clause is.
        if ctx.clause == 'offsetlimit':
            path_scope = ctx.parent_path_bonds
        else:
            path_scope = ctx.path_bonds
        relctx.enforce_path_scope(wrapper, path_scope, ctx=subctx)

    result = wrapper

    if as_value:
        rptr = ir_set.rptr
        if not rptr.ptrcls.singular(rptr.direction):
            result = output.set_to_array(query=result, env=ctx.env)

    return result


def wrap_set_rel_as_bool_disjunction(
        ir_set: irast.Set, set_rel: pgast.Query, *,
        ctx: context.CompilerContext) -> pgast.Query:
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
        wrapper.where_clause = get_var_for_set_expr(ir_set, rvar, ctx=subctx)
        relctx.enforce_path_scope(wrapper, ctx.path_bonds, ctx=subctx)

    return pgast.SubLink(
        type=pgast.SubLinkType.EXISTS,
        expr=wrapper
    )


def get_var_for_set_expr(
        ir_set: irast.Set, source_rvar: pgast.BaseRangeVar, *,
        ctx: context.CompilerContext) -> pgast.Base:
    if ((irutils.is_subquery_set(ir_set) or
            isinstance(ir_set.expr, irast.SetOp)) and
            output.in_serialization_ctx(ctx)):
        # If the set is a subquery or a set op, and we are serializing the
        # output then we know the output has been serialized and we don't want
        # to fall into the Tuple branch below.
        return pathctx.get_rvar_path_var(
            ctx.env, source_rvar, ir_set, raw=False)

    if isinstance(ir_set.scls, s_obj.Tuple):
        if ir_set.scls.named:
            targets = []
            attmap = []

            for n in ir_set.scls.element_types:
                val = dbobj.get_column(source_rvar, n)
                attmap.append(n)
                targets.append(val)

            rtlist = astutils.ResTargetList(targets, attmap)
            return output.serialize_expr(ctx, rtlist)
        else:
            elements = []
            for n in ir_set.scls.element_types:
                val = dbobj.get_column(source_rvar, n)
                elements.append(val)

            rowexpr = pgast.ImplicitRowExpr(args=elements)
            result = output.serialize_expr(ctx, rowexpr)
            return result

    elif isinstance(ir_set.expr, irast.TupleIndirection):
        return dbobj.get_column(source_rvar, ir_set.expr.name)

    else:
        return pathctx.get_rvar_path_var(
            ctx.env, source_rvar, ir_set, raw=False)


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
        ir_set: irast.Set, stmt: pgast.Query, *, ctx: context.CompilerContext):
    """Populate the CTE for a Set defined by a path root."""
    set_rvar = relctx.get_root_rvar(ir_set, stmt, ctx=ctx)
    stmt.from_clause.append(set_rvar)
    relctx.enforce_path_scope(stmt, ctx.parent_path_bonds, ctx=ctx)
    ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=ctx))


def process_set_as_parent_scope(
        ir_set: irast.Set, stmt: pgast.Query, *, ctx: context.CompilerContext):
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
        for path_id in parent_rvar.path_bonds:
            parent_scope[path_id] = pathctx.LazyPathVarRef(
                pathctx.get_rvar_path_var, ctx.env, parent_rvar, path_id)
            parent_scope[path_id].grouped = grouped

        relctx.enforce_path_scope(stmt, parent_scope, ctx=ctx)

    ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=ctx))


def process_set_as_path_step(
        ir_set: irast.Set, stmt: pgast.Query, *, ctx: context.CompilerContext):
    """Populate the CTE for Set defined by a single path step."""
    rptr = ir_set.rptr
    ptrcls = rptr.ptrcls

    # Path is a reference to Atom.__class__.
    is_atom_class_ref = (
        isinstance(rptr.source.scls, s_atoms.Atom) and
        ptrcls.shortname == 'std::__class__'
    )

    # Path is a reference to a link property.
    is_link_prop_ref = isinstance(ptrcls, s_lprops.LinkProperty)

    if not is_atom_class_ref and not is_link_prop_ref:
        ptr_info = pg_types.get_pointer_storage_info(
            ptrcls, resolve_type=False, link_bias=False)

        # Path is a reference to a relationship represented
        # in a mapping table.
        is_mapped_target_ref = ptr_info.table_type != 'concept'

        # Path target is a Concept class.
        is_concept_ref = isinstance(ir_set.scls, s_concepts.Concept)
    else:
        is_mapped_target_ref = False
        is_concept_ref = is_atom_class_ref

    # Check if the source CTE has all the data to resolve this path.
    return_parent = not (
        is_atom_class_ref or
        is_mapped_target_ref or
        is_concept_ref
    )

    with ctx.new() as newctx:
        newctx.path_bonds = ctx.parent_path_bonds.copy()
        ir_source = ir_set.rptr.source

        if return_parent:
            source_rel = set_to_cte(ir_source, ctx=newctx)
        else:
            with newctx.new() as srcctx:
                srcctx.expr_exposed = False
                source_rel = set_to_cte(ir_source, ctx=srcctx)

            relctx.include_range(
                stmt, source_rel, join_type='inner',
                lateral=True, ctx=srcctx)

        if isinstance(source_rel, pgast.CommonTableExpr):
            source_query = source_rel.query
        else:
            source_query = source_rel

        set_rvar = relctx.get_root_rvar(
            ir_set, stmt, nullable=ctx.lax_paths > 0, ctx=newctx)

        if is_atom_class_ref:
            # Special case to support Path.atom.__class__ paths
            pathctx.join_class_rel(
                ctx.env, stmt=stmt, set_rvar=set_rvar, ir_set=ir_set)

        else:
            map_join_type = 'left' if ctx.lax_paths else 'inner'

            if is_link_prop_ref:
                # Reference to a link property.
                pathctx.join_mapping_rel(
                    ctx.env,
                    stmt=source_query, set_rvar=set_rvar,
                    ir_set=ir_set, map_join_type='left')

            elif is_mapped_target_ref:
                # Reference to an object through a link relation.
                pathctx.join_mapping_rel(
                    ctx.env,
                    stmt=stmt, set_rvar=set_rvar, ir_set=ir_set,
                    map_join_type=map_join_type)

            elif is_concept_ref:
                # Direct reference to another object.
                pathctx.join_inline_rel(
                    ctx.env,
                    stmt=stmt, set_rvar=set_rvar, ir_set=ir_set,
                    back_id_col=ptr_info.column_name,
                    join_type=map_join_type)

            else:
                # The path step target is stored in the root relation.
                # No need to do anything else here.
                pass

    if return_parent:
        relctx.put_set_cte(ir_set, source_rel, ctx=ctx)
    else:
        cte = relctx.get_set_cte(ir_set, ctx=ctx)
        ctx.query.ctes.append(cte)


def process_set_as_view(
        ir_set: irast.Set, stmt: pgast.Query, *, ctx: context.CompilerContext):
    """Populate the CTE for Set defined by a subquery defining a view."""
    cte = relctx.get_set_cte(ir_set, ctx=ctx)
    parent_stmt = ctx.stmtmap.get(ir_set.expr.parent_stmt)

    with ctx.new() as newctx:
        if parent_stmt is not None:
            newctx.path_bonds = ctx.path_bonds_by_stmt[parent_stmt].copy()
            newctx.ctemap = ctx.ctemap_by_stmt[parent_stmt].copy()
        else:
            newctx.path_bonds = {}
            newctx.ctemap = {}

        newctx.computed_node_rels = {}

        newctx.expr_exposed = False

        subquery = dispatch.compile(ir_set.expr, ctx=newctx)

        restype = irutils.infer_type(ir_set, ctx.schema)
        if not isinstance(restype, s_obj.Tuple):
            s_rvar = pgast.RangeSubselect(
                subquery=subquery,
                alias=pgast.Alias(
                    aliasname=ctx.genalias(hint='vw')
                )
            )

            subquery = wrap_view_ref(
                ir_set, ir_set.real_path_id, ir_set.path_id,
                s_rvar, ctx=newctx)

            pathctx.get_path_output(ctx.env, subquery, ir_set.path_id)

        for path_id in list(subquery.path_bonds):
            if not path_id.startswith(ir_set.path_id):
                subquery.path_bonds.discard(path_id)

    rt_name = output.ensure_query_restarget_name(
        subquery, hint=cte.name, env=ctx.env)
    pathctx.put_path_output(ctx.env, subquery, ir_set, rt_name)

    cte.query = subquery
    ctx.toplevel_stmt.ctes.append(cte)

    if ctx.stmt is not ctx.toplevel_stmt:
        with ctx.new() as c1:
            c1.stmt = c1.toplevel_stmt
            c1.ctemap = c1.ctemap_by_stmt[c1.stmt]
            relctx.put_set_cte(ir_set, cte, ctx=c1)


def process_set_as_subquery(
        ir_set: irast.Set, stmt: pgast.Query, *, ctx: context.CompilerContext):
    """Populate the CTE for Set defined by a subquery."""
    cte = relctx.get_set_cte(ir_set, ctx=ctx)

    with ctx.new() as newctx:
        newctx.path_bonds = ctx.path_bonds.copy()

        if irutils.is_strictly_view_set(ir_set.expr.result):
            outer_id = ir_set.path_id
            inner_id = ir_set.expr.result.path_id

            newctx.view_path_id_map = {
                outer_id: inner_id
            }

        subquery = dispatch.compile(ir_set.expr, ctx=newctx)

    if not isinstance(ir_set.expr, irast.MutatingStmt):
        rt_name = output.ensure_query_restarget_name(
            subquery, hint=cte.name, env=ctx.env)
        pathctx.put_path_output(ctx.env, subquery, ir_set, rt_name)

    relctx.put_set_cte(ir_set, subquery, ctx=ctx)


def process_set_as_view_inner_reference(
        ir_set: irast.Set, stmt: pgast.Query, *, ctx: context.CompilerContext):
    """Populate the CTE for Set for inner view references."""
    cte = relctx.get_set_cte(ir_set, ctx=ctx)
    inner_set = ir_set.view_source

    with ctx.new() as newctx:
        newctx.path_bonds = ctx.path_bonds.copy()
        newctx.expr_exposed = False

        # rptr source is a view, so we need to make sure that all
        # references to source set in this subquery are properly
        # mapped to the view rel.
        src = ir_set.rptr.source
        # Naked source set.
        src_ir_set = irutils.get_subquery_shape(src)
        source_rvar = None

        if (irutils.is_strictly_view_set(src) or
                (irutils.is_subquery_set(src) and
                 irutils.is_strictly_view_set(src.expr.result)) or
                (irutils.is_inner_view_reference(src))):

            if src.path_id in newctx.path_bonds:
                newctx.path_bonds[src_ir_set.path_id] = \
                    newctx.path_bonds[src.path_id]
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

                newctx.path_bonds = ctx.path_bonds.copy()

        # Prevent ensure_correct_set from wrapping the subquery as we may
        # need to fiddle with it to ensure correct cardinality first.
        newctx.correct_set_assumed = True

        newctx.view_path_id_map = {
            ir_set.path_id: inner_set.expr.result.path_id
        }

        # We need to make sure that the target expression is computed at
        # least N times, where N is the cardinality of the ``rptr.source``
        # set.  However, we cannot simply inject ``source_rvar`` here, as
        # it might have already been injected if the expression has the
        # relevant path bond.
        # To determine whether the source_rvar JOIN is necessary, do a
        # deep search for the ``target_ir_set``.
        flt = lambda n: n is src_ir_set
        expr_refers_to_target = ast.find_children(
            inner_set.expr, flt, terminate_early=True)

        if not expr_refers_to_target:
            if source_rvar is None:
                with newctx.new() as subctx:
                    subctx.path_bonds = ctx.path_bonds.copy()
                    source_cte = set_to_cte(src, ctx=subctx)
                    source_rvar = dbobj.rvar_for_rel(ctx.env, source_cte)
            newctx.expr_injected_path_bond = {
                'ref': pathctx.get_rvar_path_var(
                    ctx.env, source_rvar, src.path_id),
                'path_id': src.path_id
            }

        subquery = dispatch.compile(inner_set.expr, ctx=newctx)

        if not expr_refers_to_target:
            # Use a "where" join here to avoid mangling the canonical set
            # rvar in from_clause[0], as _pull_path_rvar will choke on a
            # JOIN there.
            pathctx.rel_join(ctx.env, subquery, source_rvar,
                             type='where', front=True)

    # We inhibited ensure_correct_set above.  Now that we are done with
    # the query, ensure set correctness explicitly.
    subquery = ensure_correct_set(inner_set.expr, subquery,
                                  enforce_uniqueness=True,
                                  path_id=ir_set.path_id,
                                  ctx=ctx)

    rt_name = output.ensure_query_restarget_name(
        subquery, hint=cte.name, env=ctx.env)
    pathctx.put_path_output(ctx.env, subquery, ir_set, rt_name)

    cte.query = subquery
    relctx.put_set_cte(ir_set, cte, ctx=ctx)
    ctx.query.ctes.append(cte)


def process_set_as_setop(
        ir_set: irast.Set, stmt: pgast.Query, *, ctx: context.CompilerContext):
    """Populate the CTE for Set defined by set operation."""
    expr = ir_set.expr

    with ctx.new() as newctx:
        newctx.unique_set_assumed = True
        newctx.path_bonds = ctx.parent_path_bonds.copy()
        newctx.view_path_id_map = {
            ir_set.path_id: expr.left.result.path_id
        }
        larg = dispatch.compile(expr.left, ctx=newctx)
        newctx.path_bonds = ctx.parent_path_bonds.copy()
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

        if (isinstance(ir_set.scls, s_obj.Tuple) and
                not output.in_serialization_ctx(ctx=ctx)):
            for n in ir_set.scls.element_types:
                stmt.target_list.append(
                    pgast.ResTarget(
                        name=common.edgedb_name_to_pg_name(n),
                        val=dbobj.get_column(sub_rvar, n)
                    )
                )
        else:
            rt_name = output.ensure_query_restarget_name(
                subqry, env=ctx.env)
            pathctx.put_path_output(ctx.env, subqry, ir_set, rt_name)
            pathctx.put_path_rvar(ctx.env, subqry, ir_set, None)

    relctx.pull_path_namespace(target=stmt, source=sub_rvar, ctx=ctx)
    stmt.from_clause = [sub_rvar]

    cte = relctx.get_set_cte(ir_set, ctx=ctx)
    cte.query = stmt
    ctx.query.ctes.append(cte)


def process_set_as_named_tuple(
        ir_set: irast.Set, stmt: pgast.Query, *, ctx: context.CompilerContext):
    """Populate the CTE for Set defined by a named tuple."""
    expr = ir_set.expr

    with ctx.new() as subctx:
        for element in expr.elements:
            subctx.path_bonds = ctx.parent_path_bonds.copy()
            el_ref = dispatch.compile(element.val, ctx=subctx)
            stmt.target_list.append(
                pgast.ResTarget(
                    name=common.edgedb_name_to_pg_name(element.name),
                    val=el_ref
                )
            )

    ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=ctx))


def process_set_as_named_tuple_indirection(
        ir_set: irast.Set, stmt: pgast.Query, *, ctx: context.CompilerContext):
    """Populate the CTE for Set defined by a named tuple indirection."""
    expr = ir_set.expr

    with ctx.new() as subctx:
        subctx.expr_exposed = False
        dispatch.compile(expr.expr, ctx=subctx)
        tuple_cte = relctx.get_set_cte(expr.expr, ctx=subctx)

    relctx.put_set_cte(ir_set, tuple_cte, ctx=ctx)


def process_set_as_typefilter(
        ir_set: irast.Set, stmt: pgast.Query, *, ctx: context.CompilerContext):
    """Populate the CTE for Set defined by a Expr[IS Type] expression."""
    root_rvar = relctx.get_root_rvar(ir_set, stmt, ctx=ctx)
    stmt.from_clause.append(root_rvar)
    pathctx.put_path_rvar(
        ctx.env, stmt, ir_set.expr.expr.path_id, root_rvar)
    dispatch.compile(ir_set.expr.expr, ctx=ctx)
    stmt.as_type = irast.PathId([ir_set.scls])

    ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=ctx))


def process_set_as_expr(
        ir_set: irast.Set, stmt: pgast.Query, *, ctx: context.CompilerContext):
    """Populate the CTE for Set defined by an expression."""

    with ctx.new() as newctx:
        newctx.rel = stmt
        set_expr = dispatch.compile(ir_set.expr, ctx=newctx)

    if isinstance(set_expr, astutils.ResTargetList):
        for i, rt in enumerate(set_expr.targets):
            stmt.target_list.append(
                pgast.ResTarget(val=rt, name=set_expr.attmap[i])
            )
    else:
        relctx.ensure_correct_rvar_for_expr(
            ir_set, stmt, set_expr, ctx=ctx)

    if relctx.apply_path_bond_injections(stmt, ctx=ctx):
        # Due to injection this rel must not be a CTE.
        relctx.put_set_cte(ir_set, stmt, ctx=ctx)
    else:
        ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=ctx))


def process_set_as_func_expr(
        ir_set: irast.Set, stmt: pgast.Query, *, ctx: context.CompilerContext):
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

        if funcobj.from_function:
            name = (funcobj.from_function,)
        else:
            name = (
                common.edgedb_module_name_to_schema_name(
                    funcobj.shortname.module),
                common.edgedb_name_to_pg_name(
                    funcobj.shortname.name)
            )

        set_expr = pgast.FuncCall(name=name, args=args)

    relctx.ensure_correct_rvar_for_expr(ir_set, stmt, set_expr, ctx=ctx)

    if relctx.apply_path_bond_injections(stmt, ctx=ctx):
        # Due to injection this rel must not be a CTE.
        relctx.put_set_cte(ir_set, stmt, ctx=ctx)
    else:
        ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=ctx))


def process_set_as_agg_expr(
        ir_set: irast.Set, stmt: pgast.Query, *, ctx: context.CompilerContext):
    """Populate the CTE for Set defined by an aggregate."""

    with ctx.new() as newctx:
        newctx.rel = stmt
        newctx.expr_as_isolated_set = False

        path_scope = set(ctx.stmt_path_scope)

        newctx.stmt_path_scope = newctx.stmt_path_scope.copy()
        newctx.stmt_path_scope.update(ir_set.path_scope)
        newctx.stmt_specific_path_scope = \
            newctx.stmt_specific_path_scope.copy()
        newctx.stmt_specific_path_scope.update(ir_set.stmt_path_scope)

        expr = ir_set.expr
        funcobj = expr.func
        agg_filter = None
        agg_sort = []

        with newctx.new() as argctx:
            argctx.lax_paths = True

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

            argctx.unique_set_assumed = True

            if not serialization_safe:
                argctx.expr_exposed = False

            args = []

            for ir_arg in ir_set.expr.args:
                arg_ref = dispatch.compile(ir_arg, ctx=argctx)

                if (isinstance(ir_arg.scls, s_atoms.Atom) and
                        ir_arg.scls.bases):
                    # Cast atom refs to the base type in aggregate
                    # expressions, since PostgreSQL does not create array
                    # types for custom domains and will fail to process a
                    # query with custom domains appearing as array
                    # elements.
                    pgtype = pg_types.pg_type_from_atom(
                        ctx.schema, ir_arg.scls, topbase=True)
                    pgtype = pgast.TypeName(name=pgtype)
                    arg_ref = pgast.TypeCast(arg=arg_ref, type_name=pgtype)

                args.append(arg_ref)

        if expr.agg_filter:
            agg_filter = dispatch.compile(expr.agg_filter, ctx=newctx)

        for arg in args:
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
                    iv = output.serialize_expr(ctx, iv)
                    set_expr = output.serialize_expr(ctx, set_expr)
            else:
                iv = dispatch.compile(expr.initial_value, ctx=newctx)

            set_expr = pgast.CoalesceExpr(args=[set_expr, iv])

        # Add an explicit GROUP BY for each non-aggregated path bond.
        for path_id in list(stmt.path_bonds):
            if path_id in path_scope:
                path_var = pathctx.get_path_var(ctx.env, stmt, path_id)
                stmt.group_clause.append(path_var)
            else:
                stmt.path_bonds.discard(path_id)
                stmt.path_rvar_map.pop(path_id, None)

    if not stmt.group_clause and not stmt.having:
        # This is a sentinel HAVING clause so that the optimizer
        # knows how to inline the resulting query correctly.
        stmt.having = pgast.Constant(val=True)

    relctx.ensure_correct_rvar_for_expr(ir_set, stmt, set_expr, ctx=ctx)

    if relctx.apply_path_bond_injections(stmt, ctx=ctx):
        # Due to injection this rel must not be a CTE.
        relctx.put_set_cte(ir_set, stmt, ctx=ctx)
    else:
        ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=ctx))


def process_set_as_exists_expr(
        ir_set: irast.Set, stmt: pgast.Query, *, ctx: context.CompilerContext):
    """Populate the CTE for Set defined by an EXISTS() expression."""

    if isinstance(ir_set.expr.expr, irast.Stmt):
        # Statement varant.
        return process_set_as_exists_stmt_expr(ir_set, stmt, ctx=ctx)

    with ctx.new() as newctx:
        newctx.lax_paths = 1
        newctx.rel = stmt

        path_scope = set(ctx.stmt_path_scope)

        newctx.stmt_path_scope = newctx.stmt_path_scope.copy()
        newctx.stmt_path_scope.update(ir_set.path_scope)
        newctx.stmt_specific_path_scope = \
            newctx.stmt_specific_path_scope.copy()
        newctx.stmt_specific_path_scope.update(ir_set.stmt_path_scope)

        ir_expr = ir_set.expr.expr
        set_ref = dispatch.compile(ir_expr, ctx=newctx)

        for path_id in list(stmt.path_bonds):
            if not path_id.starts_any_of(path_scope):
                stmt.path_bonds.discard(path_id)
            else:
                var = pathctx.get_path_var(ctx.env, stmt, path_id)
                stmt.group_clause.append(var)

        if not stmt.group_clause and not stmt.having:
            # This is a sentinel HAVING clause so that the optimizer
            # knows how to inline the resulting query correctly.
            stmt.having = pgast.Constant(val=True)

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

        restarget = pgast.ResTarget(val=set_expr, name='v')
        stmt.target_list.append(restarget)
        pathctx.put_path_output(ctx.env, stmt, ir_set, restarget.name)
        ctx.query.ctes.append(relctx.get_set_cte(ir_set, ctx=newctx))


def process_set_as_exists_stmt_expr(
        ir_set: irast.Set, stmt: pgast.Query, *, ctx: context.CompilerContext):
    """Populate the CTE for Set defined by an EXISTS() expression."""
    with ctx.new() as newctx:
        newctx.lax_paths = 2
        newctx.rel = stmt

        path_scope = set(ctx.stmt_path_scope)

        newctx.stmt_path_scope = newctx.stmt_path_scope.copy()
        newctx.stmt_path_scope.update(ir_set.path_scope)
        newctx.stmt_specific_path_scope = \
            newctx.stmt_specific_path_scope.copy()
        newctx.stmt_specific_path_scope.update(ir_set.stmt_path_scope)

        ir_expr = ir_set.expr.expr
        set_expr = dispatch.compile(ir_expr, ctx=newctx)

        for path_id in list(set_expr.path_bonds):
            if not path_id.starts_any_of(path_scope):
                set_expr.path_bonds.discard(path_id)

    if not set_expr.path_bonds:
        set_expr = astutils.set_as_exists_op(
            set_expr, negated=ir_set.expr.negated)
    else:
        set_rvar = relctx.include_range(stmt, set_expr, ctx=ctx)
        set_ref = pathctx.get_rvar_path_var(
            ctx.env, set_rvar, ir_expr.result.path_id)

        for path_id in stmt.path_bonds:
            var = pathctx.get_path_var(ctx.env, stmt, path_id)
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

    restarget = pgast.ResTarget(val=set_expr, name='v')
    stmt.target_list.append(restarget)
    pathctx.put_path_output(ctx.env, stmt, ir_set, restarget.name)
    relctx.put_set_cte(ir_set, stmt, ctx=ctx)


def wrap_view_ref(
        ir_set: irast.Set,
        inner_path_id: irast.PathId, outer_path_id: irast.PathId,
        view_rvar: pgast.BaseRangeVar, *,
        ctx: context.CompilerContext) -> pgast.SelectStmt:
    wrapper = pgast.SelectStmt(
        from_clause=[view_rvar],
        view_path_id_map={
            outer_path_id: inner_path_id
        }
    )

    if isinstance(view_rvar, pgast.RangeSubselect):
        query = view_rvar.subquery
        orig_sort = list(query.sort_clause)
        for i, sortby in enumerate(query.sort_clause):
            query.target_list.append(
                pgast.ResTarget(val=sortby.node, name=f's{i}')
            )

        if isinstance(ir_set.expr, irast.TupleIndirection):
            ref = query.target_list[0].val
        else:
            ref = pathctx.get_path_var(
                ctx.env, query, inner_path_id)

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

        wrapper.ctes = view_rvar.subquery.ctes
        view_rvar.subquery.ctes = []

    relctx.pull_path_namespace(target=wrapper, source=view_rvar, ctx=ctx)
    return wrapper
