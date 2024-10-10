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


"""EdgeQL access policy compilation."""


from __future__ import annotations

from typing import Optional, Tuple, List

from edb.ir import ast as irast

from edb.schema import name as s_name
from edb.schema import objtypes as s_objtypes
from edb.schema import policies as s_policies
from edb.schema import schema as s_schema
from edb.schema import types as s_types
from edb.schema import expr as s_expr

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from . import astutils
from . import context
from . import dispatch
from . import setgen


def should_ignore_rewrite(
    stype: s_types.Type,
    *,
    ctx: context.ContextLevel,
) -> bool:
    if not ctx.suppress_rewrites:
        return False

    if stype in ctx.suppress_rewrites:
        return True

    # If we are in any access policy at all, suppress all
    # policies except the stdlib ones.
    #
    # (Eventually will might do a generalization of this based on
    # RBAC ownership of schema objects.)
    schema = ctx.env.schema
    if (
        isinstance(stype, s_objtypes.ObjectType)
        and s_name.UnqualName(stype.get_name(schema).module)
            not in s_schema.STD_MODULES
    ):
        return True

    return False


def get_access_policies(
    stype: s_objtypes.ObjectType,
    *,
    ctx: context.ContextLevel,
) -> Tuple[s_policies.AccessPolicy, ...]:
    schema = ctx.env.schema
    if not ctx.env.options.apply_query_rewrites:
        return ()

    # The apply_access_policies config flag disables user-specified
    # access polices, but not stdlib ones
    if (
        not ctx.env.options.apply_user_access_policies
        and s_name.UnqualName(stype.get_name(schema).module)
            not in s_schema.STD_MODULES
    ):
        return ()

    return stype.get_access_policies(schema).objects(schema)


def has_own_policies(
    *,
    stype: s_objtypes.ObjectType,
    skip_from: Optional[s_objtypes.ObjectType]=None,
    ctx: context.ContextLevel,
) -> bool:
    # TODO: some kind of caching or precomputation

    schema = ctx.env.schema
    for pol in get_access_policies(stype, ctx=ctx):
        if not any(
            skip_from == base.get_subject(schema)
            for base in pol.get_bases(schema).objects(schema)
        ):
            return True

    return any(
        has_own_policies(stype=child, skip_from=stype, ctx=ctx)
        for child in stype.children(schema)
    )


def compile_pol(
    pol: s_policies.AccessPolicy,
    *,
    ctx: context.ContextLevel,
) -> irast.Set:
    """Compile the condition from an individual policy.

    A policy is evaluated in a context where it is allowed to access
    the *original subject type of the policy* and *all of its
    descendants*.

    Because it is based on the original source of the policy,
    we need to compile each policy separately.
    """
    schema = ctx.env.schema

    expr_field: Optional[s_expr.Expression] = pol.get_expr(schema)
    if expr_field:
        expr = expr_field.parse()
    else:
        expr = qlast.Constant.boolean(True)

    if condition := pol.get_condition(schema):
        assert isinstance(condition, s_expr.Expression)
        expr = qlast.BinOp(op='AND', left=condition.parse(), right=expr)

    # Find all descendants of the original subject of the rule
    subject = pol.get_original_subject(schema)
    descs = {subject} | {
        desc for desc in subject.descendants(schema)
        if desc.is_material_object_type(schema)
    }

    # Compile it with all of the
    with ctx.detached() as dctx:
        dctx.schema_factoring()
        dctx.partial_path_prefix = ctx.partial_path_prefix
        dctx.expr_exposed = context.Exposure.UNEXPOSED
        dctx.suppress_rewrites = frozenset(descs)

        return dispatch.compile(expr, ctx=dctx)


def get_extra_function_rewrite_filter(ctx: context.ContextLevel) -> qlast.Expr:
    # Functions need to check whether access policies are disabled,
    # which is signalled through a field in globals json object.
    # It's only populated when policies are disabled.
    #
    # We could also have done this by checking
    # cfg::Config.apply_access_policies, but that's probably slower,
    # and we have this mechanism anyway.
    json_type = qlast.TypeName(maintype=qlast.ObjectRef(
        module='__std__', name='json'))
    glob_set = setgen.get_func_global_json_arg(ctx=ctx)
    func_override = qlast.FunctionCall(
        func=('__std__', 'json_get'),
        args=[
            ctx.create_anchor(glob_set, 'a'),
            qlast.Constant.string(value="__disable_access_policies"),
        ],
        kwargs={
            'default': qlast.TypeCast(
                expr=qlast.Constant.boolean(False),
                type=json_type,
            )
        },
    )
    return qlast.TypeCast(
        expr=func_override,
        type=qlast.TypeName(maintype=qlast.ObjectRef(
            module='__std__', name='bool'))
    )


def get_rewrite_filter(
    stype: s_objtypes.ObjectType,
    *,
    mode: qltypes.AccessKind,
    ctx: context.ContextLevel,
) -> Optional[qlast.Expr]:
    schema = ctx.env.schema
    pols = get_access_policies(stype, ctx=ctx)
    if not pols:
        return None

    ctx.anchors = ctx.anchors.copy()

    allow, deny = [], []
    for pol in pols:
        if mode not in pol.get_access_kinds(schema):
            continue

        ir_set = compile_pol(pol, ctx=ctx)
        expr: qlast.Expr = ctx.create_anchor(ir_set)

        is_allow = pol.get_action(schema) == qltypes.AccessPolicyAction.Allow
        if is_allow:
            allow.append(expr)
        else:
            deny.append(expr)

    if ctx.env.options.func_params is not None:
        allow.append(get_extra_function_rewrite_filter(ctx))

    if allow:
        filter_expr = astutils.extend_binop(None, *allow, op='OR')
    else:
        filter_expr = qlast.Constant.boolean(False)

    if deny:
        deny_expr = qlast.UnaryOp(
            op='NOT',
            operand=astutils.extend_binop(None, *deny, op='OR')
        )
        filter_expr = astutils.extend_binop(filter_expr, deny_expr)

    # This is a bad hack, but add an always false condition that
    # postgres does not *know* is always false. This prevents postgres
    # from bogusly optimizing away the entire type CTE if it can prove
    # it empty (which could then result in assert_exists on links to
    # the type not always firing).
    if mode == qltypes.AccessKind.Select:
        bogus_check = qlast.BinOp(
            op='?=',
            left=qlast.Path(partial=True, steps=[qlast.Ptr(name='id')]),
            right=qlast.TypeCast(
                type=qlast.TypeName(maintype=qlast.ObjectRef(
                    module='__std__', name='uuid')),
                expr=qlast.Set(elements=[]),
            )
        )
        filter_expr = astutils.extend_binop(filter_expr, bogus_check, op='OR')

    return filter_expr


def try_type_rewrite(
    stype: s_objtypes.ObjectType,
    *,
    skip_subtypes: bool,
    ctx: context.ContextLevel,
) -> None:
    schema = ctx.env.schema
    rw_key = (stype, skip_subtypes)
    type_rewrites = ctx.env.type_rewrites

    # Make sure the base types in unions and intersections have their
    # rewrites compiled
    if stype.is_compound_type(schema):
        type_rewrites[rw_key] = None
        objs = (
            stype.get_union_of(schema).objects(schema) +
            stype.get_intersection_of(schema).objects(schema)
        )
        for obj in objs:
            srw_key = (obj, skip_subtypes)
            if srw_key not in type_rewrites:
                try_type_rewrite(
                    stype=obj, skip_subtypes=skip_subtypes, ctx=ctx)
                # Mark this as having a real rewrite if any parts do
                if type_rewrites[srw_key]:
                    type_rewrites[rw_key] = True
        return

    # What we *hope* to do, is to just directly select from the view
    # for our type and apply filters to it.
    #
    # Note that this is mostly optimizing the size/complexity of the
    # output *text*, by using views instead of expanding it out
    # manually.
    #
    # If some of our children have their own policies, though, we want
    # to instead union together all of our children.
    #
    # But if that is the case, and some of our children have
    # overlapping descendants, then we can't do that either, so we
    # need to explicitly list out *all* of the descendants.
    children_have_policies = not skip_subtypes and any(
        has_own_policies(stype=child, skip_from=stype, ctx=ctx)
        for child in stype.children(schema)
    )

    pols = get_access_policies(stype, ctx=ctx)
    if not pols and not children_have_policies:
        type_rewrites[rw_key] = None
        return

    # TODO: caching?
    children_overlap = False
    if children_have_policies:
        all_descs = [
            x
            for child in stype.children(schema)
            for x in child.descendants(schema)
        ]
        descs = set(all_descs)
        if len(descs) != len(all_descs):
            children_overlap = True

    # Put a placeholder to prevent recursion.
    type_rewrites[rw_key] = None

    sets = []
    # Generate the the filters for the base type we are actually considering.
    # If the type is abstract, though, and there are policies on the children,
    # then we skip it.
    if not (children_have_policies and stype.get_abstract(schema)):
        with ctx.detached() as subctx:
            # We skip looking at subtypes in two cases:
            # 1. When some children have policies of their own, and thus
            #    need to be handled separately
            # 2. When skip_subtypes was set, and so we must
            base_set = setgen.class_set(
                stype=stype,
                skip_subtypes=children_have_policies or skip_subtypes,
                ctx=subctx)

            if children_have_policies:
                # If children have policies, then all of the filtered sets
                # will be generated on skip_subtypes sets, so we don't have
                # any work to do.
                filtered_set = base_set
            else:
                # Otherwise, do the actual work of filtering.
                from . import clauses

                filtered_stmt = irast.SelectStmt(result=base_set)
                subctx.anchors['__subject__'] = base_set
                subctx.partial_path_prefix = base_set
                subctx.path_scope = subctx.env.path_scope.root.attach_fence()

                filtered_stmt.where = clauses.compile_where_clause(
                    get_rewrite_filter(
                        stype, mode=qltypes.AccessKind.Select, ctx=subctx),
                    ctx=subctx)

                filtered_set = setgen.scoped_set(filtered_stmt, ctx=subctx)

            sets.append(filtered_set)

    if children_have_policies and not skip_subtypes:
        # N.B: we don't filter here, we just generate references
        # they will go in their own CTEs
        children = stype.children(schema) if not children_overlap else descs
        sets += [
            # We need to wrap it in a type override so that unioning
            # them all together works...
            setgen.expression_set(
                setgen.ensure_stmt(
                    setgen.class_set(
                        stype=child, skip_subtypes=children_overlap, ctx=ctx),
                    ctx=ctx),
                type_override=stype,
                ctx=ctx,
            )
            for child in children
            if child.is_material_object_type(schema)
        ]

    # If we have multiple sets, union them together
    rewritten_set: Optional[irast.Set]
    if len(sets) > 1:
        with ctx.new() as subctx:
            subctx.expr_exposed = context.Exposure.UNEXPOSED
            subctx.anchors = subctx.anchors.copy()
            parts: List[qlast.Expr] = [subctx.create_anchor(x) for x in sets]
            rewritten_set = dispatch.compile(
                qlast.Set(elements=parts), ctx=subctx)
    elif len(sets) > 0:
        rewritten_set = sets[0]
    else:
        rewritten_set = None

    type_rewrites[rw_key] = rewritten_set


def compile_dml_write_policies(
    stype: s_objtypes.ObjectType,
    result: irast.Set,
    mode: qltypes.AccessKind, *,
    ctx: context.ContextLevel,
) -> Optional[irast.WritePolicies]:
    """Compile policy filters and wrap them into irast.WritePolicies"""
    pols = get_access_policies(stype, ctx=ctx)
    if not pols:
        return None

    with ctx.detached() as _, _.newscope(fenced=True) as subctx:
        # TODO: can we make sure to always avoid generating needless
        # select filters
        _prepare_dml_policy_context(stype, result, ctx=subctx)

        schema = subctx.env.schema
        subctx.anchors = subctx.anchors.copy()

        policies = []
        for pol in pols:
            if mode not in pol.get_access_kinds(schema):
                continue

            ir_set = compile_pol(pol, ctx=subctx)

            action = pol.get_action(schema)
            name = str(pol.get_shortname(schema))

            policies.append(
                irast.WritePolicy(
                    expr=ir_set,
                    action=action,
                    name=name,
                    error_msg=pol.get_errmessage(schema),
                )
            )

        return irast.WritePolicies(policies=policies)


def compile_dml_read_policies(
    stype: s_objtypes.ObjectType,
    result: irast.Set,
    mode: qltypes.AccessKind,
    *,
    ctx: context.ContextLevel,
) -> Optional[irast.ReadPolicyExpr]:
    """Compile a policy filter for a DML statement at a particular type"""
    if not get_access_policies(stype, ctx=ctx):
        return None

    with ctx.detached() as _, _.newscope(fenced=True) as subctx:
        # TODO: can we make sure to always avoid generating needless
        # select filters
        _prepare_dml_policy_context(stype, result, ctx=subctx)

        condition = get_rewrite_filter(stype, mode=mode, ctx=subctx)
        if not condition:
            return None

        return irast.ReadPolicyExpr(
            expr=setgen.scoped_set(
                dispatch.compile(condition, ctx=subctx), ctx=subctx
            ),
        )


def _prepare_dml_policy_context(
    stype: s_objtypes.ObjectType,
    result: irast.Set,
    *,
    ctx: context.ContextLevel,
) -> None:
    # It doesn't matter whether we skip subtypes here, so don't skip
    # subtypes if it has already been compiled that way, otherwise do.
    skip_subtypes = (stype, False) not in ctx.env.type_rewrites
    result = setgen.class_set(
        stype, path_id=result.path_id, skip_subtypes=skip_subtypes, ctx=ctx
    )

    ctx.anchors['__subject__'] = result
    ctx.partial_path_prefix = result
    ctx.schema_factoring()
