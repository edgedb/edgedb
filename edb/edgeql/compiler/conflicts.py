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


"""Compilation of DML exclusive constraint conflict handling."""


from __future__ import annotations
from typing import Optional, Tuple, Iterable, Sequence, Dict, List, Set

from edb import errors

from edb.ir import ast as irast
from edb.ir import utils as irutils
from edb.ir import typeutils

from edb.schema import constraints as s_constr
from edb.schema import name as s_name
from edb.schema import links as s_links
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import utils as s_utils
from edb.schema import expr as s_expr

from edb.edgeql import ast as qlast
from edb.edgeql import utils as qlutils
from edb.edgeql import qltypes

from . import astutils
from . import context
from . import dispatch
from . import inference
from . import pathctx
from . import schemactx
from . import setgen
from . import typegen


def _get_needed_ptrs(
    subject_typ: s_objtypes.ObjectType,
    obj_constrs: Sequence[s_constr.Constraint],
    initial_ptrs: Iterable[str],
    ctx: context.ContextLevel,
) -> Tuple[Set[str], Dict[str, qlast.Expr]]:
    needed_ptrs = set(initial_ptrs)
    for constr in obj_constrs:
        subjexpr: Optional[s_expr.Expression] = (
            constr.get_subjectexpr(ctx.env.schema)
        )
        assert subjexpr
        needed_ptrs |= qlutils.find_subject_ptrs(subjexpr.parse())
        if except_expr := constr.get_except_expr(ctx.env.schema):
            assert isinstance(except_expr, s_expr.Expression)
            needed_ptrs |= qlutils.find_subject_ptrs(except_expr.parse())

    wl = list(needed_ptrs)
    ptr_anchors = {}
    while wl:
        p = wl.pop()
        ptr = subject_typ.getptr(ctx.env.schema, s_name.UnqualName(p))
        if expr := ptr.get_expr(ctx.env.schema):
            assert isinstance(expr.parse(), qlast.Expr)
            ptr_anchors[p] = expr.parse()
            for ref in qlutils.find_subject_ptrs(expr.parse()):
                if ref not in needed_ptrs:
                    wl.append(ref)
                    needed_ptrs.add(ref)

    return needed_ptrs, ptr_anchors


def _compile_conflict_select_for_obj_type(
    stmt: irast.MutatingStmt,
    subject_typ: s_objtypes.ObjectType,
    *,
    for_inheritance: bool,
    fake_dml_set: Optional[irast.Set],
    obj_constrs: Sequence[s_constr.Constraint],
    constrs: Dict[str, Tuple[s_pointers.Pointer, List[s_constr.Constraint]]],
    span: Optional[irast.Span],
    ctx: context.ContextLevel,
) -> tuple[Optional[qlast.Expr], bool]:
    """Synthesize a select of conflicting objects

    ... for a single object type. This gets called once for each ancestor
    type that provides constraints to the type being inserted.

    `cnstrs` contains the constraints to consider.
    """
    # Find which pointers we need to grab
    needed_ptrs, ptr_anchors = _get_needed_ptrs(
        subject_typ, obj_constrs, constrs.keys(), ctx=ctx
    )

    # Check that no pointers in constraints are rewritten
    for p in needed_ptrs:
        ptr = subject_typ.getptr(ctx.env.schema, s_name.UnqualName(p))
        rewrite_kind = (
            qltypes.RewriteKind.Insert
            if isinstance(stmt, irast.InsertStmt)
            else qltypes.RewriteKind.Update
            if isinstance(stmt, irast.UpdateStmt)
            else None
        )
        if rewrite_kind:
            rewrite = ptr.get_rewrite(ctx.env.schema, rewrite_kind)
            if rewrite:
                raise errors.UnsupportedFeatureError(
                    "INSERT UNLESS CONFLICT cannot be used on properties or "
                    "links that have a rewrite rule specified",
                    span=span,
                )

    ctx.anchors = ctx.anchors.copy()

    # If we are given a fake_dml_set to directly represent the result
    # of our DML, use that instead of populating the result.
    if fake_dml_set:
        for p in needed_ptrs | {'id'}:
            ptr = subject_typ.getptr(ctx.env.schema, s_name.UnqualName(p))
            val = setgen.extend_path(fake_dml_set, ptr, ctx=ctx)

            ptr_anchors[p] = ctx.create_anchor(val, p)

    # Find the IR corresponding to the fields we care about and
    # produce anchors for them
    ptrs_in_shape = set()
    for elem, _ in stmt.subject.shape:
        rptr = elem.expr
        name = rptr.ptrref.shortname.name
        ptrs_in_shape.add(name)
        if name in needed_ptrs and name not in ptr_anchors:
            assert rptr.expr
            # We don't properly support hoisting volatile properties out of
            # UNLESS CONFLICT, so disallow it. We *do* support handling DML
            # there, since that gets hoisted into CTEs via its own mechanism.
            # See issue #1699.
            if inference.infer_volatility(
                rptr.expr, ctx.env, exclude_dml=True
            ).is_volatile():
                if for_inheritance:
                    error = (
                        'INSERT does not support volatile properties with '
                        'exclusive constraints when another statement in '
                        'the same query modifies a related type'
                    )
                else:
                    error = (
                        'INSERT UNLESS CONFLICT ON does not support volatile '
                        'properties'
                    )
                raise errors.UnsupportedFeatureError(
                    error, span=span
                )

            # We want to use the same path_scope_id as the original
            elem_set = setgen.ensure_set(rptr.expr, ctx=ctx)
            elem_set.path_scope_id = elem.path_scope_id

            # FIXME: The wrong thing will definitely happen if there are
            # volatile entries here
            ptr_anchors[name] = ctx.create_anchor(elem_set, name)

    if for_inheritance and not ptrs_in_shape:
        return None, False

    # Fill in empty sets for pointers that are needed but not present
    present_ptrs = set(ptr_anchors)
    for p in (needed_ptrs - present_ptrs):
        ptr = subject_typ.getptr(ctx.env.schema, s_name.UnqualName(p))
        typ = ptr.get_target(ctx.env.schema)
        assert typ
        ptr_anchors[p] = qlast.TypeCast(
            expr=qlast.Set(elements=[]),
            type=typegen.type_to_ql_typeref(typ, ctx=ctx))

    if not ptr_anchors:
        raise errors.QueryError(
            'INSERT UNLESS CONFLICT property requires matching shape',
            span=span,
        )

    conds: List[qlast.Expr] = []
    for ptrname, (ptr, ptr_cnstrs) in constrs.items():
        if ptrname not in present_ptrs:
            continue
        anchor = qlutils.subject_paths_substitute(
            ptr_anchors[ptrname], ptr_anchors)
        ptr_val = qlast.Path(partial=True, steps=[
            qlast.Ptr(name=ptrname)
        ])
        ptr, ptr_cnstrs = constrs[ptrname]
        ptr_card = ptr.get_cardinality(ctx.env.schema)

        for cnstr in ptr_cnstrs:
            lhs: qlast.Expr = anchor
            rhs: qlast.Expr = ptr_val
            # If there is a subjectexpr, substitute our lhs and rhs in
            # for __subject__ in the subjectexpr and compare *that*
            if (subjectexpr := cnstr.get_subjectexpr(ctx.env.schema)):
                assert isinstance(subjectexpr, s_expr.Expression)
                assert isinstance(subjectexpr.parse(), qlast.Expr)
                lhs = qlutils.subject_substitute(subjectexpr.parse(), lhs)
                rhs = qlutils.subject_substitute(subjectexpr.parse(), rhs)

            conds.append(qlast.BinOp(
                op='=' if ptr_card.is_single() else 'IN',
                left=lhs, right=rhs,
            ))

    # If the type we are looking at is BaseObject, then this must a
    # conflict check we are synthesizing for an explicit .id. We need
    # to ignore access policies in that case, since there is no
    # trigger to back us up.
    # (We can't insert directly into the abstract BaseObject, so this
    # is a safe assumption.)
    ignore_rewrites = (
        str(subject_typ.get_name(ctx.env.schema)) == 'std::BaseObject')
    if ignore_rewrites:
        assert not obj_constrs
        assert len(constrs) == 1 and len(constrs['id'][1]) == 1
    insert_subject = ctx.create_anchor(setgen.class_set(
        subject_typ, ignore_rewrites=ignore_rewrites, ctx=ctx
    ))

    for constr in obj_constrs:
        subject_expr: Optional[s_expr.Expression] = (
            constr.get_subjectexpr(ctx.env.schema)
        )
        assert subject_expr and isinstance(subject_expr.parse(), qlast.Expr)
        lhs = qlutils.subject_paths_substitute(
            subject_expr.parse(), ptr_anchors
        )
        rhs = qlutils.subject_substitute(
            subject_expr.parse(), insert_subject
        )
        op = qlast.BinOp(op='=', left=lhs, right=rhs)

        # If there is an except expr, we need to add in those checks also
        if except_expr := constr.get_except_expr(ctx.env.schema):
            assert isinstance(except_expr, s_expr.Expression)

            e_lhs = qlutils.subject_paths_substitute(
                except_expr.parse(), ptr_anchors)
            e_rhs = qlutils.subject_substitute(
                except_expr.parse(), insert_subject)

            true_ast = qlast.Constant.boolean(True)
            on = qlast.BinOp(
                op='AND',
                left=qlast.BinOp(op='?!=', left=e_lhs, right=true_ast),
                right=qlast.BinOp(op='?!=', left=e_rhs, right=true_ast),
            )
            op = qlast.BinOp(op='AND', left=op, right=on)

        conds.append(op)

    if not conds:
        return None, False

    # We use `any` to compute the disjunction here because some might
    # be empty.
    if len(conds) == 1:
        cond = conds[0]
    else:
        cond = qlast.FunctionCall(
            func='any',
            args=[qlast.Set(elements=conds)],
        )

    # For the result filtering we need to *ignore* the same object
    if fake_dml_set:
        anchor = qlutils.subject_paths_substitute(
            ptr_anchors['id'], ptr_anchors)
        ptr_val = qlast.Path(partial=True, steps=[qlast.Ptr(name='id')])
        cond = qlast.BinOp(
            op='AND',
            left=cond,
            right=qlast.BinOp(op='!=', left=anchor, right=ptr_val),
        )

    # Produce a query that finds the conflicting objects
    select_ast = qlast.DetachedExpr(
        expr=qlast.SelectQuery(result=insert_subject, where=cond)
    )

    # If one of the pointers we care about is multi, then we have to always
    # use a conflict CTE check instead of trying to use a constraint.
    has_multi = False
    for ptrname in needed_ptrs:
        ptr = subject_typ.getptr(ctx.env.schema, s_name.UnqualName(ptrname))
        if not ptr.get_cardinality(ctx.env.schema).is_single():
            has_multi = True

    return select_ast, has_multi


def _constr_matters(
    constr: s_constr.Constraint,
    *,
    only_local: bool = False,
    ctx: context.ContextLevel,
) -> bool:
    schema = ctx.env.schema
    return (
        not constr.is_non_concrete(schema)
        and not constr.get_delegated(schema)
        and (
            # In some use sites we always process ancestor constraints
            # too, so in those cases a constraint only matters if it
            # is the "top" constraint where it actually starts
            # applying.
            not only_local
            or constr.get_owned(schema)
            or all(
                anc.get_delegated(schema) or anc.is_non_concrete(schema)
                for anc in constr.get_ancestors(schema).objects(schema)
            )
        )
    )


PointerConstraintMap = Dict[
    str,
    Tuple[s_pointers.Pointer, List[s_constr.Constraint]],
]
ConstraintPair = Tuple[PointerConstraintMap, List[s_constr.Constraint]]
ConflictTypeMap = Dict[s_objtypes.ObjectType, ConstraintPair]


def _split_constraints(
    obj_constrs: Sequence[s_constr.Constraint],
    constrs: PointerConstraintMap,
    ctx: context.ContextLevel,
) -> ConflictTypeMap:
    schema = ctx.env.schema

    type_maps: ConflictTypeMap = {}

    # Split up pointer constraints by what object types they come from
    for name, (_, p_constrs) in constrs.items():
        for p_constr in p_constrs:
            ancs = (p_constr,) + p_constr.get_ancestors(schema).objects(schema)
            for anc in ancs:
                if not _constr_matters(anc, only_local=True, ctx=ctx):
                    continue
                p_ptr = anc.get_subject(schema)
                assert isinstance(p_ptr, s_pointers.Pointer)
                obj = p_ptr.get_source(schema)
                assert isinstance(obj, s_objtypes.ObjectType)
                map, _ = type_maps.setdefault(obj, ({}, []))
                _, entry = map.setdefault(name, (p_ptr, []))
                entry.append(anc)

    # Split up object constraints by what object types they come from
    for obj_constr in obj_constrs:
        ancs = (obj_constr,) + obj_constr.get_ancestors(schema).objects(schema)
        for anc in ancs:
            if not _constr_matters(anc, only_local=True, ctx=ctx):
                continue
            obj = anc.get_subject(schema)
            assert isinstance(obj, s_objtypes.ObjectType)
            _, o_constr_entry = type_maps.setdefault(obj, ({}, []))
            o_constr_entry.append(anc)

    return type_maps


def _compile_conflict_select(
    stmt: irast.MutatingStmt,
    subject_typ: s_objtypes.ObjectType,
    *,
    for_inheritance: bool=False,
    fake_dml_set: Optional[irast.Set]=None,
    obj_constrs: Sequence[s_constr.Constraint],
    constrs: PointerConstraintMap,
    span: Optional[irast.Span],
    ctx: context.ContextLevel,
) -> Tuple[irast.Set, bool, bool]:
    """Synthesize a select of conflicting objects

    This teases apart the constraints we care about based on which
    type they originate from, generates a SELECT for each type, and
    unions them together.

    `cnstrs` contains the constraints to consider.
    """
    schema = ctx.env.schema

    if for_inheritance:
        type_maps = {subject_typ: (constrs, list(obj_constrs))}
    else:
        type_maps = _split_constraints(obj_constrs, constrs, ctx=ctx)

    always_check = False

    # Generate a separate query for each type
    from_parent = False
    frags = []
    for a_obj, (a_constrs, a_obj_constrs) in type_maps.items():
        frag, frag_always_check = _compile_conflict_select_for_obj_type(
            stmt, a_obj, obj_constrs=a_obj_constrs, constrs=a_constrs,
            for_inheritance=for_inheritance,
            fake_dml_set=fake_dml_set,
            span=span, ctx=ctx,
        )
        always_check |= frag_always_check
        if frag:
            if a_obj != subject_typ:
                from_parent = True
            frags.append(frag)

    always_check |= from_parent or any(
        not child.is_view(schema) for child in subject_typ.children(schema)
    )

    # Union them all together
    select_ast = qlast.Set(elements=frags)
    with ctx.new() as ectx:
        ectx.allow_factoring()

        ectx.implicit_limit = 0
        ectx.allow_endpoint_linkprops = True
        select_ir = dispatch.compile(select_ast, ctx=ectx)
        select_ir = setgen.scoped_set(
            select_ir, force_reassign=True, ctx=ectx)
        assert isinstance(select_ir, irast.Set)

    # If we have an empty set, remake it with the right type
    if isinstance(select_ir.expr, irast.EmptySet):
        select_ir = setgen.new_empty_set(stype=subject_typ, ctx=ctx)

    return select_ir, always_check, from_parent


def _get_exclusive_ptr_constraints(
    typ: s_objtypes.ObjectType,
    include_id: bool,
    *, ctx: context.ContextLevel,
) -> Dict[str, Tuple[s_pointers.Pointer, List[s_constr.Constraint]]]:
    schema = ctx.env.schema
    pointers = {}

    exclusive_constr = schema.get('std::exclusive', type=s_constr.Constraint)
    for ptr in typ.get_pointers(schema).objects(schema):
        ptr = ptr.get_nearest_non_derived_parent(schema)
        ex_cnstrs = [c for c in ptr.get_constraints(schema).objects(schema)
                     if c.issubclass(schema, exclusive_constr)]
        if ex_cnstrs:
            name = ptr.get_shortname(schema).name
            if name != 'id' or include_id:
                pointers[name] = ptr, ex_cnstrs

    return pointers


def compile_insert_unless_conflict(
    stmt: irast.InsertStmt,
    typ: s_objtypes.ObjectType,
    *, ctx: context.ContextLevel,
) -> irast.OnConflictClause:
    """Compile an UNLESS CONFLICT clause with no ON

    This requires synthesizing a conditional based on all the exclusive
    constraints on the object.
    """
    has_id_write = _has_explicit_id_write(stmt)
    pointers = _get_exclusive_ptr_constraints(
        typ, include_id=has_id_write, ctx=ctx)
    obj_constrs = typ.get_constraints(ctx.env.schema).objects(ctx.env.schema)

    select_ir, always_check, _ = _compile_conflict_select(
        stmt, typ,
        constrs=pointers,
        obj_constrs=obj_constrs,
        span=stmt.span, ctx=ctx)

    return irast.OnConflictClause(
        constraint=None, select_ir=select_ir, always_check=always_check,
        else_ir=None)


def compile_insert_unless_conflict_on(
    stmt: irast.InsertStmt,
    typ: s_objtypes.ObjectType,
    constraint_spec: qlast.Expr,
    else_branch: Optional[qlast.Expr],
    *, ctx: context.ContextLevel,
) -> irast.OnConflictClause:

    with ctx.new() as constraint_ctx:
        constraint_ctx.partial_path_prefix = setgen.class_set(typ, ctx=ctx)

        # We compile the name here so we can analyze it, but we don't do
        # anything else with it.
        cspec_res = dispatch.compile(constraint_spec, ctx=constraint_ctx)

    # We accept a property, link, or a list of them in the form of a
    # tuple.
    if isinstance(cspec_res.expr, irast.Tuple):
        cspec_args = [elem.val for elem in cspec_res.expr.elements]
    else:
        cspec_args = [cspec_res]

    cspec_args = [irutils.unwrap_set(arg) for arg in cspec_args]

    for cspec_arg in cspec_args:
        if not isinstance(cspec_arg.expr, irast.Pointer):
            raise errors.QueryError(
                'UNLESS CONFLICT argument must be a property, link, '
                'or tuple of properties and links',
                span=constraint_spec.span,
            )

        if cspec_arg.expr.source.path_id != stmt.subject.path_id:
            raise errors.QueryError(
                'UNLESS CONFLICT argument must be a property of the '
                'type being inserted',
                span=constraint_spec.span,
            )

    schema = ctx.env.schema

    ptrs = []
    exclusive_constr = schema.get('std::exclusive', type=s_constr.Constraint)
    for cspec_arg in cspec_args:
        assert isinstance(cspec_arg.expr, irast.Pointer)
        schema, ptr = (
            typeutils.ptrcls_from_ptrref(cspec_arg.expr.ptrref, schema=schema))
        if not isinstance(ptr, s_pointers.Pointer):
            raise errors.QueryError(
                'UNLESS CONFLICT argument must be a property, link, '
                'or tuple of properties and links',
                span=constraint_spec.span,
            )

        ptr = ptr.get_nearest_non_derived_parent(schema)
        ptrs.append(ptr)

    obj_constrs = inference.cardinality.get_object_exclusive_constraints(
        typ, set(ptrs), ctx.env)

    field_constrs = []
    if len(ptrs) == 1:
        field_constrs = [
            c for c in ptrs[0].get_constraints(schema).objects(schema)
            if c.issubclass(schema, exclusive_constr)]

    all_constrs = list(obj_constrs) + field_constrs
    if len(all_constrs) != 1:
        raise errors.QueryError(
            'UNLESS CONFLICT property must have a single exclusive constraint',
            span=constraint_spec.span,
        )

    ds = {ptr.get_shortname(schema).name: (ptr, field_constrs)
          for ptr in ptrs}
    select_ir, always_check, from_anc = _compile_conflict_select(
        stmt, typ, constrs=ds, obj_constrs=list(obj_constrs),
        span=stmt.span, ctx=ctx)

    # Compile an else branch
    else_ir = None
    if else_branch:
        # TODO: We should support this, but there is some semantic and
        # implementation trickiness.
        if from_anc:
            raise errors.UnsupportedFeatureError(
                'UNLESS CONFLICT can not use ELSE when constraint is from a '
                'parent type',
                details=(
                    f"The existing object can't be exposed in the ELSE clause "
                    f"because it may not have type {typ.get_name(schema)}"),
                span=constraint_spec.span,
            )

        with ctx.new() as ectx:
            # The ELSE needs to be able to reference the subject in an
            # UPDATE, even though that would normally be prohibited.
            ectx.iterator_path_ids |= {stmt.subject.path_id}

            pathctx.ban_inserting_path(
                stmt.subject.path_id, location='else', ctx=ectx)

            # Compile else
            else_ir = dispatch.compile(
                astutils.ensure_ql_query(else_branch), ctx=ectx
            )
        assert isinstance(else_ir, irast.Set)

    return irast.OnConflictClause(
        constraint=irast.ConstraintRef(id=all_constrs[0].id),
        select_ir=select_ir,
        always_check=always_check,
        else_ir=else_ir
    )


def _has_explicit_id_write(stmt: irast.MutatingStmt) -> bool:
    for elem, _ in stmt.subject.shape:
        if elem.expr.ptrref.shortname.name == 'id':
            return elem.span is not None
    return False


def _disallow_exclusive_linkprops(
    stmt: irast.MutatingStmt,
    typ: s_objtypes.ObjectType,
    *, ctx: context.ContextLevel,

) -> None:
    # TODO: It should be possible to support this, but we don't deal
    # with it yet, so disallow it for safety reasons.
    schema = ctx.env.schema
    exclusive_constr = schema.get('std::exclusive', type=s_constr.Constraint)
    for ptr in typ.get_pointers(schema).objects(schema):
        if not isinstance(ptr, s_links.Link):
            continue
        ptr = ptr.get_nearest_non_derived_parent(schema)
        for lprop in ptr.get_pointers(schema).objects(schema):
            ex_cnstrs = [
                c for c in lprop.get_constraints(schema).objects(schema)
                if c.issubclass(schema, exclusive_constr)]
            if ex_cnstrs:
                raise errors.UnsupportedFeatureError(
                    'INSERT/UPDATE do not support exclusive constraints on '
                    'link properties when another statement in '
                    'the same query modifies a related type',
                    span=stmt.span,
                )


def _compile_inheritance_conflict_selects(
    stmt: irast.MutatingStmt,
    conflict: irast.MutatingStmt,
    typ: s_objtypes.ObjectType,
    subject_type: s_objtypes.ObjectType,
    *, ctx: context.ContextLevel,
) -> List[irast.OnConflictClause]:
    """Compile the selects needed to resolve multiple DML to related types

    Generate a SELECT that finds all objects of type `typ` that conflict with
    the insert `stmt`. The backend will use this to explicitly check that
    no conflicts exist, and raise an error if they do.

    This is needed because we mostly use triggers to enforce these
    cross-type exclusive constraints, and they use a snapshot
    beginning at the start of the statement.
    """
    _disallow_exclusive_linkprops(stmt, typ, ctx=ctx)
    has_id_write = _has_explicit_id_write(stmt)
    pointers = _get_exclusive_ptr_constraints(
        typ, include_id=has_id_write, ctx=ctx)
    exclusive = ctx.env.schema.get('std::exclusive', type=s_constr.Constraint)
    obj_constrs = [
        constr for constr in
        typ.get_constraints(ctx.env.schema).objects(ctx.env.schema)
        if constr.issubclass(ctx.env.schema, exclusive)
    ]

    shape_ptrs = set()
    for elem, op in stmt.subject.shape:
        if op != qlast.ShapeOp.MATERIALIZE:
            shape_ptrs.add(elem.expr.ptrref.shortname.name)

    # This is a little silly, but for *this* we need to do one per
    # constraint (so that we can properly identify which constraint
    # failed in the error messages)
    entries: List[Tuple[s_constr.Constraint, ConstraintPair]] = []
    for name, (ptr, ptr_constrs) in pointers.items():
        for ptr_constr in ptr_constrs:
            # For updates, we only need to emit the check if we actually
            # modify a pointer used by the constraint. For inserts, though
            # everything must be in play, since constraints can depend on
            # nonexistence also.
            if (
                _constr_matters(ptr_constr, ctx=ctx)
                and (
                    isinstance(stmt, irast.InsertStmt)
                    or (_get_needed_ptrs(typ, (), [name], ctx)[0] & shape_ptrs)
                )
            ):
                entries.append((ptr_constr, ({name: (ptr, [ptr_constr])}, [])))
    for obj_constr in obj_constrs:
        # See note above about needed ptrs check
        if (
            _constr_matters(obj_constr, ctx=ctx)
            and (
                isinstance(stmt, irast.InsertStmt)
                or (_get_needed_ptrs(
                    typ, [obj_constr], (), ctx)[0] & shape_ptrs)
            )
        ):
            entries.append((obj_constr, ({}, [obj_constr])))

    # For updates, we need to pull from the actual result overlay,
    # since the final row can depend on things not in the query.
    fake_dml_set = None
    if isinstance(stmt, irast.UpdateStmt):
        fake_subject = qlast.DetachedExpr(expr=qlast.Path(steps=[
            s_utils.name_to_ast_ref(subject_type.get_name(ctx.env.schema))]))

        fake_dml_set = dispatch.compile(fake_subject, ctx=ctx)

    clauses = []
    for cnstr, (p, o) in entries:
        select_ir, _, _ = _compile_conflict_select(
            stmt, typ,
            for_inheritance=True,
            fake_dml_set=fake_dml_set,
            constrs=p,
            obj_constrs=o,
            span=stmt.span, ctx=ctx)
        if isinstance(select_ir.expr, irast.EmptySet):
            continue
        cnstr_ref = irast.ConstraintRef(id=cnstr.id)
        clauses.append(
            irast.OnConflictClause(
                constraint=cnstr_ref, select_ir=select_ir, always_check=False,
                else_ir=None, else_fail=conflict,
                update_query_set=fake_dml_set)
        )
    return clauses


def compile_inheritance_conflict_checks(
    stmt: irast.MutatingStmt,
    subject_stype: s_objtypes.ObjectType,
    *, ctx: context.ContextLevel,
) -> Optional[List[irast.OnConflictClause]]:

    has_id_write = _has_explicit_id_write(stmt)

    relevant_dml = [
        dml for dml in ctx.env.dml_stmts
        if not isinstance(dml, irast.DeleteStmt)
    ]
    # Updates can conflict with themselves
    if isinstance(stmt, irast.UpdateStmt):
        relevant_dml.append(stmt)

    if not relevant_dml and not has_id_write:
        return None

    assert isinstance(subject_stype, s_objtypes.ObjectType)
    modified_ancestors = set()
    base_object = ctx.env.schema.get(
        'std::BaseObject', type=s_objtypes.ObjectType)

    subject_stype = subject_stype.get_nearest_non_derived_parent(
        ctx.env.schema)
    subject_stype = schemactx.concretify(subject_stype, ctx=ctx)
    # For updates, we need to also consider all descendants, because
    # those could also have interesting constraints of their own.
    if isinstance(stmt, irast.UpdateStmt):
        subject_stypes = list(
            schemactx.get_all_concrete(subject_stype, ctx=ctx))
    else:
        subject_stypes = [subject_stype]

    for ir in relevant_dml:
        # N.B that for updates, the update itself will be in dml_stmts,
        # since an update can conflict with itself if there are subtypes.
        # If there aren't subtypes, though, skip it.
        if ir is stmt and len(subject_stypes) == 1:
            continue

        typ = setgen.get_set_type(ir.subject, ctx=ctx)
        assert isinstance(typ, s_objtypes.ObjectType)
        typ = schemactx.concretify(typ, ctx=ctx)

        # As mentioned above, need to consider descendants of updates
        if isinstance(ir, irast.UpdateStmt):
            typs = list(schemactx.get_all_concrete(typ, ctx=ctx))
        else:
            typs = [typ]

        for typ in typs:
            for subject_stype in subject_stypes:
                # If the earlier DML has a shared ancestor that isn't
                # BaseObject and isn't the same type, then we need to
                # see if we need a conflict select.
                #
                # Note that two DMLs on the same type *can* require a
                # conflict select if at least one of them is an UPDATE
                # and there are children, but that is accounted for by
                # the above loops over all descendants when ir is an
                # UPDATE.
                if subject_stype == typ:
                    continue

                ancs = s_utils.get_class_nearest_common_ancestors(
                    ctx.env.schema, [subject_stype, typ])
                for anc in ancs:
                    if anc != base_object:
                        modified_ancestors.add((subject_stype, anc, ir))

    # If `id` is explicitly written to, synthesize a check against
    # BaseObject to ensure that it doesn't conflict with anything,
    # since we disable the trigger for id's exclusive constraint for
    # performance reasons.
    if has_id_write:
        modified_ancestors.add((subject_stype, base_object, stmt))

    conflicters = []
    for subject_stype, anc_type, ir in modified_ancestors:

        # don't enforce any constraints for abstract object type
        if subject_stype.get_abstract(schema=ctx.env.schema):
            continue

        conflicters.extend(
            _compile_inheritance_conflict_selects(
                stmt, ir, anc_type, subject_stype, ctx=ctx
            )
        )

    return conflicters or None
