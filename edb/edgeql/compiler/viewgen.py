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


"""EdgeQL shape compilation functions."""


from __future__ import annotations

import collections
import dataclasses
import functools
from typing import (
    Callable,
    Optional,
    Tuple,
    Type,
    Union,
    AbstractSet,
    Mapping,
    Sequence,
    Dict,
    List,
    Set,
    NamedTuple,
    cast,
    TYPE_CHECKING,
)

from edb import errors
from edb.common import ast
from edb.common import parsing
from edb.common import topological
from edb.common.typeutils import downcast, not_none

from edb.ir import ast as irast
from edb.ir import typeutils
from edb.ir import utils as irutils
import edb.ir.typeutils as irtypeutils

from edb.schema import links as s_links
from edb.schema import name as sn
from edb.schema import objtypes as s_objtypes
from edb.schema import objects as s_objects
from edb.schema import pointers as s_pointers
from edb.schema import properties as s_props
from edb.schema import types as s_types
from edb.schema import expr as s_expr

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from . import astutils
from . import context
from . import dispatch
from . import eta_expand
from . import pathctx
from . import schemactx
from . import setgen
from . import typegen

if TYPE_CHECKING:
    from edb.schema import sources as s_sources


class ShapeElementDesc(NamedTuple):
    """Annotated QL shape element for processing convenience"""

    #: Shape element AST
    ql: qlast.ShapeElement
    #: Canonical Path AST for the shape element
    path_ql: qlast.Path
    #: The underlying pointer AST
    ptr_ql: qlast.Ptr
    #: The name of the pointer
    ptr_name: str
    #: Pointer source object
    source: s_sources.Source
    #: Target type intersection (if any)
    target_typexpr: Optional[qlast.TypeExpr]
    #: Whether the source is a type intersection
    is_polymorphic: bool
    #: Whether the pointer is a link property
    is_linkprop: bool


class EarlyShapePtr(NamedTuple):
    """Stage 1 shape processing result element"""
    ptrcls: s_pointers.Pointer
    target_set: Optional[irast.Set]
    shape_origin: qlast.ShapeOrigin


class ShapePtr(NamedTuple):
    """Stage 2 shape processing result element"""
    source_set: irast.Set
    ptrcls: s_pointers.Pointer
    shape_op: qlast.ShapeOp
    target_set: Optional[irast.Set]


@dataclasses.dataclass(kw_only=True, frozen=True)
class ShapeContext:
    # a helper object for passing shape compile parameters

    path_id_namespace: Optional[irast.Namespace] = None

    view_rptr: Optional[context.ViewRPtr] = None

    view_name: Optional[sn.QualName] = None

    exprtype: s_types.ExprType = s_types.ExprType.Select


def process_view(
    ir_set: irast.Set,
    *,
    stype: s_objtypes.ObjectType,
    elements: Sequence[qlast.ShapeElement],
    view_rptr: Optional[context.ViewRPtr] = None,
    view_name: Optional[sn.QualName] = None,
    exprtype: s_types.ExprType = s_types.ExprType.Select,
    ctx: context.ContextLevel,
    span: Optional[parsing.Span],
) -> Tuple[s_objtypes.ObjectType, irast.Set]:

    cache_key = (stype, exprtype, tuple(elements))
    view_scls = ctx.env.shape_type_cache.get(cache_key)
    if view_scls is not None:
        return view_scls, ir_set

    # XXX: This is an unfortunate hack to ensure that "cannot
    # reference correlated set" errors get produced correctly,
    # since there needs to be an intervening branch for a
    # factoring fence to be respected.
    hackscope = ctx.path_scope.attach_branch()
    pathctx.register_set_in_scope(ir_set, path_scope=hackscope, ctx=ctx)
    hackscope.remove()
    ctx.path_scope.attach_subtree(hackscope, ctx=ctx)

    # Make a snapshot of aliased_views that can't be mutated
    # in any parent scopes.
    ctx.aliased_views = collections.ChainMap(dict(ctx.aliased_views))

    s_ctx = ShapeContext(
        path_id_namespace=None,
        view_rptr=view_rptr,
        view_name=view_name,
        exprtype=exprtype,
    )

    view_scls, ir = _process_view(
        ir_set,
        stype=stype,
        elements=elements,
        ctx=ctx,
        s_ctx=s_ctx,
        span=span,
    )

    ctx.env.shape_type_cache[cache_key] = view_scls

    return view_scls, ir


def _process_view(
    ir_set: irast.Set,
    *,
    stype: s_objtypes.ObjectType,
    elements: Optional[Sequence[qlast.ShapeElement]],
    s_ctx: ShapeContext,
    ctx: context.ContextLevel,
    span: Optional[parsing.Span],
) -> Tuple[s_objtypes.ObjectType, irast.Set]:
    path_id = ir_set.path_id
    view_rptr = s_ctx.view_rptr

    view_name = s_ctx.view_name
    needs_real_name = view_name is None and ctx.env.options.schema_view_mode
    generated_name = None
    if needs_real_name and view_rptr is not None:
        # Make sure persistent schema expression aliases have properly formed
        # names as opposed to the usual mangled form of the ephemeral
        # aliases.  This is needed for introspection readability, as well
        # as helps in maintaining proper type names for schema
        # representations that require alphanumeric names, such as
        # GraphQL.
        #
        # We use the name of the source together with the name
        # of the inbound link to form the name, so in e.g.
        #    CREATE ALIAS V := (SELECT Foo { bar: { baz: { ... } })
        # The name of the innermost alias would be "__V__bar__baz".
        source_name = view_rptr.source.get_name(ctx.env.schema).name
        if not source_name.startswith('__'):
            source_name = f'__{source_name}'
        if view_rptr.ptrcls_name is not None:
            ptr_name = view_rptr.ptrcls_name.name
        elif view_rptr.ptrcls is not None:
            ptr_name = view_rptr.ptrcls.get_shortname(ctx.env.schema).name
        else:
            raise errors.InternalServerError(
                '_process_view in schema mode received view_rptr with '
                'neither ptrcls_name, not ptrcls'
            )

        generated_name = f'{source_name}__{ptr_name}'
    elif needs_real_name and ctx.env.alias_result_view_name:
        # If this is a persistent schema expression but we aren't just
        # obviously sitting on an rptr (e.g CREATE ALIAS V := (Foo { x }, 10)),
        # we create a name like __V__Foo__2.
        source_name = ctx.env.alias_result_view_name.name
        type_name = stype.get_name(ctx.env.schema).name
        generated_name = f'__{source_name}__{type_name}'

    if generated_name:
        # If there are multiple, we want to stick a number on, but we'd
        # like to skip the number if there aren't.
        name = ctx.aliases.get(
            generated_name).replace('~1', '').replace('~', '__')
        view_name = sn.QualName(
            module=ctx.derived_target_module or '__derived__',
            name=name,
        )

    view_scls = schemactx.derive_view(
        stype,
        exprtype=s_ctx.exprtype,
        derived_name=view_name,
        ctx=ctx,
    )
    assert isinstance(view_scls, s_objtypes.ObjectType), view_scls
    is_mutation = s_ctx.exprtype.is_insert() or s_ctx.exprtype.is_update()
    is_defining_shape = ctx.expr_exposed or is_mutation

    ir_set = setgen.ensure_set(ir_set, type_override=view_scls, ctx=ctx)
    # Maybe rematerialize the set. The old ir_set might have already
    # been materialized, but the new version would be missing from the
    # use_sets.
    if isinstance(ir_set.expr, irast.Pointer):
        ctx.env.schema, remat_ptrcls = typeutils.ptrcls_from_ptrref(
            ir_set.expr.ptrref, schema=ctx.env.schema
        )
        setgen.maybe_materialize(remat_ptrcls, ir_set, ctx=ctx)

    if view_rptr is not None and view_rptr.ptrcls is None:
        target_scls = stype if is_mutation else view_scls
        derive_ptrcls(view_rptr, target_scls=target_scls, ctx=ctx)

    pointers: Dict[s_pointers.Pointer, EarlyShapePtr] = {}

    if elements is None:
        elements = []

    shape_desc: List[ShapeElementDesc] = []
    # First, find all explicit pointers (i.e. non-splat elements)
    for shape_el in elements:
        if isinstance(shape_el.expr.steps[0], qlast.Splat):
            continue

        shape_desc.append(
            _shape_el_ql_to_shape_el_desc(
                shape_el, source=view_scls, s_ctx=s_ctx, ctx=ctx
            )
        )

    explicit_ptr_names = {
        desc.ptr_name for desc in shape_desc if not desc.is_linkprop
    }

    explicit_lprop_names = {
        desc.ptr_name for desc in shape_desc if desc.is_linkprop
    }

    # Now look for any splats and expand them.
    # Track descriptions by name and whether they are link properties.
    splat_descs: dict[Tuple[str, bool], ShapeElementDesc] = {}
    for shape_el in elements:
        if not isinstance(shape_el.expr.steps[0], qlast.Splat):
            continue

        if s_ctx.exprtype is not s_types.ExprType.Select:
            raise errors.QueryError(
                "unexpected splat operator in non-SELECT shape",
                span=shape_el.expr.span,
            )

        if ctx.env.options.func_params is not None:
            raise errors.UnsupportedFeatureError(
                "splat operators in function bodies are not supported",
                span=shape_el.expr.span,
            )

        splat = shape_el.expr.steps[0]
        if splat.type is not None:
            splat_type = typegen.ql_typeexpr_to_type(splat.type, ctx=ctx)
            if not isinstance(splat_type, s_objtypes.ObjectType):
                vn = splat_type.get_verbosename(schema=ctx.env.schema)
                raise errors.QueryError(
                    f"splat operator expects an object type, got {vn}",
                    span=splat.type.span,
                )

            if not stype.issubclass(ctx.env.schema, splat_type):
                vn = stype.get_verbosename(ctx.env.schema)
                vn2 = splat_type.get_verbosename(schema=ctx.env.schema)
                raise errors.QueryError(
                    f"splat type must be {vn} or its parent type, "
                    f"got {vn2}",
                    span=splat.type.span,
                )

            if splat.intersection is not None:
                intersector_type = typegen.ql_typeexpr_to_type(
                    splat.intersection.type, ctx=ctx)
                splat_type = schemactx.apply_intersection(
                    splat_type,
                    intersector_type,
                    ctx=ctx,
                ).stype
                assert isinstance(splat_type, s_objtypes.ObjectType)

        elif splat.intersection is not None:
            splat_type = typegen.ql_typeexpr_to_type(
                splat.intersection.type, ctx=ctx)
            if not isinstance(splat_type, s_objtypes.ObjectType):
                vn = splat_type.get_verbosename(schema=ctx.env.schema)
                raise errors.QueryError(
                    f"splat operator expects an object type, got {vn}",
                    span=splat.intersection.type.span,
                )
        else:
            splat_type = stype

        if (
            view_rptr is not None
            and isinstance(view_rptr.ptrcls, s_links.Link)
        ):
            splat_rlink = view_rptr.ptrcls
        else:
            splat_rlink = None

        expanded_splat = _expand_splat(
            splat_type,
            depth=splat.depth,
            intersection=splat.intersection,
            rlink=splat_rlink,
            skip_ptrs=explicit_ptr_names,
            skip_lprops=explicit_lprop_names,
            ctx=ctx,
        )

        for splat_el in expanded_splat:
            desc = _shape_el_ql_to_shape_el_desc(
                splat_el, source=view_scls, s_ctx=s_ctx, ctx=ctx
            )
            desc_key: Tuple[str, bool] = (desc.ptr_name, desc.is_linkprop)
            if old_desc := splat_descs.get(desc_key):
                # If pointers appear in multiple splats, we take the
                # one from the ancestor class. If neither class is an
                # ancestor, we reject it.
                # TODO: Accept it instead, if the types are the same.
                new_source: object = desc.source
                old_source: object = old_desc.source
                if isinstance(new_source, s_links.Link):
                    new_source = new_source.get_source(ctx.env.schema)
                assert isinstance(new_source, s_objtypes.ObjectType)
                if isinstance(old_source, s_links.Link):
                    old_source = old_source.get_source(ctx.env.schema)
                assert isinstance(old_source, s_objtypes.ObjectType)
                new_source = schemactx.concretify(new_source, ctx=ctx)
                old_source = schemactx.concretify(old_source, ctx=ctx)

                if new_source.issubclass(ctx.env.schema, old_source):
                    # Do nothing.
                    pass
                elif old_source.issubclass(ctx.env.schema, new_source):
                    # Take the new one
                    splat_descs[desc_key] = desc
                else:
                    vn1 = old_source.get_verbosename(schema=ctx.env.schema)
                    vn2 = new_source.get_verbosename(schema=ctx.env.schema)
                    raise errors.QueryError(
                        f"link or property '{desc.ptr_name}' appears in splats "
                        f"for unrelated types: {vn1} and {vn2}",
                        span=splat.span,
                    )

            else:
                splat_descs[desc_key] = desc

    shape_desc.extend(splat_descs.values())

    for shape_el_desc in shape_desc:
        with ctx.new() as scopectx:
            # when doing insert or update with a compexpr, generate the
            # the anchor for __default__
            if (
                (s_ctx.exprtype.is_insert() or s_ctx.exprtype.is_update())
                and shape_el_desc.ql.compexpr is not None
                and shape_el_desc.ptr_name not in (
                    ctx.special_computables_in_mutation_shape
                )
            ):
                # mutating statement, ptrcls guaranteed to exist
                ptrcls = setgen.resolve_ptr(
                    shape_el_desc.source,
                    shape_el_desc.ptr_name,
                    track_ref=shape_el_desc.ptr_ql,
                    ctx=scopectx
                )

                compexpr_uses_default = False
                compexpr_default_span: Optional[parsing.Span] = None
                for path_node in ast.find_children(
                    shape_el_desc.ql.compexpr, qlast.Path
                ):
                    for step in path_node.steps:
                        if not isinstance(step, qlast.SpecialAnchor):
                            continue
                        if step.name != '__default__':
                            continue

                        compexpr_uses_default = True
                        compexpr_default_span = step.span
                        break

                    if compexpr_uses_default:
                        break

                if compexpr_uses_default:
                    def make_error(
                        span: Optional[parsing.Span], hint: str
                    ) -> errors.InvalidReferenceError:
                        return errors.InvalidReferenceError(
                            f'__default__ cannot be used in this expression',
                            span=span,
                            hint=hint,
                        )

                    default_expr: Optional[s_expr.Expression] = (
                        ptrcls.get_default(scopectx.env.schema)
                    )
                    if default_expr is None:
                        raise make_error(
                            compexpr_default_span,
                            'No default expression exists',
                        )

                    default_ast_expr = default_expr.parse()

                    if any(
                        any(
                            (
                                isinstance(step, qlast.SpecialAnchor)
                                and step.name == '__source__'
                            )
                            for step in path_node.steps
                        )
                        for path_node in ast.find_children(
                            default_ast_expr, qlast.Path
                        )
                    ):
                        raise make_error(
                            compexpr_default_span,
                            'Default expression uses __source__',
                        )

                    if astutils.contains_dml(default_ast_expr, ctx=ctx):
                        raise make_error(
                            compexpr_default_span,
                            'Default expression uses DML',
                        )

                    default_set = dispatch.compile(
                        default_ast_expr, ctx=scopectx
                    )

                    scopectx.anchors['__default__'] = default_set

            pointer, ptr_set = _normalize_view_ptr_expr(
                ir_set,
                shape_el_desc,
                view_scls,
                path_id=path_id,
                pending_pointers=pointers,
                s_ctx=s_ctx,
                ctx=scopectx,
            )

            pointers[pointer] = EarlyShapePtr(
                pointer, ptr_set, shape_el_desc.ql.origin)

    # If we are not defining a shape (so we might care about
    # materialization), look through our parent view (if one exists)
    # for materialized properties that are not present in this shape.
    # If any are found, inject them.
    # (See test_edgeql_volatility_rebind_flat_01 for an example.)
    schema = ctx.env.schema
    base = view_scls.get_bases(schema).objects(schema)[0]
    base_ptrs = (view_scls.get_pointers(schema).objects(schema)
                 if not is_defining_shape else ())
    for ptrcls in base_ptrs:
        if ptrcls in pointers or base not in ctx.env.view_shapes:
            continue
        pptr = ptrcls.get_bases(schema).objects(schema)[0]
        if (pptr, qlast.ShapeOp.MATERIALIZE) not in ctx.env.view_shapes[base]:
            continue

        # Make up a dummy shape element
        name = ptrcls.get_shortname(schema).name
        dummy_el = qlast.ShapeElement(expr=qlast.Path(
            steps=[qlast.Ptr(name=name)]))
        dummy_el_desc = _shape_el_ql_to_shape_el_desc(
            dummy_el, source=view_scls, s_ctx=s_ctx, ctx=ctx
        )

        with ctx.new() as scopectx:
            pointer, ptr_set = _normalize_view_ptr_expr(
                ir_set,
                dummy_el_desc,
                view_scls,
                path_id=path_id,
                s_ctx=s_ctx,
                ctx=scopectx,
            )

        pointers[pointer] = EarlyShapePtr(
            pointer, ptr_set, qlast.ShapeOrigin.MATERIALIZATION)

    specified_ptrs = {
        ptrcls.get_local_name(ctx.env.schema) for ptrcls in pointers
    }

    # defaults
    if s_ctx.exprtype.is_insert():
        defaults_ptrs = _gen_pointers_from_defaults(
            specified_ptrs, view_scls, ir_set, stype, s_ctx, ctx
        )
        pointers.update(defaults_ptrs)

    # rewrites
    rewrite_kind = (
        qltypes.RewriteKind.Insert
        if s_ctx.exprtype.is_insert()
        else qltypes.RewriteKind.Update
        if s_ctx.exprtype.is_update()
        else None
    )

    if rewrite_kind:
        rewrites = _compile_rewrites(
            specified_ptrs, rewrite_kind, view_scls, ir_set, stype, s_ctx, ctx
        )
        if rewrites:
            ctx.env.dml_rewrites[ir_set] = rewrites
    else:
        rewrites = None

    if s_ctx.exprtype.is_insert():
        _raise_on_missing(pointers, stype, rewrites, ctx, span=span)

    set_shape = []
    shape_ptrs: List[ShapePtr] = []

    for ptrcls, ptr_set, _ in pointers.values():
        source: Union[s_types.Type, s_pointers.PointerLike]

        if ptrcls.is_link_property(ctx.env.schema):
            assert view_rptr is not None and view_rptr.ptrcls is not None
            source = view_rptr.ptrcls
        else:
            source = view_scls

        if is_defining_shape:
            cinfo = ctx.env.source_map.get(ptrcls)
            if cinfo is not None:
                shape_op = cinfo.shape_op
            else:
                shape_op = qlast.ShapeOp.ASSIGN
        elif ptrcls.get_computable(ctx.env.schema):
            shape_op = qlast.ShapeOp.MATERIALIZE
        else:
            continue

        ctx.env.view_shapes[source].append((ptrcls, shape_op))
        shape_ptrs.append(ShapePtr(ir_set, ptrcls, shape_op, ptr_set))

    rptrcls = view_rptr.ptrcls if view_rptr else None
    shape_ptrs = _get_early_shape_configuration(
        ir_set, shape_ptrs, rptrcls=rptrcls, ctx=ctx)

    # Produce the shape. The main thing here is that we need to fixup
    # all of the rptrs to properly point back at ir_set.
    for _, ptrcls, shape_op, ptr_set in shape_ptrs:
        ptr_span = None
        if ptrcls in ctx.env.pointer_specified_info:
            _, _, ptr_span = ctx.env.pointer_specified_info[ptrcls]

        if ptr_set:
            src_path_id = path_id
            if ptrcls.is_link_property(ctx.env.schema):
                src_path_id = src_path_id.ptr_path()

            ptr_set.path_id = pathctx.extend_path_id(
                src_path_id,
                ptrcls=ptrcls,
                ns=ctx.path_id_namespace,
                ctx=ctx,
            )
            assert not isinstance(ptr_set.expr, irast.Pointer)
            ptr_set.expr = irast.Pointer(
                source=ir_set,
                expr=ptr_set.expr,
                direction=s_pointers.PointerDirection.Outbound,
                ptrref=not_none(ptr_set.path_id.rptr()),
                is_definition=True,
            )
            # XXX: We would maybe like to *not* do this when it
            # already has a context, since for explain output that
            # seems nicer, but this is what we want for producing
            # actual error messages.
            ptr_set.span = ptr_span

        else:
            # The set must be something pretty trivial, so just do it
            ptr_set = setgen.extend_path(
                ir_set,
                ptrcls,
                same_computable_scope=True,
                span=ptr_span or span,
                ctx=ctx,
            )

        assert irutils.is_set_instance(ptr_set, irast.Pointer)
        set_shape.append((ptr_set, shape_op))

    ir_set.shape = tuple(set_shape)

    if (view_rptr is not None and view_rptr.ptrcls is not None and
            view_scls != stype):
        ctx.env.schema = view_scls.set_field_value(
            ctx.env.schema, 'rptr', view_rptr.ptrcls)

    return view_scls, ir_set


def _shape_el_ql_to_shape_el_desc(
    shape_el: qlast.ShapeElement,
    *,
    source: s_sources.Source,
    s_ctx: ShapeContext,
    ctx: context.ContextLevel,
) -> ShapeElementDesc:
    """Look at ShapeElement AST and annotate it for more convenient handing."""

    steps = shape_el.expr.steps
    is_linkprop = False
    is_polymorphic = False
    plen = len(steps)
    target_typexpr = None
    source_intersection = []

    if plen >= 2 and isinstance(steps[-1], qlast.TypeIntersection):
        # Target type intersection: foo: Type
        target_typexpr = steps[-1].type
        plen -= 1
        steps = steps[:-1]

    if plen == 1:
        # regular shape
        lexpr = steps[0]
        assert isinstance(lexpr, qlast.Ptr)
        is_linkprop = lexpr.type == 'property'
        if is_linkprop:
            view_rptr = s_ctx.view_rptr
            if view_rptr is None or view_rptr.ptrcls is None:
                raise errors.QueryError(
                    'invalid reference to link property '
                    'in top level shape', span=lexpr.span)
            assert isinstance(view_rptr.ptrcls, s_links.Link)
            source = view_rptr.ptrcls
    elif plen == 2 and isinstance(steps[0], qlast.TypeIntersection):
        # Source type intersection: [IS Type].foo
        source_intersection = [steps[0]]
        lexpr = steps[1]
        ptype = steps[0].type
        source_spec = typegen.ql_typeexpr_to_type(ptype, ctx=ctx)
        if not isinstance(source_spec, s_objtypes.ObjectType):
            raise errors.QueryError(
                f"expected object type, got "
                f"{source_spec.get_verbosename(ctx.env.schema)}",
                span=ptype.span,
            )
        source = source_spec
        is_polymorphic = True
    else:  # pragma: no cover
        raise RuntimeError(
            f'unexpected path length in view shape: {len(steps)}')

    assert isinstance(lexpr, qlast.Ptr)
    ptrname = lexpr.name

    if target_typexpr is None:
        path_ql = qlast.Path(
            steps=[
                *source_intersection,
                lexpr,
            ],
            partial=True,
        )
    else:
        path_ql = qlast.Path(
            steps=[
                *source_intersection,
                lexpr,
                qlast.TypeIntersection(type=target_typexpr),
            ],
            partial=True,
        )

    return ShapeElementDesc(
        ql=shape_el,
        path_ql=path_ql,
        ptr_ql=lexpr,
        ptr_name=ptrname,
        source=source,
        target_typexpr=target_typexpr,
        is_polymorphic=is_polymorphic,
        is_linkprop=is_linkprop,
    )


def _expand_splat(
    stype: s_objtypes.ObjectType,
    *,
    depth: int,
    skip_ptrs: AbstractSet[str] = frozenset(),
    skip_lprops: AbstractSet[str] = frozenset(),
    rlink: Optional[s_links.Link] = None,
    intersection: Optional[qlast.TypeIntersection] = None,
    ctx: context.ContextLevel,
) -> List[qlast.ShapeElement]:
    """Expand a splat (possibly recursively) into a list of ShapeElements"""
    elements = []
    pointers = stype.get_pointers(ctx.env.schema)
    path: list[qlast.PathElement] = []
    if intersection is not None:
        path.append(intersection)
    for ptr in pointers.objects(ctx.env.schema):
        if not isinstance(ptr, s_props.Property):
            continue
        if ptr.get_secret(ctx.env.schema):
            continue
        sname = ptr.get_shortname(ctx.env.schema)
        if sname.name in skip_ptrs:
            continue
        step = qlast.Ptr(name=sname.name)
        # Make sure not to overwrite the id property.
        if not ptr.is_id_pointer(ctx.env.schema):
            steps = path + [step]
        else:
            steps = [step]
        elements.append(qlast.ShapeElement(
            expr=qlast.Path(steps=steps),
            origin=qlast.ShapeOrigin.SPLAT_EXPANSION,
        ))

    if rlink is not None:
        for prop in rlink.get_pointers(ctx.env.schema).objects(ctx.env.schema):
            if prop.is_endpoint_pointer(ctx.env.schema):
                continue
            assert isinstance(prop, s_props.Property), \
                "non-property pointer on link?"
            sname = prop.get_shortname(ctx.env.schema)
            if sname.name in skip_lprops:
                continue
            elements.append(
                qlast.ShapeElement(
                    expr=qlast.Path(
                        steps=[qlast.Ptr(
                            name=sname.name,
                            type='property',
                        )]
                    ),
                    origin=qlast.ShapeOrigin.SPLAT_EXPANSION,
                )
            )

    if depth > 1:
        for ptr in pointers.objects(ctx.env.schema):
            if not isinstance(ptr, s_links.Link):
                continue
            pn = ptr.get_shortname(ctx.env.schema)
            if pn.name == '__type__' or pn.name in skip_ptrs:
                continue
            elements.append(
                qlast.ShapeElement(
                    expr=qlast.Path(steps=path + [qlast.Ptr(name=pn.name)]),
                    elements=_expand_splat(
                        ptr.get_target(ctx.env.schema),
                        rlink=ptr,
                        depth=depth - 1,
                        ctx=ctx,
                    ),
                    origin=qlast.ShapeOrigin.SPLAT_EXPANSION,
                )
            )

    return elements


def _gen_pointers_from_defaults(
    specified_ptrs: Set[sn.UnqualName],
    view_scls: s_objtypes.ObjectType,
    ir_set: irast.Set,
    stype: s_objtypes.ObjectType,
    s_ctx: ShapeContext,
    ctx: context.ContextLevel,
) -> Dict[s_pointers.Pointer, EarlyShapePtr]:
    path_id = ir_set.path_id
    result: List[EarlyShapePtr] = []

    if stype in ctx.active_defaults:
        vn = stype.get_verbosename(ctx.env.schema)
        raise errors.QueryError(
            f"default on property of {vn} is part of a default cycle",
        )

    scls_pointers = stype.get_pointers(ctx.env.schema)
    for pn, ptrcls in scls_pointers.items(ctx.env.schema):
        if (
            (pn in specified_ptrs or ptrcls.is_pure_computable(ctx.env.schema))
            and not ptrcls.get_protected(ctx.env.schema)
        ):
            continue

        default_expr: Optional[s_expr.Expression] = (
            ptrcls.get_default(ctx.env.schema)
        )
        if not default_expr:
            continue

        ptrcls_sn = ptrcls.get_shortname(ctx.env.schema)
        default_ql = qlast.ShapeElement(
            expr=qlast.Path(
                steps=[qlast.Ptr(name=ptrcls_sn.name)],
            ),
            compexpr=qlast.DetachedExpr(
                expr=default_expr.parse(),
                preserve_path_prefix=True,
            ),
            origin=qlast.ShapeOrigin.DEFAULT,
        )
        default_ql_desc = _shape_el_ql_to_shape_el_desc(
            default_ql, source=view_scls, s_ctx=s_ctx, ctx=ctx
        )

        with ctx.new() as scopectx:
            scopectx.schema_factoring()

            scopectx.active_defaults |= {stype}

            # add __source__ to anchors
            source_set = ir_set
            scopectx.path_scope.attach_path(
                source_set.path_id, span=None,
                optional=False,
                ctx=ctx,
            )
            scopectx.iterator_path_ids |= {source_set.path_id}
            scopectx.anchors['__source__'] = source_set

            pointer, ptr_set = _normalize_view_ptr_expr(
                ir_set,
                default_ql_desc,
                view_scls,
                path_id=path_id,
                from_default=True,
                s_ctx=s_ctx,
                ctx=scopectx,
            )

            result.append(EarlyShapePtr(
                pointer, ptr_set, qlast.ShapeOrigin.DEFAULT))

    schema = ctx.env.schema

    # Toposort defaults
    # This is required because defaults may reference each other
    # (and even contain cyclical dependencies).
    # We cannot check or preprocess this at migration time, because some
    # defaults may not be used for some inserts.
    pointer_indexes = {}
    for (index, (pointer, _, _)) in enumerate(result):
        p = pointer.get_nearest_non_derived_parent(schema)
        pointer_indexes[p.get_name(schema).name] = index
    graph = {}
    for (index, (_, irset, _)) in enumerate(result):
        assert irset
        dep_pointers = ast.find_children(irset, irast.Pointer)
        dep_rptrs = (
            # pointer.target_path_id.rptr() for pointer in dep_pointers
            pointer.ptrref for pointer in dep_pointers
            if pointer.source.typeref.id == stype.id
        )
        deps = {
            pointer_indexes[rpts.name.name] for rpts in dep_rptrs
            if rpts and rpts.name.name in pointer_indexes
        }
        graph[index] = topological.DepGraphEntry(
            item=index, deps=deps, extra=False,
        )

    ordered = [
        result[i] for i in topological.sort(graph, allow_unresolved=True)
    ]

    return {v.ptrcls: v for v in ordered}


def _raise_on_missing(
    pointers: Dict[s_pointers.Pointer, EarlyShapePtr],
    stype: s_objtypes.ObjectType,
    rewrites: Optional[irast.Rewrites],
    ctx: context.ContextLevel,
    span: Optional[parsing.Span],
) -> None:
    pointer_names = {
        ptr.get_local_name(ctx.env.schema) for ptr in pointers
    }

    scls_pointers = stype.get_pointers(ctx.env.schema)
    for pn, ptrcls in scls_pointers.items(ctx.env.schema):
        if pn == sn.UnqualName("__type__"):
            continue

        if pn in pointer_names or ptrcls.is_pure_computable(ctx.env.schema):
            continue

        if not ptrcls.get_required(ctx.env.schema):
            continue

        # is it rewritten?
        if rewrites:
            # (inserts must produce rewrites only for stype)
            assert len(rewrites.by_type) == 1
            if pn.name in next(iter(rewrites.by_type.values())):
                continue

        if ptrcls.is_property(ctx.env.schema):
            # If the target is a sequence, there's no need
            # for an explicit value.
            ptrcls_target = ptrcls.get_target(ctx.env.schema)
            assert ptrcls_target is not None
            if ptrcls_target.issubclass(
                ctx.env.schema,
                ctx.env.schema.get(
                    "std::sequence", type=s_objects.SubclassableObject
                ),
            ):
                continue

        vn = ptrcls.get_verbosename(ctx.env.schema, with_parent=True)
        msg = f"missing value for required {vn}"
        # If this is happening in the context of DDL, report a
        # QueryError because it is weird to report an ExecutionError
        # (MissingRequiredError) when nothing is really executing.
        if ctx.env.options.schema_object_context:
            raise errors.SchemaDefinitionError(msg, span=span)
        else:
            raise errors.MissingRequiredError(msg, span=span)


@dataclasses.dataclass(kw_only=True, repr=False, eq=False)
class RewriteContext:
    specified_ptrs: Set[sn.UnqualName]
    kind: qltypes.RewriteKind

    base_type: s_objtypes.ObjectType
    shape_type: s_objtypes.ObjectType


def _compile_rewrites(
    specified_ptrs: Set[sn.UnqualName],
    kind: qltypes.RewriteKind,
    view_scls: s_objtypes.ObjectType,
    ir_set: irast.Set,
    stype: s_objtypes.ObjectType,
    s_ctx: ShapeContext,
    ctx: context.ContextLevel,
) -> Optional[irast.Rewrites]:
    # init
    r_ctx = RewriteContext(
        specified_ptrs=specified_ptrs,
        kind=kind,
        base_type=stype,
        shape_type=view_scls,
    )

    # Computing anchors isn't cheap, so we want to only do it once,
    # and only do it when it is necessary.
    anchors: Dict[s_objtypes.ObjectType, RewriteAnchors] = {}

    def get_anchors(stype: s_objtypes.ObjectType) -> RewriteAnchors:
        if stype not in anchors:
            anchors[stype] = prepare_rewrite_anchors(stype, r_ctx, s_ctx, ctx)
        return anchors[stype]

    rewrites = _compile_rewrites_for_stype(
        stype, kind, ir_set, get_anchors, s_ctx, ctx=ctx
    )

    if kind == qltypes.RewriteKind.Insert:
        type_ref = typegen.type_to_typeref(stype, ctx.env)
        rewrites_by_type = {type_ref: rewrites}

    elif kind == qltypes.RewriteKind.Update:
        # Update may also change objects that are children of stype
        # Here we build a dict of rewrites for each descendent type for each
        # of its pointers.

        # This dict is stored in the context and pulled into the update
        # statement later.

        rewrites_by_type = _compile_rewrites_of_children(
            stype, rewrites, kind, ir_set, get_anchors, s_ctx, ctx
        )

    else:
        raise NotImplementedError()

    schema = ctx.env.schema
    by_type: Dict[irast.TypeRef, irast.RewritesOfType] = {}
    for ty, rewrites_of_type in rewrites_by_type.items():
        ty = ty.real_material_type

        by_type[ty] = {}
        for element in rewrites_of_type.values():
            target = element.target_set
            assert target

            ptrref = typegen.ptr_to_ptrref(element.ptrcls, ctx=ctx)
            actual_ptrref = irtypeutils.find_actual_ptrref(ty, ptrref)
            pn = actual_ptrref.shortname.name
            path_id = irast.PathId.from_pointer(
                schema, element.ptrcls, env=ctx.env
            )

            # construct a new set with correct path_id
            ptr_set = setgen.new_set_from_set(
                target,
                path_id=path_id,
                ctx=ctx,
            )

            # construct a new set with correct path_id
            ptr_set.expr = irast.Pointer(
                source=ir_set,
                expr=ptr_set.expr,
                direction=s_pointers.PointerDirection.Outbound,
                ptrref=actual_ptrref,
                is_definition=True,
            )
            assert irutils.is_set_instance(ptr_set, irast.Pointer)

            by_type[ty][pn] = (ptr_set, ptrref.real_material_ptr)

    anc = next(iter(anchors.values()), None)
    if not anc:
        return None

    return irast.Rewrites(
        old_path_id=anc.old_set.path_id if anc.old_set else None,
        by_type=by_type,
    )


def _compile_rewrites_of_children(
    stype: s_objtypes.ObjectType,
    parent_rewrites: Dict[sn.UnqualName, EarlyShapePtr],
    kind: qltypes.RewriteKind,
    ir_set: irast.Set,
    get_anchors: Callable[[s_objtypes.ObjectType], RewriteAnchors],
    s_ctx: ShapeContext,
    ctx: context.ContextLevel,
) -> Dict[irast.TypeRef, Dict[sn.UnqualName, EarlyShapePtr]]:
    rewrites_for_type: Dict[
        irast.TypeRef, Dict[sn.UnqualName, EarlyShapePtr]
    ] = {}

    # save parent to result
    type_ref = typegen.type_to_typeref(stype, ctx.env)
    rewrites_for_type[type_ref] = parent_rewrites.copy()

    for child in stype.children(ctx.env.schema):
        if child.get_is_derived(ctx.env.schema):
            continue

        # base on parent rewrites
        child_rewrites = parent_rewrites.copy()
        # override with rewrites defined here
        rewrites_defined_here = _compile_rewrites_for_stype(
            child, kind, ir_set, get_anchors, s_ctx,
            already_defined_rewrites=child_rewrites,
            ctx=ctx
        )
        child_rewrites.update(rewrites_defined_here)

        # recurse for children
        rewrites_for_type.update(
            _compile_rewrites_of_children(
                child,
                child_rewrites,
                kind,
                ir_set,
                get_anchors,
                s_ctx,
                ctx=ctx,
            )
        )

    return rewrites_for_type


def _compile_rewrites_for_stype(
    stype: s_objtypes.ObjectType,
    kind: qltypes.RewriteKind,
    ir_set: irast.Set,
    get_anchors: Callable[[s_objtypes.ObjectType], RewriteAnchors],
    s_ctx: ShapeContext,
    *,
    already_defined_rewrites: Optional[
        Mapping[sn.UnqualName, EarlyShapePtr]] = None,
    ctx: context.ContextLevel,
) -> Dict[sn.UnqualName, EarlyShapePtr]:
    schema = ctx.env.schema

    path_id = ir_set.path_id

    res = {}

    if stype in ctx.active_rewrites:
        vn = stype.get_verbosename(ctx.env.schema)
        raise errors.QueryError(
            f"rewrite rule on {vn} is part of a rewrite rule cycle",
        )

    scls_pointers = stype.get_pointers(schema)
    for pn, ptrcls in scls_pointers.items(schema):
        if ptrcls.is_pure_computable(schema):
            continue

        rewrite = ptrcls.get_rewrite(schema, kind)
        if not rewrite:
            continue
        rewrite_pointer = downcast(
            s_pointers.Pointer, rewrite.get_subject(schema))

        # Because rewrites are not duplicated on inherited properties, the
        # subject this pointer will not be on stype, but on one of its
        # ancestors. Mitigation is to pick the correct pointer from the stype.
        rewrite_pointer = downcast(
            s_pointers.Pointer, stype.get_pointers(schema).get(schema, pn)
        )

        # get_rewrite searches in ancestors for rewrites, but if the rewrite
        # for that ancestor has already been compiled, skip it to avoid
        # duplicating work
        if (
            already_defined_rewrites
            and (existing := already_defined_rewrites.get(pn))
            and (existing[0].get_nearest_non_derived_parent(schema)
                 == rewrite_pointer)
        ):
            continue

        anchors = get_anchors(stype)

        rewrite_expr: Optional[s_expr.Expression] = (
            rewrite.get_expr(ctx.env.schema)
        )
        assert rewrite_expr

        with ctx.newscope(fenced=True) as scopectx:
            scopectx.schema_factoring()
            scopectx.active_rewrites |= {stype}

            # prepare context
            scopectx.partial_path_prefix = anchors.subject_set
            nanchors = {}
            nanchors["__specified__"] = anchors.specified_set
            nanchors["__subject__"] = anchors.subject_set
            if anchors.old_set:
                nanchors["__old__"] = anchors.old_set

            for key, anchor in nanchors.items():
                scopectx.path_scope.attach_path(
                    anchor.path_id,
                    optional=(anchor is anchors.subject_set),
                    span=None,
                    ctx=ctx,
                )
                scopectx.iterator_path_ids |= {anchor.path_id}
                scopectx.anchors[key] = anchor

            # XXX: I am pretty sure this must be wrong, but we get
            # a failure without due to volatility issues in
            # test_edgeql_rewrites_16
            scopectx.env.singletons.append(anchors.subject_set.path_id)

            ctx.path_scope.factoring_allowlist.add(anchors.subject_set.path_id)

            # prepare expression
            ptrcls_sn = ptrcls.get_shortname(ctx.env.schema)
            shape_ql = qlast.ShapeElement(
                expr=qlast.Path(
                    steps=[qlast.Ptr(name=ptrcls_sn.name)],
                ),
                compexpr=qlast.DetachedExpr(
                    expr=rewrite_expr.parse(),
                    preserve_path_prefix=True,
                ),
            )
            shape_ql_desc = _shape_el_ql_to_shape_el_desc(
                shape_ql,
                source=anchors.rewrite_type,
                s_ctx=s_ctx,
                ctx=scopectx,
            )

            # compile as normal shape element
            pointer, ptr_set = _normalize_view_ptr_expr(
                anchors.subject_set,
                shape_ql_desc,
                anchors.rewrite_type,
                path_id=path_id,
                from_default=True,
                s_ctx=s_ctx,
                ctx=scopectx,
            )
            res[pn] = EarlyShapePtr(
                pointer, ptr_set, qlast.ShapeOrigin.DEFAULT
            )
    return res


@dataclasses.dataclass(kw_only=True, repr=False, eq=False)
class RewriteAnchors:
    subject_set: irast.Set
    specified_set: irast.Set
    old_set: Optional[irast.Set]

    rewrite_type: s_objtypes.ObjectType


def prepare_rewrite_anchors(
    stype: s_objtypes.ObjectType,
    r_ctx: RewriteContext,
    s_ctx: ShapeContext,
    ctx: context.ContextLevel,
) -> RewriteAnchors:
    schema = ctx.env.schema

    # init set for __subject__
    subject_path_id = irast.PathId.from_type(
        schema, stype,
        namespace=ctx.path_id_namespace, env=ctx.env,
    )
    subject_set = setgen.class_set(
        stype, path_id=subject_path_id, ctx=ctx
    )

    # init reference to std::bool
    bool_type = schema.get("std::bool", type=s_types.Type)
    bool_path = irast.PathId.from_type(
        schema,
        bool_type,
        typename=sn.QualName(module="std", name="bool"),
        env=ctx.env,
    )

    # init set for __specified__
    specified_pointers: List[irast.TupleElement] = []
    for pn, _ in stype.get_pointers(schema).items(schema):
        pointer_path_id = irast.PathId.from_type(
            schema,
            bool_type,
            typename=sn.QualName(module="__derived__", name=pn.name),
            namespace=ctx.path_id_namespace,
            env=ctx.env,
        )

        specified_pointers.append(
            irast.TupleElement(
                name=pn.name,
                val=setgen.ensure_set(
                    irast.BooleanConstant(
                        value=str(pn in r_ctx.specified_ptrs),
                        typeref=bool_path.target,
                    ),
                    ctx=ctx
                ),
                path_id=pointer_path_id
            )
        )
    specified_set = setgen.new_tuple_set(
        specified_pointers, named=True, ctx=ctx
    )

    # init set for __old__
    if r_ctx.kind == qltypes.RewriteKind.Update:
        old_name = sn.QualName("__derived__", "__old__")
        old_path_id = irast.PathId.from_type(
            schema, stype, typename=old_name,
            namespace=ctx.path_id_namespace, env=ctx.env,
        )
        old_set = setgen.new_set(
            stype=stype, path_id=old_path_id, ctx=ctx,
            expr=irast.TriggerAnchor(
                typeref=typegen.type_to_typeref(stype, env=ctx.env)),
        )
    else:
        old_set = None

    rewrite_type = r_ctx.shape_type
    if stype != r_ctx.shape_type.get_nearest_non_derived_parent(schema):
        rewrite_type = downcast(
            s_objtypes.ObjectType,
            schemactx.derive_view(
                stype,
                exprtype=s_ctx.exprtype,
                ctx=ctx,
            )
        )
        subject_set = setgen.class_set(
            rewrite_type, path_id=subject_set.path_id, ctx=ctx)
        if old_set:
            old_set = setgen.class_set(
                rewrite_type, path_id=old_set.path_id, ctx=ctx)

    return RewriteAnchors(
        subject_set=subject_set,
        specified_set=specified_set,
        old_set=old_set,
        rewrite_type=rewrite_type,
    )


def _compile_qlexpr(
    ir_source: irast.Set,
    qlexpr: qlast.Base,
    view_scls: s_objtypes.ObjectType,
    *,
    ptrcls: Optional[s_pointers.Pointer],
    ptrsource: s_sources.Source,
    ptr_name: sn.QualName,
    is_linkprop: bool,
    should_set_partial_prefix: bool,
    s_ctx: ShapeContext,
    ctx: context.ContextLevel,
) -> Tuple[irast.Set, context.ViewRPtr]:

    with ctx.newscope(fenced=True) as shape_expr_ctx:
        # Put current pointer class in context, so
        # that references to link properties in sub-SELECT
        # can be resolved.  This is necessary for proper
        # evaluation of link properties on computable links,
        # most importantly, in INSERT/UPDATE context.
        shape_expr_ctx.view_rptr = context.ViewRPtr(
            source=ptrsource if is_linkprop else view_scls,
            ptrcls=ptrcls,
            ptrcls_name=ptr_name,
            ptrcls_is_linkprop=is_linkprop,
            exprtype=s_ctx.exprtype,
        )

        shape_expr_ctx.defining_view = view_scls
        shape_expr_ctx.path_scope.unnest_fence = True
        source_set = setgen.fixup_computable_source_set(
            ir_source, ctx=shape_expr_ctx
        )

        if should_set_partial_prefix:
            shape_expr_ctx.partial_path_prefix = source_set

        if ptrcls is not None:
            if s_ctx.exprtype.is_mutation():
                shape_expr_ctx.expr_exposed = context.Exposure.EXPOSED

            shape_expr_ctx.empty_result_type_hint = \
                ptrcls.get_target(ctx.env.schema)

        shape_expr_ctx.view_map = ctx.view_map.new_child()
        setgen.update_view_map(
            source_set.path_id, source_set, ctx=shape_expr_ctx)

        irexpr = dispatch.compile(qlexpr, ctx=shape_expr_ctx)

    if ctx.expr_exposed:
        irexpr = eta_expand.eta_expand_ir(irexpr, ctx=ctx)

    return irexpr, shape_expr_ctx.view_rptr


def _normalize_view_ptr_expr(
    ir_source: irast.Set,
    shape_el_desc: ShapeElementDesc,
    view_scls: s_objtypes.ObjectType,
    *,
    path_id: irast.PathId,
    from_default: bool = False,
    pending_pointers: Mapping[s_pointers.Pointer, EarlyShapePtr] | None = None,
    s_ctx: ShapeContext,
    ctx: context.ContextLevel,
) -> Tuple[s_pointers.Pointer, Optional[irast.Set]]:
    is_mutation = s_ctx.exprtype.is_insert() or s_ctx.exprtype.is_update()

    materialized = None
    qlexpr: Optional[qlast.Expr] = None
    base_ptrcls_is_alias = False
    irexpr = None

    shape_el = shape_el_desc.ql
    ptrsource = shape_el_desc.source
    ptrname = shape_el_desc.ptr_name
    is_linkprop = shape_el_desc.is_linkprop
    is_polymorphic = shape_el_desc.is_polymorphic
    target_typexpr = shape_el_desc.target_typexpr

    is_independent_polymorphic = False

    compexpr: Optional[qlast.Expr] = shape_el.compexpr
    if compexpr is None and is_mutation:
        raise errors.QueryError(
            "mutation queries must specify values with ':='",
            span=shape_el.expr.steps[-1].span,
        )

    ptrcls: Optional[s_pointers.Pointer]

    if compexpr is None:
        ptrcls = setgen.resolve_ptr(
            ptrsource,
            ptrname,
            track_ref=shape_el_desc.ptr_ql,
            ctx=ctx,
            span=shape_el.span,
        )
        real_ptrcls = None
        if is_polymorphic:
            # For polymorphic pointers, we need to see if the *real*
            # base class has the pointer, because if so we need to use
            # that when doing cardinality inference (since it may need
            # to raise an error, if it is required). If it isn't
            # present on the real type, take note of that so that we
            # suppress the inherited cardinality.
            try:
                real_ptrcls = setgen.resolve_ptr(
                    view_scls,
                    ptrname,
                    track_ref=shape_el_desc.ptr_ql,
                    ctx=ctx,
                    span=shape_el.span,
                )
            except errors.InvalidReferenceError:
                is_independent_polymorphic = True
            ptrcls = schemactx.derive_ptr(ptrcls, view_scls, ctx=ctx)
        real_ptrcls = real_ptrcls or ptrcls

        base_ptrcls = real_ptrcls.get_bases(
            ctx.env.schema).first(ctx.env.schema)
        base_ptr_is_computable = base_ptrcls in ctx.env.source_map
        ptr_name = sn.QualName(
            module='__',
            name=ptrcls.get_shortname(ctx.env.schema).name,
        )

        # Schema computables that point to opaque unions will just have
        # BaseObject as their target, but in order to properly compile
        # it, we need to know the actual type here, so we recompute it.
        # XXX: This is a hack, though, and hopefully we can fix it once
        # the computable/alias rework lands.
        is_opaque_schema_computable = (
            ptrcls.is_pure_computable(ctx.env.schema)
            and (t := ptrcls.get_target(ctx.env.schema))
            and t.get_name(ctx.env.schema) == sn.QualName('std', 'BaseObject')
        )

        base_required = base_ptrcls.get_required(ctx.env.schema)
        base_cardinality = _get_base_ptr_cardinality(base_ptrcls, ctx=ctx)
        base_is_singleton = False
        if base_cardinality is not None and base_cardinality.is_known():
            base_is_singleton = base_cardinality.is_single()

        is_nontrivial = astutils.is_nontrivial_shape_element(shape_el)
        is_obj = not_none(ptrcls.get_target(ctx.env.schema)).is_object_type()

        if (
            is_obj
            or is_nontrivial
            or shape_el.elements

            or base_ptr_is_computable
            or is_polymorphic
            or target_typexpr is not None
            or (ctx.implicit_limit and not base_is_singleton)
            or is_opaque_schema_computable
        ):
            qlexpr = shape_el_desc.path_ql
            if shape_el.elements:
                qlexpr = qlast.Shape(expr=qlexpr, elements=shape_el.elements)

            qlexpr = astutils.ensure_ql_query(qlexpr)
            assert isinstance(qlexpr, qlast.SelectQuery)
            qlexpr.where = shape_el.where
            qlexpr.orderby = shape_el.orderby

            if shape_el.offset or shape_el.limit:
                qlexpr = qlast.SelectQuery(result=qlexpr, implicit=True)
                qlexpr.offset = shape_el.offset
                qlexpr.limit = shape_el.limit

            if (
                ctx.expr_exposed
                and ctx.implicit_limit
                and not base_is_singleton
            ):
                qlexpr = qlast.SelectQuery(result=qlexpr, implicit=True)
                qlexpr.limit = qlast.Constant.integer(ctx.implicit_limit)

        if target_typexpr is not None:
            assert isinstance(target_typexpr, qlast.TypeName)
            intersector_type = schemactx.get_schema_type(
                target_typexpr.maintype, ctx=ctx)

            int_result = schemactx.apply_intersection(
                ptrcls.get_target(ctx.env.schema),  # type: ignore
                intersector_type,
                ctx=ctx,
            )

            ptr_target = int_result.stype
        else:
            _ptr_target = ptrcls.get_target(ctx.env.schema)
            assert _ptr_target
            ptr_target = _ptr_target

        ptr_required = base_required
        ptr_cardinality = base_cardinality
        if shape_el.where or is_polymorphic:
            # If the shape has a filter on it, we need to force a reinference
            # of the cardinality, to produce an error if needed.
            ptr_cardinality = None
        if ptr_cardinality is None or not ptr_cardinality.is_known():
            # We do not know the parent's pointer cardinality yet.
            ctx.env.pointer_derivation_map[base_ptrcls].append(ptrcls)
            ctx.env.pointer_specified_info[ptrcls] = (
                shape_el.cardinality, shape_el.required, shape_el.span)

        # If we generated qlexpr for the element, we process the
        # subview by just compiling the qlexpr. This is so that we can
        # figure out if it needs materialization and also so that
        # `qlexpr is not None` always implies that we did the
        # compilation.
        if qlexpr:
            irexpr, _ = _compile_qlexpr(
                ir_source,
                qlexpr,
                view_scls,
                ptrcls=ptrcls,
                ptrsource=ptrsource,
                ptr_name=ptr_name,
                is_linkprop=is_linkprop,
                should_set_partial_prefix=True,
                s_ctx=s_ctx,
                ctx=ctx,
            )
            materialized = setgen.should_materialize(
                irexpr, ptrcls=ptrcls,
                materialize_visible=True, skipped_bindings={path_id},
                ctx=ctx)
            ptr_target = setgen.get_set_type(irexpr, ctx=ctx)

    # compexpr is not None
    else:
        base_ptrcls = ptrcls = None

        if (is_mutation
                and ptrname not in ctx.special_computables_in_mutation_shape):
            # If this is a mutation, the pointer must exist.
            ptrcls = setgen.resolve_ptr(
                ptrsource, ptrname, track_ref=shape_el_desc.ptr_ql, ctx=ctx)
            if ptrcls.is_pure_computable(ctx.env.schema) and not from_default:
                ptr_vn = ptrcls.get_verbosename(ctx.env.schema,
                                                with_parent=True)
                raise errors.QueryError(
                    f'modification of computed {ptr_vn} is prohibited',
                    span=shape_el.span)

            base_ptrcls = ptrcls.get_bases(
                ctx.env.schema).first(ctx.env.schema)

            ptr_name = sn.QualName(
                module='__',
                name=ptrcls.get_shortname(ctx.env.schema).name,
            )

        else:
            ptr_name = sn.QualName(
                module='__',
                name=ptrname,
            )

            try:
                is_linkprop_mutation = (
                    is_linkprop
                    and s_ctx.view_rptr is not None
                    and s_ctx.view_rptr.exprtype.is_mutation()
                )

                ptrcls = setgen.resolve_ptr(
                    ptrsource,
                    ptrname,
                    track_ref=(
                        False if not is_linkprop_mutation
                        else shape_el_desc.ptr_ql
                    ),
                    ctx=ctx,
                )

                base_ptrcls = ptrcls.get_bases(
                    ctx.env.schema).first(ctx.env.schema)
            except errors.InvalidReferenceError:
                # Check if we aren't inside of modifying statement
                # for link property, otherwise this is a NEW
                # computable pointer, it's fine.
                if is_linkprop_mutation:
                    raise

        qlexpr = astutils.ensure_ql_query(compexpr)
        # HACK: For scope tree related reasons, DML inside of free objects
        # needs to be wrapped in a SELECT. This is probably fixable.
        if irutils.is_trivial_free_object(ir_source):
            qlexpr = astutils.ensure_ql_select(qlexpr)

        if (
            ctx.expr_exposed
            and ctx.implicit_limit
        ):
            qlexpr = qlast.SelectQuery(result=qlexpr, implicit=True)
            qlexpr.limit = qlast.Constant.integer(ctx.implicit_limit)

        irexpr, sub_view_rptr = _compile_qlexpr(
            ir_source,
            qlexpr,
            view_scls,
            ptrcls=ptrcls,
            ptrsource=ptrsource,
            ptr_name=ptr_name,
            is_linkprop=is_linkprop,
            # do not set partial path prefix if in the insert
            # shape but not in defaults
            should_set_partial_prefix=(
                not s_ctx.exprtype.is_insert() or from_default),
            s_ctx=s_ctx,
            ctx=ctx,
        )
        materialized = setgen.should_materialize(
            irexpr, ptrcls=ptrcls,
            materialize_visible=True, skipped_bindings={path_id},
            ctx=ctx)
        ptr_target = setgen.get_set_type(irexpr, ctx=ctx)

        if (
            shape_el.operation.op is qlast.ShapeOp.APPEND
            or shape_el.operation.op is qlast.ShapeOp.SUBTRACT
        ):
            if not s_ctx.exprtype.is_update():
                op = (
                    '+=' if shape_el.operation.op is qlast.ShapeOp.APPEND
                    else '-='
                )
                raise errors.EdgeQLSyntaxError(
                    f"unexpected '{op}'",
                    span=shape_el.operation.span,
                )

        irexpr.span = compexpr.span

        is_inbound_alias = False
        if base_ptrcls is None:
            base_ptrcls = sub_view_rptr.base_ptrcls
            base_ptrcls_is_alias = sub_view_rptr.ptrcls_is_alias
            is_inbound_alias = (
                sub_view_rptr.rptr_dir is s_pointers.PointerDirection.Inbound)

        if ptrcls is not None:
            ctx.env.schema = ptrcls.set_field_value(
                ctx.env.schema, 'owned', True)

        ptr_cardinality = None
        ptr_required = False

        _record_created_collection_types(ptr_target, ctx)

        generic_type = ptr_target.find_generic(ctx.env.schema)
        if generic_type is not None:
            raise errors.QueryError(
                'expression returns value of indeterminate type',
                span=ctx.env.type_origins.get(generic_type),
            )

        # Validate that the insert/update expression is
        # of the correct class.
        if is_mutation and ptrcls is not None:
            base_target = ptrcls.get_target(ctx.env.schema)
            assert base_target is not None
            if ptr_target.assignment_castable_to(
                    base_target,
                    schema=ctx.env.schema):
                # Force assignment casts if the target type is not a
                # subclass of the base type and the cast is not to an
                # object type.
                if not (
                    base_target.is_object_type()
                    or s_types.is_type_compatible(
                        base_target, ptr_target, schema=ctx.env.schema
                    )
                ):
                    qlexpr = astutils.ensure_ql_query(
                        qlast.TypeCast(
                            type=typegen.type_to_ql_typeref(
                                base_target, ctx=ctx
                            ),
                            expr=compexpr,
                        )
                    )
                    ptr_target = base_target
                    # We also need to compile the cast to IR.
                    with ctx.new() as subctx:
                        subctx.anchors = subctx.anchors.copy()
                        source_path = subctx.create_anchor(irexpr, 'a')
                        cast_qlexpr = astutils.ensure_ql_query(
                            qlast.TypeCast(
                                type=typegen.type_to_ql_typeref(
                                    base_target, ctx=ctx
                                ),
                                expr=source_path,
                            )
                        )

                        # HACK: This is mad dodgy. Hide the Pointer
                        # when compiling.
                        old_expr = irexpr.expr
                        if isinstance(old_expr, irast.Pointer):
                            assert old_expr.expr
                            irexpr.expr = old_expr.expr
                        irexpr = dispatch.compile(cast_qlexpr, ctx=subctx)
                        if isinstance(old_expr, irast.Pointer):
                            old_expr.expr = irexpr.expr
                            irexpr.expr = old_expr

            else:
                expected = [
                    repr(str(base_target.get_displayname(ctx.env.schema)))
                ]

                ercls: Type[errors.EdgeDBError]
                if ptrcls.is_property(ctx.env.schema):
                    ercls = errors.InvalidPropertyTargetError
                else:
                    ercls = errors.InvalidLinkTargetError

                ptr_vn = ptrcls.get_verbosename(ctx.env.schema,
                                                with_parent=True)

                raise ercls(
                    f'invalid target for {ptr_vn}: '
                    f'{str(ptr_target.get_displayname(ctx.env.schema))!r} '
                    f'(expecting {" or ".join(expected)})'
                )

    # Prohibit update of readonly
    if (
        s_ctx.exprtype.is_update()
        and ptrcls
        and ptrcls.get_readonly(ctx.env.schema)
    ):
        raise errors.QueryError(
            f'cannot update {ptrcls.get_verbosename(ctx.env.schema)}: '
            f'it is declared as read-only',
            span=compexpr.span if compexpr else None,
        )

    if (
        s_ctx.exprtype.is_mutation()
        and ptrcls
        and ptrcls.get_protected(ctx.env.schema)
        and not from_default
    ):
        # 4.0 shipped with a bug where dumps included protected fields
        # in config values, so we need to suppress the error in that
        # case.  Default value injection is set up to *always* inject
        # on protected pointers.
        if ctx.env.options.dump_restore_mode:
            return ptrcls, None
        raise errors.QueryError(
            f'cannot assign to {ptrcls.get_verbosename(ctx.env.schema)}: '
            f'it is protected',
            span=compexpr.span if compexpr else None,
        )

    # Prohibit invalid operations on id
    id_access = (
        ptrcls
        and ptrcls.is_id_pointer(ctx.env.schema)
        and (
            not ctx.env.options.allow_user_specified_id
            or not s_ctx.exprtype.is_mutation()
        )
    )
    if (
        (compexpr is not None or is_polymorphic)
        and id_access and not from_default and ptrcls
    ):
        vn = ptrcls.get_verbosename(ctx.env.schema)
        if is_polymorphic:
            msg = (f'cannot access {vn} on a polymorphic '
                   f'shape element')
        else:
            msg = f'cannot assign to {vn}'
        if (
            not ctx.env.options.allow_user_specified_id
            and s_ctx.exprtype.is_mutation()
        ):
            hint = (
                'consider enabling the "allow_user_specified_id" '
                'configuration parameter to allow setting custom object ids'
            )
        else:
            hint = None

        raise errors.QueryError(msg, span=shape_el.span, hint=hint)

    # Common code for computed/not computed

    if (
        pending_pointers is not None and ptrcls is not None
        and (prev := pending_pointers.get(ptrcls)) is not None
        and prev.shape_origin is not qlast.ShapeOrigin.SPLAT_EXPANSION
    ):
        vnp = ptrcls.get_verbosename(ctx.env.schema, with_parent=True)
        raise errors.QueryError(
            f'duplicate definition of {vnp}',
            span=shape_el.span)

    if qlexpr is not None or ptrcls is None:
        src_scls: s_sources.Source

        if is_linkprop:
            # Proper checking was done when is_linkprop is defined.
            assert s_ctx.view_rptr is not None
            assert isinstance(s_ctx.view_rptr.ptrcls, s_links.Link)
            src_scls = s_ctx.view_rptr.ptrcls
        else:
            src_scls = view_scls

        if ptr_target.is_object_type():
            base = ctx.env.get_schema_object_and_track(
                sn.QualName('std', 'link'), expr=None)
        else:
            base = ctx.env.get_schema_object_and_track(
                sn.QualName('std', 'property'), expr=None)

        if base_ptrcls is not None:
            derive_from = base_ptrcls
        else:
            derive_from = base

        derived_name = schemactx.derive_view_name(
            base_ptrcls,
            derived_name_base=ptr_name,
            derived_name_quals=[str(src_scls.get_name(ctx.env.schema))],
            ctx=ctx,
        )

        existing = ctx.env.schema.get(
            derived_name, default=None, type=s_pointers.Pointer)
        if existing is not None:
            existing_target = existing.get_target(ctx.env.schema)
            assert existing_target is not None
            if ctx.recompiling_schema_alias:
                ptr_cardinality = existing.get_cardinality(ctx.env.schema)
                ptr_required = existing.get_required(ctx.env.schema)
            if ptr_target == existing_target:
                ptrcls = existing
            elif ptr_target.implicitly_castable_to(
                    existing_target, ctx.env.schema):

                ctx.env.ptr_ref_cache.pop(existing, None)
                ctx.env.schema = existing.set_target(
                    ctx.env.schema, ptr_target)
                ptrcls = existing
            else:
                vnp = existing.get_verbosename(
                    ctx.env.schema, with_parent=True)

                t1_vn = existing_target.get_verbosename(ctx.env.schema)
                t2_vn = ptr_target.get_verbosename(ctx.env.schema)

                if compexpr is not None:
                    span = compexpr.span
                else:
                    span = shape_el.expr.steps[-1].span
                raise errors.SchemaError(
                    f'cannot redefine {vnp} as {t2_vn}',
                    details=f'{vnp} is defined as {t1_vn}',
                    span=span,
                )
        else:
            ptrcls = schemactx.derive_ptr(
                derive_from, src_scls, ptr_target,
                derive_backlink=is_inbound_alias,
                derived_name=derived_name,
                ctx=ctx)

    elif ptrcls.get_target(ctx.env.schema) != ptr_target:
        ctx.env.ptr_ref_cache.pop(ptrcls, None)
        ctx.env.schema = ptrcls.set_target(ctx.env.schema, ptr_target)

    assert ptrcls is not None

    if materialized and not is_mutation and ctx.qlstmt:
        assert ptrcls not in ctx.env.materialized_sets
        ctx.env.materialized_sets[ptrcls] = ctx.qlstmt, materialized

        if irexpr:
            setgen.maybe_materialize(ptrcls, irexpr, ctx=ctx)

    if qlexpr is not None:
        ctx.env.schema = ptrcls.set_field_value(
            ctx.env.schema, 'defined_here', True
        )

    if qlexpr is not None:
        ctx.env.source_map[ptrcls] = irast.ComputableInfo(
            qlexpr=qlexpr,
            irexpr=irexpr,
            context=ctx,
            path_id=path_id,
            path_id_ns=s_ctx.path_id_namespace,
            shape_op=shape_el.operation.op,
            should_materialize=materialized or [],
        )

    if compexpr is not None or is_polymorphic or materialized:
        if (old_ptrref := ctx.env.ptr_ref_cache.get(ptrcls)):
            old_ptrref.is_computable = True

        ctx.env.schema = ptrcls.set_field_value(
            ctx.env.schema,
            'computable',
            True,
        )

        ctx.env.schema = ptrcls.set_field_value(
            ctx.env.schema,
            'owned',
            True,
        )

    if ptr_cardinality is not None:
        ctx.env.schema = ptrcls.set_field_value(
            ctx.env.schema, 'cardinality', ptr_cardinality)
        ctx.env.schema = ptrcls.set_field_value(
            ctx.env.schema, 'required', ptr_required)
    else:
        if qlexpr is None and ptrcls is not base_ptrcls:
            ctx.env.pointer_derivation_map[base_ptrcls].append(ptrcls)

        base_cardinality = None
        base_required = None
        if (
            base_ptrcls is not None
            and not base_ptrcls_is_alias
            and not is_independent_polymorphic
        ):
            base_cardinality = _get_base_ptr_cardinality(base_ptrcls, ctx=ctx)
            base_required = base_ptrcls.get_required(ctx.env.schema)

        if base_cardinality is None or not base_cardinality.is_known():
            # If the base cardinality is not known the we can't make
            # any checks here and will rely on validation in the
            # cardinality inferer.
            specified_cardinality = shape_el.cardinality
            specified_required = shape_el.required
        else:
            specified_cardinality = base_cardinality

            # Inferred optionality overrides that of the base pointer
            # if base pointer is not `required`, hence the is True check.
            if shape_el.required is not None:
                specified_required = shape_el.required
            elif base_required is True:
                specified_required = base_required
            else:
                specified_required = None

            if (
                shape_el.cardinality is not None
                and base_ptrcls is not None
                and shape_el.cardinality != base_cardinality
            ):
                base_src = base_ptrcls.get_source(ctx.env.schema)
                assert base_src is not None
                base_src_name = base_src.get_verbosename(ctx.env.schema)
                raise errors.SchemaError(
                    f'cannot redefine the cardinality of '
                    f'{ptrcls.get_verbosename(ctx.env.schema)}: '
                    f'it is defined as {base_cardinality.as_ptr_qual()!r} '
                    f'in the base {base_src_name}',
                    span=compexpr.span if compexpr else None,
                )

            if (
                shape_el.required is False
                and base_ptrcls is not None
                and base_required
            ):
                base_src = base_ptrcls.get_source(ctx.env.schema)
                assert base_src is not None
                base_src_name = base_src.get_verbosename(ctx.env.schema)
                raise errors.SchemaError(
                    f'cannot redefine '
                    f'{ptrcls.get_verbosename(ctx.env.schema)} '
                    f'as optional: it is defined as required '
                    f'in the base {base_src_name}',
                    span=compexpr.span if compexpr else None,
                )

        ctx.env.pointer_specified_info[ptrcls] = (
            specified_cardinality, specified_required, shape_el.span)

        ctx.env.schema = ptrcls.set_field_value(
            ctx.env.schema, 'cardinality', qltypes.SchemaCardinality.Unknown)

    if irexpr and not irexpr.span:
        irexpr.span = shape_el.span

    return ptrcls, irexpr


def derive_ptrcls(
    view_rptr: context.ViewRPtr,
    *,
    target_scls: s_types.Type,
    ctx: context.ContextLevel
) -> s_pointers.Pointer:

    if view_rptr.ptrcls is None:
        if view_rptr.base_ptrcls is None:
            if target_scls.is_object_type():
                base = ctx.env.get_schema_object_and_track(
                    sn.QualName('std', 'link'), expr=None)
                view_rptr.base_ptrcls = cast(s_links.Link, base)
            else:
                base = ctx.env.get_schema_object_and_track(
                    sn.QualName('std', 'property'), expr=None)
                view_rptr.base_ptrcls = cast(s_props.Property, base)

        derived_name = schemactx.derive_view_name(
            view_rptr.base_ptrcls,
            derived_name_base=view_rptr.ptrcls_name,
            derived_name_quals=(
                str(view_rptr.source.get_name(ctx.env.schema)),
            ),
            ctx=ctx)

        is_inbound_alias = (
            view_rptr.rptr_dir is s_pointers.PointerDirection.Inbound)
        view_rptr.ptrcls = schemactx.derive_ptr(
            view_rptr.base_ptrcls, view_rptr.source, target_scls,
            derived_name=derived_name,
            derive_backlink=is_inbound_alias,
            ctx=ctx
        )

    else:
        view_rptr.ptrcls = schemactx.derive_ptr(
            view_rptr.ptrcls, view_rptr.source, target_scls,
            derived_name_quals=(
                str(view_rptr.source.get_name(ctx.env.schema)),
            ),
            ctx=ctx
        )

    return view_rptr.ptrcls


def _link_has_shape(
    ptrcls: s_pointers.PointerLike, *, ctx: context.ContextLevel
) -> bool:
    if not isinstance(ptrcls, s_links.Link):
        return False

    ptr_shape = {p for p, _ in ctx.env.view_shapes[ptrcls]}
    for p in ptrcls.get_pointers(ctx.env.schema).objects(ctx.env.schema):
        if p.is_special_pointer(ctx.env.schema) or p not in ptr_shape:
            continue
        else:
            return True

    return False


def _get_base_ptr_cardinality(
    ptrcls: s_pointers.Pointer,
    *,
    ctx: context.ContextLevel,
) -> Optional[qltypes.SchemaCardinality]:
    ptr_name = ptrcls.get_name(ctx.env.schema)
    if ptr_name in {
        sn.QualName('std', 'link'),
        sn.QualName('std', 'property')
    }:
        return None
    else:
        return ptrcls.get_cardinality(ctx.env.schema)


def has_implicit_tid(
    stype: s_types.Type, *, is_mutation: bool, ctx: context.ContextLevel
) -> bool:

    return (
        stype.is_object_type()
        and not stype.is_free_object_type(ctx.env.schema)
        and not is_mutation
        and ctx.implicit_tid_in_shapes
    )


def has_implicit_tname(
    stype: s_types.Type, *, is_mutation: bool, ctx: context.ContextLevel
) -> bool:

    return (
        stype.is_object_type()
        and not stype.is_free_object_type(ctx.env.schema)
        and not is_mutation
        and ctx.implicit_tname_in_shapes
    )


def has_implicit_type_computables(
    stype: s_types.Type, *, is_mutation: bool, ctx: context.ContextLevel
) -> bool:

    return (
        has_implicit_tid(stype, is_mutation=is_mutation, ctx=ctx)
        or has_implicit_tname(stype, is_mutation=is_mutation, ctx=ctx)
    )


def _inline_type_computable(
    ir_set: irast.Set,
    stype: s_objtypes.ObjectType,
    compname: str,
    propname: str,
    *,
    shape_ptrs: List[ShapePtr],
    ctx: context.ContextLevel,
) -> None:
    assert isinstance(stype, s_objtypes.ObjectType)
    # Injecting into non-view objects /almost/ works, but it fails if the
    # object is in the std library, and is dodgy always.
    # Prevent it in general to find bugs faster.
    assert stype.is_view(ctx.env.schema)

    ptr: Optional[s_pointers.Pointer]
    try:
        ptr = setgen.resolve_ptr(stype, compname, track_ref=False, ctx=ctx)
        # The pointer might exist on the base type. That doesn't count,
        # and we need to re-inject it.
        if ptr not in ctx.env.source_map:
            ptr = None
    except errors.InvalidReferenceError:
        ptr = None

    ptr_set = None
    if ptr is None:
        ql = qlast.ShapeElement(
            required=True,
            expr=qlast.Path(
                steps=[qlast.Ptr(
                    name=compname,
                    direction=s_pointers.PointerDirection.Outbound,
                )],
            ),
            compexpr=qlast.Path(
                steps=[
                    qlast.SpecialAnchor(name='__source__'),
                    qlast.Ptr(
                        name='__type__',
                        direction=s_pointers.PointerDirection.Outbound,
                    ),
                    qlast.Ptr(
                        name=propname,
                        direction=s_pointers.PointerDirection.Outbound,
                    )
                ]
            )
        )
        ql_desc = _shape_el_ql_to_shape_el_desc(
            ql, source=stype, s_ctx=ShapeContext(), ctx=ctx
        )

        with ctx.new() as scopectx:
            scopectx.anchors = scopectx.anchors.copy()
            # Use the actual base type as the root of the injection, so that
            # if a user has overridden `__type__` in a computable,
            # we see through that.
            base_stype = stype.get_nearest_non_derived_parent(ctx.env.schema)
            base_ir_set = setgen.ensure_set(
                ir_set, type_override=base_stype, ctx=scopectx)

            scopectx.anchors['__source__'] = base_ir_set
            ptr, ptr_set = _normalize_view_ptr_expr(
                base_ir_set,
                ql_desc,
                stype,
                path_id=ir_set.path_id,
                s_ctx=ShapeContext(),
                ctx=scopectx
            )

    # even if the pointer was not created here, or was already present in
    # the shape, we set defined_here, so it is not inlined in `extend_path`.
    ctx.env.schema = ptr.set_field_value(
        ctx.env.schema, 'defined_here', True
    )

    view_shape = ctx.env.view_shapes[stype]
    view_shape_ptrs = {p for p, _ in view_shape}
    if ptr not in view_shape_ptrs:
        if ptr not in ctx.env.pointer_specified_info:
            ctx.env.pointer_specified_info[ptr] = (None, None, None)
        view_shape.insert(0, (ptr, qlast.ShapeOp.ASSIGN))
        shape_ptrs.insert(
            0, ShapePtr(ir_set, ptr, qlast.ShapeOp.ASSIGN, ptr_set))


def _get_shape_configuration_inner(
    ir_set: irast.Set,
    shape_ptrs: List[ShapePtr],
    stype: s_types.Type,
    *,
    parent_view_type: Optional[s_types.ExprType]=None,
    ctx: context.ContextLevel
) -> None:
    is_objtype = ir_set.path_id.is_objtype_path()
    all_materialize = all(
        op == qlast.ShapeOp.MATERIALIZE for _, _, op, _ in shape_ptrs)

    if is_objtype:
        assert isinstance(stype, s_objtypes.ObjectType)

        view_type = stype.get_expr_type(ctx.env.schema)
        is_mutation = view_type in (s_types.ExprType.Insert,
                                    s_types.ExprType.Update)
        is_parent_update = parent_view_type is s_types.ExprType.Update

        implicit_id = (
            # shape is not specified at all
            not shape_ptrs
            # implicit ids are always wanted
            or (ctx.implicit_id_in_shapes and not is_mutation)
            # we are inside an UPDATE shape and this is
            # an explicit expression (link target update)
            or (is_parent_update and irutils.sub_expr(ir_set) is not None)
            or all_materialize
        )
        # We actually *always* inject an implicit id, but it's just
        # there in case materialization needs it, in many cases.
        implicit_op = qlast.ShapeOp.ASSIGN
        if not implicit_id:
            implicit_op = qlast.ShapeOp.MATERIALIZE

        # We want the id in this shape and it's not already there,
        # so insert it in the first position.
        pointers = stype.get_pointers(ctx.env.schema).objects(
            ctx.env.schema)
        view_shape = ctx.env.view_shapes[stype]
        view_shape_ptrs = {p for p, _ in view_shape}
        for ptr in pointers:
            if ptr.is_id_pointer(ctx.env.schema):
                if ptr not in view_shape_ptrs:
                    shape_metadata = ctx.env.view_shapes_metadata[stype]
                    view_shape.insert(0, (ptr, implicit_op))
                    shape_metadata.has_implicit_id = True
                    shape_ptrs.insert(
                        0, ShapePtr(ir_set, ptr, implicit_op, None))
                break

    is_mutation = parent_view_type in {
        s_types.ExprType.Insert,
        s_types.ExprType.Update
    }

    if (
        stype is not None
        and has_implicit_tid(stype, is_mutation=is_mutation, ctx=ctx)
    ):
        assert isinstance(stype, s_objtypes.ObjectType)
        _inline_type_computable(
            ir_set, stype, '__tid__', 'id', ctx=ctx, shape_ptrs=shape_ptrs)

    if (
        stype is not None
        and has_implicit_tname(stype, is_mutation=is_mutation, ctx=ctx)
    ):
        assert isinstance(stype, s_objtypes.ObjectType)
        _inline_type_computable(
            ir_set, stype, '__tname__', 'name', ctx=ctx, shape_ptrs=shape_ptrs)


def _get_early_shape_configuration(
    ir_set: irast.Set,
    in_shape_ptrs: List[ShapePtr],
    *,
    rptrcls: Optional[s_pointers.Pointer],
    parent_view_type: Optional[s_types.ExprType]=None,
    ctx: context.ContextLevel
) -> List[ShapePtr]:
    """Return a list of (source_set, ptrcls) pairs as a shape for a given set.
    """

    stype = setgen.get_set_type(ir_set, ctx=ctx)

    # HACK: For some reason, all the link properties need to go last or
    # things choke in native output mode?
    shape_ptrs = sorted(
        in_shape_ptrs,
        key=lambda arg: arg.ptrcls.is_link_property(ctx.env.schema),
    )

    _get_shape_configuration_inner(
        ir_set, shape_ptrs, stype, parent_view_type=parent_view_type, ctx=ctx)

    return shape_ptrs


def _get_late_shape_configuration(
    ir_set: irast.Set,
    *,
    rptr: Optional[irast.Pointer]=None,
    parent_view_type: Optional[s_types.ExprType]=None,
    ctx: context.ContextLevel
) -> List[ShapePtr]:

    """Return a list of (source_set, ptrcls) pairs as a shape for a given set.
    """

    stype = setgen.get_set_type(ir_set, ctx=ctx)

    sources: List[Union[s_types.Type, s_pointers.PointerLike]] = []
    link_view = False
    is_objtype = ir_set.path_id.is_objtype_path()

    if rptr is None:
        if isinstance(ir_set.expr, irast.Pointer):
            rptr = ir_set.expr
    elif ir_set.expr and not isinstance(ir_set.expr, irast.Pointer):
        # If we have a specified rptr but set is not a pointer itself,
        # construct a version of the set that is pointer so it can be used
        # as the path tip for applying pointers. This ensures that
        # we can find link properties on late shapes.
        ir_set = setgen.new_set_from_set(
            ir_set, expr=rptr.replace(expr=ir_set.expr, is_phony=True), ctx=ctx
        )

    rptrcls: Optional[s_pointers.PointerLike]
    if rptr is not None:
        rptrcls = typegen.ptrcls_from_ptrref(rptr.ptrref, ctx=ctx)
    else:
        rptrcls = None

    link_view = (
        rptrcls is not None and
        not rptrcls.is_link_property(ctx.env.schema) and
        _link_has_shape(rptrcls, ctx=ctx)
    )

    if is_objtype or not link_view:
        sources.append(stype)

    if link_view:
        assert rptrcls is not None
        sources.append(rptrcls)

    shape_ptrs: List[ShapePtr] = []

    for source in sources:
        for ptr, shape_op in ctx.env.view_shapes[source]:
            shape_ptrs.append(ShapePtr(ir_set, ptr, shape_op, None))

    _get_shape_configuration_inner(
        ir_set, shape_ptrs, stype, parent_view_type=parent_view_type, ctx=ctx)

    return shape_ptrs


@functools.singledispatch
def late_compile_view_shapes(
    expr: irast.Base,
    *,
    rptr: Optional[irast.Pointer] = None,
    parent_view_type: Optional[s_types.ExprType] = None,
    ctx: context.ContextLevel,
) -> None:
    """Do a late insertion of any unprocessed shapes.

    We mainly compile shapes in process_view, but late_compile_view_shapes
    is responsible for compiling implicit exposed shapes (containing
    only id) and in cases like accessing a semi-joined shape.

    """
    pass


@late_compile_view_shapes.register(irast.Set)
def _late_compile_view_shapes_in_set(
        ir_set: irast.Set, *,
        rptr: Optional[irast.Pointer] = None,
        parent_view_type: Optional[s_types.ExprType] = None,
        ctx: context.ContextLevel) -> None:

    shape_ptrs = _get_late_shape_configuration(
        ir_set, rptr=rptr, parent_view_type=parent_view_type, ctx=ctx)

    # We want to push down the shape to better correspond with where it
    # appears in the query (rather than lifting it up to the first
    # place the view_type appears---this is a little hacky, because
    # letting it be lifted up is the natural thing with our view type-driven
    # shape compilation).
    #
    # This is to avoid losing subquery distinctions (in cases
    # like test_edgeql_scope_tuple_15), and generally seems more natural.
    is_definition_or_not_pointer = (
        not isinstance(ir_set.expr, irast.Pointer) or ir_set.expr.is_definition
    )
    expr = irutils.sub_expr(ir_set)
    if (
        isinstance(expr, (irast.SelectStmt, irast.GroupStmt))
        and is_definition_or_not_pointer
        and (setgen.get_set_type(ir_set, ctx=ctx) ==
             setgen.get_set_type(expr.result, ctx=ctx))
    ):
        child = expr.result
        set_scope = pathctx.get_set_scope(ir_set, ctx=ctx)

        if shape_ptrs:
            pathctx.register_set_in_scope(ir_set, ctx=ctx)
        with ctx.new() as scopectx:
            if set_scope is not None:
                scopectx.path_scope = set_scope

            if not rptr and isinstance(ir_set.expr, irast.Pointer):
                rptr = ir_set.expr
            late_compile_view_shapes(
                child,
                rptr=rptr,
                parent_view_type=parent_view_type,
                ctx=scopectx)

        ir_set.shape_source = child if child.shape else child.shape_source
        return

    if shape_ptrs:
        pathctx.register_set_in_scope(ir_set, ctx=ctx)
        stype = setgen.get_set_type(ir_set, ctx=ctx)

        # If the shape has already been populated (because the set is
        # referenced multiple times), then we've got nothing to do.
        if ir_set.shape:
            # We want to make sure anything inside of the shape gets
            # processed, though, so we do need to look through the
            # internals.
            for element, _ in ir_set.shape:
                element_scope = pathctx.get_set_scope(element, ctx=ctx)
                with ctx.new() as scopectx:
                    if element_scope:
                        scopectx.path_scope = element_scope
                    late_compile_view_shapes(
                        element,
                        parent_view_type=stype.get_expr_type(ctx.env.schema),
                        ctx=scopectx)

            return

        shape = []
        for path_tip, ptr, shape_op, _ in shape_ptrs:
            span = None
            if ptr in ctx.env.pointer_specified_info:
                _, _, span = ctx.env.pointer_specified_info[ptr]

            element = setgen.extend_path(
                path_tip,
                ptr,
                same_computable_scope=True,
                span=span,
                ctx=ctx,
            )

            element_scope = pathctx.get_set_scope(element, ctx=ctx)

            if element_scope is None:
                element_scope = ctx.path_scope.attach_fence()
                pathctx.assign_set_scope(element, element_scope, ctx=ctx)

            if element_scope.namespaces:
                element.path_id = element.path_id.merge_namespace(
                    element_scope.namespaces)

            with ctx.new() as scopectx:
                scopectx.path_scope = element_scope
                late_compile_view_shapes(
                    element,
                    parent_view_type=stype.get_expr_type(ctx.env.schema),
                    ctx=scopectx)

            shape.append((element, shape_op))

        ir_set.shape = tuple(shape)

    elif expr is not None:
        set_scope = pathctx.get_set_scope(ir_set, ctx=ctx)
        if set_scope is not None:
            with ctx.new() as scopectx:
                scopectx.path_scope = set_scope
                late_compile_view_shapes(expr, ctx=scopectx)
        else:
            late_compile_view_shapes(expr, ctx=ctx)

    elif isinstance(ir_set.expr, irast.TupleIndirectionPointer):
        late_compile_view_shapes(ir_set.expr.source, ctx=ctx)


@late_compile_view_shapes.register(irast.SelectStmt)
def _late_compile_view_shapes_in_select(
    stmt: irast.SelectStmt,
    *,
    rptr: Optional[irast.Pointer] = None,
    parent_view_type: Optional[s_types.ExprType] = None,
    ctx: context.ContextLevel,
) -> None:
    late_compile_view_shapes(
        stmt.result, rptr=rptr, parent_view_type=parent_view_type, ctx=ctx)


@late_compile_view_shapes.register(irast.Call)
def _late_compile_view_shapes_in_call(
    expr: irast.Call,
    *,
    rptr: Optional[irast.Pointer] = None,
    parent_view_type: Optional[s_types.ExprType] = None,
    ctx: context.ContextLevel,
) -> None:

    if expr.func_polymorphic:
        for call_arg in expr.args.values():
            arg = call_arg.expr
            arg_scope = pathctx.get_set_scope(arg, ctx=ctx)
            if arg_scope is not None:
                with ctx.new() as scopectx:
                    scopectx.path_scope = arg_scope
                    late_compile_view_shapes(arg, ctx=scopectx)
            else:
                late_compile_view_shapes(arg, ctx=ctx)


@late_compile_view_shapes.register(irast.Tuple)
def _late_compile_view_shapes_in_tuple(
    expr: irast.Tuple,
    *,
    rptr: Optional[irast.Pointer] = None,
    parent_view_type: Optional[s_types.ExprType] = None,
    ctx: context.ContextLevel,
) -> None:
    for element in expr.elements:
        late_compile_view_shapes(element.val, ctx=ctx)


@late_compile_view_shapes.register(irast.Array)
def _late_compile_view_shapes_in_array(
    expr: irast.Array,
    *,
    rptr: Optional[irast.Pointer] = None,
    parent_view_type: Optional[s_types.ExprType] = None,
    ctx: context.ContextLevel,
) -> None:
    for element in expr.elements:
        late_compile_view_shapes(element, ctx=ctx)


def _record_created_collection_types(
    type: s_types.Type, ctx: context.ContextLevel
) -> None:
    """
    Record references to implicitly defined collection types,
    so that the alias delta machinery can pick them up.
    """

    if isinstance(
        type, s_types.Collection
    ) and not ctx.env.orig_schema.get_by_id(type.id, default=None):
        for sub_type in type.get_subtypes(ctx.env.schema):
            _record_created_collection_types(sub_type, ctx)
