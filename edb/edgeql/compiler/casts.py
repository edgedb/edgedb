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


"""EdgeQL compiler routines for type casts."""


from __future__ import annotations

from typing import *

from edb import errors

from edb.common import parsing

from edb.ir import ast as irast
from edb.ir import utils as irutils

from edb.schema import casts as s_casts
from edb.schema import functions as s_func
from edb.schema import modules as s_mod
from edb.schema import objects as s_objects
from edb.schema import types as s_types

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes as ft

from . import context
from . import dispatch
from . import pathctx
from . import polyres
from . import setgen
from . import typegen
from . import viewgen

if TYPE_CHECKING:
    from edb.schema import schema as s_schema


def compile_cast(
        ir_expr: Union[irast.Set, irast.Expr],
        new_stype: s_types.Type, *,
        srcctx: Optional[parsing.ParserContext],
        ctx: context.ContextLevel) -> irast.Set:

    if isinstance(ir_expr, irast.EmptySet):
        # For the common case of casting an empty set, we simply
        # generate a new EmptySet node of the requested type.
        return setgen.new_empty_set(
            stype=new_stype,
            alias=ir_expr.path_id.target_name_hint.name,
            ctx=ctx,
            srcctx=ir_expr.context)

    elif irutils.is_untyped_empty_array_expr(ir_expr):
        # Ditto for empty arrays.
        new_typeref = typegen.type_to_typeref(new_stype, ctx.env)
        return setgen.ensure_set(
            irast.Array(elements=[], typeref=new_typeref), ctx=ctx)

    ir_set = setgen.ensure_set(ir_expr, ctx=ctx)
    orig_stype = setgen.get_set_type(ir_set, ctx=ctx)

    if orig_stype == new_stype:
        return ir_set
    elif orig_stype.is_object_type() and new_stype.is_object_type():
        # Object types cannot be cast between themselves,
        # as cast is a _constructor_ operation, and the only
        # valid way to construct an object is to INSERT it.
        raise errors.QueryError(
            f'cannot cast object type '
            f'{orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}, use '
            f'`...[IS {new_stype.get_displayname(ctx.env.schema)}]` instead',
            context=srcctx)

    if isinstance(ir_set.expr, irast.Array):
        return _cast_array_literal(
            ir_set, orig_stype, new_stype, srcctx=srcctx, ctx=ctx)

    elif orig_stype.is_tuple(ctx.env.schema):
        return _cast_tuple(
            ir_set, orig_stype, new_stype, srcctx=srcctx, ctx=ctx)

    elif orig_stype.issubclass(ctx.env.schema, new_stype):
        # The new type is a supertype of the old type,
        # and is always a wider domain, so we simply reassign
        # the stype.
        return _inheritance_cast_to_ir(
            ir_set, orig_stype, new_stype, ctx=ctx)

    elif new_stype.issubclass(ctx.env.schema, orig_stype):
        # The new type is a subtype, so may potentially have
        # a more restrictive domain, generate a cast call.
        return _inheritance_cast_to_ir(
            ir_set, orig_stype, new_stype, ctx=ctx)

    elif orig_stype.is_array():
        return _cast_array(
            ir_set, orig_stype, new_stype, srcctx=srcctx, ctx=ctx)

    else:
        json_t = cast(
            s_objects.InheritingObject,
            ctx.env.get_track_schema_object('std::json'),
        )
        if (new_stype.issubclass(ctx.env.schema, json_t) and
                ir_set.path_id.is_objtype_path()):
            # JSON casts of objects are special: we want the full shape
            # and not just an identity.
            with ctx.new() as subctx:
                subctx.implicit_id_in_shapes = False
                subctx.implicit_tid_in_shapes = False
                viewgen.compile_view_shapes(ir_set, ctx=subctx)

        return _compile_cast(
            ir_expr, orig_stype, new_stype, srcctx=srcctx, ctx=ctx)


def _compile_cast(
        ir_expr: Union[irast.Set, irast.Expr],
        orig_stype: s_types.Type,
        new_stype: s_types.Type, *,
        srcctx: Optional[parsing.ParserContext],
        ctx: context.ContextLevel) -> irast.Set:

    ir_set = setgen.ensure_set(ir_expr, ctx=ctx)
    cast = _find_cast(orig_stype, new_stype, srcctx=srcctx, ctx=ctx)

    if cast is None:
        raise errors.QueryError(
            f'cannot cast '
            f'{orig_stype.get_displayname(ctx.env.schema)!r} to '
            f'{new_stype.get_displayname(ctx.env.schema)!r}',
            context=srcctx or ir_set.context)

    return _cast_to_ir(ir_set, cast, orig_stype, new_stype, ctx=ctx)


def _cast_to_ir(
        ir_set: irast.Set,
        cast: s_casts.Cast,
        orig_stype: s_types.Type,
        new_stype: s_types.Type, *,
        ctx: context.ContextLevel) -> irast.Set:

    orig_typeref = typegen.type_to_typeref(orig_stype, env=ctx.env)
    new_typeref = typegen.type_to_typeref(new_stype, env=ctx.env)
    cast_name = cast.get_name(ctx.env.schema)
    cast_ir = irast.TypeCast(
        expr=ir_set,
        from_type=orig_typeref,
        to_type=new_typeref,
        cast_name=cast_name,
        cast_module_id=ctx.env.schema.get_global(
            s_mod.Module, cast_name.module).id,
        sql_function=cast.get_from_function(ctx.env.schema),
        sql_cast=cast.get_from_cast(ctx.env.schema),
        sql_expr=bool(cast.get_code(ctx.env.schema)),
    )

    return setgen.ensure_set(cast_ir, ctx=ctx)


def _inheritance_cast_to_ir(
        ir_set: irast.Set,
        orig_stype: s_types.Type,
        new_stype: s_types.Type, *,
        ctx: context.ContextLevel) -> irast.Set:

    orig_typeref = typegen.type_to_typeref(orig_stype, env=ctx.env)
    new_typeref = typegen.type_to_typeref(new_stype, env=ctx.env)
    cast_ir = irast.TypeCast(
        expr=ir_set,
        from_type=orig_typeref,
        to_type=new_typeref,
        cast_name=None,
        sql_function=None,
        sql_cast=True,
        sql_expr=False,
    )

    return setgen.ensure_set(cast_ir, ctx=ctx)


class CastParamListWrapper(s_func.ParameterLikeList):

    def __init__(self, params: Iterable[s_func.ParameterDesc]) -> None:
        self._params = tuple(params)

    def get_by_name(
        self,
        schema: s_schema.Schema,
        name: str,
    ) -> s_func.ParameterDesc:
        raise NotImplementedError

    def as_str(self, schema: s_schema.Schema) -> str:
        raise NotImplementedError

    def find_named_only(
        self,
        schema: s_schema.Schema,
    ) -> Mapping[str, s_func.ParameterDesc]:
        return {}

    def find_variadic(
        self,
        schema: s_schema.Schema,
    ) -> Optional[s_func.ParameterDesc]:
        return None

    def has_polymorphic(
        self,
        schema: s_schema.Schema,
    ) -> bool:
        return False

    def objects(
        self,
        schema: s_schema.Schema,
    ) -> Tuple[s_func.ParameterDesc, ...]:
        return self._params

    def has_required_params(self, schema: s_schema.Schema) -> bool:
        return True

    def get_in_canonical_order(
        self,
        schema: s_schema.Schema,
    ) -> Tuple[s_func.ParameterDesc, ...]:
        return self._params


class CastCallableWrapper(s_func.CallableLike):
    # A wrapper around a cast object to make it quack like a callable
    # for the purposes of polymorphic resolution.
    def __init__(self, cast: s_casts.Cast) -> None:
        self._cast = cast

    def has_inlined_defaults(self, schema: s_schema.Schema) -> bool:
        return False

    def get_params(
        self,
        schema: s_schema.Schema,
    ) -> s_func.ParameterLikeList:
        from_type_param = s_func.ParameterDesc(
            num=0,
            name='val',
            type=self._cast.get_from_type(schema).as_shell(schema),
            typemod=ft.TypeModifier.SINGLETON,
            kind=ft.ParameterKind.POSITIONAL,
            default=None,
        )

        to_type_param = s_func.ParameterDesc(
            num=0,
            name='_',
            type=self._cast.get_to_type(schema).as_shell(schema),
            typemod=ft.TypeModifier.SINGLETON,
            kind=ft.ParameterKind.POSITIONAL,
            default=None,
        )

        return CastParamListWrapper((from_type_param, to_type_param))

    def get_return_type(self, schema: s_schema.Schema) -> s_types.Type:
        return self._cast.get_to_type(schema)

    def get_return_typemod(self, schema: s_schema.Schema) -> ft.TypeModifier:
        return ft.TypeModifier.SINGLETON

    def get_verbosename(self, schema: s_schema.Schema) -> str:
        return self._cast.get_verbosename(schema)

    def get_is_abstract(self, schema: s_schema.Schema) -> bool:
        return False


def _find_cast(
        orig_stype: s_types.Type,
        new_stype: s_types.Type, *,
        srcctx: Optional[parsing.ParserContext],
        ctx: context.ContextLevel) -> Optional[s_casts.Cast]:

    casts = ctx.env.schema.get_casts_to_type(new_stype)
    if not casts and isinstance(new_stype, s_types.InheritingType):
        ancestors = new_stype.get_ancestors(ctx.env.schema)
        for t in ancestors.objects(ctx.env.schema):
            casts = ctx.env.schema.get_casts_to_type(t)
            if casts:
                break
        else:
            return None

    args = [
        (orig_stype, irast.EmptySet()),
        (new_stype, irast.EmptySet()),
    ]

    matched = polyres.find_callable(
        (CastCallableWrapper(c) for c in casts), args=args, kwargs={}, ctx=ctx)

    if len(matched) == 1:
        return cast(CastCallableWrapper, matched[0].func)._cast
    elif len(matched) > 1:
        raise errors.QueryError(
            f'cannot unambiguously cast '
            f'{orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}',
            context=srcctx)
    else:
        return None


def _cast_tuple(
        ir_set: irast.Set,
        orig_stype: s_types.Type,
        new_stype: s_types.Type, *,
        srcctx: Optional[parsing.ParserContext],
        ctx: context.ContextLevel) -> irast.Set:

    assert isinstance(orig_stype, s_types.Tuple)

    # Make sure the source tuple expression is pinned in the scope,
    # so that we don't generate a cross-product of it by evaluating
    # the tuple indirections.
    pathctx.register_set_in_scope(ir_set, ctx=ctx)

    direct_cast = _find_cast(orig_stype, new_stype, srcctx=srcctx, ctx=ctx)
    orig_subtypes = dict(orig_stype.iter_subtypes(ctx.env.schema))

    if direct_cast is not None:
        # Direct casting to non-tuple involves casting each tuple
        # element and also keeping the cast around the whole tuple.
        # This is to trigger the downstream logic of casting
        # objects (in elements of the tuple).
        elements = []
        for n in orig_subtypes:
            val = setgen.tuple_indirection_set(
                ir_set,
                source=orig_stype,
                ptr_name=n,
                ctx=ctx,
            )
            val_type = setgen.get_set_type(val, ctx=ctx)
            # Element cast
            val = compile_cast(val, new_stype, ctx=ctx, srcctx=srcctx)

            elements.append(irast.TupleElement(name=n, val=val))

        new_tuple = setgen.new_tuple_set(
            elements,
            named=orig_stype.is_named(ctx.env.schema),
            ctx=ctx,
        )

        return _cast_to_ir(
            new_tuple, direct_cast, orig_stype, new_stype, ctx=ctx)

    if not new_stype.is_tuple(ctx.env.schema):
        raise errors.QueryError(
            f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}',
            context=srcctx)

    assert isinstance(new_stype, s_types.Tuple)
    new_subtypes = list(new_stype.iter_subtypes(ctx.env.schema))
    if len(orig_subtypes) != len(new_subtypes):
        raise errors.QueryError(
            f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}: '
            f'the number of elements is not the same',
            context=srcctx)

    # For tuple-to-tuple casts we generate a new tuple
    # to simplify things on sqlgen side.
    elements = []
    for i, n in enumerate(orig_subtypes):
        val = setgen.tuple_indirection_set(
            ir_set,
            source=orig_stype,
            ptr_name=n,
            ctx=ctx,
        )
        val_type = setgen.get_set_type(val, ctx=ctx)
        new_el_name, new_st = new_subtypes[i]
        if val_type != new_st:
            # Element cast
            val = compile_cast(val, new_st, ctx=ctx, srcctx=srcctx)

        elements.append(irast.TupleElement(name=new_el_name, val=val))

    return setgen.new_tuple_set(
        elements,
        named=new_stype.is_named(ctx.env.schema),
        ctx=ctx,
    )


def _cast_array(
        ir_set: irast.Set,
        orig_stype: s_types.Type,
        new_stype: s_types.Type, *,
        srcctx: Optional[parsing.ParserContext],
        ctx: context.ContextLevel) -> irast.Set:

    assert isinstance(orig_stype, s_types.Array)

    direct_cast = _find_cast(orig_stype, new_stype, srcctx=srcctx, ctx=ctx)

    if direct_cast is None:
        if not new_stype.is_array():
            raise errors.QueryError(
                f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
                f'to {new_stype.get_displayname(ctx.env.schema)!r}',
                context=srcctx)
        assert isinstance(new_stype, s_types.Array)
        el_type = new_stype.get_subtypes(ctx.env.schema)[0]
    else:
        el_type = new_stype

    orig_el_type = orig_stype.get_subtypes(ctx.env.schema)[0]

    el_cast = _find_cast(orig_el_type, el_type, srcctx=srcctx, ctx=ctx)

    if el_cast is not None and el_cast.get_from_cast(ctx.env.schema):
        # Simple cast
        return _cast_to_ir(
            ir_set, el_cast, orig_stype, new_stype, ctx=ctx)
    else:
        pathctx.register_set_in_scope(ir_set, ctx=ctx)

        with ctx.new() as subctx:
            subctx.anchors = subctx.anchors.copy()
            source_alias = subctx.aliases.get('a')
            subctx.anchors[source_alias] = ir_set

            unpacked = qlast.FunctionCall(
                func=('std', 'array_unpack'),
                args=[
                    qlast.Path(
                        steps=[qlast.ObjectRef(name=source_alias)],
                    ),
                ],
            )

            enumerated = setgen.ensure_set(
                dispatch.compile(
                    qlast.FunctionCall(
                        func=('std', 'enumerate'),
                        args=[unpacked],
                    ),
                    ctx=subctx,
                ),
                ctx=subctx,
            )

            enumerated_alias = subctx.aliases.get('e')
            subctx.anchors[enumerated_alias] = enumerated
            enumerated_ref = qlast.Path(
                steps=[qlast.ObjectRef(name=enumerated_alias)],
            )

            elements = qlast.FunctionCall(
                func=('std', 'array_agg'),
                args=[
                    qlast.SelectQuery(
                        result=qlast.TypeCast(
                            expr=qlast.Path(
                                steps=[
                                    enumerated_ref,
                                    qlast.Ptr(
                                        ptr=qlast.ObjectRef(
                                            name='1',
                                            direction='>',
                                        ),
                                    ),
                                ],
                            ),
                            type=typegen.type_to_ql_typeref(
                                el_type,
                                ctx=subctx,
                            ),
                        ),
                        orderby=[
                            qlast.SortExpr(
                                path=qlast.Path(
                                    steps=[
                                        enumerated_ref,
                                        qlast.Ptr(
                                            ptr=qlast.ObjectRef(
                                                name='0',
                                                direction='>',
                                            ),
                                        ),
                                    ],
                                ),
                                direction=qlast.SortOrder.Asc,
                            ),
                        ],
                    ),
                ],
            )

            array_ir = dispatch.compile(elements, ctx=subctx)
            assert isinstance(array_ir, irast.Set)

            if direct_cast is not None:
                ctx.env.schema, array_stype = s_types.Array.from_subtypes(
                    ctx.env.schema, [el_type])
                return _cast_to_ir(
                    array_ir, direct_cast, array_stype, new_stype, ctx=ctx
                )
            else:
                return array_ir


def _cast_array_literal(
        ir_set: irast.Set,
        orig_stype: s_types.Type,
        new_stype: s_types.Type, *,
        srcctx: Optional[parsing.ParserContext],
        ctx: context.ContextLevel) -> irast.Set:

    assert isinstance(ir_set.expr, irast.Array)

    orig_typeref = typegen.type_to_typeref(orig_stype, env=ctx.env)
    new_typeref = typegen.type_to_typeref(new_stype, env=ctx.env)
    direct_cast = _find_cast(orig_stype, new_stype, srcctx=srcctx, ctx=ctx)

    if direct_cast is None:
        if not new_stype.is_array():
            raise errors.QueryError(
                f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
                f'to {new_stype.get_displayname(ctx.env.schema)!r}',
                context=srcctx) from None
        assert isinstance(new_stype, s_types.Array)
        el_type = new_stype.get_subtypes(ctx.env.schema)[0]
    else:
        el_type = new_stype

    casted_els = []
    for el in ir_set.expr.elements:
        el = compile_cast(el, el_type, ctx=ctx, srcctx=srcctx)
        casted_els.append(el)

    new_array = setgen.ensure_set(
        irast.Array(elements=casted_els, typeref=orig_typeref),
        ctx=ctx)

    if direct_cast is not None:
        return _cast_to_ir(
            new_array, direct_cast, orig_stype, new_stype, ctx=ctx)

    else:
        cast_ir = irast.TypeCast(
            expr=new_array,
            from_type=orig_typeref,
            to_type=new_typeref,
            sql_cast=True,
        )

    return setgen.ensure_set(cast_ir, ctx=ctx)
