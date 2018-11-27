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


import typing

from edb import errors

from edb.lang.common import parsing

from edb.lang.ir import ast as irast
from edb.lang.ir import utils as irutils

from edb.lang.schema import casts as s_casts
from edb.lang.schema import functions as s_func
from edb.lang.schema import types as s_types

from edb.lang.edgeql import functypes as ft

from . import astutils
from . import context
from . import inference
from . import polyres
from . import setgen
from . import viewgen


def compile_cast(
        ir_expr: irast.Base, new_stype: s_types.Type, *,
        srcctx: parsing.ParserContext,
        ctx: context.ContextLevel) -> irast.OperatorCall:

    if isinstance(ir_expr, irast.EmptySet):
        # For the common case of casting an empty set, we simply
        # generate a new EmptySet node of the requested type.
        return irutils.new_empty_set(ctx.env.schema, stype=new_stype,
                                     alias=ir_expr.path_id.target_name.name)

    elif irutils.is_untyped_empty_array_expr(ir_expr):
        # Ditto for empty arrays.
        return setgen.generated_set(
            irast.Array(elements=[], stype=new_stype), ctx=ctx)

    ir_set = setgen.ensure_set(ir_expr, ctx=ctx)
    orig_stype = ir_set.stype

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

    elif orig_stype.is_tuple():
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
        json_t = ctx.env.schema.get('std::json')

        if (new_stype.issubclass(ctx.env.schema, json_t) and
                ir_set.path_id.is_objtype_path()):
            # JSON casts of objects are special: we want the full shape
            # and not just an identity.
            viewgen.compile_view_shapes(ir_set, ctx=ctx)

        return _compile_cast(
            ir_expr, orig_stype, new_stype, srcctx=srcctx, ctx=ctx)


def _compile_cast(
        ir_expr: irast.Base,
        orig_stype: s_types.Type,
        new_stype: s_types.Type, *,
        srcctx: parsing.ParserContext,
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

    orig_typeref = irutils.type_to_typeref(ctx.env.schema, orig_stype)
    new_typeref = irutils.type_to_typeref(ctx.env.schema, new_stype)
    cast_ir = irast.TypeCast(
        expr=ir_set,
        from_type=orig_typeref,
        to_type=new_typeref,
        cast_name=cast.get_name(ctx.env.schema),
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

    orig_typeref = irutils.type_to_typeref(ctx.env.schema, orig_stype)
    new_typeref = irutils.type_to_typeref(ctx.env.schema, new_stype)
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


class CastParamListWrapper(list):
    def find_named_only(self, schema):
        return {}

    def find_variadic(self, schema):
        return None

    def has_polymorphic(self, schema):
        return False


class CastCallableWrapper:
    # A wrapper around a cast object to make it quack like a callable
    # for the purposes of polymorphic resolution.
    def __init__(self, cast):
        self._cast = cast

    def has_inlined_defaults(self, schema):
        return False

    def get_params(self, schema):
        from_type_param = s_func.ParameterDesc(
            num=0, name='val', type=self._cast.get_from_type(schema),
            typemod=ft.TypeModifier.SINGLETON,
            kind=ft.ParameterKind.POSITIONAL,
            default=None,
        )

        to_type_param = s_func.ParameterDesc(
            num=0, name='_', type=self._cast.get_to_type(schema),
            typemod=ft.TypeModifier.SINGLETON,
            kind=ft.ParameterKind.POSITIONAL,
            default=None,
        )

        return CastParamListWrapper([from_type_param, to_type_param])

    def get_return_type(self, schema):
        return self._cast.get_to_type(schema)


def _find_cast(
        orig_stype: s_types.Type,
        new_stype: s_types.Type, *,
        srcctx: parsing.ParserContext,
        ctx: context.ContextLevel) -> typing.Optional[s_casts.Cast]:

    casts = ctx.env.schema.get_casts_to_type(new_stype)
    if not casts:
        return None

    args = [
        (orig_stype, None),
        (new_stype, None),
    ]

    matched = polyres.find_callable(
        (CastCallableWrapper(c) for c in casts), args=args, kwargs={}, ctx=ctx)

    if len(matched) == 1:
        return matched[0].func._cast
    elif len(matched) > 1:
        raise errors.QueryError(
            f'cannot unambiguously cast '
            f'{orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}',
            context=srcctx)
    else:
        return None


def _cast_tuple(
        ir_set: irast.Base,
        orig_stype: s_types.Type,
        new_stype: s_types.Type, *,
        srcctx: parsing.ParserContext,
        ctx: context.ContextLevel) -> irast.Base:

    direct_cast = _find_cast(orig_stype, new_stype, srcctx=srcctx, ctx=ctx)

    if direct_cast is not None:
        # Direct casting to non-tuple involves casting each tuple
        # element and also keeping the cast around the whole tuple.
        # This is to trigger the downstream logic of casting
        # objects (in elements of the tuple).
        elements = []
        for i, n in enumerate(orig_stype.element_types):
            val = setgen.generated_set(
                irast.TupleIndirection(
                    expr=ir_set,
                    name=n
                ),
                ctx=ctx
            )
            val.path_id = irutils.tuple_indirection_path_id(
                ir_set.path_id, n, orig_stype.element_types[n],
                schema=ctx.env.schema)

            val_type = inference.infer_type(val, ctx.env)
            # Element cast
            val = compile_cast(val, new_stype, ctx=ctx, srcctx=srcctx)

            elements.append(irast.TupleElement(name=n, val=val))

        new_tuple = setgen.ensure_set(
            astutils.make_tuple(elements, named=orig_stype.named, ctx=ctx),
            ctx=ctx
        )

        return _cast_to_ir(
            new_tuple, direct_cast, orig_stype, new_stype, ctx=ctx)

    if not new_stype.is_tuple():
        raise errors.QueryError(
            f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}',
            context=srcctx)

    if len(orig_stype.element_types) != len(new_stype.element_types):
        raise errors.QueryError(
            f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}: ',
            f'the number of elements is not the same',
            context=srcctx)

    # For tuple-to-tuple casts we generate a new tuple
    # to simplify things on sqlgen side.
    new_names = list(new_stype.element_types)

    elements = []
    for i, n in enumerate(orig_stype.element_types):
        val = setgen.generated_set(
            irast.TupleIndirection(
                expr=ir_set,
                name=n
            ),
            ctx=ctx
        )
        val.path_id = irutils.tuple_indirection_path_id(
            ir_set.path_id, n, orig_stype.element_types[n],
            schema=ctx.env.schema)

        val_type = inference.infer_type(val, ctx.env)
        new_el_name = new_names[i]
        new_subtypes = list(new_stype.get_subtypes())
        if val_type != new_stype.element_types[new_el_name]:
            # Element cast
            val = compile_cast(
                val, new_subtypes[i], ctx=ctx, srcctx=srcctx)

        elements.append(irast.TupleElement(name=new_el_name, val=val))

    return setgen.ensure_set(astutils.make_tuple(
        named=new_stype.named, elements=elements, ctx=ctx), ctx=ctx)


def _cast_array(
        ir_set: irast.Set,
        orig_stype: s_types.Type,
        new_stype: s_types.Type, *,
        srcctx: parsing.ParserContext,
        ctx: context.ContextLevel) -> irast.Base:

    direct_cast = _find_cast(orig_stype, new_stype, srcctx=srcctx, ctx=ctx)

    if direct_cast is None:
        if not new_stype.is_array():
            raise errors.QueryError(
                f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
                f'to {new_stype.get_displayname(ctx.env.schema)!r}',
                context=srcctx)
        el_type = new_stype.get_subtypes()[0]
    else:
        el_type = new_stype

    orig_el_type = orig_stype.get_subtypes()[0]

    el_cast = _find_cast(orig_el_type, el_type, srcctx=srcctx, ctx=ctx)
    if el_cast is None:
        raise errors.QueryError(
            f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}',
            context=srcctx) from None

    if el_cast.get_from_cast(ctx.env.schema):
        # Simple cast
        return _cast_to_ir(
            ir_set, direct_cast, orig_stype, new_stype, ctx=ctx)
    else:
        # Functional cast, need to apply element-wise.
        raise errors.QueryError(
            f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
            f'to {new_stype.get_displayname(ctx.env.schema)!r}: '
            f'non-trivial array casts are not implemented',
            context=srcctx) from None


def _cast_array_literal(
        ir_set: irast.Set,
        orig_stype: s_types.Type,
        new_stype: s_types.Type, *,
        srcctx: parsing.ParserContext,
        ctx: context.ContextLevel) -> irast.Base:

    orig_typeref = irutils.type_to_typeref(ctx.env.schema, orig_stype)
    new_typeref = irutils.type_to_typeref(ctx.env.schema, new_stype)

    direct_cast = _find_cast(orig_stype, new_stype, srcctx=srcctx, ctx=ctx)

    if direct_cast is None:
        if not new_stype.is_array():
            raise errors.QueryError(
                f'cannot cast {orig_stype.get_displayname(ctx.env.schema)!r} '
                f'to {new_stype.get_displayname(ctx.env.schema)!r}',
                context=srcctx) from None
        el_type = new_stype.get_subtypes()[0]
    else:
        el_type = new_stype

    casted_els = []
    for el in ir_set.expr.elements:
        el = compile_cast(el, el_type, ctx=ctx, srcctx=srcctx)
        casted_els.append(el)

    new_array = setgen.generated_set(
        irast.Array(elements=casted_els, stype=orig_stype),
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
