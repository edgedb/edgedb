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


"""CONFIGURE statement compilation functions."""


import typing

from edb import errors

from edb.ir import ast as irast
from edb.ir import staeval as ireval
from edb.ir import typeutils as irtyputils

from edb.schema import links as s_links
from edb.schema import types as s_types

from edb.edgeql import ast as qlast

from . import context
from . import dispatch
from . import setgen

from .inference import cardinality as card_inference


@dispatch.compile.register
def compile_ConfigSet(
        expr: qlast.ConfigSet, *, ctx: context.ContextLevel) -> irast.Set:

    param_name, _ = _validate_op(expr, ctx=ctx)

    return irast.ConfigSet(
        name=param_name,
        system=expr.system,
        context=expr.context,
        expr=dispatch.compile(expr.expr, ctx=ctx),
    )


@dispatch.compile.register
def compile_ConfigReset(
        expr: qlast.ConfigReset, *, ctx: context.ContextLevel) -> irast.Set:
    param_name, param_type = _validate_op(expr, ctx=ctx)
    filter_properties = []
    filter_expr = expr.where

    if not param_type.is_object_type() and filter_expr is not None:
        raise errors.QueryError(
            'RESET of a primitive configuration parameter '
            'must not have a FILTER clause',
            context=expr.context,
        )

    elif param_type.is_object_type():
        if filter_expr is None:
            raise errors.QueryError(
                'RESET of a composite configuration parameter '
                'must have a FILTER clause',
                context=expr.context,
            )

        param_type_name = param_type.get_name(ctx.env.schema)
        select = qlast.SelectQuery(
            result=qlast.Path(steps=[
                qlast.ObjectRef(name=param_type_name.name,
                                module=param_type_name.module)
            ]),
            where=filter_expr,
        )

        env = ctx.env

        ctx.modaliases[None] = 'cfg'
        select_ir = dispatch.compile(select, ctx=ctx)

        filters = card_inference.extract_filters(
            select_ir, select_ir.expr.where,
            scope_tree=ctx.path_scope, env=env)

        exclusive_constr = ctx.env.schema.get('std::exclusive')
        for ptr, value in filters:
            is_exclusive = any(
                c.issubclass(env.schema, exclusive_constr)
                for c in ptr.get_constraints(env.schema).objects(env.schema)
            )

            if is_exclusive:
                filter_properties.append(
                    irast.ConfigFilter(
                        property_name=ptr.get_shortname(env.schema).name,
                        value=value,
                    )
                )
                break

        if not filter_properties:
            raise errors.QueryError(
                'the FILTER clause of a RESET of a composite configuration '
                'parameter must include an equality check against '
                'at least one exclusive property',
                context=expr.context,
            )

    return irast.ConfigReset(
        name=param_name,
        filter_properties=filter_properties,
        system=expr.system,
        context=expr.context,
    )


@dispatch.compile.register
def compile_ConfigInsert(
        expr: qlast.ConfigInsert, *, ctx: context.ContextLevel) -> irast.Set:

    param_name, _ = _validate_op(expr, ctx=ctx)

    if not expr.system:
        raise errors.UnsupportedFeatureError(
            f'CONFIGURE SESSION INSERT is not supported'
        )

    level = 'SYSTEM' if expr.system else 'SESSION'
    schema = ctx.env.schema
    subject = schema.get(f'cfg::{expr.name.name}', None)
    if subject is None:
        raise errors.ConfigurationError(
            f'{expr.name.name!r} is not a valid configuration item',
            context=expr.context,
        )

    insert_stmt = qlast.InsertQuery(
        subject=qlast.Path(
            steps=[
                qlast.ObjectRef(
                    name=expr.name.name,
                    module='cfg',
                )
            ]
        ),
        shape=expr.shape,
    )

    for el in expr.shape:
        if isinstance(el.compexpr, qlast.InsertQuery):
            _inject_tname(el.compexpr, ctx=ctx)

    with ctx.newscope() as subctx:
        subctx.expr_exposed = True
        subctx.modaliases = ctx.modaliases.copy()
        subctx.modaliases[None] = 'cfg'
        subctx.special_computables_in_mutation_shape |= {'__tname__'}
        insert_ir = dispatch.compile(insert_stmt, ctx=subctx)
        insert_subject = insert_ir.expr.subject

        _validate_config_object(insert_subject, level=level, ctx=subctx)

    return setgen.ensure_set(
        irast.ConfigInsert(
            name=param_name,
            system=expr.system,
            expr=insert_subject,
            context=expr.context,
        ),
        ctx=ctx,
    )


def _inject_tname(
        insert_stmt: qlast.InsertQuery, *,
        ctx: context.ContextLevel) -> None:

    for el in insert_stmt.shape:
        if isinstance(el.compexpr, qlast.InsertQuery):
            _inject_tname(el.compexpr, ctx=ctx)

    insert_stmt.shape.append(
        qlast.ShapeElement(
            expr=qlast.Path(
                steps=[qlast.Ptr(ptr=qlast.ObjectRef(name='__tname__'))],
            ),
            compexpr=qlast.Path(
                steps=[
                    qlast.Introspect(
                        type=qlast.TypeName(
                            maintype=insert_stmt.subject.steps[0],
                        ),
                    ),
                    qlast.Ptr(ptr=qlast.ObjectRef(name='name')),
                ],
            ),
        ),
    )


def _validate_config_object(
        expr: irast.Set, *,
        level: str,
        ctx: context.ContextLevel) -> None:

    for element in expr.shape:
        if element.rptr.ptrref.shortname.name == 'id':
            continue

        if (irtyputils.is_object(element.typeref)
                and isinstance(element.expr, irast.InsertStmt)):
            _validate_config_object(element, level=level, ctx=ctx)
        else:
            try:
                ireval.evaluate(element, schema=ctx.env.schema)
            except ireval.UnsupportedExpressionError:
                raise errors.QueryError(
                    f'non-constant expression in CONFIGURE {level} INSERT',
                    context=element.context
                ) from None


def _validate_op(
        expr: qlast.ConfigOp, *,
        ctx: context.ContextLevel) -> typing.Tuple[str, s_types.Type]:

    if expr.name.module and expr.name.module != 'cfg':
        raise errors.QueryError(
            'invalid configuration parameter name: module must be either '
            '\'cfg\' or empty', context=expr.name.context,
        )

    name = expr.name.name
    cfg_host_type = ctx.env.schema.get('cfg::Config')
    cfg_type = None

    if isinstance(expr, (qlast.ConfigSet, qlast.ConfigReset)):
        # expr.name is the actual name of the property.
        ptr = cfg_host_type.getptr(ctx.env.schema, name)
        if ptr is not None:
            cfg_type = ptr.get_target(ctx.env.schema)

    if cfg_type is None:
        if isinstance(expr, qlast.ConfigSet):
            raise errors.ConfigurationError(
                f'unrecognized configuration parameter {name!r}',
                context=expr.context
            )

        # expr.name is the name of the configuration type
        cfg_type = ctx.env.schema.get(f'cfg::{name}', None)
        if cfg_type is None:
            raise errors.ConfigurationError(
                f'unrecognized configuration object {name!r}',
                context=expr.context
            )

        ptr = None

        mro = [cfg_type] + list(
            cfg_type.get_mro(ctx.env.schema).objects(ctx.env.schema))
        for ct in mro:
            ptrs = ctx.env.schema.get_referrers(
                ct, scls_type=s_links.Link, field_name='target')

            if ptrs:
                ptr = next(iter(ptrs))
                break

        if ptr is None or ptr.get_source(ctx.env.schema) != cfg_host_type:
            raise errors.ConfigurationError(
                f'{name!r} cannot be configured directly'
            )

        name = ptr.get_shortname(ctx.env.schema).name

    sys_attr = ptr.get_attributes(ctx.env.schema).get(
        ctx.env.schema, 'cfg::system', None)

    system = (
        sys_attr is not None
        and sys_attr.get_value(ctx.env.schema) == 'true'
    )

    if not expr.system and system:
        raise errors.ConfigurationError(
            f'{name!r} is a system-level configuration parameter; '
            f'use "CONFIGURE SYSTEM"')

    return name, cfg_type
