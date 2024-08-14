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


from __future__ import annotations
from typing import Optional, NamedTuple

import json

from edb import errors

from edb.edgeql import qltypes

from edb.ir import ast as irast
from edb.ir import staeval as ireval
from edb.ir import statypes as statypes
from edb.ir import typeutils as irtyputils

from edb.schema import constraints as s_constr
from edb.schema import globals as s_globals
from edb.schema import links as s_links
from edb.schema import name as sn
from edb.schema import objtypes as s_objtypes
from edb.schema import pointers as s_pointers
from edb.schema import types as s_types
from edb.schema import utils as s_utils
from edb.schema import expr as s_expr

from edb.edgeql import ast as qlast

from . import casts
from . import context
from . import dispatch
from . import setgen
from . import typegen


class SettingInfo(NamedTuple):
    param_name: str
    param_type: s_types.Type
    cardinality: qltypes.SchemaCardinality
    required: bool
    requires_restart: bool
    backend_setting: str | None
    affects_compilation: bool
    is_system_config: bool
    ptr: Optional[s_pointers.Pointer]


@dispatch.compile.register
def compile_ConfigSet(
    expr: qlast.ConfigSet,
    *,
    ctx: context.ContextLevel,
) -> irast.Set:

    info = _validate_op(expr, ctx=ctx)
    param_val = dispatch.compile(expr.expr, ctx=ctx)
    param_type = info.param_type
    val_type = setgen.get_set_type(param_val, ctx=ctx)
    compatible = s_types.is_type_compatible(
        val_type, param_type, schema=ctx.env.schema)
    if not compatible:
        if not val_type.assignment_castable_to(param_type, ctx.env.schema):
            raise errors.ConfigurationError(
                f'invalid setting value type for {info.param_name}: '
                f'{val_type.get_displayname(ctx.env.schema)!r} '
                f'(expecting {param_type.get_displayname(ctx.env.schema)!r})'
            )
        else:
            param_val = casts.compile_cast(
                param_val, param_type, span=None, ctx=ctx)

    try:
        if expr.scope != qltypes.ConfigScope.GLOBAL:
            val = ireval.evaluate_to_python_val(
                param_val, schema=ctx.env.schema)
        else:
            val = None
    except ireval.UnsupportedExpressionError as e:
        raise errors.QueryError(
            f'non-constant expression in CONFIGURE {expr.scope} SET',
            span=expr.expr.span
        ) from e
    else:
        if isinstance(val, statypes.ScalarType) and info.backend_setting:
            backend_expr = dispatch.compile(
                qlast.Constant.string(val.to_backend_str()),
                ctx=ctx,
            )
        else:
            backend_expr = None

    if info.ptr:
        _enforce_pointer_constraints(
            info.ptr, param_val, ctx=ctx, for_obj=False)

    config_set = irast.ConfigSet(
        name=info.param_name,
        cardinality=info.cardinality,
        required=info.required,
        scope=expr.scope,
        requires_restart=info.requires_restart,
        backend_setting=info.backend_setting,
        is_system_config=info.is_system_config,
        span=expr.span,
        expr=param_val,
        backend_expr=backend_expr,
    )
    return setgen.ensure_set(config_set, ctx=ctx)


@dispatch.compile.register
def compile_ConfigReset(
    expr: qlast.ConfigReset,
    *,
    ctx: context.ContextLevel,
) -> irast.Set:

    info = _validate_op(expr, ctx=ctx)
    filter_expr = expr.where
    select_ir = None

    if not info.param_type.is_object_type() and filter_expr is not None:
        raise errors.QueryError(
            'RESET of a primitive configuration parameter '
            'must not have a FILTER clause',
            span=expr.span,
        )

    elif isinstance(info.param_type, s_objtypes.ObjectType):
        param_type_name = info.param_type.get_name(ctx.env.schema)
        param_type_ref = qlast.ObjectRef(
            name=param_type_name.name,
            module=param_type_name.module,
        )
        body = qlast.Shape(
            expr=qlast.Path(steps=[param_type_ref]),
            elements=s_utils.get_config_type_shape(
                ctx.env.schema, info.param_type, path=[param_type_ref]),
        )
        # The body needs to have access to secrets, since they get put
        # into the shape and are necessary for compiling the deletion
        # code, so compile the body in a way that we allow it.
        # The filter should *not* be able to access secret pointers, though.
        with ctx.new() as sctx:
            sctx.current_schema_views += (info.param_type,)
            body_ir = dispatch.compile(body, ctx=sctx)

        with ctx.new() as sctx:
            sctx.anchors = sctx.anchors.copy()
            select = qlast.SelectQuery(
                result=sctx.create_anchor(body_ir, 'a'),
                where=filter_expr,
            )

            sctx.modaliases = ctx.modaliases.copy()
            sctx.modaliases[None] = 'cfg'
            select_ir = setgen.ensure_set(
                dispatch.compile(select, ctx=sctx), ctx=sctx)

    config_reset = irast.ConfigReset(
        name=info.param_name,
        cardinality=info.cardinality,
        scope=expr.scope,
        requires_restart=info.requires_restart,
        backend_setting=info.backend_setting,
        is_system_config=info.is_system_config,
        span=expr.span,
        selector=select_ir,
    )
    return setgen.ensure_set(config_reset, ctx=ctx)


@dispatch.compile.register
def compile_ConfigInsert(
    expr: qlast.ConfigInsert, *, ctx: context.ContextLevel
) -> irast.Set:

    info = _validate_op(expr, ctx=ctx)

    if expr.scope not in (
        qltypes.ConfigScope.INSTANCE, qltypes.ConfigScope.DATABASE
    ):
        raise errors.UnsupportedFeatureError(
            f'CONFIGURE {expr.scope} INSERT is not supported'
        )

    subject = info.param_type
    insert_stmt = qlast.InsertQuery(
        subject=s_utils.name_to_ast_ref(subject.get_name(ctx.env.schema)),
        shape=expr.shape,
    )

    _inject_tname(insert_stmt, ctx=ctx)

    with ctx.newscope(fenced=False) as subctx:
        subctx.expr_exposed = context.Exposure.EXPOSED
        subctx.modaliases = ctx.modaliases.copy()
        subctx.modaliases[None] = 'cfg'
        subctx.special_computables_in_mutation_shape |= {'_tname'}
        insert_ir = dispatch.compile(insert_stmt, ctx=subctx)
        insert_ir_set = setgen.ensure_set(insert_ir, ctx=subctx)
        assert isinstance(insert_ir_set.expr, irast.InsertStmt)
        insert_subject = insert_ir_set.expr.subject

        _validate_config_object(insert_subject, scope=expr.scope, ctx=subctx)

    return setgen.ensure_set(
        irast.ConfigInsert(
            name=info.param_name,
            cardinality=info.cardinality,
            scope=expr.scope,
            requires_restart=info.requires_restart,
            backend_setting=info.backend_setting,
            is_system_config=info.is_system_config,
            expr=insert_subject,
            span=expr.span,
        ),
        ctx=ctx,
    )


def _inject_tname(
    insert_stmt: qlast.InsertQuery, *, ctx: context.ContextLevel
) -> None:

    for el in insert_stmt.shape:
        if isinstance(el.compexpr, qlast.InsertQuery):
            _inject_tname(el.compexpr, ctx=ctx)

    assert isinstance(insert_stmt.subject, qlast.BaseObjectRef)
    insert_stmt.shape.append(
        qlast.ShapeElement(
            expr=qlast.Path(
                steps=[qlast.Ptr(name='_tname')],
            ),
            compexpr=qlast.Path(
                steps=[
                    qlast.Introspect(
                        type=qlast.TypeName(
                            maintype=insert_stmt.subject,
                        ),
                    ),
                    qlast.Ptr(name='name'),
                ],
            ),
        ),
    )


def _validate_config_object(
    expr: irast.Set, *, scope: str, ctx: context.ContextLevel
) -> None:

    for element, _ in expr.shape:
        assert isinstance(element.expr, irast.Pointer)
        if element.expr.ptrref.shortname.name == 'id':
            continue

        ptr = typegen.ptrcls_from_ptrref(
            element.expr.ptrref.real_material_ptr,
            ctx=ctx,
        )
        if isinstance(ptr, s_pointers.Pointer):
            _enforce_pointer_constraints(
                ptr, element, ctx=ctx, for_obj=True)

        if (irtyputils.is_object(element.typeref)
                and isinstance(element.expr, irast.InsertStmt)):
            _validate_config_object(element, scope=scope, ctx=ctx)


def _validate_global_op(
    expr: qlast.ConfigOp, *, ctx: context.ContextLevel
) -> SettingInfo:
    glob_name = s_utils.ast_ref_to_name(expr.name)
    glob = ctx.env.get_schema_object_and_track(
        glob_name, expr.name,
        modaliases=ctx.modaliases, type=s_globals.Global)
    assert isinstance(glob, s_globals.Global)

    if isinstance(expr, (qlast.ConfigSet, qlast.ConfigReset)):
        if glob.get_expr(ctx.env.schema):
            raise errors.ConfigurationError(
                f"global '{glob_name}' is computed from an expression and "
                f"cannot be modified",
                span=expr.name.span
            )

    param_type = glob.get_target(ctx.env.schema)

    return SettingInfo(param_name=str(glob.get_name(ctx.env.schema)),
                       param_type=param_type,
                       cardinality=glob.get_cardinality(ctx.env.schema),
                       required=glob.get_required(ctx.env.schema),
                       requires_restart=False,
                       backend_setting=None,
                       is_system_config=False,
                       affects_compilation=False,
                       ptr=None)


def _enforce_pointer_constraints(
    ptr: s_pointers.Pointer,
    expr: irast.Set,
    *,
    ctx: context.ContextLevel,
    for_obj: bool,
) -> None:
    constraints = ptr.get_constraints(ctx.env.schema)
    for constraint in constraints.objects(ctx.env.schema):
        if constraint.issubclass(
            ctx.env.schema,
            ctx.env.schema.get('std::exclusive', type=s_constr.Constraint),
        ):
            continue

        with ctx.detached() as sctx:
            sctx.partial_path_prefix = expr
            sctx.anchors = ctx.anchors.copy()
            sctx.anchors['__subject__'] = expr

            final_expr: Optional[s_expr.Expression] = (
                constraint.get_finalexpr(ctx.env.schema)
            )
            assert final_expr is not None and final_expr.parse() is not None
            ir = dispatch.compile(final_expr.parse(), ctx=sctx)

        result = ireval.evaluate(ir, schema=ctx.env.schema)
        assert isinstance(result, irast.BooleanConstant)
        if result.value != 'true':
            if for_obj:
                name = ptr.get_verbosename(ctx.env.schema, with_parent=True)
            else:
                name = repr(ptr.get_shortname(ctx.env.schema).name)
            raise errors.ConfigurationError(
                f'invalid setting value for {name}'
            )


def _validate_op(
    expr: qlast.ConfigOp, *, ctx: context.ContextLevel
) -> SettingInfo:

    if expr.scope == qltypes.ConfigScope.GLOBAL:
        return _validate_global_op(expr, ctx=ctx)

    cfg_host_type = None
    is_ext_config = False
    if expr.name.module:
        cfg_host_name = sn.name_from_string(expr.name.module)
        cfg_host_type = ctx.env.get_schema_type_and_track(
            cfg_host_name, default=None)
        is_ext_config = bool(cfg_host_type)

    abstract_config = ctx.env.get_schema_type_and_track(
        sn.QualName('cfg', 'AbstractConfig'))
    ext_config = ctx.env.get_schema_type_and_track(
        sn.QualName('cfg', 'ExtensionConfig'))

    if not cfg_host_type:
        cfg_host_type = abstract_config

    name = fullname = expr.name.name
    if is_ext_config:
        fullname = f'{cfg_host_type.get_name(ctx.env.schema)}::{name}'

    assert isinstance(cfg_host_type, s_objtypes.ObjectType)
    cfg_type = None
    ptr = None

    if isinstance(expr, (qlast.ConfigSet, qlast.ConfigReset)):
        if is_ext_config and expr.scope == qltypes.ConfigScope.INSTANCE:
            raise errors.ConfigurationError(
                'INSTANCE configuration of extension-defined config variables '
                'is not allowed'
            )

        # expr.name is the actual name of the property.
        ptr = cfg_host_type.maybe_get_ptr(ctx.env.schema, sn.UnqualName(name))
        if ptr is not None:
            cfg_type = ptr.get_target(ctx.env.schema)

    if cfg_type is None:
        if isinstance(expr, qlast.ConfigSet):
            raise errors.ConfigurationError(
                f'unrecognized configuration parameter {name!r}',
                span=expr.span
            )

        cfg_type = ctx.env.get_schema_type_and_track(
            s_utils.ast_ref_to_name(expr.name), default=None)
        if not cfg_type and not expr.name.module:
            # expr.name is the name of the configuration type
            cfg_type = ctx.env.get_schema_type_and_track(
                sn.QualName('cfg', name), default=None)
        if not cfg_type:
            raise errors.ConfigurationError(
                f'unrecognized configuration object {name!r}',
                span=expr.span
            )

        assert isinstance(cfg_type, s_objtypes.ObjectType)
        ptr_candidate: Optional[s_pointers.Pointer] = None

        mro = [cfg_type] + list(
            cfg_type.get_ancestors(ctx.env.schema).objects(ctx.env.schema))
        for ct in mro:
            ptrs = ctx.env.schema.get_referrers(
                ct, scls_type=s_links.Link, field_name='target')

            if ptrs:
                pointer_link = next(iter(ptrs))
                assert isinstance(pointer_link, s_links.Link)
                ptr_candidate = pointer_link
                break

        if (
            ptr_candidate is None
            or (ptr_source := ptr_candidate.get_source(ctx.env.schema)) is None
            or not ptr_source.issubclass(
                ctx.env.schema, (abstract_config, ext_config))
        ):
            raise errors.ConfigurationError(
                f'{name!r} cannot be configured directly'
            )

        ptr = ptr_candidate

        fullname = ptr.get_shortname(ctx.env.schema).name
        if ptr_source.issubclass(ctx.env.schema, ext_config):
            fullname = f'{ptr_source.get_name(ctx.env.schema)}::{fullname}'

    assert isinstance(ptr, s_pointers.Pointer)

    sys_attr = ptr.get_annotations(ctx.env.schema).get(
        ctx.env.schema, sn.QualName('cfg', 'system'), None)

    system = (
        sys_attr is not None
        and sys_attr.get_value(ctx.env.schema) == 'true'
    )

    cardinality = ptr.get_cardinality(ctx.env.schema)
    assert cardinality is not None

    restart_attr = ptr.get_annotations(ctx.env.schema).get(
        ctx.env.schema, sn.QualName('cfg', 'requires_restart'), None)

    requires_restart = (
        restart_attr is not None
        and restart_attr.get_value(ctx.env.schema) == 'true'
    )

    backend_attr = ptr.get_annotations(ctx.env.schema).get(
        ctx.env.schema, sn.QualName('cfg', 'backend_setting'), None)

    if backend_attr is not None:
        backend_setting = json.loads(backend_attr.get_value(ctx.env.schema))
    else:
        backend_setting = None

    system_attr = ptr.get_annotations(ctx.env.schema).get(
        ctx.env.schema, sn.QualName('cfg', 'system'), None)

    is_system_config = (
        system_attr is not None
        and system_attr.get_value(ctx.env.schema) == 'true'
    )

    compilation_attr = ptr.get_annotations(ctx.env.schema).get(
        ctx.env.schema, sn.QualName('cfg', 'affects_compilation'), None)

    if compilation_attr is not None:
        affects_compilation = (
            json.loads(compilation_attr.get_value(ctx.env.schema)))
    else:
        affects_compilation = False

    if system and expr.scope is not qltypes.ConfigScope.INSTANCE:
        raise errors.ConfigurationError(
            f'{name!r} is a system-level configuration parameter; '
            f'use "CONFIGURE INSTANCE"')

    return SettingInfo(param_name=fullname,
                       param_type=cfg_type,
                       cardinality=cardinality,
                       required=False,
                       requires_restart=requires_restart,
                       backend_setting=backend_setting,
                       is_system_config=is_system_config,
                       affects_compilation=affects_compilation,
                       ptr=ptr)
