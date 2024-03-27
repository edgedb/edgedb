#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


"""Implementation of DESCRIBE ... CONFIG"""

from __future__ import annotations
from typing import Dict, List

import textwrap

from edb.edgeql import parser as qlparser
from edb.edgeql import qltypes
from edb.edgeql import quote as qlquote

from . import context
from . import dispatch

from edb.ir import ast as irast

from edb.schema import name as s_name
from edb.schema import objtypes as s_objtypes
from edb.schema import scalars as s_scalars
from edb.schema import schema as s_schema
from edb.schema import types as s_types

from edb.pgsql import common

ql = common.quote_literal


def compile_describe_config(
    scope: qltypes.ConfigScope, ctx: context.ContextLevel
) -> irast.Set:
    config_edgeql = _describe_config(
        ctx.env.schema, scope, ctx.env.options.testmode)
    config_ast = qlparser.parse_fragment(config_edgeql)

    return dispatch.compile(config_ast, ctx=ctx)


def _describe_config(
    schema: s_schema.Schema,
    scope: qltypes.ConfigScope,
    testmode: bool,
) -> str:
    """Generate an EdgeQL query to render config as DDL."""

    if scope is qltypes.ConfigScope.INSTANCE:
        source = 'system override'
        config_object_name = 'cfg::InstanceConfig'
    elif scope is qltypes.ConfigScope.DATABASE:
        source = 'database'
        config_object_name = 'cfg::DatabaseConfig'
    else:
        raise AssertionError(f'unexpected configuration source: {scope!r}')

    cfg = schema.get(config_object_name, type=s_objtypes.ObjectType)
    items = []
    items.extend(_describe_config_inner(
        schema, scope, config_object_name, cfg, testmode
    ))
    ext = schema.get('cfg::ExtensionConfig', type=s_objtypes.ObjectType)
    for ext_cfg in sorted(
        ext.descendants(schema), key=lambda x: x.get_name(schema)
    ):
        items.extend(_describe_config_inner(
            schema, scope, config_object_name, ext_cfg, testmode
        ))

    testmode_check = (
        "<bool>json_get(cfg::get_config_json(),'__internal_testmode','value')"
        " ?? false"
    )
    query = (
        "assert_exists(assert_single(("
        + f"FOR conf IN {{cfg::get_config_json(sources := [{ql(source)}])}} "
        + "UNION (\n"
        + (f"FOR testmode IN {{{testmode_check}}} UNION (\n"
           if testmode else "")
        + "SELECT array_join([" + ', '.join(items) + "], '')"
        + (")" if testmode else "")
        + ")"
        + ")))"
    )
    return query


def _describe_config_inner(
    schema: s_schema.Schema,
    scope: qltypes.ConfigScope,
    config_object_name: str,
    cfg: s_objtypes.ObjectType,
    testmode: bool,
) -> list[str]:
    """Generate an EdgeQL query to render config as DDL."""

    actual_name = str(cfg.get_name(schema))
    cast = (
        f'.extensions[is {actual_name}]' if actual_name != config_object_name
        else ''
    )

    items = []
    for ptr_name, p in sorted(
        cfg.get_pointers(schema).items(schema),
        key=lambda x: x[0],
    ):
        pn = str(ptr_name)
        if (
            pn == 'id'
            or p.get_computable(schema)
            or p.get_protected(schema)
        ):
            continue

        is_internal = (
            p.get_annotation(
                schema,
                s_name.QualName('cfg', 'internal')
            ) == 'true'
        )
        if is_internal and not testmode:
            continue

        ptype = p.get_target(schema)
        assert ptype is not None

        # Skip backlinks to the base object. The will get plenty of
        # special treatment.
        if str(ptype.get_name(schema)) == 'cfg::AbstractConfig':
            continue

        ptr_card = p.get_cardinality(schema)
        mult = ptr_card.is_multi()
        psource = f'{config_object_name}{cast}.{qlquote.quote_ident(pn)}'
        if isinstance(ptype, s_objtypes.ObjectType):
            item = textwrap.indent(
                _render_config_object(
                    schema=schema,
                    valtype=ptype,
                    value_expr=psource,
                    scope=scope,
                    join_term='',
                    level=1,
                ),
                ' ' * 4,
            )
        else:
            fn = (
                pn if actual_name == config_object_name
                else f'{actual_name}::{pn}'
            )
            renderer = (
                _render_config_redacted if p.get_secret(schema)
                else _render_config_set if mult
                else _render_config_scalar
            )
            item = textwrap.indent(
                renderer(
                    schema=schema,
                    valtype=ptype,
                    value_expr=psource,
                    name=fn,
                    scope=scope,
                    level=1,
                ),
                ' ' * 4,
            )

        fpn = f'{actual_name}::{pn}' if cast else pn

        condition = f'EXISTS json_get(conf, {ql(fpn)})'
        if is_internal:
            condition = f'({condition}) AND testmode'
        items.append(f"(\n{item}\n    IF {condition} ELSE ''\n  )")

    return items


def _render_config_value(
    *,
    schema: s_schema.Schema,
    valtype: s_types.Type,
    value_expr: str,
) -> str:
    if valtype.issubclass(
        schema,
        schema.get('std::anyreal', type=s_scalars.ScalarType),
    ):
        val = f'<str>{value_expr}'
    elif valtype.issubclass(
        schema,
        schema.get('std::bool', type=s_scalars.ScalarType),
    ):
        val = f'<str>{value_expr}'
    elif valtype.issubclass(
        schema,
        schema.get('std::duration', type=s_scalars.ScalarType),
    ):
        val = f'"<std::duration>" ++ cfg::_quote(<str>{value_expr})'
    elif valtype.issubclass(
        schema,
        schema.get('cfg::memory', type=s_scalars.ScalarType),
    ):
        val = f'"<cfg::memory>" ++ cfg::_quote(<str>{value_expr})'
    elif valtype.issubclass(
        schema,
        schema.get('std::str', type=s_scalars.ScalarType),
    ):
        val = f'cfg::_quote({value_expr})'
    elif valtype.is_enum(schema):
        tn = valtype.get_name(schema)
        val = f'"<{str(tn)}>" ++ cfg::_quote(<str>{value_expr})'
    else:
        raise AssertionError(
            f'unexpected configuration value type: '
            f'{valtype.get_displayname(schema)}'
        )

    return val


def _render_config_redacted(
    *,
    schema: s_schema.Schema,
    valtype: s_types.Type,
    value_expr: str,
    scope: qltypes.ConfigScope,
    name: str,
    level: int,
) -> str:
    if level == 1:
        return (
            f"'CONFIGURE {scope.to_edgeql()} "
            f"SET {qlquote.quote_ident(name)} := {{}};  # REDACTED\\n'"
        )
    else:
        indent = ' ' * (4 * (level - 1))
        return f"'{indent}{qlquote.quote_ident(name)} := {{}},  # REDACTED'"


def _render_config_set(
    *,
    schema: s_schema.Schema,
    valtype: s_types.Type,
    value_expr: str,
    scope: qltypes.ConfigScope,
    name: str,
    level: int,
) -> str:
    assert isinstance(valtype, s_scalars.ScalarType)
    v = _render_config_value(
        schema=schema, valtype=valtype, value_expr=value_expr)
    if level == 1:
        return (
            f"'CONFIGURE {scope.to_edgeql()} "
            f"SET {qlquote.quote_ident(name)} := {{' ++ "
            f"array_join(array_agg((select _ := {v} order by _)), ', ') "
            f"++ '}};\\n'"
        )
    else:
        indent = ' ' * (4 * (level - 1))
        return (
            f"'{indent}{qlquote.quote_ident(name)} := {{' ++ "
            f"array_join(array_agg((SELECT _ := {v} ORDER BY _)), ', ') "
            f"++ '}},'"
        )


def _render_config_scalar(
    *,
    schema: s_schema.Schema,
    valtype: s_types.Type,
    value_expr: str,
    scope: qltypes.ConfigScope,
    name: str,
    level: int,
) -> str:
    assert isinstance(valtype, s_scalars.ScalarType)
    v = _render_config_value(
        schema=schema, valtype=valtype, value_expr=value_expr)
    if level == 1:
        return (
            f"'CONFIGURE {scope.to_edgeql()} "
            f"SET {qlquote.quote_ident(name)} := ' ++ {v} ++ ';\\n'"
        )
    else:
        indent = ' ' * (4 * (level - 1))
        return f"'{indent}{qlquote.quote_ident(name)} := ' ++ {v} ++ ','"


def _render_config_object(
    *,
    schema: s_schema.Schema,
    valtype: s_objtypes.ObjectType,
    value_expr: str,
    scope: qltypes.ConfigScope,
    join_term: str,
    level: int,
) -> str:
    # Generate a valid `CONFIGURE <SCOPE> INSERT ConfigObject`
    # shape for a given configuration object type or
    # `INSERT ConfigObject` for a nested configuration type.
    sub_layouts = _describe_config_object(
        schema=schema, valtype=valtype, level=level + 1, scope=scope)
    sub_layouts_items = []
    if level == 1:
        decor = [f'CONFIGURE {scope.to_edgeql()} INSERT ', ';\\n']
    else:
        decor = ['(INSERT ', ')']

    indent = ' ' * (4 * (level - 1))

    for type_name, type_layout in sub_layouts.items():
        if type_layout:
            sub_layout_item = (
                f"'{indent}{decor[0]}{type_name} {{\\n'\n++ "
                + "\n++ ".join(type_layout)
                + f" ++ '{indent}}}{decor[1]}'"
            )
        else:
            sub_layout_item = (
                f"'{indent}{decor[0]}{type_name}{decor[1]}'"
            )

        if len(sub_layouts) > 1:
            if type_layout:
                sub_layout_item = (
                    f'(WITH item := item[IS {type_name}]'
                    f' SELECT {sub_layout_item}) '
                    f'IF item.__type__.name = {ql(str(type_name))}'
                )
            else:
                sub_layout_item = (
                    f'{sub_layout_item} '
                    f'IF item.__type__.name = {ql(str(type_name))}'
                )

        sub_layouts_items.append(sub_layout_item)

    if len(sub_layouts_items) > 1:
        sli_render = '\nELSE '.join(sub_layouts_items) + "\nELSE ''"
    else:
        sli_render = sub_layouts_items[0]

    return '\n'.join((
        f"array_join(array_agg((SELECT _ := (",
        f"  FOR item IN {{ {value_expr} }}",
        f"  UNION (",
        f"{textwrap.indent(sli_render, ' ' * 4)}",
        f"  )",
        f") ORDER BY _)), {ql(join_term)})",
    ))


def _describe_config_object(
    *,
    schema: s_schema.Schema,
    valtype: s_objtypes.ObjectType,
    level: int,
    scope: qltypes.ConfigScope,
) -> Dict[s_name.QualName, List[str]]:
    cfg_types = [valtype]
    cfg_types.extend(cfg_types[0].descendants(schema))
    layouts = {}
    for cfg in cfg_types:
        items = []
        for ptr_name, p in sorted(
            cfg.get_pointers(schema).items(schema),
            key=lambda x: x[0],
        ):
            pn = str(ptr_name)
            if (
                pn == 'id'
                or p.get_protected(schema)
                or p.get_annotation(
                    schema,
                    s_name.QualName('cfg', 'internal'),
                ) == 'true'
            ):
                continue

            ptype = p.get_target(schema)
            assert ptype is not None
            if str(ptype.get_name(schema)) == 'cfg::AbstractConfig':
                continue

            ptr_card = p.get_cardinality(schema)
            mult = ptr_card.is_multi()
            psource = f'item.{qlquote.quote_ident(pn)}'

            if isinstance(ptype, s_objtypes.ObjectType):
                rval = textwrap.indent(
                    _render_config_object(
                        schema=schema,
                        valtype=ptype,
                        value_expr=psource,
                        scope=scope,
                        join_term=' UNION ',
                        level=level + 1,
                    ),
                    ' ' * 2,
                ).strip()
                indent = ' ' * (4 * (level - 1))
                item = (
                    f"'{indent}{qlquote.quote_ident(pn)} "
                    f":= (\\n'\n++ {rval} ++ '\\n{indent}),\\n'"
                )
                condition = None
            else:
                render = (
                    _render_config_redacted if p.get_secret(schema)
                    else _render_config_set if mult
                    else _render_config_scalar
                )
                item = render(
                    schema=schema,
                    valtype=ptype,
                    value_expr=psource,
                    scope=scope,
                    name=pn,
                    level=level,
                )
                if p.get_secret(schema):
                    condition = 'true'
                else:
                    condition = f'EXISTS {psource}'

            if condition is not None:
                item = f"({item} ++ '\\n' IF {condition} ELSE '')"

            items.append(item)

        layouts[cfg.get_name(schema)] = items

    return layouts
