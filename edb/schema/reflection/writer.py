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

"""Schema reflection helpers."""

from __future__ import annotations
from typing import *

import functools
import json
import numbers
import textwrap

from edb.edgeql import qltypes

from edb.schema import constraints as s_constr
from edb.schema import delta as sd
from edb.schema import objects as so
from edb.schema import objtypes as s_objtypes
from edb.schema import referencing as s_ref
from edb.schema import scalars as s_scalars
from edb.schema import schema as s_schema
from edb.schema import types as s_types

from edb.schema.reflection import structure as sr_struct


@functools.singledispatch
def write_meta(
    cmd: sd.Command,
    *,
    classlayout: Dict[Type[so.Object], sr_struct.SchemaTypeLayout],
    schema: s_schema.Schema,
    context: sd.CommandContext,
    blocks: List[Tuple[str, Dict[str, Any]]],
    internal_schema_mode: bool,
    stdmode: bool,
) -> None:
    """Generate EdgeQL statements populating schema metadata.

    Args:
        cmd:
            Delta command tree for which EdgeQL DML must be generated.
        classlayout:
            Schema class layout as returned from
            :func:`schema.reflection.structure.generate_structure`.
        schema:
            A schema instance.
        context:
            Delta context corresponding to *cmd*.
        blocks:
            A list where a sequence of (edgeql, args) tuples will
            be appended to.
        internal_schema_mode:
            If True, *cmd* represents internal `schema` modifications.
        stdmode:
            If True, *cmd* represents a standard library bootstrap DDL.
    """
    raise NotImplementedError(f"cannot handle {cmd!r}")


def _descend(
    cmd: sd.Command,
    *,
    classlayout: Dict[Type[so.Object], sr_struct.SchemaTypeLayout],
    schema: s_schema.Schema,
    context: sd.CommandContext,
    blocks: List[Tuple[str, Dict[str, Any]]],
    internal_schema_mode: bool,
    stdmode: bool,
    prerequisites: bool = False,
) -> None:

    if prerequisites:
        commands = cmd.get_prerequisites()
    else:
        commands = cmd.get_subcommands(include_prerequisites=False)

    ctxcls = cmd.get_context_class()
    if ctxcls is not None:
        if (
            issubclass(ctxcls, sd.ObjectCommandContext)
            and isinstance(cmd, sd.ObjectCommand)
        ):
            objctxcls = cast(
                Type[sd.ObjectCommandContext[so.Object]],
                ctxcls,
            )
            ctx = objctxcls(schema=schema, op=cmd, scls=sd._dummy_object)
        else:
            # I could not find a way to convince mypy here.
            ctx = ctxcls(schema=schema, op=cmd)  # type: ignore

        with context(ctx):
            for subcmd in commands:
                if isinstance(subcmd, sd.AlterObjectProperty):
                    continue
                write_meta(
                    subcmd,
                    classlayout=classlayout,
                    schema=schema,
                    context=context,
                    blocks=blocks,
                    internal_schema_mode=internal_schema_mode,
                    stdmode=stdmode,
                )
    else:
        for subcmd in commands:
            if isinstance(subcmd, sd.AlterObjectProperty):
                continue
            write_meta(
                subcmd,
                classlayout=classlayout,
                schema=schema,
                context=context,
                blocks=blocks,
                internal_schema_mode=internal_schema_mode,
                stdmode=stdmode,
            )


@write_meta.register
def write_meta_delta_root(
    cmd: sd.DeltaRoot,
    *,
    classlayout: Dict[Type[so.Object], sr_struct.SchemaTypeLayout],
    schema: s_schema.Schema,
    context: sd.CommandContext,
    blocks: List[Tuple[str, Dict[str, Any]]],
    internal_schema_mode: bool,
    stdmode: bool,
) -> None:
    _descend(
        cmd,
        classlayout=classlayout,
        schema=schema,
        context=context,
        blocks=blocks,
        internal_schema_mode=internal_schema_mode,
        stdmode=stdmode,
    )


def _build_object_mutation_shape(
    cmd: sd.ObjectCommand[so.Object],
    *,
    classlayout: Dict[Type[so.Object], sr_struct.SchemaTypeLayout],
    lprop_fields: Optional[
        Dict[str, Tuple[s_types.Type, sr_struct.FieldType]]
    ] = None,
    lprops_only: bool = False,
    internal_schema_mode: bool,
    stdmode: bool,
    var_prefix: str = '',
    schema: s_schema.Schema,
    context: sd.CommandContext,
) -> Tuple[str, Dict[str, Any]]:

    props = cmd.get_resolved_attributes(schema, context)
    mcls = cmd.get_schema_metaclass()
    layout = classlayout[mcls]

    if lprop_fields is None:
        lprop_fields = {}

    # XXX: This is a hack around the fact that _update_lprops works by
    # removing all the links and recreating them. Since that will lose
    # data in situations where not every lprop attribute is specified,
    # merge AlterOwned props up into the enclosing command. (This avoids
    # trouble with annotations, which is the main place where we have
    # multiple interesting lprops at once.)
    if isinstance(cmd, s_ref.AlterOwned):
        return '', {}
    for sub in cmd.get_subcommands(type=s_ref.AlterOwned):
        props.update(sub.get_resolved_attributes(schema, context))

    assignments = []
    variables: Dict[str, str] = {}
    if isinstance(cmd, sd.CreateObject):
        empties = {
            v.fieldname: None for f, v in layout.items()
            if (
                f != 'backend_id'
                and v.storage is not None
                and v.storage.ptrkind != 'link'
                and v.storage.ptrkind != 'multi link'
            )
        }
        all_props = {**empties, **props}
    else:
        all_props = props

    for n, v in sorted(all_props.items(), key=lambda i: i[0]):
        ns = mcls.get_field(n).sname

        lprop_target = lprop_fields.get(n)
        if lprop_target is not None:
            target, ftype = lprop_target
            cardinality = qltypes.SchemaCardinality.One
            is_ordered = False
            reflection_proxy = None
        elif lprops_only:
            continue
        else:
            layout_entry = layout.get(ns)
            if layout_entry is None:
                # The field is ephemeral, skip it.
                continue
            else:
                target = layout_entry.type
                cardinality = layout_entry.cardinality
                is_ordered = layout_entry.is_ordered
                reflection_proxy = layout_entry.reflection_proxy
                assert layout_entry.storage is not None
                ftype = layout_entry.storage.fieldtype

        target_value: Any

        var_n = f'__{var_prefix}{n}'

        if (
            issubclass(mcls, s_constr.Constraint)
            and n == 'params'
            and isinstance(cmd, s_ref.ReferencedObjectCommand)
            and cmd.get_referrer_context(context) is not None
        ):
            # Constraint args are represented as a `@value` link property
            # on the `params` link.
            # TODO: replace this hack by a generic implementation of
            # an ObjectKeyDict collection that allow associating objects
            # with arbitrary values (a transposed ObjectDict).
            target_expr = f"""assert_distinct((
                FOR v IN {{ json_array_unpack(<json>${var_n}) }}
                UNION (
                    SELECT {target.get_name(schema)} {{
                        @value := <str>v[1]
                    }}
                    FILTER .id = <uuid>v[0]
                )
            ))"""
            args = props.get('args', [])
            target_value = []
            if v is not None:
                for i, param in enumerate(v.objects(schema)):
                    if i == 0:
                        # skip the implicit __subject__ parameter
                        arg_expr = ''
                    else:
                        try:
                            arg = args[i - 1]
                        except IndexError:
                            arg_expr = ''
                        else:
                            pkind = param.get_kind(schema)
                            if pkind is qltypes.ParameterKind.VariadicParam:
                                rest = [arg.text for arg in args[i - 1:]]
                                arg_expr = f'[{",".join(rest)}]'
                            else:
                                arg_expr = arg.text

                    target_value.append((str(param.id), arg_expr))

        elif n == 'name':
            target_expr = f'<str>${var_n}'
            assignments.append(f'{ns}__internal := <str>${var_n}__internal')
            if v is not None:
                target_value = mcls.get_displayname_static(v)
                variables[f'{var_n}__internal'] = json.dumps(str(v))
            else:
                target_value = None
                variables[f'{var_n}__internal'] = json.dumps(None)

        elif isinstance(target, s_objtypes.ObjectType):
            if cardinality is qltypes.SchemaCardinality.Many:
                if ftype is sr_struct.FieldType.OBJ_DICT:
                    target_expr, target_value = _reflect_object_dict_value(
                        schema=schema,
                        value=v,
                        is_ordered=is_ordered,
                        value_var_name=var_n,
                        target=target,
                        reflection_proxy=reflection_proxy,
                    )
                elif is_ordered:
                    target_expr = f'''(
                        FOR v IN {{
                            enumerate(assert_distinct(
                                <uuid>json_array_unpack(<json>${var_n})
                            ))
                        }}
                        UNION (
                            SELECT (DETACHED {target.get_name(schema)}) {{
                                @index := v.0,
                            }}
                            FILTER .id = v.1
                        )
                    )'''
                    if v is not None:
                        target_value = [str(i) for i in v.ids(schema)]
                    else:
                        target_value = []
                else:
                    target_expr = f'''(
                        SELECT (DETACHED {target.get_name(schema)})
                        FILTER .id IN <uuid>json_array_unpack(<json>${var_n})
                    )'''
                    if v is not None:
                        target_value = [str(i) for i in v.ids(schema)]
                    else:
                        target_value = []
            else:
                target_expr = f'''(
                    SELECT (DETACHED {target.get_name(schema)})
                    FILTER .id = <uuid>${var_n}
                )'''
                if v is not None:
                    target_value = str(v.id)
                else:
                    target_value = None

        elif ftype is sr_struct.FieldType.EXPR:
            target_expr = f'<str>${var_n}'
            if v is not None:
                target_value = v.text
            else:
                target_value = None

            shadow_target_expr = (
                f'sys::_expr_from_json(<json>${var_n}_expr)'
            )

            assignments.append(f'{ns}__internal := {shadow_target_expr}')
            if v is not None:
                ids = [str(i) for i in v.refs.ids(schema)]
                variables[f'{var_n}_expr'] = json.dumps(
                    {'text': v.text, 'refs': ids}
                )
            else:
                variables[f'{var_n}_expr'] = json.dumps(None)

        elif ftype is sr_struct.FieldType.EXPR_LIST:
            target_expr = f'''
                array_agg(<str>json_array_unpack(<json>${var_n})["text"])
            '''
            if v is not None:
                target_value = [
                    {
                        'text': ex.text,
                        'refs': (
                            [str(i) for i in ex.refs.ids(schema)]
                            if ex.refs else []
                        )
                    }
                    for ex in v
                ]
            else:
                target_value = []

            shadow_target_expr = f'''
                (SELECT
                    array_agg(
                        sys::_expr_from_json(
                            json_array_unpack(<json>${var_n})
                        )
                    )
                )
            '''

            assignments.append(f'{ns}__internal := {shadow_target_expr}')

        elif ftype is sr_struct.FieldType.EXPR_DICT:
            target_expr = f'''
                array_agg(<str>json_array_unpack(
                    <json>${var_n})["expr"]["text"])
            '''
            if v is not None:
                target_value = [
                    {
                        'name': key,
                        'expr': {
                            'text': ex.text,
                            'refs': (
                                [str(i) for i in ex.refs.ids(schema)]
                                if ex.refs else []
                            )
                        }
                    }
                    for key, ex in v.items()
                ]
            else:
                target_value = []

            shadow_target_expr = f'''
                (
                    WITH
                        orig_json := json_array_unpack(<json>${var_n})
                    SELECT
                        array_agg(
                            (
                                name := <str>orig_json['name'],
                                expr := sys::_expr_from_json(
                                    orig_json['expr']
                                )
                            )
                        )
                )
            '''

            assignments.append(f'{ns}__internal := {shadow_target_expr}')

        elif isinstance(target, s_types.Array):
            eltype = target.get_element_type(schema)
            target_expr = f'''
                array_agg(<{eltype.get_name(schema)}>
                    json_array_unpack(<json>${var_n}))
                IF json_typeof(<json>${var_n}) != 'null'
                ELSE <array<{eltype.get_name(schema)}>>{{}}
            '''
            if v is not None:
                target_value = list(v)
            else:
                target_value = None

        else:
            target_expr = f'${var_n}'
            if cardinality and cardinality.is_multi():
                target_expr = f'json_array_unpack(<json>{target_expr})'
            if target.is_enum(schema):
                target_expr = f'<str>{target_expr}'
            target_expr = f'<{target.get_name(schema)}>{target_expr}'

            if v is not None and cardinality.is_multi():
                target_value = list(v)
            elif v is None or isinstance(v, numbers.Number):
                target_value = v
            else:
                target_value = str(v)

        if lprop_target is not None:
            assignments.append(f'@{ns} := {target_expr}')
        else:
            assignments.append(f'{ns} := {target_expr}')

        variables[var_n] = json.dumps(target_value)

    if isinstance(cmd, sd.CreateObject):
        if (
            issubclass(mcls, (s_scalars.ScalarType, s_types.Collection))
            and not issubclass(mcls, s_types.CollectionExprAlias)
            and not cmd.get_attribute_value('abstract')
        ):
            kind = f'"schema::{mcls.__name__}"'

            if issubclass(mcls, (s_types.Array, s_types.Range)):
                assignments.append(
                    f'backend_id := sys::_get_pg_type_for_edgedb_type('
                    f'<uuid>$__{var_prefix}id, '
                    f'{kind}, '
                    f'<uuid>$__{var_prefix}element_type)'
                )
            else:
                assignments.append(
                    f'backend_id := sys::_get_pg_type_for_edgedb_type('
                    f'<uuid>$__{var_prefix}id, {kind}, <uuid>{{}})'
                )
            variables[f'__{var_prefix}id'] = json.dumps(
                str(cmd.get_attribute_value('id')))

    shape = ',\n'.join(assignments)

    return shape, variables


def _reflect_object_dict_value(
    *,
    schema: s_schema.Schema,
    value: Optional[so.ObjectDict[str, so.Object]],
    is_ordered: bool,
    value_var_name: str,
    target: s_types.Type,
    reflection_proxy: Optional[Tuple[str, str]],
) -> Tuple[str, Any]:

    if reflection_proxy is not None:
        # Non-unique ObjectDict, reflecting via a proxy object
        proxy_type, proxy_link = reflection_proxy

        if is_ordered:
            target_expr = f'''(
                FOR v IN {{
                    enumerate(
                        json_array_unpack(<json>${value_var_name})
                    )
                }}
                UNION (
                    INSERT {proxy_type} {{
                        {proxy_link} := (
                            SELECT (DETACHED {target.get_name(schema)})
                            FILTER .id = <uuid>v.1[1]
                        ),
                        name := <str>v.1[0],
                        @index := v.0,
                    }}
                )
            )'''
        else:
            target_expr = f'''(
                FOR v IN {{
                    json_array_unpack(<json>${value_var_name})
                }}
                UNION (
                    INSERT {proxy_type} {{
                        {proxy_link} := (
                            SELECT (DETACHED {target.get_name(schema)})
                            FILTER .id = <uuid>v[1]
                        ),
                        name := <str>v[0],
                    }}
                )
            )'''
    else:
        if is_ordered:
            target_expr = f'''(
                FOR v IN {{
                    enumerate(
                        json_array_unpack(<json>${value_var_name})
                    )
                }}
                UNION (
                    SELECT (DETACHED {target.get_name(schema)}) {{
                        name := v.1[0],
                        @index := v.0,
                    }}
                    FILTER .id = <uuid>v.1[1]
                )
            )'''
        else:
            target_expr = f'''(
                FOR v IN {{
                    json_array_unpack(<json>${value_var_name})
                }}
                UNION (
                    SELECT (DETACHED {target.get_name(schema)}) {{
                        @key := v[0],
                    }}
                    FILTER .id = <uuid>v[1]
                )
            )'''

    if value is None:
        target_value = []
    else:
        target_value = [(n, str(i.id)) for n, i in value.items(schema)]

    return target_expr, target_value


# type ignore below because mypy's wishes of generic parametrization
# clash with the expectations of singledispatch receiving an actual type.
@write_meta.register
def write_meta_create_object(
    cmd: sd.CreateObject,  # type: ignore
    *,
    classlayout: Dict[Type[so.Object], sr_struct.SchemaTypeLayout],
    schema: s_schema.Schema,
    context: sd.CommandContext,
    blocks: List[Tuple[str, Dict[str, Any]]],
    internal_schema_mode: bool,
    stdmode: bool,
) -> None:
    _descend(
        cmd,
        classlayout=classlayout,
        schema=schema,
        context=context,
        blocks=blocks,
        prerequisites=True,
        internal_schema_mode=internal_schema_mode,
        stdmode=stdmode,
    )

    mcls = cmd.maybe_get_schema_metaclass()
    if mcls is not None and not issubclass(mcls, so.GlobalObject):
        if isinstance(cmd, s_ref.ReferencedObjectCommand):
            refctx = cmd.get_referrer_context(context)
        else:
            refctx = None

        if refctx is None:
            shape, variables = _build_object_mutation_shape(
                cmd,
                classlayout=classlayout,
                internal_schema_mode=internal_schema_mode,
                stdmode=stdmode,
                schema=schema,
                context=context,
            )

            insert_query = f'''
                INSERT schema::{mcls.__name__} {{
                    {shape}
                }}
            '''

            blocks.append((insert_query, variables))
        else:
            refop = refctx.op
            refcls = refop.get_schema_metaclass()
            refdict = refcls.get_refdict_for_class(mcls)
            layout = classlayout[refcls][refdict.attr]
            lprops = layout.properties

            reflect_as_link = (
                mcls.get_reflection_method() is so.ReflectionMethod.AS_LINK
            )

            shape, variables = _build_object_mutation_shape(
                cmd,
                classlayout=classlayout,
                lprop_fields=lprops,
                lprops_only=reflect_as_link,
                internal_schema_mode=internal_schema_mode,
                stdmode=stdmode,
                schema=schema,
                context=context,
            )

            assignments = []

            if reflect_as_link:
                target_link = mcls.get_reflection_link()
                assert target_link is not None
                target_field = mcls.get_field(target_link)
                target = cmd.get_attribute_value(target_link)

                append_query = f'''
                    SELECT DETACHED schema::{target_field.type.__name__} {{
                        {shape}
                    }} FILTER
                        .name__internal = <str>$__{target_link}
                '''

                variables[f'__{target_link}'] = (
                    json.dumps(str(target.get_name(schema)))
                )

                shadow_clslayout = classlayout[refcls]
                shadow_link_layout = (
                    shadow_clslayout[f'{refdict.attr}__internal'])
                shadow_shape, shadow_variables = _build_object_mutation_shape(
                    cmd,
                    classlayout=classlayout,
                    internal_schema_mode=internal_schema_mode,
                    lprop_fields=shadow_link_layout.properties,
                    stdmode=stdmode,
                    var_prefix='shadow_',
                    schema=schema,
                    context=context,
                )

                variables.update(shadow_variables)

                shadow_append_query = f'''
                    INSERT schema::{mcls.__name__} {{
                        {shadow_shape}
                    }}
                '''

                assignments.append(f'''
                    {refdict.attr}__internal += (
                        {shadow_append_query}
                    )
                ''')

            else:
                append_query = f'''
                    INSERT schema::{mcls.__name__} {{
                        {shape}
                    }}
                '''

            assignments.append(f'''
                {refdict.attr} += (
                    {append_query}
                )
            ''')

            update_shape = ',\n'.join(assignments)

            parent_update_query = f'''
                UPDATE schema::{refcls.__name__}
                FILTER .name__internal = <str>$__parent_classname
                SET {{
                    {update_shape}
                }}
            '''

            ref_name = context.get_referrer_name(refctx)
            variables['__parent_classname'] = json.dumps(str(ref_name))
            blocks.append((parent_update_query, variables))

    _descend(
        cmd,
        classlayout=classlayout,
        schema=schema,
        context=context,
        blocks=blocks,
        internal_schema_mode=internal_schema_mode,
        stdmode=stdmode,
    )


@write_meta.register
def write_meta_alter_object(
    cmd: sd.ObjectCommand,  # type: ignore
    *,
    classlayout: Dict[Type[so.Object], sr_struct.SchemaTypeLayout],
    schema: s_schema.Schema,
    context: sd.CommandContext,
    blocks: List[Tuple[str, Dict[str, Any]]],
    internal_schema_mode: bool,
    stdmode: bool,
) -> None:
    _descend(
        cmd,
        classlayout=classlayout,
        schema=schema,
        context=context,
        blocks=blocks,
        prerequisites=True,
        internal_schema_mode=internal_schema_mode,
        stdmode=stdmode,
    )

    mcls = cmd.maybe_get_schema_metaclass()
    if mcls is not None and not issubclass(mcls, so.GlobalObject):
        shape, variables = _build_object_mutation_shape(
            cmd,
            classlayout=classlayout,
            internal_schema_mode=internal_schema_mode,
            stdmode=stdmode,
            schema=schema,
            context=context,
        )

        if shape:
            query = f'''
                UPDATE schema::{mcls.__name__}
                FILTER .name__internal = <str>$__classname
                SET {{
                    {shape}
                }};
            '''
            variables['__classname'] = json.dumps(str(cmd.classname))
            blocks.append((query, variables))

        if isinstance(cmd, s_ref.ReferencedObjectCommand):
            refctx = cmd.get_referrer_context(context)
            if refctx is not None:
                _update_lprops(
                    cmd,
                    classlayout=classlayout,
                    schema=schema,
                    blocks=blocks,
                    context=context,
                    internal_schema_mode=internal_schema_mode,
                    stdmode=stdmode,
                )

    _descend(
        cmd,
        classlayout=classlayout,
        schema=schema,
        context=context,
        blocks=blocks,
        internal_schema_mode=internal_schema_mode,
        stdmode=stdmode,
    )


def _update_lprops(
    cmd: s_ref.ReferencedObjectCommand,  # type: ignore
    *,
    classlayout: Dict[Type[so.Object], sr_struct.SchemaTypeLayout],
    schema: s_schema.Schema,
    blocks: List[Tuple[str, Dict[str, Any]]],
    context: sd.CommandContext,
    internal_schema_mode: bool,
    stdmode: bool,
) -> None:
    mcls = cmd.get_schema_metaclass()
    refctx = cmd.get_referrer_context_or_die(context)
    refop = refctx.op
    refcls = refop.get_schema_metaclass()
    refdict = refcls.get_refdict_for_class(mcls)
    layout = classlayout[refcls][refdict.attr]
    lprops = layout.properties

    if not lprops:
        return

    reflect_as_link = (
        mcls.get_reflection_method() is so.ReflectionMethod.AS_LINK
    )

    if reflect_as_link:
        target_link = mcls.get_reflection_link()
        assert target_link is not None
        target_field = mcls.get_field(target_link)
        target_obj = cmd.get_ddl_identity(target_link)
        if target_obj is None:
            raise AssertionError(
                f'cannot find link target in ddl_identity of a command for '
                f'schema class reflected as link: {cmd!r}'
            )
        target_clsname = target_field.type.__name__
    else:
        referrer_cls = refop.get_schema_metaclass()
        target_field = referrer_cls.get_field(refdict.attr)
        if issubclass(target_field.type, so.ObjectCollection):
            target_type = target_field.type.type
        else:
            target_type = target_field.type
        target_clsname = target_type.__name__
        target_link = refdict.attr
        target_obj = cmd.scls

    shape, append_variables = _build_object_mutation_shape(
        cmd,
        classlayout=classlayout,
        lprop_fields=lprops,
        lprops_only=True,
        internal_schema_mode=internal_schema_mode,
        stdmode=stdmode,
        schema=schema,
        context=context,
    )

    if shape:
        parent_variables = {}
        parent_variables[f'__{target_link}'] = json.dumps(str(target_obj.id))
        ref_name = context.get_referrer_name(refctx)
        parent_variables['__parent_classname'] = json.dumps(str(ref_name))

        # XXX: we have to do a -= followed by a += because
        # support for filtered nested link property updates
        # is currently broken.
        # This is fragile! If not all of the lprops are specified,
        # we will drop them.

        assignments = []

        assignments.append(textwrap.dedent(
            f'''\
            {refdict.attr} -= (
                SELECT DETACHED (schema::{target_clsname})
                FILTER .id = <uuid>$__{target_link}
            )'''
        ))

        if reflect_as_link:
            parent_variables[f'__{target_link}_shadow'] = (
                json.dumps(str(cmd.classname)))

            assignments.append(textwrap.dedent(
                f'''\
                {refdict.attr}__internal -= (
                    SELECT DETACHED (schema::{mcls.__name__})
                    FILTER .name__internal = <str>$__{target_link}_shadow
                )'''
            ))

        update_shape = textwrap.indent(
            '\n' + ',\n'.join(assignments), '    ' * 4)

        parent_update_query = textwrap.dedent(f'''\
            UPDATE schema::{refcls.__name__}
            FILTER .name__internal = <str>$__parent_classname
            SET {{{update_shape}
            }}
        ''')

        blocks.append((parent_update_query, parent_variables))

        assignments = []

        shape = textwrap.indent(f'\n{shape}', '    ' * 5)

        assignments.append(textwrap.dedent(
            f'''\
            {refdict.attr} += (
                SELECT DETACHED schema::{target_clsname} {{{shape}
                }} FILTER .id = <uuid>$__{target_link}
            )'''
        ))

        if reflect_as_link:
            shadow_clslayout = classlayout[refcls]
            shadow_link_layout = shadow_clslayout[f'{refdict.attr}__internal']
            shadow_shape, shadow_variables = _build_object_mutation_shape(
                cmd,
                classlayout=classlayout,
                internal_schema_mode=internal_schema_mode,
                lprop_fields=shadow_link_layout.properties,
                lprops_only=True,
                stdmode=stdmode,
                var_prefix='shadow_',
                schema=schema,
                context=context,
            )

            shadow_shape = textwrap.indent(f'\n{shadow_shape}', '    ' * 6)

            assignments.append(textwrap.dedent(
                f'''\
                {refdict.attr}__internal += (
                    SELECT DETACHED schema::{mcls.__name__} {{{shadow_shape}
                    }} FILTER .name__internal = <str>$__{target_link}_shadow
                )'''
            ))

            parent_variables.update(shadow_variables)

        update_shape = textwrap.indent(
            '\n' + ',\n'.join(assignments), '    ' * 4)

        parent_update_query = textwrap.dedent(f'''
            UPDATE schema::{refcls.__name__}
            FILTER .name__internal = <str>$__parent_classname
            SET {{{update_shape}
            }}
        ''')

        parent_variables.update(append_variables)
        blocks.append((parent_update_query, parent_variables))


@write_meta.register
def write_meta_delete_object(
    cmd: sd.DeleteObject,  # type: ignore
    *,
    classlayout: Dict[Type[so.Object], sr_struct.SchemaTypeLayout],
    schema: s_schema.Schema,
    context: sd.CommandContext,
    blocks: List[Tuple[str, Dict[str, Any]]],
    internal_schema_mode: bool,
    stdmode: bool,
) -> None:
    _descend(
        cmd,
        classlayout=classlayout,
        schema=schema,
        context=context,
        blocks=blocks,
        prerequisites=True,
        internal_schema_mode=internal_schema_mode,
        stdmode=stdmode,
    )

    _descend(
        cmd,
        classlayout=classlayout,
        schema=schema,
        context=context,
        blocks=blocks,
        internal_schema_mode=internal_schema_mode,
        stdmode=stdmode,
    )

    mcls = cmd.maybe_get_schema_metaclass()
    if mcls is not None and not issubclass(mcls, so.GlobalObject):
        if isinstance(cmd, s_ref.ReferencedObjectCommand):
            refctx = cmd.get_referrer_context(context)
        else:
            refctx = None

        if (
            refctx is not None
            and mcls.get_reflection_method() is so.ReflectionMethod.AS_LINK
        ):
            refop = refctx.op
            refcls = refop.get_schema_metaclass()
            refdict = refcls.get_refdict_for_class(mcls)

            target_link = mcls.get_reflection_link()
            assert target_link is not None

            target_field = mcls.get_field(target_link)
            target = cmd.get_orig_attribute_value(target_link)

            parent_variables = {}

            parent_variables[f'__{target_link}'] = (
                json.dumps(str(target.id))
            )

            parent_update_query = f'''
                UPDATE schema::{refcls.__name__}
                FILTER .name__internal = <str>$__parent_classname
                SET {{
                    {refdict.attr} -= (
                        SELECT DETACHED (schema::{target_field.type.__name__})
                        FILTER .id = <uuid>$__{target_link}
                    )
                }}
            '''

            ref_name = context.get_referrer_name(refctx)
            parent_variables['__parent_classname'] = (
                json.dumps(str(ref_name))
            )

            blocks.append((parent_update_query, parent_variables))

        # We need to delete any links created via reflection_proxy
        layout = classlayout[mcls]
        proxy_links = [
            link for link, layout_entry in layout.items()
            if layout_entry.reflection_proxy
        ]

        to_delete = ['D'] + [f'D.{link}' for link in proxy_links]
        operations = [f'(DELETE {x})' for x in to_delete]
        query = f'''
            WITH D := (SELECT schema::{mcls.__name__}
                       FILTER .name__internal = <str>$__classname),
            SELECT {{{", ".join(operations)}}};
        '''
        variables = {'__classname': json.dumps(str(cmd.classname))}
        blocks.append((query, variables))


@write_meta.register
def write_meta_rename_object(
    cmd: sd.RenameObject,  # type: ignore
    *,
    classlayout: Dict[Type[so.Object], sr_struct.SchemaTypeLayout],
    schema: s_schema.Schema,
    context: sd.CommandContext,
    blocks: List[Tuple[str, Dict[str, Any]]],
    internal_schema_mode: bool,
    stdmode: bool,
) -> None:
    # Delegate to the more general function, and then record the rename.
    write_meta_alter_object(
        cmd,
        classlayout=classlayout,
        schema=schema,
        context=context,
        blocks=blocks,
        internal_schema_mode=internal_schema_mode,
        stdmode=stdmode,
    )

    context.early_renames[cmd.classname] = cmd.new_name


@write_meta.register
def write_meta_nop(
    cmd: sd.Nop,
    *,
    classlayout: Dict[Type[so.Object], sr_struct.SchemaTypeLayout],
    schema: s_schema.Schema,
    context: sd.CommandContext,
    blocks: List[Tuple[str, Dict[str, Any]]],
    internal_schema_mode: bool,
    stdmode: bool,
) -> None:
    pass


@write_meta.register
def write_meta_query(
    cmd: sd.Query,
    *,
    classlayout: Dict[Type[so.Object], sr_struct.SchemaTypeLayout],
    schema: s_schema.Schema,
    context: sd.CommandContext,
    blocks: List[Tuple[str, Dict[str, Any]]],
    internal_schema_mode: bool,
    stdmode: bool,
) -> None:
    pass
