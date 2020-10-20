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


from __future__ import annotations
from typing import *

import collections
import uuid

from edb.common import adapter
from edb.common import checked
from edb.common import enum

from edb.edgeql import qltypes

from edb.schema import ddl as s_ddl
from edb.schema import delta as sd
from edb.schema import expr as s_expr
from edb.schema import inheriting as s_inh
from edb.schema import links as s_links
from edb.schema import name as sn
from edb.schema import objects as s_obj
from edb.schema import objtypes as s_objtypes
from edb.schema import schema as s_schema
from edb.schema import types as s_types


class FieldType(enum.StrEnum):
    """Field type tag for fields requiring special handling."""

    #: An Expression field.
    EXPR = 'EXPR'
    #: An ExpressionList field.
    EXPR_LIST = 'EXPR_LIST'
    #: An ObjectDict field.
    OBJ_DICT = 'OBJ_DICT'
    #: All other field types.
    OTHER = 'OTHER'


class FieldStorage(NamedTuple):
    """Schema object field storage descriptor."""

    #: Field type specifying special handling, if necessary.
    fieldtype: FieldType
    #: Pointer kind (property or link) and cardinality (single or multi)
    ptrkind: str
    #: Fully-qualified pointer target type.
    ptrtype: str
    #: Shadow pointer kind, if any.
    shadow_ptrkind: Optional[str] = None
    #: Shadow pointer type, if any.
    shadow_ptrtype: Optional[str] = None


class SchemaFieldDesc(NamedTuple):
    """Schema object field descriptor."""

    type: s_types.Type
    cardinality: qltypes.SchemaCardinality
    properties: Dict[str, Tuple[s_types.Type, FieldType]]
    fieldname: str
    is_ordered: bool = False
    reflection_proxy: Optional[Tuple[str, str]] = None
    storage: Optional[FieldStorage] = None
    is_refdict: bool = False


SchemaTypeLayout = Dict[str, SchemaFieldDesc]


def _run_ddl(
    ddl_text: str,
    *,
    schema: s_schema.Schema,
    delta: sd.Command,
) -> s_schema.Schema:

    schema, cmd = s_ddl.apply_ddl_script_ex(
        ddl_text,
        schema=schema,
        stdmode=True,
        internal_schema_mode=True,
    )

    delta.update(cmd.get_subcommands())

    return schema


def _classify_object_field(field: s_obj.Field[Any]) -> FieldStorage:
    """Determine FieldStorage for a given schema class field."""

    ftype = field.type
    shadow_ptr_kind = None
    shadow_ptr_type = None
    fieldtype = FieldType.OTHER

    if issubclass(ftype, s_obj.ObjectCollection):
        ptr_kind = 'multi link'
        ptr_type = 'schema::Object'
        if issubclass(ftype, s_obj.ObjectDict):
            fieldtype = FieldType.OBJ_DICT

    elif issubclass(ftype, s_obj.Object):
        ptr_kind = 'link'
        ptr_type = f'schema::{ftype.__name__}'

    elif issubclass(ftype, s_expr.Expression):
        shadow_ptr_kind = 'property'
        shadow_ptr_type = 'tuple<text: str, origtext: str, refs: array<uuid>>'
        ptr_kind = 'property'
        ptr_type = 'str'
        fieldtype = FieldType.EXPR

    elif issubclass(ftype, s_expr.ExpressionList):
        shadow_ptr_kind = 'property'
        shadow_ptr_type = (
            'array<tuple<text: str, origtext: str, refs: array<uuid>>>'
        )
        ptr_kind = 'property'
        ptr_type = 'array<str>'
        fieldtype = FieldType.EXPR_LIST

    elif (issubclass(ftype, (checked.CheckedList, checked.FrozenCheckedList,
                             checked.CheckedSet, checked.FrozenCheckedSet))
            and issubclass(ftype.type, str)):  # type: ignore
        ptr_kind = 'property'
        ptr_type = 'array<str>'

    elif (issubclass(ftype, (checked.CheckedList, checked.FrozenCheckedList))
            and issubclass(ftype.type, int)):  # type: ignore
        ptr_kind = 'property'
        ptr_type = 'array<int64>'

    elif issubclass(ftype, collections.abc.Mapping):
        ptr_kind = 'property'
        ptr_type = 'json'

    elif issubclass(ftype, str):
        ptr_kind = 'property'
        ptr_type = 'str'

        if field.name == 'name':
            shadow_ptr_kind = 'property'
            shadow_ptr_type = 'str'

    elif issubclass(ftype, bool):
        ptr_kind = 'property'
        ptr_type = 'bool'

    elif issubclass(ftype, int):
        ptr_kind = 'property'
        ptr_type = 'int64'

    elif issubclass(ftype, uuid.UUID):
        ptr_kind = 'property'
        ptr_type = 'uuid'

    else:
        raise RuntimeError(
            f'no metaschema reflection for field {field.name} of type {ftype}'
        )

    return FieldStorage(
        fieldtype=fieldtype,
        ptrkind=ptr_kind,
        ptrtype=ptr_type,
        shadow_ptrkind=shadow_ptr_kind,
        shadow_ptrtype=shadow_ptr_type,
    )


def get_schema_name_for_pycls(py_cls: Type[s_obj.Object]) -> str:
    py_cls_name = py_cls.__name__
    if issubclass(py_cls, s_obj.GlobalObject):
        # Global objects, like Role and Database live in the sys:: module
        return sn.Name(module='sys', name=py_cls_name)
    else:
        return sn.Name(module='schema', name=py_cls_name)


def get_default_base_for_pycls(py_cls: Type[s_obj.Object]) -> str:
    if issubclass(py_cls, s_obj.GlobalObject):
        # Global objects, like Role and Database live in the sys:: module
        return sn.Name(module='sys', name='SystemObject')
    else:
        return sn.Name(module='schema', name='Object')


def generate_structure(
    schema: s_schema.Schema,
) -> Tuple[sd.Command, Dict[Type[s_obj.Object], SchemaTypeLayout], List[str]]:
    """Generate schema reflection structure from Python schema classes.

    Returns:
        A triple containing:
            - Delta, which, when applied to stdlib, yields an enhanced
              version of the `schema` module that contains all types
              and properties, not just those that are publicly exposed
              for introspection.
            - A mapping, containing type layout description for all
              schema classes.
            - A sequence of EdgeQL queries necessary to introspect
              the schema.
    """

    delta = sd.DeltaRoot()
    classlayout: Dict[
        Type[s_obj.Object],
        SchemaTypeLayout,
    ] = {}

    ordered_link = schema.get('schema::ordered', type=s_links.Link)

    py_classes = []

    schema = _run_ddl(
        '''
            CREATE FUNCTION sys::_get_pg_type_for_scalar_type(
                typeid: std::uuid
            ) -> std::int64 {
                USING SQL $$
                    SELECT
                        coalesce(
                            (
                                SELECT
                                    tn::regtype::oid
                                FROM
                                    edgedb._get_base_scalar_type_map()
                                        AS m(tid uuid, tn text)
                                WHERE
                                    m.tid = "typeid"
                            ),
                            (
                                SELECT
                                    typ.oid
                                FROM
                                    pg_catalog.pg_type typ
                                WHERE
                                    typ.typname = "typeid"::text || '_domain'
                            ),

                            edgedb._raise_specific_exception(
                                'invalid_parameter_value',
                                'cannot determine OID of ' || typeid::text,
                                '',
                                NULL::bigint
                            )
                        )::bigint
                $$;
                SET volatility := 'STABLE';
            };

            CREATE FUNCTION sys::_expr_from_json(
                data: json
            ) -> OPTIONAL tuple<text: str, origtext: str, refs: array<uuid>> {
                USING SQL $$
                    SELECT
                        "data"->>'text'                     AS text,
                        "data"->>'origtext'                 AS origtext,
                        coalesce(r.refs, ARRAY[]::uuid[])   AS refs
                    FROM
                        (SELECT
                            array_agg(v::uuid) AS refs
                         FROM
                            jsonb_array_elements_text("data"->'refs') AS v
                        ) AS r
                    WHERE
                        jsonb_typeof("data") != 'null'
                $$;
                SET volatility := 'IMMUTABLE';
            };
        ''',
        schema=schema,
        delta=delta,
    )

    for py_cls in s_obj.ObjectMeta.get_schema_metaclasses():
        if isinstance(py_cls, adapter.Adapter):
            continue

        if py_cls is s_obj.GlobalObject:
            continue

        py_classes.append(py_cls)

    read_sets: Dict[str, List[str]] = {}

    for py_cls in py_classes:
        rschema_name = get_schema_name_for_pycls(py_cls)
        schema_objtype = schema.get(
            rschema_name,
            type=s_objtypes.ObjectType,
            default=None,
        )

        bases = []
        for base in py_cls.__bases__:
            if base in py_classes:
                bases.append(get_schema_name_for_pycls(base))

        default_base = get_default_base_for_pycls(py_cls)
        if not bases and rschema_name != default_base:
            bases.append(default_base)

        reflection = py_cls.get_reflection_method()
        is_simple_wrapper = issubclass(py_cls, s_types.CollectionExprAlias)

        if schema_objtype is None:
            as_abstract = (
                reflection is s_obj.ReflectionMethod.REGULAR
                and not is_simple_wrapper
            )

            schema = _run_ddl(
                f'''
                    CREATE {'ABSTRACT' if as_abstract else ''}
                    TYPE {rschema_name}
                    EXTENDING {', '.join(bases)};
                ''',
                schema=schema,
                delta=delta,
            )

            schema_objtype = schema.get(
                rschema_name, type=s_objtypes.ObjectType)
        else:
            ex_bases = schema_objtype.get_bases(schema).names(schema)
            _, added_bases = s_inh.delta_bases(ex_bases, bases)

            if added_bases:
                for subset, position in added_bases:
                    if isinstance(position, tuple):
                        position_clause = (
                            f'{position[0]} {position[1].name}'
                        )
                    else:
                        position_clause = position

                    bases_expr = ', '.join(t.name for t in subset)

                    stmt = f'''
                        ALTER TYPE {rschema_name} {{
                            EXTENDING {bases_expr} {position_clause}
                        }}
                    '''

                    schema = _run_ddl(
                        stmt,
                        schema=schema,
                        delta=delta,
                    )

        if reflection is s_obj.ReflectionMethod.NONE:
            continue

        referrers = py_cls.get_referring_classes()

        if reflection is s_obj.ReflectionMethod.AS_LINK:
            if not referrers:
                raise RuntimeError(
                    f'schema class {py_cls.__name__} is declared with AS_LINK '
                    f'reflection method but is not referenced in any RefDict'
                )

        is_concrete = not schema_objtype.get_is_abstract(schema)

        if (
            is_concrete
            and not is_simple_wrapper
            and any(
                not b.get_is_abstract(schema)
                for b in schema_objtype.get_ancestors(schema).objects(schema)
            )
        ):
            raise RuntimeError(
                f'non-abstract {schema_objtype.get_verbosename(schema)} has '
                f'non-abstract ancestors'
            )

        read_shape = read_sets[rschema_name] = []

        if is_concrete:
            read_shape.append(
                '_tname := .__type__[IS schema::ObjectType].name'
            )

        classlayout[py_cls] = {}
        ownfields = py_cls.get_ownfields()

        for fn, field in py_cls.get_fields().items():
            if (
                field.ephemeral
                or (
                    field.reflection_method
                    is not s_obj.ReflectionMethod.REGULAR
                )
            ):
                continue

            storage = _classify_object_field(field)

            ptr = schema_objtype.getptr(schema, fn)

            if fn in ownfields:
                qual = "REQUIRED" if field.required else "OPTIONAL"
                if ptr is None:
                    schema = _run_ddl(
                        f'''
                            ALTER TYPE {rschema_name} {{
                                CREATE {qual}
                                {storage.ptrkind} {fn} -> {storage.ptrtype};
                            }}
                        ''',
                        schema=schema,
                        delta=delta,
                    )
                    ptr = schema_objtype.getptr(schema, fn)
                    assert ptr is not None

                if storage.shadow_ptrkind is not None:
                    pn = f'{fn}__internal'
                    internal_ptr = schema_objtype.getptr(schema, pn)
                    if internal_ptr is None:
                        ptrkind = storage.shadow_ptrkind
                        ptrtype = storage.shadow_ptrtype
                        schema = _run_ddl(
                            f'''
                                ALTER TYPE {rschema_name} {{
                                    CREATE {qual}
                                    {ptrkind} {pn} -> {ptrtype};
                                }}
                            ''',
                            schema=schema,
                            delta=delta,
                        )

            else:
                assert ptr is not None

            if is_concrete:
                read_ptr = fn

                if field.type_is_generic_self:
                    read_ptr = f'{read_ptr}[IS {rschema_name}]'

                if field.reflection_proxy:
                    proxy_type, proxy_link = field.reflection_proxy
                    read_ptr = (
                        f'{read_ptr}: {{name, value := .{proxy_link}.id}}'
                    )

                if ptr.issubclass(schema, ordered_link):
                    read_ptr = f'{read_ptr} ORDER BY @index'

                read_shape.append(read_ptr)

                if storage.shadow_ptrkind is not None:
                    read_shape.append(f'{fn}__internal')

            if field.reflection_proxy:
                proxy_type_name, proxy_link_name = field.reflection_proxy
                proxy_obj = schema.get(
                    proxy_type_name, type=s_objtypes.ObjectType)
                proxy_link_obj = proxy_obj.getptr(schema, proxy_link_name)
                assert proxy_link_obj is not None
                tgt = proxy_link_obj.get_target(schema)
            else:
                tgt = ptr.get_target(schema)
            assert tgt is not None
            cardinality = ptr.get_cardinality(schema)
            assert cardinality is not None
            classlayout[py_cls][fn] = SchemaFieldDesc(
                fieldname=fn,
                type=tgt,
                cardinality=cardinality,
                properties={},
                storage=storage,
                is_ordered=ptr.issubclass(schema, ordered_link),
                reflection_proxy=field.reflection_proxy,
            )

    # Second pass: deal with RefDicts, which are reflected as links.
    for py_cls in py_classes:
        rschema_name = get_schema_name_for_pycls(py_cls)
        schema_cls = schema.get(rschema_name, type=s_objtypes.ObjectType)

        for refdict in py_cls.get_own_refdicts().values():
            ref_ptr = schema_cls.getptr(schema, refdict.attr)
            ref_cls = refdict.ref_cls
            assert issubclass(ref_cls, s_obj.Object)
            shadow_ref_ptr = None
            reflect_as_link = (
                ref_cls.get_reflection_method()
                is s_obj.ReflectionMethod.AS_LINK
            )

            if reflect_as_link:
                reflection_link = ref_cls.get_reflection_link()
                assert reflection_link is not None
                target_field = ref_cls.get_field(reflection_link)
                target_cls = target_field.type
                shadow_pn = f'{refdict.attr}__internal'

                schema = _run_ddl(
                    f'''
                        ALTER TYPE {rschema_name} {{
                            CREATE OPTIONAL MULTI LINK {shadow_pn}
                            EXTENDING schema::reference
                             -> {get_schema_name_for_pycls(ref_cls)} {{
                                 ON TARGET DELETE ALLOW;
                             }};
                        }}
                    ''',
                    schema=schema,
                    delta=delta,
                )
                shadow_ref_ptr = schema_cls.getptr(schema, shadow_pn)
                assert shadow_ref_ptr is not None
            else:
                target_cls = ref_cls

            if ref_ptr is None:
                ptr_type = get_schema_name_for_pycls(target_cls)
                schema = _run_ddl(
                    f'''
                        ALTER TYPE {rschema_name} {{
                            CREATE OPTIONAL MULTI LINK {refdict.attr}
                            EXTENDING schema::reference
                             -> {ptr_type} {{
                                 ON TARGET DELETE ALLOW;
                             }};
                        }}
                    ''',
                    schema=schema,
                    delta=delta,
                )

                ref_ptr = schema_cls.getptr(schema, refdict.attr)
            else:
                schema = _run_ddl(
                    f'''
                        ALTER TYPE {rschema_name} {{
                            ALTER LINK {refdict.attr}
                            ON TARGET DELETE ALLOW;
                        }}
                    ''',
                    schema=schema,
                    delta=delta,
                )

            assert isinstance(ref_ptr, s_links.Link)

            if py_cls not in classlayout:
                classlayout[py_cls] = {}

            # First, fields declared to be reflected as link properties.
            props = _get_reflected_link_props(
                ref_ptr=ref_ptr,
                target_cls=ref_cls,
                schema=schema,
            )

            if reflect_as_link:
                # Then, because it's a passthrough reflection, all scalar
                # fields of the proxy object.
                fields_as_props = [
                    f
                    for f in ref_cls.get_ownfields().values()
                    if (
                        not f.ephemeral
                        and (
                            f.reflection_method
                            is not s_obj.ReflectionMethod.AS_LINK
                        )
                        and f.name != refdict.backref_attr
                        and f.name != ref_cls.get_reflection_link()
                    )
                ]

                extra_props = _classify_scalar_object_fields(fields_as_props)

            for fn, storage in {**props, **extra_props}.items():
                prop_ptr = ref_ptr.getptr(schema, fn)
                if prop_ptr is None:
                    pty = storage.ptrtype
                    schema = _run_ddl(
                        f'''
                            ALTER TYPE {rschema_name} {{
                                ALTER LINK {refdict.attr} {{
                                    CREATE OPTIONAL PROPERTY {fn} -> {pty};
                                }}
                            }}
                        ''',
                        schema=schema,
                        delta=delta,
                    )

            if shadow_ref_ptr is not None:
                assert isinstance(shadow_ref_ptr, s_links.Link)
                shadow_pn = shadow_ref_ptr.get_shortname(schema).name
                for fn, storage in props.items():
                    prop_ptr = shadow_ref_ptr.getptr(schema, fn)
                    if prop_ptr is None:
                        pty = storage.ptrtype
                        schema = _run_ddl(
                            f'''
                                ALTER TYPE {rschema_name} {{
                                    ALTER LINK {shadow_pn} {{
                                        CREATE OPTIONAL PROPERTY {fn} -> {pty};
                                    }}
                                }}
                            ''',
                            schema=schema,
                            delta=delta,
                        )

    for py_cls in py_classes:
        rschema_name = get_schema_name_for_pycls(py_cls)
        schema_cls = schema.get(rschema_name, type=s_objtypes.ObjectType)

        is_concrete = not schema_cls.get_is_abstract(schema)
        read_shape = read_sets[rschema_name]

        for refdict in py_cls.get_refdicts():
            if py_cls not in classlayout:
                classlayout[py_cls] = {}

            ref_ptr = schema_cls.getptr(schema, refdict.attr)
            assert isinstance(ref_ptr, s_links.Link)
            tgt = ref_ptr.get_target(schema)
            assert tgt is not None
            cardinality = ref_ptr.get_cardinality(schema)
            assert cardinality is not None
            classlayout[py_cls][refdict.attr] = SchemaFieldDesc(
                fieldname=refdict.attr,
                type=tgt,
                cardinality=cardinality,
                properties={},
                is_ordered=ref_ptr.issubclass(schema, ordered_link),
                reflection_proxy=None,
                is_refdict=True,
            )

            target_cls = refdict.ref_cls

            props = _get_reflected_link_props(
                ref_ptr=ref_ptr,
                target_cls=target_cls,
                schema=schema,
            )

            reflect_as_link = (
                target_cls.get_reflection_method()
                is s_obj.ReflectionMethod.AS_LINK
            )

            prop_layout = {}
            extra_prop_layout = {}

            for fn, storage in props.items():
                prop_ptr = ref_ptr.getptr(schema, fn)
                assert prop_ptr is not None
                prop_tgt = prop_ptr.get_target(schema)
                assert prop_tgt is not None
                prop_layout[fn] = (prop_tgt, storage.fieldtype)

            if reflect_as_link:
                # Then, because it's a passthrough reflection, all scalar
                # fields of the proxy object.
                fields_as_props = [
                    f
                    for f in target_cls.get_ownfields().values()
                    if (
                        not f.ephemeral
                        and (
                            f.reflection_method
                            is not s_obj.ReflectionMethod.AS_LINK
                        )
                        and f.name != refdict.backref_attr
                        and f.name != target_cls.get_reflection_link()
                    )
                ]

                extra_props = _classify_scalar_object_fields(fields_as_props)

                for fn, storage in extra_props.items():
                    prop_ptr = ref_ptr.getptr(schema, fn)
                    assert prop_ptr is not None
                    prop_tgt = prop_ptr.get_target(schema)
                    assert prop_tgt is not None
                    extra_prop_layout[fn] = (prop_tgt, storage.fieldtype)
            else:
                extra_prop_layout = {}

            classlayout[py_cls][refdict.attr].properties.update({
                **prop_layout, **extra_prop_layout,
            })

            if reflect_as_link:
                shadow_tgt = schema.get(
                    get_schema_name_for_pycls(ref_cls),
                    type=s_objtypes.ObjectType,
                )

                classlayout[py_cls][f'{refdict.attr}__internal'] = (
                    SchemaFieldDesc(
                        fieldname=refdict.attr,
                        type=shadow_tgt,
                        cardinality=qltypes.SchemaCardinality.Many,
                        properties=prop_layout,
                        is_refdict=True,
                    )
                )

            if is_concrete:
                read_ptr = refdict.attr
                prop_shape_els = []

                if reflect_as_link:
                    read_ptr = f'{read_ptr}__internal'
                    ref_ptr = schema_cls.getptr(
                        schema,
                        f'{refdict.attr}__internal',
                    )
                    assert ref_ptr is not None

                for fn in props:
                    prop_shape_els.append(f'@{fn}')

                if prop_shape_els:
                    prop_shape = ',\n'.join(prop_shape_els)
                    read_ptr = f'{read_ptr}: {{id, {prop_shape}}}'

                if ref_ptr.issubclass(schema, ordered_link):
                    read_ptr = f'{read_ptr} ORDER BY @index'

                read_shape.append(read_ptr)

    union_parts = []
    for objname, shape_els in read_sets.items():
        if (
            not shape_els
            or objname in {'schema::TupleExprAlias', 'schema::ArrayExprAlias'}
        ):
            continue

        shape = ',\n'.join(shape_els)
        qry = f'''
            SELECT {objname} {{
                {shape}
            }}
        '''
        if objname not in {'schema::Tuple', 'schema::Array'}:
            qry += ' FILTER NOT .builtin'
        union_parts.append(qry)

    delta.canonical = True
    return delta, classlayout, union_parts


def _get_reflected_link_props(
    *,
    ref_ptr: s_links.Link,
    target_cls: Type[s_obj.Object],
    schema: s_schema.Schema,
) -> Dict[str, FieldStorage]:

    fields = [
        f
        for f in target_cls.get_fields().values()
        if (
            not f.ephemeral
            and (
                f.reflection_method
                is s_obj.ReflectionMethod.AS_LINK
            )
        )
    ]

    return _classify_scalar_object_fields(fields)


def _classify_scalar_object_fields(
    fields: Sequence[s_obj.Field[Any]],
) -> Dict[str, FieldStorage]:

    props = {}

    for field in fields:
        fn = field.name
        storage = _classify_object_field(field)
        if storage.ptrkind != 'property' and fn != 'id':
            continue

        props[fn] = storage

    return props
