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
from edb.common import verutils

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
    #: An ExpressionDict field.
    EXPR_DICT = 'EXPR_DICT'
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
    schema_fieldname: str
    is_ordered: bool = False
    reflection_proxy: Optional[Tuple[str, str]] = None
    storage: Optional[FieldStorage] = None
    is_refdict: bool = False


# N.B: Indexed by schema_fieldname
SchemaTypeLayout = Dict[str, SchemaFieldDesc]


class SchemaReflectionParts(NamedTuple):

    intro_schema_delta: sd.Command
    class_layout: Dict[Type[s_obj.Object], SchemaTypeLayout]
    local_intro_parts: List[str]
    global_intro_parts: List[str]


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

    is_array = is_multiprop = False
    if issubclass(ftype, s_obj.MultiPropSet):
        is_multiprop = True
        ftype = ftype.type
    elif (
        issubclass(
            ftype,
            (checked.CheckedList, checked.FrozenCheckedList,
             checked.CheckedSet, checked.FrozenCheckedSet))
        and not issubclass(ftype, s_expr.ExpressionList)
    ):
        is_array = True
        ftype = ftype.type  # type: ignore

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
        shadow_ptr_type = 'tuple<text: str, refs: array<uuid>>'
        ptr_kind = 'property'
        ptr_type = 'str'
        fieldtype = FieldType.EXPR

    elif issubclass(ftype, s_expr.ExpressionList):
        shadow_ptr_kind = 'property'
        shadow_ptr_type = (
            'array<tuple<text: str, refs: array<uuid>>>'
        )
        ptr_kind = 'property'
        ptr_type = 'array<str>'
        fieldtype = FieldType.EXPR_LIST

    elif issubclass(ftype, s_expr.ExpressionDict):
        shadow_ptr_kind = 'property'
        shadow_ptr_type = '''array<tuple<
            name: str,
            expr: tuple<text: str, refs: array<uuid>>
        >>'''
        ptr_kind = 'property'
        ptr_type = 'array<str>'
        fieldtype = FieldType.EXPR_DICT

    elif issubclass(ftype, collections.abc.Mapping):
        ptr_kind = 'property'
        ptr_type = 'json'

    elif issubclass(ftype, (str, sn.Name)):
        ptr_kind = 'property'
        ptr_type = 'str'

        if field.name == 'name':
            # TODO: consider shadow-reflecting names as tuples
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

    elif issubclass(ftype, verutils.Version):
        ptr_kind = 'property'
        ptr_type = '''
            tuple<
                major: std::int64,
                minor: std::int64,
                stage: sys::VersionStage,
                stage_no: std::int64,
                local: array<std::str>,
            >
        '''
    else:
        raise RuntimeError(
            f'no metaschema reflection for field {field.name} of type {ftype}'
        )

    if is_multiprop:
        ptr_kind = 'multi property'
    if is_array:
        ptr_type = f'array<{ptr_type}>'

    return FieldStorage(
        fieldtype=fieldtype,
        ptrkind=ptr_kind,
        ptrtype=ptr_type,
        shadow_ptrkind=shadow_ptr_kind,
        shadow_ptrtype=shadow_ptr_type,
    )


def get_schema_name_for_pycls(py_cls: Type[s_obj.Object]) -> sn.Name:
    py_cls_name = py_cls.__name__
    if issubclass(py_cls, s_obj.GlobalObject):
        # Global objects, like Role and Database live in the sys:: module
        return sn.QualName(module='sys', name=py_cls_name)
    else:
        return sn.QualName(module='schema', name=py_cls_name)


def get_default_base_for_pycls(py_cls: Type[s_obj.Object]) -> sn.Name:
    if issubclass(py_cls, s_obj.GlobalObject):
        # Global objects, like Role and Database live in the sys:: module
        return sn.QualName(module='sys', name='SystemObject')
    else:
        return sn.QualName(module='schema', name='Object')


def generate_structure(
    schema: s_schema.Schema,
    *,
    make_funcs: bool=True,
) -> SchemaReflectionParts:
    """Generate schema reflection structure from Python schema classes.

    Returns:
        A quadruple (as a SchemaReflectionParts instance) containing:
            - Delta, which, when applied to stdlib, yields an enhanced
              version of the `schema` module that contains all types
              and properties, not just those that are publicly exposed
              for introspection.
            - A mapping, containing type layout description for all
              schema classes.
            - A sequence of EdgeQL queries necessary to introspect
              a database schema.
            - A sequence of EdgeQL queries necessary to introspect
              global objects, such as roles and databases.
    """

    delta = sd.DeltaRoot()
    classlayout: Dict[
        Type[s_obj.Object],
        SchemaTypeLayout,
    ] = {}

    ordered_link = schema.get('schema::ordered', type=s_links.Link)

    if make_funcs:
        schema = _run_ddl(
            '''
            CREATE FUNCTION sys::_get_pg_type_for_edgedb_type(
                typeid: std::uuid,
                kind: std::str,
                elemid: OPTIONAL std::uuid,
            ) -> std::int64 {
                USING SQL FUNCTION 'edgedb.get_pg_type_for_edgedb_type';
                SET volatility := 'STABLE';
                SET impl_is_strict := false;
            };

            CREATE FUNCTION sys::_expr_from_json(
                data: json
            ) -> OPTIONAL tuple<text: str, refs: array<uuid>> {
                USING SQL $$
                    SELECT
                        "data"->>'text'                     AS text,
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

    py_classes = []
    for py_cls in s_obj.ObjectMeta.get_schema_metaclasses():
        if isinstance(py_cls, adapter.Adapter):
            continue

        if py_cls is s_obj.GlobalObject:
            continue

        py_classes.append(py_cls)

    read_sets: Dict[Type[s_obj.Object], List[str]] = {}

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
                and (
                    py_cls is s_obj.InternalObject
                    or not issubclass(py_cls, s_obj.InternalObject)
                )
            )

            schema = _run_ddl(
                f'''
                    CREATE {'ABSTRACT' if as_abstract else ''}
                    TYPE {rschema_name}
                    EXTENDING {', '.join(str(b) for b in bases)};
                ''',
                schema=schema,
                delta=delta,
            )

            schema_objtype = schema.get(
                rschema_name, type=s_objtypes.ObjectType)
        else:
            ex_bases = schema_objtype.get_bases(schema).names(schema)
            _, added_bases = s_inh.delta_bases(
                ex_bases,
                bases,
                t=type(schema_objtype),
            )

            if added_bases:
                for subset, position in added_bases:
                    # XXX: Don't generate changes for just moving around the
                    # order of types when the mismatch between python and
                    # the schema, since it doesn't work anyway and causes mass
                    # grief when trying to patch the schema.
                    subset = [x for x in subset if x.name not in ex_bases]
                    if not subset:
                        continue

                    if isinstance(position, tuple):
                        position_clause = (
                            f'{position[0]} {position[1].name}'
                        )
                    else:
                        position_clause = position

                    bases_expr = ', '.join(str(t.name) for t in subset)

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

        is_concrete = not schema_objtype.get_abstract(schema)

        if (
            is_concrete
            and not is_simple_wrapper
            and any(
                not b.get_abstract(schema)
                for b in schema_objtype.get_ancestors(schema).objects(schema)
            )
        ):
            raise RuntimeError(
                f'non-abstract {schema_objtype.get_verbosename(schema)} has '
                f'non-abstract ancestors'
            )

        read_shape = read_sets[py_cls] = []

        if is_concrete:
            read_shape.append(
                '_tname := .__type__[IS schema::ObjectType].name'
            )

        classlayout[py_cls] = {}
        ownfields = py_cls.get_ownfields()

        for fn, field in py_cls.get_fields().items():
            sfn = field.sname

            if (
                field.ephemeral
                or (
                    field.reflection_method
                    is not s_obj.ReflectionMethod.REGULAR
                )
            ):
                continue

            storage = _classify_object_field(field)

            ptr = schema_objtype.maybe_get_ptr(schema, sn.UnqualName(sfn))

            if fn in ownfields:
                qual = "REQUIRED" if field.required else "OPTIONAL"
                otd = " { ON TARGET DELETE ALLOW }" if field.weak_ref else ""
                if ptr is None:
                    schema = _run_ddl(
                        f'''
                            ALTER TYPE {rschema_name} {{
                                CREATE {qual}
                                {storage.ptrkind} {sfn} -> {storage.ptrtype}
                                {otd};
                            }}
                        ''',
                        schema=schema,
                        delta=delta,
                    )
                    ptr = schema_objtype.getptr(schema, sn.UnqualName(fn))

                if storage.shadow_ptrkind is not None:
                    pn = f'{sfn}__internal'
                    internal_ptr = schema_objtype.maybe_get_ptr(
                        schema, sn.UnqualName(pn))
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
                read_ptr = sfn

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
                    read_shape.append(f'{sfn}__internal')

            if field.reflection_proxy:
                proxy_type_name, proxy_link_name = field.reflection_proxy
                proxy_obj = schema.get(
                    proxy_type_name, type=s_objtypes.ObjectType)
                proxy_link_obj = proxy_obj.getptr(
                    schema, sn.UnqualName(proxy_link_name))
                tgt = proxy_link_obj.get_target(schema)
            else:
                tgt = ptr.get_target(schema)
            assert tgt is not None
            cardinality = ptr.get_cardinality(schema)
            assert cardinality is not None
            classlayout[py_cls][sfn] = SchemaFieldDesc(
                fieldname=fn,
                schema_fieldname=sfn,
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
            ref_ptr = schema_cls.maybe_get_ptr(
                schema, sn.UnqualName(refdict.attr))
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
                shadow_ref_ptr = schema_cls.maybe_get_ptr(
                    schema, sn.UnqualName(shadow_pn))

            if reflect_as_link and not shadow_ref_ptr:
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
                shadow_ref_ptr = schema_cls.getptr(
                    schema, sn.UnqualName(shadow_pn))
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

                ref_ptr = schema_cls.getptr(
                    schema, sn.UnqualName(refdict.attr))

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

            for field, storage in {**props, **extra_props}.items():
                sfn = field.sname
                prop_ptr = ref_ptr.maybe_get_ptr(schema, sn.UnqualName(sfn))
                if prop_ptr is None:
                    pty = storage.ptrtype
                    schema = _run_ddl(
                        f'''
                            ALTER TYPE {rschema_name} {{
                                ALTER LINK {refdict.attr} {{
                                    CREATE OPTIONAL PROPERTY {sfn} -> {pty};
                                }}
                            }}
                        ''',
                        schema=schema,
                        delta=delta,
                    )

            if shadow_ref_ptr is not None:
                assert isinstance(shadow_ref_ptr, s_links.Link)
                shadow_pn = shadow_ref_ptr.get_shortname(schema).name
                for field, storage in props.items():
                    sfn = field.sname
                    prop_ptr = shadow_ref_ptr.maybe_get_ptr(
                        schema, sn.UnqualName(sfn))
                    if prop_ptr is None:
                        pty = storage.ptrtype
                        schema = _run_ddl(
                            f'''
                                ALTER TYPE {rschema_name} {{
                                    ALTER LINK {shadow_pn} {{
                                        CREATE OPTIONAL PROPERTY {sfn}
                                            -> {pty};
                                    }}
                                }}
                            ''',
                            schema=schema,
                            delta=delta,
                        )

    for py_cls in py_classes:
        rschema_name = get_schema_name_for_pycls(py_cls)
        schema_cls = schema.get(rschema_name, type=s_objtypes.ObjectType)

        is_concrete = not schema_cls.get_abstract(schema)
        read_shape = read_sets[py_cls]

        for refdict in py_cls.get_refdicts():
            if py_cls not in classlayout:
                classlayout[py_cls] = {}

            ref_ptr = schema_cls.getptr(
                schema, sn.UnqualName(refdict.attr), type=s_links.Link)
            tgt = ref_ptr.get_target(schema)
            assert tgt is not None
            cardinality = ref_ptr.get_cardinality(schema)
            assert cardinality is not None
            classlayout[py_cls][refdict.attr] = SchemaFieldDesc(
                fieldname=refdict.attr,
                schema_fieldname=refdict.attr,
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

            for field, storage in props.items():
                prop_ptr = ref_ptr.getptr(schema, sn.UnqualName(field.sname))
                prop_tgt = prop_ptr.get_target(schema)
                assert prop_tgt is not None
                prop_layout[field.name] = (prop_tgt, storage.fieldtype)

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

                for field, storage in extra_props.items():
                    prop_ptr = ref_ptr.getptr(
                        schema, sn.UnqualName(field.sname))
                    prop_tgt = prop_ptr.get_target(schema)
                    assert prop_tgt is not None
                    extra_prop_layout[field.name] = (
                        prop_tgt, storage.fieldtype)
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

                iname = f'{refdict.attr}__internal'
                classlayout[py_cls][iname] = (
                    SchemaFieldDesc(
                        fieldname=refdict.attr,
                        schema_fieldname=iname,
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
                        sn.UnqualName(f'{refdict.attr}__internal'),
                    )

                for field in props:
                    sfn = field.sname
                    prop_shape_els.append(f'@{sfn}')

                if prop_shape_els:
                    prop_shape = ',\n'.join(prop_shape_els)
                    read_ptr = f'{read_ptr}: {{id, {prop_shape}}}'

                if ref_ptr.issubclass(schema, ordered_link):
                    read_ptr = f'{read_ptr} ORDER BY @index'

                read_shape.append(read_ptr)

    local_parts = []
    global_parts = []
    for py_cls, shape_els in read_sets.items():
        if (
            not shape_els
            # The CollectionExprAlias family needs to be excluded
            # because TupleExprAlias and ArrayExprAlias inherit from
            # concrete classes and so are picked up from those.
            or issubclass(py_cls, s_types.CollectionExprAlias)
        ):
            continue

        rschema_name = get_schema_name_for_pycls(py_cls)
        shape = ',\n'.join(shape_els)
        qry = f'''
            SELECT {rschema_name} {{
                {shape}
            }}
        '''
        if not issubclass(py_cls, (s_types.Collection, s_obj.GlobalObject)):
            qry += ' FILTER NOT .builtin'

        if issubclass(py_cls, s_obj.GlobalObject):
            global_parts.append(qry)
        else:
            local_parts.append(qry)

    delta.canonical = True
    return SchemaReflectionParts(
        intro_schema_delta=delta,
        class_layout=classlayout,
        local_intro_parts=local_parts,
        global_intro_parts=global_parts,
    )


def _get_reflected_link_props(
    *,
    ref_ptr: s_links.Link,
    target_cls: Type[s_obj.Object],
    schema: s_schema.Schema,
) -> Dict[s_obj.Field[Any], FieldStorage]:

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
) -> Dict[s_obj.Field[Any], FieldStorage]:

    props = {}

    for field in fields:
        fn = field.name
        storage = _classify_object_field(field)
        if storage.ptrkind != 'property' and fn != 'id':
            continue

        props[field] = storage

    return props
