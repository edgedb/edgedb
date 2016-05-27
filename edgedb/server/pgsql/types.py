##
# Copyright (c) 2010-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos.lang.schema.types import geo as geo_objects

from metamagic.caos.lang.schema import atoms as s_atoms
from metamagic.caos.lang.schema import concepts as s_concepts
from metamagic.caos.lang.schema import error as s_err
from metamagic.caos.lang.schema import lproperties as s_lprops
from metamagic.caos.lang.schema import name as sn
from metamagic.caos.lang.schema import types as s_types

from . import common


def parse_geo_type(connection, type_name, *type_mods):
    if type_mods:
        cls = geo_objects.GeometryMeta.class_from_name(type_mods[0])
        if cls is None:
            details = 'Could not find matching atom for GIS type %s(%s)' % \
                        (type_name, ', '.join(type_mods))
            raise s_err.SchemaError('internal metadata error', details=details)
        return s_types.proto_name_from_type(cls), ()
    else:
        details = 'Could not find matching atom for GIS type %s(%s)' % \
                        (type_name, ', '.join(type_mods))
        raise s_err.SchemaError('internal metadata error', details=details)


base_type_name_map = {
    sn.Name('metamagic.caos.builtins.str'): 'text',
    sn.Name('metamagic.caos.builtins.int'): 'bigint',
    sn.Name('metamagic.caos.builtins.sequence'): 'text',
    sn.Name('metamagic.caos.builtins.none'): 'text',
    sn.Name('metamagic.caos.builtins.decimal'): 'numeric',
    sn.Name('metamagic.caos.builtins.bool'): 'boolean',
    sn.Name('metamagic.caos.builtins.float'): 'double precision',
    sn.Name('metamagic.caos.builtins.uuid'): 'uuid',
    sn.Name('metamagic.caos.builtins.datetime'): 'timestamp with time zone',
    sn.Name('metamagic.caos.builtins.date'): 'date',
    sn.Name('metamagic.caos.builtins.time'): 'time without time zone',
    sn.Name('metamagic.caos.builtins.timedelta'): 'interval',
    sn.Name('metamagic.caos.builtins.bytes'): 'bytea',

    sn.Name('metamagic.caos.geo.point'): 'caos_aux_feat_gis.geography(POINT)',
    sn.Name('metamagic.caos.geo.linestring'): 'caos_aux_feat_gis.geography(LINESTRING)',
    sn.Name('metamagic.caos.geo.polygon'): 'caos_aux_feat_gis.geography(POLYGON)'
}

base_type_name_map_r = {
    'character varying': sn.Name('metamagic.caos.builtins.str'),
    'character': sn.Name('metamagic.caos.builtins.str'),
    'text': sn.Name('metamagic.caos.builtins.str'),
    'numeric': sn.Name('metamagic.caos.builtins.decimal'),
    'integer': sn.Name('metamagic.caos.builtins.int'),
    'bigint': sn.Name('metamagic.caos.builtins.int'),
    'int8': sn.Name('metamagic.caos.builtins.int'),
    'smallint': sn.Name('metamagic.caos.builtins.int'),
    'boolean': sn.Name('metamagic.caos.builtins.bool'),
    'bool': sn.Name('metamagic.caos.builtins.bool'),
    'double precision': sn.Name('metamagic.caos.builtins.float'),
    'float8': sn.Name('metamagic.caos.builtins.float'),
    'uuid': sn.Name('metamagic.caos.builtins.uuid'),
    'timestamp with time zone': sn.Name('metamagic.caos.builtins.datetime'),
    'timestamptz': sn.Name('metamagic.caos.builtins.datetime'),
    'date': sn.Name('metamagic.caos.builtins.date'),
    'time without time zone': sn.Name('metamagic.caos.builtins.time'),
    'time': sn.Name('metamagic.caos.builtins.time'),
    'interval': sn.Name('metamagic.caos.builtins.timedelta'),
    'bytea': sn.Name('metamagic.caos.builtins.bytes'),

    ('caos_aux_feat_gis', 'geography'): parse_geo_type
}


def get_atom_base(schema, atom):
    if atom.bases:
        # Base is another atom prototype, check if it is fundamental,
        # if not, then it is another domain.
        #
        try:
            base = base_type_name_map[atom.bases[0].name]
        except KeyError:
            base = common.atom_name_to_domain_name(atom.bases[0].name)
    else:
        # Base is a Python type, must correspond to PostgreSQL type
        try:
            base = base_type_name_map[atom.name]
        except KeyError:
            base = 'text'

    return base


def pg_type_from_atom(schema, atom, topbase=False):
    if topbase:
        base = atom.get_topmost_base()
    else:
        base = get_atom_base(schema, atom)

    if topbase:
        column_type = base_type_name_map.get(base.name)
        if not column_type:
            base_class = base.bases[0]
            column_type = base_type_name_map[base_class.adapts]
    else:
        column_type = base_type_name_map.get(atom.name)
        if column_type:
            column_type = base
        else:
            column_type = common.atom_name_to_domain_name(atom.name)

    return column_type


def pg_type_from_object(schema, obj, topbase=False):
    if isinstance(obj, s_atoms.Atom):
        return pg_type_from_atom(schema, obj, topbase=topbase)
    else:
        return common.get_table_name(obj, catenate=True)


class PointerStorageInfo:
    @classmethod
    def _source_table_info(cls, pointer):
        table = common.get_table_name(pointer.source, catenate=False)
        ptr_name = pointer.normal_name()
        col_name = common.caos_name_to_pg_name(ptr_name)
        table_type = 'concept'

        return table, table_type, col_name

    @classmethod
    def _pointer_table_info(cls, pointer):
        table = common.get_table_name(pointer, catenate=False)
        col_name = 'metamagic.caos.builtins.target'

        if pointer.atomic():
            col_name += '@atom'

        table_type = 'link'
        col_name = common.caos_name_to_pg_name(col_name)

        return table, table_type, col_name

    @classmethod
    def _resolve_type(cls, pointer, proto_schema):
        if pointer.target is not None:
            if isinstance(pointer.target, s_concepts.Concept):
                column_type = 'uuid'
            else:
                column_type = pg_type_from_object(proto_schema, pointer.target)
        else:
            # The target may not be known in circular concept-to-concept linking scenarios
            column_type = 'uuid'

        if column_type is None:
            msg = '{}: cannot determine pointer storage coltype: target is None'
            raise ValueError(msg.format(pointer.name))

        return column_type

    @classmethod
    def _storable_in_source(cls, pointer):
        return pointer.singular() and pointer.atomic()

    @classmethod
    def _storable_in_pointer(cls, pointer):
        return (not pointer.singular() or not pointer.atomic()
                                       or pointer.has_user_defined_properties())

    def __new__(cls, proto_schema, pointer, source=None, resolve_type=True, link_bias=False):
        is_prop = isinstance(pointer, s_lprops.LinkProperty)

        if resolve_type and proto_schema is None:
            msg = 'PointerStorageInfo needs a schema to resolve column_type'
            raise ValueError(msg)

        if source is None:
            source = pointer.source

        if is_prop and pointer.normal_name() == 'metamagic.caos.builtins.target':
            # Normalize link@target to link
            pointer = source
            is_prop = False

        if is_prop:
            table = common.get_table_name(source, catenate=False)
            table_type = 'link'
            col_name = common.caos_name_to_pg_name(pointer.normal_name())
        else:
            if cls._storable_in_source(pointer) and not link_bias:
                table, table_type, col_name = cls._source_table_info(pointer)
            elif cls._storable_in_pointer(pointer):
                table, table_type, col_name = cls._pointer_table_info(pointer)
            else:
                return None

        column_type = cls._resolve_type(pointer, proto_schema) if resolve_type else None

        result = super().__new__(cls)

        result.table_name = table
        result.table_type = table_type
        result.column_name = col_name
        result.column_type = column_type

        return result

    def __init__(self, *args, **kwargs):
        pass

    def __repr__(self):
        return '<{} (table_name={}, table_type={}, column_name={}, column_type={}) at 0x{:x}>'\
                    .format(self.__class__.__name__, '.'.join(self.table_name),
                            self.table_type, self.column_name, self.column_type, id(self))


def get_pointer_storage_info(pointer, *, schema=None, source=None,
                                         resolve_type=True, link_bias=False):
    assert not pointer.generic(), "only specialized pointers can be stored"
    return PointerStorageInfo(schema, pointer, source=source,
                              resolve_type=resolve_type, link_bias=link_bias)
