##
# Copyright (c) 2010-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic import caos
from metamagic.caos import proto
from metamagic.utils.lang.import_ import get_object
from metamagic.caos.objects import geo as geo_objects

from . import common


def parse_geo_type(connection, type_name, *type_mods):
    if type_mods:
        cls = geo_objects.GeometryMeta.class_from_name(type_mods[0])
        if cls is None:
            details = 'Could not find matching atom for GIS type %s(%s)' % \
                        (type_name, ', '.join(type_mods))
            raise caos.types.MetaError('internal metadata error', details=details)
        return caos.types.proto_name_from_type(cls), ()
    else:
        details = 'Could not find matching atom for GIS type %s(%s)' % \
                        (type_name, ', '.join(type_mods))
        raise caos.types.MetaError('internal metadata error', details=details)


base_type_name_map = {
    caos.Name('metamagic.caos.builtins.str'): 'text',
    caos.Name('metamagic.caos.builtins.int'): 'bigint',
    caos.Name('metamagic.caos.builtins.sequence'): 'text',
    caos.Name('metamagic.caos.builtins.none'): 'text',
    caos.Name('metamagic.caos.builtins.decimal'): 'numeric',
    caos.Name('metamagic.caos.builtins.bool'): 'boolean',
    caos.Name('metamagic.caos.builtins.float'): 'double precision',
    caos.Name('metamagic.caos.builtins.uuid'): 'uuid',
    caos.Name('metamagic.caos.builtins.datetime'): 'timestamp with time zone',
    caos.Name('metamagic.caos.builtins.date'): 'date',
    caos.Name('metamagic.caos.builtins.time'): 'time without time zone',
    caos.Name('metamagic.caos.builtins.timedelta'): 'interval',
    caos.Name('metamagic.caos.builtins.bytes'): 'bytea',

    caos.Name('metamagic.caos.geo.point'): 'caos_aux_feat_gis.geography(POINT)',
    caos.Name('metamagic.caos.geo.linestring'): 'caos_aux_feat_gis.geography(LINESTRING)',
    caos.Name('metamagic.caos.geo.polygon'): 'caos_aux_feat_gis.geography(POLYGON)'
}

base_type_name_map_r = {
    'character varying': caos.Name('metamagic.caos.builtins.str'),
    'character': caos.Name('metamagic.caos.builtins.str'),
    'text': caos.Name('metamagic.caos.builtins.str'),
    'numeric': caos.Name('metamagic.caos.builtins.decimal'),
    'integer': caos.Name('metamagic.caos.builtins.int'),
    'bigint': caos.Name('metamagic.caos.builtins.int'),
    'int8': caos.Name('metamagic.caos.builtins.int'),
    'smallint': caos.Name('metamagic.caos.builtins.int'),
    'boolean': caos.Name('metamagic.caos.builtins.bool'),
    'bool': caos.Name('metamagic.caos.builtins.bool'),
    'double precision': caos.Name('metamagic.caos.builtins.float'),
    'float8': caos.Name('metamagic.caos.builtins.float'),
    'uuid': caos.Name('metamagic.caos.builtins.uuid'),
    'timestamp with time zone': caos.Name('metamagic.caos.builtins.datetime'),
    'timestamptz': caos.Name('metamagic.caos.builtins.datetime'),
    'date': caos.Name('metamagic.caos.builtins.date'),
    'time without time zone': caos.Name('metamagic.caos.builtins.time'),
    'time': caos.Name('metamagic.caos.builtins.time'),
    'interval': caos.Name('metamagic.caos.builtins.timedelta'),
    'bytea': caos.Name('metamagic.caos.builtins.bytes'),

    ('caos_aux_feat_gis', 'geography'): parse_geo_type
}


def get_atom_base_and_constraints(meta, atom, own_only=True):
    constraints = set()
    extraconstraints = set()
    constraints_encoded = ()

    atom_constraints = atom.effective_local_constraints if own_only else atom.constraints

    base = atom.bases[0]

    if isinstance(base, caos.types.ProtoAtom):
        # Base is another atom prototype, check if it is fundamental,
        # if not, then it is another domain
        base = base_type_name_map.get(atom.bases[0].name)
        if base:
            if not isinstance(base, str):
                base, constraints_encoded = base(atom.bases[0], atom_constraints)
        else:
            base = common.atom_name_to_domain_name(atom.bases[0].name)
    else:
        # Base is a Python type, must correspond to PostgreSQL type
        base = base_type_name_map.get(atom.name)
        if not base:
            base_class = get_object(str(atom.bases[0].class_name))
            base_type = getattr(base_class, 'adapts', None)
            assert base_type, '"%s" is not in builtins and does not define "adapts" attribute' \
                              % atom.bases[0].name
            base = base_type_name_map[base_type]

        if not isinstance(base, str):
            base, constraints_encoded = base(meta.get(atom.name), atom_constraints)

    directly_supported_constraints = (proto.AtomConstraintMaxLength, proto.AtomConstraintMinLength,
                               proto.AtomConstraintRegExp, proto.AtomConstraintMaxValue,
                               proto.AtomConstraintMaxExValue, proto.AtomConstraintMinValue,
                               proto.AtomConstraintMinExValue)

    for constraint in atom_constraints.values():
        if constraint in constraints_encoded:
            continue
        elif isinstance(constraint, directly_supported_constraints):
            constraints.add(constraint)
        else:
            extraconstraints.add(constraint)

    return base, constraints_encoded, constraints, extraconstraints


def pg_type_from_atom(meta, atom, topbase=False):
    if topbase:
        base = atom.get_topmost_base(meta, top_prototype=True)
    else:
        base, _, constraints, _ = get_atom_base_and_constraints(meta, atom)

    if topbase:
        column_type = base_type_name_map.get(base.name)
        if not column_type:
            base_class = base.bases[0]
            column_type = base_type_name_map[base_class.adapts]
    elif not atom.automatic:
        column_type = base_type_name_map.get(atom.name)
        if column_type:
            column_type = base
        else:
            column_type = common.atom_name_to_domain_name(atom.name)
    else:
        column_type = base

    return column_type


def pg_type_from_object(schema, obj, topbase=False):
    if isinstance(obj, caos.types.ProtoAtom):
        return pg_type_from_atom(schema, obj, topbase=topbase)
    else:
        return common.get_table_name(obj, catenate=True)


class PointerStorageInfo:
    def __init__(self, proto_schema, pointer, resolve_type=True):
        is_prop = isinstance(pointer, caos.types.ProtoLinkProperty)

        if not is_prop and (not pointer.atomic() or not pointer.singular()):
            table = common.get_table_name(pointer, catenate=False)
            ptr_type = 'generic'
            col_name = 'metamagic.caos.builtins.target'
            if pointer.atomic():
                col_name += '@atom'
                ptr_type = 'specialized'

            table_type = ('pointer', ptr_type)
            col_name = common.caos_name_to_pg_name(col_name)
        else:
            table = common.get_table_name(pointer.source, catenate=False)
            ptr_name = pointer.normal_name()

            if ptr_name == 'metamagic.caos.builtins.target' and pointer.atomic():
                ptr_name += '@atom'

            col_name = common.caos_name_to_pg_name(ptr_name)
            table_type = ('source', 'generic')

        self.table_name = table
        self.table_type = table_type
        self.column_name = col_name
        self.column_type = None

        if resolve_type:
            if pointer.target is not None:
                if isinstance(pointer.target, caos.types.ProtoConcept):
                    self.column_type = 'uuid'
                else:
                    self.column_type = pg_type_from_object(proto_schema, pointer.target)
            else:
                # The target may not be known in circular concept-to-concept linking scenarios
                self.column_type = 'uuid'

            if self.column_type is None:
                msg = '{}: cannot determine pointer storage coltype: target is None'
                raise ValueError(msg.format(pointer.name))


def get_pointer_storage_info(proto_schema, pointer, resolve_type=True):
    assert not pointer.generic(), "only specialized pointers can be stored"
    return PointerStorageInfo(proto_schema, pointer, resolve_type=resolve_type)
