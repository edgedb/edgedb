##
# Copyright (c) 2010-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix import caos
from semantix.caos import proto

from semantix.utils import helper

from . import common


base_type_name_map = {
    caos.Name('semantix.caos.builtins.str'): 'text',
    caos.Name('semantix.caos.builtins.int'): 'bigint',
    caos.Name('semantix.caos.builtins.sequence'): 'text',
    caos.Name('semantix.caos.builtins.none'): 'text',
    caos.Name('semantix.caos.builtins.decimal'): 'numeric',
    caos.Name('semantix.caos.builtins.bool'): 'boolean',
    caos.Name('semantix.caos.builtins.float'): 'double precision',
    caos.Name('semantix.caos.builtins.uuid'): 'uuid',
    caos.Name('semantix.caos.builtins.datetime'): 'timestamp with time zone',
    caos.Name('semantix.caos.builtins.date'): 'date',
    caos.Name('semantix.caos.builtins.time'): 'time without time zone',
    caos.Name('semantix.caos.builtins.timedelta'): 'interval',
    caos.Name('semantix.caos.builtins.bytes'): 'bytea'
}

base_type_name_map_r = {
    'character varying': caos.Name('semantix.caos.builtins.str'),
    'character': caos.Name('semantix.caos.builtins.str'),
    'text': caos.Name('semantix.caos.builtins.str'),
    'numeric': caos.Name('semantix.caos.builtins.decimal'),
    'integer': caos.Name('semantix.caos.builtins.int'),
    'bigint': caos.Name('semantix.caos.builtins.int'),
    'int8': caos.Name('semantix.caos.builtins.int'),
    'smallint': caos.Name('semantix.caos.builtins.int'),
    'boolean': caos.Name('semantix.caos.builtins.bool'),
    'bool': caos.Name('semantix.caos.builtins.bool'),
    'double precision': caos.Name('semantix.caos.builtins.float'),
    'float8': caos.Name('semantix.caos.builtins.float'),
    'uuid': caos.Name('semantix.caos.builtins.uuid'),
    'timestamp with time zone': caos.Name('semantix.caos.builtins.datetime'),
    'timestamptz': caos.Name('semantix.caos.builtins.datetime'),
    'date': caos.Name('semantix.caos.builtins.date'),
    'time without time zone': caos.Name('semantix.caos.builtins.time'),
    'time': caos.Name('semantix.caos.builtins.time'),
    'interval': caos.Name('semantix.caos.builtins.timedelta'),
    'bytea': caos.Name('semantix.caos.builtins.bytes')
}


def get_atom_base_and_constraints(meta, atom, own_only=True):
    constraints = set()
    extraconstraints = set()
    constraints_encoded = ()

    atom_constraints = atom.effective_local_constraints if own_only else atom.constraints

    base = meta.get(atom.base, include_pyobjects=True, index_only=False)

    if isinstance(base, caos.types.ProtoAtom):
        # Base is another atom prototype, check if it is fundamental,
        # if not, then it is another domain
        base = base_type_name_map.get(atom.base)
        if base:
            if not isinstance(base, str):
                base, constraints_encoded = base(meta.get(atom.base), atom_constraints)
        else:
            base = common.atom_name_to_domain_name(atom.base)
    else:
        # Base is a Python type, must correspond to PostgreSQL type
        base = base_type_name_map.get(atom.name)
        if not base:
            base_class = helper.get_object(str(atom.base))
            base_type = getattr(base_class, 'adapts', None)
            assert base_type, '"%s" is not in builtins and does not define "adapts" attribute' \
                              % atom.base
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
            base_class = meta.get(base.base, include_pyobjects=True, index_only=False)
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
        return common.get_record_name(obj, catenate=True)


class PointerStorageInfo:
    def __init__(self, proto_schema, pointer, resolve_type=True, record_mode=False):
        is_prop = isinstance(pointer, caos.types.ProtoLinkProperty)

        if is_prop and pointer.normal_name() in {'semantix.caos.builtins.source',
                                                 'semantix.caos.builtins.target'}:
            record_mode = False

        if not is_prop and (not pointer.atomic() or not pointer.singular()):
            table = common.get_table_name(pointer, catenate=False)
            ptr_type = 'generic'
            if not record_mode:
                col_name = 'semantix.caos.builtins.target'
                if pointer.atomic():
                    col_name += '@atom'
                    ptr_type = 'specialized'
            else:
                col_name = pointer.normal_name()

            table_type = ('pointer', ptr_type)
            col_name = common.caos_name_to_pg_name(col_name)
        else:
            table = common.get_table_name(pointer.source, catenate=False)
            ptr_name = pointer.normal_name()

            if ptr_name == 'semantix.caos.builtins.target' and pointer.atomic():
                ptr_name += '@atom'

            col_name = common.caos_name_to_pg_name(ptr_name)
            table_type = ('source', 'generic')

        self.table_name = table
        self.table_type = table_type
        self.column_name = col_name
        self.in_record = pointer.get_loading_behaviour() == caos.types.EagerLoading
        self.column_type = None

        if resolve_type:
            if pointer.target is not None:
                if isinstance(pointer.target, caos.types.ProtoConcept) and not record_mode:
                    self.column_type = 'uuid'
                else:
                    self.column_type = pg_type_from_object(proto_schema, pointer.target)
            elif not record_mode:
                # The target may not be known in circular concept-to-concept linking scenarios
                self.column_type = 'uuid'

            if self.column_type is None:
                msg = '{}: cannot determine pointer storage coltype: target is None'
                raise ValueError(msg.format(pointer.name))

            if not pointer.singular() and record_mode:
                self.column_type += '[]'


def get_pointer_storage_info(proto_schema, pointer, resolve_type=True, record_mode=False):
    assert not pointer.generic(), "only specialized pointers can be stored"
    return PointerStorageInfo(proto_schema, pointer, resolve_type=resolve_type,
                                                     record_mode=record_mode)
