##
# Copyright (c) 2010 Sprymix Inc.
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
    caos.Name('semantix.caos.builtins.time'): 'time without time zone',
    caos.Name('semantix.caos.builtins.timedelta'): 'interval'
}

base_type_name_map_r = {
    'character varying': caos.Name('semantix.caos.builtins.str'),
    'character': caos.Name('semantix.caos.builtins.str'),
    'text': caos.Name('semantix.caos.builtins.str'),
    'numeric': caos.Name('semantix.caos.builtins.decimal'),
    'integer': caos.Name('semantix.caos.builtins.int'),
    'bigint': caos.Name('semantix.caos.builtins.int'),
    'smallint': caos.Name('semantix.caos.builtins.int'),
    'boolean': caos.Name('semantix.caos.builtins.bool'),
    'double precision': caos.Name('semantix.caos.builtins.float'),
    'uuid': caos.Name('semantix.caos.builtins.uuid'),
    'timestamp with time zone': caos.Name('semantix.caos.builtins.datetime'),
    'time without time zone': caos.Name('semantix.caos.builtins.time'),
    'interval': caos.Name('semantix.caos.builtins.timedelta')
}


def get_atom_base_and_constraints(meta, atom, own_only=True):
    constraints = set()
    extraconstraints = set()
    constraints_encoded = ()

    atom_constraints = atom.effective_local_constraints if own_only else atom.constraints

    if proto.Atom.is_prototype(atom.base):
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
            base_class = meta.get(base.base, include_pyobjects=True)
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
