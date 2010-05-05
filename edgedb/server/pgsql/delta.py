##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import itertools
import math
import os
import re

import postgresql

from semantix import caos
from semantix.caos import proto
from semantix.caos import delta as delta_cmds

from semantix.caos.backends.pgsql import common, Config

from semantix.utils import datastructures
from semantix.utils.debug import debug
from semantix.utils.lang import yaml
from semantix.utils.algos.persistent_hash import persistent_hash

from . import datasources


numeric_spec = collections.OrderedDict((
                    #(((-32768, 32767), 0), 'smallint'),
                    #(((-2147483648, 2147483647), 0), 'int'),
                    (((-9223372036854775808, 9223372036854775807), 0), 'bigint'),
                    (None, 'numeric')
               ))

def numeric_to_pg(atom, mods, force_numeric=False):
    min_value = mods.get(proto.AtomModMinValue)
    min_value_ex = mods.get(proto.AtomModMinExValue)
    max_value = mods.get(proto.AtomModMaxValue)
    max_value_ex = mods.get(proto.AtomModMaxExValue)
    precision_mod = mods.get(proto.AtomModPrecision)

    int_precision = precision = max_length = upper = lower = None

    if precision_mod:
        precision = precision_mod.value
        if precision[1]:
            int_precision = precision[0] - precision[1]
        else:
            int_precision = precision[0]
        max_length = 10 ** int_precision - 1

    if min_value and min_value_ex:
        lower = max(min_value.value, min_value_ex.value)
    elif min_value:
        lower = min_value.value
    elif min_value_ex:
        lower = min_value_ex.value

    if max_value and max_value_ex:
        upper = max(max_value.value, max_value_ex.value)
    elif max_value:
        upper = max_value.value
    elif max_value_ex:
        upper = max_value_ex.value

    if max_length is not None:
        upper = min(max_length, upper) if upper is not None else max_length
        lower = max(-max_length, lower) if lower is not None else -max_length


    mod_used = set()

    if force_numeric:
        type = 'numeric'
    else:
        type = None
        for spec, type in numeric_spec.items():
            if spec is None:
                break
            else:
                length, spec_precision = spec
                within_bounds = True
                if length is not None:
                    spec_lower, spec_upper = length
                    within_bounds = (spec_lower is None or \
                                         (lower is not None and lower > spec_lower)) \
                                    and (spec_upper is None or \
                                         (upper is not None and upper < spec_upper))

                within_bounds = within_bounds and \
                                (spec_precision is None \
                                 or (precision is None or precision[1] <= spec_precision))

                if within_bounds:
                    break
        else:
            type = None

    if type == 'numeric':
        if upper:
            len = math.ceil(math.log(upper, 10))
            if precision:
                if len == int_precision:
                    mod_used.add(precision_mod)

                scale = precision[1] or 0
                type = 'numeric(%d,%d)' % (len + scale, scale)
            else:
                type = 'numeric(%d)' % len
        # if there is no upper limit on length, we cannot use any modifiers
        # on the numeric type, including precision, even if it is specified,
        # and need to use unlimited version instead

    return type, mod_used


def decimal_to_pg(atom, mods):
    return numeric_to_pg(atom, mods, force_numeric=True)


def str_to_pg(atom, mods):
    max_length_mod = mods.get(proto.AtomModMaxLength)

    if max_length_mod:
        return 'character varying (%d)' % max_length_mod.value, (max_length_mod,)
    else:
        return 'text', ()


base_type_name_map = {
    caos.Name('semantix.caos.builtins.str'): str_to_pg,
    caos.Name('semantix.caos.builtins.int'): numeric_to_pg,
    caos.Name('semantix.caos.builtins.decimal'): decimal_to_pg,
    caos.Name('semantix.caos.builtins.bool'): 'boolean',
    caos.Name('semantix.caos.builtins.float'): 'double precision',
    caos.Name('semantix.caos.builtins.uuid'): 'uuid',
    caos.Name('semantix.caos.builtins.datetime'): 'timestamp with time zone',
    caos.Name('semantix.caos.builtins.timedelta'): 'interval'
}


def str_from_pg(type, typmod):
    name = caos.Name('semantix.caos.builtins.str')
    mods = []

    assert typmod is None or len(typmod) == 1

    if typmod:
        mods.append(proto.AtomModMaxLength(int(typmod[0])))

    return name, mods


def numeric_from_pg(type, typmod):
    if typmod and len(typmod) == 2:
        name = caos.Name('semantix.caos.builtins.decimal')
        mods = [proto.AtomModPrecision((int(typmod[0]), int(typmod[1])))]
    else:
        name = caos.Name('semantix.caos.builtins.int')
        if typmod:
            mods = [proto.AtomModPrecision((int(typmod[0])))]
        else:
            mods = ()

    return name, mods


base_type_name_map_r = {
    'character varying': str_from_pg,
    'character': str_from_pg,
    'text': str_from_pg,
    'numeric': numeric_from_pg,
    'integer': numeric_from_pg,
    'bigint': numeric_from_pg,
    'smallint': numeric_from_pg,
    'boolean': caos.Name('semantix.caos.builtins.bool'),
    'double precision': caos.Name('semantix.caos.builtins.float'),
    'uuid': caos.Name('semantix.caos.builtins.uuid'),
    'timestamp with time zone': caos.Name('semantix.caos.builtins.datetime'),
    'interval': caos.Name('semantix.caos.builtins.timedelta')
}


class CommandMeta(delta_cmds.CommandMeta):
    pass


class MetaCommand(delta_cmds.Command, metaclass=CommandMeta):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pgops = datastructures.OrderedSet()

    def apply(self, meta, context=None):
        for op in self.ops:
            self.pgops.add(op)

    def execute(self, context):
        for op in sorted(self.pgops, key=lambda i: i.priority, reverse=True):
            op.execute(context)

    def dump(self):
        result = [repr(self)]

        for op in self.pgops:
            result.extend('  %s' % l for l in op.dump().split('\n'))

        return '\n'.join(result)


class BaseCommand:
    def get_code_and_vars(self, context):
        code = self.code(context)
        assert code
        if isinstance(code, tuple):
            code, vars = code
        else:
            vars = None

        return code, vars

    @debug
    def execute(self, context):
        code, vars = self.get_code_and_vars(context)
        """LOG [caos.meta.sync.cmd] Sync command:
        print(self)
        """

        """LOG [caos.sql] Sync command code:
        print(code, vars)
        """

        return context.db.prepare(code)(*vars)

    def dump(self):
        return str(self)


class Command(BaseCommand):
    def __init__(self, *, conditions=None, neg_conditions=None, priority=0):
        self.opid = id(self)
        self.conditions = conditions or set()
        self.neg_conditions = neg_conditions or set()
        self.priority = priority

    @debug
    def execute(self, context):
        ok = self.check_conditions(context, self.conditions, True) and \
             self.check_conditions(context, self.neg_conditions, False)

        result = None
        if ok:
            code, vars = self.get_code_and_vars(context)

            """LOG [caos.meta.sync.cmd] Sync command:
            print(self)
            """

            """LOG [caos.sql] Sync command code:
            print(code, vars)
            """

            if vars is not None:
                result = context.db.prepare(code)(*vars)
            else:
                result = context.db.execute(code)
        return result

    def check_conditions(self, context, conditions, positive):
        result = True
        if conditions:
            for condition in conditions:
                code, vars = condition.get_code_and_vars(context)
                result = context.db.prepare(code)(*vars)

                if bool(result) ^ positive:
                    result = False
                    break
            else:
                result = True

        return result


class PrototypeMetaCommand(MetaCommand, delta_cmds.PrototypeCommand):
    pass


class NamedPrototypeMetaCommand(MetaCommand, delta_cmds.NamedPrototypeCommand):
    def fill_record(self, rec=None, obj=None):
        updates = {}

        myrec = self.table.record()

        if not obj:
            for name, value in itertools.chain(self.get_struct_properties().items(),
                                               self.get_properties(('source', 'target')).items()):
                updates[name] = value
                if hasattr(myrec, name):
                    if not rec:
                        rec = self.table.record()
                    setattr(rec, name, value)
        else:
            for field in obj.__class__._fields:
                value = getattr(obj, field)
                updates[field] = value
                if hasattr(myrec, field):
                    if not rec:
                        rec = self.table.record()
                    setattr(rec, field, value)

        if rec:
            if rec.name:
                rec.name = str(rec.name)

            if rec.title:
                rec.title = rec.title.as_dict()

            if rec.description:
                rec.description = rec.description.as_dict()

        return rec, updates

    def create_schema(self, name):
        condition = SchemaExists(name=name)
        self.pgops.add(CreateSchema(name=name, neg_conditions={condition}))

    def create_object(self, prototype):
        schema_name = common.caos_module_name_to_schema_name(prototype.name.module)
        rec, updates = self.fill_record()
        self.pgops.add(Insert(table=self.table, records=[rec]))

        self.create_schema(schema_name)

        return updates


class AlterPrototypeProperty(MetaCommand, adapts=delta_cmds.AlterPrototypeProperty):
    pass


class CreateAtomMod(PrototypeMetaCommand, adapts=delta_cmds.CreateAtomMod):
    def apply(self, meta, context=None):
        result = delta_cmds.CreateAtomMod.apply(self, meta, context)
        PrototypeMetaCommand.apply(self, meta, context)
        return result


class DeleteAtomMod(PrototypeMetaCommand, adapts=delta_cmds.DeleteAtomMod):
    def apply(self, meta, context=None):
        result = delta_cmds.DeleteAtomMod.apply(self, meta, context)
        PrototypeMetaCommand.apply(self, meta, context)
        return result


class AtomMetaCommand(NamedPrototypeMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = AtomTable()

    @classmethod
    def get_atom_base_and_mods(cls, meta, atom):
        mods = set()
        extramods = set()
        mods_encoded = ()

        if proto.Atom.is_prototype(atom.base):
            # Base is another atom prototype, check if it is fundamental,
            # if not, then it is another domain
            base = base_type_name_map.get(atom.base)
            if base:
                if not isinstance(base, str):
                    base, mods_encoded = base(meta.get(atom.base), atom.mods)
            else:
                base = common.atom_name_to_domain_name(atom.base)
        else:
            # Base is a Python type, must correspond to PostgreSQL type
            base = base_type_name_map[atom.name]
            if not isinstance(base, str):
                base, mods_encoded = base(meta.get(atom.name), atom.mods)

        directly_supported_mods = (proto.AtomModMaxLength, proto.AtomModMinLength,
                                   proto.AtomModRegExp, proto.AtomModMaxValue,
                                   proto.AtomModMaxExValue, proto.AtomModMinValue,
                                   proto.AtomModMinExValue)

        for mod in atom.mods.values():
            if mod in mods_encoded:
                continue
            elif isinstance(mod, directly_supported_mods):
                mods.add(mod)
            else:
                extramods.add(mod)

        return base, mods_encoded, mods, extramods

    def fill_record(self, rec=None, obj=None):
        rec, updates = super().fill_record(rec, obj)
        if rec:
            if rec.base:
                rec.base = str(rec.base)

            if rec.default not in (None, Default):
                rec.default = yaml.Language.dump(rec.default)

        return rec, updates

    def get_atom_host_and_pointer(self, atom, meta, context):
        if context:
            concept = context.get(delta_cmds.ConceptCommandContext)
            link = context.get(delta_cmds.LinkCommandContext)
        else:
            concept = link = None

        if concept and link:
            host, pointer = concept, link
        elif link:
            property = context.get(delta_cmds.LinkPropertyCommandContext)
            host, pointer = link, property
        else:
            host = pointer = None

        return host, pointer

    def alter_atom_type(self, atom, meta, host, pointer, new_type, intent):

        users = []

        if host:
            # Automatic atom type change.  There is only one user: host concept table
            users.append((host, pointer))
        else:
            for link in meta(type='link'):
                if link.target and link.target.name == atom.name:
                    users.append((link.source, link))

        domain_name = common.atom_name_to_domain_name(atom.name, catenate=False)

        base, mods_encoded, new_mods, _ = self.get_atom_base_and_mods(meta, atom)

        target_type = new_type

        if intent == 'alter':
            simple_alter = atom.automatic and not new_mods
            if not simple_alter:
                new_name = domain_name[0], domain_name[1] + '_tmp'
                self.pgops.add(RenameDomain(domain_name, new_name))
                target_type = common.qname(*domain_name)

                self.pgops.add(CreateDomain(name=domain_name, base=new_type))
                for mod in new_mods:
                    self.pgops.add(AlterDomainAddConstraint(name=domain_name, constraint=mod))

                domain_name = new_name
        elif intent == 'create':
            self.pgops.add(CreateDomain(name=domain_name, base=base))

        for host_proto, item_proto in users:
            if isinstance(item_proto, proto.Link):
                name = item_proto.base[0] if item_proto.implicit_derivative else item_proto.name
            else:
                name = item_proto.name

            table_name = common.get_table_name(host_proto, catenate=False)
            column_name = common.caos_name_to_pg_colname(name)

            alter_type = AlterTableAlterColumnType(column_name, target_type)
            alter_table = AlterTable(table_name)
            alter_table.add_operation(alter_type)
            self.pgops.add(alter_table)

        if not host:
            for child_atom in meta(type='atom', include_automatic=True):
                if child_atom.base == atom.name:
                    self.alter_atom_type(child_atom, meta, None, None, target_type, 'alter')

        if intent == 'drop' or (intent == 'alter' and not simple_alter):
            self.pgops.add(DropDomain(domain_name))


class CreateAtom(AtomMetaCommand, adapts=delta_cmds.CreateAtom):
    def apply(self, meta, context=None):
        atom = delta_cmds.CreateAtom.apply(self, meta, context)
        AtomMetaCommand.apply(self, meta, context)

        new_domain_name = common.atom_name_to_domain_name(atom.name, catenate=False)
        base, _, mods, extramods = self.get_atom_base_and_mods(meta, atom)

        updates = self.create_object(atom)

        if not atom.automatic or mods:
            self.pgops.add(CreateDomain(name=new_domain_name, base=base))

            for mod in mods:
                self.pgops.add(AlterDomainAddConstraint(name=new_domain_name, constraint=mod))

            default = updates.get('default')

            if default is not None:
                self.pgops.add(AlterDomainAlterDefault(name=new_domain_name, default=default))

        if extramods:
            values = {}

            for mod in extramods:
                cls = mod.__class__.get_canonical_class()
                key = '%s.%s' % (cls.__module__, cls.__name__)
                values[key] = yaml.Language.dump(mod.get_value())

            rec = self.table.record()
            rec.mods = values
            condition = [('name', str(atom.name))]
            self.pgops.add(Update(table=self.table, record=rec, condition=condition))

        return atom


class RenameAtom(AtomMetaCommand, adapts=delta_cmds.RenameAtom):
    def apply(self, meta, context=None):
        proto = delta_cmds.RenameAtom.apply(self, meta, context)
        AtomMetaCommand.apply(self, meta, context)

        domain_name = common.atom_name_to_domain_name(self.prototype_name, catenate=False)
        new_domain_name = common.atom_name_to_domain_name(self.new_name, catenate=False)

        self.pgops.add(RenameDomain(name=domain_name, new_name=new_domain_name))
        updaterec = self.table.record(name=str(self.new_name))
        condition = [('name', str(self.prototype_name))]
        self.pgops.add(Update(table=self.table, record=updaterec, condition=condition))

        return proto


class AlterAtom(AtomMetaCommand, adapts=delta_cmds.AlterAtom):
    def apply(self, meta, context=None):
        old_atom = meta.get(self.prototype_name).copy()
        new_atom = delta_cmds.AlterAtom.apply(self, meta, context)
        AtomMetaCommand.apply(self, meta, context)

        domain_name = common.atom_name_to_domain_name(new_atom.name, catenate=False)

        updaterec, updates = self.fill_record()

        if updaterec:
            condition = [('name', str(old_atom.name))]
            self.pgops.add(Update(table=self.table, record=updaterec, condition=condition))

        old_base, old_mods_encoded, old_mods, _ = self.get_atom_base_and_mods(meta, old_atom)
        base, mods_encoded, new_mods, _ = self.get_atom_base_and_mods(meta, new_atom)

        new_type = None
        type_intent = 'alter'

        if new_atom.automatic:
            if old_mods_encoded and not old_mods and new_mods:
                new_type = common.qname(*domain_name)
                type_intent = 'create'
            elif old_mods_encoded and old_mods and not new_mods:
                new_type = base
                type_intent = 'drop'

        if not new_type and old_base != base:
            new_type = base

        if context:
            concept = context.get(delta_cmds.ConceptCommandContext)
            link = context.get(delta_cmds.LinkCommandContext)
        else:
            concept = link = None

        if new_type:
            # The change of the underlying data type for domains is a complex problem.
            # There is no direct way in PostgreSQL to change the base type of a domain.
            # Instead, a new domain must be created, all users of the old domain altered
            # to use the new one, and then the old domain dropped.  Obviously this
            # recurses down to every child domain.
            #
            host, pointer = self.get_atom_host_and_pointer(new_atom, meta, context)
            host_proto = host.proto if host else None
            pointer_proto = pointer.proto if pointer else None
            self.alter_atom_type(new_atom, meta, host_proto, pointer_proto, new_type,
                                 intent=type_intent)

        if type_intent != 'drop':
            default_delta = updates.get('default')
            if default_delta:
                self.pgops.add(AlterDomainAlterDefault(name=domain_name,
                                                       default=default_delta))

            for mod in old_mods - new_mods:
                self.pgops.add(AlterDomainDropConstraint(name=domain_name, constraint=mod))

            for mod in new_mods - old_mods:
                self.pgops.add(AlterDomainAddConstraint(name=domain_name, constraint=mod))

        return new_atom


class DeleteAtom(AtomMetaCommand, adapts=delta_cmds.DeleteAtom):
    def apply(self, meta, context=None):
        atom = delta_cmds.DeleteAtom.apply(self, meta, context)
        AtomMetaCommand.apply(self, meta, context)

        link = None
        if context:
            link = context.get(delta_cmds.LinkCommandContext)

        ops = link.op.pgops if link else self.pgops

        old_domain_name = common.atom_name_to_domain_name(self.prototype_name, catenate=False)

        # Domain dropping gets low priority since other things may depend on it
        cond = DomainExists(old_domain_name)
        ops.add(DropDomain(name=old_domain_name, conditions=[cond], priority=3))
        ops.add(Delete(table=AtomTable(), condition=[('name', str(self.prototype_name))]))

        return atom


class UpdateSearchIndexes(MetaCommand):
    def __init__(self, host, **kwargs):
        super().__init__(**kwargs)
        self.host = host

    def get_index_name(self, host_table_name, language, index_class='default'):
        return '%s_%s_%s_search_idx' % (host_table_name[1], language, index_class)

    def apply(self, meta, context):
        if isinstance(self.host, caos.types.ProtoConcept):
            columns = []

            names = sorted(self.host.links.keys())

            for link_name in names:
                for link in self.host.links[link_name]:
                    if link.search:
                        column_name = common.caos_name_to_pg_colname(link_name)
                        columns.append(TextSearchIndexColumn(column_name, link.search.weight,
                                                             'english'))

            if columns:
                table_name = common.get_table_name(self.host, catenate=False)

                index_name = self.get_index_name(table_name, 'default')
                index = TextSearchIndex(name=index_name, table_name=table_name, columns=columns)

                cond = IndexExists(index_name=(table_name[0], index_name))
                op = DropIndex(index_name=(table_name[0], index_name), conditions=(cond,))
                self.pgops.add(op)
                op = CreateIndex(index=index)
                self.pgops.add(op)


class CompositePrototypeMetaCommand(NamedPrototypeMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.alter_table = None
        self.update_search_indexes = None

    @classmethod
    def _pg_type_from_atom(cls, meta, atom_obj):
        base, _, mods, _ = AtomMetaCommand.get_atom_base_and_mods(meta, atom_obj)

        need_to_create = False

        if not atom_obj.automatic or mods:
            column_type = base_type_name_map.get(atom_obj.name)
            if column_type:
                column_type = base
            else:
                column_type = common.atom_name_to_domain_name(atom_obj.name)
                need_to_create = bool(mods)
        else:
            column_type = base

        return column_type, need_to_create

    @classmethod
    def pg_type_from_atom(cls, meta, atom):
        return cls._pg_type_from_atom(meta, atom)[0]

    def rename(self, old_name, new_name):
        old_table_name = common.concept_name_to_table_name(old_name, catenate=False)
        new_table_name = common.concept_name_to_table_name(new_name, catenate=False)

        if old_name.module != new_name.module:
            self.pgops.add(AlterTableSetSchema(old_table_name, new_table_name[0]))
            old_table_name = (new_table_name[0], old_table_name[1])

        if old_name.name != new_name.name:
            self.pgops.add(AlterTableRenameTo(old_table_name, new_table_name[1]))

        updaterec = self.table.record(name=str(new_name))
        condition = [('name', str(old_name))]
        self.pgops.add(Update(table=self.table, record=updaterec, condition=condition))


    def search_index_add(self, host, pointer, meta, context):
        if self.update_search_indexes is None:
            self.update_search_indexes = UpdateSearchIndexes(host)

    def search_index_alter(self, host, pointer, meta, context):
        if self.update_search_indexes is None:
            self.update_search_indexes = UpdateSearchIndexes(host)

    def search_index_delete(self, host, pointer, meta, context):
        if self.update_search_indexes is None:
            self.update_search_indexes = UpdateSearchIndexes(host)


class ConceptMetaCommand(CompositePrototypeMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = ConceptTable()

    def fill_record(self, rec=None):
        rec, updates = super().fill_record(rec)
        if rec and rec.custombases:
            rec.custombases = tuple(str(b) for b in rec.custombases)
        return rec, updates


class CreateConcept(ConceptMetaCommand, adapts=delta_cmds.CreateConcept):
    def apply(self, meta, context=None):
        new_table_name = common.concept_name_to_table_name(self.prototype_name, catenate=False)
        concept_table = Table(name=new_table_name)
        schema_name = common.caos_module_name_to_schema_name(self.prototype_name.module)
        self.create_schema(schema_name)
        self.pgops.add(CreateTable(table=concept_table))

        self.alter_table = AlterTable(new_table_name)

        concept = delta_cmds.CreateConcept.apply(self, meta, context)
        ConceptMetaCommand.apply(self, meta, context)

        fields = self.create_object(concept)

        if concept.name == 'semantix.caos.builtins.BaseObject':
            col = Column(name='concept_id', type='integer', required=True)
            self.alter_table.add_operation(AlterTableAddColumn(col))

        constraint = PrimaryKey(columns=['semantix.caos.builtins.id'])
        self.alter_table.add_operation(AlterTableAddConstraint(constraint))

        bases = (common.concept_name_to_table_name(p, catenate=False)
                 for p in fields['base'] if proto.Concept.is_prototype(p))
        concept_table.bases = list(bases)

        if self.alter_table.ops:
            self.pgops.add(self.alter_table)

        if self.update_search_indexes:
            self.update_search_indexes.apply(meta, context)
            self.pgops.add(self.update_search_indexes)

        return concept


class RenameConcept(ConceptMetaCommand, adapts=delta_cmds.RenameConcept):
    def apply(self, meta, context=None):
        proto = delta_cmds.RenameConcept.apply(self, meta, context)
        ConceptMetaCommand.apply(self, meta, context)
        self.rename(self.prototype_name, self.new_name)

        if context:
            concept = context.get(delta_cmds.ConceptCommandContext)
            if concept:
                if concept.op.alter_table.ops:
                    concept.op.pgops.add(concept.op.alter_table)
                table_name = common.concept_name_to_table_name(self.new_name, catenate=False)
                concept.op.alter_table = AlterTable(table_name)

        return proto


class AlterConcept(ConceptMetaCommand, adapts=delta_cmds.AlterConcept):
    def apply(self, meta, context=None):
        table_name = common.concept_name_to_table_name(self.prototype_name, catenate=False)
        self.alter_table = AlterTable(table_name)

        concept = delta_cmds.AlterConcept.apply(self, meta, context=context)
        ConceptMetaCommand.apply(self, meta, context)

        updaterec, _ = self.fill_record()

        if updaterec:
            condition = [('name', str(concept.name))]
            self.pgops.add(Update(table=self.table, record=updaterec, condition=condition))

        if self.alter_table.ops:
            self.pgops.add(self.alter_table)

        if self.update_search_indexes:
            self.update_search_indexes.apply(meta, context)
            self.pgops.add(self.update_search_indexes)

        return concept


class DeleteConcept(ConceptMetaCommand, adapts=delta_cmds.DeleteConcept):
    def apply(self, meta, context=None):
        old_table_name = common.concept_name_to_table_name(self.prototype_name, catenate=False)
        self.alter_table = AlterTable(old_table_name)

        concept = delta_cmds.DeleteConcept.apply(self, meta, context)
        ConceptMetaCommand.apply(self, meta, context)

        self.pgops.add(DropTable(name=old_table_name))
        self.pgops.add(Delete(table=self.table, condition=[('name', str(concept.name))]))

        return concept


class ScheduleLinkMappingUpdate(MetaCommand):
    pass


class CancelLinkMappingUpdate(MetaCommand):
    pass


class LinkMetaCommand(CompositePrototypeMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = LinkTable()

    def record_metadata(self, link, old_link, meta, context):
        rec, updates = self.fill_record()

        if rec:
            concept = context.get(delta_cmds.ConceptCommandContext) if context else None

            source = updates.get('source')
            if source:
                source = source
            elif concept:
                source = concept.proto.name

            if source:
                rec.source_id = Query('(SELECT id FROM caos.metaobject WHERE name = $1)',
                                      [str(source)], type='integer')

            target = updates.get('target')
            if target:
                rec.target_id = Query('(SELECT id FROM caos.metaobject WHERE name = $1)',
                                      [str(target)],
                                      type='integer')

        if not old_link or old_link.constraints != link.constraints:
            if not rec:
                rec = self.table.record()

            constraints = {}
            for constraint in link.constraints.values():
                cls = constraint.__class__.get_canonical_class()
                key = '%s.%s' % (cls.__module__, cls.__name__)
                constraints[key] = yaml.Language.dump(constraint.values)

            rec.constraints = constraints

        return rec

    def get_columns(self, link, meta):
        columns = []

        if link.atomic():
            if not isinstance(link.target, proto.Atom):
                link.target = meta.get(link.target)

            column_type = self.pg_type_from_atom(meta, link.target)

            name = link.base[0] if link.implicit_derivative else link.name
            column_name = common.caos_name_to_pg_colname(name)

            columns.append(Column(name=column_name, type=column_type,
                                  required=link.required))

        return columns

    def create_table(self, link, meta, context, conditional=False):
        new_table_name = common.link_name_to_table_name(link.name, catenate=False)
        self.create_schema(new_table_name[0])

        constraints = []
        columns = []

        if link.name == 'semantix.caos.builtins.link':
            columns.append(Column(name='source_id', type='uuid', required=True))
            columns.append(Column(name='target_id', type='uuid', required=True))
            columns.append(Column(name='link_type_id', type='integer', required=True))

        constraints.append(PrimaryKey(columns=['source_id', 'target_id', 'link_type_id']))

        table = Table(name=new_table_name)
        table.add_columns(columns)
        table.constraints = constraints

        if link.base:
            bases = (common.link_name_to_table_name(p, catenate=False)
                     for p in link.base if proto.Concept.is_prototype(p))
            table.bases = list(bases)

        if conditional:
            c = CreateTable(table=table, neg_conditions=[TableExists(new_table_name)])
        else:
            c = CreateTable(table=table)
        self.pgops.add(c)

    def has_table(self, link, meta, context):
        return (not link.atomic() or link.properties) and not link.implicit_derivative

    def schedule_mapping_update(self, link, meta, context):
        if (not link.atomic() or link.properties):
            mapping_indexes = context.get(delta_cmds.RealmCommandContext).op.update_mapping_indexes
            link_name = link.normal_name()
            ops = mapping_indexes.links.get(link_name)
            if not ops:
                mapping_indexes.links[link_name] = ops = []
            ops.append((self, link))
            self.pgops.add(ScheduleLinkMappingUpdate())

    def cancel_mapping_update(self, link, meta, context):
        name = link.normal_name()
        mapping_indexes = context.get(delta_cmds.RealmCommandContext).op.update_mapping_indexes
        mapping_indexes.links.pop(name, None)
        self.pgops.add(CancelLinkMappingUpdate())

    def alter_host_table_column(self, link, meta, context, new_type):
        concept_ctx = context.get(delta_cmds.ConceptCommandContext)

        atom = meta.get(new_type)
        target_type = self.pg_type_from_atom(meta, atom)

        if isinstance(link, proto.Link):
            name = link.normal_name()
        else:
            name = link.name

        table_name = common.get_table_name(concept_ctx.proto, catenate=False)
        column_name = common.caos_name_to_pg_colname(name)

        alter_type = AlterTableAlterColumnType(column_name, target_type)
        alter_table = AlterTable(table_name)
        alter_table.add_operation(alter_type)
        self.pgops.add(alter_table)


class CreateLink(LinkMetaCommand, adapts=delta_cmds.CreateLink):
    def apply(self, meta, context=None):
        link = delta_cmds.CreateLink.apply(self, meta, context)
        LinkMetaCommand.apply(self, meta, context)

        # We do not want to create a separate table for atomic links (unless they have
        # properties) since those are represented by table columns.
        #
        # Implicit derivative links also do not get their own table since they're just
        # a special case of the parent.
        #
        # On the other hand, much like with concepts we want all other links to be in
        # separate tables even if they do not define additional properties.
        # This is to allow for further schema evolution.
        #
        if self.has_table(link, meta, context):
            self.create_table(link, meta, context, conditional=True)

        if link.atomic() and link.implicit_derivative:
            concept = context.get(delta_cmds.ConceptCommandContext)
            assert concept, "Link command must be run in Concept command context"

            cols = self.get_columns(link, meta)
            table_name = common.get_table_name(concept.proto, catenate=False)

            for col in cols:
                # The column may already exist as inherited from parent table
                cond = ColumnExists(table_name=table_name, column_name=col.name)
                cmd = AlterTableAddColumn(col)
                concept.op.alter_table.add_operation((cmd, None, (cond,)))

        if self.alter_table and self.alter_table.ops:
            self.pgops.add(self.alter_table)

        rec = self.record_metadata(link, None, meta, context)
        self.pgops.add(Insert(table=self.table, records=[rec], priority=1))

        if link.mapping != caos.types.ManyToMany:
            self.schedule_mapping_update(link, meta, context)

        return link


class RenameLink(LinkMetaCommand, adapts=delta_cmds.RenameLink):
    def apply(self, meta, context=None):
        result = delta_cmds.RenameLink.apply(self, meta, context)
        LinkMetaCommand.apply(self, meta, context)

        if context:
            concept = context.get(delta_cmds.ConceptCommandContext)
            prototype_name = proto.Link.normalize_link_name(self.prototype_name)
            new_name = proto.Link.normalize_link_name(self.new_name)
            if concept and result.atomic() and prototype_name != new_name:
                table_name = common.concept_name_to_table_name(concept.proto.name, catenate=False)

                prototype_name = common.caos_name_to_pg_colname(prototype_name)
                new_name = common.caos_name_to_pg_colname(new_name)

                rename = AlterTableRenameColumn(table_name, prototype_name, new_name)
                self.pgops.add(rename)

        if self.alter_table and self.alter_table.ops:
            self.pgops.add(self.alter_table)

        rec = self.table.record()
        rec.name = str(self.new_name)
        self.pgops.add(Update(table=self.table, record=rec,
                              condition=[('name', str(self.prototype_name))], priority=1))

        return result


class AlterLink(LinkMetaCommand, adapts=delta_cmds.AlterLink):
    def apply(self, meta, context=None):
        self.old_link = old_link = meta.get(self.prototype_name).copy()
        link = delta_cmds.AlterLink.apply(self, meta, context)
        LinkMetaCommand.apply(self, meta, context)

        rec = self.record_metadata(link, old_link, meta, context)

        if rec:
            self.pgops.add(Update(table=self.table, record=rec,
                                  condition=[('name', str(link.name))], priority=1))

        if self.alter_table and self.alter_table.ops:
            self.pgops.add(self.alter_table)

        new_type = None
        for op in self(delta_cmds.AlterPrototypeProperty):
            if op.property == 'target':
                new_type = op.new_value
                break

        if new_type:
            self.alter_host_table_column(link, meta, context, new_type)

        if old_link.mapping != link.mapping:
            self.schedule_mapping_update(link, meta, context)

        return link


class DeleteLink(LinkMetaCommand, adapts=delta_cmds.DeleteLink):
    def apply(self, meta, context=None):
        result = delta_cmds.DeleteLink.apply(self, meta, context)
        LinkMetaCommand.apply(self, meta, context)

        if result.atomic() and result.implicit_derivative:
            concept = context.get(delta_cmds.ConceptCommandContext)

            name = result.base[0] if result.implicit_derivative else result.name
            column_name = common.caos_name_to_pg_colname(name)
            # We don't really care about the type -- we're dropping the thing
            column_type = 'text'

            col = AlterTableDropColumn(Column(name=column_name, type=column_type))
            concept.op.alter_table.add_operation(col)

            if result.mapping != caos.types.ManyToMany:
                self.schedule_mapping_update(result, meta, context)

        elif not result.atomic() and not result.implicit_derivative:
            old_table_name = common.link_name_to_table_name(result.name, catenate=False)
            self.pgops.add(DropTable(name=old_table_name))
            self.cancel_mapping_update(result, meta, context)

        self.pgops.add(Delete(table=self.table, condition=[('name', str(result.name))]))

        return result


class CreateLinkSet(PrototypeMetaCommand, adapts=delta_cmds.CreateLinkSet):
    def apply(self, meta, context=None):
        result = delta_cmds.CreateLinkSet.apply(self, meta, context)
        PrototypeMetaCommand.apply(self, meta, context)
        return result


class RenameLinkSet(PrototypeMetaCommand, adapts=delta_cmds.RenameLinkSet):
    def apply(self, meta, context=None):
        result = delta_cmds.RenameLinkSet.apply(self, meta, context)
        PrototypeMetaCommand.apply(self, meta, context)
        return result


class AlterLinkSet(PrototypeMetaCommand, adapts=delta_cmds.AlterLinkSet):
    def apply(self, meta, context=None):
        result = delta_cmds.AlterLinkSet.apply(self, meta, context)
        PrototypeMetaCommand.apply(self, meta, context)
        return result


class DeleteLinkSet(PrototypeMetaCommand, adapts=delta_cmds.DeleteLinkSet):
    def apply(self, meta, context=None):
        result = delta_cmds.DeleteLinkSet.apply(self, meta, context)
        PrototypeMetaCommand.apply(self, meta, context)
        return result


class LinkConstraintMetaCommand(PrototypeMetaCommand):
    pass


class CreateLinkConstraint(LinkConstraintMetaCommand, adapts=delta_cmds.CreateLinkConstraint):
    def apply(self, meta, context=None):
        constraint = delta_cmds.CreateLinkConstraint.apply(self, meta, context)
        LinkConstraintMetaCommand.apply(self, meta, context)
        return constraint


class DeleteLinkConstraint(LinkConstraintMetaCommand, adapts=delta_cmds.DeleteLinkConstraint):
    def apply(self, meta, context=None):
        constraint = delta_cmds.DeleteLinkConstraint.apply(self, meta, context)
        LinkConstraintMetaCommand.apply(self, meta, context)
        return constraint


class LinkPropertyMetaCommand(CompositePrototypeMetaCommand):
    def get_columns(self, property, meta):
        columns = []

        if not isinstance(property.atom, proto.Atom):
            property.atom = meta.get(property.atom)

        column_type = self.pg_type_from_atom(meta, property.atom)

        name = property.name
        column_name = common.caos_name_to_pg_colname(name)

        columns.append(Column(name=column_name, type=column_type))

        return columns


class CreateLinkProperty(LinkPropertyMetaCommand, adapts=delta_cmds.CreateLinkProperty):
    def apply(self, meta, context=None):
        property = delta_cmds.CreateLinkProperty.apply(self, meta, context)
        LinkPropertyMetaCommand.apply(self, meta, context)

        link = context.get(delta_cmds.LinkCommandContext)
        assert link, "Link property command must be run in Link command context"

        link_table_name = common.link_name_to_table_name(link.proto.name, catenate=False)
        link.op.create_table(link.proto, meta, context, conditional=True)

        if not link.op.alter_table:
            link.op.alter_table = AlterTable(link_table_name)

        cols = self.get_columns(property, meta)
        for col in cols:
            link.op.alter_table.add_operation(AlterTableAddColumn(col))

        return property


class RenameLinkProperty(LinkPropertyMetaCommand, adapts=delta_cmds.RenameLinkProperty):
    pass


class AlterLinkProperty(LinkPropertyMetaCommand, adapts=delta_cmds.AlterLinkProperty):
    pass


class DeleteLinkProperty(LinkPropertyMetaCommand, adapts=delta_cmds.DeleteLinkProperty):
    def apply(self, meta, context=None):
        property = delta_cmds.DeleteLinkProperty.apply(self, meta, context)
        LinkPropertyMetaCommand.apply(self, meta, context)

        link = context.get(delta_cmds.LinkCommandContext)
        assert link, "Link property command must be run in Link command context"

        link_table_name = common.link_name_to_table_name(link.proto.name, catenate=False)
        if not link.op.alter_table:
            link.op.alter_table = AlterTable(link_table_name)

        column_name = common.caos_name_to_pg_colname(property.name)
        # We don't really care about the type -- we're dropping the thing
        column_type = 'text'

        col = AlterTableDropColumn(Column(name=column_name, type=column_type))
        link.op.alter_table.add_operation(col)

        return property


class LinkSearchConfigurationMetaCommand(PrototypeMetaCommand):
    pass


class CreateLinkSearchConfiguration(LinkSearchConfigurationMetaCommand,
                                    adapts=delta_cmds.CreateLinkSearchConfiguration):
    def apply(self, meta, context=None):
        config = delta_cmds.CreateLinkSearchConfiguration.apply(self, meta, context)
        LinkSearchConfigurationMetaCommand.apply(self, meta, context)

        link = context.get(delta_cmds.LinkCommandContext)
        assert link, "Link search configuration command must be run in Link command context"

        concept = context.get(delta_cmds.ConceptCommandContext)
        assert concept, "Link search configuration command must be run in Concept command context"

        concept.op.search_index_add(concept.proto, link.proto, meta, context)

        return config


class AlterLinkSearchConfiguration(LinkSearchConfigurationMetaCommand,
                                   adapts=delta_cmds.AlterLinkSearchConfiguration):
    def apply(self, meta, context=None):
        delta_cmds.AlterLinkSearchConfiguration.apply(self, meta, context)
        LinkSearchConfigurationMetaCommand.apply(self, meta, context)

        link = context.get(delta_cmds.LinkCommandContext)
        assert link, "Link search configuration command must be run in Link command context"

        concept = context.get(delta_cmds.ConceptCommandContext)
        assert concept, "Link search configuration command must be run in Concept command context"

        concept.op.search_index_alter(concept.proto, link.proto, meta, context)


class DeleteLinkSearchConfiguration(LinkSearchConfigurationMetaCommand,
                                    adapts=delta_cmds.DeleteLinkSearchConfiguration):
    def apply(self, meta, context=None):
        config = delta_cmds.DeleteLinkSearchConfiguration.apply(self, meta, context)
        LinkSearchConfigurationMetaCommand.apply(self, meta, context)

        link = context.get(delta_cmds.LinkCommandContext)
        assert link, "Link search configuration command must be run in Link command context"

        concept = context.get(delta_cmds.ConceptCommandContext)
        assert concept, "Link search configuration command must be run in Concept command context"

        concept.op.search_index_delete(concept.proto, link.proto, meta, context)

        return config


class CreateMappingIndexes(MetaCommand):
    def __init__(self, table_name, mapping, maplinks):
        super().__init__()

        key = str(table_name[1])
        if mapping == caos.types.OneToOne:
            # Each source can have only one target and
            # each target can have only one source
            sides = ('source', 'target')

        elif mapping == caos.types.OneToMany:
            # Each target can have only one source, but
            # one source can have many targets
            sides = ('target',)

        elif mapping == caos.types.ManyToOne:
            # Each source can have only one target, but
            # one target can have many sources
            sides = ('source',)

        else:
            sides = ()

        for side in sides:
            index = MappingIndex(key + '_%s' % side, mapping, maplinks, table_name)
            index.add_columns(('%s_id' % side, 'link_type_id'))
            self.pgops.add(CreateIndex(index, priority=3))


class AlterMappingIndexes(MetaCommand):
    def __init__(self, idx_names, table_name, mapping, maplinks):
        super().__init__()

        self.pgops.add(DropMappingIndexes(idx_names, table_name, mapping))
        self.pgops.add(CreateMappingIndexes(table_name, mapping, maplinks))


class DropMappingIndexes(MetaCommand):
    def __init__(self, idx_names, table_name, mapping):
        super().__init__()

        for idx_name in idx_names:
            self.pgops.add(DropIndex((table_name[0], idx_name), priority=3))


class UpdateMappingIndexes(MetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.links = {}
        self.idx_name_re = re.compile(r'.*(?P<mapping>[1*]{2})_link_mapping_idx$')
        self.idx_pred_re = re.compile(r'''
                              \( \s* link_type_id \s* = \s*
                                  (?:(?: ANY \s* \( \s* ARRAY \s* \[
                                      (?P<type_ids> \d+ (?:\s* , \s* \d+)* )
                                  \s* \] \s* \) \s* )
                                  |
                                  (?P<type_id>\d+))
                              \s* \)
                           ''', re.X)
        self.schema_exists = SchemaExists(name='caos')

    def interpret_index(self, index_name, index_predicate, link_map):
        m = self.idx_name_re.match(index_name)
        if not m:
            raise caos.MetaError('could not interpret index %s' % index_name)

        mapping = m.group('mapping')

        m = self.idx_pred_re.match(index_predicate)
        if not m:
            raise caos.MetaError('could not interpret index %s predicate: %s' % \
                                 (index_name, index_predicate))

        link_type_ids = (int(i) for i in re.split('\D+', m.group('type_ids') or m.group('type_id')))

        return mapping, list(link_map[i] for i in link_type_ids)

    def interpret_indexes(self, indexes, link_map):
        for idx_name, idx_pred in zip(indexes['index_names'], indexes['index_predicates']):
            yield idx_name, self.interpret_index(idx_name, idx_pred, link_map)

    def _group_indexes(self, indexes):
        """Group indexes by link name"""

        for index_name, (mapping, link_names) in indexes:
            for link_name in link_names:
                yield link_name, index_name

    def group_indexes(self, indexes):
        key = lambda i: i[0]
        grouped = itertools.groupby(sorted(self._group_indexes(indexes), key=key), key=key)
        for link_name, indexes in grouped:
            yield link_name, tuple(i[1] for i in indexes)

    def apply(self, meta, context):
        db = context.db
        if self.schema_exists.execute(context):
            link_map = context._get_link_map(reverse=True)
            index_ds = datasources.introspection.tables.TableIndexes(db)
            indexes = {}
            for row in index_ds.fetch(schema_pattern='caos%', index_pattern='%_link_mapping_idx'):
                indexes[tuple(row['table_name'])] = self.interpret_indexes(row, link_map)
        else:
            link_map = {}
            indexes = {}

        for link_name, ops in self.links.items():
            table_name = common.link_name_to_table_name(link_name, catenate=False)

            new_indexes = {k: [] for k in caos.types.LinkMapping.values()}
            alter_indexes = {k: [] for k in caos.types.LinkMapping.values()}

            existing = indexes.get(table_name)

            if existing:
                existing_by_name = dict(existing)
                existing = dict(self.group_indexes(existing_by_name.items()))
            else:
                existing_by_name = {}
                existing = {}

            processed = {}

            for op, proto in ops:
                already_processed = processed.get(proto.name)

                if isinstance(op, CreateLink):
                    assert proto.name not in existing
                    # CreateLink can only happen once
                    assert not already_processed
                    new_indexes[proto.mapping].append((proto.name, None, None))

                elif isinstance(op, AlterLink):
                    # We are in apply stage, so the potential link changes, renames
                    # have not yet been pushed to the database, so link_map potentially
                    # contains old link names
                    ex_idx_names = existing.get(op.old_link.name)

                    if ex_idx_names:
                        ex_idx = existing_by_name[ex_idx_names[0]]
                        queue = alter_indexes
                    else:
                        ex_idx = None
                        queue = new_indexes

                    item = (proto.name, op.old_link.name, ex_idx_names)

                    # Delta generator could have yielded several AlterLink commands
                    # for the same link, we need to respect only the last state.
                    if already_processed:
                        if already_processed != proto.mapping:
                            queue[already_processed].remove(item)

                            if not ex_idx or ex_idx[0] != proto.mapping:
                                queue[proto.mapping].append(item)

                    elif not ex_idx or ex_idx[0] != proto.mapping:
                        queue[proto.mapping].append(item)

                processed[proto.name] = proto.mapping

            for mapping, maplinks in new_indexes.items():
                if maplinks:
                    maplinks = list(i[0] for i in maplinks)
                    self.pgops.append(CreateMappingIndexes(table_name, mapping, maplinks))

            for mapping, maplinks in alter_indexes.items():
                new = []
                alter = {}
                for maplink in maplinks:
                    maplink_name, orig_maplink_name, ex_idx_names = maplink
                    ex_idx = existing_by_name[ex_idx_names[0]]

                    alter_links = alter.get(ex_idx_names)
                    if alter_links is None:
                        alter[ex_idx_names] = alter_links = set(ex_idx[1])
                    alter_links.discard(orig_maplink_name)

                    new.append(maplink_name)

                if new:
                    self.pgops.append(CreateMappingIndexes(table_name, mapping, new))

                for idx_names, altlinks in alter.items():
                    if not altlinks:
                        self.pgops.append(DropMappingIndexes(ex_idx_names, table_name, mapping))
                    else:
                        self.pgops.append(AlterMappingIndexes(idx_names, table_name, mapping,
                                                              altlinks))


class CommandContext(delta_cmds.CommandContext):
    def __init__(self, db):
        super().__init__()
        self.db = db
        self.link_name_to_id_map = None

    def _get_link_map(self, reverse=False):
        link_ds = datasources.meta.links.ConceptLinks(self.db)
        links = link_ds.fetch(include_derivatives=True)
        grouped = itertools.groupby(links, key=lambda i: i['id'])
        if reverse:
            link_map = {k: next(i)['name'] for k, i in grouped}
        else:
            link_map = {next(i)['name']: k for k, i in grouped}
        return link_map

    def get_link_map(self):
        link_map = self.link_name_to_id_map
        if not link_map:
            link_map = self._get_link_map()
            self.link_name_to_id_map = link_map
        return link_map


class AlterRealm(MetaCommand, adapts=delta_cmds.AlterRealm):
    def apply(self, meta, context):
        self.pgops.add(CreateSchema(name='caos', priority=-2))

        self.pgops.add(EnableFeature(feature=UuidFeature(),
                                     neg_conditions=[FunctionExists(('caos', 'uuid_nil'))],
                                     priority=-2))

        self.pgops.add(EnableHstoreFeature(feature=HstoreFeature(),
                                           neg_conditions=[TypeExists(('caos', 'hstore'))],
                                           priority=-2))

        deltalogtable = DeltaLogTable()
        self.pgops.add(CreateTable(table=deltalogtable,
                                   neg_conditions=[TableExists(name=deltalogtable.name)],
                                   priority=-1))

        deltareftable = DeltaRefTable()
        self.pgops.add(CreateTable(table=deltareftable,
                                   neg_conditions=[TableExists(name=deltareftable.name)],
                                   priority=-1))

        metatable = MetaObjectTable()
        self.pgops.add(CreateTable(table=metatable,
                                   neg_conditions=[TableExists(name=metatable.name)],
                                   priority=-1))

        atomtable = AtomTable()
        self.pgops.add(CreateTable(table=atomtable,
                                   neg_conditions=[TableExists(name=atomtable.name)],
                                   priority=-1))

        concepttable = ConceptTable()
        self.pgops.add(CreateTable(table=concepttable,
                                   neg_conditions=[TableExists(name=concepttable.name)],
                                   priority=-1))

        linktable = LinkTable()
        self.pgops.add(CreateTable(table=linktable,
                                   neg_conditions=[TableExists(name=linktable.name)],
                                   priority=-1))

        self.update_mapping_indexes = UpdateMappingIndexes()

        delta_cmds.AlterRealm.apply(self, meta, context)
        MetaCommand.apply(self, meta)

        self.update_mapping_indexes.apply(meta, context)
        self.pgops.append(self.update_mapping_indexes)

    def is_material(self):
        return True

    def execute(self, context):
        for op in self.serialize_ops():
            op.execute(context)

    def serialize_ops(self):
        queues = {}
        self._serialize_ops(self, queues)
        queues = (i[1] for i in sorted(queues.items(), key=lambda i: i[0]))
        return itertools.chain.from_iterable(queues)

    def _serialize_ops(self, obj, queues):
        for op in obj.pgops:
            if isinstance(op, MetaCommand):
                self._serialize_ops(op, queues)
            else:
                queue = queues.get(op.priority)
                if not queue:
                    queues[op.priority] = queue = []
                queue.append(op)


#
# Primitive commands follow
#

class DDLOperation(Command):
    pass


class DMLOperation(Command):
    pass


class Condition(BaseCommand):
    pass


class Query:
    def __init__(self, text, params, type):
        self.text = text
        self.params = params
        self.type = type


class Insert(DMLOperation):
    def __init__(self, table, records, *, priority=0):
        super().__init__(priority=priority)

        self.table = table
        self.records = records

    def code(self, context):
        cols = [c.name for c in self.table.columns(writable_only=True)]
        l = len(cols)

        vals = []
        placeholders = []
        i = 1
        for row in self.records:
            placeholder_row = []
            for col in cols:
                val = getattr(row, col, None)
                if val and isinstance(val, Query):
                    vals.extend(val.params)
                    qtext = re.sub(r'\$(\d+)', lambda m: '$%s' % (int(m.groups(1)[0]) + i - 1), val.text)
                    placeholder_row.append('(%s)::%s' % (qtext, val.type))
                    i += len(val.params)
                elif val is Default:
                    placeholder_row.append('DEFAULT')
                else:
                    vals.append(val)
                    placeholder_row.append('$%d' % i)
                    i += 1
            placeholders.append('(%s)' % ','.join(placeholder_row))

        code = 'INSERT INTO %s (%s) VALUES %s' % \
                (common.qname(*self.table.name),
                 ','.join(common.quote_ident(c) for c in cols),
                 ','.join(placeholders))

        return (code, vals)

    def __repr__(self):
        vals = (('(%s)' % ', '.join('%s=%r' % (col, v) for col, v in row)) for row in self.records)
        return '<caos.sync.%s %s (%s)>' % (self.__class__.__name__, self.table.name, ', '.join(vals))


class Update(DMLOperation):
    def __init__(self, table, record, condition, *, priority=0):
        super().__init__(priority=priority)

        self.table = table
        self.record = record
        self.fields = [f for f, v in record if v is not Default]
        self.condition = condition

    def code(self, context):
        e = common.quote_ident

        placeholders = []
        vals = []

        i = 1
        for f in self.fields:
            val = getattr(self.record, f)

            if val is Default:
                continue

            if isinstance(val, Query):
                expr = re.sub(r'\$(\d+)', lambda m: '$%s' % (int(m.groups(1)[0]) + i - 1), val.text)
                i += len(val.params)
                vals.extend(val.params)
            else:
                expr = '$%d' % i
                i += 1
                vals.append(val)

            placeholders.append('%s = %s' % (e(f), expr))

        where = ' AND '.join('%s = $%d' % (e(c[0]), ci + i) for ci, c in enumerate(self.condition))

        code = 'UPDATE %s SET %s WHERE %s' % \
                (common.qname(*self.table.name), ', '.join(placeholders), where)

        vals += [c[1] for c in self.condition]

        return (code, vals)

    def __repr__(self):
        expr = ','.join('%s=%s' % (f, getattr(self.record, f)) for f in self.fields)
        where = ','.join('%s=%s' % (c[0], c[1]) for c in self.condition)
        return '<caos.sync.%s %s %s (%s)>' % (self.__class__.__name__, self.table.name, expr, where)


class Merge(Update):
    def code(self, context):
        code = super().code(context)
        cols = (common.quote_ident(c[0]) for c in self.condition)
        result = (code[0] + ' RETURNING %s' % (','.join(cols)), code[1])
        return result

    def execute(self, context):
        result = super().execute(context)

        if not result:
            op = Insert(self.table, records=[self.record])
            result = op.execute(context)

        return result


class Delete(DMLOperation):
    def __init__(self, table, condition, *, priority=0):
        super().__init__(priority=priority)

        self.table = table
        self.condition = condition

    def code(self, context):
        e = common.quote_ident
        where = ' AND '.join('%s = $%d' % (e(c[0]), i + 1) for i, c in enumerate(self.condition))

        code = 'DELETE FROM %s WHERE %s' % (common.qname(*self.table.name), where)

        vals = [c[1] for c in self.condition]

        return (code, vals)

    def __repr__(self):
        where = ','.join('%s=%s' % (c[0], c[1]) for c in self.condition)
        return '<caos.sync.%s %s (%s)>' % (self.__class__.__name__, self.table.name, where)


class DBObject:
    pass


class PrimaryKey(DBObject):
    def __init__(self, columns):
        self.columns = columns

    def code(self, context):
        code = 'PRIMARY KEY (%s)' % ', '.join(common.quote_ident(c) for c in self.columns)
        return code

class UniqueConstraint(DBObject):
    def __init__(self, columns):
        self.columns = columns

    def code(self, context):
        code = 'UNIQUE (%s)' % ', '.join(common.quote_ident(c) for c in self.columns)
        return code


class Column(DBObject):
    def __init__(self, name, type, required=False, default=None, readonly=False):
        self.name = name
        self.type = type
        self.required = required
        self.default = default
        self.readonly = readonly

    def code(self, context):
        e = common.quote_ident
        return '%s %s %s %s' % (common.quote_ident(self.name), self.type,
                                'NOT NULL' if self.required else '',
                                ('DEFAULT %s' % self.default) if self.default is not None else '')

    def __repr__(self):
        return '<%s.%s "%s" %s>' % (self.__class__.__module__, self.__class__.__name__,
                                    self.name, self.type)


class IndexColumn(DBObject):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<%s.%s "%s">' % (self.__class__.__module__, self.__class__.__name__, self.name)


class TextSearchIndexColumn(IndexColumn):
    def __init__(self, name, weight, language):
        super().__init__(name)
        self.weight = weight
        self.language = language

    def code(self, context):
        ql = postgresql.string.quote_literal
        qi = common.quote_ident

        return "setweight(to_tsvector(%s, coalesce(%s, '')), %s)" % \
                (ql(self.language), qi(self.name), ql(self.weight))



class DefaultMeta(type):
    def __bool__(cls):
        return False

    def __repr__(self):
        return '<DEFAULT>'

    __str__ = __repr__


class Default(metaclass=DefaultMeta):
    pass


class Index(DBObject):
    def __init__(self, name, table_name, unique=True):
        super().__init__()

        self.name = name
        self.table_name = table_name
        self.__columns = datastructures.OrderedSet()
        self.predicate = None
        self.unique = unique

    def add_columns(self, columns):
        self.__columns.update(columns)

    def creation_code(self, context):
        code = 'CREATE %(unique)s INDEX %(name)s ON %(table)s (%(cols)s) %(predicate)s' % \
                {'unique': 'UNIQUE' if self.unique else '',
                 'name': common.qname(self.name),
                 'table': common.qname(*self.table_name),
                 'cols': ', '.join(self.columns),
                 'predicate': 'WHERE %s' % self.predicate if self.predicate else ''
                }
        return code

    @property
    def columns(self):
        return iter(self.__columns)

    def __repr__(self):
        return '<%(mod)s.%(cls)s name=%(name)s cols=(%(cols)s) unique=%(uniq)s predicate=%(pred)s>'\
               % {'mod': self.__class__.__module__, 'cls': self.__class__.__name__,
                  'name': self.name, 'cols': ','.join('%r' % c for c in self.columns),
                  'uniq': self.unique, 'pred': self.predicate}


class TextSearchIndex(Index):
    def __init__(self, name, table_name, columns):
        super().__init__(name, table_name)
        self.add_columns(columns)

    def creation_code(self, context):
        code = 'CREATE INDEX %(name)s ON %(table)s USING gin((%(cols)s)) %(predicate)s' % \
                {'name': common.qname(self.name),
                 'table': common.qname(*self.table_name),
                 'cols': ' || '.join(c.code(context) for c in self.columns),
                 'predicate': 'WHERE %s' % self.predicate if self.predicate else ''
                }
        return code


class MappingIndex(Index):
    def __init__(self, name_prefix, mapping, link_names, table_name):
        super().__init__(None, table_name, True)
        self.link_names = link_names
        self.name_prefix = name_prefix
        self.mapping = mapping

    def creation_code(self, context):
        link_map = context.get_link_map()

        ids = tuple(sorted(list(link_map[n] for n in self.link_names)))
        id_hash = persistent_hash(ids)

        name = '%s_%x_%s_link_mapping_idx' % (self.name_prefix, id_hash, self.mapping)
        predicate = 'link_type_id IN (%s)' % ', '.join(str(id) for id in ids)

        code = 'CREATE %(unique)s INDEX %(name)s ON %(table)s (%(cols)s) %(predicate)s' % \
                {'unique': 'UNIQUE',
                 'name': common.qname(name),
                 'table': common.qname(*self.table_name),
                 'cols': ', '.join(self.columns),
                 'predicate': ('WHERE %s' % predicate)
                }
        return code

    def __repr__(self):
        name = '%s_%s_%s_link_mapping_idx' % (self.name_prefix, '<HASH>', self.mapping)
        predicate = 'link_type_id IN (%s)' % ', '.join(str(n) for n in self.link_names)

        return '<%(mod)s.%(cls)s name="%(name)s" cols=(%(cols)s) unique=%(uniq)s ' \
               'predicate=%(pred)s>' \
               % {'mod': self.__class__.__module__, 'cls': self.__class__.__name__,
                  'name': name, 'cols': ','.join(self.columns), 'uniq': self.unique,
                  'pred': predicate}


class Table(DBObject):
    def __init__(self, name):
        super().__init__()

        self.name = name
        self.__columns = datastructures.OrderedSet()
        self.constraints = set()
        self.bases = set()
        self.data = []

    @property
    def record(self):
        return datastructures.Record(self.__class__.__name__ + '_record',
                                     [c.name for c in self.columns()],
                                     default=Default)

    def columns(self, writable_only=False, only_self=False):
        cols = []
        tables = [self.__class__] if only_self else reversed(self.__class__.__mro__)
        for c in tables:
            if issubclass(c, Table):
                columns = getattr(self, '_' + c.__name__ + '__columns', [])
                if writable_only:
                    cols.extend(c for c in columns if not c.readonly)
                else:
                    cols.extend(columns)
        return cols

    def add_columns(self, iterable):
        self.__columns.update(iterable)


class DeltaRefTable(Table):
    def __init__(self, name=None):
        name = name or ('caos', 'deltaref')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            Column(name='id', type='varchar', required=True),
            Column(name='ref', type='text', required=True)
        ])

        self.constraints = set([
            PrimaryKey(columns=('ref',))
        ])


class DeltaLogTable(Table):
    def __init__(self, name=None):
        name = name or ('caos', 'deltalog')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            Column(name='id', type='varchar', required=True),
            Column(name='parents', type='varchar[]', required=False),
            Column(name='checksum', type='varchar', required=True),
            Column(name='commit_date', type='timestamp with time zone', required=True,
                                                                        default='CURRENT_TIMESTAMP'),
            Column(name='committer', type='text', required=True),
            Column(name='comment', type='text', required=False)
        ])

        self.constraints = set([
            PrimaryKey(columns=('id',))
        ])


class MetaObjectTable(Table):
    def __init__(self, name=None):
        name = name or ('caos', 'metaobject')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            Column(name='id', type='serial', required=True, readonly=True),
            Column(name='name', type='text', required=True),
            Column(name='is_abstract', type='boolean', required=True, default=False),
            Column(name='title', type='caos.hstore'),
            Column(name='description', type='caos.hstore')
        ])

        self.constraints = set([
            PrimaryKey(columns=('id',)),
            UniqueConstraint(columns=('name',))
        ])


class AtomTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('caos', 'atom'))

        self.bases = [('caos', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            Column(name='automatic', type='boolean', required=True, default=False),
            Column(name='base', type='text', required=True),
            Column(name='mods', type='caos.hstore'),
            Column(name='default', type='text')
        ])

        self.constraints = set([
            PrimaryKey(columns=('id',)),
            UniqueConstraint(columns=('name',))
        ])


class ConceptTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('caos', 'concept'))

        self.bases = [('caos', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            Column(name='custombases', type='text[]'),
        ])

        self.constraints = set([
            PrimaryKey(columns=('id',)),
            UniqueConstraint(columns=('name',))
        ])


class LinkTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('caos', 'link'))

        self.bases = [('caos', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            Column(name='source_id', type='integer'),
            Column(name='target_id', type='integer'),
            Column(name='mapping', type='char(2)', required=True),
            Column(name='required', type='boolean', required=True, default=False),
            Column(name='implicit_derivative', type='boolean', required=True, default=False),
            Column(name='is_atom', type='boolean', required=True, default=False),
            Column(name='readonly', type='boolean', required=True, default=False),
            Column(name='constraints', type='caos.hstore')
        ])

        self.constraints = set([
            PrimaryKey(columns=('id',)),
            UniqueConstraint(columns=('name',))
        ])


class Feature:
    def __init__(self, name, schema='caos'):
        self.name = name
        self.schema = schema

    def code(self, context):
        pgpath = Config.pg_install_path
        source = self.source % {'pgpath': pgpath}
        source = source % {'version': '%s.%s' % context.db.version_info[:2]}

        with open(source, 'r') as f:
            code = re.sub(r'SET\s+search_path\s*=\s*[^;]+;',
                          'SET search_path = %s;' % common.quote_ident(self.schema),
                          f.read())
        return code


class TypeExists(Condition):
    def __init__(self, name):
        self.name = name

    def code(self, context):
        code = '''SELECT
                        t.oid
                    FROM
                        pg_catalog.pg_type t
                        INNER JOIN pg_catalog.pg_namespace ns ON t.typnamespace = ns.oid
                    WHERE
                        t.typname = $2 and ns.nspname = $1'''
        return code, self.name


class UuidFeature(Feature):
    source = '%(pgpath)s/contrib/uuid-ossp.sql'

    def __init__(self, schema='caos'):
        super().__init__(name='uuid', schema=schema)


class HstoreFeature(Feature):
    source = '%(pgpath)s/contrib/hstore.sql'

    def __init__(self, schema='caos'):
        super().__init__(name='hstore', schema=schema)


class EnableFeature(DDLOperation):
    def __init__(self, feature, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.feature = feature
        self.opid = feature.name

    def code(self, context):
        return self.feature.code(context)

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.feature.name)


class EnableHstoreFeature(EnableFeature):
    def execute(self, context):
        super().execute(context)
        self.init_hstore(context.db)

    @classmethod
    def init_hstore(cls, db):
        try:
            db.typio.identify(contrib_hstore='caos.hstore')
        except postgresql.exceptions.SchemaNameError:
            pass


class SchemaExists(Condition):
    def __init__(self, name):
        self.name = name

    def code(self, context):
        return ('SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = $1', [self.name])


class CreateSchema(DDLOperation):
    def __init__(self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.name = name
        self.opid = name
        self.neg_conditions.add(SchemaExists(self.name))

    def code(self, context):
        return 'CREATE SCHEMA %s' % common.quote_ident(self.name)

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.name)


class SchemaObjectOperation(DDLOperation):
    def __init__(self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.name = name
        self.opid = name

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.name)


class DomainExists(Condition):
    def __init__(self, name):
        self.name = name

    def code(self, context):
        code = '''SELECT
                        domain_name
                    FROM
                        information_schema.domains
                    WHERE
                        domain_schema = $1 AND domain_name = $2'''
        return code, self.name


class CreateDomain(SchemaObjectOperation):
    def __init__(self, name, base):
        super().__init__(name)
        self.base = base

    def code(self, context):
        return 'CREATE DOMAIN %s AS %s' % (common.qname(*self.name), self.base)


class RenameDomain(SchemaObjectOperation):
    def __init__(self, name, new_name):
        super().__init__(name)
        self.new_name = new_name

    def code(self, context):
        return '''UPDATE
                        pg_catalog.pg_type AS t
                    SET
                        typname = $1,
                        typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $2)
                    FROM
                        pg_catalog.pg_namespace ns
                    WHERE
                        t.typname = $3
                        AND t.typnamespace = ns.oid
                        AND ns.nspname = $4
                        AND t.typtype = 'd'
               ''', [self.new_name[1], self.new_name[0], self.name[1], self.name[0]]


class DropDomain(SchemaObjectOperation):
    def code(self, context):
        return 'DROP DOMAIN %s' % common.qname(*self.name)


class AlterDomain(DDLOperation):
    def __init__(self, name):
        super().__init__()

        self.name = name


    def code(self, context):
        return 'ALTER DOMAIN %s ' % common.qname(*self.name)

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.name)


class AlterDomainAlterDefault(AlterDomain):
    def __init__(self, name, default):
        super().__init__(name)
        self.default = default

    def code(self, context):
        code = super().code(context)
        if self.default is None:
            code += ' DROP DEFAULT ';
        else:
            value = postgresql.string.quote_literal(str(self.default)) if self.default is not None else 'None'
            code += ' SET DEFAULT ' + value
        return code


class AlterDomainAlterNull(AlterDomain):
    def __init__(self, name, null):
        super().__init__(name)
        self.null = null

    def code(self, context):
        code = super().code(context)
        if self.null:
            code += ' DROP NOT NULL ';
        else:
            code += ' SET NOT NULL ';
        return code


class AlterDomainAlterConstraint(AlterDomain):
    def __init__(self, name, constraint):
        super().__init__(name)
        self.constraint = constraint

    def constraint_name(self, constraint):
        canonical = constraint.__class__.get_canonical_class()
        return common.quote_ident('%s.%s' % (canonical.__module__, canonical.__name__))

    def constraint_code(self, constraint):
        if isinstance(constraint, proto.AtomModRegExp):
            expr = ['VALUE ~ %s' % postgresql.string.quote_literal(re) for re in constraint.values]
            expr = ' AND '.join(expr)
        elif isinstance(constraint, proto.AtomModMaxLength):
            expr = 'length(VALUE::text) <= ' + str(constraint.value)
        elif isinstance(constraint, proto.AtomModMinLength):
            expr = 'length(VALUE::text) >= ' + str(constraint.value)
        elif isinstance(constraint, proto.AtomModMaxValue):
            expr = 'VALUE <= ' + postgresql.string.quote_literal(str(constraint.value))
        elif isinstance(constraint, proto.AtomModMaxExValue):
            expr = 'VALUE < ' + postgresql.string.quote_literal(str(constraint.value))
        elif isinstance(constraint, proto.AtomModMinValue):
            expr = 'VALUE >= ' + postgresql.string.quote_literal(str(constraint.value))
        elif isinstance(constraint, proto.AtomModMinExValue):
            expr = 'VALUE > ' + postgresql.string.quote_literal(str(constraint.value))

        return 'CHECK (%s)' % expr


class AlterDomainDropConstraint(AlterDomainAlterConstraint):
    def code(self, context):
        code = super().code(context)
        code += ' DROP CONSTRAINT %s ' % self.constraint_name(self.constraint)
        return code


class AlterDomainAddConstraint(AlterDomainAlterConstraint):
    def code(self, context):
        code = super().code(context)
        code += ' ADD CONSTRAINT %s %s' % (self.constraint_name(self.constraint),
                                           self.constraint_code(self.constraint))
        return code


class TableExists(Condition):
    def __init__(self, name):
        self.name = name

    def code(self, context):
        code = '''SELECT
                        tablename
                    FROM
                        pg_catalog.pg_tables
                    WHERE
                        schemaname = $1 AND tablename = $2'''
        return code, self.name


class CreateTable(SchemaObjectOperation):
    def __init__(self, table, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(table.name, conditions=conditions, neg_conditions=neg_conditions,
                         priority=priority)
        self.table = table

    def code(self, context):
        elems = [c.code(context) for c in self.table.columns(only_self=True)]
        elems += [c.code(context) for c in self.table.constraints]
        code = 'CREATE TABLE %s (%s)' % (common.qname(*self.table.name), ', '.join(c for c in elems))

        if self.table.bases:
            code += ' INHERITS (' + ','.join(common.qname(*b) for b in self.table.bases) + ')'

        return code


class DropTable(SchemaObjectOperation):
    def code(self, context):
        return 'DROP TABLE %s' % common.qname(*self.name)


class AlterTableBase(DDLOperation):
    def __init__(self, name):
        super().__init__()
        self.name = name

    def code(self, context):
        return 'ALTER TABLE %s' % common.qname(*self.name)

    def __repr__(self):
        return '<%s.%s %s>' % (self.__class__.__module__, self.__class__.__name__, self.name)


class AlterTableFragment(DDLOperation):
    pass


class AlterTable(AlterTableBase):
    def __init__(self, name):
        super().__init__(name)
        self.ops = []

    def add_operation(self, op):
        self.ops.append(op)

    def code(self, context):
        if self.ops:
            code = super().code(context)
            ops = []
            for op in self.ops:
                if isinstance(op, tuple):
                    cond = True
                    if op[1]:
                        cond = cond and self.check_conditions(context, op[1], True)
                    if op[2]:
                        cond = cond and self.check_conditions(context, op[2], False)
                    if cond:
                        ops.append(op[0].code(context))
                else:
                    ops.append(op.code(context))
            code += ' ' + ', '.join(ops)
            return code

    def dump(self):
        result = [repr(self)]

        for op in self.ops:
            if isinstance(op, tuple):
                op = op[0]
            result.extend('  %s' % l for l in op.dump().split('\n'))

        return '\n'.join(result)


class IndexExists(Condition):
    def __init__(self, index_name):
        self.index_name = index_name

    def code(self, context):
        code = '''SELECT
                       i.indexrelid
                   FROM
                       pg_catalog.pg_index i
                       INNER JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
                       INNER JOIN pg_catalog.pg_namespace icn ON icn.oid = ic.relnamespace
                   WHERE
                       icn.nspname = $1 AND ic.relname = $2'''

        return code, self.index_name


class CreateIndex(DDLOperation):
    def __init__(self, index, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.index = index

    def code(self, context):
        code = self.index.creation_code(context)
        return code

    def __repr__(self):
        return '<%s.%s "%r">' % (self.__class__.__module__, self.__class__.__name__, self.index)


class DropIndex(DDLOperation):
    def __init__(self, index_name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.index_name = index_name

    def code(self, context):
        return 'DROP INDEX %s' % common.qname(*self.index_name)

    def __repr__(self):
        return '<%s.%s %s>' % (self.__class__.__module__, self.__class__.__name__,
                               common.qname(*self.index_name))


class ColumnExists(Condition):
    def __init__(self, table_name, column_name):
        self.table_name = table_name
        self.column_name = column_name

    def code(self, context):
        code = '''SELECT
                        column_name
                    FROM
                        information_schema.columns
                    WHERE
                        table_schema = $1 AND table_name = $2 AND column_name = $3'''
        return code, self.table_name + (self.column_name,)


class AlterTableAddColumn(AlterTableFragment):
    def __init__(self, column):
        self.column = column

    def code(self, context):
        return 'ADD COLUMN ' + self.column.code(context)

    def __repr__(self):
        return '<%s.%s %r>' % (self.__class__.__module__, self.__class__.__name__, self.column)


class AlterTableDropColumn(AlterTableFragment):
    def __init__(self, column):
        self.column = column

    def code(self, context):
        return 'DROP COLUMN %s' % common.quote_ident(self.column.name)

    def __repr__(self):
        return '<%s.%s %r>' % (self.__class__.__module__, self.__class__.__name__, self.column)


class AlterTableAlterColumnType(AlterTableFragment):
    def __init__(self, column_name, new_type):
        self.column_name = column_name
        self.new_type = new_type

    def code(self, context):
        return 'ALTER COLUMN %s SET DATA TYPE %s' % \
                (common.quote_ident(str(self.column_name)), self.new_type)

    def __repr__(self):
        return '<%s.%s "%s" to %s>' % (self.__class__.__module__, self.__class__.__name__,
                                       self.column_name, self.new_type)


class AlterTableAddConstraint(AlterTableFragment):
    def __init__(self, constraint):
        self.constraint = constraint

    def code(self, context):
        return 'ADD  ' + self.constraint.code(context)


class AlterTableSetSchema(AlterTableBase):
    def __init__(self, name, schema):
        super().__init__(name)
        self.schema = schema

    def code(self, context):
        code = super().code(context)
        code += ' SET SCHEMA %s ' % common.quote_ident(self.new_name)
        return code


class AlterTableRenameTo(AlterTableBase):
    def __init__(self, name, new_name):
        super().__init__(name)
        self.new_name = new_name

    def code(self, context):
        code = super().code(context)
        code += ' RENAME TO %s ' % common.quote_ident(self.new_name)
        return code


class AlterTableRenameColumn(AlterTableBase):
    def __init__(self, name, old_col_name, new_col_name):
        super().__init__(name)
        self.old_col_name = old_col_name
        self.new_col_name = new_col_name

    def code(self, context):
        code = super().code(context)
        code += ' RENAME COLUMN %s TO %s ' % (common.quote_ident(self.old_col_name),
                                              common.quote_ident(self.new_col_name))
        return code


class FunctionExists(Condition):
    def __init__(self, name):
        self.name = name

    def code(self, context):
        code = '''SELECT
                        p.proname
                    FROM
                        pg_catalog.pg_proc p
                        INNER JOIN pg_catalog.pg_namespace ns ON (ns.oid = p.pronamespace)
                    WHERE
                        p.proname = $2 and ns.nspname = $1'''

        return code, self.name
