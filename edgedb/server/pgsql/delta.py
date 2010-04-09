##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import itertools
import os
import re

import postgresql

from semantix import caos
from semantix.caos import proto
from semantix.caos import delta as delta_cmds

from semantix.caos.backends.pgsql import common

from semantix.utils import datastructures
from semantix.utils.debug import debug
from semantix.utils.lang import yaml


base_type_name_map = {
    caos.Name('semantix.caos.builtins.str'): 'character varying',
    caos.Name('semantix.caos.builtins.int'): 'numeric',
    caos.Name('semantix.caos.builtins.bool'): 'boolean',
    caos.Name('semantix.caos.builtins.float'): 'double precision',
    caos.Name('semantix.caos.builtins.uuid'): 'uuid',
    caos.Name('semantix.caos.builtins.datetime'): 'timestamp with time zone'
}


base_type_name_map_r = {
    'character varying': caos.Name('semantix.caos.builtins.str'),
    'character': caos.Name('semantix.caos.builtins.str'),
    'text': caos.Name('semantix.caos.builtins.str'),
    'integer': caos.Name('semantix.caos.builtins.int'),
    'boolean': caos.Name('semantix.caos.builtins.bool'),
    'numeric': caos.Name('semantix.caos.builtins.int'),
    'double precision': caos.Name('semantix.caos.builtins.float'),
    'uuid': caos.Name('semantix.caos.builtins.uuid'),
    'timestamp with time zone': caos.Name('semantix.caos.builtins.datetime')
}


typmod_types = ('character', 'character varying', 'numeric')
fixed_length_types = {'character varying': 'character'}


class CommandMeta(delta_cmds.CommandMeta):
    pass


class MetaCommand(delta_cmds.Command, metaclass=CommandMeta):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pgops = datastructures.OrderedSet()

    def apply(self, meta, context=None):
        for op in self.ops:
            self.pgops.add(op)

    def execute(self, db):
        for op in sorted(self.pgops, key=lambda i: i.priority, reverse=True):
            op.execute(db)


class BaseCommand:
    def get_code_and_vars(self, db):
        code = self.code(db)
        assert code
        if isinstance(code, tuple):
            code, vars = code
        else:
            vars = None

        return code, vars

    def execute(self, db):
        code, vars = self.get_code_and_vars(db)
        return db.prepare(code)(*vars)


class Command(BaseCommand):
    def __init__(self, *, conditions=None, neg_conditions=None, priority=0):
        self.opid = id(self)
        self.conditions = conditions or set()
        self.neg_conditions = neg_conditions or set()
        self.priority = priority

    @debug
    def execute(self, db):
        ok = self.check_conditions(db, self.conditions, True) and \
             self.check_conditions(db, self.neg_conditions, False)

        result = None
        if ok:
            code, vars = self.get_code_and_vars(db)

            """LOG [caos.meta.sync.cmd] Sync command:
            print(self)
            """

            """LOG [caos.meta.sync.sql] Sync command code:
            print(code, vars)
            """

            if vars is not None:
                result = db.prepare(code)(*vars)
            else:
                result = db.execute(code)
        return result

    def check_conditions(self, db, conditions, positive):
        result = True
        if conditions:
            for condition in conditions:
                code, vars = condition.get_code_and_vars(db)
                result = db.prepare(code)(*vars)

                if bool(result) ^ positive:
                    result = False
                    break
            else:
                result = True

        return result


class PrototypeMetaCommand(MetaCommand, delta_cmds.PrototypeCommand):
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


class CreateAtomMod(MetaCommand, adapts=delta_cmds.CreateAtomMod):
    def apply(self, meta, context=None):
        result = delta_cmds.CreateAtomMod.apply(self, meta, context)
        MetaCommand.apply(self, meta, context)
        return result


class DeleteAtomMod(MetaCommand, adapts=delta_cmds.DeleteAtomMod):
    def apply(self, meta, context=None):
        result = delta_cmds.DeleteAtomMod.apply(self, meta, context)
        MetaCommand.apply(self, meta, context)
        return result


class AtomMetaCommand(PrototypeMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = AtomTable()

    @classmethod
    def get_atom_base_and_mods(cls, atom):
        if proto.Atom.is_prototype(atom.base):
            base = base_type_name_map.get(atom.base)
            if not base:
                base = common.atom_name_to_domain_name(atom.base)
        else:
            base = base_type_name_map[atom.name]

        has_max_length = has_min_length = None

        if base in typmod_types:
            for mod, modvalue in atom.mods.items():
                if issubclass(mod, proto.AtomModMaxLength):
                    has_max_length = modvalue
                    break

        if has_max_length:
            #
            # Convert basetype + max-length constraint into a postgres-native
            # type with typmod, e.g str[max-length: 20] --> varchar(20)
            #
            # Handle the case when min-length == max-length and yield a fixed-size
            # type correctly
            #
            has_min_length = False
            if base in fixed_length_types:
                for mod, modvalue in atom.mods.items():
                    if issubclass(mod, proto.AtomModMinLength):
                        has_min_length = modvalue
                        break

            if (has_min_length and has_min_length.value == has_max_length.value):
                base = fixed_length_types[base]
            else:
                has_min_length = False
            base += '(' + str(has_max_length.value) + ')'

        mods = set()
        extramods = set()

        directly_supported_mods = (proto.AtomModMaxLength, proto.AtomModMinLength,
                                   proto.AtomModRegExp)

        for mod in atom.mods.values():
            if ((has_max_length and isinstance(mod, proto.AtomModMaxLength))
                    or (has_min_length and isinstance(mod, proto.AtomModMinLength))):
                continue
            elif isinstance(mod, directly_supported_mods):
                mods.add(mod)
            else:
                extramods.add(mod)

        return base, has_min_length, has_max_length, mods, extramods

    def fill_record(self, rec=None, obj=None):
        rec, updates = super().fill_record(rec, obj)
        if rec:
            if rec.base:
                rec.base = str(rec.base)

            if rec.default not in (None, Default):
                rec.default = yaml.Language.dump(rec.default)

        return rec, updates


class CreateAtom(AtomMetaCommand, adapts=delta_cmds.CreateAtom):
    def apply(self, meta, context=None):
        atom = delta_cmds.CreateAtom.apply(self, meta, context)
        AtomMetaCommand.apply(self, meta, context)

        new_domain_name = common.atom_name_to_domain_name(atom.name, catenate=False)
        base, _, _, mods, extramods = self.get_atom_base_and_mods(atom)

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

        domain_name = common.atom_name_to_domain_name(self.old_name, catenate=False)
        new_domain_name = common.atom_name_to_domain_name(self.new_name, catenate=False)

        self.pgops.add(RenameDomain(name=domain_name, new_name=new_domain_name))
        updaterec = self.table.record(name=str(self.new_name))
        condition = [('name', str(self.old_name))]
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

        old_base, _, old_max_length, old_mods, _ = self.get_atom_base_and_mods(old_atom)
        base, _, _, new_mods, _ = self.get_atom_base_and_mods(new_atom)

        new_type = None

        if old_max_length and not old_mods and new_mods:
            rec = self.table.record(name=str(domain_name))
            rec, _ = self.fill_record(rec, obj=new_atom)
            self.pgops.add(CreateDomain(name=domain_name, base=base))

            new_type = common.qname(*domain_name)
        elif old_base != base:
            new_type = base

        if context and new_type:
            concept = context.get(delta_cmds.ConceptCommandContext)
            link = context.get(delta_cmds.LinkCommandContext)

            if concept and link:
                name = link.proto.base[0] if link.proto.implicit_derivative else link.proto.name
                column_name = common.caos_name_to_pg_colname(name)
                alter_type = AlterTableAlterColumnType(column_name, new_type)
                concept.op.alter_table.add_operation(alter_type)

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
        ops.add(Delete(table=AtomTable(), condition=[('name', str(old_domain_name))]))

        return atom


class CompositePrototypeMetaCommand(PrototypeMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.alter_table = None

    @classmethod
    def _pg_type_from_atom(cls, atom_obj):
        base, _, _, mods, _ = AtomMetaCommand.get_atom_base_and_mods(atom_obj)

        need_to_create = False

        if not atom_obj.automatic or mods:
            column_type = base_type_name_map.get(atom_obj.name)
            if not column_type:
                column_type = common.atom_name_to_domain_name(atom_obj.name)
                need_to_create = bool(mods)
        else:
            column_type = base

        return column_type, need_to_create

    @classmethod
    def pg_type_from_atom(cls, atom):
        return cls._pg_type_from_atom(atom)[0]

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
        self.pgops.add(CreateTable(table=concept_table))

        self.alter_table = AlterTable(new_table_name)

        concept = delta_cmds.CreateConcept.apply(self, meta, context)
        ConceptMetaCommand.apply(self, meta, context)

        fields = self.create_object(concept)

        if concept.name == 'semantix.caos.builtins.Object':
            col = Column(name='concept_id', type='integer', required=True)
            self.alter_table.add_operation(AlterTableAddColumn(col))

        constraint = PrimaryKey(columns=['semantix.caos.builtins.id'])
        self.alter_table.add_operation(AlterTableAddConstraint(constraint))

        bases = (common.concept_name_to_table_name(p, catenate=False)
                 for p in fields['base'] if proto.Concept.is_prototype(p))
        concept_table.bases = list(bases)

        if self.alter_table.ops:
            self.pgops.add(self.alter_table)

        return concept


class RenameConcept(ConceptMetaCommand, adapts=delta_cmds.RenameConcept):
    def apply(self, meta, context=None):
        proto = delta_cmds.RenameConcept.apply(self, meta, context)
        ConceptMetaCommand.apply(self, meta, context)
        self.rename(self.old_name, self.new_name)

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


class LinkMetaCommand(CompositePrototypeMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = LinkTable()

    def record_metadata(self, link, meta, context):
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

        return rec

    def get_columns(self, link, meta):
        columns = []

        if link.atomic():
            if not isinstance(link.target, proto.Atom):
                link.target = meta.get(link.target)

            column_type = self.pg_type_from_atom(link.target)

            name = link.base[0] if link.implicit_derivative else link.name
            column_name = common.caos_name_to_pg_colname(name)

            columns.append(Column(name=column_name, type=column_type,
                                  required=link.required))

        return columns

    def create_table(self, link, meta, conditional=False):
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
        if (not link.atomic() or link.properties) and not link.implicit_derivative:
            self.create_table(link, meta, conditional=True)

        if link.atomic() and link.implicit_derivative:
            concept = context.get(delta_cmds.ConceptCommandContext)
            assert concept, "Link command must be run in Concept command context"

            cols = self.get_columns(link, meta)

            for col in cols:
                concept.op.alter_table.add_operation(AlterTableAddColumn(col))

        if self.alter_table and self.alter_table.ops:
            self.pgops.add(self.alter_table)

        rec = self.record_metadata(link, meta, context)
        self.pgops.add(Insert(table=self.table, records=[rec], priority=1))

        return link


class RenameLink(LinkMetaCommand, adapts=delta_cmds.RenameLink):
    def apply(self, meta, context=None):
        result = delta_cmds.RenameLink.apply(self, meta, context)
        LinkMetaCommand.apply(self, meta, context)

        if context:
            concept = context.get(delta_cmds.ConceptCommandContext)
            if concept and result.atomic():
                table_name = common.concept_name_to_table_name(concept.proto.name, catenate=False)
                old_name = common.caos_name_to_pg_colname(self.old_name)
                new_name = common.caos_name_to_pg_colname(self.new_name)
                rename = AlterTableRenameColumn(table_name, old_name, new_name)
                self.pgops.add(rename)

        if self.alter_table and self.alter_table.ops:
            self.pgops.add(self.alter_table)

        rec = self.table.record()
        rec.name = str(self.new_name)
        self.pgops.add(Update(table=self.table, record=rec,
                              condition=[('name', str(self.old_name))], priority=1))

        return result


class AlterLink(LinkMetaCommand, adapts=delta_cmds.AlterLink):
    def apply(self, meta, context=None):
        result = delta_cmds.AlterLink.apply(self, meta, context)
        LinkMetaCommand.apply(self, meta, context)

        rec = self.record_metadata(result, meta, context)

        if rec:
            self.pgops.add(Update(table=self.table, record=rec,
                                  condition=[('name', str(result.name))], priority=1))

        if self.alter_table and self.alter_table.ops:
            self.pgops.add(self.alter_table)

        return result


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
        elif not result.atomic():
            old_table_name = common.link_name_to_table_name(result.name,
                                                            catenate=False)
            self.pgops.add(DropTable(name=old_table_name))

        self.pgops.add(Delete(table=self.table, condition=[('name', str(result.name))]))

        return result


class CreateLinkSet(MetaCommand, adapts=delta_cmds.CreateLinkSet):
    def apply(self, meta, context=None):
        result = delta_cmds.CreateLinkSet.apply(self, meta, context)
        MetaCommand.apply(self, meta, context)
        return result


class RenameLinkSet(MetaCommand, adapts=delta_cmds.RenameLinkSet):
    def apply(self, meta, context=None):
        result = delta_cmds.RenameLinkSet.apply(self, meta, context)
        MetaCommand.apply(self, meta, context)
        return result


class AlterLinkSet(MetaCommand, adapts=delta_cmds.AlterLinkSet):
    def apply(self, meta, context=None):
        result = delta_cmds.AlterLinkSet.apply(self, meta, context)
        MetaCommand.apply(self, meta, context)
        return result


class DeleteLinkSet(MetaCommand, adapts=delta_cmds.DeleteLinkSet):
    def apply(self, meta, context=None):
        result = delta_cmds.DeleteLinkSet.apply(self, meta, context)
        MetaCommand.apply(self, meta, context)
        return result


class LinkPropertyMetaCommand(CompositePrototypeMetaCommand):
    def get_columns(self, property, meta):
        columns = []

        if not isinstance(property.atom, proto.Atom):
            property.atom = meta.get(property.atom)

        column_type = self.pg_type_from_atom(property.atom)

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
        link.op.create_table(link.proto, meta, conditional=True)

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


class AlterRealm(MetaCommand, adapts=delta_cmds.AlterRealm):
    def apply(self, meta):
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

        delta_cmds.AlterRealm.apply(self, meta)
        MetaCommand.apply(self, meta)

    def is_material(self):
        return True

    def execute(self, db):
        for op in self.serialize_ops():
            op.execute(db)

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

    def code(self, db):
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
        vals = (('(%s)' % ', '.join(str(v) for col, v in row)) for row in self.records)
        return '<caos.sync.%s %s (%s)>' % (self.__class__.__name__, self.table.name, ', '.join(vals))


class Update(DMLOperation):
    def __init__(self, table, record, condition, *, priority=0):
        super().__init__(priority=priority)

        self.table = table
        self.record = record
        self.fields = [f for f, v in record if v is not Default]
        self.condition = condition

    def code(self, db):
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
    def code(self, db):
        code = super().code(db)
        cols = (common.quote_ident(c[0]) for c in self.condition)
        result = (code[0] + ' RETURNING %s' % (','.join(cols)), code[1])
        return result

    def execute(self, db):
        result = super().execute(db)

        if not result:
            op = Insert(self.table, records=[self.record])
            result = op.execute(db)

        return result


class Delete(DMLOperation):
    def __init__(self, table, condition, *, priority=0):
        super().__init__(priority=priority)

        self.table = table
        self.condition = condition

    def code(self, db):
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

    def code(self, db):
        code = 'PRIMARY KEY (%s)' % ', '.join(common.quote_ident(c) for c in self.columns)
        return code

class UniqueConstraint(DBObject):
    def __init__(self, columns):
        self.columns = columns

    def code(self, db):
        code = 'UNIQUE (%s)' % ', '.join(common.quote_ident(c) for c in self.columns)
        return code


class Column(DBObject):
    def __init__(self, name, type, required=False, default=None, readonly=False):
        self.name = name
        self.type = type
        self.required = required
        self.default = default
        self.readonly = readonly

    def code(self, db):
        e = common.quote_ident
        return '%s %s %s %s' % (common.quote_ident(self.name), self.type,
                                'NOT NULL' if self.required else '',
                                ('DEFAULT %s' % self.default) if self.default is not None else '')


class DefaultMeta(type):
    def __bool__(cls):
        return False


class Default(metaclass=DefaultMeta):
    pass


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
        ])

        self.constraints = set([
            PrimaryKey(columns=('id',)),
            UniqueConstraint(columns=('name',))
        ])


class Feature:
    def __init__(self, name, schema='caos'):
        self.name = name
        self.schema = schema

    def code(self, db):
        pgpath = os.getenv('SEMANTIX_PGPATH', '/usr/share/postgresql-%(version)s')
        source = self.source % {'pgpath': pgpath}
        source = source % {'version': '%s.%s' % db.version_info[:2]}

        with open(source, 'r') as f:
            code = re.sub(r'SET\s+search_path\s*=\s*[^;]+;',
                          'SET search_path = %s;' % common.quote_ident(self.schema),
                          f.read())
        return code


class TypeExists(Condition):
    def __init__(self, name):
        self.name = name

    def code(self, db):
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

    def code(self, db):
        return self.feature.code(db)

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.feature.name)


class EnableHstoreFeature(EnableFeature):
    def execute(self, db):
        super().execute(db)
        self.init_hstore(db)

    @classmethod
    def init_hstore(cls, db):
        try:
            db.typio.identify(contrib_hstore='caos.hstore')
        except postgresql.exceptions.SchemaNameError:
            pass


class SchemaExists(Condition):
    def __init__(self, name):
        self.name = name

    def code(self, db):
        return ('SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = $1', [self.name])


class CreateSchema(DDLOperation):
    def __init__(self, name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.name = name
        self.opid = name
        self.neg_conditions.add(SchemaExists(self.name))

    def code(self, db):
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

    def code(self, db):
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

    def code(self, db):
        return 'CREATE DOMAIN %s AS %s' % (common.qname(*self.name), self.base)


class RenameDomain(SchemaObjectOperation):
    def __init__(self, name, new_name):
        super().__init__(name)
        self.new_name = new_name

    def code(self, db):
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
    def code(self, db):
        return 'DROP DOMAIN %s' % common.qname(*self.name)


class AlterDomain(DDLOperation):
    def __init__(self, name):
        super().__init__()

        self.name = name


    def code(self, db):
        return 'ALTER DOMAIN %s ' % common.qname(*self.name)

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.name)


class AlterDomainAlterDefault(AlterDomain):
    def __init__(self, name, default):
        super().__init__(name)
        self.default = default

    def code(self, db):
        code = super().code(db)
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

    def code(self, db):
        code = super().code(db)
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

        return 'CHECK (%s)' % expr


class AlterDomainDropConstraint(AlterDomainAlterConstraint):
    def code(self, db):
        code = super().code(db)
        code += ' DROP CONSTRAINT %s ' % self.constraint_name(self.constraint)
        return code


class AlterDomainAddConstraint(AlterDomainAlterConstraint):
    def code(self, db):
        code = super().code(db)
        code += ' ADD CONSTRAINT %s %s' % (self.constraint_name(self.constraint),
                                           self.constraint_code(self.constraint))
        return code


class TableExists(Condition):
    def __init__(self, name):
        self.name = name

    def code(self, db):
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

    def code(self, db):
        elems = [c.code(db) for c in self.table.columns(only_self=True)]
        elems += [c.code(db) for c in self.table.constraints]
        code = 'CREATE TABLE %s (%s)' % (common.qname(*self.table.name), ', '.join(c for c in elems))

        if self.table.bases:
            code += ' INHERITS (' + ','.join(common.qname(*b) for b in self.table.bases) + ')'

        return code


class DropTable(SchemaObjectOperation):
    def code(self, db):
        return 'DROP TABLE %s' % common.qname(*self.name)


class AlterTableBase(DDLOperation):
    def __init__(self, name):
        super().__init__()
        self.name = name

    def code(self, db):
        return 'ALTER TABLE %s' % common.qname(*self.name)

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.name)


class AlterTable(AlterTableBase):
    def __init__(self, name):
        super().__init__(name)
        self.ops = []

    def add_operation(self, op):
        self.ops.append(op)

    def code(self, db):
        if self.ops:
            code = super().code(db)
            code += ' ' + ', '.join(op.code(db) for op in self.ops)
            return code


class AlterTableAddColumn(DDLOperation):
    def __init__(self, column):
        self.column = column

    def code(self, db):
        return 'ADD COLUMN ' + self.column.code(db)


class AlterTableDropColumn(DDLOperation):
    def __init__(self, column):
        self.column = column

    def code(self, db):
        return 'DROP COLUMN %s' % common.quote_ident(self.column.name)


class AlterTableAlterColumnType(DDLOperation):
    def __init__(self, column_name, new_type):
        self.column_name = column_name
        self.new_type = new_type

    def code(self, db):
        return 'ALTER COLUMN %s SET DATA TYPE %s' % \
                (common.quote_ident(str(self.column_name)), self.new_type)


class AlterTableAddConstraint(DDLOperation):
    def __init__(self, constraint):
        self.constraint = constraint

    def code(self, db):
        return 'ADD  ' + self.constraint.code(db)


class AlterTableSetSchema(AlterTableBase):
    def __init__(self, name, schema):
        super().__init__(name)
        self.schema = schema

    def code(self, db):
        code = super().code(db)
        code += ' SET SCHEMA %s ' % common.quote_ident(self.new_name)
        return code


class AlterTableRenameTo(AlterTableBase):
    def __init__(self, name, new_name):
        super().__init__(name)
        self.new_name = new_name

    def code(self, db):
        code = super().code(db)
        code += ' RENAME TO %s ' % common.quote_ident(self.new_name)
        return code


class AlterTableRenameColumn(AlterTableBase):
    def __init__(self, name, old_col_name, new_col_name):
        super().__init__(name)
        self.old_col_name = old_col_name
        self.new_col_name = new_col_name

    def code(self, db):
        code = super().code(db)
        code += ' RENAME COLUMN %s TO %s ' % (common.quote_ident(self.old_col_name),
                                              common.quote_ident(self.new_col_name))
        return code


class FunctionExists(Condition):
    def __init__(self, name):
        self.name = name

    def code(self, db):
        code = '''SELECT
                        p.proname
                    FROM
                        pg_catalog.pg_proc p
                        INNER JOIN pg_catalog.pg_namespace ns ON (ns.oid = p.pronamespace)
                    WHERE
                        p.proname = $2 and ns.nspname = $1'''

        return code, self.name
