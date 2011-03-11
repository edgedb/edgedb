##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import itertools
import re

import postgresql

from semantix import caos
from semantix.caos import proto
from semantix.caos import delta as delta_cmds
from semantix.caos.caosql import expr as caosql_expr
from semantix.caos import objects as caos_objects

from semantix.caos.backends.pgsql import common, Config

from semantix.utils import datastructures
from semantix.utils.debug import debug
from semantix.utils.lang import yaml
from semantix.utils.algos.persistent_hash import persistent_hash
from semantix.utils import helper

from . import ast as pg_ast
from . import codegen
from . import datasources
from . import transformer
from . import types


BACKEND_FORMAT_VERSION = 1


class CommandMeta(delta_cmds.CommandMeta):
    pass


class MetaCommand(delta_cmds.Command, metaclass=CommandMeta):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pgops = datastructures.OrderedSet()

    def apply(self, meta, context=None):
        for op in self.ops:
            self.pgops.add(op)

    @debug
    def execute(self, context):
        """LINE [caos.delta.execute] EXECUTING
        repr(self)
        """
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
        assert code is not None
        if isinstance(code, tuple):
            code, vars = code
        else:
            vars = None

        return code, vars

    @debug
    def execute(self, context):
        code, vars = self.get_code_and_vars(context)

        if code:
            """LOG [caos.sql] Sync command code:
            print(code, vars)
            """

            """LINE [caos.delta.execute] EXECUTING
            repr(self)
            """
            result = context.db.prepare(code)(*vars)
            extra = self.extra(context)
            if extra:
                for cmd in extra:
                    cmd.execute(context)
            return result

    def dump(self):
        return str(self)

    def code(self, context):
        return ''

    def extra(self, context, *args, **kwargs):
        return None


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

            if code:
                """LOG [caos.sql] Sync command code:
                print(code, vars)
                """

                """LINE [caos.delta.execute] EXECUTING
                repr(self)
                """

                if vars is not None:
                    result = context.db.prepare(code)(*vars)
                else:
                    result = context.db.execute(code)

                extra = self.extra(context)
                if extra:
                    for cmd in extra:
                        cmd.execute(context)
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


class CommandGroup(Command):
    def __init__(self, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.commands = []

    def add_command(self, cmd):
        self.commands.append(cmd)

    def add_commands(self, cmds):
        self.commands.extend(cmds)

    def execute(self, context):
        result = None
        ok = self.check_conditions(context, self.conditions, True) and \
             self.check_conditions(context, self.neg_conditions, False)

        if ok:
            result = [c.execute(context) for c in self.commands]

        return result

    def dump(self):
        result = [repr(self)]

        for op in self.commands:
            result.extend('  %s' % l for l in op.dump().split('\n'))

        return '\n'.join(result)

    def __iter__(self):
        return iter(self.commands)


class PrototypeMetaCommand(MetaCommand, delta_cmds.PrototypeCommand):
    pass


class NamedPrototypeMetaCommand(PrototypeMetaCommand, delta_cmds.NamedPrototypeCommand):
    def fill_record(self, rec=None, obj=None):
        updates = {}

        myrec = self.table.record()

        if not obj:
            for name, value in itertools.chain(self.get_struct_properties(True).items(),
                                               self.get_properties(('source', 'target'), True).items()):
                updates[name] = value
                if hasattr(myrec, name):
                    if not rec:
                        rec = self.table.record()
                    setattr(rec, name, value[1])
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
                rec.description = rec.description

        return rec, updates

    def pack_default(self, alter_default):
        if alter_default.new_value is not None:
            return yaml.Language.dump(alter_default.new_value)
        else:
            result = None
        return result

    def create_module(self, module_name):
        schema_name = common.caos_module_name_to_schema_name(module_name)
        condition = SchemaExists(name=schema_name)

        cmd = CommandGroup(neg_conditions={condition})
        cmd.add_command(CreateSchema(name=schema_name))

        modtab = ModuleTable()
        rec = modtab.record()
        rec.name = module_name
        rec.schema_name = schema_name
        cmd.add_command(Insert(modtab, [rec]))

        self.pgops.add(cmd)

    def create_object(self, prototype):
        rec, updates = self.fill_record()
        self.pgops.add(Insert(table=self.table, records=[rec]))

        self.create_module(prototype.name.module)

        return updates


class AlterPrototypeProperty(MetaCommand, adapts=delta_cmds.AlterPrototypeProperty):
    pass


class AlterDefault(MetaCommand, adapts=delta_cmds.AlterDefault):
    def apply(self, meta, context):
        result = delta_cmds.AlterDefault.apply(self, meta, context)
        MetaCommand.apply(self, meta, context)

        return result


class CreateAtomConstraint(PrototypeMetaCommand, adapts=delta_cmds.CreateAtomConstraint):
    def apply(self, meta, context=None):
        result = delta_cmds.CreateAtomConstraint.apply(self, meta, context)
        PrototypeMetaCommand.apply(self, meta, context)
        return result


class AlterAtomConstraint(PrototypeMetaCommand, adapts=delta_cmds.AlterAtomConstraint):
    def apply(self, meta, context=None):
        result = delta_cmds.AlterAtomConstraint.apply(self, meta, context)
        PrototypeMetaCommand.apply(self, meta, context)
        return result


class DeleteAtomConstraint(PrototypeMetaCommand, adapts=delta_cmds.DeleteAtomConstraint):
    def apply(self, meta, context=None):
        result = delta_cmds.DeleteAtomConstraint.apply(self, meta, context)
        PrototypeMetaCommand.apply(self, meta, context)
        return result


class AtomMetaCommand(NamedPrototypeMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = AtomTable()

    def fill_record(self, rec=None, obj=None):
        rec, updates = super().fill_record(rec, obj)
        if rec:
            if rec.base:
                rec.base = str(rec.base)

        default = list(self(delta_cmds.AlterDefault))
        if default:
            if not rec:
                rec = self.table.record()
            rec.default = self.pack_default(default[0])

        return rec, updates

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

        base, constraints_encoded, new_constraints, _ = types.get_atom_base_and_constraints(meta, atom)

        target_type = new_type

        if intent == 'alter':
            simple_alter = atom.automatic and not new_constraints
            if not simple_alter:
                new_name = domain_name[0], domain_name[1] + '_tmp'
                self.pgops.add(RenameDomain(domain_name, new_name))
                target_type = common.qname(*domain_name)

                self.pgops.add(CreateDomain(name=domain_name, base=new_type))
                for constraint in new_constraints:
                    self.pgops.add(AlterDomainAddConstraint(name=domain_name, constraint=constraint))

                domain_name = new_name
        elif intent == 'create':
            self.pgops.add(CreateDomain(name=domain_name, base=base))

        for host_proto, item_proto in users:
            if isinstance(item_proto, proto.Link):
                name = item_proto.normal_name()
            else:
                name = item_proto.name

            table_name = common.get_table_name(host_proto, catenate=False)
            column_name = common.caos_name_to_pg_name(name)

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
        base, _, constraints, extraconstraints = types.get_atom_base_and_constraints(meta, atom)

        updates = self.create_object(atom)

        if not atom.automatic:
            self.pgops.add(CreateDomain(name=new_domain_name, base=base))

            if atom.issubclass(meta, caos_objects.sequence.Sequence):
                seq_name = common.atom_name_to_sequence_name(atom.name, catenate=False)
                self.pgops.add(CreateSequence(name=seq_name))

            for constraint in constraints:
                self.pgops.add(AlterDomainAddConstraint(name=new_domain_name, constraint=constraint))

            default = list(self(delta_cmds.AlterDefault))

            if default:
                default = default[0]
                if len(default.new_value) > 0 and \
                                        isinstance(default.new_value[0], proto.LiteralDefaultSpec):
                    # We only care to support literal defaults here.  Supporting
                    # defaults based on queries has no sense on the database level
                    # since the database forbids queries for DEFAULT and pre-calculating
                    # the value does not make sense either since the whole point of
                    # query defaults is for them to be dynamic.
                    self.pgops.add(AlterDomainAlterDefault(name=new_domain_name,
                                                           default=default.new_value[0].value))
        else:
            source, pointer = CompositePrototypeMetaCommand.get_source_and_pointer_ctx(meta, context)

            # Skip inherited links
            if pointer.proto.source.name == source.proto.name:
                alter_table = source.op.get_alter_table(context)

                for constraint in constraints:
                    constraint = source.op.get_pointer_constraint(meta, context, constraint)
                    op = AlterTableAddConstraint(constraint=constraint)
                    alter_table.add_operation(op)


        if extraconstraints:
            values = {}

            for constraint in extraconstraints:
                cls = constraint.__class__.get_canonical_class()
                key = '%s.%s' % (cls.__module__, cls.__name__)
                values[key] = yaml.Language.dump(constraint.get_value())

            rec = self.table.record()
            rec.constraints = values
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

        if not proto.automatic and proto.issubclass(meta, caos_objects.sequence.Sequence):
            seq_name = common.atom_name_to_sequence_name(self.prototype_name, catenate=False)
            new_seq_name = common.atom_name_to_sequence_name(self.new_name, catenate=False)

            self.pgops.add(RenameSequence(name=seq_name, new_name=new_seq_name))

        return proto


class AlterAtom(AtomMetaCommand, adapts=delta_cmds.AlterAtom):
    def apply(self, meta, context=None):
        old_atom = meta.get(self.prototype_name).copy()
        new_atom = delta_cmds.AlterAtom.apply(self, meta, context)
        AtomMetaCommand.apply(self, meta, context)

        updaterec, updates = self.fill_record()

        if updaterec:
            condition = [('name', str(old_atom.name))]
            self.pgops.add(Update(table=self.table, record=updaterec, condition=condition))

        self.alter_atom(self, meta, context, old_atom, new_atom, updates=updates)

        return new_atom

    @classmethod
    def alter_atom(cls, op, meta, context, old_atom, new_atom, in_place=True, updates=None):

        old_base, old_constraints_encoded, old_constraints, _ = \
                                                types.get_atom_base_and_constraints(meta, old_atom)
        base, constraints_encoded, new_constraints, _ = \
                                                types.get_atom_base_and_constraints(meta, new_atom)

        domain_name = common.atom_name_to_domain_name(new_atom.name, catenate=False)

        new_type = None
        type_intent = 'alter'

        source, pointer = CompositePrototypeMetaCommand.get_source_and_pointer_ctx(meta, context)

        if new_atom.automatic:
            if old_constraints_encoded and not old_constraints and new_constraints:
                new_type = common.qname(*domain_name)
                type_intent = 'create'
            elif old_constraints_encoded and old_constraints and not new_constraints:
                new_type = base
                type_intent = 'drop'
        elif old_atom.automatic:
            type_intent = 'drop'

        if not new_type and old_base != base:
            new_type = base

        if new_type:
            # The change of the underlying data type for domains is a complex problem.
            # There is no direct way in PostgreSQL to change the base type of a domain.
            # Instead, a new domain must be created, all users of the old domain altered
            # to use the new one, and then the old domain dropped.  Obviously this
            # recurses down to every child domain.
            #
            source_proto = source.proto if source else None
            pointer_proto = pointer.proto if pointer else None

            if in_place:
                op.alter_atom_type(new_atom, meta, source_proto, pointer_proto, new_type,
                                   intent=type_intent)

        if type_intent != 'drop':
            if updates:
                default_delta = list(op(delta_cmds.AlterDefault))
                if default_delta:
                    default_delta = default_delta[0]
                    if not new_atom.automatic:
                        if not default_delta.new_value or \
                           not isinstance(default_delta.new_value[0], proto.LiteralDefaultSpec):
                            new_default = None
                        else:
                            new_default = default_delta.new_value[0].value
                        # Only non-automatic atoms can get their own defaults.
                        # Automatic atoms are not represented by domains and inherit
                        # their defaults from parent.
                        op.pgops.add(AlterDomainAlterDefault(name=domain_name, default=new_default))

            if new_atom.automatic:
                alter_table = source.op.get_alter_table(context)

                for constraint in old_constraints - new_constraints:
                    constraint = source.op.get_pointer_constraint(meta, context, constraint)
                    op = AlterTableDropConstraint(constraint=constraint)
                    alter_table.add_operation(op)

                for constraint in new_constraints - old_constraints:
                    constraint = source.op.get_pointer_constraint(meta, context, constraint)
                    op = AlterTableAddConstraint(constraint=constraint)
                    alter_table.add_operation(op)

            else:
                for constraint in old_constraints - new_constraints:
                    op.pgops.add(AlterDomainDropConstraint(name=domain_name, constraint=constraint))

                for constraint in new_constraints - old_constraints:
                    op.pgops.add(AlterDomainAddConstraint(name=domain_name, constraint=constraint))
        else:
            # We need to drop orphan constraints
            if old_atom.automatic:
                alter_table = source.op.get_alter_table(context)

                for constraint in old_constraints:
                    constraint = source.op.get_pointer_constraint(meta, context, constraint)
                    op = AlterTableDropConstraint(constraint=constraint)
                    alter_table.add_operation(op)



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

        if not atom.automatic and atom.issubclass(meta, caos_objects.sequence.Sequence):
            seq_name = common.atom_name_to_sequence_name(self.prototype_name, catenate=False)
            self.pgops.add(DropSequence(name=seq_name))

        return atom


class UpdateSearchIndexes(MetaCommand):
    def __init__(self, host, **kwargs):
        super().__init__(**kwargs)
        self.host = host

    def get_index_name(self, host_table_name, language, index_class='default'):
        name = '%s_%s_%s_search_idx' % (host_table_name[1], language, index_class)
        return common.caos_name_to_pg_name(name)

    def apply(self, meta, context):
        if isinstance(self.host, caos.types.ProtoConcept):
            columns = []

            names = sorted(self.host.pointers.keys())

            for link_name in names:
                for link in self.host.pointers[link_name]:
                    if getattr(link, 'search', None):
                        column_name = common.caos_name_to_pg_name(link_name)
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
        self.table_name = None
        self.alter_tables = {}
        self.update_search_indexes = None
        self.pointer_constraints = {}
        self.abstract_pointer_constraints = {}
        self.dropped_pointer_constraints = {}

    def get_alter_table(self, context, priority=0, force_new=False, contained=False, manual=False):
        key = (priority, contained)
        alter_table = self.alter_tables.get(key)
        if alter_table is None or force_new or manual:
            if not self.table_name:
                assert self.__class__.context_class
                ctx = context.get(self.__class__.context_class)
                assert ctx
                self.table_name = common.get_table_name(ctx.proto, catenate=False)
            alter_table = AlterTable(self.table_name, priority=priority, contained=contained)
            if not manual:
                self.alter_tables.setdefault(key, []).append(alter_table)
        else:
            alter_table = alter_table[-1]

        return alter_table

    def attach_alter_table(self, context, priority=None, clear=True):
        if priority:
            alter_tables = list(self.alter_tables.get(priority))
            if alter_tables and clear:
                self.alter_tables[priority][:] = ()
        else:
            alter_tables = list(itertools.chain.from_iterable(self.alter_tables.values()))
            if alter_tables:
                if clear:
                    self.alter_tables.clear()
                alter_tables = sorted(alter_tables, key=lambda i: i.priority)

        if alter_tables:
            self.pgops.update(alter_tables)

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

        old_func_name = (old_table_name[0],
                         common.caos_name_to_pg_name(old_name.name + '_batch_merger'))
        new_func_name = (new_table_name[0],
                         common.caos_name_to_pg_name(new_name.name + '_batch_merger'))

        cond = FunctionExists(old_func_name, args=('text',),)
        cmd = RenameFunction(old_func_name, args=('text',), new_name=new_func_name,
                             conditions=(cond,))
        self.pgops.add(cmd)

    def delete(self, proto, meta, context):
        schema = common.caos_module_name_to_schema_name(proto.name.module)
        name = common.caos_name_to_pg_name(proto.name.name + '_batch_merger')
        func_name = (schema, name)
        cond = FunctionExists(func_name, args=('text',),)
        cmd = DropFunction(func_name, args=('text',), conditions=(cond,))
        self.pgops.add(cmd)

    def search_index_add(self, host, pointer, meta, context):
        if self.update_search_indexes is None:
            self.update_search_indexes = UpdateSearchIndexes(host)

    def search_index_alter(self, host, pointer, meta, context):
        if self.update_search_indexes is None:
            self.update_search_indexes = UpdateSearchIndexes(host)

    def search_index_delete(self, host, pointer, meta, context):
        if self.update_search_indexes is None:
            self.update_search_indexes = UpdateSearchIndexes(host)

    def adjust_indexes(self, meta, context, source):
        source_context = context.get(delta_cmds.LinkCommandContext)
        if not source_context:
            source_context = context.get(delta_cmds.ConceptCommandContext)
        source_table = common.get_table_name(source_context.proto, catenate=False)
        for index in source_context.proto.indexes:
            old_name = SourceIndexCommand.get_index_name(source_context.original_proto, index)
            new_name = SourceIndexCommand.get_index_name(source_context.proto, index)

            self.pgops.add(RenameIndex(old_name=(source_table[0], old_name), new_name=new_name))

    @classmethod
    def get_source_and_pointer_ctx(cls, meta, context):
        if context:
            concept = context.get(delta_cmds.ConceptCommandContext)
            link = context.get(delta_cmds.LinkCommandContext)
        else:
            concept = link = None

        if concept and link:
            source, pointer = concept, link
        elif link:
            property = context.get(delta_cmds.LinkPropertyCommandContext)
            source, pointer = link, property
        else:
            source = pointer = None

        return source, pointer

    @classmethod
    def get_pointer_constraint(cls, meta, context, constraint, original=False,
                                                               source=None, pointer_name=None):
        if not source:
            host, pointer = cls.get_source_and_pointer_ctx(meta, context)

            if original:
                source, pointer_name = host.original_proto, pointer.original_proto.normal_name()
            else:
                source, pointer_name = host.proto, pointer.proto.normal_name()

        column_name = common.caos_name_to_pg_name(pointer_name)
        prefix = (source.name, pointer_name)
        table_name = common.get_table_name(source, catenate=False)

        if isinstance(constraint, proto.AtomConstraint):
            constraint = AtomConstraintTableConstraint(table_name=table_name,
                                                column_name=column_name,
                                                prefix=prefix,
                                                constraint=constraint)
        else:
            constraint = PointerConstraintTableConstraint(table_name=table_name,
                                                          column_name=column_name,
                                                          prefix=prefix,
                                                          constraint=constraint)

        return constraint

    def apply_inherited_deltas(self, source, meta, context):
        top_ctx = context.get(delta_cmds.RealmCommandContext)

        if isinstance(source, caos.types.ProtoConcept):
            cmd_class = delta_cmds.ConceptCommand
        else:
            cmd_class = delta_cmds.LinkCommand

        proto_idx = {cmd.prototype: cmd for cmd in top_ctx.op(cmd_class)
                                        if getattr(cmd, 'prototype', None)}

        for pointer_name in source.pointers:
            if pointer_name not in source.own_pointers:
                # Get the nearest source defining the pointer, i. e., the source
                # that has pointer_name in its own_pointers index.
                #
                origin = source.get_pointer_origin(meta, pointer_name)
                origin_op = proto_idx.get(origin)
                if origin_op:
                    # Replicate each pointer origin constraint operation.
                    # Usually, the origin operation will iterate through children and
                    # replicate the constraint creation itself, but it is obviously
                    # unable to do so for children that appear after the parent operation
                    # is applied, so we must fill the gap here.
                    #
                    for constr in origin_op.pointer_constraints.get(pointer_name, {}).values():
                        self.add_pointer_constraint(source, pointer_name,
                                                    constr, meta, context)
                    abstract_constr = origin_op.abstract_pointer_constraints.get(pointer_name, {})
                    if abstract_constr:
                        for constr in abstract_constr.values():
                            self.add_pointer_constraint(source, pointer_name,
                                                        constr, meta, context)

    def affirm_pointer_defaults(self, source, meta, context):
        for pointer_name, pointer in source.pointers.items():
            if isinstance(pointer, proto.LinkSet):
                pointer = pointer.first

            if pointer.generic() or not pointer.atomic() or \
                    isinstance(pointer, proto.Computable) or not pointer.default:
                continue

            default = None
            ld = list(filter(lambda i: isinstance(i, proto.LiteralDefaultSpec),
                             pointer.default))
            if ld:
                default = ld[0].value

            if default is not None:
                alter_table = self.get_alter_table(context, priority=3, contained=True)
                column_name = common.caos_name_to_pg_name(pointer_name)
                alter_table.add_operation(AlterTableAlterColumnDefault(column_name=column_name,
                                                                       default=default))

    def create_pointer_constraints(self, source, meta, context):
        for pointer_name, pointer in source.pointers.items():
            if pointer_name not in source.own_pointers:
                if isinstance(pointer, proto.LinkSet):
                    pointer = pointer.first

                if pointer.generic() or not pointer.atomic() or isinstance(pointer, proto.Computable):
                    continue

                constraints = itertools.chain(pointer.constraints.values(),
                                              pointer.abstract_constraints.values())
                for constraint in constraints:
                    if isinstance(constraint, proto.PointerConstraintUnique):
                        key = persistent_hash(constraint)
                        try:
                            existing = self.pointer_constraints[pointer_name][key]
                        except KeyError:
                            self.add_pointer_constraint(source, pointer_name, constraint,
                                                                meta, context)

    def adjust_pointer_constraints(self, meta, context, source):

        for pointer in (p for p in source.pointers.values() if p.atomic()):
            if isinstance(pointer, proto.LinkSet):
                pointer = pointer.first

            target = pointer.target

            pointer_name = pointer.normal_name()

            if isinstance(source, caos.types.ProtoConcept):
                ctx_class = delta_cmds.ConceptCommandContext
                ptr_ctx_class = delta_cmds.LinkCommandContext
                ptr_op_class = delta_cmds.AlterLink
            elif isinstance(source, caos.types.ProtoLink):
                ctx_class = delta_cmds.LinkCommandContext
                ptr_ctx_class = delta_cmds.LinkPropertyCommandContext
                ptr_op_class = delta_cmds.AlterLinkProperty
            else:
                assert False

            source_context = context.get(ctx_class)
            alter_table = source_context.op.get_alter_table(context)
            table = common.get_table_name(source, catenate=False)

            drop_constraints = {}

            for op in alter_table(TableConstraintCommand):
                if isinstance(op, AlterTableDropConstraint):
                    name = op.constraint.raw_constraint_name()
                    drop_constraints[name] = op

            if pointer_name in source.own_pointers:
                ptr_op = ptr_op_class(prototype_name=pointer.name,
                                      prototype_class=pointer.__class__.get_canonical_class())


                if target.automatic:
                    # We need to establish fake AlterLink context here since
                    # atom constraint constraint ops need it.
                    with context(ptr_ctx_class(ptr_op, pointer)):
                        for constraint in target.effective_local_constraints.values():
                            old_constraint = self.get_pointer_constraint(meta, context, constraint,
                                                                         original=True)

                            if old_constraint.raw_constraint_name() in drop_constraints:
                                # No need to rename constraints that are to be dropped
                                continue

                            new_constraint = self.get_pointer_constraint(meta, context, constraint)

                            op = AlterTableRenameConstraint(table_name=table,
                                                            constraint=old_constraint,
                                                            new_constraint=new_constraint)
                            self.pgops.add(op)

            if not pointer.generic():
                orig_source = source_context.original_proto

                ptr_op = ptr_op_class(prototype_name=pointer.name,
                                      prototype_class=pointer.__class__.get_canonical_class())

                with context(ptr_ctx_class(ptr_op, pointer)):
                    for constraint in pointer.constraints.values():
                        if isinstance(constraint, proto.PointerConstraintUnique):
                            old_constraint = self.get_pointer_constraint(meta, context,
                                                                         constraint,
                                                                         original=True)

                            if old_constraint.raw_constraint_name() in drop_constraints:
                                # No need to rename constraints that are to be dropped
                                continue

                            new_constraint = self.get_pointer_constraint(meta, context,
                                                                         constraint)

                            op = self.rename_pointer_constraint(orig_source, source, pointer_name,
                                                                old_constraint, new_constraint,
                                                                meta, context)
                            self.pgops.add(op)


    def add_pointer_constraint(self, source, pointer_name, constraint, meta, context):
        constr_key = persistent_hash(constraint)
        self_constrs = self.pointer_constraints.get(pointer_name)

        if not self_constrs or constr_key not in self_constrs:
            alter_table = self.get_alter_table(context, priority=2)
            constr = self.get_pointer_constraint(meta, context, constraint,
                                                 source=source, pointer_name=pointer_name)
            op = AlterTableAddConstraint(constraint=constr)
            alter_table.add_operation(op)

            constraint_origins = source.get_constraint_origins(meta, pointer_name, constraint)
            assert constraint_origins

            self.pgops.add(self.create_unique_constraint_trigger(source, pointer_name, constr,
                                                                 constraint_origins, meta, context))

            self.pointer_constraints.setdefault(pointer_name, {})[constr_key] = constraint

    def del_pointer_constraint(self, source, pointer_name, constraint, meta, context,
                               conditional=False):
        constr_key = persistent_hash(constraint)
        self_constrs = self.dropped_pointer_constraints.get(pointer_name)

        if not self_constrs or constr_key not in self_constrs:
            alter_table = self.get_alter_table(context, priority=2, force_new=conditional,
                                               manual=conditional)
            constr = self.get_pointer_constraint(meta, context, constraint,
                                                 source=source, pointer_name=pointer_name)
            op = AlterTableDropConstraint(constraint=constr)
            alter_table.add_operation(op)

            if not conditional:
                constraint_origins = source.get_constraint_origins(meta, pointer_name, constraint)
                assert constraint_origins

            drop_trig = self.drop_unique_constraint_trigger(source, pointer_name, constr,
                                                            meta, context)

            if conditional:
                cond = self.unique_constraint_trigger_exists(source, pointer_name, constr,
                                                             meta, context)
                op = CommandGroup(conditions=[cond])
                op.add_commands([alter_table, drop_trig])
                self.pgops.add(op)
            else:
                self.pgops.add(drop_trig)
                self.dropped_pointer_constraints.setdefault(pointer_name, {})[constr_key] = constraint

    def rename_pointer_constraint(self, orig_source, source, pointer_name,
                                        old_constraint, new_constraint, meta, context):

        table = common.get_table_name(source, catenate=False)

        result = CommandGroup()

        result.add_command(AlterTableRenameConstraint(table_name=table,
                                                      constraint=old_constraint,
                                                      new_constraint=new_constraint))

        ops = self.rename_unique_constraint_trigger(orig_source, source, pointer_name,
                                                    old_constraint, new_constraint, meta, context)

        result.add_commands(ops)

        return result

    def create_unique_constraint_trigger(self, source, pointer_name, constraint,
                                               constraint_origins, meta, context, priority=3):

        colname = common.quote_ident(common.caos_name_to_pg_name(pointer_name))
        if len(constraint_origins) == 1:
            origin = common.get_table_name(next(iter(constraint_origins)))
        else:
            origin = []
            for o in constraint_origins:
                origin.append('(SELECT * FROM %s)' % common.get_table_name(o))
            origin = ' UNION ALL '.join(origin)

        text = '''
                  BEGIN
                  PERFORM
                        TRUE
                      FROM %(origin)s
                      WHERE %(colname)s = NEW.%(colname)s;
                  IF FOUND THEN
                      RAISE unique_violation
                          USING
                              MESSAGE = 'duplicate key value violates unique constraint %(constr)s',
                              DETAIL = 'Key (%(colname)s)=(' || NEW.%(colname)s || ') already exists.';
                  END IF;
                  RETURN NEW;
                  END;
               ''' % {'colname': colname,
                      'origin': origin,
                      'constr': constraint.constraint_name()}

        schema = common.caos_module_name_to_schema_name(source.name.module)
        proc_name = constraint.raw_constraint_name() + '_trigproc'
        proc_name = schema, common.caos_name_to_pg_name(proc_name)
        table_name = common.get_table_name(source, catenate=False)
        proc = CreateTriggerFunction(name=proc_name, text=text, volatility='stable')

        trigger_name = common.caos_name_to_pg_name(constraint.raw_constraint_name() + '_instrigger')
        instrigger = CreateConstraintTrigger(trigger_name=trigger_name,
                                             table_name=table_name,
                                             events=('insert',), procedure=proc_name)

        trigger_name = common.caos_name_to_pg_name(constraint.raw_constraint_name() + '_updtrigger')
        condition = 'OLD.%(colname)s IS DISTINCT FROM NEW.%(colname)s' % {'colname': colname}
        updtrigger = CreateConstraintTrigger(trigger_name=trigger_name,
                                             table_name=table_name,
                                             events=('update',),
                                             condition=condition, procedure=proc_name)

        result = CommandGroup(priority=priority)
        result.add_command(proc)
        result.add_command(instrigger)
        result.add_command(updtrigger)

        return result

    def unique_constraint_trigger_exists(self, source, pointer_name, constraint, meta, context):
        schema = common.caos_module_name_to_schema_name(source.name.module)
        proc_name = constraint.raw_constraint_name() + '_trigproc'
        proc_name = schema, common.caos_name_to_pg_name(proc_name)
        return FunctionExists(proc_name)

    def drop_unique_constraint_trigger(self, source, pointer_name, constraint, meta, context):
        schema = common.caos_module_name_to_schema_name(source.name.module)
        table_name = common.get_table_name(source, catenate=False)

        result = CommandGroup()

        trigger_name = common.caos_name_to_pg_name(constraint.raw_constraint_name() + '_instrigger')
        result.add_command(DropTrigger(trigger_name=trigger_name, table_name=table_name))
        trigger_name = common.caos_name_to_pg_name(constraint.raw_constraint_name() + '_updtrigger')
        result.add_command(DropTrigger(trigger_name=trigger_name, table_name=table_name))

        proc_name = constraint.raw_constraint_name() + '_trigproc'
        proc_name = schema, common.caos_name_to_pg_name(proc_name)
        result.add_command(DropFunction(name=proc_name, args=()))

        return result

    def rename_unique_constraint_trigger(self, orig_source, source, pointer_name,
                                               old_constraint, new_constraint, meta, context):

        result = CommandGroup()

        table_name = common.get_table_name(source, catenate=False)
        orig_table_name = common.get_table_name(orig_source, catenate=False)

        old_trigger_name = common.caos_name_to_pg_name('%s_instrigger' % \
                                                       old_constraint.raw_constraint_name())
        new_trigger_name = common.caos_name_to_pg_name('%s_instrigger' % \
                                                       new_constraint.raw_constraint_name())

        result.add_command(AlterTriggerRenameTo(trigger_name=old_trigger_name,
                                                new_trigger_name=new_trigger_name,
                                                table_name=table_name))

        old_trigger_name = common.caos_name_to_pg_name('%s_updtrigger' % \
                                                       old_constraint.raw_constraint_name())
        new_trigger_name = common.caos_name_to_pg_name('%s_updtrigger' % \
                                                       new_constraint.raw_constraint_name())

        result.add_command(AlterTriggerRenameTo(trigger_name=old_trigger_name,
                                                new_trigger_name=new_trigger_name,
                                                table_name=table_name))

        old_proc_name = common.caos_name_to_pg_name('%s_trigproc' % \
                                                    old_constraint.raw_constraint_name())
        old_proc_name = orig_table_name[0], old_proc_name


        new_proc_name = common.caos_name_to_pg_name('%s_trigproc' % \
                                                    new_constraint.raw_constraint_name())
        new_proc_name = table_name[0], new_proc_name

        result.add_command(RenameFunction(name=old_proc_name, args=(), new_name=new_proc_name))

        return result

    def apply_base_delta(self, orig_source, source, meta, context):
        dropped_bases = set(orig_source.base) - set(source.base)
        added_bases = set(source.base) - set(orig_source.base)

        if isinstance(source, caos.types.ProtoConcept):
            nameconv = common.concept_name_to_table_name
            source_ctx = context.get(delta_cmds.ConceptCommandContext)
            ptr_cmd = delta_cmds.CreateLinkSet
        else:
            nameconv = common.link_name_to_table_name
            source_ctx = context.get(delta_cmds.LinkCommandContext)
            ptr_cmd = delta_cmds.CreateLinkProperty

        alter_table = source_ctx.op.get_alter_table(context)

        if isinstance(source, caos.types.ProtoConcept) or \
                    (source_ctx.op.has_table(orig_source, meta, context) and \
                     source_ctx.op.has_table(source, meta, context)):

            source.acquire_parent_data(meta)
            orig_source.acquire_parent_data(meta)

            created_ptrs = set()
            for ptr in source_ctx.op(ptr_cmd):
                created_ptrs.add(ptr.prototype_name)

            inherited_aptrs = set()

            for base in source.base:
                base = meta.get(base)

                for ptr in base.pointers.values():
                    if ptr.atomic():
                        inherited_aptrs.add(ptr.normal_name())

            added_inh_ptrs = inherited_aptrs - {p.normal_name() for p in orig_source.pointers.values()}

            for added_ptr in added_inh_ptrs - created_ptrs:
                ptr = source.pointers[added_ptr]
                if ptr.atomic():
                    if isinstance(ptr, caos.proto.LinkSet):
                        ptr = ptr.first

                    col_name = common.caos_name_to_pg_name(added_ptr)
                    col_type = types.pg_type_from_atom(meta, ptr.target)
                    col_required = ptr.required
                    col = Column(name=col_name, type=col_type, required=col_required)
                    alter_table.add_operation(AlterTableAddColumn(col))

            if dropped_bases:
                for dropped_base in dropped_bases:
                    parent_table_name = nameconv(caos.name.Name(dropped_base), catenate=False)
                    op = AlterTableDropParent(parent_name=parent_table_name)
                    alter_table.add_operation(op)

                alter_table = source_ctx.op.get_alter_table(context, force_new=True)

                dropped_inh_ptrs = {p.normal_name() for p in orig_source.pointers.values()} - \
                                   {p.normal_name() for p in source.pointers.values()}

                for dropped_ptr in dropped_inh_ptrs:
                    ptr = orig_source.pointers[dropped_ptr]
                    if ptr.atomic():
                        col_name = common.caos_name_to_pg_name(dropped_ptr)
                        col = Column(name=col_name, type="text")
                        alter_table.add_operation(AlterTableDropColumn(col))

            for added_base in added_bases:
                parent_table_name = nameconv(caos.name.Name(added_base), catenate=False)
                op = AlterTableAddParent(parent_name=parent_table_name)
                alter_table.add_operation(op)


class SourceIndexCommand(PrototypeMetaCommand):
    @classmethod
    def get_index_name(cls, host, index):
        index_name = '%s_%s_reg_idx' % (host.name, persistent_hash(index.expr))
        index_name = common.caos_name_to_pg_name(index_name)
        return index_name


class CreateSourceIndex(SourceIndexCommand, adapts=delta_cmds.CreateSourceIndex):
    def apply(self, meta, context=None):
        index = delta_cmds.CreateSourceIndex.apply(self, meta, context)
        SourceIndexCommand.apply(self, meta, context)

        source = context.get(delta_cmds.LinkCommandContext)
        if not source:
            source = context.get(delta_cmds.ConceptCommandContext)
        table_name = common.get_table_name(source.proto, catenate=False)

        expr = caosql_expr.CaosQLExpression(meta).process_concept_expr(index.expr, source.proto)
        sql_tree = transformer.SimpleExprTransformer().transform(expr, True)
        sql_expr = codegen.SQLSourceGenerator.to_source(sql_tree)
        if isinstance(sql_tree, pg_ast.SequenceNode):
            # Trim the parentheses to avoid PostgreSQL choking on double parentheses.
            # since it expects only a single set around the column list.
            #
            sql_expr = sql_expr[1:-1]
        index_name = self.get_index_name(source.proto, index)
        pg_index = Index(name=index_name, table_name=table_name, expr=sql_expr, unique=False)
        self.pgops.add(CreateIndex(pg_index, priority=3))

        return index


class AlterSourceIndex(SourceIndexCommand, adapts=delta_cmds.AlterSourceIndex):
    def apply(self, meta, context=None):
        result = delta_cmds.AlterSourceIndex.apply(self, meta, context)
        SourceIndexCommand.apply(self, meta, context)
        return result


class DeleteSourceIndex(SourceIndexCommand, adapts=delta_cmds.DeleteSourceIndex):
    def apply(self, meta, context=None):
        index = delta_cmds.DeleteSourceIndex.apply(self, meta, context)
        SourceIndexCommand.apply(self, meta, context)

        source = context.get(delta_cmds.LinkCommandContext)
        if not source:
            source = context.get(delta_cmds.ConceptCommandContext)

        if not isinstance(source.op, delta_cmds.DeleteNamedPrototype):
            # We should not drop indexes when the host is being dropped since
            # the indexes are dropped automatically in this case.
            #
            table_name = common.get_table_name(source.proto, catenate=False)
            index_name = self.get_index_name(source.proto, index)
            index_exists = IndexExists((table_name[0], index_name))
            self.pgops.add(DropIndex((table_name[0], index_name), priority=3,
                                     conditions=(index_exists,)))

        return index


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
        self.table_name = new_table_name
        concept_table = Table(name=new_table_name)
        self.create_module(self.prototype_name.module)
        self.pgops.add(CreateTable(table=concept_table))

        alter_table = self.get_alter_table(context)

        concept = delta_cmds.CreateConcept.apply(self, meta, context)
        ConceptMetaCommand.apply(self, meta, context)

        fields = self.create_object(concept)

        if concept.name == 'semantix.caos.builtins.BaseObject':
            col = Column(name='concept_id', type='integer', required=True)
            alter_table.add_operation(AlterTableAddColumn(col))

        constraint = PrimaryKey(table_name=alter_table.name,
                                columns=['semantix.caos.builtins.id'])
        alter_table.add_operation(AlterTableAddConstraint(constraint))

        bases = (common.concept_name_to_table_name(p, catenate=False)
                 for p in fields['base'][1] if proto.Concept.is_prototype(p))
        concept_table.bases = list(bases)

        self.apply_inherited_deltas(concept, meta, context)
        self.create_pointer_constraints(concept, meta, context)

        self.affirm_pointer_defaults(concept, meta, context)

        self.attach_alter_table(context)

        if self.update_search_indexes:
            self.update_search_indexes.apply(meta, context)
            self.pgops.add(self.update_search_indexes)

        return concept


class RenameConcept(ConceptMetaCommand, adapts=delta_cmds.RenameConcept):
    def apply(self, meta, context=None):
        proto = delta_cmds.RenameConcept.apply(self, meta, context)
        ConceptMetaCommand.apply(self, meta, context)

        concept = context.get(delta_cmds.ConceptCommandContext)
        assert concept

        concept.op.attach_alter_table(context)

        self.rename(self.prototype_name, self.new_name)

        concept.op.table_name = common.concept_name_to_table_name(self.new_name, catenate=False)

        # Need to update all bits that reference concept name

        # Constraints
        self.adjust_pointer_constraints(meta, context, proto)

        # Indexes
        self.adjust_indexes(meta, context, proto)

        self.table_name = common.concept_name_to_table_name(self.new_name, catenate=False)

        return proto


class RebaseConcept(ConceptMetaCommand, adapts=delta_cmds.RebaseConcept):
    def apply(self, meta, context):
        result = delta_cmds.RebaseConcept.apply(self, meta, context)
        ConceptMetaCommand.apply(self, meta, context)

        concept_ctx = context.get(delta_cmds.ConceptCommandContext)
        source = concept_ctx.proto
        orig_source = concept_ctx.original_proto
        self.apply_base_delta(orig_source, source, meta, context)

        return result


class AlterConcept(ConceptMetaCommand, adapts=delta_cmds.AlterConcept):
    def apply(self, meta, context=None):
        self.table_name = common.concept_name_to_table_name(self.prototype_name, catenate=False)
        concept = delta_cmds.AlterConcept.apply(self, meta, context=context)
        ConceptMetaCommand.apply(self, meta, context)

        updaterec, updates = self.fill_record()

        if updaterec:
            condition = [('name', str(concept.name))]
            self.pgops.add(Update(table=self.table, record=updaterec, condition=condition))

        self.attach_alter_table(context)

        if self.update_search_indexes:
            self.update_search_indexes.apply(meta, context)
            self.pgops.add(self.update_search_indexes)

        return concept


class DeleteConcept(ConceptMetaCommand, adapts=delta_cmds.DeleteConcept):
    def apply(self, meta, context=None):
        old_table_name = common.concept_name_to_table_name(self.prototype_name, catenate=False)

        concept = delta_cmds.DeleteConcept.apply(self, meta, context)
        ConceptMetaCommand.apply(self, meta, context)

        self.delete(concept, meta, concept)

        self.pgops.add(DropTable(name=old_table_name))
        self.pgops.add(Delete(table=self.table, condition=[('name', str(concept.name))]))

        return concept


class ScheduleLinkMappingUpdate(MetaCommand):
    pass


class CancelLinkMappingUpdate(MetaCommand):
    pass


class PointerMetaCommand(MetaCommand):

    def get_host(self, meta, context):
        if context:
            link = context.get(delta_cmds.LinkCommandContext)
            if link and isinstance(self, (delta_cmds.LinkPropertyCommand, delta_cmds.ComputableCommand)):
                return link
            concept = context.get(delta_cmds.ConceptCommandContext)
            if concept:
                return concept

    def pack_constraints(self, pointer, constraints, abstract=False):
        result = {}
        for constraint in constraints:
            if not pointer.generic() and pointer.atomic() \
                    and isinstance(constraint, proto.PointerConstraintUnique) and not abstract:
                # Unique constraints on atomic links are represented as table constraints
                continue

            cls = constraint.__class__.get_canonical_class()
            key = '%s.%s' % (cls.__module__, cls.__name__)
            result[key] = yaml.Language.dump(constraint.values)

        if not result:
            return None
        else:
            return result

    def record_metadata(self, pointer, old_pointer, meta, context):
        rec, updates = self.fill_record()

        if rec:
            host = self.get_host(meta, context)

            source = updates.get('source')
            if source:
                source = source[1]
            elif host:
                source = host.proto.name

            if source:
                rec.source_id = Query('(SELECT id FROM caos.metaobject WHERE name = $1)',
                                      [str(source)], type='integer')

            target = updates.get('target')
            if target:
                rec.target_id = Query('(SELECT id FROM caos.metaobject WHERE name = $1)',
                                      [str(target[1])],
                                      type='integer')

            if rec.base:
                if isinstance(rec.base, caos.Name):
                    rec.base = str(rec.base)
                else:
                    rec.base = tuple(str(b) for b in rec.base)

        default = list(self(delta_cmds.AlterDefault))
        if default:
            if not rec:
                rec = self.table.record()
            rec.default = self.pack_default(default[0])

        if not old_pointer or old_pointer.constraints != pointer.constraints:
            if not rec:
                rec = self.table.record()

            rec.constraints = self.pack_constraints(pointer, pointer.constraints.values())
            rec.abstract_constraints = self.pack_constraints(pointer,
                                                             pointer.abstract_constraints.values(),
                                                             True)

        return rec, updates

    def alter_host_table_column(self, link, meta, context, old_type, new_type):

        dropped_atom = None

        for op in self(delta_cmds.AtomCommand):
            for rename in op(delta_cmds.RenameAtom):
                if old_type == rename.prototype_name and new_type == rename.new_name:
                    # Our target alter is a mere rename
                    return
            if isinstance(op, delta_cmds.CreateAtom):
                if op.prototype_name == new_type:
                    # CreateAtom will take care of everything for us
                    return
            elif isinstance(op, delta_cmds.DeleteAtom):
                if op.prototype_name == old_type:
                    # The former target atom might as well have been dropped
                    dropped_atom = op.old_prototype

        old_target = meta.get(old_type, dropped_atom)
        assert old_target
        new_target = meta.get(new_type)

        alter_table = context.get(delta_cmds.ConceptCommandContext).op.get_alter_table(context)
        column_name = common.caos_name_to_pg_name(link.normal_name())

        if isinstance(new_target, caos.types.ProtoAtom):
            target_type = types.pg_type_from_atom(meta, new_target)

            if isinstance(old_target, caos.types.ProtoAtom):
                AlterAtom.alter_atom(self, meta, context, old_target, new_target, in_place=False)
                alter_type = AlterTableAlterColumnType(column_name, target_type)
                alter_table.add_operation(alter_type)
            else:
                cols = self.get_columns(link, meta)
                ops = [AlterTableAddColumn(col) for col in cols]
                for op in ops:
                    alter_table.add_operation(op)
        else:
            col = Column(name=column_name, type='text')
            alter_table.add_operation(AlterTableDropColumn(col))

    def get_pointer_default(self, pointer, meta, context):
        default = list(self(delta_cmds.AlterDefault))
        default_value = None

        if default:
            default = default[0]
            if default.new_value:
                ld = list(filter(lambda i: isinstance(i, proto.LiteralDefaultSpec),
                                 default.new_value))
                if ld:
                    default_value = postgresql.string.quote_literal(str(ld[0].value))

        return default_value

    def alter_pointer_default(self, pointer, meta, context):
        default = list(self(delta_cmds.AlterDefault))
        if default:
            default = default[0]

            new_default = None
            have_new_default = True

            if not default.new_value:
                new_default = None
            else:
                ld = list(filter(lambda i: isinstance(i, proto.LiteralDefaultSpec),
                                 default.new_value))
                if ld:
                    new_default = ld[0].value
                else:
                    have_new_default = False

            if have_new_default:
                concept_op = context.get(delta_cmds.ConceptCommandContext).op
                alter_table = concept_op.get_alter_table(context, contained=True, priority=3)
                column_name = common.caos_name_to_pg_name(pointer.normal_name())
                alter_table.add_operation(AlterTableAlterColumnDefault(column_name=column_name,
                                                                       default=new_default))

    def get_columns(self, pointer, meta, default=None):
        columns = []

        if pointer.atomic():
            if not isinstance(pointer.target, proto.Atom):
                pointer.target = meta.get(pointer.target)

            column_type = types.pg_type_from_atom(meta, pointer.target)

            name = pointer.normal_name()
            column_name = common.caos_name_to_pg_name(name)

            columns.append(Column(name=column_name, type=column_type,
                                  required=pointer.required,
                                  default=default))

        return columns

    def rename_pointer(self, pointer, meta, context, old_name, new_name):
        if context:
            old_name = pointer.normalize_name(old_name)
            new_name = pointer.normalize_name(new_name)

            host = self.get_host(meta, context)

            if host and pointer.atomic() and old_name != new_name:
                table_name = common.get_table_name(host.proto, catenate=False)

                old_col_name = common.caos_name_to_pg_name(old_name)
                new_col_name = common.caos_name_to_pg_name(new_name)

                rename = AlterTableRenameColumn(table_name, old_col_name, new_col_name)
                self.pgops.add(rename)

        rec = self.table.record()
        rec.name = str(self.new_name)
        self.pgops.add(Update(table=self.table, record=rec,
                              condition=[('name', str(self.prototype_name))], priority=1))



class LinkMetaCommand(CompositePrototypeMetaCommand, PointerMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = LinkTable()

    def create_table(self, link, meta, context, conditional=False):
        self.table_name = new_table_name = common.link_name_to_table_name(link.name, catenate=False)
        self.create_module(link.name.module)

        constraints = []
        columns = []

        if link.name == 'semantix.caos.builtins.link':
            columns.append(Column(name='source_id', type='uuid', required=True))
            # target_id column is not required, since there may be records for atomic links,
            # and atoms are stored in the source table.
            columns.append(Column(name='target_id', type='uuid', required=False))
            columns.append(Column(name='link_type_id', type='integer', required=True))

        constraints.append(UniqueConstraint(table_name=new_table_name,
                                            columns=['source_id', 'target_id', 'link_type_id']))

        table = Table(name=new_table_name)
        table.add_columns(columns)
        table.constraints = constraints

        if link.base:
            bases = (common.link_name_to_table_name(p, catenate=False)
                     for p in link.base if proto.Concept.is_prototype(p))
            table.bases = list(bases)

        ct = CreateTable(table=table)

        index_name = common.caos_name_to_pg_name(str(link.name)  + 'target_id_default_idx')
        index = Index(index_name, new_table_name, unique=False)
        index.add_columns(['target_id'])
        ci = CreateIndex(index)

        if conditional:
            c = CommandGroup(neg_conditions=[TableExists(new_table_name)])
        else:
            c = CommandGroup()

        c.add_command(ct)
        c.add_command(ci)

        self.pgops.add(c)
        self.table_name = new_table_name

    def has_table(self, link, meta, context):
        if link.generic():
            nonatomic = (l for l in link.children() if not l.generic() and not l.atomic())
            return bool(link.pointers or bool(nonatomic))
        else:
            return False

    def provide_table(self, link, meta, context):
        if self.has_table(link, meta, context):
            self.create_table(link, meta, context, conditional=True)

    def schedule_mapping_update(self, link, meta, context):
        if (not link.atomic() or link.pointers):
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


class CreateLink(LinkMetaCommand, adapts=delta_cmds.CreateLink):
    def apply(self, meta, context=None):
        # Need to do this early, since potential table alters triggered by sub-commands
        # need this.
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
        self.provide_table(link, meta, context)

        if not link.generic() and link.atomic():
            concept = context.get(delta_cmds.ConceptCommandContext)
            assert concept, "Link command must be run in Concept command context"

            default_value = self.get_pointer_default(link, meta, context)

            cols = self.get_columns(link, meta, default_value)
            table_name = common.get_table_name(concept.proto, catenate=False)
            concept_alter_table = concept.op.get_alter_table(context)

            for col in cols:
                # The column may already exist as inherited from parent table
                cond = ColumnExists(table_name=table_name, column_name=col.name)
                cmd = AlterTableAddColumn(col)
                concept_alter_table.add_operation((cmd, None, (cond,)))

        if link.generic():
            self.affirm_pointer_defaults(link, meta, context)

        if self.has_table(link, meta, context):
            self.apply_inherited_deltas(link, meta, context)
            self.create_pointer_constraints(link, meta, context)

        self.attach_alter_table(context)

        rec, updates = self.record_metadata(link, None, meta, context)
        self.pgops.add(Insert(table=self.table, records=[rec], priority=1))

        if not link.generic() and link.mapping != caos.types.ManyToMany:
            self.schedule_mapping_update(link, meta, context)

        return link


class RenameLink(LinkMetaCommand, adapts=delta_cmds.RenameLink):
    def apply(self, meta, context=None):
        result = delta_cmds.RenameLink.apply(self, meta, context)
        LinkMetaCommand.apply(self, meta, context)

        self.rename_pointer(result, meta, context, self.prototype_name, self.new_name)

        self.attach_alter_table(context)

        if result.generic():
            # Indexes
            self.adjust_indexes(meta, context, result)
        else:
            # Constraints
            self.adjust_pointer_constraints(meta, context, result)

        return result


class RebaseLink(LinkMetaCommand, adapts=delta_cmds.RebaseLink):
    def apply(self, meta, context):
        result = delta_cmds.RebaseLink.apply(self, meta, context)
        LinkMetaCommand.apply(self, meta, context)

        link_ctx = context.get(delta_cmds.LinkCommandContext)
        source = link_ctx.proto

        orig_source = link_ctx.original_proto
        self.apply_base_delta(orig_source, source, meta, context)

        return result


class AlterLink(LinkMetaCommand, adapts=delta_cmds.AlterLink):
    def apply(self, meta, context=None):
        self.old_link = old_link = meta.get(self.prototype_name).copy()
        link = delta_cmds.AlterLink.apply(self, meta, context)
        LinkMetaCommand.apply(self, meta, context)

        with context(delta_cmds.LinkCommandContext(self, link)):
            rec, updates = self.record_metadata(link, old_link, meta, context)

            self.provide_table(link, meta, context)

            if rec:
                self.pgops.add(Update(table=self.table, record=rec,
                                      condition=[('name', str(link.name))], priority=1))

            self.attach_alter_table(context)

            new_type = None
            for op in self(delta_cmds.AlterPrototypeProperty):
                if op.property == 'target':
                    new_type = op.new_value
                    old_type = op.old_value
                    break

            if new_type:
                if not isinstance(link.target, caos.types.ProtoObject):
                    link.target = meta.get(link.target)

            if new_type and (isinstance(link.target, caos.types.ProtoAtom) or \
                             isinstance(self.old_link.target, caos.types.ProtoAtom)):
                self.alter_host_table_column(link, meta, context, old_type, new_type)

            if isinstance(link.target, caos.types.ProtoAtom) and \
                    isinstance(self.old_link.target, caos.types.ProtoAtom) and \
                    link.required != self.old_link.required:

                alter_table = context.get(delta_cmds.ConceptCommandContext).op.get_alter_table(context)
                column_name = common.caos_name_to_pg_name(link.normal_name())
                alter_table.add_operation(AlterTableAlterColumnNull(column_name=column_name,
                                                                    null=not link.required))

            if isinstance(link.target, caos.types.ProtoAtom):
                self.alter_pointer_default(link, meta, context)

            if not link.generic() and old_link.mapping != link.mapping:
                self.schedule_mapping_update(link, meta, context)

        return link


class DeleteLink(LinkMetaCommand, adapts=delta_cmds.DeleteLink):
    def apply(self, meta, context=None):
        result = delta_cmds.DeleteLink.apply(self, meta, context)
        LinkMetaCommand.apply(self, meta, context)

        if not result.generic() and result.atomic():
            concept = context.get(delta_cmds.ConceptCommandContext)

            name = result.normal_name()
            column_name = common.caos_name_to_pg_name(name)
            # We don't really care about the type -- we're dropping the thing
            column_type = 'text'

            alter_table = concept.op.get_alter_table(context)
            col = AlterTableDropColumn(Column(name=column_name, type=column_type))
            alter_table.add_operation(col)

        elif result.generic() and \
                            [l for l in result.children() if not l.generic() and not l.atomic()]:
            old_table_name = common.link_name_to_table_name(result.name, catenate=False)
            self.pgops.add(DropTable(name=old_table_name))
            self.cancel_mapping_update(result, meta, context)

        if not result.generic() and result.mapping != caos.types.ManyToMany:
            self.schedule_mapping_update(result, meta, context)

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


class PointerConstraintMetaCommand(PrototypeMetaCommand):
    pass


class CreatePointerConstraint(PointerConstraintMetaCommand,
                              adapts=delta_cmds.CreatePointerConstraint):
    def apply(self, meta, context=None):
        constraint = delta_cmds.CreatePointerConstraint.apply(self, meta, context)
        PointerConstraintMetaCommand.apply(self, meta, context)

        source, pointer = CompositePrototypeMetaCommand.get_source_and_pointer_ctx(meta, context)

        if pointer and not pointer.proto.generic() and pointer.proto.atomic():
            pointer_name = pointer.proto.normal_name()
            constr_key = persistent_hash(constraint)
            ptr_constr = source.op.pointer_constraints.get(pointer_name)

            if (not ptr_constr or constr_key not in ptr_constr) and \
                                            isinstance(constraint, proto.PointerConstraintUnique):

                ptr = source.proto.pointers.get(pointer_name)
                if isinstance(ptr, proto.LinkSet):
                    ptr = ptr.first

                if self.abstract:
                    # Record abstract constraint in parent op so that potential future children
                    # see it in apply_inherited_deltas()
                    #
                    ac = source.op.abstract_pointer_constraints.setdefault(pointer_name, {})
                    ac[constr_key] = constraint

                    # Need to clean up any non-abstract constraints of this type in case
                    # the constraint was altered from non-abstract to abstract.
                    #
                    # XXX: This will go away once AlterConstraint is implemented properly
                    #
                    source.op.del_pointer_constraint(source.proto, pointer_name, constraint,
                                                     meta, context, conditional=True)
                else:
                    source.op.add_pointer_constraint(source.proto, pointer_name, constraint,
                                                     meta, context)


                for child in source.proto.children(recursive=True):
                    if isinstance(child, caos.types.ProtoLink) and not child.generic():
                        continue

                    protoclass = source.proto.__class__.get_canonical_class()
                    cmd = source.op.__class__(prototype_name=child.name, prototype_class=protoclass)

                    with context(source.op.__class__.context_class(cmd, child)):
                        # XXX: This can lead to duplicate constraint errors if the child has
                        # been created in the same sync session.
                        cmd.add_pointer_constraint(child, pointer_name, constraint, meta, context)
                        cmd.attach_alter_table(context)
                        self.pgops.add(cmd)

        return constraint


class DeletePointerConstraint(PointerConstraintMetaCommand, adapts=delta_cmds.DeletePointerConstraint):
    def apply(self, meta, context=None):
        source, pointer = CompositePrototypeMetaCommand.get_source_and_pointer_ctx(meta, context)
        constraint = pointer.proto.constraints.get(self.prototype_class)

        delta_cmds.DeletePointerConstraint.apply(self, meta, context)
        PointerConstraintMetaCommand.apply(self, meta, context)

        if pointer and not pointer.proto.generic() and pointer.proto.atomic():
            pointer_name = pointer.proto.normal_name()
            constr_key = persistent_hash(constraint)
            ptr_constr = source.op.dropped_pointer_constraints.get(pointer_name)

            if (not ptr_constr or constr_key not in ptr_constr) and \
                                            isinstance(constraint, proto.PointerConstraintUnique):

                if not self.abstract:
                    # Abstract constraints apply only to children
                    source.op.del_pointer_constraint(source.proto, pointer_name, constraint, meta,
                                                     context)

                for child in source.proto.children(recursive=True):
                    if isinstance(child, caos.types.ProtoLink) and not child.generic():
                        continue

                    protoclass = source.proto.__class__.get_canonical_class()
                    cmd = source.op.__class__(prototype_name=child.name, prototype_class=protoclass)

                    with context(source.op.__class__.context_class(cmd, child)):
                        cmd.del_pointer_constraint(child, pointer_name, constraint, meta, context)
                        cmd.attach_alter_table(context)
                        self.pgops.add(cmd)

        return constraint


class LinkPropertyMetaCommand(NamedPrototypeMetaCommand, PointerMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = LinkPropertyTable()


class CreateLinkProperty(LinkPropertyMetaCommand, adapts=delta_cmds.CreateLinkProperty):
    def apply(self, meta, context):
        property = delta_cmds.CreateLinkProperty.apply(self, meta, context)
        LinkPropertyMetaCommand.apply(self, meta, context)

        link = context.get(delta_cmds.LinkCommandContext)

        if link and link.proto.generic():
            link.op.provide_table(link.proto, meta, context)
            alter_table = link.op.get_alter_table(context)

            default_value = self.get_pointer_default(property, meta, context)

            cols = self.get_columns(property, meta, default_value)
            for col in cols:
                # The column may already exist as inherited from parent table
                cond = ColumnExists(table_name=alter_table.name, column_name=col.name)
                cmd = AlterTableAddColumn(col)
                alter_table.add_operation((cmd, None, (cond,)))

        with context(delta_cmds.LinkPropertyCommandContext(self, property)):
            rec, updates = self.record_metadata(property, None, meta, context)

        # Priority is set to 2 to make sure that INSERT is run after the host link
        # is INSERTed into caos.link.
        #
        self.pgops.add(Insert(table=self.table, records=[rec], priority=2))

        return property


class RenameLinkProperty(LinkPropertyMetaCommand, adapts=delta_cmds.RenameLinkProperty):
    def apply(self, meta, context=None):
        result = delta_cmds.RenameLinkProperty.apply(self, meta, context)
        LinkPropertyMetaCommand.apply(self, meta, context)

        self.rename_pointer(result, meta, context, self.prototype_name, self.new_name)

        return result


class AlterLinkProperty(LinkPropertyMetaCommand, adapts=delta_cmds.AlterLinkProperty):
    def apply(self, meta, context=None):
        self.old_prop = old_prop = meta.get(self.prototype_name, type=self.prototype_class).copy()
        prop = delta_cmds.AlterLinkProperty.apply(self, meta, context)
        LinkPropertyMetaCommand.apply(self, meta, context)

        with context(delta_cmds.LinkPropertyCommandContext(self, prop)):
            rec, updates = self.record_metadata(prop, old_prop, meta, context)

            if rec:
                self.pgops.add(Update(table=self.table, record=rec,
                                      condition=[('name', str(prop.name))], priority=1))

            new_type = None
            for op in self(delta_cmds.AlterPrototypeProperty):
                if op.property == 'target':
                    new_type = op.new_value
                    old_type = op.old_value
                    break

            if new_type:
                self.alter_host_table_column(prop, meta, context, old_type, new_type)

            self.alter_pointer_default(prop, meta, context)

        return prop


class DeleteLinkProperty(LinkPropertyMetaCommand, adapts=delta_cmds.DeleteLinkProperty):
    def apply(self, meta, context=None):
        property = delta_cmds.DeleteLinkProperty.apply(self, meta, context)
        LinkPropertyMetaCommand.apply(self, meta, context)

        link = context.get(delta_cmds.LinkCommandContext)

        if link:
            alter_table = link.op.get_alter_table(context)

            column_name = common.caos_name_to_pg_name(property.normal_name())
            # We don't really care about the type -- we're dropping the thing
            column_type = 'text'

            col = AlterTableDropColumn(Column(name=column_name, type=column_type))
            alter_table.add_operation(col)

        self.pgops.add(Delete(table=self.table, condition=[('name', str(property.name))]))

        return property


class ComputableMetaCommand(NamedPrototypeMetaCommand, PointerMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = ComputableTable()

    def record_metadata(self, pointer, old_pointer, meta, context):
        rec, updates = self.fill_record()

        if rec:
            host = self.get_host(meta, context)

            source = updates.get('source')
            if source:
                source = source[1]
            elif host:
                source = host.proto.name

            if source:
                rec.source_id = Query('(SELECT id FROM caos.metaobject WHERE name = $1)',
                                      [str(source)], type='integer')

            target = updates.get('target')
            if target:
                rec.target_id = Query('(SELECT id FROM caos.metaobject WHERE name = $1)',
                                      [str(target[1])],
                                      type='integer')

        return rec, updates


class CreateComputable(ComputableMetaCommand, adapts=delta_cmds.CreateComputable):
    def apply(self, meta, context):
        computable = delta_cmds.CreateComputable.apply(self, meta, context)
        ComputableMetaCommand.apply(self, meta, context)

        source = self.get_host(meta, context)

        with context(self.context_class(self, computable)):
            rec, updates = self.record_metadata(computable, None, meta, context)

        self.pgops.add(Insert(table=self.table, records=[rec], priority=2))

        return computable


class RenameComputable(ComputableMetaCommand, adapts=delta_cmds.RenameComputable):
    def apply(self, meta, context=None):
        result = delta_cmds.RenameComputable.apply(self, meta, context)
        ComputableMetaCommand.apply(self, meta, context)

        rec = self.table.record()
        rec.name = str(self.new_name)
        self.pgops.add(Update(table=self.table, record=rec,
                              condition=[('name', str(self.prototype_name))], priority=1))

        return result


class AlterComputable(ComputableMetaCommand, adapts=delta_cmds.AlterComputable):
    def apply(self, meta, context=None):
        self.old_computable = old_computable = meta.get(self.prototype_name, type=self.prototype_class).copy()
        computable = delta_cmds.AlterComputable.apply(self, meta, context)
        ComputableMetaCommand.apply(self, meta, context)

        with context(self.context_class(self, computable)):
            rec, updates = self.record_metadata(computable, old_computable, meta, context)

            if rec:
                self.pgops.add(Update(table=self.table, record=rec,
                                      condition=[('name', str(computable.name))], priority=1))

        return computable


class DeleteComputable(ComputableMetaCommand, adapts=delta_cmds.DeleteComputable):
    def apply(self, meta, context=None):
        computable = delta_cmds.DeleteComputable.apply(self, meta, context)
        ComputableMetaCommand.apply(self, meta, context)

        self.pgops.add(Delete(table=self.table, condition=[('name', str(computable.name))]))

        return computable


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
        links = link_ds.fetch()
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
        self.pgops.add(CreateSchema(name='caos', priority=-3))

        featuretable = FeatureTable()
        self.pgops.add(CreateTable(table=featuretable,
                                   neg_conditions=[TableExists(name=featuretable.name)],
                                   priority=-3))

        backendinfotable = BackendInfoTable()
        self.pgops.add(CreateTable(table=backendinfotable,
                                   neg_conditions=[TableExists(name=backendinfotable.name)],
                                   priority=-3))

        self.pgops.add(EnableFeature(feature=UuidFeature(),
                                     neg_conditions=[FunctionExists(('caos', 'uuid_nil'))],
                                     priority=-2))

        self.pgops.add(EnableFeature(feature=HstoreFeature(),
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

        moduletable = ModuleTable()
        self.pgops.add(CreateTable(table=moduletable,
                                   neg_conditions=[TableExists(name=moduletable.name)],
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

        linkproptable = LinkPropertyTable()
        self.pgops.add(CreateTable(table=linkproptable,
                                   neg_conditions=[TableExists(name=linkproptable.name)],
                                   priority=-1))

        computabletable = ComputableTable()
        self.pgops.add(CreateTable(table=computabletable,
                                   neg_conditions=[TableExists(name=computabletable.name)],
                                   priority=-1))

        entity_modstat_type = EntityModStatType()
        self.pgops.add(CreateCompositeType(type=entity_modstat_type,
                                neg_conditions=[CompositeTypeExists(name=entity_modstat_type.name)],
                                priority=-1))


        self.update_mapping_indexes = UpdateMappingIndexes()

        delta_cmds.AlterRealm.apply(self, meta, context)
        MetaCommand.apply(self, meta)

        self.update_mapping_indexes.apply(meta, context)
        self.pgops.append(self.update_mapping_indexes)

        self.pgops.append(UpgradeBackend.update_backend_info())

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


class UpgradeBackend(MetaCommand):
    def __init__(self, backend_info, **kwargs):
        super().__init__(**kwargs)

        self.actual_version = backend_info['format_version']
        self.current_version = BACKEND_FORMAT_VERSION

    def execute(self, context):
        for version in range(self.actual_version, self.current_version):
            getattr(self, 'update_to_version_%d' % (version + 1))(context)
        op = self.update_backend_info()
        op.execute(context)

    def update_to_version_1(self, context):
        featuretable = FeatureTable()
        ct = CreateTable(table=featuretable,
                         neg_conditions=[TableExists(name=featuretable.name)])
        ct.execute(context)

        backendinfotable = BackendInfoTable()
        ct = CreateTable(table=backendinfotable,
                         neg_conditions=[TableExists(name=backendinfotable.name)])
        ct.execute(context)

        # Version 0 did not have feature registry, fix that up
        for feature in (UuidFeature, HstoreFeature):
            cmd = EnableFeature(feature=feature())
            ins = cmd.extra(context)[0]
            ins.execute(context)

    @classmethod
    def update_backend_info(cls):
        backendinfotable = BackendInfoTable()
        record = backendinfotable.record()
        record.format_version = BACKEND_FORMAT_VERSION
        return Merge(table=backendinfotable, record=record, condition=None)


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
    def __init__(self, table, records, returning=None, *, conditions=None, neg_conditions=None,
                                                                           priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.table = table
        self.records = records
        self.returning = returning

    def code(self, context):
        cols = [(c.name, c.type) for c in self.table.columns(writable_only=True)]
        l = len(cols)

        vals = []
        placeholders = []
        i = 1
        for row in self.records:
            placeholder_row = []
            for col, coltype in cols:
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
                    placeholder_row.append('$%d::%s' % (i, coltype))
                    i += 1
            placeholders.append('(%s)' % ','.join(placeholder_row))

        code = 'INSERT INTO %s (%s) VALUES %s' % \
                (common.qname(*self.table.name),
                 ','.join(common.quote_ident(c[0]) for c in cols),
                 ','.join(placeholders))

        if self.returning:
            code += ' RETURNING ' + ', '.join(self.returning)

        return (code, vals)

    def __repr__(self):
        vals = (('(%s)' % ', '.join('%s=%r' % (col, v) for col, v in row.items())) for row in self.records)
        return '<caos.sync.%s %s (%s)>' % (self.__class__.__name__, self.table.name, ', '.join(vals))


class Update(DMLOperation):
    def __init__(self, table, record, condition, returning=None, *, priority=0):
        super().__init__(priority=priority)

        self.table = table
        self.record = record
        self.fields = [f for f, v in record.items() if v is not Default]
        self.condition = condition
        self.returning = returning
        self.cols = {c.name: c.type for c in self.table.columns(writable_only=True)}


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
                expr = '$%d::%s' % (i, self.cols[f])
                i += 1
                vals.append(val)

            placeholders.append('%s = %s' % (e(f), expr))

        if self.condition:
            cond = []
            for field, value in self.condition:
                field = e(field)

                if value is None:
                    cond.append('%s IS NULL' % field)
                else:
                    cond.append('%s = $%d' % (field, i))
                    vals.append(value)
                    i += 1

            where = 'WHERE ' +  ' AND '.join(cond)
        else:
            where = ''

        code = 'UPDATE %s SET %s %s' % \
                (common.qname(*self.table.name), ', '.join(placeholders), where)

        if self.returning:
            code += ' RETURNING ' + ', '.join(self.returning)

        return (code, vals)

    def __repr__(self):
        expr = ','.join('%s=%s' % (f, getattr(self.record, f)) for f in self.fields)
        where = ','.join('%s=%s' % (c[0], c[1]) for c in self.condition) if self.condition else ''
        return '<caos.sync.%s %s %s (%s)>' % (self.__class__.__name__, self.table.name, expr, where)


class Merge(Update):
    def code(self, context):
        code = super().code(context)
        if self.condition:
            cols = (common.quote_ident(c[0]) for c in self.condition)
            returning = ','.join(cols)
        else:
            returning = '*'

        code = (code[0] + ' RETURNING %s' % returning, code[1])
        return code

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


class TableConstraint(DBObject):
    def __init__(self, table_name, column_name=None):
        self.table_name = table_name
        self.column_name = column_name

    def constraint_name(self):
        raise NotImplementedError

    def code(self, context):
        return None

    def rename_code(self, context):
        return None

    def extra(self, context, alter_table):
        return None

    def rename_extra(self, context, new_name):
        return None


class PrimaryKey(TableConstraint):
    def __init__(self, table_name, columns):
        super().__init__(table_name)
        self.columns = columns

    def code(self, context):
        code = 'PRIMARY KEY (%s)' % ', '.join(common.quote_ident(c) for c in self.columns)
        return code


class UniqueConstraint(TableConstraint):
    def __init__(self, table_name, columns):
        super().__init__(table_name)
        self.columns = columns

    def code(self, context):
        code = 'UNIQUE (%s)' % ', '.join(common.quote_ident(c) for c in self.columns)
        return code


class TableClassConstraint(TableConstraint):
    def __init__(self, table_name, column_name, prefix, constrobj):
        super().__init__(table_name, column_name)
        self.prefix = prefix if isinstance(prefix, tuple) else (prefix,)
        self.constrobj = constrobj

    def code(self, context):
        return 'CONSTRAINT %s %s' % (self.constraint_name(),
                                     self.constraint_code(context, self.column_name))

    def extra(self, context, alter_table):
        text = self.raw_constraint_name()
        cmd = Comment(object=self, text=text)
        return [cmd]

    def raw_constraint_name(self):
        cls = self.constrobj.__class__.get_canonical_class()
        name = '%s::%s.%s::%s' % (':'.join(str(p) for p in self.prefix),
                                  cls.__module__, cls.__name__, self.suffix)
        return name

    def constraint_name(self):
        name = self.raw_constraint_name()
        name = common.caos_name_to_pg_name(name)
        return common.quote_ident(name)

    def rename_code(self, context, new_constraint):
        return '''UPDATE
                        pg_catalog.pg_constraint AS con
                    SET
                        conname = $1
                    FROM
                        pg_catalog.pg_class AS c,
                        pg_catalog.pg_namespace AS ns
                    WHERE
                        con.conrelid = c.oid
                        AND c.relnamespace = ns.oid
                        AND ns.nspname = $3
                        AND c.relname = $4
                        AND con.conname = $2
               ''', [common.caos_name_to_pg_name(new_constraint.raw_constraint_name()),
                     common.caos_name_to_pg_name(self.raw_constraint_name()),
                     new_constraint.table_name[0], new_constraint.table_name[1]]

    def rename_extra(self, context, new_constraint):
        new_name = new_constraint.raw_constraint_name()
        cmd = Comment(object=new_constraint, text=new_name)
        return [cmd]

    def __repr__(self):
        return '<%s.%s "%s" "%r">' % (self.__class__.__module__, self.__class__.__name__,
                                      self.column_name, self.constrobj)


class AtomConstraintTableConstraint(TableClassConstraint):
    def __init__(self, table_name, column_name, prefix, constraint):
        super().__init__(table_name, column_name, prefix, constraint)
        self.constraint = constraint
        self.suffix = 'atom_constr'

    def constraint_code(self, context, value_holder='VALUE'):
        ql = postgresql.string.quote_literal
        value_holder = common.quote_ident(value_holder)

        if isinstance(self.constraint, proto.AtomConstraintRegExp):
            expr = ['%s ~ %s' % (value_holder, ql(re)) for re in self.constraint.values]
            expr = ' AND '.join(expr)
        elif isinstance(self.constraint, proto.AtomConstraintMaxLength):
            expr = 'length(%s::text) <= %s' % (value_holder, str(self.constraint.value))
        elif isinstance(self.constraint, proto.AtomConstraintMinLength):
            expr = 'length(%s::text) >= %s' % (value_holder, str(self.constraint.value))
        elif isinstance(self.constraint, proto.AtomConstraintMaxValue):
            expr = '%s <= %s' % (value_holder, ql(str(self.constraint.value)))
        elif isinstance(self.constraint, proto.AtomConstraintMaxExValue):
            expr = '%s < %s' % (value_holder, ql(str(self.constraint.value)))
        elif isinstance(self.constraint, proto.AtomConstraintMinValue):
            expr = '%s >= %s' % (value_holder, ql(str(self.constraint.value)))
        elif isinstance(self.constraint, proto.AtomConstraintMinExValue):
            expr = '%s > %s' % (value_holder, ql(str(self.constraint.value)))
        else:
            assert False, 'unexpected constraint type: "%r"' % self.constraint

        return 'CHECK (%s)' % expr


class PointerConstraintTableConstraint(TableClassConstraint):
    def __init__(self, table_name, column_name, prefix, constraint):
        super().__init__(table_name, column_name, prefix, constraint)
        self.constraint = constraint
        self.suffix = 'ptr_constr'

    def constraint_code(self, context, value_holder='VALUE'):
        ql = postgresql.string.quote_literal
        value_holder = common.quote_ident(value_holder)

        if isinstance(self.constraint, proto.PointerConstraintUnique):
            expr = 'UNIQUE (%s)' % common.quote_ident(self.column_name)
        else:
            assert False, 'unexpected constraint type: "%r"' % self.constr

        return expr


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
    def __init__(self, name, table_name, unique=True, expr=None):
        super().__init__()

        self.name = name
        self.table_name = table_name
        self.__columns = datastructures.OrderedSet()
        self.predicate = None
        self.unique = unique
        self.expr = expr

    def add_columns(self, columns):
        self.__columns.update(columns)

    def creation_code(self, context):
        if self.expr:
            expr = self.expr
        else:
            expr = ', '.join(self.columns)

        code = 'CREATE %(unique)s INDEX %(name)s ON %(table)s (%(expr)s) %(predicate)s' % \
                {'unique': 'UNIQUE' if self.unique else '',
                 'name': common.qname(self.name),
                 'table': common.qname(*self.table_name),
                 'expr': expr,
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
        id_str = '_'.join(str(i) for i in ids)

        name = '%s_%s_%s_link_mapping_idx' % (self.name_prefix, id_str, self.mapping)
        name = common.caos_name_to_pg_name(name)
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


class CompositeDBObject(DBObject):
    def __init__(self, name):
        super().__init__()
        self.name = name

    @property
    def record(self):
        return datastructures.Record(self.__class__.__name__ + '_record',
                                     [c.name for c in self._columns],
                                     default=Default)


class Table(CompositeDBObject):
    def __init__(self, name):
        super().__init__(name)

        self.__columns = datastructures.OrderedSet()
        self._columns = []

        self.constraints = set()
        self.bases = set()
        self.data = []

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
        self._columns = self.columns()


class DeltaRefTable(Table):
    def __init__(self, name=None):
        name = name or ('caos', 'deltaref')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            Column(name='id', type='varchar', required=True),
            Column(name='ref', type='text', required=True)
        ])

        self.constraints = set([
            PrimaryKey(name, columns=('ref',))
        ])

        self._columns = self.columns()


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
            PrimaryKey(name, columns=('id',))
        ])

        self._columns = self.columns()


class ModuleTable(Table):
    def __init__(self, name=None):
        name = name or ('caos', 'module')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            Column(name='name', type='text', required=True),
            Column(name='schema_name', type='text', required=True)
        ])

        self.constraints = set([
            PrimaryKey(name, columns=('name',)),
        ])

        self._columns = self.columns()


class MetaObjectTable(Table):
    def __init__(self, name=None):
        name = name or ('caos', 'metaobject')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            Column(name='id', type='serial', required=True, readonly=True),
            Column(name='name', type='text', required=True),
            Column(name='is_abstract', type='boolean', required=True, default=False),
            Column(name='is_final', type='boolean', required=True, default=False),
            Column(name='title', type='caos.hstore'),
            Column(name='description', type='text')
        ])

        self.constraints = set([
            PrimaryKey(name, columns=('id',)),
            UniqueConstraint(name, columns=('name',))
        ])

        self._columns = self.columns()


class AtomTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('caos', 'atom'))

        self.bases = [('caos', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            Column(name='automatic', type='boolean', required=True, default=False),
            Column(name='base', type='text', required=True),
            Column(name='constraints', type='caos.hstore'),
            Column(name='default', type='text'),
            Column(name='attributes', type='caos.hstore')
        ])

        self.constraints = set([
            PrimaryKey(('caos', 'atom'), columns=('id',)),
            UniqueConstraint(('caos', 'atom'), columns=('name',))
        ])

        self._columns = self.columns()


class ConceptTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('caos', 'concept'))

        self.bases = [('caos', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            Column(name='custombases', type='text[]'),
        ])

        self.constraints = set([
            PrimaryKey(('caos', 'concept'), columns=('id',)),
            UniqueConstraint(('caos', 'concept'), columns=('name',))
        ])

        self._columns = self.columns()


class LinkTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('caos', 'link'))

        self.bases = [('caos', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            Column(name='source_id', type='integer'),
            Column(name='target_id', type='integer'),
            Column(name='mapping', type='char(2)', required=True),
            Column(name='required', type='boolean', required=True, default=False),
            Column(name='is_atom', type='boolean'),
            Column(name='readonly', type='boolean', required=True, default=False),
            Column(name='loading', type='text'),
            Column(name='base', type='text[]'),
            Column(name='default', type='text'),
            Column(name='constraints', type='caos.hstore'),
            Column(name='abstract_constraints', type='caos.hstore')
        ])

        self.constraints = set([
            PrimaryKey(('caos', 'link'), columns=('id',)),
            UniqueConstraint(('caos', 'link'), columns=('name',))
        ])

        self._columns = self.columns()


class LinkPropertyTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('caos', 'link_property'))

        self.bases = [('caos', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            Column(name='source_id', type='integer'),
            Column(name='target_id', type='integer'),
            Column(name='required', type='boolean', required=True, default=False),
            Column(name='readonly', type='boolean', required=True, default=False),
            Column(name='loading', type='text'),
            Column(name='base', type='text[]'),
            Column(name='default', type='text'),
            Column(name='constraints', type='caos.hstore'),
            Column(name='abstract_constraints', type='caos.hstore')
        ])

        self.constraints = set([
            PrimaryKey(('caos', 'link_property'), columns=('id',)),
            UniqueConstraint(('caos', 'link_property'), columns=('name',))
        ])

        self._columns = self.columns()


class ComputableTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('caos', 'computable'))

        self.bases = [('caos', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            Column(name='source_id', type='integer'),
            Column(name='target_id', type='integer'),
            Column(name='expression', type='text'),
            Column(name='is_local', type='bool')
        ])

        self.constraints = set([
            PrimaryKey(('caos', 'link'), columns=('id',)),
            UniqueConstraint(('caos', 'link'), columns=('name',))
        ])

        self._columns = self.columns()


class FeatureTable(Table):
    def __init__(self, name=None):
        name = name or ('caos', 'feature')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            Column(name='name', type='text', required=True),
            Column(name='class_name', type='text', required=True)
        ])

        self.constraints = set([
            PrimaryKey(name, columns=('name',)),
        ])

        self._columns = self.columns()


class BackendInfoTable(Table):
    def __init__(self, name=None):
        name = name or ('caos', 'backend_info')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            Column(name='format_version', type='int', required=True),
        ])

        self._columns = self.columns()


class CompositeType(CompositeDBObject):
    def columns(self):
        return self.__columns


class EntityModStatType(CompositeType):
    def __init__(self):
        super().__init__(name=('caos', 'entity_modstat_rec_t'))

        self._columns = datastructures.OrderedSet([
            Column(name='semantix.caos.builtins.id', type='uuid'),
            Column(name='semantix.caos.builtins.mtime', type='timestamptz'),
        ])


class Feature:
    def __init__(self, name, schema='caos'):
        self.name = name
        self.schema = schema

    def code(self, context):
        source = self.get_source(context)

        with open(source, 'r') as f:
            code = re.sub(r'SET\s+search_path\s*=\s*[^;]+;',
                          'SET search_path = %s;' % common.quote_ident(self.schema),
                          f.read())
        return code

    def get_source(self, context):
        pgpath = Config.pg_install_path
        source = self.source % {'pgpath': pgpath}
        source = source % {'version': '%s.%s' % context.db.version_info[:2]}
        return source

    @classmethod
    def init_feature(cls, db):
        pass


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

    @classmethod
    def init_feature(cls, db):
        try:
            db.typio.identify(contrib_hstore='caos.hstore')
        except postgresql.exceptions.SchemaNameError:
            pass


class EnableFeature(DDLOperation):
    def __init__(self, feature, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.feature = feature
        self.opid = feature.name

    def code(self, context):
        return self.feature.code(context)

    def extra(self, context, *args, **kwargs):
        table = FeatureTable()
        record = table.record()
        record.name = self.feature.name
        record.class_name = '%s.%s' % (self.feature.__class__.__module__,
                                       self.feature.__class__.__name__)
        return [Insert(table, records=[record])]

    def execute(self, context):
        super().execute(context)
        self.feature.init_feature(context.db)

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.feature.name)


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


class CreateSequence(SchemaObjectOperation):
    def __init__(self, name):
        super().__init__(name)

    def code(self, context):
        return 'CREATE SEQUENCE %s' % common.qname(*self.name)


class RenameSequence(CommandGroup):
    def __init__(self, name, new_name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.name = name
        self.new_name = new_name

        if name[0] != new_name[0]:
            cmd = AlterSequenceSetSchema(name, new_name[0])
            self.add_command(cmd)
            name = (new_name[0], name[1])

        if name[1] != new_name[1]:
            cmd = AlterSequenceRenameTo(name, new_name[1])
            self.add_command(cmd)

    def __repr__(self):
        return '<%s.%s "%s.%s" to "%s.%s">' % (self.__class__.__module__, self.__class__.__name__,
                                               self.name[0], self.name[1], self.new_name[0],
                                               self.new_name[1])


class AlterSequenceSetSchema(DDLOperation):
    def __init__(self, name, new_schema, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name
        self.new_schema = new_schema

    def code(self, context):
        code = 'ALTER SEQUENCE %s SET SCHEMA %s' % \
                (common.qname(*self.name),
                 common.quote_ident(self.new_schema))
        return code

    def __repr__(self):
        return '<%s.%s "%s.%s" to "%s">' % (self.__class__.__module__, self.__class__.__name__,
                                               self.name[0], self.name[1], self.new_schema)


class AlterSequenceRenameTo(DDLOperation):
    def __init__(self, name, new_name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name
        self.new_name = new_name

    def code(self, context):
        code = 'ALTER SEQUENCE %s RENAME TO %s' % \
                (common.qname(*self.name),
                 common.quote_ident(self.new_name))
        return code

    def __repr__(self):
        return '<%s.%s "%s.%s" to "%s">' % (self.__class__.__module__, self.__class__.__name__,
                                               self.name[0], self.name[1], self.new_name)


class DropSequence(SchemaObjectOperation):
    def __init__(self, name):
        super().__init__(name)

    def code(self, context):
        return 'DROP SEQUENCE %s' % common.qname(*self.name)


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
        if isinstance(constraint, proto.AtomConstraintRegExp):
            expr = ['VALUE ~ %s' % postgresql.string.quote_literal(re) for re in constraint.values]
            expr = ' AND '.join(expr)
        elif isinstance(constraint, proto.AtomConstraintMaxLength):
            expr = 'length(VALUE::text) <= ' + str(constraint.value)
        elif isinstance(constraint, proto.AtomConstraintMinLength):
            expr = 'length(VALUE::text) >= ' + str(constraint.value)
        elif isinstance(constraint, proto.AtomConstraintMaxValue):
            expr = 'VALUE <= ' + postgresql.string.quote_literal(str(constraint.value))
        elif isinstance(constraint, proto.AtomConstraintMaxExValue):
            expr = 'VALUE < ' + postgresql.string.quote_literal(str(constraint.value))
        elif isinstance(constraint, proto.AtomConstraintMinValue):
            expr = 'VALUE >= ' + postgresql.string.quote_literal(str(constraint.value))
        elif isinstance(constraint, proto.AtomConstraintMinExValue):
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
    def __init__(self, table, temporary=False, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(table.name, conditions=conditions, neg_conditions=neg_conditions,
                         priority=priority)
        self.table = table
        self.temporary = temporary

    def code(self, context):
        elems = [c.code(context) for c in self.table.columns(only_self=True)]
        elems += [c.code(context) for c in self.table.constraints]

        name = common.qname(*self.table.name)
        cols = ', '.join(c for c in elems)
        temp = 'TEMPORARY ' if self.temporary else ''

        code = 'CREATE %sTABLE %s (%s)' % (temp, name, cols)

        if self.table.bases:
            code += ' INHERITS (' + ','.join(common.qname(*b) for b in self.table.bases) + ')'

        return code


class DropTable(SchemaObjectOperation):
    def code(self, context):
        return 'DROP TABLE %s' % common.qname(*self.name)


class AlterTableBase(DDLOperation):
    def __init__(self, name, contained=False, **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.contained = contained

    def code(self, context):
        return 'ALTER TABLE %s%s' % ('ONLY ' if self.contained else '', common.qname(*self.name))

    def __repr__(self):
        return '<%s.%s %s>' % (self.__class__.__module__, self.__class__.__name__, self.name)


class AlterTableFragment(DDLOperation):
    pass


class AlterTable(AlterTableBase):
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
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
            if ops:
                return code + ' ' + ', '.join(ops)
        return False

    def extra(self, context):
        extra = []
        for op in self.ops:
            if isinstance(op, tuple):
                op = op[0]
            op_extra = op.extra(context, self)
            if op_extra:
                extra.extend(op_extra)

        return extra

    def dump(self):
        result = [repr(self)]

        for op in self.ops:
            if isinstance(op, tuple):
                op = op[0]
            result.extend('  %s' % l for l in op.dump().split('\n'))

        return '\n'.join(result)

    def __iter__(self):
        return iter(self.ops)

    def __call__(self, typ):
        return filter(lambda i: isinstance(i, typ), self.ops)


class CompositeTypeExists(Condition):
    def __init__(self, name):
        self.name = name

    def code(self, context):
        code = '''SELECT
                        typname
                    FROM
                        pg_catalog.pg_type typ
                        INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = typ.typnamespace
                    WHERE
                        nsp.nspname = $1 AND typ.typname = $2'''
        return code, self.name


class CreateCompositeType(SchemaObjectOperation):
    def __init__(self, type, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(type.name, conditions=conditions, neg_conditions=neg_conditions,
                         priority=priority)
        self.type = type

    def code(self, context):
        elems = [c.code(context) for c in self.type._columns]

        name = common.qname(*self.type.name)
        cols = ', '.join(c for c in elems)

        code = 'CREATE TYPE %s AS (%s)' % (name, cols)

        return code


class DropCompositeType(SchemaObjectOperation):
    def code(self, context):
        return 'DROP TYPE %s' % common.qname(*self.name)


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


class RenameIndex(DDLOperation):
    def __init__(self, old_name, new_name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.old_name = old_name
        self.new_name = new_name

    def code(self, context):
        code = 'ALTER INDEX %s RENAME TO %s' % (common.qname(*self.old_name),
                                                common.quote_ident(self.new_name))
        return code

    def __repr__(self):
        return '<%s.%s "%s" to "%s">' % (self.__class__.__module__, self.__class__.__name__,
                                         common.qname(*self.old_name),
                                         common.quote_ident(self.new_name))


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


class AlterTableAddParent(AlterTableFragment):
    def __init__(self, parent_name):
        self.parent_name = parent_name

    def code(self, context):
        return 'INHERIT %s' % common.qname(*self.parent_name)

    def __repr__(self):
        return '<%s.%s %s>' % (self.__class__.__module__, self.__class__.__name__, self.parent_name)


class AlterTableDropParent(AlterTableFragment):
    def __init__(self, parent_name):
        self.parent_name = parent_name

    def code(self, context):
        return 'NO INHERIT %s' % common.qname(*self.parent_name)

    def __repr__(self):
        return '<%s.%s %s>' % (self.__class__.__module__, self.__class__.__name__, self.parent_name)


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


class AlterTableAlterColumnNull(AlterTableFragment):
    def __init__(self, column_name, null):
        self.column_name = column_name
        self.null = null

    def code(self, context):
        return 'ALTER COLUMN %s %s NOT NULL' % \
                (common.quote_ident(str(self.column_name)), 'DROP' if self.null else 'SET')

    def __repr__(self):
        return '<%s.%s "%s" %s NOT NULL>' % (self.__class__.__module__, self.__class__.__name__,
                                             self.column_name, 'DROP' if self.null else 'SET')


class AlterTableAlterColumnDefault(AlterTableFragment):
    def __init__(self, column_name, default):
        self.column_name = column_name
        self.default = default

    def code(self, context):
        if self.default is None:
            return 'ALTER COLUMN %s DROP DEFAULT' % (common.quote_ident(str(self.column_name)),)
        else:
            return 'ALTER COLUMN %s SET DEFAULT %s' % \
                    (common.quote_ident(str(self.column_name)),
                     postgresql.string.quote_literal(str(self.default)))

    def __repr__(self):
        return '<%s.%s "%s" %s DEFAULT%s>' % (self.__class__.__module__, self.__class__.__name__,
                                              self.column_name,
                                              'DROP' if self.default is None else 'SET',
                                              '' if self.default is None else ' %r' % self.default)


class TableConstraintCommand:
    pass


class AlterTableAddConstraint(AlterTableFragment, TableConstraintCommand):
    def __init__(self, constraint):
        self.constraint = constraint

    def code(self, context):
        return 'ADD  ' + self.constraint.code(context)

    def extra(self, context, alter_table):
        return self.constraint.extra(context, alter_table)

    def __repr__(self):
        return '<%s.%s %r>' % (self.__class__.__module__, self.__class__.__name__,
                               self.constraint)


class AlterTableRenameConstraint(AlterTableBase, TableConstraintCommand):
    def __init__(self, table_name, constraint, new_constraint):
        super().__init__(table_name)
        self.constraint = constraint
        self.new_constraint = new_constraint

    def code(self, context):
        return self.constraint.rename_code(context, self.new_constraint)

    def extra(self, context):
        return self.constraint.rename_extra(context, self.new_constraint)

    def __repr__(self):
        return '<%s.%s %r to %r>' % (self.__class__.__module__, self.__class__.__name__,
                                       self.constraint, self.new_constraint)


class AlterTableDropConstraint(AlterTableFragment, TableConstraintCommand):
    def __init__(self, constraint):
        self.constraint = constraint

    def code(self, context):
        return 'DROP CONSTRAINT ' + self.constraint.constraint_name()

    def __repr__(self):
        return '<%s.%s %r>' % (self.__class__.__module__, self.__class__.__name__,
                               self.constraint)


class AlterTableSetSchema(AlterTableBase):
    def __init__(self, name, schema):
        super().__init__(name)
        self.schema = schema

    def code(self, context):
        code = super().code(context)
        code += ' SET SCHEMA %s ' % common.quote_ident(self.schema)
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


class CreateFunction(DDLOperation):
    def __init__(self, name, args, returns, text, language='plpgsql', volatility='volatile',
                                                                      **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.args = args
        self.returns = returns
        self.text = text
        self.volatility = volatility
        self.language = language

    def code(self, context):
        code = '''CREATE FUNCTION %(name)s(%(args)s)
                  RETURNS %(return)s
                  LANGUAGE %(lang)s
                  %(volatility)s
                  AS $____funcbody____$
                      %(text)s
                  $____funcbody____$;
               ''' % {
                   'name': common.qname(*self.name),
                   'args': ', '.join(common.quote_ident(a) for a in self.args),
                   'return': common.quote_ident(self.returns),
                   'lang': self.language,
                   'volatility': self.volatility,
                   'text': self.text
               }
        return code


class CreateTriggerFunction(CreateFunction):
    def __init__(self, name, text, language='plpgsql', volatility='volatile', **kwargs):
        super().__init__(name, args=(), returns='trigger', text=text, language=language,
                         volatility=volatility, **kwargs)


class RenameFunction(CommandGroup):
    def __init__(self, name, args, new_name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        if name[0] != new_name[0]:
            cmd = AlterFunctionSetSchema(name, args, new_name[0])
            self.add_command(cmd)
            name = (new_name[0], name[1])

        if name[1] != new_name[1]:
            cmd = AlterFunctionRenameTo(name, args, new_name[1])
            self.add_command(cmd)


class AlterFunctionSetSchema(DDLOperation):
    def __init__(self, name, args, new_schema, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name
        self.args = args
        self.new_schema = new_schema

    def code(self, context):
        code = 'ALTER FUNCTION %s(%s) SET SCHEMA %s' % \
                (common.qname(*self.name),
                 ', '.join(common.quote_ident(a) for a in self.args),
                 common.quote_ident(self.new_schema))
        return code


class AlterFunctionRenameTo(DDLOperation):
    def __init__(self, name, args, new_name, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name
        self.args = args
        self.new_name = new_name

    def code(self, context):
        code = 'ALTER FUNCTION %s(%s) RENAME TO %s' % \
                (common.qname(*self.name),
                 ', '.join(common.quote_ident(a) for a in self.args),
                 common.quote_ident(self.new_name))
        return code


class DropFunction(DDLOperation):
    def __init__(self, name, args, *, conditions=None, neg_conditions=None, priority=0):
        self.conditional = False
        if conditions:
            c = []
            for cond in conditions:
                if isinstance(cond, FunctionExists) and cond.name == name and cond.args == args:
                    self.conditional = True
                else:
                    c.append(cond)
            conditions = c
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)
        self.name = name
        self.args = args

    def code(self, context):
        code = 'DROP FUNCTION%s %s(%s)' % \
                (' IF EXISTS' if self.conditional else '',
                 common.qname(*self.name),
                 ', '.join(common.quote_ident(a) for a in self.args))
        return code


class FunctionExists(Condition):
    def __init__(self, name, args=None):
        self.name = name
        self.args = args

    def code(self, context):
        code = '''SELECT
                        p.proname
                    FROM
                        pg_catalog.pg_proc p
                        INNER JOIN pg_catalog.pg_namespace ns ON (ns.oid = p.pronamespace)
                    WHERE
                        p.proname = $2 AND ns.nspname = $1
                        AND ($3::text[] IS NULL
                             OR $3::text[] = ARRAY(SELECT
                                                      format_type(t, NULL)::text
                                                    FROM
                                                      unnest(p.proargtypes) t))
                '''

        return code, self.name + (self.args,)


class CreateTrigger(DDLOperation):
    def __init__(self, trigger_name, *, table_name, events, timing='after', granularity='row',
                                                            procedure, condition=None, **kwargs):
        super().__init__(**kwargs)

        self.trigger_name = trigger_name
        self.table_name = table_name
        self.events = events
        self.timing = timing
        self.granularity = granularity
        self.procedure = procedure
        self.condition = condition

    def code(self, context):
        return '''CREATE TRIGGER %(trigger_name)s %(timing)s %(events)s ON %(table_name)s
                  FOR EACH %(granularity)s %(condition)s EXECUTE PROCEDURE %(procedure)s
               ''' % {
                      'trigger_name': common.quote_ident(self.trigger_name),
                      'timing': self.timing,
                      'events': ' OR '.join(self.events),
                      'table_name': common.qname(*self.table_name),
                      'granularity': self.granularity,
                      'condition': ('WHEN (%s)' % self.condition) if self.condition else '',
                      'procedure': '%s()' % common.qname(*self.procedure)
                     }


class CreateConstraintTrigger(CreateTrigger):
    def __init__(self, trigger_name, *, table_name, events, procedure, condition=None,
                                        conditions=None, neg_conditions=None, priority=0):

        super().__init__(trigger_name=trigger_name, table_name=table_name, events=events,
                         procedure=procedure, condition=condition,
                         conditions=conditions, neg_conditions=neg_conditions, priority=priority)

    def code(self, context):
        return '''CREATE CONSTRAINT TRIGGER %(trigger_name)s %(timing)s %(events)s
                  ON %(table_name)s
                  FOR EACH %(granularity)s %(condition)s EXECUTE PROCEDURE %(procedure)s
               ''' % {
                      'trigger_name': common.quote_ident(self.trigger_name),
                      'timing': self.timing,
                      'events': ' OR '.join(self.events),
                      'table_name': common.qname(*self.table_name),
                      'granularity': self.granularity,
                      'condition': ('WHEN (%s)' % self.condition) if self.condition else '',
                      'procedure': '%s()' % common.qname(*self.procedure)
                     }


class AlterTriggerRenameTo(DDLOperation):
    def __init__(self, *, trigger_name, new_trigger_name, table_name, **kwargs):
        super().__init__(**kwargs)

        self.trigger_name = trigger_name
        self.new_trigger_name = new_trigger_name
        self.table_name = table_name

    def code(self, context):
        return 'ALTER TRIGGER %s ON %s RENAME TO %s' % \
                (common.quote_ident(self.trigger_name), common.qname(*self.table_name),
                 common.quote_ident(self.new_trigger_name))


class DropTrigger(DDLOperation):
    def __init__(self, trigger_name, *, table_name, **kwargs):
        super().__init__(**kwargs)

        self.trigger_name = trigger_name
        self.table_name = table_name

    def code(self, context):
        return 'DROP TRIGGER %(trigger_name)s ON %(table_name)s' % \
                {'trigger_name': common.quote_ident(self.trigger_name),
                 'table_name': common.qname(*self.table_name)}


class Comment(DDLOperation):
    def __init__(self, object, text, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__()

        self.object = object
        self.text = text

    def code(self, context):
        if isinstance(self.object, TableConstraint):
            object_type = 'CONSTRAINT'
            object_name = self.object.constraint_name()
            table_name = self.object.table_name
        else:
            assert False

        code = 'COMMENT ON %s %s %s IS %s' % \
                (object_type, object_name,
                 'ON %s' % common.qname(*table_name) if table_name else '',
                  postgresql.string.quote_literal(self.text))

        return code
