##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import itertools
import postgresql.string
import re

from semantix import caos
from semantix.caos import proto
from semantix.caos import delta as delta_cmds
from semantix.caos.caosql import expr as caosql_expr
from semantix.caos import objects as caos_objects

from semantix.utils import datastructures
from semantix.utils.debug import debug
from semantix.utils.lang import yaml
from semantix.utils.algos.persistent_hash import persistent_hash
from semantix.utils import markup

from semantix.caos.backends.pgsql import common
from semantix.caos.backends.pgsql import dbops, deltadbops, features
from . import ast as pg_ast
from . import codegen
from . import datasources
from . import schemamech
from . import transformer
from . import types


BACKEND_FORMAT_VERSION = 9


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
        for op in sorted(self.pgops, key=lambda i: getattr(i, 'priority', 0), reverse=True):
            op.execute(context)

    @classmethod
    def as_markup(cls, self, *, ctx):
        node = markup.elements.lang.TreeNode(name=repr(self))

        for op in self.pgops:
            node.add_child(node=markup.serialize(op, ctx=ctx))

        return node


class CommandGroupAdapted(MetaCommand, adapts=delta_cmds.CommandGroup):
    def apply(self, meta, context):
        delta_cmds.CommandGroup.apply(self, meta, context)
        MetaCommand.apply(self, meta, context)


class PrototypeMetaCommand(MetaCommand, delta_cmds.PrototypeCommand):
    pass


class NamedPrototypeMetaCommand(PrototypeMetaCommand, delta_cmds.NamedPrototypeCommand):
    def fill_record(self, rec=None, obj=None):
        updates = {}

        myrec = self.table.record()

        if not obj:
            fields = self.get_struct_properties(include_old_value=True)

            for name, value in fields.items():
                # XXX: for backwards compatibility, should convert the delta code
                #      to expect objects in 'source' and 'target' fields of pointers,
                if isinstance(value[0], caos.proto.PrototypeRef):
                    v0 = value[0].prototype_name
                else:
                    v0 = value[0]

                if isinstance(value[1], caos.proto.PrototypeRef):
                    v1 = value[1].prototype_name
                else:
                    v1 = value[1]

                updates[name] = (v0, v1)
                if hasattr(myrec, name):
                    if not rec:
                        rec = self.table.record()
                    setattr(rec, name, v1)
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

            if getattr(rec, 'title', None):
                rec.title = rec.title.as_dict()

        return rec, updates

    def pack_default(self, alter_default):
        if alter_default.new_value is not None:
            return yaml.Language.dump(alter_default.new_value)
        else:
            result = None
        return result

    def create_object(self, prototype):
        rec, updates = self.fill_record()
        self.pgops.add(dbops.Insert(table=self.table, records=[rec]))
        return updates

    def rename(self, old_name, new_name):
        pass


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
        self.table = deltadbops.AtomTable()

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
                self.pgops.add(dbops.RenameDomain(domain_name, new_name))
                target_type = common.qname(*domain_name)

                self.pgops.add(dbops.CreateDomain(name=domain_name, base=new_type))

                adapt = deltadbops.SchemaDBObjectMeta.adapt
                for constraint in new_constraints:
                    adapted = adapt(constraint)
                    constraint_name = adapted.get_backend_constraint_name()
                    constraint_code = adapted.get_backend_constraint_check_code()
                    self.pgops.add(dbops.AlterDomainAddConstraint(name=domain_name,
                                                                  constraint_name=constraint_name,
                                                                  constraint_code=constraint_code))

                domain_name = new_name
        elif intent == 'create':
            self.pgops.add(dbops.CreateDomain(name=domain_name, base=base))

        for host_proto, item_proto in users:
            if isinstance(item_proto, proto.Link):
                name = item_proto.normal_name()
            else:
                name = item_proto.name

            table_name = common.get_table_name(host_proto, catenate=False)
            column_name = common.caos_name_to_pg_name(name)

            alter_type = dbops.AlterTableAlterColumnType(column_name, target_type)
            alter_table = dbops.AlterTable(table_name)
            alter_table.add_operation(alter_type)
            self.pgops.add(alter_table)

        if not host:
            for child_atom in meta(type='atom', include_automatic=True):
                if child_atom.base == atom.name:
                    self.alter_atom_type(child_atom, meta, None, None, target_type, 'alter')

        if intent == 'drop' or (intent == 'alter' and not simple_alter):
            self.pgops.add(dbops.DropDomain(domain_name))


class CreateAtom(AtomMetaCommand, adapts=delta_cmds.CreateAtom):
    def apply(self, meta, context=None):
        atom = delta_cmds.CreateAtom.apply(self, meta, context)
        AtomMetaCommand.apply(self, meta, context)

        new_domain_name = common.atom_name_to_domain_name(atom.name, catenate=False)
        base, _, constraints, extraconstraints = types.get_atom_base_and_constraints(meta, atom)

        updates = self.create_object(atom)

        if not atom.automatic:
            self.pgops.add(dbops.CreateDomain(name=new_domain_name, base=base))

            if atom.issubclass(meta, caos_objects.sequence.Sequence):
                seq_name = common.atom_name_to_sequence_name(atom.name, catenate=False)
                self.pgops.add(dbops.CreateSequence(name=seq_name))

            adapt = deltadbops.SchemaDBObjectMeta.adapt
            for constraint in constraints:
                adapted = adapt(constraint)
                constraint_name = adapted.get_backend_constraint_name()
                constraint_code = adapted.get_backend_constraint_check_code()
                self.pgops.add(dbops.AlterDomainAddConstraint(name=new_domain_name,
                                                              constraint_name=constraint_name,
                                                              constraint_code=constraint_code))

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
                    self.pgops.add(dbops.AlterDomainAlterDefault(name=new_domain_name,
                                                                 default=default.new_value[0].value))
        else:
            source, pointer = CompositePrototypeMetaCommand.get_source_and_pointer_ctx(meta, context)

            # Skip inherited links
            if pointer.proto.source.name == source.proto.name:
                alter_table = source.op.get_alter_table(context)

                for constraint in constraints:
                    constraint = source.op.get_pointer_constraint(meta, context, constraint)
                    op = dbops.AlterTableAddConstraint(constraint=constraint)
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
            self.pgops.add(dbops.Update(table=self.table, record=rec, condition=condition))

        return atom


class RenameAtom(AtomMetaCommand, adapts=delta_cmds.RenameAtom):
    def apply(self, meta, context=None):
        proto = delta_cmds.RenameAtom.apply(self, meta, context)
        AtomMetaCommand.apply(self, meta, context)

        super().rename(self.prototype_name, self.new_name)

        domain_name = common.atom_name_to_domain_name(self.prototype_name, catenate=False)
        new_domain_name = common.atom_name_to_domain_name(self.new_name, catenate=False)

        self.pgops.add(dbops.RenameDomain(name=domain_name, new_name=new_domain_name))
        updaterec = self.table.record(name=str(self.new_name))
        condition = [('name', str(self.prototype_name))]
        self.pgops.add(dbops.Update(table=self.table, record=updaterec, condition=condition))

        if not proto.automatic and proto.issubclass(meta, caos_objects.sequence.Sequence):
            seq_name = common.atom_name_to_sequence_name(self.prototype_name, catenate=False)
            new_seq_name = common.atom_name_to_sequence_name(self.new_name, catenate=False)

            self.pgops.add(dbops.RenameSequence(name=seq_name, new_name=new_seq_name))

        return proto


class AlterAtom(AtomMetaCommand, adapts=delta_cmds.AlterAtom):
    def apply(self, meta, context=None):
        old_atom = meta.get(self.prototype_name).copy()
        new_atom = delta_cmds.AlterAtom.apply(self, meta, context)
        AtomMetaCommand.apply(self, meta, context)

        updaterec, updates = self.fill_record()

        if updaterec:
            condition = [('name', str(old_atom.name))]
            self.pgops.add(dbops.Update(table=self.table, record=updaterec, condition=condition))

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
                        adad = dbops.AlterDomainAlterDefault(name=domain_name, default=new_default)
                        op.pgops.add(adad)

            if new_atom.automatic:
                alter_table = source.op.get_alter_table(context)

                for constraint in old_constraints - new_constraints:
                    constraint = source.op.get_pointer_constraint(meta, context, constraint)
                    op = dbops.AlterTableDropConstraint(constraint=constraint)
                    alter_table.add_operation(op)

                for constraint in new_constraints - old_constraints:
                    constraint = source.op.get_pointer_constraint(meta, context, constraint)
                    op = dbops.AlterTableAddConstraint(constraint=constraint)
                    alter_table.add_operation(op)

            else:
                adapt = deltadbops.SchemaDBObjectMeta.adapt

                for constraint in old_constraints - new_constraints:
                    adapted = adapt(constraint)
                    constraint_name = adapted.get_backend_constraint_name()
                    constraint_code = adapted.get_backend_constraint_check_code()
                    addc = dbops.AlterDomainDropConstraint(name=domain_name,
                                                           constraint_name=constraint_name,
                                                           constraint_code=constraint_code)
                    op.pgops.add(addc)

                for constraint in new_constraints - old_constraints:
                    adapted = adapt(constraint)
                    constraint_name = adapted.get_backend_constraint_name()
                    constraint_code = adapted.get_backend_constraint_check_code()
                    adac = dbops.AlterDomainAddConstraint(name=domain_name,
                                                          constraint_name=constraint_name,
                                                          constraint_code=constraint_code)
                    op.pgops.add(adac)
        else:
            # We need to drop orphan constraints
            if old_atom.automatic:
                alter_table = source.op.get_alter_table(context)

                for constraint in old_constraints:
                    constraint = source.op.get_pointer_constraint(meta, context, constraint)
                    op = dbops.AlterTableDropConstraint(constraint=constraint)
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
        cond = dbops.DomainExists(old_domain_name)
        ops.add(dbops.DropDomain(name=old_domain_name, conditions=[cond], priority=3))
        ops.add(dbops.Delete(table=deltadbops.AtomTable(),
                             condition=[('name', str(self.prototype_name))]))

        if not atom.automatic and atom.issubclass(meta, caos_objects.sequence.Sequence):
            seq_name = common.atom_name_to_sequence_name(self.prototype_name, catenate=False)
            self.pgops.add(dbops.DropSequence(name=seq_name))

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
                        columns.append(dbops.TextSearchIndexColumn(column_name, link.search.weight,
                                                                   'english'))

            if columns:
                table_name = common.get_table_name(self.host, catenate=False)

                index_name = self.get_index_name(table_name, 'default')
                index = dbops.TextSearchIndex(name=index_name, table_name=table_name,
                                              columns=columns)

                cond = dbops.IndexExists(index_name=(table_name[0], index_name))
                op = dbops.DropIndex(index_name=(table_name[0], index_name), conditions=(cond,))
                self.pgops.add(op)
                op = dbops.CreateIndex(index=index)
                self.pgops.add(op)


class CompositePrototypeMetaCommand(NamedPrototypeMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table_name = None
        self.record_name = None
        self._multicommands = {}
        self.update_search_indexes = None
        self.pointer_constraints = {}
        self.abstract_pointer_constraints = {}
        self.dropped_pointer_constraints = {}
        self._constr_mech = schemamech.ConstraintMech()

    def _get_multicommand(self, context, cmdtype, object_name, *, priority=0, force_new=False,
                                                                  manual=False, cmdkwargs={}):
        key = (priority, frozenset(cmdkwargs.items()))

        try:
            typecommands = self._multicommands[cmdtype]
        except KeyError:
            typecommands = self._multicommands[cmdtype] = {}

        commands = typecommands.get(key)

        if commands is None or force_new or manual:
            command = cmdtype(object_name, priority=priority, **cmdkwargs)

            if not manual:
                try:
                    commands = typecommands[key]
                except KeyError:
                    commands = typecommands[key] = []

                commands.append(command)
        else:
            command = commands[-1]

        return command

    def _attach_multicommand(self, context, cmdtype):
        try:
            typecommands = self._multicommands[cmdtype]
        except KeyError:
            return
        else:
            commands = list(itertools.chain.from_iterable(typecommands.values()))

            if commands:
                commands = sorted(commands, key=lambda i: i.priority)
                self.pgops.update(commands)

    def get_alter_table(self, context, priority=0, force_new=False, contained=False, manual=False):
        if not self.table_name:
            assert self.__class__.context_class
            ctx = context.get(self.__class__.context_class)
            assert ctx
            self.table_name = common.get_table_name(ctx.proto, catenate=False)

        return self._get_multicommand(context, dbops.AlterTable, self.table_name,
                                      priority=priority,
                                      force_new=force_new, manual=manual,
                                      cmdkwargs={'contained': contained})

    def attach_alter_table(self, context):
        self._attach_multicommand(context, dbops.AlterTable)

    def get_alter_record(self, context, priority=0, force_new=False, manual=False, source=None):
        if source is None:
            if not self.record_name:
                assert self.__class__.context_class
                ctx = context.get(self.__class__.context_class)
                assert ctx
                self.record_name = common.get_record_name(ctx.proto, catenate=False)
            record_name = self.record_name
        else:
            record_name = common.get_record_name(source, catenate=False)

        return self._get_multicommand(context, dbops.AlterCompositeType, record_name,
                                      priority=priority,
                                      force_new=force_new, manual=manual)

    def attach_alter_record(self, context):
        self._attach_multicommand(context, dbops.AlterCompositeType)

    def rename(self, old_name, new_name, obj=None):
        super().rename(old_name, new_name)

        if obj is not None and isinstance(obj, caos.types.ProtoLink):
            old_table_name = common.link_name_to_table_name(old_name, catenate=False)
            old_rec_name = common.link_name_to_record_name(old_name, catenate=False)
            new_table_name = common.link_name_to_table_name(new_name, catenate=False)
            new_rec_name = common.link_name_to_record_name(new_name, catenate=False)
        else:
            old_table_name = common.concept_name_to_table_name(old_name, catenate=False)
            old_rec_name = common.concept_name_to_record_name(old_name, catenate=False)
            new_table_name = common.concept_name_to_table_name(new_name, catenate=False)
            new_rec_name = common.concept_name_to_record_name(new_name, catenate=False)

        cond = dbops.TableExists(name=old_table_name)
        rec_cond = dbops.CompositeTypeExists(name=old_rec_name)

        if old_name.module != new_name.module:
            self.pgops.add(dbops.AlterTableSetSchema(old_table_name, new_table_name[0],
                                                     conditions=(cond,)))
            old_table_name = (new_table_name[0], old_table_name[1])

            self.pgops.add(dbops.AlterCompositeTypeSetSchema(old_rec_name, new_rec_name[0],
                                                             conditions=(rec_cond,)))
            old_rec_name = (new_rec_name[0], old_rec_name[1])

        if old_name.name != new_name.name:
            self.pgops.add(dbops.AlterTableRenameTo(old_table_name, new_table_name[1],
                                                    conditions=(cond,)))

            self.pgops.add(dbops.AlterCompositeTypeRenameTo(old_rec_name, new_rec_name[1],
                                                            conditions=(rec_cond,)))

        updaterec = self.table.record(name=str(new_name))
        condition = [('name', str(old_name))]
        self.pgops.add(dbops.Update(table=self.table, record=updaterec, condition=condition))

        old_func_name = (old_table_name[0],
                         common.caos_name_to_pg_name(old_name.name + '_batch_merger'))
        new_func_name = (new_table_name[0],
                         common.caos_name_to_pg_name(new_name.name + '_batch_merger'))

        cond = dbops.FunctionExists(old_func_name, args=('text',),)
        cmd = dbops.RenameFunction(old_func_name, args=('text',), new_name=new_func_name,
                             conditions=(cond,))
        self.pgops.add(cmd)

    def delete(self, proto, meta, context):
        schema = common.caos_module_name_to_schema_name(proto.name.module)
        name = common.caos_name_to_pg_name(proto.name.name + '_batch_merger')
        func_name = (schema, name)
        cond = dbops.FunctionExists(func_name, args=('text',),)
        cmd = dbops.DropFunction(func_name, args=('text',), conditions=(cond,))
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

            self.pgops.add(dbops.RenameIndex(old_name=(source_table[0], old_name),
                                             new_name=new_name))

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
        if source is None:
            host, pointer = cls.get_source_and_pointer_ctx(meta, context)

            if original:
                source, pointer_name = host.original_proto, pointer.original_proto.normal_name()
            else:
                source, pointer_name = host.proto, pointer.proto.normal_name()

        schemac_to_backendc = schemamech.ConstraintMech.schema_constraint_to_backend_constraint
        return schemac_to_backendc(constraint, source, pointer_name)

    def apply_inherited_deltas(self, source, meta, context):
        top_ctx = context.get(delta_cmds.RealmCommandContext)

        if isinstance(source, caos.types.ProtoConcept):
            cmd_class = delta_cmds.ConceptCommand
        else:
            cmd_class = delta_cmds.LinkCommand

        proto_idx = {cmd.prototype: cmd for cmd in top_ctx.op(cmd_class)
                                        if getattr(cmd, 'prototype', None)}

        for pointer_name, pointer in source.pointers.items():
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

                if not isinstance(pointer, caos.types.ProtoComputable) \
                        and not isinstance(source, caos.types.ProtoPointer) \
                        and pointer.target is not None:
                    ptr_stor_info = types.get_pointer_storage_info(meta, pointer, record_mode=True)

                    if ptr_stor_info.in_record:
                        alter_record = self.get_alter_record(context, force_new=True)
                        col = dbops.Column(name=ptr_stor_info.column_name,
                                           type=ptr_stor_info.column_type,
                                           comment=pointer.normal_name())
                        cond = dbops.CompositeTypeAttributeExists(self.record_name,
                                                                  ptr_stor_info.column_name)
                        alter_record.add_command((dbops.AlterCompositeTypeAddAttribute(col), None,
                                                  (cond,)))

    def affirm_pointer_defaults(self, source, meta, context):
        for pointer_name, pointer in source.pointers.items():
            if pointer.generic() or not pointer.atomic() or not pointer.singular() or \
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
                alter_table.add_operation(dbops.AlterTableAlterColumnDefault(column_name=column_name,
                                                                             default=default))

    def create_pointer_constraints(self, source, meta, context):
        for pointer_name, pointer in source.pointers.items():
            if pointer_name not in source.own_pointers:
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

    def adjust_pointer_constraints(self, meta, context, source, pointer_names=None):
        source.materialize(meta)

        for pointer in (p for p in source.pointers.values() if p.atomic()):
            target = pointer.target

            pointer_name = pointer.normal_name()

            if pointer_names is not None and pointer_name not in pointer_names:
                continue

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

            for op in alter_table(dbops.TableConstraintCommand):
                if isinstance(op, dbops.AlterTableDropConstraint):
                    name = op.constraint.raw_constraint_name()
                    drop_constraints[name] = op

            if pointer_name in source.own_pointers:
                ptr_op = ptr_op_class(prototype_name=pointer.name,
                                      prototype_class=pointer.__class__.get_canonical_class())


                if target.automatic:
                    # We need to establish fake AlterLink context here since
                    # atom constraint constraint ops need it.
                    orig_ctx = context.get(ptr_ctx_class)

                    with context(ptr_ctx_class(ptr_op, pointer)) as ptr_ctx:
                        if orig_ctx is not None:
                            ptr_ctx.original_proto = orig_ctx.original_proto

                        for constraint in target.effective_local_constraints.values():
                            old_constraint = self.get_pointer_constraint(meta, context, constraint,
                                                                         original=True)

                            if old_constraint.raw_constraint_name() in drop_constraints:
                                # No need to rename constraints that are to be dropped
                                continue

                            new_constraint = self.get_pointer_constraint(meta, context, constraint)

                            op = dbops.AlterTableRenameConstraint(table_name=table,
                                                                  constraint=old_constraint,
                                                                  new_constraint=new_constraint)
                            self.pgops.add(op)

            if not pointer.generic():
                orig_source = source_context.original_proto

                ptr_op = ptr_op_class(prototype_name=pointer.name,
                                      prototype_class=pointer.__class__.get_canonical_class())

                with context(ptr_ctx_class(ptr_op, pointer)):
                    for constraint in itertools.chain(pointer.constraints.values(),
                                                      pointer.abstract_constraints.values()):
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
            op = dbops.AlterTableAddConstraint(constraint=constr)
            alter_table.add_operation(op)

            constraint_origins = source.get_constraint_origins(meta, pointer_name, constraint)
            assert constraint_origins

            cuct = self._constr_mech.create_unique_constraint_trigger(source, pointer_name, constr,
                                                                      constraint_origins, meta,
                                                                      context)
            self.pgops.add(cuct)

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
            op = dbops.AlterTableDropConstraint(constraint=constr)
            alter_table.add_operation(op)

            if not conditional:
                constraint_origins = source.get_constraint_origins(meta, pointer_name, constraint)
                assert constraint_origins

            drop_trig = self._constr_mech.drop_unique_constraint_trigger(source, pointer_name,
                                                                         constr, meta, context)

            if conditional:
                conds = self._constr_mech.unique_constraint_trigger_exists(source, pointer_name,
                                                                           constr)
                op = dbops.CommandGroup(conditions=conds)
                op.add_commands([alter_table, drop_trig])
                self.pgops.add(op)
            else:
                self.pgops.add(drop_trig)

            try:
                ptr_dropped_constr = self.dropped_pointer_constraints[pointer_name]
            except KeyError:
                ptr_dropped_constr = self.dropped_pointer_constraints[pointer_name] = {}

            ptr_dropped_constr[constr_key] = constraint

    def rename_pointer_constraint(self, orig_source, source, pointer_name,
                                        old_constraint, new_constraint, meta, context):

        table = common.get_table_name(source, catenate=False)

        result = dbops.CommandGroup()

        result.add_command(dbops.AlterTableRenameConstraint(table_name=table,
                                                            constraint=old_constraint,
                                                            new_constraint=new_constraint))

        ops = self._constr_mech.rename_unique_constraint_trigger(orig_source, source, pointer_name,
                                                                 old_constraint, new_constraint,
                                                                 meta, context)

        result.add_commands(ops)

        return result

    def adjust_pointer_storage(self, orig_pointer, pointer, meta, context):
        old_ptr_stor_info = types.get_pointer_storage_info(meta, orig_pointer)
        new_ptr_stor_info = types.get_pointer_storage_info(meta, pointer)

        old_target = orig_pointer.target
        new_target = pointer.target

        source_ctx = context.get(delta_cmds.ConceptCommandContext)
        source_proto = source_ctx.proto
        source_op = source_ctx.op

        type_change_ok = False

        if old_target.name != new_target.name \
                                or old_ptr_stor_info.table_type != new_ptr_stor_info.table_type:

            for op in self(delta_cmds.AtomCommand):
                for rename in op(delta_cmds.RenameAtom):
                    if old_target.name == rename.prototype_name \
                                        and new_target.name == rename.new_name:
                        # Our target alter is a mere rename
                        type_change_ok = True

                if isinstance(op, delta_cmds.CreateAtom):
                    if op.prototype_name == new_target.name:
                        # CreateAtom will take care of everything for us
                        type_change_ok = True

            if old_ptr_stor_info.table_type != new_ptr_stor_info.table_type:
                # The attribute is being moved from one table to another
                opg = dbops.CommandGroup(priority=1)
                at = source_op.get_alter_table(context, manual=True)
                pat = self.get_alter_table(context, manual=True)

                if old_ptr_stor_info.table_type[0] == 'source':
                    # Moved from concept table to link table
                    col = dbops.Column(name=old_ptr_stor_info.column_name,
                                       type=old_ptr_stor_info.column_type)
                    at.add_command(dbops.AlterTableDropColumn(col))

                    newcol = dbops.Column(name=new_ptr_stor_info.column_name,
                                          type=new_ptr_stor_info.column_type)

                    pat.add_command(dbops.AlterTableAddColumn(newcol))
                else:
                    oldcol = dbops.Column(name=old_ptr_stor_info.column_name,
                                          type=old_ptr_stor_info.column_type)
                    pat.add_command(dbops.AlterTableDropColumn(oldcol))

                    # Moved from link to concept
                    cols = self.get_columns(pointer, meta)
                    ops = [dbops.AlterTableAddColumn(col) for col in cols]
                    for op in ops:
                        at.add_operation(op)

                opg.add_command(at)
                opg.add_command(pat)

                self.pgops.add(opg)

            else:
                if old_target != new_target and not type_change_ok:
                    if isinstance(old_target, caos.types.ProtoAtom):
                        AlterAtom.alter_atom(self, meta, context, old_target,
                                                                  new_target, in_place=False)

                        alter_table = source_op.get_alter_table(context, priority=1)
                        alter_type = dbops.AlterTableAlterColumnType(
                                                old_ptr_stor_info.column_name,
                                                types.pg_type_from_object(meta, new_target))
                        alter_table.add_operation(alter_type)

                        opg = dbops.CommandGroup(priority=1)
                        alter_type = dbops.AlterCompositeTypeAlterAttributeType(
                                                old_ptr_stor_info.column_name,
                                                types.pg_type_from_object(meta, new_target))

                        for src in itertools.chain((source_proto,), source_proto.children()):
                            alter_record = source_op.get_alter_record(context, manual=True,
                                                                      source=src)
                            alter_record.add_command(alter_type)
                            opg.add_command(alter_record)

                        self.pgops.add(opg)

        if orig_pointer.get_loading_behaviour() != pointer.get_loading_behaviour():
            opg = dbops.CommandGroup(priority=1)

            if orig_pointer.get_loading_behaviour() == 'eager':
                # Pointer is no longer eager -- drop it from the record
                attr = dbops.Column(name=old_ptr_stor_info.column_name, type='text')

                for src in itertools.chain((source_proto,), source_proto.children()):
                    alter_record = source_op.get_alter_record(context, manual=True,
                                                                       source=src)
                    alter_record.add_command(dbops.AlterCompositeTypeDropAttribute(attr))
                    opg.add_command(alter_record)
            else:
                # Pointer became eager -- add it to the record
                attrs = self.get_columns(pointer, meta, record_mode=True)
                ops = [dbops.AlterCompositeTypeAddAttribute(attr) for attr in attrs]

                for src in itertools.chain((source_proto,), source_proto.children()):
                    alter_record = source_op.get_alter_record(context, manual=True,
                                                                       source=src)
                    for op in ops:
                        alter_record.add_command(op)
                    opg.add_command(alter_record)

            self.pgops.add(opg)

        old_ptr_stor_info = types.get_pointer_storage_info(meta, orig_pointer,
                                                           record_mode=True)
        new_ptr_stor_info = types.get_pointer_storage_info(meta, pointer,
                                                           record_mode=True)

        if old_ptr_stor_info.column_type != new_ptr_stor_info.column_type:
            # Composite type attribute type change, possibly due to mapping change
            opg = dbops.CommandGroup(priority=1)

            for src in itertools.chain((source_proto,), source_proto.children()):
                alter_record = source_op.get_alter_record(context, manual=True,
                                                                   source=src)
                actaat = dbops.AlterCompositeTypeAlterAttributeType(
                                        new_ptr_stor_info.column_name,
                                        new_ptr_stor_info.column_type)
                alter_record.add_command(actaat)
                opg.add_command(alter_record)

            self.pgops.add(opg)

    def apply_base_delta(self, orig_source, source, meta, context):
        realm = context.get(delta_cmds.RealmCommandContext)
        orig_source.base = tuple(realm.op._renames.get(b, b) for b in orig_source.base)

        dropped_bases = set(orig_source.base) - set(source.base)
        added_bases = set(source.base) - set(orig_source.base)

        if isinstance(source, caos.types.ProtoConcept):
            nameconv = common.concept_name_to_table_name
            source_ctx = context.get(delta_cmds.ConceptCommandContext)
            ptr_cmd = delta_cmds.CreateLink
        else:
            nameconv = common.link_name_to_table_name
            source_ctx = context.get(delta_cmds.LinkCommandContext)
            ptr_cmd = delta_cmds.CreateLinkProperty

        alter_table = source_ctx.op.get_alter_table(context)
        alter_record = source_ctx.op.get_alter_record(context)

        if isinstance(source, caos.types.ProtoConcept) \
                        or source_ctx.op.has_table(source, meta):

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
                ptr_stor_info = types.get_pointer_storage_info(meta, ptr)

                if ptr_stor_info.table_type[0] == 'source':
                    col = dbops.Column(name=ptr_stor_info.column_name,
                                       type=ptr_stor_info.column_type,
                                       required=ptr.required)
                    alter_table.add_operation(dbops.AlterTableAddColumn(col))

                ptr_stor_info = types.get_pointer_storage_info(meta, ptr, record_mode=True)

                if ptr_stor_info.in_record:
                    col = dbops.Column(name=ptr_stor_info.column_name,
                                       type=ptr_stor_info.column_type,
                                       required=ptr.required)
                    alter_table.add_operation(dbops.AlterTableAddColumn(col))
                    alter_record.add_command(dbops.AlterCompositeTypeAddAttribute(col))

            if dropped_bases:
                dropped_ptrs = set(orig_source.pointers) - set(source.pointers)

                if dropped_ptrs:
                    for dropped_ptr in dropped_ptrs:
                        ptr = orig_source.pointers[dropped_ptr]
                        ptr_stor_info = types.get_pointer_storage_info(meta, ptr, record_mode=True)

                        if ptr_stor_info.in_record:
                            col = dbops.Column(name=ptr_stor_info.column_name,
                                               type=ptr_stor_info.column_type,
                                               required=ptr.required)
                            alter_record.add_command(dbops.AlterCompositeTypeDropAttribute(col))

                for dropped_base in dropped_bases:
                    parent_table_name = nameconv(caos.name.Name(dropped_base), catenate=False)
                    op = dbops.AlterTableDropParent(parent_name=parent_table_name)
                    alter_table.add_operation(op)

            for added_base in added_bases:
                parent_table_name = nameconv(caos.name.Name(added_base), catenate=False)
                table_name = nameconv(source.name, catenate=False)
                cond = dbops.TableInherits(table_name, parent_table_name)
                op = dbops.AlterTableAddParent(parent_name=parent_table_name)
                alter_table.add_operation((op, None, [cond]))


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
        pg_index = dbops.Index(name=index_name, table_name=table_name, expr=sql_expr, unique=False)
        self.pgops.add(dbops.CreateIndex(pg_index, priority=3))

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
            index_exists = dbops.IndexExists((table_name[0], index_name))
            self.pgops.add(dbops.DropIndex((table_name[0], index_name), priority=3,
                                           conditions=(index_exists,)))

        return index


class ConceptMetaCommand(CompositePrototypeMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = deltadbops.ConceptTable()

    def fill_record(self, rec=None):
        rec, updates = super().fill_record(rec)
        if rec and rec.custombases:
            rec.custombases = tuple(str(b) for b in rec.custombases)
        return rec, updates


class CreateConcept(ConceptMetaCommand, adapts=delta_cmds.CreateConcept):
    def apply(self, meta, context=None):
        new_table_name = common.concept_name_to_table_name(self.prototype_name, catenate=False)
        new_record_name = common.concept_name_to_record_name(self.prototype_name)
        self.table_name = new_table_name
        self.record_name = new_record_name
        concept_table = dbops.Table(name=new_table_name)
        concept_record = dbops.CompositeType(name=new_record_name)
        self.pgops.add(dbops.CreateTable(table=concept_table))
        self.pgops.add(dbops.CreateCompositeType(type=concept_record))

        alter_table = self.get_alter_table(context)
        alter_record = self.get_alter_record(context)

        concept = delta_cmds.CreateConcept.apply(self, meta, context)
        ConceptMetaCommand.apply(self, meta, context)

        fields = self.create_object(concept)

        cid_col = dbops.Column(name='concept_id', type='integer', required=True)

        if concept.name == 'semantix.caos.builtins.BaseObject' or concept.is_virtual:
            alter_table.add_operation(dbops.AlterTableAddColumn(cid_col))

        alter_record.add_command(dbops.AlterCompositeTypeAddAttribute(cid_col))

        if not concept.is_virtual:
            constraint = dbops.PrimaryKey(table_name=alter_table.name,
                                          columns=['semantix.caos.builtins.id'])
            alter_table.add_operation(dbops.AlterTableAddConstraint(constraint))

        bases = (common.concept_name_to_table_name(p, catenate=False)
                 for p in fields['base'][1] if proto.Concept.is_prototype(meta, p))
        concept_table.bases = list(bases)

        self.apply_inherited_deltas(concept, meta, context)
        self.create_pointer_constraints(concept, meta, context)

        self.affirm_pointer_defaults(concept, meta, context)

        self.attach_alter_table(context)
        self.attach_alter_record(context)

        if self.update_search_indexes:
            self.update_search_indexes.apply(meta, context)
            self.pgops.add(self.update_search_indexes)

        self.pgops.add(dbops.Comment(object=concept_table, text=self.prototype_name))

        return concept


class RenameConcept(ConceptMetaCommand, adapts=delta_cmds.RenameConcept):
    def apply(self, meta, context=None):
        proto = delta_cmds.RenameConcept.apply(self, meta, context)
        ConceptMetaCommand.apply(self, meta, context)

        concept = context.get(delta_cmds.ConceptCommandContext)
        assert concept

        realm = context.get(delta_cmds.RealmCommandContext)
        assert realm

        realm.op._renames[concept.original_proto.name] = proto.name

        concept.op.attach_alter_table(context)
        concept.op.attach_alter_record(context)

        self.rename(self.prototype_name, self.new_name)

        concept.op.table_name = common.concept_name_to_table_name(self.new_name, catenate=False)

        # Need to update all bits that reference concept name

        # Constraints
        self.adjust_pointer_constraints(meta, context, proto)

        # Indexes
        self.adjust_indexes(meta, context, proto)

        self.table_name = common.concept_name_to_table_name(self.new_name, catenate=False)
        self.record_name = common.concept_name_to_record_name(self.new_name, catenate=False)

        concept.original_proto.name = proto.name

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
            self.pgops.add(dbops.Update(table=self.table, record=updaterec, condition=condition))

        self.attach_alter_table(context)
        self.attach_alter_record(context)

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

        self.pgops.add(dbops.DropTable(name=old_table_name))
        self.pgops.add(dbops.Delete(table=self.table, condition=[('name', str(concept.name))]))

        old_record_name = common.concept_name_to_record_name(self.prototype_name, catenate=False)
        self.pgops.add(dbops.DropCompositeType(name=old_record_name))

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
                rec.source_id = dbops.Query('(SELECT id FROM caos.metaobject WHERE name = $1)',
                                            [str(source)], type='integer')

            target = updates.get('target')
            if target:
                rec.target_id = dbops.Query('(SELECT id FROM caos.metaobject WHERE name = $1)',
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

    def alter_host_table_column(self, old_ptr, ptr, meta, context, old_type, new_type):

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

        alter_table = context.get(delta_cmds.ConceptCommandContext).op.get_alter_table(context,
                                                                                       priority=1)
        column_name = common.caos_name_to_pg_name(ptr.normal_name())

        if isinstance(new_target, caos.types.ProtoAtom):
            target_type = types.pg_type_from_atom(meta, new_target)

            if isinstance(old_target, caos.types.ProtoAtom):
                AlterAtom.alter_atom(self, meta, context, old_target, new_target, in_place=False)
                alter_type = dbops.AlterTableAlterColumnType(column_name, target_type)
                alter_table.add_operation(alter_type)
            else:
                cols = self.get_columns(ptr, meta)
                ops = [dbops.AlterTableAddColumn(col) for col in cols]
                for op in ops:
                    alter_table.add_operation(op)
        else:
            col = dbops.Column(name=column_name, type='text')
            alter_table.add_operation(dbops.AlterTableDropColumn(col))

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
                alter_table.add_operation(dbops.AlterTableAlterColumnDefault(column_name=column_name,
                                                                             default=new_default))

    def get_columns(self, pointer, meta, default=None, record_mode=False):
        ptr_stor_info = types.get_pointer_storage_info(meta, pointer, record_mode=record_mode)
        return [dbops.Column(name=ptr_stor_info.column_name,
                             type=ptr_stor_info.column_type,
                             required=pointer.required,
                             default=default, comment=pointer.normal_name())]

    def rename_pointer(self, pointer, meta, context, old_name, new_name):
        if context:
            old_name = pointer.normalize_name(old_name)
            new_name = pointer.normalize_name(new_name)

            host = self.get_host(meta, context)

            if host and old_name != new_name:
                old_col_name = common.caos_name_to_pg_name(old_name)
                new_col_name = common.caos_name_to_pg_name(new_name)

                ptr_stor_info = types.get_pointer_storage_info(meta, pointer)

                if ptr_stor_info.table_type[0] == 'source':
                    table_name = common.get_table_name(host.proto, catenate=False)
                    rename = dbops.AlterTableRenameColumn(table_name, old_col_name, new_col_name)
                    self.pgops.add(rename)

                if ptr_stor_info.in_record:
                    record_name = common.get_record_name(host.proto, catenate=False)
                    rename = dbops.AlterCompositeTypeRenameAttribute(record_name, old_col_name,
                                                                     new_col_name)

        rec = self.table.record()
        rec.name = str(self.new_name)
        self.pgops.add(dbops.Update(table=self.table, record=rec,
                                    condition=[('name', str(self.prototype_name))], priority=1))

    @classmethod
    def has_nontrivial_properties(cls, link, meta):
        return bool([l for l in link.pointers if l not in {'semantix.caos.builtins.source',
                                                           'semantix.caos.builtins.target'}])

    @classmethod
    def has_table(cls, link, meta):
        if isinstance(link, caos.types.ProtoComputable):
            return False
        elif link.generic():
            if link.name == 'semantix.caos.builtins.link':
                return True
            elif link.has_user_defined_properties():
                return True
            else:
                for l in link.children():
                    if not l.generic() and not isinstance(l, caos.types.ProtoComputable):
                        ptr_stor_info = types.get_pointer_storage_info(meta, l, resolve_type=False)
                        if ptr_stor_info.table_type[0] == 'pointer':
                            return True

                return False
        else:
            if link.atomic() and link.has_user_defined_properties():
                return True
            else:
                ptr_stor_info = types.get_pointer_storage_info(meta, link, resolve_type=False)
                return ptr_stor_info.table_type == ('pointer', 'specialized')


class LinkMetaCommand(CompositePrototypeMetaCommand, PointerMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = deltadbops.LinkTable()

    def create_table(self, link, meta, context, conditional=False):
        new_table_name = common.get_table_name(link, catenate=False)

        constraints = []
        columns = []

        src_col = common.caos_name_to_pg_name('semantix.caos.builtins.source')
        tgt_col = common.caos_name_to_pg_name('semantix.caos.builtins.target')

        if link.name == 'semantix.caos.builtins.link':
            columns.append(dbops.Column(name=src_col, type='uuid', required=True))
            columns.append(dbops.Column(name=tgt_col, type='uuid', required=False))
            columns.append(dbops.Column(name='link_type_id', type='integer', required=True))

        constraints.append(dbops.UniqueConstraint(table_name=new_table_name,
                                                  columns=[src_col, tgt_col, 'link_type_id']))

        table = dbops.Table(name=new_table_name)
        table.add_columns(columns)
        table.constraints = constraints

        if link.base:
            bases = []

            for p in link.base:
                if proto.Concept.is_prototype(meta, p):
                    parent = meta.get(p)
                    tabname = common.get_table_name(parent, catenate=False)
                    self.create_table(parent, meta, context, conditional=True)
                    bases.append(tabname)

            table.bases = bases

        ct = dbops.CreateTable(table=table)

        index_name = common.caos_name_to_pg_name(str(link.name)  + 'target_id_default_idx')
        index = dbops.Index(index_name, new_table_name, unique=False)
        index.add_columns([tgt_col])
        ci = dbops.CreateIndex(index)

        if conditional:
            c = dbops.CommandGroup(neg_conditions=[dbops.TableExists(new_table_name)])
        else:
            c = dbops.CommandGroup()

        c.add_command(ct)
        c.add_command(ci)

        self.pgops.add(c)

    def provide_table(self, link, meta, context):
        if not link.generic():
            base = next(iter(link.base))
            gen_link = meta.get(base, include_pyobjects=True, default=None,
                                type=type(link).get_canonical_class(), index_only=False)

            if self.has_table(gen_link, meta):
                self.create_table(gen_link, meta, context, conditional=True)

        if self.has_table(link, meta):
            self.create_table(link, meta, context, conditional=True)

    def schedule_mapping_update(self, link, meta, context):
        if not link.atomic() or self.has_nontrivial_properties(link, meta):
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

        # We do not want to create a separate table for atomic links, unless they have
        # properties, or are non-singular, since those are stored directly in the source
        # table.
        #
        # Implicit derivative links also do not get their own table since they're just
        # a special case of the parent.
        #
        # On the other hand, much like with concepts we want all other links to be in
        # separate tables even if they do not define additional properties.
        # This is to allow for further schema evolution.
        #
        self.provide_table(link, meta, context)

        if not link.generic():
            ptr_stor_info = types.get_pointer_storage_info(meta, link, resolve_type=False)

            concept = context.get(delta_cmds.ConceptCommandContext)
            assert concept, "Link command must be run in Concept command context"

            if ptr_stor_info.table_type[0] == 'source':
                default_value = self.get_pointer_default(link, meta, context)

                cols = self.get_columns(link, meta, default_value)
                table_name = common.get_table_name(concept.proto, catenate=False)
                concept_alter_table = concept.op.get_alter_table(context)

                for col in cols:
                    # The column may already exist as inherited from parent table
                    cond = dbops.ColumnExists(table_name=table_name, column_name=col.name)
                    cmd = dbops.AlterTableAddColumn(col)
                    concept_alter_table.add_operation((cmd, None, (cond,)))

            if ptr_stor_info.in_record:
                if link.target is not None:
                    self._add_to_source_record(meta, context, link, link.target)

        if link.generic():
            self.affirm_pointer_defaults(link, meta, context)

        if self.has_table(link, meta):
            self.apply_inherited_deltas(link, meta, context)
            self.create_pointer_constraints(link, meta, context)

        self.attach_alter_table(context)
        self.attach_alter_record(context)

        concept = context.get(delta_cmds.ConceptCommandContext)
        if not concept or not concept.proto.is_virtual:
            rec, updates = self.record_metadata(link, None, meta, context)
            self.pgops.add(dbops.Insert(table=self.table, records=[rec], priority=1))

        if not link.generic() and link.mapping != caos.types.ManyToMany:
            self.schedule_mapping_update(link, meta, context)

        return link

    def ref_appears(self, schema, context, ref_proto, proto_attr, prototype):
        super().ref_appears(schema, context, ref_proto, proto_attr, prototype)
        if proto_attr == 'target':
            ptr_stor_info = types.get_pointer_storage_info(schema, ref_proto, resolve_type=False)
            if ptr_stor_info.in_record:
                self._add_to_source_record(schema, context, ref_proto, prototype)

    def _add_to_source_record(self, schema, context, pointer, target):
        concept = context.get(delta_cmds.ConceptCommandContext)
        cols = self.get_columns(pointer, schema, record_mode=True)

        opg = dbops.CommandGroup(priority=1)

        ops = [dbops.AlterCompositeTypeAddAttribute(attr) for attr in cols]

        for src in itertools.chain((concept.proto,), concept.proto.children()):
            alter_record = concept.op.get_alter_record(context, manual=True,
                                                                source=src)

            rec_name = common.get_record_name(src, catenate=False)

            for op in ops:
                cond = dbops.CompositeTypeAttributeExists(rec_name, op.attribute.name)
                alter_record.add_command((op, None, (cond,)))
            opg.add_command(alter_record)

        self.pgops.add(opg)


class RenameLink(LinkMetaCommand, adapts=delta_cmds.RenameLink):
    def apply(self, meta, context=None):
        result = delta_cmds.RenameLink.apply(self, meta, context)
        LinkMetaCommand.apply(self, meta, context)

        self.rename_pointer(result, meta, context, self.prototype_name, self.new_name)

        self.attach_alter_table(context)
        self.attach_alter_record(context)

        if result.generic():
            link_cmd = context.get(delta_cmds.LinkCommandContext)
            assert link_cmd

            self.rename(self.prototype_name, self.new_name, obj=result)
            link_cmd.op.table_name = common.link_name_to_table_name(self.new_name, catenate=False)

            # Indexes
            self.adjust_indexes(meta, context, result)
        else:
            link_cmd = context.get(delta_cmds.LinkCommandContext)

            # Constraints
            if link_cmd.proto.normal_name() != link_cmd.original_proto.normal_name():
                concept_cmd = context.get(delta_cmds.ConceptCommandContext)
                self.adjust_pointer_constraints(meta, context, concept_cmd.proto,
                                                pointer_names=(result.normal_name(),))

        return result


class RebaseLink(LinkMetaCommand, adapts=delta_cmds.RebaseLink):
    def apply(self, meta, context):
        result = delta_cmds.RebaseLink.apply(self, meta, context)
        LinkMetaCommand.apply(self, meta, context)

        result.acquire_parent_data(meta)

        link_ctx = context.get(delta_cmds.LinkCommandContext)
        source = link_ctx.proto

        orig_source = link_ctx.original_proto

        if self.has_table(source, meta):
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
                self.pgops.add(dbops.Update(table=self.table, record=rec,
                                            condition=[('name', str(link.name))], priority=1))

            new_type = None
            for op in self(delta_cmds.AlterPrototypeProperty):
                if op.property == 'target':
                    new_type = op.new_value.prototype_name if op.new_value is not None else None
                    old_type = op.old_value.prototype_name if op.old_value is not None else None
                    break

            if new_type:
                if not isinstance(link.target, caos.types.ProtoObject):
                    link.target = meta.get(link.target)

                op = deltadbops.CallDeltaHook(hook='exec_alter_link_target', stage='preprocess',
                                              op=self, priority=1)
                self.pgops.add(op)

            self.attach_alter_table(context)
            self.attach_alter_record(context)

            if not link.generic():
                self.adjust_pointer_storage(old_link, link, meta, context)

            if isinstance(link.target, caos.types.ProtoAtom) and \
                    isinstance(self.old_link.target, caos.types.ProtoAtom) and \
                    link.required != self.old_link.required:

                alter_table = context.get(delta_cmds.ConceptCommandContext).op.get_alter_table(context)
                column_name = common.caos_name_to_pg_name(link.normal_name())
                alter_table.add_operation(dbops.AlterTableAlterColumnNull(column_name=column_name,
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

        if not result.generic():
            ptr_stor_info = types.get_pointer_storage_info(meta, result)
            concept = context.get(delta_cmds.ConceptCommandContext)

            if ptr_stor_info.table_type[0] == 'source':
                name = result.normal_name()

                if name not in concept.proto.pointers:
                    # Do not drop the column if the link was reinherited in the same delta
                    alter_table = concept.op.get_alter_table(context)
                    col = dbops.Column(name=ptr_stor_info.column_name,
                                       type=ptr_stor_info.column_type)
                    col = dbops.AlterTableDropColumn(col)
                    alter_table.add_operation(col)

            if ptr_stor_info.in_record:
                alter_record = concept.op.get_alter_record(context)
                col = dbops.Column(name=ptr_stor_info.column_name,
                                   type=ptr_stor_info.column_type)
                col = dbops.AlterCompositeTypeDropAttribute(col)
                alter_record.add_command(col)

        if self.has_table(result, meta):
            old_table_name = common.get_table_name(result, catenate=False)
            self.pgops.add(dbops.DropTable(name=old_table_name))
            self.cancel_mapping_update(result, meta, context)

        if not result.generic() and result.mapping != caos.types.ManyToMany:
            self.schedule_mapping_update(result, meta, context)

        self.pgops.add(dbops.Delete(table=self.table, condition=[('name', str(result.name))]))

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
                    if isinstance(child, caos.types.ProtoLink) \
                            and not PointerMetaCommand.has_table(child, meta):
                        continue

                    protoclass = source.proto.__class__.get_canonical_class()
                    cmd = source.op.__class__(prototype_name=child.name, prototype_class=protoclass)

                    with context(source.op.__class__.context_class(cmd, child)):
                        # XXX: This can lead to duplicate constraint errors if the child has
                        # been created in the same sync session.
                        cmd.add_pointer_constraint(child, pointer_name, constraint, meta, context)
                        cmd.attach_alter_table(context)
                        cmd.attach_alter_record(context)
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
                    if isinstance(child, caos.types.ProtoLink) \
                            and not PointerMetaCommand.has_table(child, meta):
                        continue

                    protoclass = source.proto.__class__.get_canonical_class()
                    cmd = source.op.__class__(prototype_name=child.name, prototype_class=protoclass)

                    with context(source.op.__class__.context_class(cmd, child)):
                        cmd.del_pointer_constraint(child, pointer_name, constraint, meta, context,
                                                   conditional=True)
                        cmd.attach_alter_table(context)
                        cmd.attach_alter_record(context)
                        self.pgops.add(cmd)

        return constraint


class LinkPropertyMetaCommand(NamedPrototypeMetaCommand, PointerMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = deltadbops.LinkPropertyTable()


class CreateLinkProperty(LinkPropertyMetaCommand, adapts=delta_cmds.CreateLinkProperty):
    def apply(self, meta, context):
        property = delta_cmds.CreateLinkProperty.apply(self, meta, context)
        LinkPropertyMetaCommand.apply(self, meta, context)

        link = context.get(delta_cmds.LinkCommandContext)

        if link:
            generic_link = link.proto if link.proto.generic() else meta.get(link.proto.base[0])
        else:
            generic_link = None

        if link and (link.proto.generic() or \
                        (property.normal_name() in
                         {'semantix.caos.builtins.source', 'semantix.caos.builtins.target'}
                         and self.has_table(generic_link, meta))):

            link.op.provide_table(link.proto, meta, context)
            alter_table = link.op.get_alter_table(context)

            default_value = self.get_pointer_default(property, meta, context)

            cols = self.get_columns(property, meta, default_value)
            for col in cols:
                # The column may already exist as inherited from parent table
                cond = dbops.ColumnExists(table_name=alter_table.name, column_name=col.name)

                cmd = dbops.AlterTableAlterColumnNull(column_name=col.name,
                                                      null=not property.required)
                alter_table.add_operation((cmd, (cond,), None))

                cmd = dbops.AlterTableAddColumn(col)
                alter_table.add_operation((cmd, None, (cond,)))

        with context(delta_cmds.LinkPropertyCommandContext(self, property)):
            rec, updates = self.record_metadata(property, None, meta, context)

        # Priority is set to 2 to make sure that INSERT is run after the host link
        # is INSERTed into caos.link.
        #
        self.pgops.add(dbops.Insert(table=self.table, records=[rec], priority=2))

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
                self.pgops.add(dbops.Update(table=self.table, record=rec,
                                            condition=[('name', str(prop.name))], priority=1))

            if isinstance(prop.target, caos.types.ProtoAtom) and \
                    isinstance(self.old_prop.target, caos.types.ProtoAtom) and \
                    prop.required != self.old_prop.required:

                alter_table = context.get(delta_cmds.LinkCommandContext).op.get_alter_table(context)
                column_name = common.caos_name_to_pg_name(prop.normal_name())
                alter_table.add_operation(dbops.AlterTableAlterColumnNull(column_name=column_name,
                                                                          null=not prop.required))

            new_type = None
            for op in self(delta_cmds.AlterPrototypeProperty):
                if op.property == 'target' and prop.normal_name() not in \
                                {'semantix.caos.builtins.source', 'semantix.caos.builtins.target'}:
                    new_type = op.new_value.prototype_name if op.new_value is not None else None
                    old_type = op.old_value.prototype_name if op.old_value is not None else None
                    break

            if new_type:
                self.alter_host_table_column(old_prop, prop, meta, context, old_type, new_type)

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

            col = dbops.AlterTableDropColumn(dbops.Column(name=column_name, type=column_type))
            alter_table.add_operation(col)

        self.pgops.add(dbops.Delete(table=self.table, condition=[('name', str(property.name))]))

        return property


class ComputableMetaCommand(NamedPrototypeMetaCommand, PointerMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = deltadbops.ComputableTable()

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
                rec.source_id = dbops.Query('(SELECT id FROM caos.metaobject WHERE name = $1)',
                                            [str(source)], type='integer')

            target = updates.get('target')
            if target:
                rec.target_id = dbops.Query('(SELECT id FROM caos.metaobject WHERE name = $1)',
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

        self.pgops.add(dbops.Insert(table=self.table, records=[rec], priority=2))

        return computable


class RenameComputable(ComputableMetaCommand, adapts=delta_cmds.RenameComputable):
    def apply(self, meta, context=None):
        result = delta_cmds.RenameComputable.apply(self, meta, context)
        ComputableMetaCommand.apply(self, meta, context)

        rec = self.table.record()
        rec.name = str(self.new_name)
        self.pgops.add(dbops.Update(table=self.table, record=rec,
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
                self.pgops.add(dbops.Update(table=self.table, record=rec,
                                            condition=[('name', str(computable.name))], priority=1))

        return computable


class DeleteComputable(ComputableMetaCommand, adapts=delta_cmds.DeleteComputable):
    def apply(self, meta, context=None):
        computable = delta_cmds.DeleteComputable.apply(self, meta, context)
        ComputableMetaCommand.apply(self, meta, context)

        self.pgops.add(dbops.Delete(table=self.table, condition=[('name', str(computable.name))]))

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
            sides = ('semantix.caos.builtins.source', 'semantix.caos.builtins.target')

        elif mapping == caos.types.OneToMany:
            # Each target can have only one source, but
            # one source can have many targets
            sides = ('semantix.caos.builtins.target',)

        elif mapping == caos.types.ManyToOne:
            # Each source can have only one target, but
            # one target can have many sources
            sides = ('semantix.caos.builtins.source',)

        else:
            sides = ()

        for side in sides:
            index = deltadbops.MappingIndex(key + '_%s' % side, mapping, maplinks, table_name)
            index.add_columns((side, 'link_type_id'))
            self.pgops.add(dbops.CreateIndex(index, priority=3))


class AlterMappingIndexes(MetaCommand):
    def __init__(self, idx_names, table_name, mapping, maplinks):
        super().__init__()

        self.pgops.add(DropMappingIndexes(idx_names, table_name, mapping))
        self.pgops.add(CreateMappingIndexes(table_name, mapping, maplinks))


class DropMappingIndexes(MetaCommand):
    def __init__(self, idx_names, table_name, mapping):
        super().__init__()

        table_exists = dbops.TableExists(table_name)
        group = dbops.CommandGroup(conditions=(table_exists,), priority=3)

        for idx_name in idx_names:
            fq_idx_name = (table_name[0], idx_name)
            index_exists = dbops.IndexExists(fq_idx_name)
            drop = dbops.DropIndex(fq_idx_name, conditions=(index_exists,), priority=3)
            group.add_command(drop)

        self.pgops.add(group)


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
        self.schema_exists = dbops.SchemaExists(name='caos')

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

        links = []
        for i in link_type_ids:
            # XXX: in certain cases, orphaned indexes are left in the backend
            # after the link was dropped.
            try:
                links.append(link_map[i])
            except KeyError:
                pass

        return mapping, links

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
                    assert not already_processed, "duplicate CreateLink: {}".format(proto.name)
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
    def __init__(self, db, session=None):
        super().__init__()
        self.db = db
        self.session = session
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


class CreateModule(CompositePrototypeMetaCommand, adapts=delta_cmds.CreateModule):
    def apply(self, schema, context):
        CompositePrototypeMetaCommand.apply(self, schema, context)
        module = delta_cmds.CreateModule.apply(self, schema, context)

        module_name = module.name
        schema_name = common.caos_module_name_to_schema_name(module_name)
        condition = dbops.SchemaExists(name=schema_name)

        cmd = dbops.CommandGroup(neg_conditions={condition})
        cmd.add_command(dbops.CreateSchema(name=schema_name))

        modtab = deltadbops.ModuleTable()
        rec = modtab.record()
        rec.name = module_name
        rec.schema_name = schema_name
        rec.imports = module.imports
        cmd.add_command(dbops.Insert(modtab, [rec]))

        self.pgops.add(cmd)

        return module


class AlterModule(CompositePrototypeMetaCommand, adapts=delta_cmds.AlterModule):
    def apply(self, schema, context):
        self.table = deltadbops.ModuleTable()
        module = delta_cmds.AlterModule.apply(self, schema, context=context)
        CompositePrototypeMetaCommand.apply(self, schema, context)

        updaterec, updates = self.fill_record()

        if updaterec:
            condition = [('name', str(module.name))]
            self.pgops.add(dbops.Update(table=self.table, record=updaterec, condition=condition))

        self.attach_alter_table(context)

        return module


class DeleteModule(CompositePrototypeMetaCommand, adapts=delta_cmds.DeleteModule):
    def apply(self, schema, context):
        CompositePrototypeMetaCommand.apply(self, schema, context)
        module = delta_cmds.DeleteModule.apply(self, schema, context)

        module_name = module.name
        schema_name = common.caos_module_name_to_schema_name(module_name)
        condition = dbops.SchemaExists(name=schema_name)

        cmd = dbops.CommandGroup()
        cmd.add_command(dbops.DropSchema(name=schema_name, neg_conditions={condition}))
        cmd.add_command(dbops.Delete(table=deltadbops.ModuleTable(),
                                     condition=[('name', str(module.name))]))

        self.pgops.add(cmd)

        return module


class AlterRealm(MetaCommand, adapts=delta_cmds.AlterRealm):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._renames = {}

    def apply(self, meta, context):
        self.pgops.add(dbops.CreateSchema(name='caos', priority=-3))

        featuretable = deltadbops.FeatureTable()
        self.pgops.add(dbops.CreateTable(table=featuretable,
                                         neg_conditions=[dbops.TableExists(name=featuretable.name)],
                                         priority=-3))

        backendinfotable = deltadbops.BackendInfoTable()
        self.pgops.add(dbops.CreateTable(table=backendinfotable,
                                         neg_conditions=[dbops.TableExists(name=backendinfotable.name)],
                                         priority=-3))

        self.pgops.add(deltadbops.EnableFeature(feature=features.UuidFeature(),
                                                neg_conditions=[dbops.FunctionExists(('caos', 'uuid_nil'))],
                                                priority=-2))

        self.pgops.add(deltadbops.EnableFeature(feature=features.HstoreFeature(),
                                                neg_conditions=[dbops.TypeExists(('caos', 'hstore'))],
                                                priority=-2))

        self.pgops.add(deltadbops.EnableFeature(feature=features.FuzzystrmatchFeature(),
                                                neg_conditions=[dbops.FunctionExists(('caos', 'levenshtein'))],
                                                priority=-2))

        self.pgops.add(deltadbops.EnableFeature(feature=features.ProductAggregateFeature(),
                                                neg_conditions=[dbops.FunctionExists(('caos', 'agg_product'))],
                                                priority=-2))

        deltalogtable = deltadbops.DeltaLogTable()
        self.pgops.add(dbops.CreateTable(table=deltalogtable,
                                         neg_conditions=[dbops.TableExists(name=deltalogtable.name)],
                                         priority=-1))

        deltareftable = deltadbops.DeltaRefTable()
        self.pgops.add(dbops.CreateTable(table=deltareftable,
                                         neg_conditions=[dbops.TableExists(name=deltareftable.name)],
                                         priority=-1))

        moduletable = deltadbops.ModuleTable()
        self.pgops.add(dbops.CreateTable(table=moduletable,
                                         neg_conditions=[dbops.TableExists(name=moduletable.name)],
                                         priority=-1))

        metatable = deltadbops.MetaObjectTable()
        self.pgops.add(dbops.CreateTable(table=metatable,
                                         neg_conditions=[dbops.TableExists(name=metatable.name)],
                                         priority=-1))

        atomtable = deltadbops.AtomTable()
        self.pgops.add(dbops.CreateTable(table=atomtable,
                                         neg_conditions=[dbops.TableExists(name=atomtable.name)],
                                         priority=-1))

        concepttable = deltadbops.ConceptTable()
        self.pgops.add(dbops.CreateTable(table=concepttable,
                                         neg_conditions=[dbops.TableExists(name=concepttable.name)],
                                         priority=-1))

        linktable = deltadbops.LinkTable()
        self.pgops.add(dbops.CreateTable(table=linktable,
                                         neg_conditions=[dbops.TableExists(name=linktable.name)],
                                         priority=-1))

        linkproptable = deltadbops.LinkPropertyTable()
        self.pgops.add(dbops.CreateTable(table=linkproptable,
                                         neg_conditions=[dbops.TableExists(name=linkproptable.name)],
                                         priority=-1))

        computabletable = deltadbops.ComputableTable()
        self.pgops.add(dbops.CreateTable(table=computabletable,
                                         neg_conditions=[dbops.TableExists(name=computabletable.name)],
                                         priority=-1))

        entity_modstat_type = deltadbops.EntityModStatType()
        self.pgops.add(dbops.CreateCompositeType(type=entity_modstat_type,
                                                 neg_conditions=[dbops.CompositeTypeExists(name=entity_modstat_type.name)],
                                                 priority=-1))

        link_endpoints_type = deltadbops.LinkEndpointsType()
        self.pgops.add(dbops.CreateCompositeType(type=link_endpoints_type,
                                                 neg_conditions=[dbops.CompositeTypeExists(name=link_endpoints_type.name)],
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
            print('Upgrading PostgreSQL backend metadata to version {}'.format(version + 1))
            getattr(self, 'update_to_version_{}'.format(version + 1))(context)
        op = self.update_backend_info()
        op.execute(context)

    def update_to_version_1(self, context):
        featuretable = deltadbops.FeatureTable()
        ct = dbops.CreateTable(table=featuretable,
                               neg_conditions=[dbops.TableExists(name=featuretable.name)])
        ct.execute(context)

        backendinfotable = deltadbops.BackendInfoTable()
        ct = dbops.CreateTable(table=backendinfotable,
                               neg_conditions=[dbops.TableExists(name=backendinfotable.name)])
        ct.execute(context)

        # Version 0 did not have feature registry, fix that up
        for feature in (features.UuidFeature, features.HstoreFeature):
            cmd = deltadbops.EnableFeature(feature=feature())
            ins = cmd.extra(context)[0]
            ins.execute(context)

    def update_to_version_2(self, context):
        """
        Backend format 2 adds LinkEndpointsType required for improved link deletion
        procedure.
        """
        group = dbops.CommandGroup()

        link_endpoints_type = deltadbops.LinkEndpointsType()
        cond = dbops.CompositeTypeExists(name=link_endpoints_type.name)
        cmd = dbops.CreateCompositeType(type=link_endpoints_type, neg_conditions=[cond])
        group.add_command(cmd)

        group.execute(context)

    def update_to_version_3(self, context):
        """
        Backend format 3 adds is_virtual attribute to concept description table.
        """
        cond = dbops.ColumnExists(table_name=('caos', 'concept'), column_name='is_virtual')
        cmd = dbops.AlterTable(('caos', 'concept'), neg_conditions=(cond,))
        column = dbops.Column(name='is_virtual', type='boolean', required=True, default=False)
        add_column = dbops.AlterTableAddColumn(column)
        cmd.add_operation(add_column)

        cmd.execute(context)

    def update_to_version_4(self, context):
        """
        Backend format 4 adds automatic attribute to concept description table.
        """
        cond = dbops.ColumnExists(table_name=('caos', 'concept'), column_name='automatic')
        cmd = dbops.AlterTable(('caos', 'concept'), neg_conditions=(cond,))
        column = dbops.Column(name='automatic', type='boolean', required=True, default=False)
        add_column = dbops.AlterTableAddColumn(column)
        cmd.add_operation(add_column)

        cmd.execute(context)

    def update_to_version_5(self, context):
        """\
        Backend format 5 adds imports attribute to module description table.  It also changes
        specialized pointer name format.
        """
        op = dbops.CommandGroup()

        cond = dbops.ColumnExists(table_name=('caos', 'module'), column_name='imports')
        cmd = dbops.AlterTable(('caos', 'module'), neg_conditions=(cond,))
        column = dbops.Column(name='imports', type='varchar[]', required=False)
        add_column = dbops.AlterTableAddColumn(column)
        cmd.add_operation(add_column)

        op.add_command(cmd)

        from semantix.utils.lang import protoschema
        schema = protoschema.get_loaded_proto_schema(proto.SchemaModule)

        modtab = deltadbops.ModuleTable()

        for module in schema.iter_modules():
            module = schema.get_module(module)

            rec = modtab.record()
            rec.imports = module.imports
            condition = [('name', module.name)]
            op.add_command(dbops.Update(table=modtab, record=rec, condition=condition))

        update_schedule = [
            (
                deltadbops.LinkTable(),
                """
                    SELECT
                        l.name AS ptr_name,
                        l.base AS ptr_bases,
                        s.name AS source_name,
                        t.name AS target_name
                    FROM
                        caos.link l
                        INNER JOIN caos.concept s ON (l.source_id = s.id)
                        INNER JOIN caos.metaobject t ON (l.target_id = t.id)
                """
            ),
            (
                deltadbops.LinkPropertyTable(),
                """
                    SELECT
                        l.name AS ptr_name,
                        l.base AS ptr_bases,
                        s.name AS source_name,
                        t.name AS target_name
                    FROM
                        caos.link_property l
                        INNER JOIN caos.link s ON (l.source_id = s.id)
                        INNER JOIN caos.metaobject t ON (l.target_id = t.id)
                """
            ),
            (
                deltadbops.ComputableTable(),
                """
                    SELECT
                        l.name AS ptr_name,
                        s.name AS source_name,
                        t.name AS target_name
                    FROM
                        caos.computable l
                        INNER JOIN caos.metaobject s ON (l.source_id = s.id)
                        INNER JOIN caos.metaobject t ON (l.target_id = t.id)
                """
            )
        ];

        updates = dbops.CommandGroup()

        for table, query in update_schedule:
            ps = context.db.prepare(query)

            for row in ps.rows():
                hash_ = None

                try:
                    base = row['ptr_bases'][0]
                except KeyError:
                    base, _, hash_ = row['ptr_name'].rpartition('_')
                    hash_ = int(hash_, 16)

                source = row['source_name']
                target = row['target_name']

                if hash_ is not None:
                    new_name = caos.types.ProtoPointer._generate_specialized_name(hash_, base)
                else:
                    new_name = caos.types.ProtoPointer.generate_specialized_name(source, target,
                                                                                 base)
                new_name = caos.name.Name(name=new_name, module=caos.name.Name(source).module)

                rec = table.record()
                rec.name = new_name
                condition = [('name', row['ptr_name'])]
                updates.add_command(dbops.Update(table=table, record=rec, condition=condition))

        op.add_command(updates)
        op.execute(context)

    def update_to_version_6(self, context):
        """
        Backend format 6 moves db feature classes to a separate module.
        """

        features = datasources.meta.features.FeatureList(context.db).fetch()

        table = deltadbops.FeatureTable()

        ops = dbops.CommandGroup()

        for feature in features:
            clsname = feature['class_name']
            oldmod = 'semantix.caos.backends.pgsql.delta.'
            if clsname.startswith(oldmod):
                rec = table.record()
                rec.class_name = 'semantix.caos.backends.pgsql.features.' + clsname[len(oldmod):]
                cond = [('name', feature['name'])]
                ops.add_command(dbops.Update(table=table, record=rec, condition=cond))

        ops.execute(context)

    def update_to_version_7(self, context):
        """
        Backend format 7 renames source_id and target_id in link tables into link property cols.
        """

        tabname = common.link_name_to_table_name(caos.Name('semantix.caos.builtins.link'),
                                                 catenate=False)
        src_col = common.caos_name_to_pg_name('semantix.caos.builtins.source')
        tgt_col = common.caos_name_to_pg_name('semantix.caos.builtins.target')

        cond = dbops.ColumnExists(table_name=tabname, column_name=src_col)
        cmd = dbops.CommandGroup(neg_conditions=[cond])

        cmd.add_command(dbops.AlterTableRenameColumn(tabname, 'source_id', src_col))
        cmd.add_command(dbops.AlterTableRenameColumn(tabname, 'target_id', tgt_col))

        cmd.execute(context)

    def update_to_version_8(self, context):
        """
        Backend format 8 adds exposed_behaviour attribute to link description table.
        """

        links_table = deltadbops.LinkTable()

        cond = dbops.ColumnExists(table_name=links_table.name, column_name='exposed_behaviour')

        cmd = dbops.CommandGroup(neg_conditions=(cond,))

        alter = dbops.AlterTable(links_table.name, )
        column = dbops.Column(name='exposed_behaviour', type='text')
        add_column = dbops.AlterTableAddColumn(column)
        alter.add_operation(add_column)
        cmd.add_command(alter)
        cmd.execute(context)

    def update_to_version_9(self, context):
        """
        Backend format 9 adds composite types for concepts and creates link tables for
        specialized atomic links with properties and/or non-singular mapping.
        """

        type_mech = schemamech.TypeMech()

        atom_list = datasources.meta.atoms.AtomList(context.db).fetch()
        atoms = {r['id']: r for r in atom_list}
        concept_list = datasources.meta.concepts.ConceptList(context.db).fetch()
        concepts = {r['id']: r for r in concept_list}
        links_list = datasources.meta.links.ConceptLinks(context.db).fetch()
        links_by_name = {r['name']: r for r in links_list}
        links_by_col_name = {common.caos_name_to_pg_name(n): l for n, l in links_by_name.items()}

        links_by_generic_name = {}

        for lname, link in links_by_name.items():
            genname = caos.types.ProtoPointer.normalize_name(lname)
            try:
                l = links_by_generic_name[genname]
            except KeyError:
                l = links_by_generic_name[genname] = []

            l.append(link)

        link_props = datasources.meta.links.LinkProperties(context.db).fetch()

        link_props_by_link = {}
        for prop in link_props:
            if not prop['source_id'] or prop['name'] in {'semantix.caos.builtins.source',
                                                         'semantix.caos.builtins.target'}:
                continue

            try:
                this_link_props = link_props_by_link[prop['source_id']]
            except KeyError:
                this_link_props = link_props_by_link[prop['source_id']] = []

            this_link_props.append(prop)

        commands = {}

        for concept in concept_list:
            cname = caos.Name(concept['name'])
            table_name = common.concept_name_to_table_name(cname, catenate=False)
            record_name = common.concept_name_to_record_name(cname, catenate=False)

            try:
                cmd = commands[concept['id']]
            except KeyError:
                cmd = commands[concept['id']] = dbops.CommandGroup()

            cond = dbops.CompositeTypeExists(record_name)
            ctype = dbops.CompositeType(name=record_name)
            create = dbops.CreateCompositeType(ctype, neg_conditions=(cond,))
            cmd.add_command(create)

            cond = dbops.CompositeTypeAttributeExists(type_name=record_name,
                                                      attribute_name='concept_id')

            alter_op = dbops.AlterCompositeType(record_name, neg_conditions=(cond,))
            typecol = dbops.Column(name='concept_id', type='integer')
            alter_op.add_command(dbops.AlterCompositeTypeAddAttribute(typecol))

            cmd.add_command(alter_op)

            cols = type_mech.get_table_columns(table_name, connection=context.db)

            for col in cols.values():
                col_name = col['column_name']

                if col_name == 'concept_id':
                    continue

                link = links_by_col_name[col_name]

                spec_links = links_by_generic_name[link['name']]

                loading = caos.types.EagerLoading

                for spec_link in spec_links:
                    ld = caos.types.PointerLoading(spec_link['loading']) \
                                     if spec_link['loading'] else None

                    if ld == caos.types.LazyLoading:
                        loading = caos.types.LazyLoading
                        break

                if loading != caos.types.EagerLoading:
                    continue

                col = dbops.Column(name=col['column_name'],
                                   type=col['column_type_formatted'],
                                   required=col['column_required'],
                                   comment=link['name'])

                cond = dbops.CompositeTypeAttributeExists(type_name=record_name,
                                                          attribute_name=col.name)
                alter_op = dbops.AlterCompositeType(record_name, neg_conditions=(cond,))
                alter_op.add_command(dbops.AlterCompositeTypeAddAttribute(col))

                cmd.add_command(alter_op)

        for group in commands.values():
            group.execute(context)

        atom_links_w_table = []

        for link in links_list:
            if not link['source_id']:
                continue

            concept = concepts[link['source_id']]

            cname = caos.Name(concept['name'])
            generic_link_name = caos.Name(link['base'][0])

            record_name = common.concept_name_to_record_name(cname, catenate=False)
            table_name = common.concept_name_to_table_name(cname, catenate=False)

            if link['target_id'] in atoms:
                parents = set()

                parent_names = [generic_link_name]

                while parent_names:
                    parent_name = parent_names.pop()
                    parent = links_by_name[parent_name]
                    parents.add(parent['id'])
                    parent_names.extend(parent['base'])

                cols = type_mech.get_table_columns(table_name, connection=context.db)
                colname = common.caos_name_to_pg_name(generic_link_name)
                col = cols[colname]

                col = dbops.Column(name=col['column_name'],
                                   type=col['column_type_formatted'],
                                   required=col['column_required'])

                if parents & set(link_props_by_link):
                    atom_links_w_table.append((link, col))

        for link, link_col in atom_links_w_table:
            new_table_name = common.link_name_to_table_name(caos.Name(link['name']), catenate=False)
            base_table_name = common.link_name_to_table_name(caos.Name(link['base'][0]),
                                                             catenate=False)

            constraints = []
            columns = type_mech.get_table_columns(base_table_name, connection=context.db)
            columns = [dbops.Column(name=c['column_name'], type=c['column_type_formatted'],
                                    required=c['column_required'], default=c['column_default'])
                       for c in columns.values()]

            src_col = common.caos_name_to_pg_name('semantix.caos.builtins.source')
            tgt_col = common.caos_name_to_pg_name('semantix.caos.builtins.target')

            constraints.append(dbops.UniqueConstraint(table_name=new_table_name,
                                                      columns=[src_col, tgt_col, 'link_type_id']))

            link_col.name = common.caos_name_to_pg_name('semantix.caos.builtins.target@atom')
            link_col.required = False
            columns.append(link_col)

            table = dbops.Table(name=new_table_name)
            table.add_columns(columns)
            table.constraints = constraints
            table.bases = [base_table_name]

            cond = dbops.TableExists(name=new_table_name)
            ct = dbops.CreateTable(table=table, neg_conditions=(cond,))

            index_name = common.caos_name_to_pg_name(str(link['name'])  + 'target_id_default_idx')
            index = dbops.Index(index_name, new_table_name, unique=False)
            index.add_columns([tgt_col])

            cond = dbops.IndexExists(index_name=(new_table_name[0], index_name))
            ci = dbops.CreateIndex(index, neg_conditions=(cond,))

            c = dbops.CommandGroup()

            c.add_command(ct)
            c.add_command(ci)

            qtext = '''
                SELECT {cols} FROM {table} WHERE link_type_id = {link_id}
            '''.format(cols=','.join((common.qname(c.name) if c.name != 'semantix.caos.builtins.target@atom' else 'NULL') for c in table.columns()),
                       table=common.qname(*base_table_name), link_id=link['id'])
            copy = dbops.Insert(table=table, records=dbops.Query(text=qtext))

            c.add_command(copy)

            base_table = dbops.Table(name=base_table_name)
            delete = dbops.Delete(table=base_table,
                                  condition=[('link_type_id', link['id'])],
                                  include_children=False)

            c.add_command(delete)

            c.execute(context)

    @classmethod
    def update_backend_info(cls):
        backendinfotable = deltadbops.BackendInfoTable()
        record = backendinfotable.record()
        record.format_version = BACKEND_FORMAT_VERSION
        condition = [('format_version', '<', BACKEND_FORMAT_VERSION)]
        return dbops.Merge(table=backendinfotable, record=record, condition=condition)
