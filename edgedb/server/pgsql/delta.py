##
# Copyright (c) 2008-2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import itertools
import pickle
import postgresql.string
import re

from metamagic import caos
from metamagic.caos import proto
from metamagic.caos import delta as delta_cmds
from metamagic.caos import caosql
from metamagic.caos import types as caos_types
from metamagic.caos import objects as caos_objects
from metamagic.caos.objects import geo as geo_objects

from metamagic import json

from metamagic.utils import datastructures
from metamagic.utils.debug import debug
from metamagic.utils.algos.persistent_hash import persistent_hash
from metamagic.utils import markup
from importkit.import_ import get_object

from metamagic.caos.backends.pgsql import common
from metamagic.caos.backends.pgsql import dbops, deltadbops, features

from . import ast as pg_ast
from . import codegen
from . import datasources
from . import parser
from . import schemamech
from . import transformer
from . import types


BACKEND_FORMAT_VERSION = 25


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
    op_priority = 0

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._type_mech = schemamech.TypeMech()

    def _serialize_refs(self, value):
        if isinstance(value, caos.proto.PrototypeRef):
            result = value.prototype_name

        elif isinstance(value, proto.PrototypeOrNativeClassRefList):
            result = []

            for v in value:
                if isinstance(v, proto.PrototypeRef):
                    result.append(v.prototype_name)
                else:
                    result.append(v.class_name)

        elif isinstance(value, (proto.PrototypeSet, proto.PrototypeList)):
            result = [v.prototype_name for v in value]

        else:
            result = value

        return result

    def fill_record(self, schema, rec=None, obj=None):
        updates = {}

        myrec = self.table.record()

        if not obj:
            fields = self.get_struct_properties(include_old_value=True)

            for name, value in fields.items():
                if name == 'bases':
                    name = 'base'

                v0 = self._serialize_refs(value[0])
                v1 = self._serialize_refs(value[1])

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

    def pack_default(self, value):
        if value is not None:
            vals = []
            for item in value:
                if isinstance(item, caos_types.ExpressionText):
                    valtype = 'expr'
                else:
                    valtype = 'literal'
                vals.append({'type': valtype, 'value': item})
            result = json.dumps(vals)
        else:
            result = None
        return result

    def create_object(self, schema, prototype):
        rec, updates = self.fill_record(schema)
        self.pgops.add(dbops.Insert(table=self.table, records=[rec], priority=self.op_priority))
        return updates

    def update(self, meta, context):
        orig_proto = self.original_proto
        updaterec, updates = self.fill_record(meta)

        if updaterec:
            condition = [('name', str(orig_proto.name))]
            self.pgops.add(dbops.Update(table=self.table, record=updaterec, condition=condition))

        return updates

    def rename(self, meta, context, old_name, new_name):
        updaterec = self.table.record(name=str(new_name))
        condition = [('name', str(old_name))]
        self.pgops.add(dbops.Update(table=self.table, record=updaterec, condition=condition))

    def delete(self, meta, context, proto):
        self.pgops.add(dbops.Delete(table=self.table, condition=[('name', str(proto.name))]))


class CreateNamedPrototype(NamedPrototypeMetaCommand):
    def apply(self, meta, context):
        obj = self.__class__.get_adaptee().apply(self, meta, context)
        NamedPrototypeMetaCommand.apply(self, meta, context)
        updates = self.create_object(meta, obj)
        self.updates = updates
        return obj


class RenameNamedPrototype(NamedPrototypeMetaCommand):
    def apply(self, meta, context):
        obj = self.__class__.get_adaptee().apply(self, meta, context)
        NamedPrototypeMetaCommand.apply(self, meta, context)
        self.rename(meta, context, self.prototype_name, self.new_name)
        return obj


class RebaseNamedPrototype(NamedPrototypeMetaCommand):
    def apply(self, meta, context):
        obj = self.__class__.get_adaptee().apply(self, meta, context)
        NamedPrototypeMetaCommand.apply(self, meta, context)
        return obj


class AlterNamedPrototype(NamedPrototypeMetaCommand):
    def apply(self, meta, context):
        obj = self.__class__.get_adaptee().apply(self, meta, context)
        NamedPrototypeMetaCommand.apply(self, meta, context)
        self.updates = self.update(meta, context)
        return obj


class DeleteNamedPrototype(NamedPrototypeMetaCommand):
    def apply(self, meta, context):
        obj = self.__class__.get_adaptee().apply(self, meta, context)
        NamedPrototypeMetaCommand.apply(self, meta, context)
        self.delete(meta, context, obj)
        return obj


class AlterPrototypeProperty(MetaCommand, adapts=delta_cmds.AlterPrototypeProperty):
    pass


class AttributeCommand:
    table = deltadbops.AttributeTable()

    def fill_record(self, schema, rec=None, obj=None):
        rec, updates = super().fill_record(schema, rec=rec, obj=obj)

        if rec:
            type = updates.get('type')
            if type:
                rec.type = pickle.dumps(type[1])

        return rec, updates


class CreateAttribute(AttributeCommand,
                      CreateNamedPrototype,
                      adapts=delta_cmds.CreateAttribute):
    pass


class RenameAttribute(AttributeCommand,
                      RenameNamedPrototype,
                      adapts=delta_cmds.RenameAttribute):
    pass


class AlterAttribute(AttributeCommand,
                     AlterNamedPrototype,
                     adapts=delta_cmds.AlterAttribute):
    pass


class DeleteAttribute(AttributeCommand,
                      DeleteNamedPrototype,
                      adapts=delta_cmds.DeleteAttribute):
    pass


class AttributeValueCommand(metaclass=CommandMeta):
    table = deltadbops.AttributeValueTable()
    op_priority = 1

    def fill_record(self, schema, rec=None, obj=None):
        rec, updates = super().fill_record(schema, rec=rec, obj=obj)

        if rec:
            subj = updates.get('subject')
            if subj:
                rec.subject = dbops.Query(
                    '(SELECT id FROM caos.metaobject WHERE name = $1)',
                    [subj[1]], type='integer')

            attribute = updates.get('attribute')
            if attribute:
                rec.attribute = dbops.Query(
                    '(SELECT id FROM caos.metaobject WHERE name = $1)',
                    [attribute[1]], type='integer')

            value = updates.get('value')
            if value:
                rec.value = pickle.dumps(value[1])

        return rec, updates


class CreateAttributeValue(AttributeValueCommand,
                           CreateNamedPrototype,
                           adapts=delta_cmds.CreateAttributeValue):
    pass


class RenameAttributeValue(AttributeValueCommand,
                           RenameNamedPrototype,
                           adapts=delta_cmds.RenameAttributeValue):
    pass


class AlterAttributeValue(AttributeValueCommand,
                          AlterNamedPrototype,
                          adapts=delta_cmds.AlterAttributeValue):
    pass


class DeleteAttributeValue(AttributeValueCommand,
                           DeleteNamedPrototype,
                           adapts=delta_cmds.DeleteAttributeValue):
    pass


class ConstraintCommand(metaclass=CommandMeta):
    table = deltadbops.ConstraintTable()
    op_priority = 3

    def fill_record(self, schema, rec=None, obj=None):
        rec, updates = super().fill_record(schema, rec=rec, obj=obj)

        if rec:
            subj = updates.get('subject')
            if subj:
                rec.subject = dbops.Query(
                    '(SELECT id FROM caos.metaobject WHERE name = $1)',
                    [subj[1]], type='integer')

            for ptn in 'paramtypes', 'inferredparamtypes':
                paramtypes = updates.get(ptn)
                if paramtypes:
                    pt = {}
                    for k, v in paramtypes[1].items():
                        if isinstance(v, proto.Set):
                            if v.element_type:
                                v = 'set<{}>'.format(v.element_type.prototype_name)
                        elif isinstance(v, proto.PrototypeRef):
                            v = v.prototype_name
                        else:
                            msg = 'unexpected type in constraint paramtypes: {}'.format(v)
                            raise ValueError(msg)

                        pt[k] = v

                    setattr(rec, ptn, pt)

            args = updates.get('args')
            rec.args = pickle.dumps(dict(args[1])) if args and args[1] else None

            # Write the original locally-defined expression
            # so that when the schema is introspected the
            # correct finalexpr is restored with prototype
            # inheritance mechanisms.
            rec.finalexpr = rec.localfinalexpr

        return rec, updates


class CreateConstraint(ConstraintCommand, CreateNamedPrototype,
                       adapts=delta_cmds.CreateConstraint):
    def apply(self, protoschema, context):
        constraint = super().apply(protoschema, context)

        subject = constraint.subject

        if subject is not None:
            schemac_to_backendc = schemamech.ConstraintMech.schema_constraint_to_backend_constraint
            bconstr = schemac_to_backendc(subject, constraint, protoschema)

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.create_ops())
            self.pgops.add(op)

        return constraint


class RenameConstraint(ConstraintCommand, RenameNamedPrototype,
                       adapts=delta_cmds.RenameConstraint):
    def apply(self, protoschema, context):
        constr_ctx = context.get(delta_cmds.ConstraintCommandContext)
        assert constr_ctx
        orig_constraint = constr_ctx.original_proto
        schemac_to_backendc = schemamech.ConstraintMech.schema_constraint_to_backend_constraint
        orig_bconstr = schemac_to_backendc(orig_constraint.subject,
                                           orig_constraint, protoschema)

        constraint = super().apply(protoschema, context)

        subject = constraint.subject

        if subject is not None:
            bconstr = schemac_to_backendc(subject, constraint, protoschema)

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.rename_ops(orig_bconstr))
            self.pgops.add(op)

        return constraint


class AlterConstraint(ConstraintCommand, AlterNamedPrototype,
                      adapts=delta_cmds.AlterConstraint):
    def apply(self, protoschema, context):
        constraint = super().apply(protoschema, context)

        subject = constraint.subject

        if subject is not None:
            schemac_to_backendc = \
              schemamech.ConstraintMech.schema_constraint_to_backend_constraint

            bconstr = schemac_to_backendc(subject, constraint, protoschema)

            orig_constraint = self.original_proto
            orig_bconstr = schemac_to_backendc(
                            orig_constraint.subject, orig_constraint,
                            protoschema)

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.alter_ops(orig_bconstr))
            self.pgops.add(op)

        return constraint


class DeleteConstraint(ConstraintCommand, DeleteNamedPrototype,
                       adapts=delta_cmds.DeleteConstraint):
    def apply(self, protoschema, context):
        constraint = super().apply(protoschema, context)

        subject = constraint.subject

        if subject is not None:
            schemac_to_backendc = schemamech.ConstraintMech.schema_constraint_to_backend_constraint
            bconstr = schemac_to_backendc(subject, constraint, protoschema)

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.delete_ops())
            self.pgops.add(op)

        return constraint


class AtomMetaCommand(NamedPrototypeMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = deltadbops.AtomTable()

    def fill_record(self, schema, rec=None, obj=None):
        rec, updates = super().fill_record(schema, rec, obj)
        if rec:
            if rec.base:
                rec.base = str(rec.base[0])

        default = updates.get('default')
        if default:
            if not rec:
                rec = self.table.record()
            rec.default = self.pack_default(default[1])

        return rec, updates

    def alter_atom_type(self, atom, meta, new_type, intent):

        users = []

        for link in meta(type='link'):
            if link.target and link.target.name == atom.name:
                users.append((link.source, link))

        domain_name = common.atom_name_to_domain_name(atom.name, catenate=False)

        new_constraints = atom.local_constraints
        base = types.get_atom_base(meta, atom)

        target_type = new_type

        schemac_to_backendc = schemamech.ConstraintMech.schema_constraint_to_backend_constraint

        if intent == 'alter':
            new_name = domain_name[0], domain_name[1] + '_tmp'
            self.pgops.add(dbops.RenameDomain(domain_name, new_name))
            target_type = common.qname(*domain_name)

            self.pgops.add(dbops.CreateDomain(name=domain_name, base=new_type))

            adapt = deltadbops.SchemaDBObjectMeta.adapt
            for constraint in new_constraints.values():
                bconstr = schemac_to_backendc(atom, constraint, meta)
                op = dbops.CommandGroup(priority=1)
                op.add_command(bconstr.create_ops())
                self.pgops.add(op)

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

        for child_atom in meta(type='atom'):
            if [b.name for b in child_atom.bases] == [atom.name]:
                self.alter_atom_type(child_atom, meta, target_type, 'alter')

        if intent == 'drop' or (intent == 'alter' and not simple_alter):
            self.pgops.add(dbops.DropDomain(domain_name))


class CreateAtom(AtomMetaCommand, adapts=delta_cmds.CreateAtom):
    def apply(self, meta, context=None):
        atom = delta_cmds.CreateAtom.apply(self, meta, context)
        AtomMetaCommand.apply(self, meta, context)

        if atom.issubclass(geo_objects.Geometry):
            schema = dbops.CreateSchema(name='caos_aux_feat_gis')
            feat = deltadbops.EnableFeature(feature=features.GisFeature())

            cond = dbops.TypeExists(('caos_aux_feat_gis', 'geography'))
            cmd = dbops.CommandGroup(neg_conditions=[cond])
            cmd.add_commands([schema, feat])
            self.pgops.add(cmd)

        new_domain_name = common.atom_name_to_domain_name(atom.name, catenate=False)
        base = types.get_atom_base(meta, atom)

        updates = self.create_object(meta, atom)

        self.pgops.add(dbops.CreateDomain(name=new_domain_name, base=base))

        if atom.issubclass(caos_objects.sequence.Sequence):
            seq_name = common.atom_name_to_sequence_name(atom.name, catenate=False)
            self.pgops.add(dbops.CreateSequence(name=seq_name))

        default = updates.get('default')
        if default:
            default = default[1]
            if len(default) > 0 and \
                not isinstance(default[0], caos_types.ExpressionText):
                # We only care to support literal defaults here.  Supporting
                # defaults based on queries has no sense on the database level
                # since the database forbids queries for DEFAULT and pre-
                # calculating the value does not make sense either since the
                # whole point of query defaults is for them to be dynamic.
                self.pgops.add(dbops.AlterDomainAlterDefault(
                    name=new_domain_name, default=default[0]))

        return atom


class RenameAtom(AtomMetaCommand, adapts=delta_cmds.RenameAtom):
    def apply(self, meta, context=None):
        proto = delta_cmds.RenameAtom.apply(self, meta, context)
        AtomMetaCommand.apply(self, meta, context)

        domain_name = common.atom_name_to_domain_name(self.prototype_name, catenate=False)
        new_domain_name = common.atom_name_to_domain_name(self.new_name, catenate=False)

        self.pgops.add(dbops.RenameDomain(name=domain_name, new_name=new_domain_name))
        self.rename(meta, context, self.prototype_name, self.new_name)

        if proto.issubclass(caos_objects.sequence.Sequence):
            seq_name = common.atom_name_to_sequence_name(self.prototype_name, catenate=False)
            new_seq_name = common.atom_name_to_sequence_name(self.new_name, catenate=False)

            self.pgops.add(dbops.RenameSequence(name=seq_name, new_name=new_seq_name))

        return proto


class AlterAtom(AtomMetaCommand, adapts=delta_cmds.AlterAtom):
    def apply(self, meta, context=None):
        old_atom = meta.get(self.prototype_name).copy()
        new_atom = delta_cmds.AlterAtom.apply(self, meta, context)
        AtomMetaCommand.apply(self, meta, context)

        updaterec, updates = self.fill_record(meta)

        if updaterec:
            condition = [('name', str(new_atom.name))]
            self.pgops.add(dbops.Update(table=self.table, record=updaterec,
                                        condition=condition))

        self.alter_atom(self, meta, context, old_atom, new_atom,
                                                       updates=updates)

        return new_atom

    @classmethod
    def alter_atom(cls, op, meta, context, old_atom, new_atom, in_place=True,
                                                               updates=None):

        old_base = types.get_atom_base(meta, old_atom)
        base = types.get_atom_base(meta, new_atom)

        domain_name = common.atom_name_to_domain_name(new_atom.name,
                                                      catenate=False)

        new_type = None
        type_intent = 'alter'

        if not new_type and old_base != base:
            new_type = base

        if new_type:
            # The change of the underlying data type for domains is a complex problem.
            # There is no direct way in PostgreSQL to change the base type of a domain.
            # Instead, a new domain must be created, all users of the old domain altered
            # to use the new one, and then the old domain dropped.  Obviously this
            # recurses down to every child domain.
            #
            if in_place:
                op.alter_atom_type(new_atom, meta, new_type, intent=type_intent)

        if type_intent != 'drop':
            if updates:
                default_delta = updates.get('default')
                if default_delta:
                    default_delta = default_delta[1]

                    if not default_delta or \
                           isinstance(default_delta[0],
                                      caos_types.ExpressionText):
                        new_default = None
                    else:
                        new_default = default_delta[0]

                    adad = dbops.AlterDomainAlterDefault(name=domain_name, default=new_default)
                    op.pgops.add(adad)


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

        if atom.issubclass(caos_objects.sequence.Sequence):
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
                op = dbops.DropIndex(index, conditions=(cond,))
                self.pgops.add(op)
                op = dbops.CreateIndex(index=index)
                self.pgops.add(op)


class CompositePrototypeMetaCommand(NamedPrototypeMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table_name = None
        self._multicommands = {}
        self.update_search_indexes = None

    def _get_multicommand(self, context, cmdtype, object_name, *, priority=0, force_new=False,
                                                                  manual=False, cmdkwargs={}):
        key = (object_name, priority, frozenset(cmdkwargs.items()))

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

    def get_alter_table(self, context, priority=0, force_new=False, contained=False, manual=False,
                                       table_name=None):

        tabname = table_name if table_name else self.table_name

        if not tabname:
            assert self.__class__.context_class
            ctx = context.get(self.__class__.context_class)
            assert ctx
            tabname = common.get_table_name(ctx.proto, catenate=False)
            if table_name is None:
                self.table_name = tabname

        return self._get_multicommand(context, dbops.AlterTable, tabname,
                                      priority=priority,
                                      force_new=force_new, manual=manual,
                                      cmdkwargs={'contained': contained})

    def attach_alter_table(self, context):
        self._attach_multicommand(context, dbops.AlterTable)

    def rename(self, meta, context, old_name, new_name, obj=None):
        super().rename(meta, context, old_name, new_name)

        if obj is not None and isinstance(obj, caos.types.ProtoLink):
            old_table_name = common.link_name_to_table_name(old_name, catenate=False)
            new_table_name = common.link_name_to_table_name(new_name, catenate=False)
        else:
            old_table_name = common.concept_name_to_table_name(old_name, catenate=False)
            new_table_name = common.concept_name_to_table_name(new_name, catenate=False)

        cond = dbops.TableExists(name=old_table_name)

        if old_name.module != new_name.module:
            self.pgops.add(dbops.AlterTableSetSchema(old_table_name, new_table_name[0],
                                                     conditions=(cond,)))
            old_table_name = (new_table_name[0], old_table_name[1])

            cond = dbops.TableExists(name=old_table_name)

        if old_name.name != new_name.name:
            self.pgops.add(dbops.AlterTableRenameTo(old_table_name, new_table_name[1],
                                                    conditions=(cond,)))

    def search_index_add(self, host, pointer, meta, context):
        if self.update_search_indexes is None:
            self.update_search_indexes = UpdateSearchIndexes(host)

    def search_index_alter(self, host, pointer, meta, context):
        if self.update_search_indexes is None:
            self.update_search_indexes = UpdateSearchIndexes(host)

    def search_index_delete(self, host, pointer, meta, context):
        if self.update_search_indexes is None:
            self.update_search_indexes = UpdateSearchIndexes(host)

    @classmethod
    def get_source_and_pointer_ctx(cls, meta, context):
        if context:
            concept = context.get(delta_cmds.ConceptCommandContext)
            link = context.get(delta_cmds.LinkCommandContext)
        else:
            concept = link = None

        if concept:
            source, pointer = concept, link
        elif link:
            property = context.get(delta_cmds.LinkPropertyCommandContext)
            source, pointer = link, property
        else:
            source = pointer = None

        return source, pointer

    def affirm_pointer_defaults(self, source, meta, context):
        for pointer_name, pointer in source.pointers.items():
            # XXX pointer_storage_info?
            if (pointer.generic() or not pointer.atomic() or not pointer.singular()
                                                          or not pointer.default):
                continue

            default = None
            ld = list(filter(lambda i: not isinstance(i, caos_types.ExpressionText),
                             pointer.default))
            if ld:
                default = ld[0]

            if default is not None:
                alter_table = self.get_alter_table(context, priority=3, contained=True)
                column_name = common.caos_name_to_pg_name(pointer_name)
                alter_table.add_operation(dbops.AlterTableAlterColumnDefault(column_name=column_name,
                                                                             default=default))

    def adjust_pointer_storage(self, orig_pointer, pointer, meta, context):
        old_ptr_stor_info = types.get_pointer_storage_info(
                                orig_pointer, schema=meta)
        new_ptr_stor_info = types.get_pointer_storage_info(
                                pointer, schema=meta)

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

                if old_ptr_stor_info.table_type == 'concept':
                    pat = self.get_alter_table(context, manual=True)

                    # Moved from concept table to link table
                    col = dbops.Column(name=old_ptr_stor_info.column_name,
                                       type=old_ptr_stor_info.column_type)
                    at.add_command(dbops.AlterTableDropColumn(col))

                    newcol = dbops.Column(name=new_ptr_stor_info.column_name,
                                          type=new_ptr_stor_info.column_type)

                    cond = dbops.ColumnExists(new_ptr_stor_info.table_name,
                                              column_name=newcol.name)

                    pat.add_command((dbops.AlterTableAddColumn(newcol), None, (cond,)))
                else:
                    otabname = common.get_table_name(orig_pointer, catenate=False)
                    pat = self.get_alter_table(context, manual=True, table_name=otabname)

                    oldcol = dbops.Column(name=old_ptr_stor_info.column_name,
                                          type=old_ptr_stor_info.column_type)

                    if oldcol.name != 'metamagic.caos.builtins.target':
                        pat.add_command(dbops.AlterTableDropColumn(oldcol))

                    # Moved from link to concept
                    cols = self.get_columns(pointer, meta)

                    for col in cols:
                        cond = dbops.ColumnExists(new_ptr_stor_info.table_name,
                                                  column_name=col.name)
                        op = (dbops.AlterTableAddColumn(col), None, (cond,))
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

    def apply_base_delta(self, orig_source, source, meta, context):
        realm = context.get(delta_cmds.RealmCommandContext)
        orig_source.bases = [realm.op._renames.get(b, b) for b in orig_source.bases]

        dropped_bases = {b.name for b in orig_source.bases} - {b.name for b in source.bases}

        if isinstance(source, caos.types.ProtoConcept):
            nameconv = common.concept_name_to_table_name
            source_ctx = context.get(delta_cmds.ConceptCommandContext)
            ptr_cmd = delta_cmds.CreateLink
        else:
            nameconv = common.link_name_to_table_name
            source_ctx = context.get(delta_cmds.LinkCommandContext)
            ptr_cmd = delta_cmds.CreateLinkProperty

        alter_table = source_ctx.op.get_alter_table(context, force_new=True)

        if isinstance(source, caos.types.ProtoConcept) \
                        or source_ctx.op.has_table(source, meta):

            source.acquire_ancestor_inheritance(meta)
            orig_source.acquire_ancestor_inheritance(meta)

            created_ptrs = set()
            for ptr in source_ctx.op(ptr_cmd):
                created_ptrs.add(ptr.prototype_name)

            inherited_aptrs = set()

            for base in source.bases:
                for ptr in base.pointers.values():
                    if ptr.atomic():
                        inherited_aptrs.add(ptr.normal_name())

            added_inh_ptrs = inherited_aptrs - {p.normal_name() for p in orig_source.pointers.values()}

            for added_ptr in added_inh_ptrs - created_ptrs:
                ptr = source.pointers[added_ptr]
                ptr_stor_info = types.get_pointer_storage_info(
                                    ptr, schema=meta)

                if ptr_stor_info.table_type == 'concept':
                    col = dbops.Column(name=ptr_stor_info.column_name,
                                       type=ptr_stor_info.column_type,
                                       required=ptr.required)
                    cond = dbops.ColumnExists(table_name=source_ctx.op.table_name,
                                              column_name=ptr_stor_info.column_name)
                    alter_table.add_operation((dbops.AlterTableAddColumn(col), None, (cond,)))

            if dropped_bases:
                alter_table_drop_parent = source_ctx.op.get_alter_table(context, force_new=True)

                for dropped_base in dropped_bases:
                    parent_table_name = nameconv(caos.name.Name(dropped_base), catenate=False)
                    op = dbops.AlterTableDropParent(parent_name=parent_table_name)
                    alter_table_drop_parent.add_operation(op)

                dropped_ptrs = set(orig_source.pointers) - set(source.pointers)

                if dropped_ptrs:
                    alter_table_drop_ptr = source_ctx.op.get_alter_table(context, force_new=True)

                    for dropped_ptr in dropped_ptrs:
                        ptr = orig_source.pointers[dropped_ptr]
                        ptr_stor_info = types.get_pointer_storage_info(
                                            ptr, schema=meta)

                        if ptr_stor_info.table_type == 'concept':
                            col = dbops.Column(name=ptr_stor_info.column_name,
                                               type=ptr_stor_info.column_type,
                                               required=ptr.required)

                            cond = dbops.ColumnExists(table_name=ptr_stor_info.table_name,
                                                      column_name=ptr_stor_info.column_name)
                            op = dbops.AlterTableDropColumn(col)
                            alter_table_drop_ptr.add_command((op, (cond,), ()))

            current_bases = list(datastructures.OrderedSet(b.name for b in orig_source.bases)
                                                                             - dropped_bases)
            new_bases = [b.name for b in source.bases]
            for i, bn in enumerate(new_bases):
                if i >= len(current_bases) or bn != current_bases[i]:
                    break

            old_base_order = current_bases[i:]
            new_base_order = new_bases[i:]


            if new_base_order:
                table_name = nameconv(source.name, catenate=False)
                alter_table_drop_parent = source_ctx.op.get_alter_table(context, force_new=True)
                alter_table_add_parent = source_ctx.op.get_alter_table(context, force_new=True)

                for base in old_base_order:
                    parent_table_name = nameconv(caos.name.Name(base), catenate=False)
                    cond = dbops.TableInherits(table_name, parent_table_name)
                    op = dbops.AlterTableDropParent(parent_name=parent_table_name)
                    alter_table_drop_parent.add_operation((op, [cond], None))

                for added_base in new_base_order:
                    parent_table_name = nameconv(caos.name.Name(added_base), catenate=False)
                    cond = dbops.TableInherits(table_name, parent_table_name)
                    op = dbops.AlterTableAddParent(parent_name=parent_table_name)
                    alter_table_add_parent.add_operation((op, None, [cond]))


class SourceIndexCommand(PrototypeMetaCommand):
    pass


class CreateSourceIndex(SourceIndexCommand, adapts=delta_cmds.CreateSourceIndex):
    def apply(self, meta, context=None):
        index = delta_cmds.CreateSourceIndex.apply(self, meta, context)
        SourceIndexCommand.apply(self, meta, context)

        source = context.get(delta_cmds.LinkCommandContext)
        if not source:
            source = context.get(delta_cmds.ConceptCommandContext)
        table_name = common.get_table_name(source.proto, catenate=False)
        ir = caosql.compile_fragment_to_ir(index.expr, meta,
                                           location='selector')

        ircompiler = transformer.SimpleIRCompiler()
        sql_tree = ircompiler.transform(ir, meta, local=True)
        sql_expr = codegen.SQLSourceGenerator.to_source(sql_tree)
        if isinstance(sql_tree, pg_ast.SequenceNode):
            # Trim the parentheses to avoid PostgreSQL choking on double parentheses.
            # since it expects only a single set around the column list.
            #
            sql_expr = sql_expr[1:-1]
        index_name = '{}_reg_idx'.format(index.name)
        pg_index = dbops.Index(name=index_name, table_name=table_name,
                               expr=sql_expr, unique=False,
                               inherit=True,
                               metadata={'schemaname': index.name})
        self.pgops.add(dbops.CreateIndex(pg_index, priority=3))

        return index


class RenameSourceIndex(SourceIndexCommand, adapts=delta_cmds.RenameSourceIndex):
    def apply(self, meta, context):
        index = delta_cmds.RenameSourceIndex.apply(self, meta, context)
        SourceIndexCommand.apply(self, meta, context)

        subject = context.get(delta_cmds.LinkCommandContext)
        if not subject:
            subject = context.get(delta_cmds.ConceptCommandContext)
        orig_table_name = common.get_table_name(subject.original_proto,
                                                catenate=False)

        index_ctx = context.get(delta_cmds.SourceIndexCommandContext)
        new_index_name = '{}_reg_idx'.format(index.name)

        orig_idx = index_ctx.original_proto
        orig_idx_name = '{}_reg_idx'.format(orig_idx.name)
        orig_pg_idx = dbops.Index(name=orig_idx_name,
                                  table_name=orig_table_name,
                                  inherit=True,
                                  metadata={'schemaname': index.name})

        rename = dbops.RenameIndex(orig_pg_idx, new_name=new_index_name)
        self.pgops.add(rename)

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
            index_name = '{}_reg_idx'.format(index.name)
            index = dbops.Index(name=index_name, table_name=table_name,
                                inherit=True)
            index_exists = dbops.IndexExists((table_name[0],
                                              index.name_in_catalog))
            self.pgops.add(dbops.DropIndex(index, priority=3,
                                           conditions=(index_exists,)))

        return index


class ConceptMetaCommand(CompositePrototypeMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = deltadbops.ConceptTable()

    def fill_record(self, schema, rec=None):
        rec, updates = super().fill_record(schema, rec)
        if rec and rec.custombases:
            rec.custombases = tuple(str(b) for b in rec.custombases)
        return rec, updates


class CreateConcept(ConceptMetaCommand, adapts=delta_cmds.CreateConcept):
    def apply(self, meta, context=None):
        concept_props = self.get_struct_properties(include_old_value=False)
        is_virtual = concept_props['is_virtual']

        if not is_virtual:
            new_table_name = common.concept_name_to_table_name(self.prototype_name, catenate=False)
            self.table_name = new_table_name
            concept_table = dbops.Table(name=new_table_name)
            self.pgops.add(dbops.CreateTable(table=concept_table))

            alter_table = self.get_alter_table(context)

        concept = delta_cmds.CreateConcept.apply(self, meta, context)
        ConceptMetaCommand.apply(self, meta, context)

        fields = self.create_object(meta, concept)

        if not is_virtual:
            constr_name = common.caos_name_to_pg_name(self.prototype_name + '.concept_id_check')

            constr_expr = dbops.Query("""
                SELECT 'concept_id = ' || id FROM caos.concept WHERE name = $1
            """, [concept.name], type='text')

            cid_constraint = dbops.CheckConstraint(self.table_name, constr_name, constr_expr,
                                                                                 inherit=False)
            alter_table.add_operation(dbops.AlterTableAddConstraint(cid_constraint))

            cid_col = dbops.Column(name='concept_id', type='integer', required=True)

            if concept.name == 'metamagic.caos.builtins.BaseObject':
                alter_table.add_operation(dbops.AlterTableAddColumn(cid_col))

            if not concept.is_virtual:
                constraint = dbops.PrimaryKey(table_name=alter_table.name,
                                              columns=['metamagic.caos.builtins.id'])
                alter_table.add_operation(dbops.AlterTableAddConstraint(constraint))

            bases = (common.concept_name_to_table_name(caos.Name(p), catenate=False)
                     for p in fields['base'][1] if proto.Concept.is_prototype(meta, p))
            concept_table.bases = list(bases)

            self.affirm_pointer_defaults(concept, meta, context)

            self.attach_alter_table(context)

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

        realm.op._renames[concept.original_proto] = proto

        concept.op.attach_alter_table(context)

        self.rename(meta, context, self.prototype_name, self.new_name)

        new_table_name = common.concept_name_to_table_name(self.new_name, catenate=False)
        concept_table = dbops.Table(name=new_table_name)
        self.pgops.add(dbops.Comment(object=concept_table, text=self.new_name))

        concept.op.table_name = common.concept_name_to_table_name(self.new_name, catenate=False)

        # Need to update all bits that reference concept name

        old_constr_name = common.caos_name_to_pg_name(self.prototype_name + '.concept_id_check')
        new_constr_name = common.caos_name_to_pg_name(self.new_name + '.concept_id_check')

        alter_table = self.get_alter_table(context, manual=True)
        rc = dbops.AlterTableRenameConstraintSimple(
                    alter_table.name, old_name=old_constr_name,
                                      new_name=new_constr_name)
        self.pgops.add(rc)

        self.table_name = common.concept_name_to_table_name(self.new_name, catenate=False)

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

        updaterec, updates = self.fill_record(meta)

        if updaterec:
            condition = [('name', str(concept.name))]
            self.pgops.add(dbops.Update(table=self.table, record=updaterec, condition=condition))

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

        self.delete(meta, context, concept)

        self.pgops.add(dbops.DropTable(name=old_table_name, priority=3))

        return concept


class ActionCommand:
    table = deltadbops.ActionTable()


class CreateAction(CreateNamedPrototype,
                                 ActionCommand,
                                 adapts=delta_cmds.CreateAction):
    pass


class RenameAction(RenameNamedPrototype,
                                 ActionCommand,
                                 adapts=delta_cmds.RenameAction):
    pass


class AlterAction(AlterNamedPrototype,
                                ActionCommand,
                                adapts=delta_cmds.AlterAction):
    pass


class DeleteAction(DeleteNamedPrototype,
                                 ActionCommand,
                                 adapts=delta_cmds.DeleteAction):
    pass


class EventCommand(metaclass=CommandMeta):
    table = deltadbops.EventTable()

    def fill_record(self, schema, rec=None, obj=None):
        rec, updates = super().fill_record(schema, rec=rec, obj=obj)

        if rec:
            actions = updates.get('allowed_actions')
            if actions:
                rec.allowed_actions = dbops.Query(
                    '''(SELECT array_agg(id)
                        FROM caos.metaobject
                        WHERE name = any($1::text[]))''',
                    [actions[1]], type='integer[]')

        return rec, updates


class CreateEvent(EventCommand,
                                CreateNamedPrototype,
                                adapts=delta_cmds.CreateEvent):
    pass


class RenameEvent(EventCommand,
                                RenameNamedPrototype,
                                adapts=delta_cmds.RenameEvent):
    pass


class RebaseEvent(EventCommand,
                                RebaseNamedPrototype,
                                adapts=delta_cmds.RebaseEvent):
    pass


class AlterEvent(EventCommand,
                               AlterNamedPrototype,
                               adapts=delta_cmds.AlterEvent):
    pass


class DeleteEvent(EventCommand,
                                DeleteNamedPrototype,
                                adapts=delta_cmds.DeleteEvent):
    pass


class PolicyCommand(metaclass=CommandMeta):
    table = deltadbops.PolicyTable()
    op_priority = 2

    def fill_record(self, schema, rec=None, obj=None):
        rec, updates = super().fill_record(schema, rec=rec, obj=obj)

        if rec:
            subj = updates.get('subject')
            if subj:
                rec.subject = dbops.Query(
                    '(SELECT id FROM caos.metaobject WHERE name = $1)',
                    [subj[1]], type='integer')

            event = updates.get('event')
            if event:
                rec.event = dbops.Query(
                    '(SELECT id FROM caos.metaobject WHERE name = $1)',
                    [event[1]], type='integer')

            actions = updates.get('actions')
            if actions:
                rec.actions = dbops.Query(
                    '''(SELECT array_agg(id)
                        FROM caos.metaobject
                        WHERE name = any($1::text[]))''',
                    [actions[1]], type='integer[]')

        return rec, updates


class CreatePolicy(PolicyCommand,
                                 CreateNamedPrototype,
                                 adapts=delta_cmds.CreatePolicy):
    pass


class RenamePolicy(PolicyCommand,
                                 RenameNamedPrototype,
                                 adapts=delta_cmds.RenamePolicy):
    pass


class AlterPolicy(PolicyCommand,
                                AlterNamedPrototype,
                                adapts=delta_cmds.AlterPolicy):
    pass


class DeletePolicy(PolicyCommand,
                                 DeleteNamedPrototype,
                                 adapts=delta_cmds.DeletePolicy):
    pass


class ScheduleLinkMappingUpdate(MetaCommand):
    pass


class CancelLinkMappingUpdate(MetaCommand):
    pass


class PointerMetaCommand(MetaCommand):

    def get_host(self, meta, context):
        if context:
            link = context.get(delta_cmds.LinkCommandContext)
            if link and isinstance(self, delta_cmds.LinkPropertyCommand):
                return link
            concept = context.get(delta_cmds.ConceptCommandContext)
            if concept:
                return concept

    def record_metadata(self, pointer, old_pointer, meta, context):
        rec, updates = self.fill_record(meta)

        if updates:
            if not rec:
                rec = self.table.record()

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

        default = updates.get('default')
        if default:
            if not rec:
                rec = self.table.record()
            rec.default = self.pack_default(default[1])

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

    def get_pointer_default(self, link, meta, context):
        default = self.updates.get('default')
        default_value = None

        if default:
            default = default[1]
            if default:
                ld = list(filter(lambda i: not isinstance(i, caos_types.ExpressionText),
                                 default))
                if ld:
                    default_value = postgresql.string.quote_literal(str(ld[0]))

        return default_value

    def alter_pointer_default(self, pointer, meta, context):
        default = self.updates.get('default')
        if default:
            default = default[1]

            new_default = None
            have_new_default = True

            if not default:
                new_default = None
            else:
                ld = list(filter(lambda i: not isinstance(i, caos_types.ExpressionText),
                                 default))
                if ld:
                    new_default = ld[0]
                else:
                    have_new_default = False

            if have_new_default:
                source_ctx, pointer_ctx = CompositePrototypeMetaCommand.\
                                                get_source_and_pointer_ctx(meta, context)
                alter_table = source_ctx.op.get_alter_table(context, contained=True, priority=3)
                column_name = common.caos_name_to_pg_name(pointer.normal_name())
                alter_table.add_operation(dbops.AlterTableAlterColumnDefault(column_name=column_name,
                                                                             default=new_default))

    def get_columns(self, pointer, meta, default=None):
        ptr_stor_info = types.get_pointer_storage_info(pointer, schema=meta)
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
                if new_name == 'metamagic.caos.builtins.target' and pointer.atomic():
                    new_name += '@atom'

                if new_name.endswith('caos.builtins.source') and not host.proto.generic():
                    pass
                else:
                    old_col_name = common.caos_name_to_pg_name(old_name)
                    new_col_name = common.caos_name_to_pg_name(new_name)

                    ptr_stor_info = types.get_pointer_storage_info(
                                        pointer, schema=meta)

                    is_a_column = (
                        (ptr_stor_info.table_type == 'concept'
                                and isinstance(host.proto, proto.Concept))
                        or (ptr_stor_info.table_type == 'link'
                                and isinstance(host.proto, proto.Link))
                    )

                    if is_a_column:
                        table_name = common.get_table_name(host.proto, catenate=False)
                        cond = [dbops.ColumnExists(table_name=table_name, column_name=old_col_name)]
                        rename = dbops.AlterTableRenameColumn(table_name, old_col_name, new_col_name,
                                                              conditions=cond)
                        self.pgops.add(rename)

                        tabcol = dbops.TableColumn(table_name=table_name,
                                                   column=dbops.Column(name=new_col_name, type='str'))
                        self.pgops.add(dbops.Comment(tabcol, new_name))

        rec = self.table.record()
        rec.name = str(self.new_name)
        self.pgops.add(dbops.Update(table=self.table, record=rec,
                                    condition=[('name', str(self.prototype_name))], priority=1))

    @classmethod
    def has_table(cls, link, meta):
        if link.is_pure_computable():
            return False
        elif link.generic():
            if link.name == 'metamagic.caos.builtins.link':
                return True
            elif link.has_user_defined_properties():
                return True
            else:
                for l in link.children(meta):
                    if not l.generic():
                        ptr_stor_info = types.get_pointer_storage_info(
                                            l, resolve_type=False)
                        if ptr_stor_info.table_type == 'link':
                            return True

                return False
        else:
            return not link.atomic() or not link.singular() or link.has_user_defined_properties()


class LinkMetaCommand(CompositePrototypeMetaCommand, PointerMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = deltadbops.LinkTable()

    @classmethod
    def _create_table(cls, link, meta, context, conditional=False,
                           create_bases=True, create_children=True):
        new_table_name = common.get_table_name(link, catenate=False)

        create_c = dbops.CommandGroup()

        constraints = []
        columns = []

        src_col = common.caos_name_to_pg_name('metamagic.caos.builtins.source')
        tgt_col = common.caos_name_to_pg_name('metamagic.caos.builtins.target')

        if link.name == 'metamagic.caos.builtins.link':
            columns.append(dbops.Column(name=src_col, type='uuid', required=True,
                                        comment='metamagic.caos.builtins.source'))
            columns.append(dbops.Column(name=tgt_col, type='uuid', required=False,
                                        comment='metamagic.caos.builtins.target'))
            columns.append(dbops.Column(name='link_type_id', type='integer', required=True))

        constraints.append(dbops.UniqueConstraint(table_name=new_table_name,
                                                  columns=[src_col, tgt_col, 'link_type_id']))

        if not link.generic() and link.atomic():
            try:
                tgt_prop = link.pointers['metamagic.caos.builtins.target']
            except KeyError:
                pass
            else:
                tgt_ptr = types.get_pointer_storage_info(
                                tgt_prop, schema=meta)
                columns.append(dbops.Column(name=tgt_ptr.column_name,
                                            type=tgt_ptr.column_type))

        table = dbops.Table(name=new_table_name)
        table.add_columns(columns)
        table.constraints = constraints

        if link.bases:
            bases = []

            for parent in link.bases:
                if isinstance(parent, caos.types.ProtoObject):
                    if create_bases:
                        bc = cls._create_table(parent, meta, context,
                                               conditional=True,
                                               create_children=False)
                        create_c.add_command(bc)

                    tabname = common.get_table_name(parent, catenate=False)
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

        c.add_command(dbops.Comment(table, link.name))

        create_c.add_command(c)

        if create_children:
            for l_descendant in link.descendants(meta):
                if cls.has_table(l_descendant, meta):
                    lc = LinkMetaCommand._create_table(l_descendant, meta, context,
                            conditional=True, create_bases=False,
                            create_children=False)
                    create_c.add_command(lc)

        return create_c

    def create_table(self, link, meta, context, conditional=False):
        c = self._create_table(link, meta, context, conditional=conditional)
        self.pgops.add(c)

    def provide_table(self, link, meta, context):
        if not link.generic():
            gen_link = link.bases[0]

            if self.has_table(gen_link, meta):
                self.create_table(gen_link, meta, context, conditional=True)

        if self.has_table(link, meta):
            self.create_table(link, meta, context, conditional=True)

    def schedule_mapping_update(self, link, meta, context):
        if self.has_table(link, meta):
            mapping_indexes = context.get(delta_cmds.RealmCommandContext).op.update_mapping_indexes
            ops = mapping_indexes.links.get(link.name)
            if not ops:
                mapping_indexes.links[link.name] = ops = []
            ops.append((self, link))
            self.pgops.add(ScheduleLinkMappingUpdate())

    def cancel_mapping_update(self, link, meta, context):
        mapping_indexes = context.get(delta_cmds.RealmCommandContext).op.update_mapping_indexes
        mapping_indexes.links.pop(link.name, None)
        self.pgops.add(CancelLinkMappingUpdate())


class CreateLink(LinkMetaCommand, adapts=delta_cmds.CreateLink):
    def apply(self, meta, context=None):
        # Need to do this early, since potential table alters triggered by sub-commands
        # need this.
        link = delta_cmds.CreateLink.apply(self, meta, context)
        self.table_name = common.get_table_name(link, catenate=False)
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

        concept = context.get(delta_cmds.ConceptCommandContext)
        if not concept or not concept.proto.is_virtual:
            rec, updates = self.record_metadata(link, None, meta, context)
            self.updates = updates

        if not link.generic():
            ptr_stor_info = types.get_pointer_storage_info(
                                link, resolve_type=False)

            concept = context.get(delta_cmds.ConceptCommandContext)
            assert concept, "Link command must be run in Concept command context"

            if ptr_stor_info.table_type == 'concept':
                default_value = self.get_pointer_default(link, meta, context)

                cols = self.get_columns(link, meta, default_value)
                table_name = common.get_table_name(concept.proto, catenate=False)
                concept_alter_table = concept.op.get_alter_table(context)

                for col in cols:
                    # The column may already exist as inherited from parent table
                    cond = dbops.ColumnExists(table_name=table_name, column_name=col.name)
                    cmd = dbops.AlterTableAddColumn(col)
                    concept_alter_table.add_operation((cmd, None, (cond,)))

                if default_value is not None:
                    self.alter_pointer_default(link, meta, context)

        if link.generic():
            self.affirm_pointer_defaults(link, meta, context)

        self.attach_alter_table(context)

        concept = context.get(delta_cmds.ConceptCommandContext)
        if not concept or not concept.proto.is_virtual:
            self.pgops.add(dbops.Insert(table=self.table, records=[rec],
                                        priority=1))

        if (not link.generic() and link.mapping != caos.types.ManyToMany
                               and not concept.proto.is_virtual):
            self.schedule_mapping_update(link, meta, context)

        return link


class RenameLink(LinkMetaCommand, adapts=delta_cmds.RenameLink):
    def apply(self, meta, context=None):
        result = delta_cmds.RenameLink.apply(self, meta, context)
        LinkMetaCommand.apply(self, meta, context)

        self.rename_pointer(result, meta, context, self.prototype_name, self.new_name)

        self.attach_alter_table(context)

        if result.generic():
            link_cmd = context.get(delta_cmds.LinkCommandContext)
            assert link_cmd

            self.rename(meta, context, self.prototype_name, self.new_name, obj=result)
            link_cmd.op.table_name = common.link_name_to_table_name(self.new_name, catenate=False)
        else:
            link_cmd = context.get(delta_cmds.LinkCommandContext)

            if self.has_table(result, meta):
                self.rename(meta, context, self.prototype_name, self.new_name, obj=result)

        return result


class RebaseLink(LinkMetaCommand, adapts=delta_cmds.RebaseLink):
    def apply(self, meta, context):
        result = delta_cmds.RebaseLink.apply(self, meta, context)
        LinkMetaCommand.apply(self, meta, context)

        result.acquire_ancestor_inheritance(meta)

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
            self.updates = updates

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

            if not link.generic():
                self.adjust_pointer_storage(old_link, link, meta, context)

                old_ptr_stor_info = types.get_pointer_storage_info(
                                        old_link, schema=meta)
                ptr_stor_info = types.get_pointer_storage_info(
                                    link, schema=meta)
                if (old_ptr_stor_info.table_type == 'concept'
                        and ptr_stor_info.table_type == 'concept'
                        and link.required != self.old_link.required):
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
            ptr_stor_info = types.get_pointer_storage_info(result, schema=meta)
            concept = context.get(delta_cmds.ConceptCommandContext)

            name = result.normal_name()

            if ptr_stor_info.table_type == 'concept':
                # Only drop the column if the link was not reinherited in the same delta
                if name not in concept.proto.pointers:
                    # This must be a separate so that objects depending
                    # on this column can be dropped correctly.
                    #
                    alter_table = concept.op.get_alter_table(context,
                                                             manual=True,
                                                             priority=2)
                    col = dbops.Column(name=ptr_stor_info.column_name,
                                       type=ptr_stor_info.column_type)
                    cond = dbops.ColumnExists(table_name=concept.op.table_name,
                                              column_name=col.name)
                    col = dbops.AlterTableDropColumn(col)
                    alter_table.add_operation((col, [cond], []))
                    self.pgops.add(alter_table)

        old_table_name = common.get_table_name(result, catenate=False)
        condition = dbops.TableExists(name=old_table_name)
        self.pgops.add(dbops.DropTable(name=old_table_name,
                                       conditions=[condition]))
        self.cancel_mapping_update(result, meta, context)

        if not result.generic() and result.mapping != caos.types.ManyToMany:
            self.schedule_mapping_update(result, meta, context)

        self.pgops.add(dbops.Delete(table=self.table,
                                    condition=[('name', str(result.name))]))

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
            generic_link = link.proto if link.proto.generic() else link.proto.bases[0]
        else:
            generic_link = None

        with context(delta_cmds.LinkPropertyCommandContext(self, property)):
            rec, updates = self.record_metadata(property, None, meta, context)
            self.updates = updates

        if link and self.has_table(link.proto, meta):
            link.op.provide_table(link.proto, meta, context)
            alter_table = link.op.get_alter_table(context)

            default_value = self.get_pointer_default(property, meta, context)

            cols = self.get_columns(property, meta, default_value)
            for col in cols:
                # The column may already exist as inherited from parent table
                cond = dbops.ColumnExists(table_name=alter_table.name, column_name=col.name)

                if property.required:
                    # For some reaseon, Postgres allows dropping NOT NULL constraints
                    # from inherited columns, but we really should only always increase
                    # constraints down the inheritance chain.
                    cmd = dbops.AlterTableAlterColumnNull(column_name=col.name,
                                                          null=not property.required)
                    alter_table.add_operation((cmd, (cond,), None))

                cmd = dbops.AlterTableAddColumn(col)
                alter_table.add_operation((cmd, None, (cond,)))

        concept = context.get(delta_cmds.ConceptCommandContext)
        if not concept or not concept.proto.is_virtual:
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
            self.updates = updates

            if rec:
                self.pgops.add(dbops.Update(table=self.table, record=rec,
                                            condition=[('name', str(prop.name))], priority=1))

            if isinstance(prop.target, caos.types.ProtoAtom) and \
                    isinstance(self.old_prop.target, caos.types.ProtoAtom) and \
                    prop.required != self.old_prop.required:

                src_ctx = context.get(delta_cmds.LinkCommandContext)
                src_op = src_ctx.op
                alter_table = src_op.get_alter_table(context, priority=5)
                column_name = common.caos_name_to_pg_name(prop.normal_name())
                if prop.required:
                    table = src_op._type_mech.get_table(src_ctx.proto, meta)
                    rec = table.record(**{column_name:dbops.Default()})
                    cond = [(column_name, None)]
                    update = dbops.Update(table, rec, cond, priority=4)
                    self.pgops.add(update)
                alter_table.add_operation(dbops.AlterTableAlterColumnNull(column_name=column_name,
                                                                          null=not prop.required))

            new_type = None
            for op in self(delta_cmds.AlterPrototypeProperty):
                if op.property == 'target' and prop.normal_name() not in \
                                {'metamagic.caos.builtins.source', 'metamagic.caos.builtins.target'}:
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
        rec, updates = self.fill_record(meta)

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
            sides = ('metamagic.caos.builtins.source', 'metamagic.caos.builtins.target')

        elif mapping == caos.types.OneToMany:
            # Each target can have only one source, but
            # one source can have many targets
            sides = ('metamagic.caos.builtins.target',)

        elif mapping == caos.types.ManyToOne:
            # Each source can have only one target, but
            # one target can have many sources
            sides = ('metamagic.caos.builtins.source',)

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
            idx = dbops.Index(name=idx_name, table_name=table_name)
            fq_idx_name = (table_name[0], idx_name)
            index_exists = dbops.IndexExists(fq_idx_name)
            drop = dbops.DropIndex(idx, conditions=(index_exists,), priority=3)
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

    def interpret_index(self, index, link_map):
        index_name = index.name
        index_predicate = index.predicate
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

    def interpret_indexes(self, table_name, indexes, link_map):
        for idx_data in indexes:
            idx = dbops.Index.from_introspection(table_name, idx_data)
            yield idx.name, self.interpret_index(idx, link_map)

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
            idx_data = index_ds.fetch(schema_pattern='caos%',
                                      index_pattern='%_link_mapping_idx')
            for row in idx_data:
                table_name = tuple(row['table_name'])
                indexes[table_name] = self.interpret_indexes(table_name,
                                                             row['indexes'],
                                                             link_map)
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
                    self.pgops.add(CreateMappingIndexes(table_name, mapping, maplinks))

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
                    self.pgops.add(CreateMappingIndexes(table_name, mapping, new))

                for idx_names, altlinks in alter.items():
                    if not altlinks:
                        self.pgops.add(DropMappingIndexes(ex_idx_names, table_name, mapping))
                    else:
                        self.pgops.add(AlterMappingIndexes(idx_names, table_name, mapping,
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

        updaterec, updates = self.fill_record(schema)

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
        cmd.add_command(dbops.DropSchema(name=schema_name, conditions={condition}, priority=4))
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

        self.pgops.add(deltadbops.EnableFeature(feature=features.CryptoFeature(),
                                                neg_conditions=[dbops.FunctionExists(('caos', 'gen_random_bytes'))],
                                                priority=-2))

        self.pgops.add(deltadbops.EnableFeature(feature=features.FuzzystrmatchFeature(),
                                                neg_conditions=[dbops.FunctionExists(('caos', 'levenshtein'))],
                                                priority=-2))

        self.pgops.add(deltadbops.EnableFeature(feature=features.ProductAggregateFeature(),
                                                neg_conditions=[dbops.FunctionExists(('caos', 'agg_product'))],
                                                priority=-2))

        self.pgops.add(deltadbops.EnableFeature(feature=features.KnownRecordMarkerFeature(),
                                                neg_conditions=[dbops.DomainExists(('caos', 'known_record_marker_t'))],
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

        attributetable = deltadbops.AttributeTable()
        self.pgops.add(dbops.CreateTable(table=attributetable,
                                         neg_conditions=[dbops.TableExists(name=attributetable.name)],
                                         priority=-1))

        attrvaltable = deltadbops.AttributeValueTable()
        self.pgops.add(dbops.CreateTable(table=attrvaltable,
                                         neg_conditions=[dbops.TableExists(name=attrvaltable.name)],
                                         priority=-1))

        constrtable = deltadbops.ConstraintTable()
        self.pgops.add(dbops.CreateTable(table=constrtable,
                                         neg_conditions=[dbops.TableExists(name=constrtable.name)],
                                         priority=-1))

        actiontable = deltadbops.ActionTable()
        self.pgops.add(dbops.CreateTable(table=actiontable,
                                         neg_conditions=[dbops.TableExists(name=actiontable.name)],
                                         priority=-1))

        eventtable = deltadbops.EventTable()
        self.pgops.add(dbops.CreateTable(table=eventtable,
                                         neg_conditions=[dbops.TableExists(name=eventtable.name)],
                                         priority=-1))

        policytable = deltadbops.PolicyTable()
        self.pgops.add(dbops.CreateTable(table=policytable,
                                         neg_conditions=[dbops.TableExists(name=policytable.name)],
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
        self.pgops.add(self.update_mapping_indexes)

        self.pgops.add(UpgradeBackend.update_backend_info())

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

        from metamagic.caos import protoschema
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
            oldmod = 'metamagic.caos.backends.pgsql.delta.'
            if clsname.startswith(oldmod):
                rec = table.record()
                rec.class_name = 'metamagic.caos.backends.pgsql.features.' + clsname[len(oldmod):]
                cond = [('name', feature['name'])]
                ops.add_command(dbops.Update(table=table, record=rec, condition=cond))

        ops.execute(context)

    def update_to_version_7(self, context):
        """
        Backend format 7 renames source_id and target_id in link tables into link property cols.
        """

        tabname = common.link_name_to_table_name(caos.Name('metamagic.caos.builtins.link'),
                                                 catenate=False)
        src_col = common.caos_name_to_pg_name('metamagic.caos.builtins.source')
        tgt_col = common.caos_name_to_pg_name('metamagic.caos.builtins.target')

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
            if not prop['source_id'] or prop['name'] in {'metamagic.caos.builtins.source',
                                                         'metamagic.caos.builtins.target'}:
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

            src_col = common.caos_name_to_pg_name('metamagic.caos.builtins.source')
            tgt_col = common.caos_name_to_pg_name('metamagic.caos.builtins.target')

            constraints.append(dbops.UniqueConstraint(table_name=new_table_name,
                                                      columns=[src_col, tgt_col, 'link_type_id']))

            link_col.name = common.caos_name_to_pg_name('metamagic.caos.builtins.target@atom')
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
            '''.format(cols=','.join((common.qname(c.name) if c.name != 'metamagic.caos.builtins.target@atom' else 'NULL') for c in table.columns()),
                       table=common.qname(*base_table_name), link_id=link['id'])
            copy = dbops.Insert(table=table, records=dbops.Query(text=qtext))

            c.add_command(copy)

            base_table = dbops.Table(name=base_table_name)
            delete = dbops.Delete(table=base_table,
                                  condition=[('link_type_id', link['id'])],
                                  include_children=False)

            c.add_command(delete)

            c.execute(context)

    def update_to_version_10(self, context):
        """
        Backend format 10 adds base tables for policies and tables for pointer cascade policies
        """

        cg = dbops.CommandGroup()

        policytable = deltadbops.PolicyTable()
        cg.add_command(dbops.CreateTable(table=policytable,
                                         neg_conditions=[dbops.TableExists(name=policytable.name)]))

        eventpolicytable = deltadbops.EventPolicyTable()
        cg.add_command(dbops.CreateTable(table=eventpolicytable,
                                         neg_conditions=[dbops.TableExists(name=eventpolicytable.name)]))

        actiontable = deltadbops.ActionTable()
        cg.add_command(dbops.CreateTable(table=actiontable,
                                         neg_conditions=[dbops.TableExists(name=actiontable.name)]))

        eventtable = deltadbops.EventTable()
        cg.add_command(dbops.CreateTable(table=eventtable,
                                         neg_conditions=[dbops.TableExists(name=eventtable.name)]))

        policytable = deltadbops.PolicyTable()
        cg.add_command(dbops.CreateTable(table=policytable,
                                         neg_conditions=[dbops.TableExists(name=policytable.name)]))

        cg.execute(context)

    def update_to_version_11(self, context):
        """
        Backend format 11 adds known_record_marker_t type to annotate record pseudo-types
        """

        cond = [dbops.DomainExists(('caos', 'known_record_marker_t'))]
        op = deltadbops.EnableFeature(feature=features.KnownRecordMarkerFeature(),
                                      neg_conditions=cond)
        op.execute(context)

    def update_to_version_12(self, context):
        """
        Backend format 12 drops composite types for concepts.
        """

        concept_list = datasources.meta.concepts.ConceptList(context.db).fetch()
        commands = {}

        for concept in concept_list:
            cname = caos.Name(concept['name'])
            record_name = common.concept_name_to_record_name(cname, catenate=False)

            try:
                cmd = commands[concept['id']]
            except KeyError:
                cmd = commands[concept['id']] = dbops.CommandGroup()

            cond = dbops.CompositeTypeExists(record_name)
            drop = dbops.DropCompositeType(record_name, conditions=(cond,), cascade=True)
            cmd.add_command(drop)

        for group in commands.values():
            group.execute(context)

    def update_to_version_13(self, context):
        """
        Backend format 13 adds linkid property to all links.
        """

        ltab = common.link_name_to_table_name(caos.Name('metamagic.caos.builtins.link'),
                                              catenate=False)
        colname = common.caos_name_to_pg_name(caos.Name('metamagic.caos.builtins.linkid'))
        cond = dbops.ColumnExists(table_name=ltab, column_name=colname)

        cmdgroup = dbops.CommandGroup(neg_conditions=(cond,))

        alter_table = dbops.AlterTable(ltab)
        column = dbops.Column(name=colname, type='uuid', required=False)
        add_column = dbops.AlterTableAddColumn(column)
        alter_table.add_operation(add_column)

        cmdgroup.add_command(alter_table)

        idquery = dbops.Query(text='caos.uuid_generate_v1mc()', params=(), type='uuid')

        ds = datasources.introspection.tables.TableList(context.db)
        link_tables = ds.fetch(schema_name='caos%', table_pattern='%_link')

        for ltable in link_tables:
            ltabname = (ltable['schema'], ltable['name'])
            cond = dbops.TableExists(name=ltabname)

            tab = dbops.Table(name=ltabname)
            tab.add_columns((dbops.Column(name=colname, type='uuid'),))

            rec = tab.record()
            setattr(rec, colname, idquery)

            tgroup = dbops.CommandGroup(conditions=(cond,))

            tgroup.add_command(dbops.Echo('Creating link ids for "{}"...'.format(ltabname)))
            tgroup.add_command(dbops.Update(tab, rec, condition=[], include_children=False))

            cmdgroup.add_command(tgroup)

        alter_table = dbops.AlterTable(ltab)
        alter_column = dbops.AlterTableAlterColumnNull(column_name=colname, null=False)
        alter_table.add_operation(alter_column)
        cmdgroup.add_command(alter_table)

        cmdgroup.execute(context)

    def update_to_version_14(self, context):
        """
        Backend format 14: semantix became metamagic
        """

        cmdgroup = dbops.CommandGroup()

        ftab = deltadbops.FeatureTable()
        rec = ftab.record()
        rec.class_name = dbops.Query(text="(replace(class_name, 'semantix', 'metamagic'))")

        cmdgroup.add_command(dbops.Update(ftab, rec, condition=[]))

        for atab in (deltadbops.AtomTable(), deltadbops.LinkTable(), deltadbops.LinkPropertyTable()):
            rec = atab.record()

            rec.constraints = dbops.Query(text="""
                (SELECT
                    caos.hstore(array_agg(q.a))
                 FROM
                    (SELECT
                        replace(a, 'semantix', 'metamagic') AS a
                     FROM
                        unnest(caos.hstore_to_array(constraints)) AS p(a)
                    ) AS q
                )
            """)

            if hasattr(rec, 'abstract_constraints'):
                rec.abstract_constraints = dbops.Query(text="""
                    (SELECT
                        caos.hstore(array_agg(q.a))
                     FROM
                        (SELECT
                            replace(a, 'semantix', 'metamagic') AS a
                         FROM
                            unnest(caos.hstore_to_array(abstract_constraints)) AS p(a)
                        ) AS q
                    )
                """)

            cmdgroup.add_command(dbops.Update(atab, rec, condition=[]))


        ds = datasources.introspection.tables.TableConstraints(context.db)
        constraints = ds.fetch(schema_pattern='caos%', table_pattern='%_data')


        cmttab = dbops.Table(name=('pg_catalog', 'pg_description'))
        cmttab.add_columns((dbops.Column(name='description', type='text'),
                            dbops.Column(name='objoid', type='oid')))

        contab = dbops.Table(name=('pg_catalog', 'pg_constraint'))
        contab.add_columns((dbops.Column(name='conname', type='text'),
                            dbops.Column(name='contypid', type='oid'),
                            dbops.Column(name='oid', type='oid')))

        for row in constraints:
            for id, name, comment in zip(row['constraint_ids'], row['constraint_names'],
                                         row['constraint_descriptions']):
                if comment and '::semantix' in comment:

                    new_fname = comment.replace('::semantix', '::metamagic')
                    new_name = common.caos_name_to_pg_name(new_fname)

                    rec = cmttab.record()
                    rec.description = new_fname
                    condition = 'objoid', id
                    cmdgroup.add_command(dbops.Update(cmttab, rec, condition=[condition]))

                    rec = contab.record()
                    rec.conname = new_name

                    condition = 'oid', id
                    cmdgroup.add_command(dbops.Update(contab, rec, condition=[condition]))

        constr_mech = schemamech.ConstraintMech()
        ptr_constr = constr_mech.get_table_ptr_constraints(context.db)

        cconv = constr_mech._schema_constraint_to_backend_constraint
        crename = constr_mech.rename_unique_constraint_trigger

        concept_list = datasources.meta.concepts.ConceptList(context.db).fetch()
        concept_list = collections.OrderedDict((caos.Name(row['name']), row) for row in concept_list)

        tables = {common.concept_name_to_table_name(n, catenate=False): c \
                                                                for n, c in concept_list.items()}

        for src_tab, tab_constraints in ptr_constr.items():
            for ptr_name, ptr_constraints in tab_constraints.items():
                for ptr_constraint in ptr_constraints:
                    src_name = caos.name.Name(tables[src_tab]['name'])
                    orig_constr = cconv(ptr_constraint, src_name, src_tab, ptr_name)
                    new_constr = orig_constr.copy()

                    oldconstrcls = orig_constr.constrobj.__class__.get_canonical_class()
                    oldconstrclsname = '{}.{}'.format(oldconstrcls.__module__, oldconstrcls.__name__)
                    newconstrcls = get_object(oldconstrclsname.replace('semantix', 'metamagic'))
                    new_constr.constrobj = new_constr.constrobj.copy(cls=newconstrcls)

                    rename = crename(src_name, src_name, ptr_name, ptr_name, orig_constr,
                                                                             new_constr)

                    cmdgroup.add_command(rename)

        rec = contab.record()
        rec.conname = dbops.Query(text="(replace(conname, 'semantix', 'metamagic'))")
        condition = 'contypid', 'IN', dbops.Query(text="""
            (SELECT
                oid
            FROM
                pg_type
            WHERE
                typname LIKE '%_domain'
            )
        """)

        cmdgroup.add_command(dbops.Update(contab, rec, condition=[condition]))

        ds = datasources.introspection.tables.TableList(context.db)
        ctables = ds.fetch(schema_name='caos%', table_pattern='%_data')

        for ctable in ctables:
            ctabname = (ctable['schema'], ctable['name'])

            inheritance = datasources.introspection.tables.TableInheritance(context.db)
            inheritance = inheritance.fetch(table_name=ctable['name'],
                                            schema_name=ctable['schema'], max_depth=1)

            bases = tuple(i[:2] for i in inheritance[1:])

            for bt_schema, bt_name in bases:
                if bt_name.startswith('Virtual_'):
                    cmd = dbops.AlterTable(ctabname)
                    cmd.add_command(dbops.AlterTableDropParent((bt_schema, bt_name)))
                    cmdgroup.add_command(cmd)

        ltab = common.link_name_to_table_name(caos.Name('semantix.caos.builtins.link'),
                                              catenate=False)
        colname = common.caos_name_to_pg_name(caos.Name('semantix.caos.builtins.source'))
        new_colname = common.caos_name_to_pg_name(caos.Name('metamagic.caos.builtins.source'))
        rename_column = dbops.AlterTableRenameColumn(ltab, colname, new_colname)
        cmdgroup.add_command(rename_column)

        tabcol = dbops.TableColumn(table_name=ltab, column=dbops.Column(name=new_colname,
                                                                        type='text'))
        cmdgroup.add_command(dbops.Comment(tabcol, 'metamagic.caos.builtins.source'))

        colname = common.caos_name_to_pg_name(caos.Name('semantix.caos.builtins.target'))
        new_colname = common.caos_name_to_pg_name(caos.Name('metamagic.caos.builtins.target'))
        rename_column = dbops.AlterTableRenameColumn(ltab, colname, new_colname)
        cmdgroup.add_command(rename_column)

        tabcol = dbops.TableColumn(table_name=ltab, column=dbops.Column(name=new_colname,
                                                                        type='text'))
        cmdgroup.add_command(dbops.Comment(tabcol, 'metamagic.caos.builtins.target'))

        type = deltadbops.EntityModStatType()
        altertype = dbops.AlterCompositeTypeRenameAttribute(type.name,
                                                            'semantix.caos.builtins.id',
                                                            'metamagic.caos.builtins.id')
        cmdgroup.add_command(altertype)
        altertype = dbops.AlterCompositeTypeRenameAttribute(type.name,
                                                            'semantix.caos.builtins.mtime',
                                                            'metamagic.caos.builtins.mtime')
        cmdgroup.add_command(altertype)


        cmdgroup.execute(context)

    def update_to_version_15(self, context):
        """
        Backend format 15: drop virtual tables
        """

        cmdgroup = dbops.CommandGroup()

        ds = datasources.introspection.tables.TableList(context.db)
        ctables = ds.fetch(schema_name='caos%', table_pattern='%_data')

        for ctable in ctables:
            ctabname = (ctable['schema'], ctable['name'])

            inheritance = datasources.introspection.tables.TableInheritance(context.db)
            inheritance = inheritance.fetch(table_name=ctable['name'],
                                            schema_name=ctable['schema'], max_depth=1)

            bases = tuple(i[:2] for i in inheritance[1:])

            for bt_schema, bt_name in bases:
                if bt_name.startswith('Virtual_'):
                    cmd = dbops.AlterTable(ctabname)
                    cmd.add_command(dbops.AlterTableDropParent((bt_schema, bt_name)))
                    cmdgroup.add_command(cmd)

        for ctable in ctables:
            ctabname = (ctable['schema'], ctable['name'])

            if ctabname[1].startswith('Virtual_'):
                table_exists = dbops.TableExists(ctabname)
                drop = dbops.DropTable(ctabname, conditions=[table_exists])
                cmdgroup.add_command(drop)

        cmdgroup.execute(context)

    def update_to_version_16(self, context):
        """
        Backend format 16: add concept_id CHECK constraint
        """

        cmdgroup = dbops.CommandGroup()

        concept_list = datasources.meta.concepts.ConceptList(context.db).fetch()

        for concept in concept_list:
            if concept['is_virtual']:
                continue

            cname = caos.name.Name(concept['name'])
            ctabname = common.concept_name_to_table_name(cname, catenate=False)
            table_exists = dbops.TableExists(ctabname)
            alter_table = dbops.AlterTable(ctabname, conditions=[table_exists])

            constr_name = common.caos_name_to_pg_name(cname + '.concept_id_check')
            constr_expr = 'concept_id = {}'.format(concept['id'])
            cid_constraint = dbops.CheckConstraint(ctabname, constr_name, constr_expr,
                                                                          inherit=False)
            alter_table.add_operation(dbops.AlterTableAddConstraint(cid_constraint))

            cmdgroup.add_command(alter_table)

        cmdgroup.execute(context)

    def update_to_version_17(self, context):
        """
        Backend format 17: all non-trivial specialized links are given their own tables
        """

        links_list = datasources.meta.links.ConceptLinks(context.db).fetch()
        links_list = collections.OrderedDict((caos.Name(r['name']), r) for r in links_list)

        index_ds = datasources.introspection.tables.TableIndexes(context.db)
        indexes = {}
        for row in index_ds.fetch(schema_pattern='caos%', index_pattern='%_link_mapping_idx'):
            indexes[tuple(row['table_name'])] = row['index_names']

        update_mi = UpdateMappingIndexes()

        print('    Converting link tables')
        for link_name, r in links_list.items():
            lgroup = dbops.CommandGroup()

            if not r['source'] or r['is_atom']:
                continue

            base_name = caos.Name(r['base'][0])
            base_r = links_list[base_name]

            print('        {} ---{}--> {}'.format(r['source'], base_name, r['target']))

            parent_table_name = common.link_name_to_table_name(base_name, catenate=False)
            my_table = common.link_name_to_table_name(link_name, catenate=False)

            parent_table = dbops.Table(name=parent_table_name)
            table = dbops.Table(name=my_table)
            table.bases = [parent_table_name]

            src_col = common.caos_name_to_pg_name('metamagic.caos.builtins.source')
            tgt_col = common.caos_name_to_pg_name('metamagic.caos.builtins.target')

            constraint = dbops.UniqueConstraint(table_name=my_table,
                                                columns=[src_col, tgt_col, 'link_type_id'])
            table.constraints = [constraint]

            ct = dbops.CreateTable(table=table)

            index_name = common.caos_name_to_pg_name(str(link_name)  + 'target_id_default_idx')
            index = dbops.Index(index_name, my_table, unique=False)
            index.add_columns([tgt_col])
            ci = dbops.CreateIndex(index)

            lgroup.add_command(ct)
            lgroup.add_command(ci)
            lgroup.add_command(dbops.Comment(table, link_name))

            ins = dbops.Insert(table=table, records=dbops.Query(text="""
                SELECT * FROM ONLY {} WHERE link_type_id = $1
            """.format(common.qname(*parent_table_name)), params=(r['id'],)))

            delete = dbops.Delete(table=parent_table, condition=[('link_type_id', r['id'])],
                                  include_children=False)

            lgroup.add_command(ins)
            lgroup.add_command(delete)

            mapping = caos.types.LinkMapping(r['mapping'])
            base_mapping = caos.types.LinkMapping(base_r['mapping'])

            cmi = CreateMappingIndexes(my_table, mapping, (link_name,))
            lgroup.add_commands(cmi.pgops)

            lgroup.execute(context)

        print("    Dropping mapping indexes from generic tables")
        cmdgroup = dbops.CommandGroup()
        for link_name, r in links_list.items():
            lgroup = dbops.CommandGroup()

            if r['source'] or r['is_atom']:
                continue

            my_table = common.link_name_to_table_name(link_name, catenate=False)

            try:
                mapping_indexes = indexes[my_table]
            except KeyError:
                continue

            if mapping_indexes:
                mapping = caos.types.LinkMapping(r['mapping'])
                dmi = DropMappingIndexes(mapping_indexes, my_table, mapping)
                cmdgroup.add_commands(dmi.pgops)

        cmdgroup.execute(context)

    def update_to_version_18(self, context):
        """
        Backend format 18: enable Crypto feature
        """
        cond = dbops.FunctionExists(('caos', 'gen_random_bytes'))
        op = deltadbops.EnableFeature(feature=features.CryptoFeature(), neg_conditions=[cond])
        op.execute(context)

    def update_to_version_19(self, context):
        """
        Backend format 19 adds base tables for attributes and attribute values
        """

        cg = dbops.CommandGroup()

        for table in (deltadbops.AttributeTable(), deltadbops.AttributeValueTable()):
            cg.add_command(dbops.CreateTable(table=table,
                                             neg_conditions=[dbops.TableExists(name=table.name)]))

        cg.execute(context)

    def update_to_version_20(self, context):
        """
        Backend format 20 migrates saved CaosQL expressions from ':' ns-separator to '::'
        """

        from importkit import yaml

        cg = dbops.CommandGroup()

        links_list = datasources.meta.links.ConceptLinks(context.db).fetch()
        ltable = deltadbops.LinkTable()

        for r in links_list:
            if r['default']:
                value = next(iter(yaml.Language.load(r['default'])))

                result = []
                for item in value:
                    # XXX: This implicitly relies on yaml backend to be loaded, since
                    # adapter for DefaultSpec is defined there.
                    adapter = yaml.ObjectMeta.get_adapter(proto.DefaultSpec)
                    assert adapter, "could not find YAML adapter for proto.DefaultSpec"
                    data = item
                    item = adapter.resolve(item)(item)
                    item.__sx_setstate__(data)
                    result.append(item)

                for i, item in enumerate(result):
                    if isinstance(item, caos_types.ExpressionText):
                        result[i] = re.sub(r'(\w+):(\w+)\(', '\\1::\\2(', item)

                newdefault = yaml.Language.dump(result)

                if newdefault.lower() != r['default'].lower():
                    rec = ltable.record()
                    rec.default = newdefault
                    condition = [('id', r['id'])]
                    cg.add_command(dbops.Update(table=ltable, record=rec, condition=condition))

        computables_list = datasources.meta.links.Computables(context.db).fetch()
        ctable = deltadbops.ComputableTable()
        for r in computables_list:
            newexpression = re.sub(r'(\w+):(\w+)\(', '\\1::\\2(', r['expression'])

            if newexpression.lower() != r['expression'].lower():
                rec = ctable.record()
                rec.expression = newexpression
                condition = [('id', r['id'])]
                cg.add_command(dbops.Update(table=ctable, record=rec, condition=condition))

        cg.execute(context)

    def update_to_version_21(self, context):
        """
        Backend format 21: Adjust concept_id CHECK constraint to have a unique name
        """

        cmdgroup = dbops.CommandGroup()

        concept_list = datasources.meta.concepts.ConceptList(context.db).fetch()

        for concept in concept_list:
            if concept['is_virtual']:
                continue

            cname = caos.name.Name(concept['name'])
            ctabname = common.concept_name_to_table_name(cname, catenate=False)

            # Name autogenerated by Postgres, non-unique
            old_constr_name = ctabname[1] + '_concept_id_check'
            # Unique name, incorporating full concept name
            new_constr_name = common.caos_name_to_pg_name(cname + '.concept_id_check')

            table_exists = dbops.TableExists(ctabname)
            constraint_exists = dbops.TableConstraintExists(ctabname, old_constr_name)

            rc = dbops.AlterTableRenameConstraintSimple(
                    ctabname, old_name=old_constr_name,
                    new_name=new_constr_name,
                    conditions=[table_exists, constraint_exists])

            cmdgroup.add_command(rc)

        cmdgroup.execute(context)

    def update_to_version_22(self, context):
        """
        Backend format 22: New constraint system, drop old constraints
        """

        cmdgroup = dbops.CommandGroup()

        db = context.db
        cd = datasources.introspection.constraints.Constraints(db)

        # Drop constraints on tables
        constraints = cd.fetch(constraint_pattern='%::%_constr')
        for constraint in constraints:
            qry = '''
                ALTER TABLE {table} DROP CONSTRAINT {constraint}
            '''.format(
                table=common.qname(*constraint['table_name']),
                constraint=common.quote_ident(constraint['constraint_name'])
            )

            db.execute(qry)

        # Drop constraints on domains
        constraints = cd.fetch(constraint_pattern='metamagic.caos.proto.%')
        for constraint in constraints:
            qry = '''
                ALTER DOMAIN {domain} DROP CONSTRAINT {constraint}
            '''.format(
                domain=common.qname(*constraint['domain_name']),
                constraint=common.quote_ident(constraint['constraint_name'])
            )

            db.execute(qry)

        cg = dbops.CommandGroup()

        table = deltadbops.ConstraintTable()
        cg.add_command(dbops.CreateTable(table=table,
                    neg_conditions=[dbops.TableExists(name=table.name)]))
        cg.execute(context)

    def update_to_version_23(self, context):
        r"""\
        Backend format 23 migrates default format
        """

        from importkit import yaml

        cg = dbops.CommandGroup()

        def _update_defaults(obj_list, table):
            for r in obj_list:
                if r['default']:
                    value = next(iter(yaml.Language.load(r['default'])))

                    result = []
                    for item in value:
                        if isinstance(item, dict):
                            valtype = 'expr'
                            val = item['query']
                        else:
                            valtype = 'literal'
                            val = item

                        result.append({
                            'type': valtype,
                            'value': val
                        })

                    newdefault = json.dumps(result)

                    if newdefault.lower() != r['default'].lower():
                        rec = table.record()
                        rec.default = newdefault
                        condition = [('id', r['id'])]
                        cg.add_command(dbops.Update(table=table, record=rec,
                                                    condition=condition))

        links_list = datasources.meta.links.ConceptLinks(context.db).fetch()
        _update_defaults(links_list, deltadbops.LinkTable())

        props_list = datasources.meta.links.LinkProperties(context.db).fetch()
        _update_defaults(props_list, deltadbops.LinkPropertyTable())

        atoms_list = datasources.meta.atoms.AtomList(context.db).fetch()
        _update_defaults(atoms_list, deltadbops.AtomTable())

        cg.execute(context)

    def update_to_version_24(self, context):
        r"""\
        Backend format 24 migrates indexes to named objects
        """

        from metamagic.utils import ast

        cg = dbops.CommandGroup()

        inheritance = datasources.introspection.tables.TableInheritance(context.db)
        concept_list = datasources.meta.concepts.ConceptList(context.db).fetch()

        tables = {}
        concepts = {}
        concept_mros = {}

        for concept in concept_list:
            table_name = common.concept_name_to_table_name(
                            caos.Name(concept['name']), catenate=False)
            tables[table_name] = concept
            concepts[concept['name']] = concept

        def table_inheritance(table_name, schema_name):
            clslist = inheritance.fetch(table_name=table_name,
                                        schema_name=schema_name,
                                        max_depth=1)
            return tuple(i[:2] for i in clslist[1:])

        def concept_bases(table_name, schema_name):
            bases = []

            for table in table_inheritance(table_name, schema_name):
                base = tables[table[:2]]
                bases.append(base['name'])

            return tuple(bases)

        def _merge_mro(clsname, mros):
            result = []

            while True:
                nonempty = [mro for mro in mros if mro]
                if not nonempty:
                    return result

                for mro in nonempty:
                    candidate = mro[0]
                    tails = [m for m in nonempty
                             if id(candidate) in {id(c) for c in m[1:]}]
                    if not tails:
                        break
                else:
                    # Could not find consistent MRO, should not happen
                    msg = "Could not find consistent MRO for {!r}".format(clsname)
                    assert False, msg

                result.append(candidate)

                for mro in nonempty:
                    if mro[0] is candidate:
                        del mro[0]

            return result

        def _get_mro(concept):
            mros = [[concept['name']]]
            bases = concept['base']

            for base in bases:
                mros.append(_get_mro(concepts[base]))

            return _merge_mro(concept['name'], mros)

        def _get_new_index_name(subject_name, expr):
            index_name = '{}.autoidx_{:x}'.format(
                            subject_name, persistent_hash(expr))

            index_name = Idx.generate_specialized_name(subject_name,
                                                       index_name)

            index_name = caos.Name(name=index_name,
                                   module=subject_name.module)

            index_name_full = '{}_reg_idx'.format(index_name)

            return index_name, index_name_full

        def _get_old_index_name(subject_name, expr):
            index_name_full = '{}_{}_reg_idx'.format(
                                subject_name, persistent_hash(expr))

            return common.caos_name_to_pg_name(index_name_full)


        for table_name, concept in tables.items():
            concept['base'] = concept_bases(table_name[1], table_name[0])

        for concept_name, concept in concepts.items():
            concept_mros[concept_name] = _get_mro(concept)

        index_ds = datasources.introspection.tables.TableIndexes(context.db)
        idx_data = index_ds.fetch(
                        schema_pattern='caos%',
                        index_pattern='%_reg_idx',
                        table_list=['{}.{}'.format(*t) for t in tables])

        link_ds = datasources.meta.links.ConceptLinks(context.db)
        links = link_ds.fetch()

        link_name_map = {common.caos_name_to_pg_name(l['name']): l['name']
                         for l in links if l['target'] is None}

        link_subclasses = {}

        for link in links:
            if link['target'] is None:
                continue

            base = link['base'][0]

            try:
                lsc = link_subclasses[base]
            except KeyError:
                lsc = link_subclasses[base] = {}

            lsc[link['source']] = link

        sql_parser = parser.PgSQLParser()
        Idx = caos.proto.SourceIndex

        indexes = {}
        for row in idx_data:
            table_name = tuple(row['table_name'])
            concept = tables[table_name]
            subject_name = caos.Name(concept['name'])
            subject_mro = concept_mros[subject_name]
            subject_mro_set = set(subject_mro)

            for index_data in row['indexes']:
                pg_index = dbops.Index.from_introspection(
                                table_name, index_data)

                sql_expr = pg_index.expr
                if not sql_expr:
                    cols = (common.quote_ident(c) for c in pg_index.columns)
                    sql_expr = '(' + ', '.join(cols) + ')'

                sql_tree = sql_parser.parse(sql_expr)
                is_fieldref = lambda n: isinstance(n, pg_ast.FieldRefNode)
                field_refs = ast.find_children(sql_tree, is_fieldref)
                if is_fieldref(sql_tree):
                    field_refs.append(sql_tree)

                annotated_frefs = []
                possible_srcs = []

                for field_ref in field_refs:
                    link_name = link_name_map[field_ref.field]
                    spec_links = link_subclasses[link_name]

                    annotated_frefs.append((field_ref.field, link_name))
                    possible_srcs.append(set(spec_links) & subject_mro_set)

                for src_combination in itertools.product(*possible_srcs):
                    caosql_expr = sql_expr

                    for i, (col, ptr_name) in enumerate(annotated_frefs):
                        src = src_combination[i]
                        ptr_ref = '[{}].[{}]'.format(src, ptr_name)
                        qcol = '"{}"'.format(col)
                        caosql_expr = caosql_expr.replace(qcol, ptr_ref)

                    if len(field_refs) == 1:
                        # strip parentheses
                        caosql_expr = caosql_expr[1:-1]

                    old_index_names = []

                    old_index_names.append(
                        _get_old_index_name(subject_name, caosql_expr))

                    caosql_expr = 'SELECT {}'.format(caosql_expr)
                    old_index_names.append(
                        _get_old_index_name(subject_name, caosql_expr))

                    new_index_name_schema, new_index_name_db = \
                        _get_new_index_name(subject_name, caosql_expr)

                    index = dbops.Index(
                                new_index_name_db,
                                table_name=table_name,
                                inherit=True,
                                expr=pg_index.expr,
                                unique=pg_index.unique,
                                columns=pg_index.columns,
                                metadata={'schemaname': new_index_name_schema})

                    for old_index_name in old_index_names:
                        old_index_fqn = (table_name[0], old_index_name)
                        old_index = dbops.Index(old_index_name,
                                                table_name=table_name)

                        cond = dbops.IndexExists(old_index_fqn)

                        rng = dbops.CommandGroup(conditions=[cond])

                        drop = dbops.DropIndex(old_index)
                        rng.add_command(drop)

                        create = dbops.CreateIndex(index)
                        rng.add_command(create)

                        cg.add_command(rng)

        cg.execute(context)

    def update_to_version_25(self, context):
        r"""\
        Backend format 25 migrates to new policy tables
        """

        cg = dbops.CommandGroup()
        cg.add_command(dbops.DropTable(name=('caos', 'pointer_cascade_policy')))
        cg.add_command(dbops.DropTable(name=('caos', 'pointer_cascade_event')))
        cg.add_command(dbops.DropTable(name=('caos', 'pointer_cascade_action')))
        cg.add_command(dbops.DropTable(name=('caos', 'event_policy')))

        alter = dbops.AlterTable(('caos', 'policy'))

        catcol = dbops.Column(name='category', type='text')
        alter.add_command(dbops.AlterTableDropColumn(catcol))

        eventcol = dbops.Column(name='event', type='integer')
        alter.add_command(dbops.AlterTableAddColumn(eventcol))

        actcol = dbops.Column(name='actions', type='integer[]')
        alter.add_command(dbops.AlterTableAddColumn(actcol))

        cg.add_command(alter)

        actiontable = deltadbops.ActionTable()
        cg.add_command(dbops.CreateTable(table=actiontable))

        eventtable = deltadbops.EventTable()
        cg.add_command(dbops.CreateTable(table=eventtable))

        cg.execute(context)

    @classmethod
    def update_backend_info(cls):
        backendinfotable = deltadbops.BackendInfoTable()
        record = backendinfotable.record()
        record.format_version = BACKEND_FORMAT_VERSION
        condition = [('format_version', '<', BACKEND_FORMAT_VERSION)]
        return dbops.Merge(table=backendinfotable, record=record,
                           condition=condition)
