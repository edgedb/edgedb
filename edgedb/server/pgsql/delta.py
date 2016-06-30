##
# Copyright (c) 2008-2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import itertools
import pickle
import postgresql.string
import re

from edgedb.lang import caosql

from edgedb.lang.schema import attributes as s_attrs
from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import constraints as s_constr
from edgedb.lang.schema import delta as sd
from edgedb.lang.schema import error as s_err
from edgedb.lang.schema import expr as s_expr
from edgedb.lang.schema import indexes as s_indexes
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import modules as s_mod
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import named as s_named
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import policy as s_policy
from edgedb.lang.schema import realm as s_realm

from metamagic import json

from edgedb.lang.common import datastructures
from edgedb.lang.common.debug import debug
from edgedb.lang.common.algos.persistent_hash import persistent_hash
from edgedb.lang.common import markup
from importkit.import_ import get_object

from edgedb.server.pgsql import common
from edgedb.server.pgsql import dbops, deltadbops, features

from . import ast as pg_ast
from . import codegen
from . import datasources
from . import parser
from . import schemamech
from . import transformer
from . import types


BACKEND_FORMAT_VERSION = 30


class CommandMeta(sd.CommandMeta):
    pass


class MetaCommand(sd.Command, metaclass=CommandMeta):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pgops = datastructures.OrderedSet()

    def apply(self, schema, context=None):
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


class CommandGroupAdapted(MetaCommand, adapts=sd.CommandGroup):
    def apply(self, schema, context):
        sd.CommandGroup.apply(self, schema, context)
        MetaCommand.apply(self, schema, context)


class PrototypeMetaCommand(MetaCommand, sd.PrototypeCommand):
    pass


class NamedPrototypeMetaCommand(PrototypeMetaCommand, s_named.NamedPrototypeCommand):
    op_priority = 0

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._type_mech = schemamech.TypeMech()

    def _serialize_refs(self, value):
        if isinstance(value, s_obj.PrototypeRef):
            result = value.prototype_name

        elif isinstance(value, (s_obj.PrototypeSet, s_obj.PrototypeList)):
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
                if isinstance(item, s_expr.ExpressionText):
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

    def update(self, schema, context):
        updaterec, updates = self.fill_record(schema)

        if updaterec:
            condition = [('name', str(self.proto.name))]
            self.pgops.add(dbops.Update(table=self.table, record=updaterec, condition=condition, priority=self.op_priority))

        return updates

    def rename(self, schema, context, old_name, new_name):
        updaterec = self.table.record(name=str(new_name))
        condition = [('name', str(old_name))]
        self.pgops.add(dbops.Update(table=self.table, record=updaterec, condition=condition))

    def delete(self, schema, context, proto):
        self.pgops.add(dbops.Delete(table=self.table, condition=[('name', str(proto.name))]))


class CreateNamedPrototype(NamedPrototypeMetaCommand):
    def apply(self, schema, context):
        obj = self.__class__.get_adaptee().apply(self, schema, context)
        NamedPrototypeMetaCommand.apply(self, schema, context)
        updates = self.create_object(schema, obj)
        self.updates = updates
        return obj


class CreateOrAlterNamedPrototype(NamedPrototypeMetaCommand):
    def apply(self, schema, context):
        existing = schema.get(self.prototype_name, None)

        obj = self.__class__.get_adaptee().apply(self, schema, context)
        self.proto = obj
        NamedPrototypeMetaCommand.apply(self, schema, context)

        if existing is None:
            updates = self.create_object(schema, obj)
            self.updates = updates
        else:
            self.updates = self.update(schema, context)
        return obj


class RenameNamedPrototype(NamedPrototypeMetaCommand):
    def apply(self, schema, context):
        obj = self.__class__.get_adaptee().apply(self, schema, context)
        NamedPrototypeMetaCommand.apply(self, schema, context)
        self.rename(schema, context, self.prototype_name, self.new_name)
        return obj


class RebaseNamedPrototype(NamedPrototypeMetaCommand):
    def apply(self, schema, context):
        obj = self.__class__.get_adaptee().apply(self, schema, context)
        NamedPrototypeMetaCommand.apply(self, schema, context)
        return obj


class AlterNamedPrototype(NamedPrototypeMetaCommand):
    def apply(self, schema, context):
        obj = self.__class__.get_adaptee().apply(self, schema, context)
        self.proto = obj
        NamedPrototypeMetaCommand.apply(self, schema, context)
        self.updates = self.update(schema, context)
        return obj


class DeleteNamedPrototype(NamedPrototypeMetaCommand):
    def apply(self, schema, context):
        obj = self.__class__.get_adaptee().apply(self, schema, context)
        NamedPrototypeMetaCommand.apply(self, schema, context)
        self.delete(schema, context, obj)
        return obj


class AlterPrototypeProperty(MetaCommand, adapts=sd.AlterPrototypeProperty):
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
                      adapts=s_attrs.CreateAttribute):
    pass


class RenameAttribute(AttributeCommand,
                      RenameNamedPrototype,
                      adapts=s_attrs.RenameAttribute):
    pass


class AlterAttribute(AttributeCommand,
                     AlterNamedPrototype,
                     adapts=s_attrs.AlterAttribute):
    pass


class DeleteAttribute(AttributeCommand,
                      DeleteNamedPrototype,
                      adapts=s_attrs.DeleteAttribute):
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
                           CreateOrAlterNamedPrototype,
                           adapts=s_attrs.CreateAttributeValue):
    pass


class RenameAttributeValue(AttributeValueCommand,
                           RenameNamedPrototype,
                           adapts=s_attrs.RenameAttributeValue):
    pass


class AlterAttributeValue(AttributeValueCommand,
                          AlterNamedPrototype,
                          adapts=s_attrs.AlterAttributeValue):
    pass


class DeleteAttributeValue(AttributeValueCommand,
                           DeleteNamedPrototype,
                           adapts=s_attrs.DeleteAttributeValue):
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
                        if isinstance(v, s_obj.Set):
                            if v.element_type:
                                v = 'set<{}>'.format(v.element_type.prototype_name)
                        elif isinstance(v, s_obj.PrototypeRef):
                            v = v.prototype_name
                        else:
                            msg = 'unexpected type in constraint paramtypes: {}'.format(v)
                            raise ValueError(msg)

                        pt[k] = v

                    setattr(rec, ptn, pt)

            args = updates.get('args')
            if args:
                rec.args = pickle.dumps(dict(args[1])) if args[1] else None

            # Write the original locally-defined expression
            # so that when the schema is introspected the
            # correct finalexpr is restored with prototype
            # inheritance mechanisms.
            rec.finalexpr = rec.localfinalexpr

        return rec, updates


class CreateConstraint(ConstraintCommand, CreateNamedPrototype,
                       adapts=s_constr.CreateConstraint):
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
                       adapts=s_constr.RenameConstraint):
    def apply(self, protoschema, context):
        constr_ctx = context.get(s_constr.ConstraintCommandContext)
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
                      adapts=s_constr.AlterConstraint):
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
                       adapts=s_constr.DeleteConstraint):
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

    def is_sequence(self, schema, atom):
        seq = schema.get('std.sequence', default=None)
        return seq is not None and atom.issubclass(seq)

    def fill_record(self, schema, rec=None, obj=None):
        rec, updates = super().fill_record(schema, rec, obj)
        base = updates.get('base')

        if base:
            if not rec:
                rec = self.table.record()

            if base[1]:
                rec.base = str(base[1][0])
            else:
                rec.base = None

        default = updates.get('default')
        if default:
            if not rec:
                rec = self.table.record()
            rec.default = self.pack_default(default[1])

        return rec, updates

    def alter_atom_type(self, atom, schema, new_type, intent):

        users = []

        for link in schema(type='link'):
            if link.target and link.target.name == atom.name:
                users.append((link.source, link))

        domain_name = common.atom_name_to_domain_name(atom.name, catenate=False)

        new_constraints = atom.local_constraints
        base = types.get_atom_base(schema, atom)

        target_type = new_type

        schemac_to_backendc = schemamech.ConstraintMech.schema_constraint_to_backend_constraint

        if intent == 'alter':
            new_name = domain_name[0], domain_name[1] + '_tmp'
            self.pgops.add(dbops.RenameDomain(domain_name, new_name))
            target_type = common.qname(*domain_name)

            self.pgops.add(dbops.CreateDomain(name=domain_name, base=new_type))

            adapt = deltadbops.SchemaDBObjectMeta.adapt
            for constraint in new_constraints.values():
                bconstr = schemac_to_backendc(atom, constraint, schema)
                op = dbops.CommandGroup(priority=1)
                op.add_command(bconstr.create_ops())
                self.pgops.add(op)

            domain_name = new_name

        elif intent == 'create':
            self.pgops.add(dbops.CreateDomain(name=domain_name, base=base))

        for host_proto, item_proto in users:
            if isinstance(item_proto, s_links.Link):
                name = item_proto.normal_name()
            else:
                name = item_proto.name

            table_name = common.get_table_name(host_proto, catenate=False)
            column_name = common.caos_name_to_pg_name(name)

            alter_type = dbops.AlterTableAlterColumnType(column_name, target_type)
            alter_table = dbops.AlterTable(table_name)
            alter_table.add_operation(alter_type)
            self.pgops.add(alter_table)

        for child_atom in schema(type='atom'):
            if [b.name for b in child_atom.bases] == [atom.name]:
                self.alter_atom_type(child_atom, schema, target_type, 'alter')

        if intent == 'drop' or (intent == 'alter' and not simple_alter):
            self.pgops.add(dbops.DropDomain(domain_name))


class CreateAtom(AtomMetaCommand, adapts=s_atoms.CreateAtom):
    def apply(self, schema, context=None):
        atom = s_atoms.CreateAtom.apply(self, schema, context)
        AtomMetaCommand.apply(self, schema, context)

        new_domain_name = common.atom_name_to_domain_name(atom.name, catenate=False)
        base = types.get_atom_base(schema, atom)

        updates = self.create_object(schema, atom)

        self.pgops.add(dbops.CreateDomain(name=new_domain_name, base=base))

        if self.is_sequence(schema, atom):
            seq_name = common.atom_name_to_sequence_name(atom.name, catenate=False)
            self.pgops.add(dbops.CreateSequence(name=seq_name))

        default = updates.get('default')
        if default:
            default = default[1]
            if len(default) > 0 and \
                not isinstance(default[0], s_expr.ExpressionText):
                # We only care to support literal defaults here.  Supporting
                # defaults based on queries has no sense on the database level
                # since the database forbids queries for DEFAULT and pre-
                # calculating the value does not make sense either since the
                # whole point of query defaults is for them to be dynamic.
                self.pgops.add(dbops.AlterDomainAlterDefault(
                    name=new_domain_name, default=default[0]))

        return atom


class RenameAtom(AtomMetaCommand, adapts=s_atoms.RenameAtom):
    def apply(self, schema, context=None):
        proto = s_atoms.RenameAtom.apply(self, schema, context)
        AtomMetaCommand.apply(self, schema, context)

        domain_name = common.atom_name_to_domain_name(self.prototype_name, catenate=False)
        new_domain_name = common.atom_name_to_domain_name(self.new_name, catenate=False)

        self.pgops.add(dbops.RenameDomain(name=domain_name, new_name=new_domain_name))
        self.rename(schema, context, self.prototype_name, self.new_name)

        if self.is_sequence(schema, proto):
            seq_name = common.atom_name_to_sequence_name(self.prototype_name, catenate=False)
            new_seq_name = common.atom_name_to_sequence_name(self.new_name, catenate=False)

            self.pgops.add(dbops.RenameSequence(name=seq_name, new_name=new_seq_name))

        return proto


class RebaseAtom(AtomMetaCommand, adapts=s_atoms.RebaseAtom):
    # Rebase is taken care of in AlterAtom
    pass


class AlterAtom(AtomMetaCommand, adapts=s_atoms.AlterAtom):
    def apply(self, schema, context=None):
        old_atom = schema.get(self.prototype_name).copy()
        new_atom = s_atoms.AlterAtom.apply(self, schema, context)
        AtomMetaCommand.apply(self, schema, context)

        updaterec, updates = self.fill_record(schema)

        if updaterec:
            condition = [('name', str(new_atom.name))]
            self.pgops.add(dbops.Update(table=self.table, record=updaterec,
                                        condition=condition))

        self.alter_atom(self, schema, context, old_atom, new_atom,
                                                       updates=updates)

        return new_atom

    @classmethod
    def alter_atom(cls, op, schema, context, old_atom, new_atom, in_place=True,
                                                               updates=None):

        old_base = types.get_atom_base(schema, old_atom)
        base = types.get_atom_base(schema, new_atom)

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
                op.alter_atom_type(new_atom, schema, new_type, intent=type_intent)

        if type_intent != 'drop':
            if updates:
                default_delta = updates.get('default')
                if default_delta:
                    default_delta = default_delta[1]

                    if not default_delta or \
                           isinstance(default_delta[0], s_expr.ExpressionText):
                        new_default = None
                    else:
                        new_default = default_delta[0]

                    adad = dbops.AlterDomainAlterDefault(name=domain_name, default=new_default)
                    op.pgops.add(adad)


class DeleteAtom(AtomMetaCommand, adapts=s_atoms.DeleteAtom):
    def apply(self, schema, context=None):
        atom = s_atoms.DeleteAtom.apply(self, schema, context)
        AtomMetaCommand.apply(self, schema, context)

        link = None
        if context:
            link = context.get(s_links.LinkCommandContext)

        ops = link.op.pgops if link else self.pgops

        old_domain_name = common.atom_name_to_domain_name(self.prototype_name, catenate=False)

        # Domain dropping gets low priority since other things may depend on it
        cond = dbops.DomainExists(old_domain_name)
        ops.add(dbops.DropDomain(name=old_domain_name, conditions=[cond], priority=3))
        ops.add(dbops.Delete(table=deltadbops.AtomTable(),
                             condition=[('name', str(self.prototype_name))]))

        if self.is_sequence(schema, atom):
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

    def apply(self, schema, context):
        if isinstance(self.host, s_concepts.Concept):
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

    def rename(self, schema, context, old_name, new_name, obj=None):
        super().rename(schema, context, old_name, new_name)

        if obj is not None and isinstance(obj, s_links.Link):
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

    def search_index_add(self, host, pointer, schema, context):
        if self.update_search_indexes is None:
            self.update_search_indexes = UpdateSearchIndexes(host)

    def search_index_alter(self, host, pointer, schema, context):
        if self.update_search_indexes is None:
            self.update_search_indexes = UpdateSearchIndexes(host)

    def search_index_delete(self, host, pointer, schema, context):
        if self.update_search_indexes is None:
            self.update_search_indexes = UpdateSearchIndexes(host)

    @classmethod
    def get_source_and_pointer_ctx(cls, schema, context):
        if context:
            concept = context.get(s_concepts.ConceptCommandContext)
            link = context.get(s_links.LinkCommandContext)
        else:
            concept = link = None

        if concept:
            source, pointer = concept, link
        elif link:
            property = context.get(s_lprops.LinkPropertyCommandContext)
            source, pointer = link, property
        else:
            source = pointer = None

        return source, pointer

    def affirm_pointer_defaults(self, source, schema, context):
        for pointer_name, pointer in source.pointers.items():
            # XXX pointer_storage_info?
            if (pointer.generic() or not pointer.atomic() or not pointer.singular()
                                                          or not pointer.default):
                continue

            default = None
            ld = list(filter(lambda i: not isinstance(i, s_expr.ExpressionText),
                             pointer.default))
            if ld:
                default = ld[0]

            if default is not None:
                alter_table = self.get_alter_table(context, priority=3, contained=True)
                column_name = common.caos_name_to_pg_name(pointer_name)
                alter_table.add_operation(dbops.AlterTableAlterColumnDefault(column_name=column_name,
                                                                             default=default))

    def adjust_pointer_storage(self, orig_pointer, pointer, schema, context):
        old_ptr_stor_info = types.get_pointer_storage_info(
                                orig_pointer, schema=schema)
        new_ptr_stor_info = types.get_pointer_storage_info(
                                pointer, schema=schema)

        old_target = orig_pointer.target
        new_target = pointer.target

        source_ctx = context.get(s_concepts.ConceptCommandContext)
        source_proto = source_ctx.proto
        source_op = source_ctx.op

        type_change_ok = False

        if old_target.name != new_target.name \
                                or old_ptr_stor_info.table_type != new_ptr_stor_info.table_type:

            for op in self(s_atoms.AtomCommand):
                for rename in op(s_atoms.RenameAtom):
                    if old_target.name == rename.prototype_name \
                                        and new_target.name == rename.new_name:
                        # Our target alter is a mere rename
                        type_change_ok = True

                if isinstance(op, s_atoms.CreateAtom):
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

                    if oldcol.name != 'std.target':
                        pat.add_command(dbops.AlterTableDropColumn(oldcol))

                    # Moved from link to concept
                    cols = self.get_columns(pointer, schema)

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
                    if isinstance(old_target, s_atoms.Atom):
                        AlterAtom.alter_atom(self, schema, context, old_target,
                                                                  new_target, in_place=False)

                        alter_table = source_op.get_alter_table(context, priority=1)
                        alter_type = dbops.AlterTableAlterColumnType(
                                                old_ptr_stor_info.column_name,
                                                types.pg_type_from_object(schema, new_target))
                        alter_table.add_operation(alter_type)

    def apply_base_delta(self, orig_source, source, schema, context):
        realm = context.get(s_realm.RealmCommandContext)
        orig_source.bases = [realm.op._renames.get(b, b) for b in orig_source.bases]

        dropped_bases = {b.name for b in orig_source.bases} - {b.name for b in source.bases}

        if isinstance(source, s_concepts.Concept):
            nameconv = common.concept_name_to_table_name
            source_ctx = context.get(s_concepts.ConceptCommandContext)
            ptr_cmd = s_links.CreateLink
        else:
            nameconv = common.link_name_to_table_name
            source_ctx = context.get(s_links.LinkCommandContext)
            ptr_cmd = s_lprops.CreateLinkProperty

        alter_table = source_ctx.op.get_alter_table(context, force_new=True)

        if isinstance(source, s_concepts.Concept) \
                        or source_ctx.op.has_table(source, schema):

            source.acquire_ancestor_inheritance(schema)
            orig_source.acquire_ancestor_inheritance(schema)

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
                                    ptr, schema=schema)

                is_a_column = (
                    (ptr_stor_info.table_type == 'concept'
                            and isinstance(source, s_concepts.Concept))
                    or (ptr_stor_info.table_type == 'link'
                            and isinstance(source, s_links.Link))
                )

                if is_a_column:
                    col = dbops.Column(name=ptr_stor_info.column_name,
                                       type=ptr_stor_info.column_type,
                                       required=ptr.required)
                    cond = dbops.ColumnExists(table_name=source_ctx.op.table_name,
                                              column_name=ptr_stor_info.column_name)
                    alter_table.add_operation((dbops.AlterTableAddColumn(col), None, (cond,)))

            if dropped_bases:
                alter_table_drop_parent = source_ctx.op.get_alter_table(context, force_new=True)

                for dropped_base in dropped_bases:
                    parent_table_name = nameconv(sn.Name(dropped_base), catenate=False)
                    op = dbops.AlterTableDropParent(parent_name=parent_table_name)
                    alter_table_drop_parent.add_operation(op)

                dropped_ptrs = set(orig_source.pointers) - set(source.pointers)

                if dropped_ptrs:
                    alter_table_drop_ptr = source_ctx.op.get_alter_table(context, force_new=True)

                    for dropped_ptr in dropped_ptrs:
                        ptr = orig_source.pointers[dropped_ptr]
                        ptr_stor_info = types.get_pointer_storage_info(
                                            ptr, schema=schema)

                        is_a_column = (
                            (ptr_stor_info.table_type == 'concept'
                                    and isinstance(source, s_concepts.Concept))
                            or (ptr_stor_info.table_type == 'link'
                                    and isinstance(source, s_links.Link))
                        )

                        if is_a_column:
                            col = dbops.Column(name=ptr_stor_info.column_name,
                                               type=ptr_stor_info.column_type,
                                               required=ptr.required)

                            cond = dbops.ColumnExists(table_name=ptr_stor_info.table_name,
                                                      column_name=ptr_stor_info.column_name)
                            op = dbops.AlterTableDropColumn(col)
                            alter_table_drop_ptr.add_command((op, (cond,), ()))

            current_bases = list(datastructures.OrderedSet(
                                b.name for b in orig_source.bases)
                                    - dropped_bases)

            new_bases = [b.name for b in source.bases]

            unchanged_order = list(itertools.takewhile(
                                    lambda x: x[0] == x[1],
                                    zip(current_bases, new_bases)))

            old_base_order = current_bases[len(unchanged_order):]
            new_base_order = new_bases[len(unchanged_order):]

            if new_base_order:
                table_name = nameconv(source.name, catenate=False)
                alter_table_drop_parent = source_ctx.op.get_alter_table(context, force_new=True)
                alter_table_add_parent = source_ctx.op.get_alter_table(context, force_new=True)

                for base in old_base_order:
                    parent_table_name = nameconv(sn.Name(base), catenate=False)
                    cond = dbops.TableInherits(table_name, parent_table_name)
                    op = dbops.AlterTableDropParent(parent_name=parent_table_name)
                    alter_table_drop_parent.add_operation((op, [cond], None))

                for added_base in new_base_order:
                    parent_table_name = nameconv(sn.Name(added_base), catenate=False)
                    cond = dbops.TableInherits(table_name, parent_table_name)
                    op = dbops.AlterTableAddParent(parent_name=parent_table_name)
                    alter_table_add_parent.add_operation((op, None, [cond]))


class SourceIndexCommand(PrototypeMetaCommand):
    pass


class CreateSourceIndex(SourceIndexCommand, adapts=s_indexes.CreateSourceIndex):
    def apply(self, schema, context=None):
        index = s_indexes.CreateSourceIndex.apply(self, schema, context)
        SourceIndexCommand.apply(self, schema, context)

        source = context.get(s_links.LinkCommandContext)
        if not source:
            source = context.get(s_concepts.ConceptCommandContext)
        table_name = common.get_table_name(source.proto, catenate=False)
        ir = caosql.compile_fragment_to_ir(index.expr, schema,
                                           location='selector')

        ircompiler = transformer.SimpleIRCompiler()
        sql_tree = ircompiler.transform(ir, schema, local=True)
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


class RenameSourceIndex(SourceIndexCommand, adapts=s_indexes.RenameSourceIndex):
    def apply(self, schema, context):
        index = s_indexes.RenameSourceIndex.apply(self, schema, context)
        SourceIndexCommand.apply(self, schema, context)

        subject = context.get(s_links.LinkCommandContext)
        if not subject:
            subject = context.get(s_concepts.ConceptCommandContext)
        orig_table_name = common.get_table_name(subject.original_proto,
                                                catenate=False)

        index_ctx = context.get(s_indexes.SourceIndexCommandContext)
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


class AlterSourceIndex(SourceIndexCommand, adapts=s_indexes.AlterSourceIndex):
    def apply(self, schema, context=None):
        result = s_indexes.AlterSourceIndex.apply(self, schema, context)
        SourceIndexCommand.apply(self, schema, context)
        return result


class DeleteSourceIndex(SourceIndexCommand, adapts=s_indexes.DeleteSourceIndex):
    def apply(self, schema, context=None):
        index = s_indexes.DeleteSourceIndex.apply(self, schema, context)
        SourceIndexCommand.apply(self, schema, context)

        source = context.get(s_links.LinkCommandContext)
        if not source:
            source = context.get(s_concepts.ConceptCommandContext)

        if not isinstance(source.op, s_named.DeleteNamedPrototype):
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


class CreateConcept(ConceptMetaCommand, adapts=s_concepts.CreateConcept):
    def apply(self, schema, context=None):
        concept_props = self.get_struct_properties(include_old_value=False)
        is_virtual = concept_props.get('is_virtual')
        if is_virtual:
            return s_concepts.CreateConcept.apply(self, schema, context)

        new_table_name = common.concept_name_to_table_name(
                            self.prototype_name, catenate=False)
        self.table_name = new_table_name
        concept_table = dbops.Table(name=new_table_name)
        self.pgops.add(dbops.CreateTable(table=concept_table))

        alter_table = self.get_alter_table(context)

        concept = s_concepts.CreateConcept.apply(self, schema, context)
        ConceptMetaCommand.apply(self, schema, context)

        fields = self.create_object(schema, concept)

        constr_name = common.caos_name_to_pg_name(
                        self.prototype_name + '.concept_id_check')

        constr_expr = dbops.Query("""
            SELECT 'concept_id = ' || id FROM caos.concept WHERE name = $1
        """, [concept.name], type='text')

        cid_constraint = dbops.CheckConstraint(
                            self.table_name, constr_name, constr_expr,
                            inherit=False)
        alter_table.add_operation(
            dbops.AlterTableAddConstraint(cid_constraint))

        cid_col = dbops.Column(name='concept_id', type='integer',
                               required=True)

        if concept.name == 'std.BaseObject':
            alter_table.add_operation(dbops.AlterTableAddColumn(cid_col))

        constraint = dbops.PrimaryKey(
                        table_name=alter_table.name,
                        columns=['std.id'])
        alter_table.add_operation(
            dbops.AlterTableAddConstraint(constraint))

        bases = (common.concept_name_to_table_name(sn.Name(p), catenate=False)
                 for p in fields['base'][1])
        concept_table.bases = list(bases)

        self.affirm_pointer_defaults(concept, schema, context)

        self.attach_alter_table(context)

        if self.update_search_indexes:
            self.update_search_indexes.apply(schema, context)
            self.pgops.add(self.update_search_indexes)

        self.pgops.add(dbops.Comment(object=concept_table,
                                     text=self.prototype_name))

        return concept


class RenameConcept(ConceptMetaCommand, adapts=s_concepts.RenameConcept):
    def apply(self, schema, context=None):
        proto = s_concepts.RenameConcept.apply(self, schema, context)
        ConceptMetaCommand.apply(self, schema, context)

        concept = context.get(s_concepts.ConceptCommandContext)
        assert concept

        realm = context.get(s_realm.RealmCommandContext)
        assert realm

        realm.op._renames[concept.original_proto] = proto

        concept.op.attach_alter_table(context)

        self.rename(schema, context, self.prototype_name, self.new_name)

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


class RebaseConcept(ConceptMetaCommand, adapts=s_concepts.RebaseConcept):
    def apply(self, schema, context):
        result = s_concepts.RebaseConcept.apply(self, schema, context)
        ConceptMetaCommand.apply(self, schema, context)

        concept_ctx = context.get(s_concepts.ConceptCommandContext)
        source = concept_ctx.proto
        orig_source = concept_ctx.original_proto
        self.apply_base_delta(orig_source, source, schema, context)

        return result


class AlterConcept(ConceptMetaCommand, adapts=s_concepts.AlterConcept):
    def apply(self, schema, context=None):
        self.table_name = common.concept_name_to_table_name(self.prototype_name, catenate=False)
        concept = s_concepts.AlterConcept.apply(self, schema, context=context)
        ConceptMetaCommand.apply(self, schema, context)

        updaterec, updates = self.fill_record(schema)

        if updaterec:
            condition = [('name', str(concept.name))]
            self.pgops.add(dbops.Update(table=self.table, record=updaterec, condition=condition))

        self.attach_alter_table(context)

        if self.update_search_indexes:
            self.update_search_indexes.apply(schema, context)
            self.pgops.add(self.update_search_indexes)

        return concept


class DeleteConcept(ConceptMetaCommand, adapts=s_concepts.DeleteConcept):
    def apply(self, schema, context=None):
        old_table_name = common.concept_name_to_table_name(self.prototype_name, catenate=False)

        concept = s_concepts.DeleteConcept.apply(self, schema, context)
        ConceptMetaCommand.apply(self, schema, context)

        self.delete(schema, context, concept)

        self.pgops.add(dbops.DropTable(name=old_table_name, priority=3))

        return concept


class ActionCommand:
    table = deltadbops.ActionTable()


class CreateAction(CreateNamedPrototype,
                                 ActionCommand,
                                 adapts=s_policy.CreateAction):
    pass


class RenameAction(RenameNamedPrototype,
                                 ActionCommand,
                                 adapts=s_policy.RenameAction):
    pass


class AlterAction(AlterNamedPrototype,
                                ActionCommand,
                                adapts=s_policy.AlterAction):
    pass


class DeleteAction(DeleteNamedPrototype,
                                 ActionCommand,
                                 adapts=s_policy.DeleteAction):
    pass


class EventCommand(metaclass=CommandMeta):
    table = deltadbops.EventTable()


class CreateEvent(EventCommand,
                                CreateNamedPrototype,
                                adapts=s_policy.CreateEvent):
    pass


class RenameEvent(EventCommand,
                                RenameNamedPrototype,
                                adapts=s_policy.RenameEvent):
    pass


class RebaseEvent(EventCommand,
                                RebaseNamedPrototype,
                                adapts=s_policy.RebaseEvent):
    pass


class AlterEvent(EventCommand,
                               AlterNamedPrototype,
                               adapts=s_policy.AlterEvent):
    pass


class DeleteEvent(EventCommand,
                                DeleteNamedPrototype,
                                adapts=s_policy.DeleteEvent):
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
                                 adapts=s_policy.CreatePolicy):
    pass


class RenamePolicy(PolicyCommand,
                                 RenameNamedPrototype,
                                 adapts=s_policy.RenamePolicy):
    pass


class AlterPolicy(PolicyCommand,
                                AlterNamedPrototype,
                                adapts=s_policy.AlterPolicy):
    pass


class DeletePolicy(PolicyCommand,
                                 DeleteNamedPrototype,
                                 adapts=s_policy.DeletePolicy):
    pass


class ScheduleLinkMappingUpdate(MetaCommand):
    pass


class CancelLinkMappingUpdate(MetaCommand):
    pass


class PointerMetaCommand(MetaCommand):

    def get_host(self, schema, context):
        if context:
            link = context.get(s_links.LinkCommandContext)
            if link and isinstance(self, s_lprops.LinkPropertyCommand):
                return link
            concept = context.get(s_concepts.ConceptCommandContext)
            if concept:
                return concept

    def record_metadata(self, pointer, old_pointer, schema, context):
        rec, updates = self.fill_record(schema)

        if updates:
            if not rec:
                rec = self.table.record()

            host = self.get_host(schema, context)

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
                if isinstance(rec.base, sn.Name):
                    rec.base = str(rec.base)
                else:
                    rec.base = tuple(str(b) for b in rec.base)

        default = updates.get('default')
        if default:
            if not rec:
                rec = self.table.record()
            rec.default = self.pack_default(default[1])

        return rec, updates

    def alter_host_table_column(self, old_ptr, ptr, schema, context, old_type, new_type):

        dropped_atom = None

        for op in self(s_atoms.AtomCommand):
            for rename in op(s_atoms.RenameAtom):
                if old_type == rename.prototype_name and new_type == rename.new_name:
                    # Our target alter is a mere rename
                    return
            if isinstance(op, s_atoms.CreateAtom):
                if op.prototype_name == new_type:
                    # CreateAtom will take care of everything for us
                    return
            elif isinstance(op, s_atoms.DeleteAtom):
                if op.prototype_name == old_type:
                    # The former target atom might as well have been dropped
                    dropped_atom = op.old_prototype

        old_target = schema.get(old_type, dropped_atom)
        assert old_target
        new_target = schema.get(new_type)

        alter_table = context.get(s_concepts.ConceptCommandContext).op.get_alter_table(context,
                                                                                       priority=1)
        column_name = common.caos_name_to_pg_name(ptr.normal_name())

        if isinstance(new_target, s_atoms.Atom):
            target_type = types.pg_type_from_atom(schema, new_target)

            if isinstance(old_target, s_atoms.Atom):
                AlterAtom.alter_atom(self, schema, context, old_target, new_target, in_place=False)
                alter_type = dbops.AlterTableAlterColumnType(column_name, target_type)
                alter_table.add_operation(alter_type)
            else:
                cols = self.get_columns(ptr, schema)
                ops = [dbops.AlterTableAddColumn(col) for col in cols]
                for op in ops:
                    alter_table.add_operation(op)
        else:
            col = dbops.Column(name=column_name, type='text')
            alter_table.add_operation(dbops.AlterTableDropColumn(col))

    def get_pointer_default(self, link, schema, context):
        default = self.updates.get('default')
        default_value = None

        if default:
            default = default[1]
            if default:
                for d in default:
                    if isinstance(d, s_expr.ExpressionText):
                        default_value = schemamech.ptr_default_to_col_default(
                            schema, link, d)
                        if default_value is not None:
                            break
                    else:
                        default_value = postgresql.string.quote_literal(str(d))
                        break

        return default_value

    def alter_pointer_default(self, pointer, schema, context):
        default = self.updates.get('default')
        if default:
            default = default[1]

            new_default = None
            have_new_default = True

            if not default:
                new_default = None
            else:
                ld = list(filter(lambda i: not isinstance(i, s_expr.ExpressionText),
                                 default))
                if ld:
                    new_default = ld[0]
                else:
                    have_new_default = False

            if have_new_default:
                source_ctx, pointer_ctx = CompositePrototypeMetaCommand.\
                                                get_source_and_pointer_ctx(schema, context)
                alter_table = source_ctx.op.get_alter_table(context, contained=True, priority=3)
                column_name = common.caos_name_to_pg_name(pointer.normal_name())
                alter_table.add_operation(dbops.AlterTableAlterColumnDefault(column_name=column_name,
                                                                             default=new_default))

    def get_columns(self, pointer, schema, default=None):
        ptr_stor_info = types.get_pointer_storage_info(pointer, schema=schema)
        return [dbops.Column(name=ptr_stor_info.column_name,
                             type=ptr_stor_info.column_type,
                             required=pointer.required,
                             default=default, comment=pointer.normal_name())]

    def rename_pointer(self, pointer, schema, context, old_name, new_name):
        if context:
            old_name = pointer.normalize_name(old_name)
            new_name = pointer.normalize_name(new_name)

            host = self.get_host(schema, context)

            if host and old_name != new_name:
                if new_name == 'std.target' and pointer.atomic():
                    new_name += '@atom'

                if new_name.endswith('caos.builtins.source') and not host.proto.generic():
                    pass
                else:
                    old_col_name = common.caos_name_to_pg_name(old_name)
                    new_col_name = common.caos_name_to_pg_name(new_name)

                    ptr_stor_info = types.get_pointer_storage_info(
                                        pointer, schema=schema)

                    is_a_column = (
                        (ptr_stor_info.table_type == 'concept'
                                and isinstance(host.proto, s_concepts.Concept))
                        or (ptr_stor_info.table_type == 'link'
                                and isinstance(host.proto, s_links.Link))
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
    def has_table(cls, link, schema):
        if link.is_pure_computable():
            return False
        elif link.generic():
            if link.name == 'std.link':
                return True
            elif link.has_user_defined_properties():
                return True
            else:
                for l in link.children(schema):
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
    def _create_table(cls, link, schema, context, conditional=False,
                           create_bases=True, create_children=True):
        new_table_name = common.get_table_name(link, catenate=False)

        create_c = dbops.CommandGroup()

        constraints = []
        columns = []

        src_col = common.caos_name_to_pg_name('std.source')
        tgt_col = common.caos_name_to_pg_name('std.target')

        if link.name == 'std.link':
            columns.append(dbops.Column(name=src_col, type='uuid', required=True,
                                        comment='std.source'))
            columns.append(dbops.Column(name=tgt_col, type='uuid', required=False,
                                        comment='std.target'))
            columns.append(dbops.Column(name='link_type_id', type='integer', required=True))

        constraints.append(dbops.UniqueConstraint(table_name=new_table_name,
                                                  columns=[src_col, tgt_col, 'link_type_id']))

        if not link.generic() and link.atomic():
            try:
                tgt_prop = link.pointers['std.target']
            except KeyError:
                pass
            else:
                tgt_ptr = types.get_pointer_storage_info(
                                tgt_prop, schema=schema)
                columns.append(dbops.Column(name=tgt_ptr.column_name,
                                            type=tgt_ptr.column_type))

        table = dbops.Table(name=new_table_name)
        table.add_columns(columns)
        table.constraints = constraints

        if link.bases:
            bases = []

            for parent in link.bases:
                if isinstance(parent, s_obj.ProtoObject):
                    if create_bases:
                        bc = cls._create_table(parent, schema, context,
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
            for l_descendant in link.descendants(schema):
                if cls.has_table(l_descendant, schema):
                    lc = LinkMetaCommand._create_table(l_descendant, schema, context,
                            conditional=True, create_bases=False,
                            create_children=False)
                    create_c.add_command(lc)

        return create_c

    def create_table(self, link, schema, context, conditional=False):
        c = self._create_table(link, schema, context, conditional=conditional)
        self.pgops.add(c)

    def provide_table(self, link, schema, context):
        if not link.generic():
            gen_link = link.bases[0]

            if self.has_table(gen_link, schema):
                self.create_table(gen_link, schema, context, conditional=True)

        if self.has_table(link, schema):
            self.create_table(link, schema, context, conditional=True)

    def schedule_mapping_update(self, link, schema, context):
        if self.has_table(link, schema):
            mapping_indexes = context.get(s_realm.RealmCommandContext).op.update_mapping_indexes
            ops = mapping_indexes.links.get(link.name)
            if not ops:
                mapping_indexes.links[link.name] = ops = []
            ops.append((self, link))
            self.pgops.add(ScheduleLinkMappingUpdate())

    def cancel_mapping_update(self, link, schema, context):
        mapping_indexes = context.get(s_realm.RealmCommandContext).op.update_mapping_indexes
        mapping_indexes.links.pop(link.name, None)
        self.pgops.add(CancelLinkMappingUpdate())


class CreateLink(LinkMetaCommand, adapts=s_links.CreateLink):
    def apply(self, schema, context=None):
        # Need to do this early, since potential table alters triggered by sub-commands
        # need this.
        link = s_links.CreateLink.apply(self, schema, context)
        self.table_name = common.get_table_name(link, catenate=False)
        LinkMetaCommand.apply(self, schema, context)

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
        self.provide_table(link, schema, context)

        concept = context.get(s_concepts.ConceptCommandContext)
        rec, updates = self.record_metadata(link, None, schema, context)
        self.updates = updates

        if not link.generic():
            ptr_stor_info = types.get_pointer_storage_info(
                                link, resolve_type=False)

            concept = context.get(s_concepts.ConceptCommandContext)
            assert concept, "Link command must be run in Concept command context"

            if ptr_stor_info.table_type == 'concept':
                default_value = self.get_pointer_default(link, schema, context)

                cols = self.get_columns(link, schema, default_value)
                table_name = common.get_table_name(concept.proto, catenate=False)
                concept_alter_table = concept.op.get_alter_table(context)

                for col in cols:
                    # The column may already exist as inherited from parent table
                    cond = dbops.ColumnExists(table_name=table_name, column_name=col.name)
                    cmd = dbops.AlterTableAddColumn(col)
                    concept_alter_table.add_operation((cmd, None, (cond,)))

                if default_value is not None:
                    self.alter_pointer_default(link, schema, context)

                search = self.updates.get('search')
                if search:
                    search_conf = search[1]
                    concept.op.search_index_add(concept.proto, link,
                                                schema, context)

        if link.generic():
            self.affirm_pointer_defaults(link, schema, context)

        self.attach_alter_table(context)

        concept = context.get(s_concepts.ConceptCommandContext)
        self.pgops.add(dbops.Insert(table=self.table, records=[rec],
                                    priority=1))

        if not link.generic() and self.has_table(link, schema):
            alter_table = self.get_alter_table(context)
            constraint = dbops.PrimaryKey(
                            table_name=alter_table.name,
                            columns=['std.linkid'])
            alter_table.add_operation(
                dbops.AlterTableAddConstraint(constraint))

        if not link.generic() and link.mapping != s_links.LinkMapping.ManyToMany:
            self.schedule_mapping_update(link, schema, context)

        return link


class RenameLink(LinkMetaCommand, adapts=s_links.RenameLink):
    def apply(self, schema, context=None):
        result = s_links.RenameLink.apply(self, schema, context)
        LinkMetaCommand.apply(self, schema, context)

        self.rename_pointer(result, schema, context, self.prototype_name, self.new_name)

        self.attach_alter_table(context)

        if result.generic():
            link_cmd = context.get(s_links.LinkCommandContext)
            assert link_cmd

            self.rename(schema, context, self.prototype_name, self.new_name, obj=result)
            link_cmd.op.table_name = common.link_name_to_table_name(self.new_name, catenate=False)
        else:
            link_cmd = context.get(s_links.LinkCommandContext)

            if self.has_table(result, schema):
                self.rename(schema, context, self.prototype_name, self.new_name, obj=result)

        return result


class RebaseLink(LinkMetaCommand, adapts=s_links.RebaseLink):
    def apply(self, schema, context):
        result = s_links.RebaseLink.apply(self, schema, context)
        LinkMetaCommand.apply(self, schema, context)

        result.acquire_ancestor_inheritance(schema)

        link_ctx = context.get(s_links.LinkCommandContext)
        source = link_ctx.proto

        orig_source = link_ctx.original_proto

        if self.has_table(source, schema):
            self.apply_base_delta(orig_source, source, schema, context)

        return result


class AlterLink(LinkMetaCommand, adapts=s_links.AlterLink):
    def apply(self, schema, context=None):
        self.old_link = old_link = schema.get(self.prototype_name).copy()
        link = s_links.AlterLink.apply(self, schema, context)
        LinkMetaCommand.apply(self, schema, context)

        with context(s_links.LinkCommandContext(self, link)):
            rec, updates = self.record_metadata(link, old_link, schema, context)
            self.updates = updates

            self.provide_table(link, schema, context)

            if rec:
                self.pgops.add(dbops.Update(table=self.table, record=rec,
                                            condition=[('name', str(link.name))], priority=1))

            new_type = None
            for op in self(sd.AlterPrototypeProperty):
                if op.property == 'target':
                    new_type = op.new_value.prototype_name if op.new_value is not None else None
                    old_type = op.old_value.prototype_name if op.old_value is not None else None
                    break

            if new_type:
                if not isinstance(link.target, s_obj.ProtoObject):
                    link.target = schema.get(link.target)

            self.attach_alter_table(context)

            if not link.generic():
                self.adjust_pointer_storage(old_link, link, schema, context)

                old_ptr_stor_info = types.get_pointer_storage_info(
                                        old_link, schema=schema)
                ptr_stor_info = types.get_pointer_storage_info(
                                    link, schema=schema)
                if (old_ptr_stor_info.table_type == 'concept'
                        and ptr_stor_info.table_type == 'concept'
                        and link.required != self.old_link.required):
                    alter_table = context.get(s_concepts.ConceptCommandContext).op.get_alter_table(context)
                    column_name = common.caos_name_to_pg_name(link.normal_name())
                    alter_table.add_operation(dbops.AlterTableAlterColumnNull(column_name=column_name,
                                                                              null=not link.required))

                search = self.updates.get('search')
                if search:
                    concept = context.get(s_concepts.ConceptCommandContext)
                    search_conf = search[1]
                    if search[0] and search[1]:
                        concept.op.search_index_alter(concept.proto, link,
                                                      schema, context)
                    elif search[1]:
                        concept.op.search_index_add(concept.proto, link,
                                                    schema, context)
                    else:
                        concept.op.search_index_delete(concept.proto, link,
                                                       schema, context)

            if isinstance(link.target, s_atoms.Atom):
                self.alter_pointer_default(link, schema, context)

            if not link.generic() and old_link.mapping != link.mapping:
                self.schedule_mapping_update(link, schema, context)

        return link


class DeleteLink(LinkMetaCommand, adapts=s_links.DeleteLink):
    def apply(self, schema, context=None):
        result = s_links.DeleteLink.apply(self, schema, context)
        LinkMetaCommand.apply(self, schema, context)

        if not result.generic():
            ptr_stor_info = types.get_pointer_storage_info(result, schema=schema)
            concept = context.get(s_concepts.ConceptCommandContext)

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
        self.cancel_mapping_update(result, schema, context)

        if not result.generic() and result.mapping != s_links.LinkMapping.ManyToMany:
            self.schedule_mapping_update(result, schema, context)

        self.pgops.add(dbops.Delete(table=self.table,
                                    condition=[('name', str(result.name))]))

        return result


class LinkPropertyMetaCommand(NamedPrototypeMetaCommand, PointerMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = deltadbops.LinkPropertyTable()


class CreateLinkProperty(LinkPropertyMetaCommand, adapts=s_lprops.CreateLinkProperty):
    def apply(self, schema, context):
        property = s_lprops.CreateLinkProperty.apply(self, schema, context)
        LinkPropertyMetaCommand.apply(self, schema, context)

        link = context.get(s_links.LinkCommandContext)

        if link:
            generic_link = link.proto if link.proto.generic() else link.proto.bases[0]
        else:
            generic_link = None

        with context(s_lprops.LinkPropertyCommandContext(self, property)):
            rec, updates = self.record_metadata(property, None, schema, context)
            self.updates = updates

        if link and self.has_table(link.proto, schema):
            link.op.provide_table(link.proto, schema, context)
            alter_table = link.op.get_alter_table(context)

            default_value = self.get_pointer_default(property, schema, context)

            cols = self.get_columns(property, schema, default_value)
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

        concept = context.get(s_concepts.ConceptCommandContext)
        # Priority is set to 2 to make sure that INSERT is run after the host link
        # is INSERTed into caos.link.
        #
        self.pgops.add(dbops.Insert(table=self.table, records=[rec], priority=2))

        return property


class RenameLinkProperty(LinkPropertyMetaCommand, adapts=s_lprops.RenameLinkProperty):
    def apply(self, schema, context=None):
        result = s_lprops.RenameLinkProperty.apply(self, schema, context)
        LinkPropertyMetaCommand.apply(self, schema, context)

        self.rename_pointer(result, schema, context, self.prototype_name, self.new_name)

        return result


class AlterLinkProperty(LinkPropertyMetaCommand, adapts=s_lprops.AlterLinkProperty):
    def apply(self, schema, context=None):
        self.old_prop = old_prop = schema.get(self.prototype_name, type=self.prototype_class).copy()
        prop = s_lprops.AlterLinkProperty.apply(self, schema, context)
        LinkPropertyMetaCommand.apply(self, schema, context)

        with context(s_lprops.LinkPropertyCommandContext(self, prop)):
            rec, updates = self.record_metadata(prop, old_prop, schema, context)
            self.updates = updates

            if rec:
                self.pgops.add(dbops.Update(table=self.table, record=rec,
                                            condition=[('name', str(prop.name))], priority=1))

            if isinstance(prop.target, s_atoms.Atom) and \
                    isinstance(self.old_prop.target, s_atoms.Atom) and \
                    prop.required != self.old_prop.required:

                src_ctx = context.get(s_links.LinkCommandContext)
                src_op = src_ctx.op
                alter_table = src_op.get_alter_table(context, priority=5)
                column_name = common.caos_name_to_pg_name(prop.normal_name())
                if prop.required:
                    table = src_op._type_mech.get_table(src_ctx.proto, schema)
                    rec = table.record(**{column_name:dbops.Default()})
                    cond = [(column_name, None)]
                    update = dbops.Update(table, rec, cond, priority=4)
                    self.pgops.add(update)
                alter_table.add_operation(dbops.AlterTableAlterColumnNull(column_name=column_name,
                                                                          null=not prop.required))

            new_type = None
            for op in self(sd.AlterPrototypeProperty):
                if op.property == 'target' and prop.normal_name() not in \
                                {'std.source', 'std.target'}:
                    new_type = op.new_value.prototype_name if op.new_value is not None else None
                    old_type = op.old_value.prototype_name if op.old_value is not None else None
                    break

            if new_type:
                self.alter_host_table_column(old_prop, prop, schema, context, old_type, new_type)

            self.alter_pointer_default(prop, schema, context)

        return prop


class DeleteLinkProperty(LinkPropertyMetaCommand, adapts=s_lprops.DeleteLinkProperty):
    def apply(self, schema, context=None):
        property = s_lprops.DeleteLinkProperty.apply(self, schema, context)
        LinkPropertyMetaCommand.apply(self, schema, context)

        link = context.get(s_links.LinkCommandContext)

        if link:
            alter_table = link.op.get_alter_table(context)

            column_name = common.caos_name_to_pg_name(property.normal_name())
            # We don't really care about the type -- we're dropping the thing
            column_type = 'text'

            col = dbops.AlterTableDropColumn(dbops.Column(name=column_name, type=column_type))
            alter_table.add_operation(col)

        self.pgops.add(dbops.Delete(table=self.table, condition=[('name', str(property.name))]))

        return property


class CreateMappingIndexes(MetaCommand):
    def __init__(self, table_name, mapping, maplinks):
        super().__init__()

        key = str(table_name[1])
        if mapping == s_links.LinkMapping.OneToOne:
            # Each source can have only one target and
            # each target can have only one source
            sides = ('std.source', 'std.target')

        elif mapping == s_links.LinkMapping.OneToMany:
            # Each target can have only one source, but
            # one source can have many targets
            sides = ('std.target',)

        elif mapping == s_links.LinkMapping.ManyToOne:
            # Each source can have only one target, but
            # one target can have many sources
            sides = ('std.source',)

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
            raise s_err.SchemaError('could not interpret index %s' % index_name)

        mapping = m.group('mapping')

        m = self.idx_pred_re.match(index_predicate)
        if not m:
            raise s_err.SchemaError('could not interpret index %s predicate: %s' % \
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

    def apply(self, schema, context):
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
            table_name = common.link_name_to_table_name(
                link_name, catenate=False)

            new_indexes = {k: [] for k in
                           s_links.LinkMapping.__members__.values()}
            alter_indexes = {k: [] for k in
                             s_links.LinkMapping.__members__.values()}

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


class CommandContext(sd.CommandContext):
    def __init__(self, db, session=None):
        super().__init__()
        self.db = db
        self.session = session
        self.link_name_to_id_map = None

    def _get_link_map(self, reverse=False):
        link_ds = datasources.schema.links.ConceptLinks(self.db)
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


class CreateModule(CompositePrototypeMetaCommand, adapts=s_mod.CreateModule):
    def apply(self, schema, context):
        CompositePrototypeMetaCommand.apply(self, schema, context)
        module = s_mod.CreateModule.apply(self, schema, context)

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


class AlterModule(CompositePrototypeMetaCommand, adapts=s_mod.AlterModule):
    def apply(self, schema, context):
        self.table = deltadbops.ModuleTable()
        module = s_mod.AlterModule.apply(self, schema, context=context)
        CompositePrototypeMetaCommand.apply(self, schema, context)

        updaterec, updates = self.fill_record(schema)

        if updaterec:
            condition = [('name', str(module.name))]
            self.pgops.add(dbops.Update(table=self.table, record=updaterec, condition=condition))

        self.attach_alter_table(context)

        return module


class DeleteModule(CompositePrototypeMetaCommand, adapts=s_mod.DeleteModule):
    def apply(self, schema, context):
        CompositePrototypeMetaCommand.apply(self, schema, context)
        module = s_mod.DeleteModule.apply(self, schema, context)

        module_name = module.name
        schema_name = common.caos_module_name_to_schema_name(module_name)
        condition = dbops.SchemaExists(name=schema_name)

        cmd = dbops.CommandGroup()
        cmd.add_command(dbops.DropSchema(name=schema_name, conditions={condition}, priority=4))
        cmd.add_command(dbops.Delete(table=deltadbops.ModuleTable(),
                                     condition=[('name', str(module.name))]))

        self.pgops.add(cmd)

        return module


class AlterRealm(MetaCommand, adapts=s_realm.AlterRealm):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._renames = {}

    def apply(self, schema, context):
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

        s_realm.AlterRealm.apply(self, schema, context)
        MetaCommand.apply(self, schema)

        self.update_mapping_indexes.apply(schema, context)
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

    @classmethod
    def update_backend_info(cls):
        backendinfotable = deltadbops.BackendInfoTable()
        record = backendinfotable.record()
        record.format_version = BACKEND_FORMAT_VERSION
        condition = [('format_version', '<', BACKEND_FORMAT_VERSION)]
        return dbops.Merge(table=backendinfotable, record=record,
                           condition=condition)
