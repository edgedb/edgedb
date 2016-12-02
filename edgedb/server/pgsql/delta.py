##
# Copyright (c) 2008-2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import collections.abc
import itertools
import pickle
import re

from edgedb.lang import edgeql

from edgedb.lang.schema import attributes as s_attrs
from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import constraints as s_constr
from edgedb.lang.schema import database as s_db
from edgedb.lang.schema import delta as sd
from edgedb.lang.schema import error as s_err
from edgedb.lang.schema import expr as s_expr
from edgedb.lang.schema import functions as s_funcs
from edgedb.lang.schema import indexes as s_indexes
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import modules as s_mod
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import named as s_named
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import policy as s_policy

from metamagic import json

from edgedb.lang.common import ordered
from edgedb.lang.common.debug import debug
from edgedb.lang.common import markup, nlang

from edgedb.server.pgsql import common
from edgedb.server.pgsql import dbops, deltadbops, metaschema

from . import ast as pg_ast
from . import compiler
from . import codegen
from . import datasources
from . import schemamech
from . import types

BACKEND_FORMAT_VERSION = 30


class CommandMeta(sd.CommandMeta):
    pass


class MetaCommand(sd.Command, metaclass=CommandMeta):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pgops = ordered.OrderedSet()

    def apply(self, schema, context=None):
        for op in self.ops:
            self.pgops.add(op)

    @debug
    async def execute(self, context):
        """"""

        """LINE [delta.execute] EXECUTING
        repr(self)
        """
        for op in sorted(
                self.pgops, key=lambda i: getattr(i, 'priority', 0),
                reverse=True):
            await op.execute(context)

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


class ClassMetaCommand(MetaCommand, sd.ClassCommand):
    pass


class Record:
    def __init__(self, items):
        self._items = items

    def __iter__(self):
        return iter(self._items)

    def __len__(self):
        return len(self._items)

    def __repr__(self):
        return '<_Record {!r}>'.format(self._items)


class NamedClassMetaCommand(
        ClassMetaCommand, s_named.NamedClassCommand):
    op_priority = 0

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._type_mech = schemamech.TypeMech()

    def _get_name(self, value):
        if isinstance(value, s_obj.ClassRef):
            name = value.classname
        elif isinstance(value, s_named.NamedClass):
            name = value.name
        else:
            raise ValueError(
                'expecting a ClassRef or a '
                'NamedClass, got {!r}'.format(value))

        return name

    def _serialize_field(self, value, col):
        recvalue = None

        if isinstance(value, s_obj.ClassRef):
            result = value.classname

        elif isinstance(value, s_named.NamedClass):
            result = value.name

        elif isinstance(value, (s_obj.ClassSet, s_obj.ClassList)):
            result = [self._get_name(v) for v in value]

        elif isinstance(value, s_obj.ClassDict):
            result = {}

            for k, v in value.items():
                if isinstance(v, s_obj.Collection):
                    stypes = [self._get_name(st) for st in v.get_subtypes()]
                    v = (None, v.schema_name, stypes)
                else:
                    v = self._get_name(v)

                result[k] = v

        elif isinstance(value, s_obj.Collection):
            stypes = [self._get_name(st) for st in value.get_subtypes()]
            result = (None, value.schema_name, stypes)

        elif isinstance(value, sn.SchemaName):
            result = value
            recvalue = str(value)

        elif isinstance(value, nlang.WordCombination):
            result = value
            recvalue = json.dumps(value)

        elif isinstance(value, collections.abc.Mapping):
            # Other dicts are JSON'ed by default
            result = value
            recvalue = json.dumps(value)

        else:
            result = value

        if result is not value and recvalue is None:
            names = result
            if isinstance(names, list):
                recvalue = dbops.Query(
                    '''SELECT
                            array_agg(id ORDER BY st.i)
                        FROM
                            edgedb.NamedClass AS o,
                            UNNEST($1::text[]) WITH ORDINALITY AS st(t, i)
                        WHERE
                            o.name = st.t''', [names], type='uuid[]')

            elif (
                    isinstance(names, tuple) or col is not None and
                    col.type == 'edgedb.type_t'):
                if not isinstance(names, tuple):
                    names = (str(names), None, None)

                recvalue = dbops.Query(
                    '''SELECT ROW(
                            (SELECT id FROM edgedb.NamedClass WHERE name = $1),
                            $2,
                            (SELECT
                                    array_agg(id ORDER BY st.i)
                                FROM
                                    edgedb.NamedClass AS o,
                                    UNNEST($3::text[])
                                        WITH ORDINALITY AS st(t, i)
                                WHERE
                                    o.name = st.t)
                        )::edgedb.type_t''', names, type='edgedb.type_t')

            elif isinstance(names, dict):
                flattened = list(itertools.chain.from_iterable(names.items()))

                keys = [f for f in flattened[0::2]]
                jbo_args = (
                    '${:d}::text, q.types[{:d}]'.format(i + 2, i + 1)
                    for i in range(int(len(flattened) / 2)))

                names = []
                for f in flattened[1::2]:
                    if not isinstance(f, tuple):
                        f = Record((str(f), None, None))
                    else:
                        f = Record(f)

                    names.append(f)

                recvalue = dbops.Query(
                    '''(SELECT
                            jsonb_build_object({json_seq})
                        FROM
                            (SELECT array_agg(ROW(
                                (SELECT id FROM edgedb.NamedClass
                                WHERE name = t.type),
                                t.collection,
                                (SELECT array_agg(id) FROM edgedb.NamedClass
                                WHERE name = any(t.subtypes))
                                )::edgedb.type_t) AS types
                            FROM
                                UNNEST($1::edgedb.typedesc_t[]) AS t
                            ) AS q
                        )
                    '''.format(json_seq=', '.join(jbo_args)), [names] + keys,
                    type='jsonb')

            else:
                recvalue = dbops.Query(
                    '''(SELECT id FROM edgedb.NamedClass
                       WHERE name = $1)''', [names], type='uuid')

        elif recvalue is None:
            recvalue = result

        return result, recvalue

    def fill_record(self, schema):
        updates = {}

        rec = None
        table = self.table

        if isinstance(self, sd.CreateClass):
            fields = self.scls
        else:
            fields = self.get_struct_properties(schema)

        for name, value in fields.items():
            col = table.get_column(name)

            v1, refqry = self._serialize_field(value, col)

            updates[name] = v1
            if col is not None:
                if rec is None:
                    rec = table.record()
                setattr(rec, name, refqry)

        return rec, updates

    def pack_default(self, value):
        if value is not None:
            if isinstance(value, s_expr.ExpressionText):
                valtype = 'expr'
            else:
                valtype = 'literal'
            val = {'type': valtype, 'value': value}
            result = json.dumps(val)
        else:
            result = None
        return result

    def create_object(self, schema, scls):
        rec, updates = self.fill_record(schema)
        self.pgops.add(
            dbops.Insert(
                table=self.table, records=[rec], priority=self.op_priority))
        return updates

    def update(self, schema, context):
        updaterec, updates = self.fill_record(schema)

        if updaterec:
            condition = [('name', str(self.scls.name))]
            self.pgops.add(
                dbops.Update(
                    table=self.table, record=updaterec, condition=condition,
                    priority=self.op_priority))

        return updates

    def rename(self, schema, context, old_name, new_name):
        updaterec = self.table.record(name=str(new_name))
        condition = [('name', str(old_name))]
        self.pgops.add(
            dbops.Update(
                table=self.table, record=updaterec, condition=condition))

    def delete(self, schema, context, scls):
        self.pgops.add(
            dbops.Delete(
                table=self.table, condition=[('name', str(scls.name))]))


class CreateNamedClass(NamedClassMetaCommand):
    def apply(self, schema, context):
        obj = self.__class__.get_adaptee().apply(self, schema, context)
        NamedClassMetaCommand.apply(self, schema, context)
        updates = self.create_object(schema, obj)
        self.updates = updates
        return obj


class CreateOrAlterNamedClass(NamedClassMetaCommand):
    def apply(self, schema, context):
        existing = schema.get(self.classname, None)

        obj = self.__class__.get_adaptee().apply(self, schema, context)
        self.scls = obj
        NamedClassMetaCommand.apply(self, schema, context)

        if existing is None:
            updates = self.create_object(schema, obj)
            self.updates = updates
        else:
            self.updates = self.update(schema, context)
        return obj


class RenameNamedClass(NamedClassMetaCommand):
    def apply(self, schema, context):
        obj = self.__class__.get_adaptee().apply(self, schema, context)
        NamedClassMetaCommand.apply(self, schema, context)
        self.rename(schema, context, self.classname, self.new_name)
        return obj


class RebaseNamedClass(NamedClassMetaCommand):
    def apply(self, schema, context):
        obj = self.__class__.get_adaptee().apply(self, schema, context)
        NamedClassMetaCommand.apply(self, schema, context)
        return obj


class AlterNamedClass(NamedClassMetaCommand):
    def apply(self, schema, context):
        NamedClassMetaCommand.apply(self, schema, context)
        obj = self.__class__.get_adaptee().apply(self, schema, context)
        self.scls = obj
        self.updates = self.update(schema, context)
        return obj


class DeleteNamedClass(NamedClassMetaCommand):
    def apply(self, schema, context):
        obj = self.__class__.get_adaptee().apply(self, schema, context)
        NamedClassMetaCommand.apply(self, schema, context)
        self.delete(schema, context, obj)
        return obj


class AlterClassProperty(MetaCommand, adapts=sd.AlterClassProperty):
    pass


class FunctionCommand:
    table = metaschema.get_metaclass_table(s_funcs.Function)


class CreateFunction(
        FunctionCommand, CreateNamedClass, adapts=s_funcs.CreateFunction):
    pass


class RenameFunction(
        FunctionCommand, RenameNamedClass, adapts=s_funcs.RenameFunction):
    pass


class AlterFunction(
        FunctionCommand, AlterNamedClass, adapts=s_funcs.AlterFunction):
    pass


class DeleteFunction(
        FunctionCommand, DeleteNamedClass, adapts=s_funcs.DeleteFunction):
    pass


class AttributeCommand:
    table = metaschema.get_metaclass_table(s_attrs.Attribute)


class CreateAttribute(
        AttributeCommand, CreateNamedClass,
        adapts=s_attrs.CreateAttribute):
    op_priority = 1


class RenameAttribute(
        AttributeCommand, RenameNamedClass,
        adapts=s_attrs.RenameAttribute):
    pass


class AlterAttribute(
        AttributeCommand, AlterNamedClass, adapts=s_attrs.AlterAttribute):
    pass


class DeleteAttribute(
        AttributeCommand, DeleteNamedClass,
        adapts=s_attrs.DeleteAttribute):
    pass


class AttributeValueCommand(metaclass=CommandMeta):
    table = metaschema.get_metaclass_table(s_attrs.AttributeValue)
    op_priority = 1

    def fill_record(self, schema):
        rec, updates = super().fill_record(schema)

        if rec:
            subj = updates.get('subject')
            if subj:
                rec.subject = dbops.Query(
                    '(SELECT id FROM edgedb.NamedClass WHERE name = $1)',
                    [subj],
                    type='uuid')

            attribute = updates.get('attribute')
            if attribute:
                rec.attribute = dbops.Query(
                    '(SELECT id FROM edgedb.NamedClass WHERE name = $1)',
                    [attribute], type='uuid')

            value = updates.get('value')
            if value:
                rec.value = pickle.dumps(value)

        return rec, updates


class CreateAttributeValue(
        AttributeValueCommand, CreateOrAlterNamedClass,
        adapts=s_attrs.CreateAttributeValue):
    pass


class RenameAttributeValue(
        AttributeValueCommand, RenameNamedClass,
        adapts=s_attrs.RenameAttributeValue):
    pass


class AlterAttributeValue(
        AttributeValueCommand, AlterNamedClass,
        adapts=s_attrs.AlterAttributeValue):
    pass


class DeleteAttributeValue(
        AttributeValueCommand, DeleteNamedClass,
        adapts=s_attrs.DeleteAttributeValue):
    pass


class ConstraintCommand(metaclass=CommandMeta):
    table = metaschema.get_metaclass_table(s_constr.Constraint)
    op_priority = 3

    def fill_record(self, schema):
        rec, updates = super().fill_record(schema)

        if rec and False:
            # Write the original locally-defined expression
            # so that when the schema is introspected the
            # correct finalexpr is restored with scls
            # inheritance mechanisms.
            rec.finalexpr = rec.localfinalexpr

        return rec, updates


class CreateConstraint(
        ConstraintCommand, CreateNamedClass,
        adapts=s_constr.CreateConstraint):
    def apply(self, schema, context):
        constraint = super().apply(schema, context)

        subject = constraint.subject

        if subject is not None:
            schemac_to_backendc = \
                schemamech.ConstraintMech.\
                schema_constraint_to_backend_constraint
            bconstr = schemac_to_backendc(subject, constraint, schema)

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.create_ops())
            self.pgops.add(op)

        return constraint


class RenameConstraint(
        ConstraintCommand, RenameNamedClass,
        adapts=s_constr.RenameConstraint):
    def apply(self, schema, context):
        constr_ctx = context.get(s_constr.ConstraintCommandContext)
        assert constr_ctx
        orig_constraint = constr_ctx.original_class
        schemac_to_backendc = \
            schemamech.ConstraintMech.schema_constraint_to_backend_constraint
        orig_bconstr = schemac_to_backendc(
            orig_constraint.subject, orig_constraint, schema)

        constraint = super().apply(schema, context)

        subject = constraint.subject

        if subject is not None:
            bconstr = schemac_to_backendc(subject, constraint, schema)

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.rename_ops(orig_bconstr))
            self.pgops.add(op)

        return constraint


class AlterConstraint(
        ConstraintCommand, AlterNamedClass,
        adapts=s_constr.AlterConstraint):
    def _alter_finalize(self, schema, context, constraint):
        super()._alter_finalize(schema, context, constraint)

        subject = constraint.subject
        ctx = context.get(s_constr.ConstraintCommandContext)

        if subject is not None:
            schemac_to_backendc = \
                schemamech.ConstraintMech.\
                schema_constraint_to_backend_constraint

            bconstr = schemac_to_backendc(subject, constraint, schema)

            orig_constraint = ctx.original_class
            orig_bconstr = schemac_to_backendc(
                orig_constraint.subject, orig_constraint, schema)

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.alter_ops(orig_bconstr))
            self.pgops.add(op)

        return constraint


class DeleteConstraint(
        ConstraintCommand, DeleteNamedClass,
        adapts=s_constr.DeleteConstraint):
    def apply(self, schema, context):
        constraint = super().apply(schema, context)

        subject = constraint.subject

        if subject is not None:
            schemac_to_backendc = \
                schemamech.ConstraintMech.\
                schema_constraint_to_backend_constraint
            bconstr = schemac_to_backendc(subject, constraint, schema)

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.delete_ops())
            self.pgops.add(op)

        return constraint


class AtomMetaCommand(NamedClassMetaCommand):
    table = metaschema.get_metaclass_table(s_atoms.Atom)

    def is_sequence(self, schema, atom):
        seq = schema.get('std::sequence', default=None)
        return seq is not None and atom.issubclass(seq)

    def fill_record(self, schema):
        rec, updates = super().fill_record(schema)
        default = updates.get('default')
        if default:
            if not rec:
                rec = self.table.record()
            rec.default = self.pack_default(default)

        return rec, updates

    def alter_atom_type(self, atom, schema, new_type, intent):

        users = []

        for link in schema.get_objects(type='link'):
            if link.target and link.target.name == atom.name:
                users.append((link.source, link))

        domain_name = common.atom_name_to_domain_name(
            atom.name, catenate=False)

        new_constraints = atom.local_constraints
        base = types.get_atom_base(schema, atom)

        target_type = new_type

        schemac_to_backendc = \
            schemamech.ConstraintMech.\
            schema_constraint_to_backend_constraint

        if intent == 'alter':
            new_name = domain_name[0], domain_name[1] + '_tmp'
            self.pgops.add(dbops.RenameDomain(domain_name, new_name))
            target_type = common.qname(*domain_name)

            self.pgops.add(dbops.CreateDomain(
                name=domain_name, base=new_type))

            for constraint in new_constraints.values():
                bconstr = schemac_to_backendc(atom, constraint, schema)
                op = dbops.CommandGroup(priority=1)
                op.add_command(bconstr.create_ops())
                self.pgops.add(op)

            domain_name = new_name

        elif intent == 'create':
            self.pgops.add(dbops.CreateDomain(name=domain_name, base=base))

        for host_class, item_class in users:
            if isinstance(item_class, s_links.Link):
                name = item_class.shortname
            else:
                name = item_class.name

            table_name = common.get_table_name(host_class, catenate=False)
            column_name = common.edgedb_name_to_pg_name(name)

            alter_type = dbops.AlterTableAlterColumnType(
                column_name, target_type)
            alter_table = dbops.AlterTable(table_name)
            alter_table.add_operation(alter_type)
            self.pgops.add(alter_table)

        for child_atom in schema.get_objects(type='atom'):
            if [b.name for b in child_atom.bases] == [atom.name]:
                self.alter_atom_type(child_atom, schema, target_type, 'alter')

        if intent == 'drop':
            self.pgops.add(dbops.DropDomain(domain_name))


class CreateAtom(AtomMetaCommand, adapts=s_atoms.CreateAtom):
    def apply(self, schema, context=None):
        atom = s_atoms.CreateAtom.apply(self, schema, context)
        AtomMetaCommand.apply(self, schema, context)

        new_domain_name = common.atom_name_to_domain_name(
            atom.name, catenate=False)
        base = types.get_atom_base(schema, atom)

        updates = self.create_object(schema, atom)

        self.pgops.add(dbops.CreateDomain(name=new_domain_name, base=base))

        if self.is_sequence(schema, atom):
            seq_name = common.atom_name_to_sequence_name(
                atom.name, catenate=False)
            self.pgops.add(dbops.CreateSequence(name=seq_name))

        default = updates.get('default')
        if default:
            if (
                    default is not None and
                    not isinstance(default, s_expr.ExpressionText)):
                # We only care to support literal defaults here.  Supporting
                # defaults based on queries has no sense on the database level
                # since the database forbids queries for DEFAULT and pre-
                # calculating the value does not make sense either since the
                # whole point of query defaults is for them to be dynamic.
                self.pgops.add(
                    dbops.AlterDomainAlterDefault(
                        name=new_domain_name, default=default))

        return atom


class RenameAtom(AtomMetaCommand, adapts=s_atoms.RenameAtom):
    def apply(self, schema, context=None):
        scls = s_atoms.RenameAtom.apply(self, schema, context)
        AtomMetaCommand.apply(self, schema, context)

        domain_name = common.atom_name_to_domain_name(
            self.classname, catenate=False)
        new_domain_name = common.atom_name_to_domain_name(
            self.new_name, catenate=False)

        self.pgops.add(
            dbops.RenameDomain(name=domain_name, new_name=new_domain_name))
        self.rename(schema, context, self.classname, self.new_name)

        if self.is_sequence(schema, scls):
            seq_name = common.atom_name_to_sequence_name(
                self.classname, catenate=False)
            new_seq_name = common.atom_name_to_sequence_name(
                self.new_name, catenate=False)

            self.pgops.add(
                dbops.RenameSequence(name=seq_name, new_name=new_seq_name))

        return scls


class RebaseAtom(AtomMetaCommand, adapts=s_atoms.RebaseAtom):
    # Rebase is taken care of in AlterAtom
    pass


class AlterAtom(AtomMetaCommand, adapts=s_atoms.AlterAtom):
    def apply(self, schema, context=None):
        old_atom = schema.get(self.classname).copy()
        new_atom = s_atoms.AlterAtom.apply(self, schema, context)
        AtomMetaCommand.apply(self, schema, context)

        updaterec, updates = self.fill_record(schema)

        if updaterec:
            condition = [('name', str(new_atom.name))]
            self.pgops.add(
                dbops.Update(
                    table=self.table, record=updaterec, condition=condition))

        self.alter_atom(
            self, schema, context, old_atom, new_atom, updates=updates)

        return new_atom

    @classmethod
    def alter_atom(
            cls, op, schema, context, old_atom, new_atom, in_place=True,
            updates=None):

        old_base = types.get_atom_base(schema, old_atom)
        base = types.get_atom_base(schema, new_atom)

        domain_name = common.atom_name_to_domain_name(
            new_atom.name, catenate=False)

        new_type = None
        type_intent = 'alter'

        if not new_type and old_base != base:
            new_type = base

        if new_type:
            # The change of the underlying data type for domains is a complex
            # problem. There is no direct way in PostgreSQL to change the base
            # type of a domain. Instead, a new domain must be created, all
            # users of the old domain altered to use the new one, and then the
            # old domain dropped.  Obviously this recurses down to every child
            # domain.
            if in_place:
                op.alter_atom_type(
                    new_atom, schema, new_type, intent=type_intent)

        if type_intent != 'drop':
            if updates:
                default_delta = updates.get('default')
                if default_delta:
                    if (default_delta is None or
                            isinstance(default_delta, s_expr.ExpressionText)):
                        new_default = None
                    else:
                        new_default = default_delta

                    adad = dbops.AlterDomainAlterDefault(
                        name=domain_name, default=new_default)
                    op.pgops.add(adad)


class DeleteAtom(AtomMetaCommand, adapts=s_atoms.DeleteAtom):
    def apply(self, schema, context=None):
        atom = s_atoms.DeleteAtom.apply(self, schema, context)
        AtomMetaCommand.apply(self, schema, context)

        link = None
        if context:
            link = context.get(s_links.LinkCommandContext)

        ops = link.op.pgops if link else self.pgops

        old_domain_name = common.atom_name_to_domain_name(
            self.classname, catenate=False)

        # Domain dropping gets low priority since other things may
        # depend on it.
        cond = dbops.DomainExists(old_domain_name)
        ops.add(
            dbops.DropDomain(
                name=old_domain_name, conditions=[cond], priority=3))
        ops.add(
            dbops.Delete(
                table=self.table, condition=[(
                    'name', str(self.classname))]))

        if self.is_sequence(schema, atom):
            seq_name = common.atom_name_to_sequence_name(
                self.classname, catenate=False)
            self.pgops.add(dbops.DropSequence(name=seq_name))

        return atom


class UpdateSearchIndexes(MetaCommand):
    def __init__(self, host, **kwargs):
        super().__init__(**kwargs)
        self.host = host

    def get_index_name(self, host_table_name, language, index_class='default'):
        name = '%s_%s_%s_search_idx' % (
            host_table_name[1], language, index_class)
        return common.edgedb_name_to_pg_name(name)

    def apply(self, schema, context):
        if isinstance(self.host, s_concepts.Concept):
            columns = []

            names = sorted(self.host.pointers.keys())

            for link_name in names:
                for link in self.host.pointers[link_name]:
                    if getattr(link, 'search', None):
                        column_name = common.edgedb_name_to_pg_name(link_name)
                        columns.append(
                            dbops.TextSearchIndexColumn(
                                column_name, link.search.weight, 'english'))

            if columns:
                table_name = common.get_table_name(self.host, catenate=False)

                index_name = self.get_index_name(table_name, 'default')
                index = dbops.TextSearchIndex(
                    name=index_name, table_name=table_name, columns=columns)

                cond = dbops.IndexExists(
                    index_name=(table_name[0], index_name))
                op = dbops.DropIndex(index, conditions=(cond, ))
                self.pgops.add(op)
                op = dbops.CreateIndex(index=index)
                self.pgops.add(op)


class CompositeClassMetaCommand(NamedClassMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table_name = None
        self._multicommands = {}
        self.update_search_indexes = None

    def _get_multicommand(
            self, context, cmdtype, object_name, *, priority=0,
            force_new=False, manual=False, cmdkwargs={}):
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
            commands = list(
                itertools.chain.from_iterable(typecommands.values()))

            if commands:
                commands = sorted(commands, key=lambda i: i.priority)
                self.pgops.update(commands)

    def get_alter_table(
            self, context, priority=0, force_new=False, contained=False,
            manual=False, table_name=None):

        tabname = table_name if table_name else self.table_name

        if not tabname:
            assert self.__class__.context_class
            ctx = context.get(self.__class__.context_class)
            assert ctx
            tabname = common.get_table_name(ctx.scls, catenate=False)
            if table_name is None:
                self.table_name = tabname

        return self._get_multicommand(
            context, dbops.AlterTable, tabname, priority=priority,
            force_new=force_new, manual=manual,
            cmdkwargs={'contained': contained})

    def attach_alter_table(self, context):
        self._attach_multicommand(context, dbops.AlterTable)

    def rename(self, schema, context, old_name, new_name, obj=None):
        super().rename(schema, context, old_name, new_name)

        if obj is not None and isinstance(obj, s_links.Link):
            old_table_name = common.link_name_to_table_name(
                old_name, catenate=False)
            new_table_name = common.link_name_to_table_name(
                new_name, catenate=False)
        else:
            old_table_name = common.concept_name_to_table_name(
                old_name, catenate=False)
            new_table_name = common.concept_name_to_table_name(
                new_name, catenate=False)

        cond = dbops.TableExists(name=old_table_name)

        if old_name.module != new_name.module:
            self.pgops.add(
                dbops.AlterTableSetSchema(
                    old_table_name, new_table_name[0], conditions=(cond, )))
            old_table_name = (new_table_name[0], old_table_name[1])

            cond = dbops.TableExists(name=old_table_name)

        if old_name.name != new_name.name:
            self.pgops.add(
                dbops.AlterTableRenameTo(
                    old_table_name, new_table_name[1], conditions=(cond, )))

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
            if (
                    pointer.generic() or not pointer.atomic() or
                    not pointer.singular() or not pointer.default):
                continue

            default = None

            if not isinstance(pointer.default, s_expr.ExpressionText):
                default = pointer.default

            if default is not None:
                alter_table = self.get_alter_table(
                    context, priority=3, contained=True)
                column_name = common.edgedb_name_to_pg_name(pointer_name)
                alter_table.add_operation(
                    dbops.AlterTableAlterColumnDefault(
                        column_name=column_name, default=default))

    def adjust_pointer_storage(self, orig_pointer, pointer, schema, context):
        old_ptr_stor_info = types.get_pointer_storage_info(
            orig_pointer, schema=schema)
        new_ptr_stor_info = types.get_pointer_storage_info(
            pointer, schema=schema)

        old_target = orig_pointer.target
        new_target = pointer.target

        source_ctx = context.get(s_concepts.ConceptCommandContext)
        source_op = source_ctx.op

        type_change_ok = False

        if (old_target.name != new_target.name or
                old_ptr_stor_info.table_type != new_ptr_stor_info.table_type):

            for op in self.get_subcommands(type=s_atoms.AtomCommand):
                for rename in op(s_atoms.RenameAtom):
                    if (old_target.name == rename.classname and
                            new_target.name == rename.new_name):
                        # Our target alter is a mere rename
                        type_change_ok = True

                if isinstance(op, s_atoms.CreateAtom):
                    if op.classname == new_target.name:
                        # CreateAtom will take care of everything for us
                        type_change_ok = True

            if old_ptr_stor_info.table_type != new_ptr_stor_info.table_type:
                # The attribute is being moved from one table to another
                opg = dbops.CommandGroup(priority=1)
                at = source_op.get_alter_table(context, manual=True)

                if old_ptr_stor_info.table_type == 'concept':
                    pat = self.get_alter_table(context, manual=True)

                    # Moved from concept table to link table
                    col = dbops.Column(
                        name=old_ptr_stor_info.column_name,
                        type=old_ptr_stor_info.column_type)
                    at.add_command(dbops.AlterTableDropColumn(col))

                    newcol = dbops.Column(
                        name=new_ptr_stor_info.column_name,
                        type=new_ptr_stor_info.column_type)

                    cond = dbops.ColumnExists(
                        new_ptr_stor_info.table_name, column_name=newcol.name)

                    pat.add_command(
                        (dbops.AlterTableAddColumn(newcol), None, (cond, )))
                else:
                    otabname = common.get_table_name(
                        orig_pointer, catenate=False)
                    pat = self.get_alter_table(
                        context, manual=True, table_name=otabname)

                    oldcol = dbops.Column(
                        name=old_ptr_stor_info.column_name,
                        type=old_ptr_stor_info.column_type)

                    if oldcol.name != 'std::target':
                        pat.add_command(dbops.AlterTableDropColumn(oldcol))

                    # Moved from link to concept
                    cols = self.get_columns(pointer, schema)

                    for col in cols:
                        cond = dbops.ColumnExists(
                            new_ptr_stor_info.table_name, column_name=col.name)
                        op = (dbops.AlterTableAddColumn(col), None, (cond, ))
                        at.add_operation(op)

                opg.add_command(at)
                opg.add_command(pat)

                self.pgops.add(opg)

            else:
                if old_target != new_target and not type_change_ok:
                    if isinstance(old_target, s_atoms.Atom):
                        AlterAtom.alter_atom(
                            self, schema, context, old_target, new_target,
                            in_place=False)

                        alter_table = source_op.get_alter_table(
                            context, priority=1)
                        alter_type = dbops.AlterTableAlterColumnType(
                            old_ptr_stor_info.column_name,
                            types.pg_type_from_object(schema, new_target))
                        alter_table.add_operation(alter_type)

    def apply_base_delta(self, orig_source, source, schema, context):
        db_ctx = context.get(s_db.DatabaseCommandContext)
        orig_source.bases = [
            db_ctx.op._renames.get(b, b) for b in orig_source.bases
        ]

        dropped_bases = {b.name
                         for b in orig_source.bases
                         } - {b.name
                              for b in source.bases}

        if isinstance(source, s_concepts.Concept):
            nameconv = common.concept_name_to_table_name
            source_ctx = context.get(s_concepts.ConceptCommandContext)
            ptr_cmd = s_links.CreateLink
        else:
            nameconv = common.link_name_to_table_name
            source_ctx = context.get(s_links.LinkCommandContext)
            ptr_cmd = s_lprops.CreateLinkProperty

        alter_table = source_ctx.op.get_alter_table(context, force_new=True)

        if (isinstance(source, s_concepts.Concept) or
                source_ctx.op.has_table(source, schema)):

            source.acquire_ancestor_inheritance(schema)
            orig_source.acquire_ancestor_inheritance(schema)

            created_ptrs = set()
            for ptr in source_ctx.op.get_subcommands(type=ptr_cmd):
                created_ptrs.add(ptr.classname)

            inherited_aptrs = set()

            for base in source.bases:
                for ptr in base.pointers.values():
                    if ptr.atomic():
                        inherited_aptrs.add(ptr.shortname)

            added_inh_ptrs = inherited_aptrs - {
                p.shortname
                for p in orig_source.pointers.values()
            }

            for added_ptr in added_inh_ptrs - created_ptrs:
                ptr = source.pointers[added_ptr]
                ptr_stor_info = types.get_pointer_storage_info(
                    ptr, schema=schema)

                is_a_column = ((
                    ptr_stor_info.table_type == 'concept' and
                    isinstance(source, s_concepts.Concept)) or (
                        ptr_stor_info.table_type == 'link' and
                        isinstance(source, s_links.Link)))

                if is_a_column:
                    col = dbops.Column(
                        name=ptr_stor_info.column_name,
                        type=ptr_stor_info.column_type, required=ptr.required)
                    cond = dbops.ColumnExists(
                        table_name=source_ctx.op.table_name,
                        column_name=ptr_stor_info.column_name)
                    alter_table.add_operation(
                        (dbops.AlterTableAddColumn(col), None, (cond, )))

            if dropped_bases:
                alter_table_drop_parent = source_ctx.op.get_alter_table(
                    context, force_new=True)

                for dropped_base in dropped_bases:
                    parent_table_name = nameconv(
                        sn.Name(dropped_base), catenate=False)
                    op = dbops.AlterTableDropParent(
                        parent_name=parent_table_name)
                    alter_table_drop_parent.add_operation(op)

                dropped_ptrs = set(orig_source.pointers) - set(source.pointers)

                if dropped_ptrs:
                    alter_table_drop_ptr = source_ctx.op.get_alter_table(
                        context, force_new=True)

                    for dropped_ptr in dropped_ptrs:
                        ptr = orig_source.pointers[dropped_ptr]
                        ptr_stor_info = types.get_pointer_storage_info(
                            ptr, schema=schema)

                        is_a_column = ((
                            ptr_stor_info.table_type == 'concept' and
                            isinstance(source, s_concepts.Concept)) or (
                                ptr_stor_info.table_type == 'link' and
                                isinstance(source, s_links.Link)))

                        if is_a_column:
                            col = dbops.Column(
                                name=ptr_stor_info.column_name,
                                type=ptr_stor_info.column_type,
                                required=ptr.required)

                            cond = dbops.ColumnExists(
                                table_name=ptr_stor_info.table_name,
                                column_name=ptr_stor_info.column_name)
                            op = dbops.AlterTableDropColumn(col)
                            alter_table_drop_ptr.add_command(
                                (op, (cond, ), ()))

            current_bases = list(
                ordered.OrderedSet(b.name for b in orig_source.bases) -
                dropped_bases)

            new_bases = [b.name for b in source.bases]

            unchanged_order = list(
                itertools.takewhile(
                    lambda x: x[0] == x[1], zip(current_bases, new_bases)))

            old_base_order = current_bases[len(unchanged_order):]
            new_base_order = new_bases[len(unchanged_order):]

            if new_base_order:
                table_name = nameconv(source.name, catenate=False)
                alter_table_drop_parent = source_ctx.op.get_alter_table(
                    context, force_new=True)
                alter_table_add_parent = source_ctx.op.get_alter_table(
                    context, force_new=True)

                for base in old_base_order:
                    parent_table_name = nameconv(sn.Name(base), catenate=False)
                    cond = dbops.TableInherits(table_name, parent_table_name)
                    op = dbops.AlterTableDropParent(
                        parent_name=parent_table_name)
                    alter_table_drop_parent.add_operation((op, [cond], None))

                for added_base in new_base_order:
                    parent_table_name = nameconv(
                        sn.Name(added_base), catenate=False)
                    cond = dbops.TableInherits(table_name, parent_table_name)
                    op = dbops.AlterTableAddParent(
                        parent_name=parent_table_name)
                    alter_table_add_parent.add_operation((op, None, [cond]))


class SourceIndexCommand(ClassMetaCommand):
    pass


class CreateSourceIndex(
        SourceIndexCommand, adapts=s_indexes.CreateSourceIndex):
    def apply(self, schema, context=None):
        index = s_indexes.CreateSourceIndex.apply(self, schema, context)
        SourceIndexCommand.apply(self, schema, context)

        source = context.get(s_links.LinkCommandContext)
        if not source:
            source = context.get(s_concepts.ConceptCommandContext)
        table_name = common.get_table_name(source.scls, catenate=False)
        ir = edgeql.compile_fragment_to_ir(
            index.expr, schema, location='selector')

        ircompiler = compiler.SingletonExprIRCompiler()
        sql_tree = ircompiler.transform_to_sql_tree(ir, schema)
        sql_expr = codegen.SQLSourceGenerator.to_source(sql_tree)
        if isinstance(sql_tree, pg_ast.SequenceNode):
            # Trim the parentheses to avoid PostgreSQL choking on double
            # parentheses. since it expects only a single set around the column
            # list.
            sql_expr = sql_expr[1:-1]
        index_name = '{}_reg_idx'.format(index.name)
        pg_index = dbops.Index(
            name=index_name, table_name=table_name, expr=sql_expr,
            unique=False, inherit=True, metadata={'schemaname': index.name})
        self.pgops.add(dbops.CreateIndex(pg_index, priority=3))

        return index


class RenameSourceIndex(
        SourceIndexCommand, adapts=s_indexes.RenameSourceIndex):
    def apply(self, schema, context):
        index = s_indexes.RenameSourceIndex.apply(self, schema, context)
        SourceIndexCommand.apply(self, schema, context)

        subject = context.get(s_links.LinkCommandContext)
        if not subject:
            subject = context.get(s_concepts.ConceptCommandContext)
        orig_table_name = common.get_table_name(
            subject.original_class, catenate=False)

        index_ctx = context.get(s_indexes.SourceIndexCommandContext)
        new_index_name = '{}_reg_idx'.format(index.name)

        orig_idx = index_ctx.original_class
        orig_idx_name = '{}_reg_idx'.format(orig_idx.name)
        orig_pg_idx = dbops.Index(
            name=orig_idx_name, table_name=orig_table_name, inherit=True,
            metadata={'schemaname': index.name})

        rename = dbops.RenameIndex(orig_pg_idx, new_name=new_index_name)
        self.pgops.add(rename)

        return index


class AlterSourceIndex(SourceIndexCommand, adapts=s_indexes.AlterSourceIndex):
    def apply(self, schema, context=None):
        result = s_indexes.AlterSourceIndex.apply(self, schema, context)
        SourceIndexCommand.apply(self, schema, context)
        return result


class DeleteSourceIndex(
        SourceIndexCommand, adapts=s_indexes.DeleteSourceIndex):
    def apply(self, schema, context=None):
        index = s_indexes.DeleteSourceIndex.apply(self, schema, context)
        SourceIndexCommand.apply(self, schema, context)

        source = context.get(s_links.LinkCommandContext)
        if not source:
            source = context.get(s_concepts.ConceptCommandContext)

        if not isinstance(source.op, s_named.DeleteNamedClass):
            # We should not drop indexes when the host is being dropped since
            # the indexes are dropped automatically in this case.
            #
            table_name = common.get_table_name(source.scls, catenate=False)
            index_name = '{}_reg_idx'.format(index.name)
            index = dbops.Index(
                name=index_name, table_name=table_name, inherit=True)
            index_exists = dbops.IndexExists(
                (table_name[0], index.name_in_catalog))
            self.pgops.add(
                dbops.DropIndex(
                    index, priority=3, conditions=(index_exists, )))

        return index


class ConceptMetaCommand(CompositeClassMetaCommand):
    table = metaschema.get_metaclass_table(s_concepts.Concept)


class CreateConcept(ConceptMetaCommand, adapts=s_concepts.CreateConcept):
    def apply(self, schema, context=None):
        concept_props = self.get_struct_properties(schema)
        is_virtual = concept_props.get('is_virtual')
        if is_virtual:
            return s_concepts.CreateConcept.apply(self, schema, context)

        new_table_name = common.concept_name_to_table_name(
            self.classname, catenate=False)
        self.table_name = new_table_name
        concept_table = dbops.Table(name=new_table_name)
        self.pgops.add(dbops.CreateTable(table=concept_table))

        alter_table = self.get_alter_table(context)

        concept = s_concepts.CreateConcept.apply(self, schema, context)
        ConceptMetaCommand.apply(self, schema, context)

        fields = self.create_object(schema, concept)

        if concept.name.module != 'schema':
            constr_name = common.edgedb_name_to_pg_name(
                self.classname + '.class_check')

            constr_expr = dbops.Query("""
                SELECT '"std::__class__" = ' || quote_literal(id)
                FROM edgedb.concept WHERE name = $1
            """, [concept.name], type='text')

            cid_constraint = dbops.CheckConstraint(
                self.table_name, constr_name, constr_expr, inherit=False)
            alter_table.add_operation(
                dbops.AlterTableAddConstraint(cid_constraint))

            cid_col = dbops.Column(
                name='std::__class__', type='uuid', required=True)

            if concept.name == 'std::Object':
                alter_table.add_operation(dbops.AlterTableAddColumn(cid_col))

            constraint = dbops.PrimaryKey(
                table_name=alter_table.name, columns=['std::id'])
            alter_table.add_operation(
                dbops.AlterTableAddConstraint(constraint))

        cntn = common.concept_name_to_table_name

        bases = (
            dbops.Table(name=cntn(sn.Name(p), catenate=False))
            for p in fields['bases']
        )
        concept_table.add_bases(bases)

        self.affirm_pointer_defaults(concept, schema, context)

        self.attach_alter_table(context)

        if self.update_search_indexes:
            self.update_search_indexes.apply(schema, context)
            self.pgops.add(self.update_search_indexes)

        self.pgops.add(
            dbops.Comment(object=concept_table, text=self.classname))

        return concept


class RenameConcept(ConceptMetaCommand, adapts=s_concepts.RenameConcept):
    def apply(self, schema, context=None):
        scls = s_concepts.RenameConcept.apply(self, schema, context)
        ConceptMetaCommand.apply(self, schema, context)

        concept = context.get(s_concepts.ConceptCommandContext)
        assert concept

        db_ctx = context.get(s_db.DatabaseCommandContext)
        assert db_ctx

        db_ctx.op._renames[concept.original_class] = scls

        concept.op.attach_alter_table(context)

        self.rename(schema, context, self.classname, self.new_name)

        new_table_name = common.concept_name_to_table_name(
            self.new_name, catenate=False)
        concept_table = dbops.Table(name=new_table_name)
        self.pgops.add(dbops.Comment(object=concept_table, text=self.new_name))

        concept.op.table_name = common.concept_name_to_table_name(
            self.new_name, catenate=False)

        # Need to update all bits that reference concept name

        old_constr_name = common.edgedb_name_to_pg_name(
            self.classname + '.class_check')
        new_constr_name = common.edgedb_name_to_pg_name(
            self.new_name + '.class_check')

        alter_table = self.get_alter_table(context, manual=True)
        rc = dbops.AlterTableRenameConstraintSimple(
            alter_table.name, old_name=old_constr_name,
            new_name=new_constr_name)
        self.pgops.add(rc)

        self.table_name = common.concept_name_to_table_name(
            self.new_name, catenate=False)

        concept.original_class.name = scls.name

        return scls


class RebaseConcept(ConceptMetaCommand, adapts=s_concepts.RebaseConcept):
    def apply(self, schema, context):
        result = s_concepts.RebaseConcept.apply(self, schema, context)
        ConceptMetaCommand.apply(self, schema, context)

        concept_ctx = context.get(s_concepts.ConceptCommandContext)
        source = concept_ctx.scls
        orig_source = concept_ctx.original_class
        self.apply_base_delta(orig_source, source, schema, context)

        return result


class AlterConcept(ConceptMetaCommand, adapts=s_concepts.AlterConcept):
    def apply(self, schema, context=None):
        self.table_name = common.concept_name_to_table_name(
            self.classname, catenate=False)
        concept = s_concepts.AlterConcept.apply(self, schema, context=context)
        ConceptMetaCommand.apply(self, schema, context)

        updaterec, updates = self.fill_record(schema)

        if updaterec:
            condition = [('name', str(concept.name))]
            self.pgops.add(
                dbops.Update(
                    table=self.table, record=updaterec, condition=condition))

        self.attach_alter_table(context)

        if self.update_search_indexes:
            self.update_search_indexes.apply(schema, context)
            self.pgops.add(self.update_search_indexes)

        return concept


class DeleteConcept(ConceptMetaCommand, adapts=s_concepts.DeleteConcept):
    def apply(self, schema, context=None):
        old_table_name = common.concept_name_to_table_name(
            self.classname, catenate=False)

        concept = s_concepts.DeleteConcept.apply(self, schema, context)
        ConceptMetaCommand.apply(self, schema, context)

        self.delete(schema, context, concept)

        self.pgops.add(dbops.DropTable(name=old_table_name, priority=3))

        return concept


class ActionCommand:
    table = metaschema.get_metaclass_table(s_policy.Action)


class CreateAction(
        CreateNamedClass, ActionCommand, adapts=s_policy.CreateAction):
    pass


class RenameAction(
        RenameNamedClass, ActionCommand, adapts=s_policy.RenameAction):
    pass


class AlterAction(
        AlterNamedClass, ActionCommand, adapts=s_policy.AlterAction):
    pass


class DeleteAction(
        DeleteNamedClass, ActionCommand, adapts=s_policy.DeleteAction):
    pass


class EventCommand(metaclass=CommandMeta):
    table = metaschema.get_metaclass_table(s_policy.Event)


class CreateEvent(
        EventCommand, CreateNamedClass, adapts=s_policy.CreateEvent):
    pass


class RenameEvent(
        EventCommand, RenameNamedClass, adapts=s_policy.RenameEvent):
    pass


class RebaseEvent(
        EventCommand, RebaseNamedClass, adapts=s_policy.RebaseEvent):
    pass


class AlterEvent(
        EventCommand, AlterNamedClass, adapts=s_policy.AlterEvent):
    pass


class DeleteEvent(
        EventCommand, DeleteNamedClass, adapts=s_policy.DeleteEvent):
    pass


class PolicyCommand(metaclass=CommandMeta):
    table = metaschema.get_metaclass_table(s_policy.Policy)
    op_priority = 2

    def fill_record(self, schema):
        rec, updates = super().fill_record(schema)

        if rec:
            subj = updates.get('subject')
            if subj:
                rec.subject = dbops.Query(
                    '(SELECT id FROM edgedb.NamedClass WHERE name = $1)',
                    [subj],
                    type='uuid')

            event = updates.get('event')
            if event:
                rec.event = dbops.Query(
                    '(SELECT id FROM edgedb.NamedClass WHERE name = $1)',
                    [event],
                    type='uuid')

            actions = updates.get('actions')
            if actions:
                rec.actions = dbops.Query(
                    '''(SELECT array_agg(id)
                        FROM edgedb.NamedClass
                        WHERE name = any($1::text[]))''', [actions],
                    type='uuid[]')

        return rec, updates


class CreatePolicy(
        PolicyCommand, CreateNamedClass, adapts=s_policy.CreatePolicy):
    pass


class RenamePolicy(
        PolicyCommand, RenameNamedClass, adapts=s_policy.RenamePolicy):
    pass


class AlterPolicy(
        PolicyCommand, AlterNamedClass, adapts=s_policy.AlterPolicy):
    pass


class DeletePolicy(
        PolicyCommand, DeleteNamedClass, adapts=s_policy.DeletePolicy):
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

        default = updates.get('default')
        if default:
            if not rec:
                rec = self.table.record()
            rec.default = self.pack_default(default)

        return rec, updates

    def alter_host_table_column(
            self, old_ptr, ptr, schema, context, old_type, new_type):

        dropped_atom = None

        for op in self.get_subcommands(type=s_atoms.AtomCommand):
            for rename in op(s_atoms.RenameAtom):
                if (old_type == rename.classname and
                        new_type == rename.new_name):
                    # Our target alter is a mere rename
                    return
            if isinstance(op, s_atoms.CreateAtom):
                if op.classname == new_type:
                    # CreateAtom will take care of everything for us
                    return
            elif isinstance(op, s_atoms.DeleteAtom):
                if op.classname == old_type:
                    # The former target atom might as well have been dropped
                    dropped_atom = op.old_class

        old_target = schema.get(old_type, dropped_atom)
        assert old_target
        new_target = schema.get(new_type)

        alter_table = context.get(
            s_concepts.ConceptCommandContext).op.get_alter_table(
                context, priority=1)
        column_name = common.edgedb_name_to_pg_name(ptr.shortname)

        if isinstance(new_target, s_atoms.Atom):
            target_type = types.pg_type_from_atom(schema, new_target)

            if isinstance(old_target, s_atoms.Atom):
                AlterAtom.alter_atom(
                    self, schema, context, old_target, new_target,
                    in_place=False)
                alter_type = dbops.AlterTableAlterColumnType(
                    column_name, target_type)
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
            if default is not None:
                if isinstance(default, s_expr.ExpressionText):
                    default_value = schemamech.ptr_default_to_col_default(
                        schema, link, default)
                else:
                    default_value = common.quote_literal(
                        str(default))

        return default_value

    def alter_pointer_default(self, pointer, schema, context):
        default = self.updates.get('default')
        if default:
            new_default = None
            have_new_default = True

            if not default:
                new_default = None
            else:
                if not isinstance(default, s_expr.ExpressionText):
                    new_default = default
                else:
                    have_new_default = False

            if have_new_default:
                source_ctx, pointer_ctx = \
                    CompositeClassMetaCommand.get_source_and_pointer_ctx(
                        schema, context)
                alter_table = source_ctx.op.get_alter_table(
                    context, contained=True, priority=3)
                column_name = common.edgedb_name_to_pg_name(
                    pointer.shortname)
                alter_table.add_operation(
                    dbops.AlterTableAlterColumnDefault(
                        column_name=column_name, default=new_default))

    def get_columns(self, pointer, schema, default=None):
        ptr_stor_info = types.get_pointer_storage_info(pointer, schema=schema)
        return [
            dbops.Column(
                name=ptr_stor_info.column_name, type=ptr_stor_info.column_type,
                required=pointer.required, default=default,
                comment=pointer.shortname)
        ]

    def rename_pointer(self, pointer, schema, context, old_name, new_name):
        if context:
            old_name = pointer.get_shortname(old_name)
            new_name = pointer.get_shortname(new_name)

            host = self.get_host(schema, context)

            if host and old_name != new_name:
                if new_name == 'std::target' and pointer.atomic():
                    new_name += '@atom'

                if new_name.endswith('std::source') and not host.scls.generic(
                ):
                    pass
                else:
                    old_col_name = common.edgedb_name_to_pg_name(old_name)
                    new_col_name = common.edgedb_name_to_pg_name(new_name)

                    ptr_stor_info = types.get_pointer_storage_info(
                        pointer, schema=schema)

                    is_a_column = ((
                        ptr_stor_info.table_type == 'concept' and
                        isinstance(host.scls, s_concepts.Concept)) or (
                            ptr_stor_info.table_type == 'link' and
                            isinstance(host.scls, s_links.Link)))

                    if is_a_column:
                        table_name = common.get_table_name(
                            host.scls, catenate=False)
                        cond = [
                            dbops.ColumnExists(
                                table_name=table_name,
                                column_name=old_col_name)
                        ]
                        rename = dbops.AlterTableRenameColumn(
                            table_name, old_col_name, new_col_name,
                            conditions=cond)
                        self.pgops.add(rename)

                        tabcol = dbops.TableColumn(
                            table_name=table_name, column=dbops.Column(
                                name=new_col_name, type='str'))
                        self.pgops.add(dbops.Comment(tabcol, new_name))

        rec = self.table.record()
        rec.name = str(self.new_name)
        self.pgops.add(
            dbops.Update(
                table=self.table, record=rec, condition=[(
                    'name', str(self.classname))], priority=1))

    @classmethod
    def has_table(cls, link, schema):
        if link.is_pure_computable():
            return False
        elif link.generic():
            if link.name == 'std::link':
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
            return not link.atomic() or not link.singular(
            ) or link.has_user_defined_properties()


class LinkMetaCommand(CompositeClassMetaCommand, PointerMetaCommand):
    table = metaschema.get_metaclass_table(s_links.Link)

    @classmethod
    def _create_table(
            cls, link, schema, context, conditional=False, create_bases=True,
            create_children=True):
        new_table_name = common.get_table_name(link, catenate=False)

        create_c = dbops.CommandGroup()

        constraints = []
        columns = []

        src_col = common.edgedb_name_to_pg_name('std::source')
        tgt_col = common.edgedb_name_to_pg_name('std::target')

        if link.name == 'std::link':
            columns.append(
                dbops.Column(
                    name=src_col, type='uuid', required=True,
                    comment='std::source'))
            columns.append(
                dbops.Column(
                    name=tgt_col, type='uuid', required=False,
                    comment='std::target'))
            columns.append(
                dbops.Column(
                    name='link_type_id', type='uuid', required=True))

        constraints.append(
            dbops.UniqueConstraint(
                table_name=new_table_name,
                columns=[src_col, tgt_col, 'link_type_id']))

        if not link.generic() and link.atomic():
            try:
                tgt_prop = link.pointers['std::target']
            except KeyError:
                pass
            else:
                tgt_ptr = types.get_pointer_storage_info(
                    tgt_prop, schema=schema)
                columns.append(
                    dbops.Column(
                        name=tgt_ptr.column_name, type=tgt_ptr.column_type))

        table = dbops.Table(name=new_table_name)
        table.add_columns(columns)
        table.constraints = constraints

        if link.bases:
            bases = []

            for parent in link.bases:
                if isinstance(parent, s_obj.Class):
                    if create_bases:
                        bc = cls._create_table(
                            parent, schema, context, conditional=True,
                            create_children=False)
                        create_c.add_command(bc)

                    tabname = common.get_table_name(parent, catenate=False)
                    bases.append(dbops.Table(name=tabname))

            table.add_bases(bases)

        ct = dbops.CreateTable(table=table)

        index_name = common.edgedb_name_to_pg_name(
            str(link.name) + 'target_id_default_idx')
        index = dbops.Index(index_name, new_table_name, unique=False)
        index.add_columns([tgt_col])
        ci = dbops.CreateIndex(index)

        if conditional:
            c = dbops.CommandGroup(
                neg_conditions=[dbops.TableExists(new_table_name)])
        else:
            c = dbops.CommandGroup()

        trg = dbops.Trigger(
            name='link_target_check', table_name=new_table_name,
            events=['INSERT', 'UPDATE'], is_constraint=True,
            procedure=('edgedb', 'tgrf_validate_link_insert'))
        ctrg = dbops.CreateTrigger(trg)

        c.add_command(ct)
        c.add_command(ci)
        c.add_command(ctrg)

        c.add_command(dbops.Comment(table, link.name))

        create_c.add_command(c)

        if create_children:
            for l_descendant in link.descendants(schema):
                if cls.has_table(l_descendant, schema):
                    lc = LinkMetaCommand._create_table(
                        l_descendant, schema, context, conditional=True,
                        create_bases=False, create_children=False)
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
            mapping_indexes = context.get(
                s_db.DatabaseCommandContext).op.update_mapping_indexes
            ops = mapping_indexes.links.get(link.name)
            if not ops:
                mapping_indexes.links[link.name] = ops = []
            ops.append((self, link))
            self.pgops.add(ScheduleLinkMappingUpdate())

    def cancel_mapping_update(self, link, schema, context):
        mapping_indexes = context.get(
            s_db.DatabaseCommandContext).op.update_mapping_indexes
        mapping_indexes.links.pop(link.name, None)
        self.pgops.add(CancelLinkMappingUpdate())


class CreateLink(LinkMetaCommand, adapts=s_links.CreateLink):
    def apply(self, schema, context=None):
        # Need to do this early, since potential table alters triggered by
        # sub-commands need this.
        link = s_links.CreateLink.apply(self, schema, context)
        self.table_name = common.get_table_name(link, catenate=False)
        LinkMetaCommand.apply(self, schema, context)

        # We do not want to create a separate table for atomic links, unless
        # they have properties, or are non-singular, since those are stored
        # directly in the source table.
        #
        # Implicit derivative links also do not get their own table since
        # they're just a special case of the parent.
        #
        # On the other hand, much like with concepts we want all other links
        # to be in separate tables even if they do not define additional
        # properties. This is to allow for further schema evolution.
        #
        self.provide_table(link, schema, context)

        concept = context.get(s_concepts.ConceptCommandContext)
        rec, updates = self.record_metadata(link, None, schema, context)
        self.updates = updates

        if not link.generic():
            ptr_stor_info = types.get_pointer_storage_info(
                link, resolve_type=False)

            concept = context.get(s_concepts.ConceptCommandContext)

            if ptr_stor_info.table_type == 'concept':
                default_value = self.get_pointer_default(link, schema, context)

                cols = self.get_columns(link, schema, default_value)
                table_name = common.get_table_name(
                    concept.scls, catenate=False)
                concept_alter_table = concept.op.get_alter_table(context)

                for col in cols:
                    # The column may already exist as inherited from parent
                    # table.
                    cond = dbops.ColumnExists(
                        table_name=table_name, column_name=col.name)
                    cmd = dbops.AlterTableAddColumn(col)
                    concept_alter_table.add_operation((cmd, None, (cond, )))

                if default_value is not None:
                    self.alter_pointer_default(link, schema, context)

                search = self.updates.get('search')
                if search:
                    concept.op.search_index_add(
                        concept.scls, link, schema, context)

        if link.generic():
            self.affirm_pointer_defaults(link, schema, context)

        concept = context.get(s_concepts.ConceptCommandContext)
        self.pgops.add(
            dbops.Insert(table=self.table, records=[rec], priority=1))

        if not link.generic() and self.has_table(link, schema):
            alter_table = self.get_alter_table(context)
            constraint = dbops.PrimaryKey(
                table_name=alter_table.name, columns=['std::linkid'])
            alter_table.add_operation(
                dbops.AlterTableAddConstraint(constraint))

        self.attach_alter_table(context)

        if not link.generic(
        ) and link.mapping != s_links.LinkMapping.ManyToMany:
            self.schedule_mapping_update(link, schema, context)

        return link


class RenameLink(LinkMetaCommand, adapts=s_links.RenameLink):
    def apply(self, schema, context=None):
        result = s_links.RenameLink.apply(self, schema, context)
        LinkMetaCommand.apply(self, schema, context)

        self.rename_pointer(
            result, schema, context, self.classname, self.new_name)

        self.attach_alter_table(context)

        if result.generic():
            link_cmd = context.get(s_links.LinkCommandContext)
            assert link_cmd

            self.rename(
                schema, context, self.classname, self.new_name,
                obj=result)
            link_cmd.op.table_name = common.link_name_to_table_name(
                self.new_name, catenate=False)
        else:
            link_cmd = context.get(s_links.LinkCommandContext)

            if self.has_table(result, schema):
                self.rename(
                    schema, context, self.classname, self.new_name,
                    obj=result)

        return result


class RebaseLink(LinkMetaCommand, adapts=s_links.RebaseLink):
    def apply(self, schema, context):
        result = s_links.RebaseLink.apply(self, schema, context)
        LinkMetaCommand.apply(self, schema, context)

        result.acquire_ancestor_inheritance(schema)

        link_ctx = context.get(s_links.LinkCommandContext)
        source = link_ctx.scls

        orig_source = link_ctx.original_class

        if self.has_table(source, schema):
            self.apply_base_delta(orig_source, source, schema, context)

        return result


class AlterLink(LinkMetaCommand, adapts=s_links.AlterLink):
    def apply(self, schema, context=None):
        self.old_link = old_link = schema.get(self.classname).copy()
        link = s_links.AlterLink.apply(self, schema, context)
        LinkMetaCommand.apply(self, schema, context)

        with context(s_links.LinkCommandContext(self, link)):
            rec, updates = self.record_metadata(
                link, old_link, schema, context)
            self.updates = updates

            self.provide_table(link, schema, context)

            if rec:
                self.pgops.add(
                    dbops.Update(
                        table=self.table, record=rec, condition=[(
                            'name', str(link.name))], priority=1))

            new_type = None
            for op in self.get_subcommands(type=sd.AlterClassProperty):
                if op.property == 'target':
                    new_type = op.new_value.classname \
                        if op.new_value is not None else None
                    break

            if new_type:
                if not isinstance(link.target, s_obj.Class):
                    link.target = schema.get(link.target)

            self.attach_alter_table(context)

            if not link.generic():
                self.adjust_pointer_storage(old_link, link, schema, context)

                old_ptr_stor_info = types.get_pointer_storage_info(
                    old_link, schema=schema)
                ptr_stor_info = types.get_pointer_storage_info(
                    link, schema=schema)
                if (
                        old_ptr_stor_info.table_type == 'concept' and
                        ptr_stor_info.table_type == 'concept' and
                        link.required != self.old_link.required):
                    alter_table = context.get(
                        s_concepts.ConceptCommandContext).op.get_alter_table(
                            context)
                    column_name = common.edgedb_name_to_pg_name(
                        link.shortname)
                    alter_table.add_operation(
                        dbops.AlterTableAlterColumnNull(
                            column_name=column_name, null=not link.required))

                search = self.updates.get('search')
                if search:
                    concept = context.get(s_concepts.ConceptCommandContext)
                    concept.op.search_index_add(
                        concept.scls, link, schema, context)

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
            ptr_stor_info = types.get_pointer_storage_info(
                result, schema=schema)
            concept = context.get(s_concepts.ConceptCommandContext)

            name = result.shortname

            if ptr_stor_info.table_type == 'concept':
                # Only drop the column if the link was not reinherited in the
                # same delta.
                if name not in concept.scls.pointers:
                    # This must be a separate so that objects depending
                    # on this column can be dropped correctly.
                    #
                    alter_table = concept.op.get_alter_table(
                        context, manual=True, priority=2)
                    col = dbops.Column(
                        name=ptr_stor_info.column_name,
                        type=ptr_stor_info.column_type)
                    cond = dbops.ColumnExists(
                        table_name=concept.op.table_name, column_name=col.name)
                    col = dbops.AlterTableDropColumn(col)
                    alter_table.add_operation((col, [cond], []))
                    self.pgops.add(alter_table)

        old_table_name = common.get_table_name(result, catenate=False)
        condition = dbops.TableExists(name=old_table_name)
        self.pgops.add(
            dbops.DropTable(name=old_table_name, conditions=[condition]))
        self.cancel_mapping_update(result, schema, context)

        if not result.generic(
        ) and result.mapping != s_links.LinkMapping.ManyToMany:
            self.schedule_mapping_update(result, schema, context)

        self.pgops.add(
            dbops.Delete(
                table=self.table, condition=[('name', str(result.name))]))

        return result


class LinkPropertyMetaCommand(NamedClassMetaCommand, PointerMetaCommand):
    table = metaschema.get_metaclass_table(s_lprops.LinkProperty)


class CreateLinkProperty(
        LinkPropertyMetaCommand, adapts=s_lprops.CreateLinkProperty):
    def apply(self, schema, context):
        property = s_lprops.CreateLinkProperty.apply(self, schema, context)
        LinkPropertyMetaCommand.apply(self, schema, context)

        link = context.get(s_links.LinkCommandContext)

        with context(s_lprops.LinkPropertyCommandContext(self, property)):
            rec, updates = self.record_metadata(
                property, None, schema, context)
            self.updates = updates

        if link and self.has_table(link.scls, schema):
            link.op.provide_table(link.scls, schema, context)
            alter_table = link.op.get_alter_table(context)

            default_value = self.get_pointer_default(property, schema, context)

            cols = self.get_columns(property, schema, default_value)

            for col in cols:
                # The column may already exist as inherited from parent table
                cond = dbops.ColumnExists(
                    table_name=alter_table.name, column_name=col.name)

                if property.required:
                    # For some reason, Postgres allows dropping NOT NULL
                    # constraints from inherited columns, but we really should
                    # only always increase constraints down the inheritance
                    # chain.
                    cmd = dbops.AlterTableAlterColumnNull(
                        column_name=col.name, null=not property.required)
                    alter_table.add_operation((cmd, (cond, ), None))

                cmd = dbops.AlterTableAddColumn(col)
                alter_table.add_operation((cmd, None, (cond, )))

        # Priority is set to 2 to make sure that INSERT is run after the host
        # link is INSERTed into edgedb.link.
        self.pgops.add(
            dbops.Insert(table=self.table, records=[rec], priority=2))

        return property


class RenameLinkProperty(
        LinkPropertyMetaCommand, adapts=s_lprops.RenameLinkProperty):
    def apply(self, schema, context=None):
        result = s_lprops.RenameLinkProperty.apply(self, schema, context)
        LinkPropertyMetaCommand.apply(self, schema, context)

        self.rename_pointer(
            result, schema, context, self.classname, self.new_name)

        return result


class AlterLinkProperty(
        LinkPropertyMetaCommand, adapts=s_lprops.AlterLinkProperty):
    def apply(self, schema, context=None):
        self.old_prop = old_prop = schema.get(
            self.classname, type=self.metaclass).copy()
        prop = s_lprops.AlterLinkProperty.apply(self, schema, context)
        LinkPropertyMetaCommand.apply(self, schema, context)

        with context(s_lprops.LinkPropertyCommandContext(self, prop)):
            rec, updates = self.record_metadata(
                prop, old_prop, schema, context)
            self.updates = updates

            if rec:
                self.pgops.add(
                    dbops.Update(
                        table=self.table, record=rec, condition=[(
                            'name', str(prop.name))], priority=1))

            if isinstance(prop.target, s_atoms.Atom) and \
                    isinstance(self.old_prop.target, s_atoms.Atom) and \
                    prop.required != self.old_prop.required:

                src_ctx = context.get(s_links.LinkCommandContext)
                src_op = src_ctx.op
                alter_table = src_op.get_alter_table(context, priority=5)
                column_name = common.edgedb_name_to_pg_name(prop.shortname)
                if prop.required:
                    table = src_op._type_mech.get_table(src_ctx.scls, schema)
                    rec = table.record(**{column_name: dbops.Default()})
                    cond = [(column_name, None)]
                    update = dbops.Update(table, rec, cond, priority=4)
                    self.pgops.add(update)
                alter_table.add_operation(
                    dbops.AlterTableAlterColumnNull(
                        column_name=column_name, null=not prop.required))

            new_type = None
            for op in self.get_subcommands(type=sd.AlterClassProperty):
                if (op.property == 'target' and
                        prop.shortname not in
                        {'std::source', 'std::target'}):
                    new_type = op.new_value.classname \
                        if op.new_value is not None else None
                    old_type = op.old_value.classname \
                        if op.old_value is not None else None
                    break

            if new_type:
                self.alter_host_table_column(
                    old_prop, prop, schema, context, old_type, new_type)

            self.alter_pointer_default(prop, schema, context)

        return prop


class DeleteLinkProperty(
        LinkPropertyMetaCommand, adapts=s_lprops.DeleteLinkProperty):
    def apply(self, schema, context=None):
        property = s_lprops.DeleteLinkProperty.apply(self, schema, context)
        LinkPropertyMetaCommand.apply(self, schema, context)

        link = context.get(s_links.LinkCommandContext)

        if link:
            alter_table = link.op.get_alter_table(context)

            column_name = common.edgedb_name_to_pg_name(property.shortname)
            # We don't really care about the type -- we're dropping the thing
            column_type = 'text'

            col = dbops.AlterTableDropColumn(
                dbops.Column(name=column_name, type=column_type))
            alter_table.add_operation(col)

        self.pgops.add(
            dbops.Delete(
                table=self.table, condition=[('name', str(property.name))]))

        return property


class CreateMappingIndexes(MetaCommand):
    def __init__(self, table_name, mapping, maplinks):
        super().__init__()

        key = str(table_name[1])
        if mapping == s_links.LinkMapping.OneToOne:
            # Each source can have only one target and
            # each target can have only one source
            sides = ('std::source', 'std::target')

        elif mapping == s_links.LinkMapping.OneToMany:
            # Each target can have only one source, but
            # one source can have many targets
            sides = ('std::target', )

        elif mapping == s_links.LinkMapping.ManyToOne:
            # Each source can have only one target, but
            # one target can have many sources
            sides = ('std::source', )

        else:
            sides = ()

        for side in sides:
            index = deltadbops.MappingIndex(
                key + '_%s' % side, mapping, maplinks, table_name)
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
        group = dbops.CommandGroup(conditions=(table_exists, ), priority=3)

        for idx_name in idx_names:
            idx = dbops.Index(name=idx_name, table_name=table_name)
            fq_idx_name = (table_name[0], idx_name)
            index_exists = dbops.IndexExists(fq_idx_name)
            drop = dbops.DropIndex(
                idx, conditions=(index_exists, ), priority=3)
            group.add_command(drop)

        self.pgops.add(group)


class UpdateMappingIndexes(MetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.links = {}
        self.idx_name_re = re.compile(
            r'.*(?P<mapping>[1*]{2})_link_mapping_idx$')
        self.idx_pred_re = re.compile(
            r'''
                              \( \s* link_type_id \s* = \s*
                                  (?:(?: ANY \s* \( \s* ARRAY \s* \[
                                      (?P<type_ids> \d+ (?:\s* , \s* \d+)* )
                                  \s* \] \s* \) \s* )
                                  |
                                  (?P<type_id>\d+))
                              \s* \)
                           ''', re.X)
        self.schema_exists = dbops.SchemaExists(name='edgedb')

    def interpret_index(self, index, link_map):
        index_name = index.name
        index_predicate = index.predicate
        m = self.idx_name_re.match(index_name)
        if not m:
            raise s_err.SchemaError(
                'could not interpret index %s' % index_name)

        mapping = m.group('mapping')

        m = self.idx_pred_re.match(index_predicate)
        if not m:
            raise s_err.SchemaError(
                'could not interpret index {} predicate: {}'.format(
                    (index_name, index_predicate)))

        link_type_ids = (
            int(i)
            for i in re.split(
                r'\D+', m.group('type_ids') or m.group('type_id')))

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
        """Group indexes by link name."""
        for index_name, (mapping, link_names) in indexes:
            for link_name in link_names:
                yield link_name, index_name

    def group_indexes(self, indexes):
        key = lambda i: i[0]
        grouped = itertools.groupby(
            sorted(self._group_indexes(indexes), key=key), key=key)
        for link_name, indexes in grouped:
            yield link_name, tuple(i[1] for i in indexes)

    async def apply(self, schema, context):
        db = context.db
        if await self.schema_exists.execute(context):
            link_map = await context._get_link_map(reverse=True)
            index_ds = datasources.introspection.tables.TableIndexes(db)
            indexes = {}
            idx_data = await index_ds.fetch(
                schema_pattern='edgedb%', index_pattern='%_link_mapping_idx')
            for row in idx_data:
                table_name = tuple(row['table_name'])
                indexes[table_name] = self.interpret_indexes(
                    table_name, row['indexes'], link_map)
        else:
            link_map = {}
            indexes = {}

        for link_name, ops in self.links.items():
            table_name = common.link_name_to_table_name(
                link_name, catenate=False)

            new_indexes = {
                k: []
                for k in s_links.LinkMapping.__members__.values()
            }
            alter_indexes = {
                k: []
                for k in s_links.LinkMapping.__members__.values()
            }

            existing = indexes.get(table_name)

            if existing:
                existing_by_name = dict(existing)
                existing = dict(self.group_indexes(existing_by_name.items()))
            else:
                existing_by_name = {}
                existing = {}

            processed = {}

            for op, scls in ops:
                already_processed = processed.get(scls.name)

                if isinstance(op, CreateLink):
                    # CreateLink can only happen once
                    if already_processed:
                        raise RuntimeError('duplicate CreateLink: {}'.format(
                            scls.name))

                    new_indexes[scls.mapping].append(
                        (scls.name, None, None))

                elif isinstance(op, AlterLink):
                    # We are in apply stage, so the potential link changes,
                    # renames have not yet been pushed to the database, so
                    # link_map potentially contains old link names.
                    ex_idx_names = existing.get(op.old_link.name)

                    if ex_idx_names:
                        ex_idx = existing_by_name[ex_idx_names[0]]
                        queue = alter_indexes
                    else:
                        ex_idx = None
                        queue = new_indexes

                    item = (scls.name, op.old_link.name, ex_idx_names)

                    # Delta generator could have yielded several AlterLink
                    # commands for the same link, we need to respect only the
                    # last state.
                    if already_processed:
                        if already_processed != scls.mapping:
                            queue[already_processed].remove(item)

                            if not ex_idx or ex_idx[0] != scls.mapping:
                                queue[scls.mapping].append(item)

                    elif not ex_idx or ex_idx[0] != scls.mapping:
                        queue[scls.mapping].append(item)

                processed[scls.name] = scls.mapping

            for mapping, maplinks in new_indexes.items():
                if maplinks:
                    maplinks = list(i[0] for i in maplinks)
                    self.pgops.add(
                        CreateMappingIndexes(table_name, mapping, maplinks))

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
                    self.pgops.add(
                        CreateMappingIndexes(table_name, mapping, new))

                for idx_names, altlinks in alter.items():
                    if not altlinks:
                        self.pgops.add(
                            DropMappingIndexes(
                                ex_idx_names, table_name, mapping))
                    else:
                        self.pgops.add(
                            AlterMappingIndexes(
                                idx_names, table_name, mapping, altlinks))


class CommandContext(sd.CommandContext):
    def __init__(self, db, session=None):
        super().__init__()
        self.db = db
        self.session = session
        self.link_name_to_id_map = None

    async def _get_link_map(self, reverse=False):
        link_ds = datasources.schema.links.ConceptLinks(self.db)
        links = await link_ds.fetch()
        grouped = itertools.groupby(links, key=lambda i: i['id'])
        if reverse:
            link_map = {k: next(i)['name'] for k, i in grouped}
        else:
            link_map = {next(i)['name']: k for k, i in grouped}
        return link_map

    async def get_link_map(self):
        link_map = self.link_name_to_id_map
        if not link_map:
            link_map = await self._get_link_map()
            self.link_name_to_id_map = link_map
        return link_map


class ModuleMetaCommand(NamedClassMetaCommand):
    table = metaschema.get_metaclass_table(s_mod.Module)


class CreateModule(ModuleMetaCommand, adapts=s_mod.CreateModule):
    def apply(self, schema, context):
        CompositeClassMetaCommand.apply(self, schema, context)
        self.scls = module = s_mod.CreateModule.apply(self, schema, context)

        module_name = module.name
        schema_name = common.edgedb_module_name_to_schema_name(module_name)
        condition = dbops.SchemaExists(name=schema_name)

        cmd = dbops.CommandGroup(neg_conditions={condition})
        cmd.add_command(dbops.CreateSchema(name=schema_name))
        self.pgops.add(cmd)

        self.create_object(schema, module)

        return module


class AlterModule(ModuleMetaCommand, adapts=s_mod.AlterModule):
    def apply(self, schema, context):
        module = s_mod.AlterModule.apply(self, schema, context=context)
        CompositeClassMetaCommand.apply(self, schema, context)

        updaterec, updates = self.fill_record(schema)

        if updaterec:
            condition = [('name', str(module.name))]
            self.pgops.add(
                dbops.Update(
                    table=self.table, record=updaterec, condition=condition))

        self.attach_alter_table(context)

        return module


class DeleteModule(ModuleMetaCommand, adapts=s_mod.DeleteModule):
    def apply(self, schema, context):
        CompositeClassMetaCommand.apply(self, schema, context)
        module = s_mod.DeleteModule.apply(self, schema, context)

        module_name = module.name
        schema_name = common.edgedb_module_name_to_schema_name(module_name)
        condition = dbops.SchemaExists(name=schema_name)

        cmd = dbops.CommandGroup()
        cmd.add_command(
            dbops.DropSchema(
                name=schema_name, conditions={condition}, priority=4))
        cmd.add_command(
            dbops.Delete(
                table=self.table, condition=[(
                    'name', str(module.name))]))

        self.pgops.add(cmd)

        return module


class CreateDatabase(MetaCommand, adapts=s_db.CreateDatabase):
    def apply(self, schema, context):
        s_db.CreateDatabase.apply(self, schema, context)
        self.pgops.add(dbops.CreateDatabase(dbops.Database(self.name)))


class AlterDatabase(MetaCommand, adapts=s_db.AlterDatabase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._renames = {}

    def apply(self, schema, context):
        self.update_mapping_indexes = UpdateMappingIndexes()

        s_db.AlterDatabase.apply(self, schema, context)
        MetaCommand.apply(self, schema)

        # self.update_mapping_indexes.apply(schema, context)
        self.pgops.add(self.update_mapping_indexes)

    def is_material(self):
        return True

    async def execute(self, context):
        for op in self.serialize_ops():
            await op.execute(context)

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


class DropDatabase(MetaCommand, adapts=s_db.DropDatabase):
    def apply(self, schema, context):
        s_db.CreateDatabase.apply(self, schema, context)
        self.pgops.add(dbops.DropDatabase(self.name))
