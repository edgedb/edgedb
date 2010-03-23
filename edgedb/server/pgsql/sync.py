##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import hashlib
import os
import socket
import re

import postgresql

from semantix import caos
from semantix.caos import proto

from semantix.caos.backends.pgsql import common

from semantix.utils import datastructures
from semantix.utils.debug import debug


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


class Prototype:
    @classmethod
    def fill_record(cls, rec, delta):
        updates = {}
        rec_updates = {}

        for d in delta(proto.PrototypeFieldDelta):
            updates[d.name] = d
            if hasattr(rec, d.name):
                setattr(rec, d.name, d.new)
                rec_updates[d.name] = d

        rec.name = str(rec.name)

        if rec.title:
            rec.title = rec.title.as_dict()

        if rec.description:
            rec.description = rec.description.as_dict()

        return updates, rec_updates


class Atom(Prototype, proto.Atom):
    @classmethod
    def alter_object(cls, plan, delta):
        old_name, new_name = delta.old, delta.new

        if old_name:
            old_domain_name = common.atom_name_to_domain_name(old_name, catenate=False)
        else:
            old_domain_name = None

        if new_name:
            new_domain_name = common.atom_name_to_domain_name(new_name, catenate=False)
        else:
            new_domain_name = None

        table = AtomTable()

        if not new_name:
            # Drop it
            plan.add(DropDomain(name=old_domain_name))
            plan.add(Delete(table=table, condition=[('name', str(old_name))]))
            return plan

        elif not old_name:
            # Create it

            rec = table.record(name=str(new_name))
            updates, _ = cls.fill_record(rec, delta)

            plan.add(Insert(table=table, records=[rec]))
            create_schema = CreateSchema(name=new_domain_name[0])
            if create_schema not in plan:
                plan.add(create_schema)

            mods = {d.new.__class__.get_canonical_class(): d.new for d in delta(proto.AtomModDelta)}
            base, _, _, mods = cls.get_atom_base_and_mods(delta.new_context)

            plan.add(CreateDomain(name=new_domain_name, base=base))

            for mod in mods:
                plan.add(AlterDomainAddConstraint(name=new_domain_name, constraint=mod))

            default_delta = updates.get('default')

            if default_delta and default_delta.new is not None:
                plan.add(AlterDomainAlterDefault(name=new_domain_name, default=default_delta.new))

        else:
            # Alter it

            updaterec = table.record()
            updates, rec_updates = cls.fill_record(updaterec, delta)

            if rec_updates:
                condition = [('name', str(new_name))]
                plan.add(Update(table=table, record=updaterec, condition=condition))

            if old_name and old_name != new_name:
                plan.add(RenameDomain(name=old_domain_name, new_name=new_domain_name))
                updaterec = table.record(name=str(new_name))
                condition = [('name', str(old_name))]
                plan.add(Update(table=table, record=updaterec, condition=condition))

            old_mods = {}
            new_mods = {}

            for d in delta.diff:
                if isinstance(d, proto.AtomModDelta):
                    if not d.new:
                        old_mods[d.old.__class__.get_canonical_class()] = d.old
                    else:
                        new_mods[d.new.__class__.get_canonical_class()] = d.new

            base = updates.get('base')
            if base:
                old_base, new_base = base.old, base.new
            else:
                old_base, new_base = delta.old_context.base, delta.new_context.base

            base, _, old_max_length, old_mods = cls.get_atom_base_and_mods(delta.old_context)
            base, _, _, new_mods = cls.get_atom_base_and_mods(delta.new_context)

            if old_max_length and not old_mods and new_mods:
                rec = table.record(name=str(new_name))
                updates, _ = cls.fill_record(rec, delta.new_context.delta(None))
                plan.add(Insert(table=table, records=[rec]))
                plan.add(CreateDomain(name=new_domain_name, base=base))


            default_delta = updates.get('default')
            if default_delta:
                plan.add(AlterDomainAlterDefault(name=new_domain_name, default=default_delta.new))

            for mod in new_mods - old_mods:
                plan.add(AlterDomainAddConstraint(name=new_domain_name, constraint=mod))

            for mod in old_mods - new_mods:
                plan.add(AlterDomainDropConstraint(name=new_domain_name, constraint=mod))

        return plan

    @classmethod
    def get_atom_base_and_mods(cls, atom):
        if cls.is_prototype(atom.base):
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

        for mod in atom.mods.values():
            if ((has_max_length and isinstance(mod, proto.AtomModMaxLength))
                    or (has_min_length and isinstance(mod, proto.AtomModMinLength))
                    or isinstance(mod, proto.AtomModExpr)):
                continue
            mods.add(mod)

        return base, has_min_length, has_max_length, mods

    @classmethod
    def fill_record(cls, rec, delta):
        result = super(Atom, cls).fill_record(rec, delta)
        if rec.base:
            rec.base = str(rec.base)
        return result


class TableBasedObject:
    @classmethod
    def pg_type_from_atom(cls, plan, delta):
        column_type = None
        atom_obj = delta.new_context
        if atom_obj.base is not None and atom_obj.base == 'semantix.caos.builtins.str':
            if len(atom_obj.mods) == 1:
                mod = next(iter(atom_obj.mods.values()))
                if isinstance(mod, proto.AtomModMaxLength):
                    column_type = 'varchar(%d)' % mod.value
            elif len(atom_obj.mods) == 0:
                column_type = 'text'

        new_atom = False

        if not column_type:
            if atom_obj.name in base_type_name_map:
                column_type = base_type_name_map[atom_obj.name]
            else:
                if atom_obj.automatic:
                    new_atom = True
                column_type = common.atom_name_to_domain_name(atom_obj.name)

        if plan and (new_atom or delta.old):
            Atom.alter_object(plan, delta)

        return column_type

    @classmethod
    def rename(cls, plan, metatable, old_name, new_name):
        old_table_name = common.concept_name_to_table_name(old_name, catenate=False)
        new_table_name = common.concept_name_to_table_name(new_name, catenate=False)

        if old_name.module != new_name.module:
            plan.add(AlterTableSetSchema(old_table_name, new_table_name[0]))
            old_table_name = (new_table_name[0], old_table_name[1])

        if old_name.name != new_name.name:
            plan.add(AlterTableRenameTo(old_table_name, new_table_name[1]))

        updaterec = metatable.record(name=str(new_name))
        condition = [('name', str(old_name))]
        plan.add(Update(table=metatable, record=updaterec, condition=condition))


class Concept(proto.Concept, Prototype, TableBasedObject):
    @classmethod
    def alter_object(cls, plan, delta):
        old_name, new_name = delta.old, delta.new
        table = ConceptTable()

        if new_name:
            new_table_name = common.concept_name_to_table_name(new_name, catenate=False)

        if not new_name:
            # Drop it
            cls.drop_table(plan, old_name)
            plan.add(Delete(table=table, condition=[('name', str(old_name))]))
            return plan

        elif not old_name:
            # Create it
            create_schema = CreateSchema(name=new_table_name[0])
            if create_schema not in plan:
                plan.add(create_schema)

            record = cls.record_metadata(plan, delta)
            table = cls.get_new_table(plan, delta, record)
            plan.add(CreateTable(table=table))

        else:
            # Alter it

            if old_name != new_name:
                cls.rename(plan, table, old_name, new_name)

            if delta.diff:
                alter = AlterTable(new_table_name)

                updaterec = table.record()

                updates, rec_updates = cls.fill_record(updaterec, delta)

                for d in delta(proto.LinkSetDelta):
                    if (d.old_context or d.new_context).atomic():
                        cls.alter_columns(plan, alter, new_name, d)

                if rec_updates:
                    condition = [('name', str(new_name))]
                    plan.add(Update(table=table, record=updaterec, condition=condition))

                if alter.ops:
                    plan.add(alter)

    @classmethod
    def alter_columns(cls, plan, alter, obj_name, delta):
        table_name = common.concept_name_to_table_name(obj_name, catenate=False)

        if not delta.new:
            # Drop
            column_name = common.caos_name_to_pg_colname(delta.old)
            # We don't really care about the type -- we're dropping the thing
            column_type = 'text'
            col = AlterTableDropColumn(Column(name=column_name, type=column_type))
            alter.add_operation(col)

        elif not delta.old:
            # Create
            cols = cls.get_columns(plan, delta)
            for col in cols:
                alter.add_operation(AlterTableAddColumn(col))

        else:
            # Alter

            old_link_obj = delta.old_context.first
            new_link_obj = delta.new_context.first

            if delta.old != delta.new:
                rename = AlterTableRenameColumn(table_name, delta.old, delta.new)
                plan.add(rename)

            old_type = cls.pg_type_from_atom(None, old_link_obj.target.delta(None))
            new_type = cls.pg_type_from_atom(plan, new_link_obj.target.delta(old_link_obj.target))

            if old_type != new_type:
                alter.add_operation(AlterTableAlterColumnType(delta.new, new_type))
                Link.alter_object(plan, list(delta.diff)[0])

    @classmethod
    def get_columns(cls, plan, linkset_delta):
        columns = []

        for link_delta in linkset_delta(proto.LinkDelta):
            if isinstance(link_delta.new_context.target, proto.Atom):
                column_type = cls.pg_type_from_atom(plan, link_delta.new_context.target.delta(None))
                column_name = common.caos_name_to_pg_colname(linkset_delta.new)

                columns.append(Column(name=column_name, type=column_type,
                                      required=link_delta.new_context.required))
        return columns

    @classmethod
    def get_new_table(cls, plan, delta, fields):

        new_table_name = common.concept_name_to_table_name(delta.new, catenate=False)

        columns = []
        constraints = []

        for linkset_delta in delta(proto.LinkSetDelta):
            columns.extend(cls.get_columns(plan, linkset_delta))

        if delta.new == 'semantix.caos.builtins.Object':
            columns.append(Column(name='concept_id', type='integer', required=True))

        constraints.append(PrimaryKey(columns=['semantix.caos.builtins.id']))

        concept_table = Table(name=new_table_name)
        concept_table.add_columns(columns)
        concept_table.constraints = constraints

        bases = (common.concept_name_to_table_name(p, catenate=False)
                 for p in fields['base'].new if proto.Concept.is_prototype(p))
        concept_table.bases = list(bases)

        return concept_table

    @classmethod
    def drop_table(cls, plan, obj_name):
        plan.add(DropTable(name=common.concept_name_to_table_name(obj_name, catenate=False)))

    @classmethod
    def fill_record(cls, rec, delta):
        result = super(Concept, cls).fill_record(rec, delta)
        if rec.custombases:
            rec.custombases=[str(b) for b in rec.custombases]
        return result

    @classmethod
    def record_metadata(cls, plan, delta):
        table = ConceptTable()

        rec = table.record(name=str(delta.new))
        updates, rec_updates = cls.fill_record(rec, delta)
        plan.add(Insert(table=table, records=[rec]))

        return updates


class Link(proto.Link, Prototype, TableBasedObject):
    @classmethod
    def alter_object(cls, plan, delta):
        old_name, new_name = delta.old, delta.new
        table = LinkTable()

        if new_name:
            new_table_name = common.link_name_to_table_name(new_name, catenate=False)

        if not new_name:
            # Drop it
            if not delta.old_context.atomic() and not delta.old_context.implicit_derivative:
                cls.drop_table(plan, old_name)
            plan.add(Delete(table=table, condition=[('name', str(old_name))]))
            return plan

        elif not old_name:
            # We do not want to create a separate table for atomic links since those
            # are represented by table columns.  Implicit derivative links also do not get
            # their own table since they're just a special case of the parent.
            #
            # On the other hand, much like with concepts we want all other links to be in
            # separate tables even if they do not define additional properties.
            # This is to allow for further schema evolution.
            #
            if not delta.new_context.atomic() and not delta.new_context.implicit_derivative:
                create_schema = CreateSchema(name=new_table_name[0])
                if create_schema not in plan:
                    plan.add(create_schema)

                plan.add(CreateTable(table=cls.get_new_table(plan, delta.new_context)))

            cls.record_metadata(plan, delta)

        else:
            if not delta.old_context.atomic() and not delta.old_context.implicit_derivative:
                if old_name != new_name:
                    cls.rename(plan, table, old_name, new_name)

            elif delta.old_context.atomic():
                cls.record_metadata(plan, delta)

    @classmethod
    def get_new_table(cls, plan, new):

        new_table_name = common.link_name_to_table_name(new.name, catenate=False)

        columns = []
        constraints = []

        for property_name, property in new.properties.items():
            column_type = cls.pg_type_from_atom(plan, property.atom.delta(None))
            columns.append(Column(name=str(property_name), type=column_type))

        if new.name == 'semantix.caos.builtins.link':
            columns.append(Column(name='source_id', type='uuid', required=True))
            columns.append(Column(name='target_id', type='uuid', required=True))
            columns.append(Column(name='link_type_id', type='integer', required=True))

        constraints.append(PrimaryKey(columns=['source_id', 'target_id', 'link_type_id']))

        table = Table(name=new_table_name)
        table.add_columns(columns)
        table.constraints = constraints

        if new.base:
            bases = (common.link_name_to_table_name(p, catenate=False)
                     for p in new.base if proto.Concept.is_prototype(p))
            table.bases = list(bases)

        return table

    @classmethod
    def drop_table(cls, plan, obj_name):
        plan.add(DropTable(name=common.link_name_to_table_name(obj_name, catenate=False)))

    @classmethod
    def record_metadata(cls, plan, delta):
        if delta.new:
            table = LinkTable()
            if delta.new_context.source:
                source_id = Query('(SELECT id FROM caos.metaobject WHERE name = $1)',
                                  [str(delta.new_context.source.name)], type='integer')
            else:
                source_id = None

            if delta.new_context.target:
                if isinstance(delta.new_context.target, proto.Atom) and \
                                                        delta.new_context.target.base:
                    target_id = Query('''coalesce((SELECT id FROM caos.metaobject
                                                             WHERE name = $1),
                                                  (SELECT id FROM caos.metaobject
                                                             WHERE name = $2))''',
                                      [str(delta.new_context.target.name),
                                       str(delta.new_context.target.base)],
                                      type='integer')
                else:
                    target_id = Query('(SELECT id FROM caos.metaobject WHERE name = $1)',
                                      [str(delta.new_context.target.name)],
                                      type='integer')
            else:
                target_id = None

            rec = table.record(name=str(delta.new), source_id=source_id, target_id=target_id)
            cls.fill_record(rec, delta)

            if not delta.old:
                plan.add(Insert(table=table, records=[rec]))
            else:
                plan.add(Update(table=table, record=rec, condition=[('name', str(delta.old))]))


class SynchronizationPlan:
    def __init__(self):
        self.ops = collections.OrderedDict()

    def add(self, op, level=0):
        if level not in self.ops:
            self.ops[level] = datastructures.StrictOrderedIndex(key=lambda i: (i.__class__, i.opid))
        self.ops[level].add(op)

    def get_code_and_vars(self, op, db):
        code = op.code(db)
        assert code
        if isinstance(code, tuple):
            code, vars = code
        else:
            vars = tuple()

        return code, vars

    def check_conditions(self, db, conditions, positive):
        result = True
        if conditions:
            for condition in conditions:
                code, vars = self.get_code_and_vars(condition, db)
                result = db.prepare(code)(*vars)

                if bool(result) ^ positive:
                    result = False
                    break
            else:
                result = True

        return result

    @debug
    def execute(self, db):
        for level, ops in sorted(self.ops.items(), key=lambda i: i[0]):
            for op in ops:
                ok = self.check_conditions(db, op.conditions, True) and \
                     self.check_conditions(db, op.neg_conditions, False)

                if ok:
                    code, vars = self.get_code_and_vars(op, db)

                    """LOG [caos.meta.sync.cmd] Sync command:
                    print(op)
                    """

                    """LOG [caos.meta.sync.sql] Sync command code:
                    print(code, vars)
                    """

                    if vars:
                        db.prepare(code)(*vars)
                    else:
                        db.execute(code)

            if level == -2:
                self.__class__.init_hstore(db)

    def is_material(self):
        return len(self.ops) > 2 and self.ops[0]

    def __iter__(self):
        return iter(self.ops)

    def __call__(self, level=0):
        for l, ops in self.ops.items():
            if l >= level:
                for op in ops:
                    yield op

    @classmethod
    def from_delta(cls, delta):
        plan = cls()

        plan.add(CreateSchema(name='caos'),
                 level=-2)
        plan.add(EnableFeature(feature=UuidFeature(),
                               neg_conditions=[FunctionExists(('caos', 'uuid_nil'))]), level=-2)
        plan.add(EnableFeature(feature=HstoreFeature(),
                               neg_conditions=[TypeExists(('caos', 'hstore'))]), level=-2)
        metalogtable = MetaLogTable()
        plan.add(CreateTable(table=metalogtable, neg_conditions=[TableExists(name=metalogtable.name)]),
                level=-1)
        metatable = MetaObjectTable()
        plan.add(CreateTable(table=metatable, neg_conditions=[TableExists(name=metatable.name)]), level=-1)
        atomtable = AtomTable()
        plan.add(CreateTable(table=atomtable, neg_conditions=[TableExists(name=atomtable.name)]), level=-1)
        concepttable = ConceptTable()
        plan.add(CreateTable(table=concepttable, neg_conditions=[TableExists(name=concepttable.name)]),
                 level=-1)
        linktable = LinkTable()
        plan.add(CreateTable(table=linktable, neg_conditions=[TableExists(name=linktable.name)]), level=-1)

        for d in delta:
            cls.alter_object(plan, d)

        return plan

    @classmethod
    def alter_object(cls, plan, delta):
        if isinstance(delta, proto.AtomDelta):
            return Atom.alter_object(plan, delta)

        elif isinstance(delta, proto.ConceptDelta):
            return Concept.alter_object(plan, delta)

        elif isinstance(delta, proto.LinkDelta):
            return Link.alter_object(plan, delta)

        else:
            assert False, 'unexpected delta %s' % delta

    @classmethod
    def init_hstore(cls, db):
        try:
            db.typio.identify(contrib_hstore='caos.hstore')
        except postgresql.exceptions.SchemaNameError:
            pass

    @classmethod
    def logsync(cls, checksum, parent_id):
        table = MetaLogTable()

        id = hashlib.sha1(('%s_%s' % (checksum, parent_id)).encode()).hexdigest()

        rec = table.record(
                id=id,
                parents=[str(parent_id)],
                checksum=str('%x' % checksum),
                committer=os.getenv('LOGNAME', '<unknown>'),
                hostname=socket.getfqdn()
              )
        return Insert(table, records=[rec])


class SynchronizationOperation:
    def __init__(self, *, conditions=None, neg_conditions=None):
        self.opid = id(self)
        self.conditions = conditions or set()
        self.neg_conditions = neg_conditions or set()


class DDLOperation(SynchronizationOperation):
    pass


class DMLOperation(SynchronizationOperation):
    pass


class Condition:
    def execute(self, db):
        code = self.code(db)
        if isinstance(code, tuple):
            code, vars = code
        else:
            vars = []
        return db.prepare(code)(*vars)


class Query:
    def __init__(self, text, params, type):
        self.text = text
        self.params = params
        self.type = type


class Insert(DMLOperation):
    def __init__(self, table, records):
        super().__init__()

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
                 ','.join(cols),
                 ','.join(placeholders))

        return (code, vals)

    def __repr__(self):
        cols = self.table.record().fields
        vals = (('(%s)' % ', '.join(str(getattr(row, col, None)) for col in cols)) for row in self.records)
        return '<caos.sync.%s %s (%s)>' % (self.__class__.__name__, self.table.name, ', '.join(vals))


class Update(DMLOperation):
    def __init__(self, table, record, condition):
        super().__init__()

        self.table = table
        self.record = record
        self.fields = [f for f, v in record if v is not Default]
        self.condition = condition

    def code(self, db):
        e = postgresql.string.quote_ident

        placeholders = []
        vals = []

        i = 1
        for f in self.fields:
            val = getattr(self.record, f)

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


class Delete(DMLOperation):
    def __init__(self, table, condition):
        super().__init__()

        self.table = table
        self.condition = condition

    def code(self, db):
        e = postgresql.string.quote_ident
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
        code = 'PRIMARY KEY (%s)' % ', '.join(postgresql.string.quote_ident(c) for c in self.columns)
        return code

class UniqueConstraint(DBObject):
    def __init__(self, columns):
        self.columns = columns

    def code(self, db):
        code = 'UNIQUE (%s)' % ', '.join(postgresql.string.quote_ident(c) for c in self.columns)
        return code


class Column(DBObject):
    def __init__(self, name, type, required=False, default=None, readonly=False):
        self.name = name
        self.type = type
        self.required = required
        self.default = default
        self.readonly = readonly

    def code(self, db):
        e = postgresql.string.quote_ident
        return '%s %s %s %s' % (e(self.name), self.type,
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


class MetaLogTable(Table):
    def __init__(self, name=None):
        name = name or ('caos', 'metalog')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            Column(name='id', type='char(40)', required=True),
            Column(name='parents', type='char(40)[]', required=False),
            Column(name='checksum', type='char(40)', required=True),
            Column(name='commit_date', type='timestamp with time zone', required=True,
                                                                        default='CURRENT_TIMESTAMP'),
            Column(name='committer', type='text', required=True),
            Column(name='hostname', type='text', required=True),
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
        source = self.source % {'version': '%s.%s' % db.version_info[:2]}

        with open(source, 'r') as f:
            code = re.sub(r'SET\s+search_path\s*=\s*[^;]+;',
                          'SET search_path = %s;' % postgresql.string.quote_ident(self.schema),
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
    source = '/usr/share/postgresql-%(version)s/contrib/uuid-ossp.sql'

    def __init__(self, schema='caos'):
        super().__init__(name='uuid', schema=schema)


class HstoreFeature(Feature):
    source = '/usr/share/postgresql-%(version)s/contrib/hstore.sql'

    def __init__(self, schema='caos'):
        super().__init__(name='hstore', schema=schema)


class EnableFeature(DDLOperation):
    def __init__(self, feature, *, conditions=None, neg_conditions=None):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions)

        self.feature = feature
        self.opid = feature.name

    def code(self, db):
        return self.feature.code(db)

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.feature.name)


class SchemaExists(Condition):
    def __init__(self, name):
        self.name = name

    def code(self, db):
        return ('SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = $1', [self.name])


class CreateSchema(DDLOperation):
    def __init__(self, name):
        super().__init__()

        self.name = name
        self.opid = name
        self.neg_conditions.add(SchemaExists(self.name))

    def code(self, db):
        return 'CREATE SCHEMA %s' % common.quote_ident(self.name)

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.name)


class SchemaObjectOperation(DDLOperation):
    def __init__(self, name, *, conditions=None, neg_conditions=None):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions)

        self.name = name
        self.opid = name

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.name)


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
        return postgresql.string.quote_ident('%s.%s' % (canonical.__module__, canonical.__name__))

    def constraint_code(self, constraint):
        if isinstance(constraint, proto.AtomModRegExp):
            expr = ['VALUE ~ %s' % postgresql.string.quote_literal(re) for re in constraint.regexps]
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
    def __init__(self, table, *, conditions=None, neg_conditions=None):
        super().__init__(table.name, conditions=conditions, neg_conditions=neg_conditions)
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
        return 'DROP COLUMN %s' % postgresql.string.quote_ident(self.column.name)


class AlterTableAlterColumnType(DDLOperation):
    def __init__(self, column_name, new_type):
        self.column_name = column_name
        self.new_type = new_type

    def code(self, db):
        return 'ALTER COLUMN %s SET DATA TYPE %s' % (postgresql.string.quote_ident(str(self.column_name)),
                                                     self.new_type)


class AlterTableSetSchema(AlterTableBase):
    def __init__(self, name, schema):
        super().__init__(name)
        self.schema = schema

    def code(self, db):
        code = super().code(db)
        code += ' SET SCHEMA %s ' % self.quote_ident(self.new_name)
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
        code += ' RENAME COLUMN %s TO %s ' % (postgresql.string.quote_ident(self.old_col_name),
                                              postgresql.string.quote_ident(self.new_col_name))
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
