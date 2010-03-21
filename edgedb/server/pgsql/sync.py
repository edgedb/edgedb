##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import os
import socket
import re

import postgresql

from semantix import caos
from semantix.caos import proto
from semantix.caos.backends import metadelta

from semantix.caos.backends.pgsql import common

from semantix.utils import datastructures
from semantix.utils.debug import debug


base_type_name_map = {
    caos.Name('semantix.caos.builtins.str'): 'character varying',
    caos.Name('semantix.caos.builtins.int'): 'numeric',
    caos.Name('semantix.caos.builtins.bool'): 'boolean',
    caos.Name('semantix.caos.builtins.float'): 'double precision',
    caos.Name('semantix.caos.builtins.uuid'): 'uuid',
    caos.Name('semantix.caos.builtins.datetime'): 'timestamp'
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
    'timestamp': caos.Name('semantix.caos.builtins.datetime')
}


typmod_types = ('character', 'character varying', 'numeric')
fixed_length_types = {'character varying': 'character'}


class Atom(proto.Atom):
    @classmethod
    def alter_object(cls, plan, old, new):
        if old:
            old_domain_name = common.atom_name_to_domain_name(old.name, catenate=False)
        else:
            old_domain_name = None

        table = AtomTable()

        if new:
            rec = table.record(name=str(new.name),
                               title=new.title.as_dict() if new.title else None,
                               description=new.description.as_dict() if new.description else None,
                               automatic=new.automatic,
                               abstract=new.is_abstract,
                               base=str(new.base))

            if old:
                updaterec = None

                for f in ('title', 'description', 'automatic', 'is_abstract'):
                    if getattr(old, f) != getattr(new, f):
                        if not updaterec:
                            updaterec = table.record()
                        setattr(updaterec, f, getattr(rec, f))

                if updaterec:
                    condition = [('name', str(new.name))]
                    plan.add(Update(table=table, record=updaterec, condition=condition))

        else:
            plan.add(DropDomain(name=old_domain_name))
            plan.add(Delete(table=table, condition=[('name', str(old.name))]))
            return plan

        new_domain_name = common.atom_name_to_domain_name(new.name, catenate=False)

        if old and old.name != new.name:
            plan.add(RenameDomain(name=old_domain_name, new_name=new_domain_name))
            updaterec = table.record(name=str(new.name))
            condition = [('name', str(old.name))]
            plan.add(Update(table=table, record=updaterec, condition=condition))

        old_mods = set()
        new_mods = set()

        base, min_length, max_length, new_mods = cls.get_atom_base_and_mods(new)

        if old:
            old_base, old_min_length, old_max_length, old_mods = cls.get_atom_base_and_mods(old)

        if not old or (old and old_max_length and not old_mods and new_mods):
            create_schema = CreateSchema(name=new_domain_name[0])
            if create_schema not in plan:
                plan.add(create_schema)
            plan.add(CreateDomain(name=new_domain_name, base=base))
            plan.add(Insert(table=table, records=[rec]))

        if (old and old.default != new.default) or new.default:
            plan.add(AlterDomainAlterDefault(name=new_domain_name, default=new.default))

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


class TableBasedObject:
    @classmethod
    def pg_type_from_atom(cls, plan, atom_obj, old_atom_obj=None):
        column_type = None
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

        if plan and (new_atom or old_atom_obj):
            Atom.alter_object(plan, old_atom_obj, atom_obj)

        return column_type

    @classmethod
    def rename(cls, plan, metatable, old, new):
        old_table_name = common.concept_name_to_table_name(old.name, catenate=False)
        new_table_name = common.concept_name_to_table_name(new.name, catenate=False)

        if old.name.module != new.name.module:
            plan.add(AlterTableSetSchema(old_table_name, new_table_name[0]))
            old_table_name = (new_table_name[0], old_table_name[1])

        if old.name.name != new.name.name:
            plan.add(AlterTableRenameTo(old_table_name, new_table_name[1]))

        updaterec = metatable.record(name=str(new.name))
        condition = [('name', str(old.name))]
        plan.add(Update(table=metatable, record=updaterec, condition=condition))


class Concept(proto.Concept, TableBasedObject):
    @classmethod
    def alter_object(cls, plan, old, new):
        table = ConceptTable()

        if not new:
            cls.drop_table(plan, old)
            plan.add(Delete(table=table, condition=[('name', str(old.name))]))
            return plan

        new_table_name = common.concept_name_to_table_name(new.name, catenate=False)

        if old:
            if old.name != new.name:
                cls.rename(plan, table, old, new)

            delta = metadelta.concept_delta(old, new)

            if delta:
                columns_to_add = []
                columns_to_drop = []

                alter = AlterTable(new_table_name)

                for old_link, new_link in delta:
                    if not old_link:
                        columns_to_add.extend(cls.get_columns(plan, new, new_link.name))
                    elif not new_link:
                        columns_to_drop.extend(cls.get_columns(plan, old, old_link.name))
                    else:
                        cls.alter_columns(plan, alter, new, old_link, new_link)

                if columns_to_add:
                    for col in columns_to_add:
                        alter.add_operation(AlterTableAddColumn(col))

                if columns_to_drop:
                    for col in columns_to_drop:
                        alter.add_operation(AlterTableDropColumn(col))

                if alter.ops:
                    plan.add(alter)

        else:
            create_schema = CreateSchema(name=new_table_name[0])
            if create_schema not in plan:
                plan.add(create_schema)

            table = cls.get_new_table(plan, new)
            plan.add(CreateTable(table=table))

            cls.record_metadata(plan, old, new)

    @classmethod
    def alter_columns(cls, plan, alter, obj, old_link, new_link):
        table_name = common.concept_name_to_table_name(obj.name, catenate=False)
        if old_link.atomic():
            old_link_obj = old_link.first
            new_link_obj = new_link.first

            if old_link.name != new_link.name:
                rename = AlterTableRenameColumn(table_name, old_link.name, new_link.name)
                plan.add(rename)

            old_type = cls.pg_type_from_atom(None, old_link_obj.target)
            new_type = cls.pg_type_from_atom(plan, new_link_obj.target, old_link_obj.target)

            if old_type != new_type:
                alter.add_operation(AlterTableAlterColumnType(new_link.name, new_type))
                Link.alter_object(plan, old_link_obj, new_link_obj)

    @classmethod
    def get_columns(cls, plan, obj, link_name):
        columns = []

        links = obj.links[link_name]
        for link in links:
            if isinstance(link.target, proto.Atom):
                column_type = cls.pg_type_from_atom(plan, link.target)
                column_name = common.caos_name_to_pg_colname(link_name)

                columns.append(Column(name=column_name, type=column_type, required=link.required))
        return columns

    @classmethod
    def get_new_table(cls, plan, new):

        new_table_name = common.concept_name_to_table_name(new.name, catenate=False)

        columns = []
        constraints = []

        for link_name in sorted(new.ownlinks.keys()):
            columns.extend(cls.get_columns(plan, new, link_name))

        if new.name == 'semantix.caos.builtins.Object':
            columns.append(Column(name='concept_id', type='integer', required=True))

        constraints.append(PrimaryKey(columns=['semantix.caos.builtins.id']))

        concept_table = Table(name=new_table_name)
        concept_table.add_columns(columns)
        concept_table.constraints = constraints

        bases = (common.concept_name_to_table_name(p, catenate=False)
                 for p in new.base if proto.Concept.is_prototype(p))
        concept_table.bases = list(bases)

        return concept_table

    @classmethod
    def drop_table(cls, plan, obj):
        plan.add(DropTable(name=common.concept_name_to_table_name(obj.name, catenate=False)))

    @classmethod
    def record_metadata(cls, plan, old, new):
        table = ConceptTable()
        rec = table.record(name=str(new.name),
                           title=new.title.as_dict() if new.title else None,
                           description=new.description.as_dict() if new.description else None,
                           abstract=new.is_abstract,
                           custombases=[str(b) for b in new.custombases] or None)
        plan.add(Insert(table=table, records=[rec]))


class Link(proto.Link, TableBasedObject):
    @classmethod
    def alter_object(cls, plan, old, new):
        table = LinkTable()

        if not new:
            cls.drop_table(plan, old)
            plan.add(Delete(table=table, condition=[('name', str(old.name))]))
            return plan

        new_table_name = common.link_name_to_table_name(new.name, catenate=False)

        if old:
            if not old.atomic() and not old.implicit_derivative:
                if old.name != new.name:
                    cls.rename(plan, table, old, new)

            elif old.atomic():
                cls.record_metadata(plan, old, new)

        else:
            # We do not want to create a separate table for atomic links since those
            # are represented by table columns.  Implicit derivative links also do not get
            # their own table since they're just a special case of the parent.
            #
            # On the other hand, much like with concepts we want all other links to be in
            # separate tables even if they do not define additional properties.
            # This is to allow for further schema evolution.
            #
            if not new.atomic() and not new.implicit_derivative:
                create_schema = CreateSchema(name=new_table_name[0])
                if create_schema not in plan:
                    plan.add(create_schema)

                plan.add(CreateTable(table=cls.get_new_table(plan, new)))

            cls.record_metadata(plan, old, new)


    @classmethod
    def get_new_table(cls, plan, new):

        new_table_name = common.link_name_to_table_name(new.name, catenate=False)

        columns = []
        constraints = []

        for property_name, property in new.properties.items():
            column_type = cls.pg_type_from_atom(plan, property.atom)
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
    def drop_table(cls, plan, obj):
        if not obj.atomic() and not obj.implicit_derivative:
            plan.add(DropTable(name=common.link_name_to_table_name(obj.name, catenate=False)))

    @classmethod
    def record_metadata(cls, plan, old, new):
        if new:
            table = LinkTable()
            if new.source:
                source_id = Query('(SELECT id FROM caos.metaobject WHERE name = $1)', [str(new.source.name)],
                                  type='integer')
            else:
                source_id = None

            if new.target:
                if isinstance(new.target, proto.Atom) and new.target.base:
                    target_id = Query('''coalesce((SELECT id FROM caos.metaobject WHERE name = $1),
                                                  (SELECT id FROM caos.metaobject WHERE name = $2))''',
                                      [str(new.target.name), str(new.target.base)],
                                      type='integer')
                else:
                    target_id = Query('(SELECT id FROM caos.metaobject WHERE name = $1)',
                                      [str(new.target.name)],
                                      type='integer')
            else:
                target_id = None

            rec = table.record(name=str(new.name),
                               title=new.title.as_dict() if new.title else None,
                               description=new.description.as_dict() if new.description else None,
                               source_id=source_id,
                               target_id=target_id,
                               mapping=new.mapping, required=new.required,
                               implicit=new.implicit_derivative,
                               atomic=new.atomic(),
                               abstract=new.is_abstract)

            if not old:
                plan.add(Insert(table=table, records=[rec]))
            else:
                plan.add(Update(table=table, record=rec, condition=[('name', str(old.name))]))


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

        for old, new in delta:
            cls.alter_object(plan, old, new)

        return plan

    @classmethod
    def alter_object(cls, plan, old, new):
        obj = old or new

        if isinstance(obj, proto.Atom):
            if not obj.automatic:
                return Atom.alter_object(plan, old, new)

        elif isinstance(obj, proto.Concept):
            return Concept.alter_object(plan, old, new)

        elif isinstance(obj, proto.Link):
            return Link.alter_object(plan, old, new)

        else:
            assert False, 'unexpected prototype %s' % obj

    @classmethod
    def init_hstore(cls, db):
        try:
            db.typio.identify(contrib_hstore='caos.hstore')
        except postgresql.exceptions.SchemaNameError:
            pass

    @classmethod
    def logsync(cls, checksum):
        table = MetaLogTable()
        rec = table.record(
                checksum=str(checksum),
                username=os.getenv('LOGNAME', '<unknown>'),
                host=socket.getfqdn()
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
    pass


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
        self.fields = [f for f in record.__class__.fields if getattr(record, f) is not Default]
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
        return '%s %s %s %s' % (e(self.name), self.type, 'NOT NULL' if self.required else '',
                                ('DEFAULT %s' % self.default) if self.default else '')


class Default:
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
        return datastructures.Record(self.__class__.__name__ + '_record', [c.name for c in self.columns()],
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
            Column(name='id', type='serial', required=True, readonly=True),
            Column(name='checksum', type='char(40)', required=True),
            Column(name='mtime', type='timestamp with time zone', required=True, default='CURRENT_TIMESTAMP'),
            Column(name='username', type='text', required=True),
            Column(name='host', type='text', required=True)
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
            Column(name='abstract', type='boolean', required=True, default=False),
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
            Column(name='implicit', type='boolean', required=True, default=False),
            Column(name='atomic', type='boolean', required=True, default=False),
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
