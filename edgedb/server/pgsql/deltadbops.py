##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Abstractions for low-level database DDL and DML operations and structures
related to the Caos schema."""


import re
import postgresql.installation
from postgresql.string import quote_literal as pg_ql
from postgresql.string import quote_ident_if_needed as pg_quote_if_needed

from semantix import caos
from semantix.caos import proto
from semantix.caos import delta as delta_cmds

from semantix.utils import datastructures
from semantix.utils import functional

from semantix.caos.backends.pgsql import common, Config
from semantix.caos.backends.pgsql import dbops


class SchemaDBObjectMeta(functional.Adapter, type(proto.Prototype)):
    def __init__(cls, name, bases, dct, *, adapts=None):
        functional.Adapter.__init__(cls, name, bases, dct, adapts=adapts)
        type(proto.Prototype).__init__(cls, name, bases, dct)


class SchemaDBObject(metaclass=SchemaDBObjectMeta):
    @classmethod
    def adapt(cls, obj):
        return cls.copy(obj)

    @classmethod
    def get_canonical_class(cls):
        for base in cls.__bases__:
            if issubclass(base, caos.types.ProtoObject) and not issubclass(base, SchemaDBObject):
                return base

        return cls


class AtomConstraint(SchemaDBObject):
    @classmethod
    def adapt(cls, obj):
        # Default adapt() through copy() will not work, as AtomConstraint always
        # creates a copy with a canonical class.
        try:
            return cls(value=obj.value)
        except AttributeError:
            return cls(values=obj.values)

    def get_backend_constraint_name(self):
        canonical = self.__class__.get_canonical_class()
        return common.quote_ident('{}.{}'.format(canonical.__module__, canonical.__name__))


class AtomConstraintMinLength(AtomConstraint, adapts=proto.AtomConstraintMinLength):
    def get_backend_constraint_check_code(self, value_holder='VALUE'):
        value_holder = pg_quote_if_needed(value_holder)
        return 'CHECK (length({}::text) >= {})'.format(value_holder, self.value)


class AtomConstraintMinValue(AtomConstraint, adapts=proto.AtomConstraintMinValue):
    def get_backend_constraint_check_code(self, value_holder='VALUE'):
        value_holder = pg_quote_if_needed(value_holder)
        return 'CHECK ({} >= {})'.format(value_holder, pg_ql(str(self.value)))


class AtomConstraintMinExValue(AtomConstraint, adapts=proto.AtomConstraintMinExValue):
    def get_backend_constraint_check_code(self, value_holder='VALUE'):
        value_holder = pg_quote_if_needed(value_holder)
        return 'CHECK ({} > {})'.format(value_holder, pg_ql(str(self.value)))


class AtomConstraintMaxLength(AtomConstraint, adapts=proto.AtomConstraintMaxLength):
    def get_backend_constraint_check_code(self, value_holder='VALUE'):
        value_holder = pg_quote_if_needed(value_holder)
        return 'CHECK (length({}::text) <= {})'.format(value_holder, self.value)


class AtomConstraintMaxValue(AtomConstraint, adapts=proto.AtomConstraintMaxValue):
    def get_backend_constraint_check_code(self, value_holder='VALUE'):
        value_holder = pg_quote_if_needed(value_holder)
        return 'CHECK ({} <= {})'.format(value_holder, pg_ql(str(self.value)))


class AtomConstraintMaxExValue(AtomConstraint, adapts=proto.AtomConstraintMaxExValue):
    def get_backend_constraint_check_code(self, value_holder='VALUE'):
        value_holder = pg_quote_if_needed(value_holder)
        return 'CHECK ({} < {})'.format(value_holder, pg_ql(str(self.value)))


class AtomConstraintExpr(AtomConstraint, adapts=proto.AtomConstraintExpr):
    def get_backend_constraint_check_code(self, value_holder='VALUE'):
        raise NotImplementedError


class AtomConstraintRegExp(AtomConstraint, adapts=proto.AtomConstraintRegExp):
    def get_backend_constraint_check_code(self, value_holder='VALUE'):
        value_holder = pg_quote_if_needed(value_holder)
        expr = ['{} ~ {}'.format(value_holder, pg_ql(re)) for re in self.values]
        expr = ' AND '.join(expr)
        return 'CHECK ({})'.format(expr)


class CallDeltaHook(dbops.Command):
    def __init__(self, *, hook, stage, op, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.hook = hook
        self.stage = stage
        self.op = op

    def execute(self, context):
        try:
            self.op.call_hook(context.session, stage=self.stage, hook=self.hook)
        except delta_cmds.DeltaHookNotFoundError:
            pass


class TableClassConstraint(dbops.TableConstraint):
    def __init__(self, table_name, column_name, prefix, constrobj):
        super().__init__(table_name, column_name)
        self.prefix = prefix if isinstance(prefix, tuple) else (prefix,)
        self.constrobj = constrobj

    def code(self, context):
        return 'CONSTRAINT %s %s' % (self.constraint_name(),
                                     self.constraint_code(context, self.column_name))

    def extra(self, context, alter_table):
        text = self.raw_constraint_name()
        cmd = dbops.Comment(object=self, text=text)
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
        cmd = dbops.Comment(object=new_constraint, text=new_name)
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
        constraint = SchemaDBObjectMeta.adapt(self.constraint)
        return constraint.get_backend_constraint_check_code(value_holder=value_holder)


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


class MappingIndex(dbops.Index):
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
                 'cols': ', '.join(common.quote_ident(c) for c in self.columns),
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


class DeltaRefTable(dbops.Table):
    def __init__(self, name=None):
        name = name or ('caos', 'deltaref')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='id', type='varchar', required=True),
            dbops.Column(name='ref', type='text', required=True)
        ])

        self.constraints = set([
            dbops.PrimaryKey(name, columns=('ref',))
        ])

        self._columns = self.columns()


class DeltaLogTable(dbops.Table):
    def __init__(self, name=None):
        name = name or ('caos', 'deltalog')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='id', type='varchar', required=True),
            dbops.Column(name='parents', type='varchar[]', required=False),
            dbops.Column(name='checksum', type='varchar', required=True),
            dbops.Column(name='commit_date', type='timestamp with time zone', required=True,
                                                                    default='CURRENT_TIMESTAMP'),
            dbops.Column(name='committer', type='text', required=True),
            dbops.Column(name='comment', type='text', required=False)
        ])

        self.constraints = set([
            dbops.PrimaryKey(name, columns=('id',))
        ])

        self._columns = self.columns()


class ModuleTable(dbops.Table):
    def __init__(self, name=None):
        name = name or ('caos', 'module')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='name', type='text', required=True),
            dbops.Column(name='schema_name', type='text', required=True),
            dbops.Column(name='imports', type='varchar[]', required=False)
        ])

        self.constraints = set([
            dbops.PrimaryKey(name, columns=('name',)),
        ])

        self._columns = self.columns()


class MetaObjectTable(dbops.Table):
    def __init__(self, name=None):
        name = name or ('caos', 'metaobject')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='id', type='serial', required=True, readonly=True),
            dbops.Column(name='name', type='text', required=True),
            dbops.Column(name='is_abstract', type='boolean', required=True, default=False),
            dbops.Column(name='is_final', type='boolean', required=True, default=False),
            dbops.Column(name='title', type='caos.hstore'),
            dbops.Column(name='description', type='text')
        ])

        self.constraints = set([
            dbops.PrimaryKey(name, columns=('id',)),
            dbops.UniqueConstraint(name, columns=('name',))
        ])

        self._columns = self.columns()


class AtomTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('caos', 'atom'))

        self.bases = [('caos', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='automatic', type='boolean', required=True, default=False),
            dbops.Column(name='base', type='text', required=True),
            dbops.Column(name='constraints', type='caos.hstore'),
            dbops.Column(name='default', type='text'),
            dbops.Column(name='attributes', type='caos.hstore')
        ])

        self.constraints = set([
            dbops.PrimaryKey(('caos', 'atom'), columns=('id',)),
            dbops.UniqueConstraint(('caos', 'atom'), columns=('name',))
        ])

        self._columns = self.columns()


class ConceptTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('caos', 'concept'))

        self.bases = [('caos', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='custombases', type='text[]'),
            dbops.Column(name='is_virtual', type='boolean', required=True, default=False),
            dbops.Column(name='automatic', type='boolean', required=True, default=False)
        ])

        self.constraints = set([
            dbops.PrimaryKey(('caos', 'concept'), columns=('id',)),
            dbops.UniqueConstraint(('caos', 'concept'), columns=('name',))
        ])

        self._columns = self.columns()


class LinkTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('caos', 'link'))

        self.bases = [('caos', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='source_id', type='integer'),
            dbops.Column(name='target_id', type='integer'),
            dbops.Column(name='mapping', type='char(2)', required=True),
            dbops.Column(name='exposed_behaviour', type='text'),
            dbops.Column(name='required', type='boolean', required=True, default=False),
            dbops.Column(name='is_atom', type='boolean'),
            dbops.Column(name='readonly', type='boolean', required=True, default=False),
            dbops.Column(name='loading', type='text'),
            dbops.Column(name='base', type='text[]'),
            dbops.Column(name='default', type='text'),
            dbops.Column(name='constraints', type='caos.hstore'),
            dbops.Column(name='abstract_constraints', type='caos.hstore')
        ])

        self.constraints = set([
            dbops.PrimaryKey(('caos', 'link'), columns=('id',)),
            dbops.UniqueConstraint(('caos', 'link'), columns=('name',))
        ])

        self._columns = self.columns()


class LinkPropertyTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('caos', 'link_property'))

        self.bases = [('caos', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='source_id', type='integer'),
            dbops.Column(name='target_id', type='integer'),
            dbops.Column(name='required', type='boolean', required=True, default=False),
            dbops.Column(name='readonly', type='boolean', required=True, default=False),
            dbops.Column(name='loading', type='text'),
            dbops.Column(name='base', type='text[]'),
            dbops.Column(name='default', type='text'),
            dbops.Column(name='constraints', type='caos.hstore'),
            dbops.Column(name='abstract_constraints', type='caos.hstore')
        ])

        self.constraints = set([
            dbops.PrimaryKey(('caos', 'link_property'), columns=('id',)),
            dbops.UniqueConstraint(('caos', 'link_property'), columns=('name',))
        ])

        self._columns = self.columns()


class ComputableTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('caos', 'computable'))

        self.bases = [('caos', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='source_id', type='integer'),
            dbops.Column(name='target_id', type='integer'),
            dbops.Column(name='expression', type='text'),
            dbops.Column(name='is_local', type='bool')
        ])

        self.constraints = set([
            dbops.PrimaryKey(('caos', 'computable'), columns=('id',)),
            dbops.UniqueConstraint(('caos', 'computable'), columns=('name',))
        ])

        self._columns = self.columns()


class FeatureTable(dbops.Table):
    def __init__(self, name=None):
        name = name or ('caos', 'feature')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='name', type='text', required=True),
            dbops.Column(name='class_name', type='text', required=True)
        ])

        self.constraints = set([
            dbops.PrimaryKey(name, columns=('name',)),
        ])

        self._columns = self.columns()


class PolicyTable(MetaObjectTable):
    def __init__(self, name=None):
        name = name or ('caos', 'policy')
        super().__init__(name=name)

        self.bases = [('caos', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='subject', type='integer', required=True),
            dbops.Column(name='category', type='text', required=True)
        ])

        self.constraints = set([
            dbops.PrimaryKey(('caos', 'policy'), columns=('id',)),
            dbops.UniqueConstraint(('caos', 'policy'), columns=('name',))
        ])

        self._columns = self.columns()


class EventPolicyTable(PolicyTable):
    def __init__(self, name=None):
        name = name or ('caos', 'event_policy')
        super().__init__(name=name)

        self.bases = [('caos', 'policy')]

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='event', type='integer', required=True)
        ])

        self.constraints = set([
            dbops.PrimaryKey(('caos', 'policy'), columns=('id',)),
            dbops.UniqueConstraint(('caos', 'policy'), columns=('name',))
        ])

        self._columns = self.columns()


class PointerCascadeActionTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('caos', 'pointer_cascade_action'))
        self.bases = [('caos', 'metaobject')]

        self.constraints = set([
            dbops.PrimaryKey(('caos', 'pointer_cascade_action'), columns=('id',)),
            dbops.UniqueConstraint(('caos', 'pointer_cascade_action'), columns=('name',))
        ])


class PointerCascadeEventTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('caos', 'pointer_cascade_event'))
        self.bases = [('caos', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='allowed_actions', type='integer[]')
        ])

        self.constraints = set([
            dbops.PrimaryKey(('caos', 'pointer_cascade_event'), columns=('id',)),
            dbops.UniqueConstraint(('caos', 'pointer_cascade_event'), columns=('name',))
        ])

        self._columns = self.columns()


class PointerCascadePolicyTable(EventPolicyTable):
    def __init__(self):
        super().__init__(name=('caos', 'pointer_cascade_policy'))
        self.bases = [('caos', 'event_policy')]

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='action', type='integer', required=True)
        ])

        self.constraints = set([
            dbops.PrimaryKey(('caos', 'pointer_cascade_policy'), columns=('id',)),
            dbops.UniqueConstraint(('caos', 'pointer_cascade_policy'), columns=('name',))
        ])

        self._columns = self.columns()


class BackendInfoTable(dbops.Table):
    def __init__(self, name=None):
        name = name or ('caos', 'backend_info')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='format_version', type='int', required=True),
        ])

        self._columns = self.columns()


class EntityModStatType(dbops.CompositeType):
    def __init__(self):
        super().__init__(name=('caos', 'entity_modstat_rec_t'))

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='semantix.caos.builtins.id', type='uuid'),
            dbops.Column(name='semantix.caos.builtins.mtime', type='timestamptz'),
        ])

        self._columns = self.columns()


class LinkEndpointsType(dbops.CompositeType):
    def __init__(self):
        super().__init__(name=('caos', 'link_endpoints_rec_t'))

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='source_id', type='uuid'),
            dbops.Column(name='target_id', type='uuid'),
        ])

        self._columns = self.columns()


class Feature:
    def __init__(self, name, schema='caos'):
        self.name = name
        self.schema = schema

    def get_extension_name(self):
        return self.name

    def code(self, context):
        pg_ver = context.db.version_info

        if pg_ver[:2] <= (9, 0):
            return self._manual_extension_code(context)
        else:
            name = common.quote_ident(self.get_extension_name())
            schema = common.quote_ident(self.schema)
            return 'CREATE EXTENSION {} WITH SCHEMA {}'.format(name, schema)

    def _manual_extension_code(self, context):
        source = self.get_source(context)

        with open(source, 'r') as f:
            code = re.sub(r'SET\s+search_path\s*=\s*[^;]+;',
                          'SET search_path = %s;' % common.quote_ident(self.schema),
                          f.read())
        return code

    def get_source(self, context):
        pg_config_path = Config.get_pg_config_path()
        config = postgresql.installation.pg_config_dictionary(pg_config_path)
        installation = postgresql.installation.Installation(config)
        return self.source % {'pgpath': installation.sharedir}

    @classmethod
    def init_feature(cls, db):
        pass


class EnableFeature(dbops.DDLOperation):
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
        return [dbops.Insert(table, records=[record])]

    def execute(self, context):
        super().execute(context)
        self.feature.init_feature(context.db)

    def __repr__(self):
        return '<caos.sync.%s %s>' % (self.__class__.__name__, self.feature.name)
