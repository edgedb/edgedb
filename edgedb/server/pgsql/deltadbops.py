##
# Copyright (c) 2008-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Abstractions for low-level database DDL and DML operations and structures
related to the EdgeDB schema."""


import re
import postgresql.installation

from edgedb.lang.schema import delta as sd
from edgedb.lang.schema import objects as s_obj

from edgedb.lang.common import datastructures
from edgedb.lang.common import functional

from edgedb.server.pgsql import common
from edgedb.server.pgsql import dbops
from edgedb.server.pgsql.dbops import catalogs as pg_catalogs


class SchemaDBObjectMeta(functional.Adapter, type(s_obj.BasePrototype)):
    def __init__(cls, name, bases, dct, *, adapts=None):
        functional.Adapter.__init__(cls, name, bases, dct, adapts=adapts)
        type(s_obj.BasePrototype).__init__(cls, name, bases, dct)


class SchemaDBObject(metaclass=SchemaDBObjectMeta):
    @classmethod
    def adapt(cls, obj):
        return cls.copy(obj)

    @classmethod
    def get_canonical_class(cls):
        for base in cls.__bases__:
            if issubclass(base, s_obj.ProtoObject) and not issubclass(base, SchemaDBObject):
                return base

        return cls


class CallDeltaHook(dbops.Command):
    def __init__(self, *, hook, stage, op, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.hook = hook
        self.stage = stage
        self.op = op

    async def execute(self, context):
        try:
            self.op.call_hook(context.session, stage=self.stage, hook=self.hook)
        except sd.DeltaHookNotFoundError:
            pass


class ConstraintCommon:
    def constraint_name(self, quote=True):
        name = self.raw_constraint_name()
        name = common.edgedb_name_to_pg_name(name)
        return common.quote_ident(name) if quote else name

    def schema_constraint_name(self):
        return self._constraint.name

    def raw_constraint_name(self):
        name = '{};{}'.format(self._constraint.name, 'schemaconstr')
        return name

    async def extra(self, context):
        text = self.raw_constraint_name()
        cmd = dbops.Comment(object=self, text=text)
        return [cmd]

    def rename_extra(self, context, new_constraint):
        new_name = new_constraint.raw_constraint_name()
        cmd = dbops.Comment(object=new_constraint, text=new_name)
        return [cmd]

    @property
    def is_abstract(self):
        return self._constraint.is_abstract


class SchemaConstraintDomainConstraint(ConstraintCommon, dbops.DomainConstraint):
    def __init__(self, domain_name, constraint, exprdata):
        super().__init__(domain_name)
        self._exprdata = exprdata
        self._constraint = constraint

    async def extra(self, context):
        # There seems to be no direct way to COMMENT on a domain constraint.
        # See http://www.postgresql.org/message-id/5310157.yWWCtg2qIU@klinga.prans.org
        # Work this around by updating pg_description directly.
        #
        # text = self.raw_constraint_name()
        # cmd = dbops.Comment(object=self, text=text)
        # return [cmd]

        table = pg_catalogs.PgDescriptionTable()
        rec = table.record()

        objoid = dbops.Query(
            '(SELECT oid FROM pg_constraint WHERE conname = $1)',
            [self.constraint_name(quote=False)], type='oid'
        )

        classoid = dbops.Query(
            '''(SELECT c.oid
                    FROM
                        pg_class c INNER JOIN pg_namespace ns ON c.relnamespace = ns.oid
                    WHERE c.relname = 'pg_constraint' AND ns.nspname = 'pg_catalog')
            ''', [], type='oid'
        )

        rec.objoid = objoid
        rec.classoid = classoid
        rec.description = self.raw_constraint_name()
        rec.objsubid = 0

        cond = [('objoid', objoid), ('classoid', classoid)]
        cmd = dbops.Merge(table=table, record=rec, condition=cond)

        return [cmd]

    async def constraint_code(self, context):
        if len(self._exprdata) == 1:
            expr = self._exprdata[0]['exprdata']['plain']
        else:
            exprs = [e['plain'] for e in self._exprdata['exprdata']]
            expr = '(' + ') AND ('.join(exprs) + ')'

        return 'CHECK ({})'.format(expr)

    def __repr__(self):
        return '<{}.{} "{}" "%r">' % (self.__class__.__module__, self.__class__.__name__,
                                      self.domain_name, self._constraint)


class SchemaConstraintTableConstraint(ConstraintCommon, dbops.TableConstraint):
    def __init__(self, table_name, *, constraint, exprdata, scope, type):
        super().__init__(table_name, None)
        self._constraint = constraint
        self._exprdata = exprdata
        self._scope = scope
        self._type = type

    async def constraint_code(self, context):
        ql = postgresql.string.quote_literal

        if self._scope == 'row':
            if len(self._exprdata) == 1:
                expr = self._exprdata[0]['exprdata']['plain']
            else:
                exprs = [e['exprdata']['plain'] for e in self._exprdata]
                expr = '(' + ') AND ('.join(exprs) + ')'

            expr = 'CHECK ({})'.format(expr)

        else:
            if self._type != 'unique':
                raise ValueError('unexpected constraint type: {}'.format(self._type))

            constr_exprs = []

            for expr in self._exprdata:
                if expr['is_trivial']:
                    # A constraint that contains one or more
                    # references to columns, and no expressions.
                    #
                    expr = ', '.join(expr['exprdata']['plain_chunks'])
                    expr = 'UNIQUE ({})'.format(expr)
                else:
                    # Complex constraint with arbitrary expressions
                    # needs to use EXCLUDE.
                    #
                    chunks = expr['exprdata']['plain_chunks']
                    expr = ', '.join("{} WITH =".format(chunk) for chunk in chunks)
                    expr = 'EXCLUDE ({})'.format(expr)

                constr_exprs.append(expr)

            expr = constr_exprs

        return expr

    def numbered_constraint_name(self, i, quote=True):
        raw_name = self.raw_constraint_name()
        name = common.edgedb_name_to_pg_name('{}#{}'.format(raw_name, i))
        return common.quote_ident(name) if quote else name

    def get_trigger_procname(self):
        schema = common.edgedb_module_name_to_schema_name(
            self.schema_constraint_name().module)
        proc_name = common.edgedb_name_to_pg_name(
            self.raw_constraint_name() + '_trigproc')
        return schema, proc_name

    def get_trigger_condition(self):
        chunks = []

        for expr in self._exprdata:
            condition = '{old_expr} IS DISTINCT FROM {new_expr}'.format(
                            old_expr=expr['exprdata']['old'],
                            new_expr=expr['exprdata']['new'])
            chunks.append(condition)

        if len(chunks) == 1:
            return chunks[0]
        else:
            return '(' + ') OR ('.join(chunks) + ')'

    def get_trigger_proc_text(self):
        chunks = []

        if self.is_multiconstraint():
            constr_name = self.numbered_constraint_name(0)
        else:
            constr_name = self.constraint_name()

        errmsg = 'duplicate key value violates unique constraint {constr}' \
                    .format(constr=constr_name)

        for expr in self._exprdata:
            exprdata = expr['exprdata']

            text = '''
                      PERFORM
                            TRUE
                          FROM
                            {table}
                          WHERE
                            {plain_expr} = {new_expr};
                      IF FOUND THEN
                          RAISE unique_violation
                              USING
                                  MESSAGE = '{errmsg}',
                                  DETAIL = 'Key ({plain_expr}) already exists.';
                      END IF;
                   '''.format(plain_expr=exprdata['plain'],
                              new_expr=exprdata['new'],
                              table=self.get_subject_name(),
                              errmsg=errmsg)

            chunks.append(text)

        text = 'BEGIN\n' + '\n\n'.join(chunks) + '\nRETURN NEW;\nEND;'

        return text

    def is_multiconstraint(self):
        """Returns True, if this constraint needs multiple database
           constraints to satisfy all conditions.
        """
        return self._scope != 'row' and len(self._exprdata) > 1

    def is_natively_inherited(self):
        """Returns True, if this constraint can be inherited natively."""
        return self._type == 'check'

    def __repr__(self):
        return '<{}.{} {!r}>'.format(
                    self.__class__.__module__, self.__class__.__name__,
                    self._constraint)


class MultiConstraintItem:
    def __init__(self, constraint, index):
        self.constraint = constraint
        self.index = index

    def get_type(self):
        return self.constraint.get_type()

    def get_id(self):
        raw_name = self.constraint.raw_constraint_name()
        name = common.edgedb_name_to_pg_name('{}#{}'.format(raw_name, self.index))
        name = common.quote_ident(name)

        return '{} ON {} {}'.format(name,
                                    self.constraint.get_subject_type(),
                                    self.constraint.get_subject_name())


class AlterTableAddMultiConstraint(dbops.AlterTableAddConstraint):
    async def code(self, context):
        exprs = await self.constraint.constraint_code(context)

        if isinstance(exprs, list) and len(exprs) > 1:
            chunks = []

            for i, expr in enumerate(exprs):
                name = self.constraint.numbered_constraint_name(i)
                chunk = 'ADD CONSTRAINT {} {}'.format(name, expr)
                chunks.append(chunk)

            code = ', '.join(chunks)
        else:
            if isinstance(exprs, list):
                exprs = exprs[0]

            name = self.constraint.constraint_name()
            code = 'ADD CONSTRAINT {} {}'.format(name, exprs)

        return code

    async def extra(self, context, alter_table):
        comments = []

        exprs = await self.constraint.constraint_code(context)
        constr_name = self.constraint.raw_constraint_name()

        if isinstance(exprs, list) and len(exprs) > 1:
            for i, expr in enumerate(exprs):
                constraint = MultiConstraintItem(self.constraint, i)

                comment = dbops.Comment(constraint, constr_name)
                comments.append(comment)
        else:
            comment = dbops.Comment(self.constraint, constr_name)
            comments.append(comment)

        return comments


class AlterTableRenameMultiConstraint(dbops.AlterTableBaseMixin,
                                      dbops.CommandGroup):
    def __init__(self, name, *, constraint, new_constraint,
                                contained=False, conditions=None,
                                neg_conditions=None, priority=0):

        dbops.CommandGroup.__init__(self, conditions=conditions,
                                          neg_conditions=neg_conditions,
                                          priority=priority)

        dbops.AlterTableBaseMixin.__init__(self, name=name,
                                                 contained=contained)

        self.constraint = constraint
        self.new_constraint = new_constraint

    async def execute(self, context):
        c = self.constraint
        nc = self.new_constraint

        exprs = await self.constraint.constraint_code(context)

        if isinstance(exprs, list) and len(exprs) > 1:
            for i, expr in enumerate(exprs):
                old_name = c.numbered_constraint_name(i, quote=False)
                new_name = nc.numbered_constraint_name(i, quote=False)

                ac = dbops.AlterTableRenameConstraintSimple(
                        name=self.name, old_name=old_name, new_name=new_name)

                self.add_command(ac)
        else:
            old_name = c.constraint_name(quote=False)
            new_name = nc.constraint_name(quote=False)

            ac = dbops.AlterTableRenameConstraintSimple(
                    name=self.name, old_name=old_name, new_name=new_name)

            self.add_command(ac)

        return await super().execute(context)

    async def extra(self, context):
        comments = []

        exprs = await self.new_constraint.constraint_code(context)
        constr_name = self.new_constraint.raw_constraint_name()

        if isinstance(exprs, list) and len(exprs) > 1:
            for i, expr in enumerate(exprs):
                constraint = MultiConstraintItem(self.new_constraint, i)

                comment = dbops.Comment(constraint, constr_name)
                comments.append(comment)
        else:
            comment = dbops.Comment(self.new_constraint, constr_name)
            comments.append(comment)

        return comments


class AlterTableDropMultiConstraint(dbops.AlterTableDropConstraint):
    async def code(self, context):
        exprs = await self.constraint.constraint_code(context)

        if isinstance(exprs, list) and len(exprs) > 1:
            chunks = []

            for i, expr in enumerate(exprs):
                name = self.constraint.numbered_constraint_name(i)
                chunk = 'DROP CONSTRAINT {}'.format(name)
                chunks.append(chunk)

            code = ', '.join(chunks)

        else:
            name = self.constraint.constraint_name()
            code = 'DROP CONSTRAINT {}'.format(name)

        return code


class AlterTableInheritableConstraintBase(dbops.AlterTableBaseMixin,
                                          dbops.CommandGroup):
    def __init__(self, name, *, constraint, contained=False, conditions=None,
                                neg_conditions=None, priority=0):

        dbops.CompositeCommandGroup.__init__(self, conditions=conditions,
                                             neg_conditions=neg_conditions,
                                             priority=priority)

        dbops.AlterTableBaseMixin.__init__(self, name=name, contained=contained)

        self._constraint = constraint

    def create_constr_trigger(self, table_name, constraint, proc_name):
        cmds = []

        cname = constraint.raw_constraint_name()

        ins_trigger_name = common.edgedb_name_to_pg_name(cname + '_instrigger')
        ins_trigger = dbops.Trigger(
                            name=ins_trigger_name, table_name=table_name,
                            events=('insert',), procedure=proc_name,
                            is_constraint=True, inherit=True)
        cr_ins_trigger = dbops.CreateTrigger(ins_trigger)
        cmds.append(cr_ins_trigger)

        disable_ins_trigger = dbops.DisableTrigger(ins_trigger, self_only=True)
        cmds.append(disable_ins_trigger)

        upd_trigger_name = common.edgedb_name_to_pg_name(cname + '_updtrigger')
        condition = constraint.get_trigger_condition()

        upd_trigger = dbops.Trigger(
                            name=upd_trigger_name, table_name=table_name,
                            events=('update',), procedure=proc_name,
                            condition=condition, is_constraint=True,
                            inherit=True)
        cr_upd_trigger = dbops.CreateTrigger(upd_trigger)
        cmds.append(cr_upd_trigger)

        disable_upd_trigger = dbops.DisableTrigger(upd_trigger, self_only=True)
        cmds.append(disable_upd_trigger)

        return cmds

    def rename_constr_trigger(self, table_name):
        constraint = self._constraint
        new_constr = self._new_constraint

        cname = constraint.raw_constraint_name()
        ncname = new_constr.raw_constraint_name()

        ins_trigger_name = common.edgedb_name_to_pg_name(cname + '_instrigger')
        new_ins_trg_name = common.edgedb_name_to_pg_name(ncname + '_instrigger')

        ins_trigger = dbops.Trigger(
                            name=ins_trigger_name, table_name=table_name,
                            events=('insert',), procedure='null',
                            is_constraint=True, inherit=True)

        rn_ins_trigger = dbops.AlterTriggerRenameTo(
                            ins_trigger,
                            new_name=new_ins_trg_name)

        upd_trigger_name = common.edgedb_name_to_pg_name(cname + '_updtrigger')
        new_upd_trg_name = common.edgedb_name_to_pg_name(ncname + '_updtrigger')

        upd_trigger = dbops.Trigger(
                            name=upd_trigger_name, table_name=table_name,
                            events=('update',), procedure='null',
                            is_constraint=True, inherit=True)

        rn_upd_trigger = dbops.AlterTriggerRenameTo(
                            upd_trigger,
                            new_name=new_upd_trg_name)

        return (rn_ins_trigger, rn_upd_trigger)

    def drop_constr_trigger(self, table_name, constraint):
        cname = constraint.raw_constraint_name()

        ins_trigger_name = common.edgedb_name_to_pg_name(cname + '_instrigger')
        ins_trigger = dbops.Trigger(
                            name=ins_trigger_name, table_name=table_name,
                            events=('insert',), procedure='null',
                            is_constraint=True, inherit=True)

        drop_ins_trigger = dbops.DropTrigger(ins_trigger)

        upd_trigger_name = common.edgedb_name_to_pg_name(cname + '_updtrigger')
        upd_trigger = dbops.Trigger(
                            name=upd_trigger_name, table_name=table_name,
                            events=('update',), procedure='null',
                            is_constraint=True, inherit=True)

        drop_upd_trigger = dbops.DropTrigger(upd_trigger)

        return [drop_ins_trigger, drop_upd_trigger]

    def drop_constr_trigger_function(self, proc_name):
        return [dbops.DropFunction(name=proc_name, args=())]

    def create_constraint(self, constraint):
        # Add the constraint normally to our table
        #
        my_alter = dbops.AlterTable(self.name)
        add_constr = AlterTableAddMultiConstraint(constraint=constraint)
        my_alter.add_command(add_constr)

        self.add_command(my_alter)

        if not constraint.is_natively_inherited():
            # The constraint is not inherited by descendant tables natively,
            # use triggers to emulate inheritance.
            #

            # Create trigger function
            #
            proc_name = constraint.get_trigger_procname()
            proc_text = constraint.get_trigger_proc_text()
            proc = dbops.CreateTriggerFunction(name=proc_name, text=proc_text,
                                               volatility='stable')
            self.add_command(proc)

            # Add a (disabled) inheritable trigger on self.
            # Trigger inheritance will propagate and maintain
            # the trigger on current and future descendants.
            #
            cr_trigger = self.create_constr_trigger(self.name, constraint,
                                                    proc_name)
            self.add_commands(cr_trigger)

    def rename_constraint(self, old_constraint, new_constraint):
        # Rename the native constraint(s) normally
        #
        rename_constr = AlterTableRenameMultiConstraint(
                                name=self.name,
                                constraint=old_constraint,
                                new_constraint=new_constraint)
        self.add_command(rename_constr)

        if not old_constraint.is_natively_inherited():
            # Alter trigger function
            #
            old_proc_name = old_constraint.get_trigger_procname()
            new_proc_name = new_constraint.get_trigger_procname()

            rename_proc = dbops.RenameFunction(name=old_proc_name, args=(),
                                               new_name=new_proc_name)
            self.add_command(rename_proc)

            new_proc_text = new_constraint.get_trigger_proc_text()
            alter_text = dbops.AlterFunctionReplaceText(
                                    name=new_proc_name, args=(),
                                    new_text=new_proc_text)

            self.add_command(alter_text)

            mv_trigger = self.rename_constr_trigger(self.name)
            self.add_commands(mv_trigger)

    def alter_constraint(self, old_constraint, new_constraint):
        if old_constraint.is_abstract and not new_constraint.is_abstract:
            # No longer abstract, create db structures
            self.create_constraint(new_constraint)

        elif not old_constraint.is_abstract and new_constraint.is_abstract:
            # Now abstract, drop db structures
            self.drop_constraint(new_constraint)

        else:
            # Some other modification, drop/create
            self.drop_constraint(new_constraint)
            self.create_constraint(new_constraint)

    def drop_constraint(self, constraint):
        if not constraint.is_natively_inherited():
            self.add_commands(self.drop_constr_trigger(self.name, constraint))

            # Drop trigger function
            #
            proc_name = constraint.raw_constraint_name() + '_trigproc'
            proc_name = self.name[0], common.edgedb_name_to_pg_name(proc_name)

            self.add_commands(self.drop_constr_trigger_function(proc_name))

        # Drop the constraint normally from our table
        #
        my_alter = dbops.AlterTable(self.name)

        drop_constr = AlterTableDropMultiConstraint(constraint=constraint)
        my_alter.add_command(drop_constr)

        self.add_command(my_alter)


class AlterTableAddInheritableConstraint(AlterTableInheritableConstraintBase):
    def __repr__(self):
        return '<{}.{} {!r}>'.format(self.__class__.__module__,
                                     self.__class__.__name__,
                                     self._constraint)

    def _execute(self, context, code, vars):
        if not self._constraint.is_abstract:
            self.create_constraint(self._constraint)
        super()._execute(context, code, vars)


class AlterTableRenameInheritableConstraint(AlterTableInheritableConstraintBase):
    def __init__(self, name, *, constraint, new_constraint,  **kwargs):
        super().__init__(name, constraint=constraint, **kwargs)
        self._new_constraint = new_constraint

    def __repr__(self):
        return '<{}.{} {!r}>'.format(self.__class__.__module__,
                                     self.__class__.__name__,
                                     self._constraint)

    async def execute(self, context):
        if not self._constraint.is_abstract:
            self.rename_constraint(self._constraint, self._new_constraint)
        await super().execute(context)


class AlterTableAlterInheritableConstraint(AlterTableInheritableConstraintBase):
    def __init__(self, name, *, constraint, new_constraint,  **kwargs):
        super().__init__(name, constraint=constraint, **kwargs)
        self._new_constraint = new_constraint

    def __repr__(self):
        return '<{}.{} {!r}>'.format(self.__class__.__module__,
                                     self.__class__.__name__,
                                     self._constraint)

    async def execute(self, context):
        self.alter_constraint(self._constraint, self._new_constraint)
        await super().execute(context)


class AlterTableDropInheritableConstraint(AlterTableInheritableConstraintBase):
    def __repr__(self):
        return '<{}.{} {!r}>'.format(self.__class__.__module__,
                                     self.__class__.__name__,
                                     self._constraint)

    async def execute(self, context):
        if not self._constraint.is_abstract:
            self.drop_constraint(self._constraint)
        await super().execute(context)


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
        name = common.edgedb_name_to_pg_name(name)
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
        name = name or ('edgedb', 'deltaref')
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
        name = name or ('edgedb', 'deltalog')
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
        name = name or ('edgedb', 'module')
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
        name = name or ('edgedb', 'metaobject')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='id', type='serial', required=True, readonly=True),
            dbops.Column(name='name', type='text', required=True),
            dbops.Column(name='is_abstract', type='boolean', required=True, default=False),
            dbops.Column(name='is_final', type='boolean', required=True, default=False),
            dbops.Column(name='title', type='edgedb.hstore'),
            dbops.Column(name='description', type='text')
        ])

        self.constraints = set([
            dbops.PrimaryKey(name, columns=('id',)),
            dbops.UniqueConstraint(name, columns=('name',))
        ])

        self._columns = self.columns()


class AttributeTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('edgedb', 'attribute'))
        self.bases = [('edgedb', 'metaobject')]

        self.constraints = set([
            dbops.PrimaryKey(('edgedb', 'attribute'), columns=('id',)),
            dbops.UniqueConstraint(('edgedb', 'attribute'), columns=('name',))
        ])

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='type', type='bytea', required=True)
        ])

        self._columns = self.columns()


class AttributeValueTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('edgedb', 'attribute_value'))
        self.bases = [('edgedb', 'metaobject')]

        self.constraints = set([
            dbops.PrimaryKey(('edgedb', 'attribute_value'), columns=('id',)),
            dbops.UniqueConstraint(('edgedb', 'attribute_value'), columns=('name',))
        ])

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='subject', type='integer', required=True),
            dbops.Column(name='attribute', type='integer', required=True),
            dbops.Column(name='value', type='bytea')
        ])

        self._columns = self.columns()


class ConstraintTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('edgedb', 'constraint'))
        self.bases = [('edgedb', 'metaobject')]

        self.constraints = set([
            dbops.PrimaryKey(('edgedb', 'constraint'), columns=('id',)),
            dbops.UniqueConstraint(('edgedb', 'constraint'), columns=('name',))
        ])

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='base', type='text[]'),
            dbops.Column(name='subject', type='integer'),
            dbops.Column(name='expr', type='text'),
            dbops.Column(name='subjectexpr', type='text'),
            dbops.Column(name='localfinalexpr', type='text'),
            dbops.Column(name='finalexpr', type='text'),
            dbops.Column(name='errmessage', type='text'),
            dbops.Column(name='paramtypes', type='edgedb.hstore'),
            dbops.Column(name='inferredparamtypes', type='edgedb.hstore'),
            dbops.Column(name='args', type='bytea')
        ])

        self._columns = self.columns()


class AtomTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('edgedb', 'atom'))

        self.bases = [('edgedb', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='base', type='text'),
            dbops.Column(name='constraints', type='edgedb.hstore'),
            dbops.Column(name='default', type='text'),
            dbops.Column(name='attributes', type='edgedb.hstore')
        ])

        self.constraints = set([
            dbops.PrimaryKey(('edgedb', 'atom'), columns=('id',)),
            dbops.UniqueConstraint(('edgedb', 'atom'), columns=('name',))
        ])

        self._columns = self.columns()


class ConceptTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('edgedb', 'concept'))

        self.bases = [('edgedb', 'metaobject')]

        self.constraints = set([
            dbops.PrimaryKey(('edgedb', 'concept'), columns=('id',)),
            dbops.UniqueConstraint(('edgedb', 'concept'), columns=('name',))
        ])

        self._columns = self.columns()


class LinkTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('edgedb', 'link'))

        self.bases = [('edgedb', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='source_id', type='integer'),
            dbops.Column(name='target_id', type='integer'),
            dbops.Column(name='spectargets', type='text[]'),
            dbops.Column(name='mapping', type='char(2)', required=True),
            dbops.Column(name='exposed_behaviour', type='text'),
            dbops.Column(name='required', type='boolean', required=True, default=False),
            dbops.Column(name='readonly', type='boolean', required=True, default=False),
            dbops.Column(name='loading', type='text'),
            dbops.Column(name='base', type='text[]'),
            dbops.Column(name='default', type='text'),
            dbops.Column(name='constraints', type='edgedb.hstore'),
            dbops.Column(name='abstract_constraints', type='edgedb.hstore')
        ])

        self.constraints = set([
            dbops.PrimaryKey(('edgedb', 'link'), columns=('id',)),
            dbops.UniqueConstraint(('edgedb', 'link'), columns=('name',))
        ])

        self._columns = self.columns()


class LinkPropertyTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('edgedb', 'link_property'))

        self.bases = [('edgedb', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='source_id', type='integer'),
            dbops.Column(name='target_id', type='integer'),
            dbops.Column(name='required', type='boolean', required=True, default=False),
            dbops.Column(name='readonly', type='boolean', required=True, default=False),
            dbops.Column(name='loading', type='text'),
            dbops.Column(name='base', type='text[]'),
            dbops.Column(name='default', type='text'),
            dbops.Column(name='constraints', type='edgedb.hstore'),
            dbops.Column(name='abstract_constraints', type='edgedb.hstore')
        ])

        self.constraints = set([
            dbops.PrimaryKey(('edgedb', 'link_property'), columns=('id',)),
            dbops.UniqueConstraint(('edgedb', 'link_property'), columns=('name',))
        ])

        self._columns = self.columns()


class ComputableTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('edgedb', 'computable'))

        self.bases = [('edgedb', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='source_id', type='integer'),
            dbops.Column(name='target_id', type='integer'),
            dbops.Column(name='expression', type='text'),
            dbops.Column(name='is_local', type='bool')
        ])

        self.constraints = set([
            dbops.PrimaryKey(('edgedb', 'computable'), columns=('id',)),
            dbops.UniqueConstraint(('edgedb', 'computable'), columns=('name',))
        ])

        self._columns = self.columns()


class FeatureTable(dbops.Table):
    def __init__(self, name=None):
        name = name or ('edgedb', 'feature')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='name', type='text', required=True),
            dbops.Column(name='class_name', type='text', required=True)
        ])

        self.constraints = set([
            dbops.PrimaryKey(name, columns=('name',)),
        ])

        self._columns = self.columns()


class ActionTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('edgedb', 'action'))
        self.bases = [('edgedb', 'metaobject')]

        self.constraints = set([
            dbops.PrimaryKey(('edgedb', 'action'), columns=('id',)),
            dbops.UniqueConstraint(('edgedb', 'action'), columns=('name',))
        ])


class EventTable(MetaObjectTable):
    def __init__(self):
        super().__init__(name=('edgedb', 'event'))
        self.bases = [('edgedb', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='base', type='text[]')
        ])

        self.constraints = set([
            dbops.PrimaryKey(('edgedb', 'event'), columns=('id',)),
            dbops.UniqueConstraint(('edgedb', 'event'), columns=('name',))
        ])

        self._columns = self.columns()


class PolicyTable(MetaObjectTable):
    def __init__(self, name=None):
        name = name or ('edgedb', 'policy')
        super().__init__(name=name)

        self.bases = [('edgedb', 'metaobject')]

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='subject', type='integer', required=True),
            dbops.Column(name='event', type='integer', required=True),
            dbops.Column(name='actions', type='integer[]', required=True)
        ])

        self.constraints = set([
            dbops.PrimaryKey(('edgedb', 'policy'), columns=('id',)),
            dbops.UniqueConstraint(('edgedb', 'policy'), columns=('name',))
        ])

        self._columns = self.columns()


class BackendInfoTable(dbops.Table):
    def __init__(self, name=None):
        name = name or ('edgedb', 'backend_info')
        super().__init__(name=name)

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='format_version', type='int', required=True),
        ])

        self._columns = self.columns()


class EntityModStatType(dbops.CompositeType):
    def __init__(self):
        super().__init__(name=('edgedb', 'entity_modstat_rec_t'))

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='std.id', type='uuid'),
            dbops.Column(name='std.mtime', type='timestamptz'),
        ])

        self._columns = self.columns()


class LinkEndpointsType(dbops.CompositeType):
    def __init__(self):
        super().__init__(name=('edgedb', 'link_endpoints_rec_t'))

        self.__columns = datastructures.OrderedSet([
            dbops.Column(name='source_id', type='uuid'),
            dbops.Column(name='target_id', type='uuid'),
        ])

        self._columns = self.columns()


class Feature:
    def __init__(self, name, schema='edgedb'):
        self.name = name
        self.schema = schema

    def get_extension_name(self):
        return self.name

    async def code(self, context):
        pg_ver = context.db.version_info

        name = common.quote_ident(self.get_extension_name())
        schema = common.quote_ident(self.schema)
        return 'CREATE EXTENSION {} WITH SCHEMA {}'.format(name, schema)

    @classmethod
    def init_feature(cls, db):
        pass

    @classmethod
    def reset_connection(cls, connection):
        pass


class EnableFeature(dbops.DDLOperation):
    def __init__(self, feature, *, conditions=None, neg_conditions=None, priority=0):
        super().__init__(conditions=conditions, neg_conditions=neg_conditions, priority=priority)

        self.feature = feature
        self.opid = feature.name

    async def code(self, context):
        return self.feature.code(context)

    async def extra(self, context, *args, **kwargs):
        table = FeatureTable()
        record = table.record()
        record.name = self.feature.name
        record.class_name = '%s.%s' % (self.feature.__class__.__module__,
                                       self.feature.__class__.__name__)
        return [dbops.Insert(table, records=[record])]

    async def execute(self, context):
        await super().execute(context)
        await self.feature.init_feature(context.db)

    def __repr__(self):
        return '<edgedb.sync.%s %s>' % (self.__class__.__name__, self.feature.name)
