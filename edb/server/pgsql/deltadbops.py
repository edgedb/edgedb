#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""Abstractions for low-level database DDL and DML operations."""

from edb.lang.schema import name as sn
from edb.lang.schema import objects as s_obj

from edb.lang.common import adapter

from edb.lang import edgeql

from edb.server.pgsql import common
from edb.server.pgsql import dbops
from edb.server.pgsql import metaschema


class SchemaDBObjectMeta(adapter.Adapter, type(s_obj.Object)):
    def __init__(cls, name, bases, dct, *, adapts=None):
        adapter.Adapter.__init__(cls, name, bases, dct, adapts=adapts)
        type(s_obj.Object).__init__(cls, name, bases, dct)


class SchemaDBObject(metaclass=SchemaDBObjectMeta):
    @classmethod
    def adapt(cls, obj):
        return cls.copy(obj)


class ConstraintCommon:
    def constraint_name(self, quote=True):
        name = self.raw_constraint_name()
        name = common.edgedb_name_to_pg_name(name)
        return common.quote_ident(name) if quote else name

    def schema_constraint_name(self):
        return self._constraint.get_name(self._schema)

    def raw_constraint_name(self):
        name = '{};{}'.format(self._constraint.get_name(self._schema),
                              'schemaconstr')
        return name

    def generate_extra(self, block):
        text = self.raw_constraint_name()
        cmd = dbops.Comment(object=self, text=text)
        cmd.generate(block)

    @property
    def is_abstract(self):
        return self._constraint.get_is_abstract(self._schema)


class SchemaConstraintDomainConstraint(
        ConstraintCommon, dbops.DomainConstraint):
    def __init__(self, domain_name, constraint, exprdata, schema):
        super().__init__(domain_name)
        self._exprdata = exprdata
        self._constraint = constraint
        self._schema = schema

    def constraint_code(self, block: dbops.PLBlock) -> str:
        if len(self._exprdata) == 1:
            expr = self._exprdata[0]['exprdata']['plain']
        else:
            exprs = [e['plain'] for e in self._exprdata['exprdata']]
            expr = '(' + ') AND ('.join(exprs) + ')'

        return f'CHECK ({expr})'

    def __repr__(self):
        return '<{}.{} "{}" "%r">' % (
            self.__class__.__module__, self.__class__.__name__,
            self.domain_name, self._constraint)


class SchemaConstraintTableConstraint(ConstraintCommon, dbops.TableConstraint):
    def __init__(self, table_name, *,
                 constraint, exprdata, scope, type, schema):
        super().__init__(table_name, None)
        self._constraint = constraint
        self._exprdata = exprdata
        self._scope = scope
        self._type = type
        self._schema = schema

    def constraint_code(self, block: dbops.PLBlock) -> str:
        if self._scope == 'row':
            if len(self._exprdata) == 1:
                expr = self._exprdata[0]['exprdata']['plain']
            else:
                exprs = [e['exprdata']['plain'] for e in self._exprdata]
                expr = '(' + ') AND ('.join(exprs) + ')'

            expr = f'CHECK ({expr})'

        else:
            if self._type != 'unique':
                raise ValueError(
                    'unexpected constraint type: {}'.format(self._type))

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
                    expr = ', '.join(
                        "{} WITH =".format(chunk) for chunk in chunks)
                    expr = f'EXCLUDE ({expr})'

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
            raw_constr_name = self.numbered_constraint_name(0, quote=False)
        else:
            constr_name = self.constraint_name()
            raw_constr_name = self.constraint_name(quote=False)

        errmsg = 'duplicate key value violates unique ' \
                 'constraint {constr}'.format(constr=constr_name)

        subject_table = self.get_subject_name()

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
                          TABLE = '{table[1]}',
                          SCHEMA = '{table[0]}',
                          CONSTRAINT = '{constr}',
                          MESSAGE = '{errmsg}',
                          DETAIL = 'Key ({plain_expr}) already exists.';
                END IF;
            '''.format(
                plain_expr=exprdata['plain'], new_expr=exprdata['new'],
                table=subject_table, constr=raw_constr_name, errmsg=errmsg)

            chunks.append(text)

        text = 'BEGIN\n' + '\n\n'.join(chunks) + '\nRETURN NEW;\nEND;'

        return text

    def is_multiconstraint(self):
        """Determine if multiple database constraints are needed."""
        return self._scope != 'row' and len(self._exprdata) > 1

    def is_natively_inherited(self):
        """Determine if this constraint can be inherited natively."""
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
        name = common.edgedb_name_to_pg_name(
            '{}#{}'.format(raw_name, self.index))
        name = common.quote_ident(name)

        return '{} ON {} {}'.format(
            name, self.constraint.get_subject_type(),
            self.constraint.get_subject_name())


class AlterTableAddMultiConstraint(dbops.AlterTableAddConstraint):
    def code(self, block: dbops.PLBlock) -> str:
        exprs = self.constraint.constraint_code(block)

        if isinstance(exprs, list) and len(exprs) > 1:
            chunks = []

            for i, expr in enumerate(exprs):
                name = self.constraint.numbered_constraint_name(i)
                chunk = f'ADD CONSTRAINT {name} {expr}'
                chunks.append(chunk)

            code = ', '.join(chunks)
        else:
            if isinstance(exprs, list):
                exprs = exprs[0]

            name = self.constraint.constraint_name()
            code = f'ADD CONSTRAINT {name} {exprs}'

        return code

    def generate_extra(self, block, alter_table):
        comments = []

        exprs = self.constraint.constraint_code(block)
        constr_name = self.constraint.raw_constraint_name()

        if isinstance(exprs, list) and len(exprs) > 1:
            for i, expr in enumerate(exprs):
                constraint = MultiConstraintItem(self.constraint, i)

                comment = dbops.Comment(constraint, constr_name)
                comments.append(comment)
        else:
            comment = dbops.Comment(self.constraint, constr_name)
            comments.append(comment)

        for comment in comments:
            comment.generate(block)


class AlterTableRenameMultiConstraint(
        dbops.AlterTableBaseMixin, dbops.CommandGroup):
    def __init__(
            self, name, *, constraint, new_constraint, contained=False,
            conditions=None, neg_conditions=None, priority=0):

        dbops.CommandGroup.__init__(
            self, conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)

        dbops.AlterTableBaseMixin.__init__(
            self, name=name, contained=contained)

        self.constraint = constraint
        self.new_constraint = new_constraint

    def generate(self, block: dbops.PLBlock) -> None:
        c = self.constraint
        nc = self.new_constraint

        exprs = self.constraint.constraint_code(block)

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

        return super().generate(block)

    def generate_extra(self, block: dbops.PLBlock) -> None:
        comments = []

        exprs = self.new_constraint.constraint_code(block)
        constr_name = self.new_constraint.raw_constraint_name()

        if isinstance(exprs, list) and len(exprs) > 1:
            for i, expr in enumerate(exprs):
                constraint = MultiConstraintItem(self.new_constraint, i)

                comment = dbops.Comment(constraint, constr_name)
                comments.append(comment)
        else:
            comment = dbops.Comment(self.new_constraint, constr_name)
            comments.append(comment)

        for comment in comments:
            comment.generate(block)


class AlterTableDropMultiConstraint(dbops.AlterTableDropConstraint):
    def code(self, block: dbops.PLBlock) -> str:
        exprs = self.constraint.constraint_code(block)

        if isinstance(exprs, list) and len(exprs) > 1:
            chunks = []

            for i, expr in enumerate(exprs):
                name = self.constraint.numbered_constraint_name(i)
                chunk = f'DROP CONSTRAINT {name}'
                chunks.append(chunk)

            code = ', '.join(chunks)

        else:
            name = self.constraint.constraint_name()
            code = f'DROP CONSTRAINT {name}'

        return code


class AlterTableInheritableConstraintBase(
        dbops.AlterTableBaseMixin, dbops.CommandGroup):
    def __init__(
            self, name, *, constraint, contained=False, conditions=None,
            neg_conditions=None, priority=0):

        dbops.CompositeCommandGroup.__init__(
            self, conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)

        dbops.AlterTableBaseMixin.__init__(
            self, name=name, contained=contained)

        self._constraint = constraint

    def create_constr_trigger(self, table_name, constraint, proc_name):
        cmds = []

        cname = constraint.raw_constraint_name()

        ins_trigger_name = common.edgedb_name_to_pg_name(cname + '_instrigger')
        ins_trigger = dbops.Trigger(
            name=ins_trigger_name, table_name=table_name, events=('insert', ),
            procedure=proc_name, is_constraint=True, inherit=True)
        cr_ins_trigger = dbops.CreateTrigger(ins_trigger)
        cmds.append(cr_ins_trigger)

        disable_ins_trigger = dbops.DisableTrigger(ins_trigger, self_only=True)
        cmds.append(disable_ins_trigger)

        upd_trigger_name = common.edgedb_name_to_pg_name(cname + '_updtrigger')
        condition = constraint.get_trigger_condition()

        upd_trigger = dbops.Trigger(
            name=upd_trigger_name, table_name=table_name, events=('update', ),
            procedure=proc_name, condition=condition, is_constraint=True,
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
        new_ins_trg_name = common.edgedb_name_to_pg_name(
            ncname + '_instrigger')

        ins_trigger = dbops.Trigger(
            name=ins_trigger_name, table_name=table_name, events=('insert', ),
            procedure='null', is_constraint=True, inherit=True)

        rn_ins_trigger = dbops.AlterTriggerRenameTo(
            ins_trigger, new_name=new_ins_trg_name)

        upd_trigger_name = common.edgedb_name_to_pg_name(cname + '_updtrigger')
        new_upd_trg_name = common.edgedb_name_to_pg_name(
            ncname + '_updtrigger')

        upd_trigger = dbops.Trigger(
            name=upd_trigger_name, table_name=table_name, events=('update', ),
            procedure='null', is_constraint=True, inherit=True)

        rn_upd_trigger = dbops.AlterTriggerRenameTo(
            upd_trigger, new_name=new_upd_trg_name)

        return (rn_ins_trigger, rn_upd_trigger)

    def drop_constr_trigger(self, table_name, constraint):
        cname = constraint.raw_constraint_name()

        ins_trigger_name = common.edgedb_name_to_pg_name(cname + '_instrigger')
        ins_trigger = dbops.Trigger(
            name=ins_trigger_name, table_name=table_name, events=('insert', ),
            procedure='null', is_constraint=True, inherit=True)

        drop_ins_trigger = dbops.DropTrigger(ins_trigger)

        upd_trigger_name = common.edgedb_name_to_pg_name(cname + '_updtrigger')
        upd_trigger = dbops.Trigger(
            name=upd_trigger_name, table_name=table_name, events=('update', ),
            procedure='null', is_constraint=True, inherit=True)

        drop_upd_trigger = dbops.DropTrigger(upd_trigger)

        return [drop_ins_trigger, drop_upd_trigger]

    def create_constr_trigger_function(self, constraint):
        proc_name = constraint.get_trigger_procname()
        proc_text = constraint.get_trigger_proc_text()

        func = dbops.Function(
            name=proc_name, text=proc_text, volatility='stable',
            returns='trigger', language='plpgsql')

        return [dbops.CreateOrReplaceFunction(func)]

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

            # Create trigger function
            self.add_commands(self.create_constr_trigger_function(constraint))

            # Add a (disabled) inheritable trigger on self.
            # Trigger inheritance will propagate and maintain
            # the trigger on current and future descendants.
            proc_name = constraint.get_trigger_procname()
            cr_trigger = self.create_constr_trigger(
                self.name, constraint, proc_name)
            self.add_commands(cr_trigger)

    def rename_constraint(self, old_constraint, new_constraint):
        # Rename the native constraint(s) normally
        #
        rename_constr = AlterTableRenameMultiConstraint(
            name=self.name, constraint=old_constraint,
            new_constraint=new_constraint)
        self.add_command(rename_constr)

        if not old_constraint.is_natively_inherited():
            # Alter trigger function
            #
            old_proc_name = old_constraint.get_trigger_procname()
            new_proc_name = new_constraint.get_trigger_procname()

            rename_proc = dbops.RenameFunction(
                name=old_proc_name, args=(), new_name=new_proc_name)
            self.add_command(rename_proc)

            self.add_commands(
                self.create_constr_trigger_function(new_constraint))

            mv_trigger = self.rename_constr_trigger(self.name)
            self.add_commands(mv_trigger)

    def alter_constraint(self, old_constraint, new_constraint):
        if old_constraint.is_abstract and not new_constraint.is_abstract:
            # No longer delegated, create db structures
            self.create_constraint(new_constraint)

        elif not old_constraint.is_abstract and new_constraint.is_abstract:
            # Now delegated, drop db structures
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
        return '<{}.{} {!r}>'.format(
            self.__class__.__module__, self.__class__.__name__,
            self._constraint)

    def generate(self, block):
        if not self._constraint.is_abstract:
            self.create_constraint(self._constraint)
        super().generate(block)


class AlterTableRenameInheritableConstraint(
        AlterTableInheritableConstraintBase):
    def __init__(self, name, *, constraint, new_constraint, **kwargs):
        super().__init__(name, constraint=constraint, **kwargs)
        self._new_constraint = new_constraint

    def __repr__(self):
        return '<{}.{} {!r}>'.format(
            self.__class__.__module__, self.__class__.__name__,
            self._constraint)

    def generate(self, block):
        if not self._constraint.is_abstract:
            self.rename_constraint(self._constraint, self._new_constraint)
        super().generate(block)


class AlterTableAlterInheritableConstraint(
        AlterTableInheritableConstraintBase):
    def __init__(self, name, *, constraint, new_constraint, **kwargs):
        super().__init__(name, constraint=constraint, **kwargs)
        self._new_constraint = new_constraint

    def __repr__(self):
        return '<{}.{} {!r}>'.format(
            self.__class__.__module__, self.__class__.__name__,
            self._constraint)

    def generate(self, block):
        self.alter_constraint(self._constraint, self._new_constraint)
        super().generate(block)


class AlterTableDropInheritableConstraint(AlterTableInheritableConstraintBase):
    def __repr__(self):
        return '<{}.{} {!r}>'.format(
            self.__class__.__module__, self.__class__.__name__,
            self._constraint)

    def generate(self, block):
        if not self._constraint.is_abstract:
            self.drop_constraint(self._constraint)
        super().generate(block)


class MappingIndex(dbops.Index):
    def __init__(self, name_prefix, cardinality, link_names, table_name):
        super().__init__(None, table_name, True)
        self.link_names = link_names
        self.name_prefix = name_prefix
        self.cardinality = cardinality

    async def creation_code(self, context):
        link_map = await context.get_class_map()

        ids = tuple(sorted(list(link_map[n] for n in self.link_names)))
        id_str = '_'.join(str(i) for i in ids)

        name = '%s_%s_%s_cardinality_idx' % (
            self.name_prefix, id_str, self.cardinality)
        name = common.edgedb_name_to_pg_name(name)
        predicate = 'ptr_item_id IN (%s)' % ', '.join(str(id) for id in ids)

        code = '''
            CREATE {unique} INDEX {name} ON {table}s ({cols}) {predicate}
        '''.format(unique='UNIQUE',
                   name=common.qname(name),
                   table=common.qname(*self.table_name),
                   cols=', '.join(common.quote_ident(c) for c in self.columns),
                   predicate=('WHERE {}'.format(predicate)))

        return code

    def __repr__(self):
        name = '%s_%s_%s_cardinality_idx' % (
            self.name_prefix, '<HASH>', self.cardinality)
        predicate = 'ptr_item_id IN (%s)' % ', '.join(
            str(n) for n in self.link_names)

        return \
            '<{mod.{cls} name="{name}" cols=({cols}) unique={uniq} ' \
            'predicate={pred}>'.format(
                mod=self.__class__.__module__,
                cls=self.__class__.__name__,
                name=name,
                cols=','.join(self.columns),
                uniq=self.unique,
                pred=predicate)


class MangleExprObjectRefs(dbops.Command):
    def __init__(self, *, scls, field, expr,
                 conditions=None, neg_conditions=None, priority=0,
                 schema):
        super().__init__(
            conditions=conditions, neg_conditions=neg_conditions,
            priority=priority)

        self.name = scls.get_name(schema)
        self.table = metaschema.get_metaclass_table(scls.__class__)
        self.field = common.edgedb_name_to_pg_name(field)
        self.expr = expr

    async def execute(self, context):
        class_map = await context._get_class_map()

        def _cb(name):
            clsid = class_map.get(name)
            if clsid:
                return sn.Name(module='__class__', name=str(clsid))
            else:
                return name
        expr = edgeql.rewrite_refs(self.expr, _cb)

        rec = self.table.record()
        setattr(rec, self.field, expr)
        condition = [('name', str(self.name))]
        upd = dbops.Update(table=self.table, record=rec, condition=condition)

        await upd.execute(context)
