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

from __future__ import annotations
from typing import Optional, Tuple, List, Set

import itertools

from edb.common import adapter

from edb.schema import name as s_name
from edb.schema import pointers as s_pointers
from edb.schema import objects as s_obj

from edb.pgsql import common
from edb.pgsql import dbops
from edb.pgsql import schemamech
from edb.pgsql import trampoline


class SchemaDBObjectMeta(adapter.Adapter):  # type: ignore
    def __init__(cls, name, bases, dct, *, adapts=None):
        adapter.Adapter.__init__(cls, name, bases, dct, adapts=adapts)
        type(s_obj.Object).__init__(cls, name, bases, dct)


class SchemaDBObject(metaclass=SchemaDBObjectMeta):
    @classmethod
    def adapt(cls, obj):
        return cls.copy(obj)


class ConstraintCommon:
    def __init__(self, constraint, schema):
        self._constr_id = constraint.id
        self._schema_constr_name = constraint.get_name(schema)
        self._schema_constr_is_delegated = constraint.get_delegated(schema)
        self._schema = schema
        self._constraint = constraint

    def constraint_name(self, quote=True):
        name = self.raw_constraint_name()
        name = common.edgedb_name_to_pg_name(name)
        return common.quote_ident(name) if quote else name

    def schema_constraint_name(self):
        return self._schema_constr_name

    def raw_constraint_name(self):
        return common.get_constraint_raw_name(self._constr_id)

    def generate_extra(self, block):
        text = self.raw_constraint_name()
        cmd = dbops.Comment(object=self, text=text)
        cmd.generate(block)

    @property
    def delegated(self):
        return self._schema_constr_is_delegated


class SchemaConstraintDomainConstraint(
    ConstraintCommon, dbops.DomainConstraint
):
    def __init__(self, domain_name, constraint, exprdata, schema):
        ConstraintCommon.__init__(self, constraint, schema)
        dbops.DomainConstraint.__init__(self, domain_name)
        self._exprdata = exprdata

    def constraint_code(self, block: dbops.PLBlock) -> str:
        if len(self._exprdata) == 1:
            expr = self._exprdata[0].exprdata.plain
        else:
            exprs = [e.plain for e in self._exprdata.exprdata]
            expr = '(' + ') AND ('.join(exprs) + ')'

        return f'CHECK ({expr})'

    def __repr__(self):
        return '<{}.{} {!r} {!r}>'.format(
            self.__class__.__module__, self.__class__.__name__,
            self.domain_name, self._constraint)


class SchemaConstraintTableConstraint(ConstraintCommon, dbops.TableConstraint):
    def __init__(
        self,
        table_name,
        *,
        constraint,
        exprdata: List[schemamech.ExprData],
        origin_exprdata: List[schemamech.ExprData],
        scope,
        type,
        table_type,
        except_data,
        schema,
    ):
        ConstraintCommon.__init__(self, constraint, schema)
        dbops.TableConstraint.__init__(self, table_name, None)
        self._exprdata = exprdata
        self._origin_exprdata = origin_exprdata
        self._scope = scope
        self._type = type
        self._table_type = table_type
        self._except_data = except_data

    def constraint_code(self, block: dbops.PLBlock) -> str | List[str]:
        if self._scope == 'row':
            if len(self._exprdata) == 1:
                expr = self._exprdata[0].exprdata.plain
            else:
                exprs = [e.exprdata.plain for e in self._exprdata]
                expr = '(' + ') AND ('.join(exprs) + ')'

            if self._except_data:
                cond = self._except_data.plain
                expr = f'({expr}) OR ({cond}) is true'

            return f'CHECK ({expr})'

        else:
            if self._type != 'unique':
                raise ValueError(
                    'unexpected constraint type: {}'.format(self._type))

            constr_exprs = []

            for exprdata in self._exprdata:
                if exprdata.is_trivial and not self._except_data:
                    # A constraint that contains one or more
                    # references to columns, and no expressions.
                    #
                    expr = ', '.join(exprdata.exprdata.plain_chunks)
                    expr = 'UNIQUE ({})'.format(expr)
                else:
                    # Complex constraint with arbitrary expressions
                    # needs to use EXCLUDE.
                    #
                    chunks = exprdata.exprdata.plain_chunks
                    expr = ', '.join(
                        "{} WITH =".format(chunk) for chunk in chunks)
                    expr = f'EXCLUDE ({expr})'
                    if self._except_data:
                        cond = self._except_data.plain
                        expr = f'{expr} WHERE (({cond}) is not true)'

                constr_exprs.append(expr)

            return constr_exprs

    def numbered_constraint_name(self, i, quote=True):
        raw_name = self.raw_constraint_name()
        name = common.edgedb_name_to_pg_name('{}#{}'.format(raw_name, i))
        return common.quote_ident(name) if quote else name

    def get_trigger_procname(self):
        return common.get_backend_name(
            self._schema, self._constraint, catenate=False, aspect='trigproc')

    def get_trigger_condition(self):
        chunks = []

        for expr in self._exprdata:
            condition = '{old_expr} IS DISTINCT FROM {new_expr}'.format(
                old_expr=expr.exprdata.old, new_expr=expr.exprdata.new
            )
            chunks.append(condition)

        if len(chunks) == 1:
            return chunks[0]
        else:
            return '(' + ') OR ('.join(chunks) + ')'

    def get_trigger_proc_text(self):
        chunks = []

        constr_name = self.constraint_name()
        raw_constr_name = self.constraint_name(quote=False)

        errmsg = 'duplicate key value violates unique ' \
                 'constraint {constr}'.format(constr=constr_name)

        for expr, origin_expr in zip(
            itertools.cycle(self._exprdata), self._origin_exprdata
        ):
            exprdata = expr.exprdata
            origin_exprdata = origin_expr.exprdata

            except_data = self._except_data
            origin_except_data = origin_expr.origin_except_data

            if self._except_data:
                except_part = f'''
                    AND ({origin_except_data.plain} is not true)
                    AND ({except_data.new} is not true)
                '''
            else:
                except_part = ''

            # Link tables get updated by deleting and then reinserting
            # rows, and so the trigger might fire even on rows that
            # did not *really* change. Check `source` also to prevent
            # spurious errors in those cases. (Anything with the same
            # source must have the same type, so any genuine constraint
            # errors this filters away will get caught by the *actual*
            # constraint.)
            # We *could* do a check for id on object tables, but it
            # isn't needed and would take at least some time.
            src_check = (
                ' AND source != NEW.source'
                if self._table_type == 'link' else ''
            )

            schemaname, tablename = origin_expr.origin_subject_db_name
            text = '''
                PERFORM
                    TRUE
                  FROM
                    {table}
                  WHERE
                    {plain_expr} = {new_expr}{except_part}{src_check};
                IF FOUND THEN
                  RAISE unique_violation
                      USING
                          TABLE = '{tablename}',
                          SCHEMA = '{schemaname}',
                          CONSTRAINT = '{constr}',
                          MESSAGE = '{errmsg}',
                          DETAIL = {detail};
                END IF;
            '''.format(
                plain_expr=origin_exprdata.plain,
                detail=common.quote_literal(
                    f"Key ({origin_exprdata.plain}) already exists."
                ),
                new_expr=exprdata.new,
                except_part=except_part,
                table=common.qname(
                    schemaname,
                    tablename + "_" + common.get_aspect_suffix("inhview")),
                schemaname=schemaname,
                tablename=tablename,
                constr=raw_constr_name,
                src_check=src_check,
                errmsg=errmsg,
            )

            chunks.append(text)

        text = 'BEGIN\n' + '\n\n'.join(chunks) + '\nRETURN NEW;\nEND;'

        return text

    def is_multiconstraint(self):
        """Determine if multiple database constraints are needed."""
        return self._scope != 'row' and len(self._exprdata) > 1

    def requires_triggers(self):
        subject = self._constraint.get_subject(self._schema)
        cname = self._constraint.get_shortname(self._schema)
        if (
            isinstance(subject, s_pointers.Pointer)
            and subject.is_id_pointer(self._schema)
            and cname == s_name.QualName('std', 'exclusive')
        ):
            return False
        else:
            return self._type != 'check'

    def can_disable_triggers(self):
        return self._constraint.is_independent(self._schema)

    def __repr__(self):
        return '<{}.{} {!r} at 0x{:x}>'.format(
            self.__class__.__module__, self.__class__.__name__,
            self.schema_constraint_name(), id(self))


class MultiConstraintItem:
    def __init__(self, constraint, index):
        self.constraint = constraint
        self.index = index

    def get_type(self):
        return self.constraint.get_type()

    def get_id(self):
        raw_name = self.constraint.raw_constraint_name()
        # XXX
        name = trampoline.versioned_name(common.edgedb_name_to_pg_name(
            '{}#{}'.format(raw_name, self.index)))
        name = common.quote_ident(name)

        return '{} ON {} {}'.format(
            name, self.constraint.get_subject_type(),
            self.constraint.get_subject_name())


class AlterTableAddMultiConstraint(dbops.AlterTableAddConstraint):
    def code(self, block: dbops.PLBlock) -> str:
        exprs = self.constraint.constraint_code(block)

        if isinstance(exprs, list) and len(exprs) > 1:
            chunks = []

            assert isinstance(self.constraint, SchemaConstraintTableConstraint)
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
            for i, _expr in enumerate(exprs):
                constraint = MultiConstraintItem(self.constraint, i)

                comment = dbops.Comment(constraint, constr_name)
                comments.append(comment)
        else:
            comment = dbops.Comment(self.constraint, constr_name)
            comments.append(comment)

        for comment in comments:
            comment.generate(block)


class AlterTableDropMultiConstraint(dbops.AlterTableDropConstraint):
    def code(self, block: dbops.PLBlock) -> str:
        exprs = self.constraint.constraint_code(block)

        if isinstance(exprs, list) and len(exprs) > 1:
            chunks = []

            for i, _expr in enumerate(exprs):
                name = self.constraint.numbered_constraint_name(i)
                chunk = f'DROP CONSTRAINT {name}'
                chunks.append(chunk)

            code = ', '.join(chunks)

        else:
            name = self.constraint.constraint_name()
            code = f'DROP CONSTRAINT {name}'

        return code


class AlterTableConstraintBase(dbops.AlterTableBaseMixin, dbops.CommandGroup):
    def __init__(
        self,
        name: Tuple[str, ...],
        *,
        constraint: SchemaConstraintTableConstraint,
        contained: bool = False,
        conditions: Optional[Set[str | dbops.Condition]] = None,
        neg_conditions: Optional[Set[str | dbops.Condition]] = None,
    ):
        dbops.CompositeCommandGroup.__init__(
            self, conditions=conditions, neg_conditions=neg_conditions
        )

        dbops.AlterTableBaseMixin.__init__(
            self, name=name, contained=contained)

        self._constraint = constraint

    def _get_triggers(
        self,
        table_name: Tuple[str, ...],
        constraint: SchemaConstraintTableConstraint,
        proc_name='null',
    ) -> Tuple[dbops.Trigger, ...]:
        cname = constraint.raw_constraint_name()

        ins_trigger_name = cname + '_instrigger'
        ins_trigger = dbops.Trigger(
            name=ins_trigger_name, table_name=table_name, events=('insert', ),
            procedure=proc_name, is_constraint=True, inherit=True)

        upd_trigger_name = cname + '_updtrigger'
        condition = constraint.get_trigger_condition()

        upd_trigger = dbops.Trigger(
            name=upd_trigger_name, table_name=table_name, events=('update', ),
            procedure=proc_name, condition=condition, is_constraint=True,
            inherit=True)

        return ins_trigger, upd_trigger

    def create_constr_trigger(
        self,
        table_name: Tuple[str, ...],
        constraint: SchemaConstraintTableConstraint,
        proc_name: str,
    ) -> List[dbops.CreateTrigger]:
        ins_trigger, upd_trigger = self._get_triggers(
            table_name, constraint, proc_name
        )

        return [
            dbops.CreateTrigger(ins_trigger),
            dbops.CreateTrigger(upd_trigger),
        ]

    def drop_constr_trigger(
        self,
        table_name: Tuple[str, ...],
        constraint: SchemaConstraintTableConstraint,
    ) -> List[dbops.DDLOperation]:
        ins_trigger, upd_trigger = self._get_triggers(table_name, constraint)

        return [dbops.DropTrigger(ins_trigger), dbops.DropTrigger(upd_trigger)]

    def enable_constr_trigger(
        self,
        table_name: Tuple[str, ...],
        constraint: SchemaConstraintTableConstraint,
    ) -> List[dbops.DDLOperation]:
        ins_trigger, upd_trigger = self._get_triggers(table_name, constraint)

        return [
            dbops.EnableTrigger(ins_trigger),
            dbops.EnableTrigger(upd_trigger),
        ]

    def disable_constr_trigger(
        self,
        table_name: Tuple[str, ...],
        constraint: SchemaConstraintTableConstraint,
    ) -> List[dbops.DDLOperation]:
        ins_trigger, upd_trigger = self._get_triggers(table_name, constraint)

        return [
            dbops.DisableTrigger(ins_trigger),
            dbops.DisableTrigger(upd_trigger),
        ]

    def create_constr_trigger_function(
        self, constraint: SchemaConstraintTableConstraint
    ):
        proc_name = constraint.get_trigger_procname()
        proc_text = constraint.get_trigger_proc_text()

        # Because of casting is not immutable in PG, this function may not be
        # immutable, only stable. But because we check that casing in edgeql
        # *is* immutable, we can (almost) safely assume that this function is
        # also immutable.
        func = dbops.Function(
            name=proc_name,
            text=proc_text,
            volatility='immutable',
            returns='trigger',
            language='plpgsql',
        )

        return [dbops.CreateFunction(func, or_replace=True)]

    def drop_constr_trigger_function(self, proc_name: Tuple[str, ...]):
        return [dbops.DropFunction(name=proc_name, args=())]

    def create_constraint(self, constraint: SchemaConstraintTableConstraint):
        # Add the constraint normally to our table
        #
        my_alter = dbops.AlterTable(self.name)
        add_constr = AlterTableAddMultiConstraint(constraint=constraint)
        my_alter.add_command(add_constr)

        self.add_command(my_alter)

        if constraint.requires_triggers():
            # Create trigger function
            self.add_commands(self.create_constr_trigger_function(constraint))

            proc_name = constraint.get_trigger_procname()
            cr_trigger = self.create_constr_trigger(
                self.name, constraint, proc_name)
            self.add_commands(cr_trigger)

            if constraint.can_disable_triggers():
                self.add_commands(
                    self.disable_constr_trigger(self.name, constraint))

    def alter_constraint(
        self,
        old_constraint: SchemaConstraintTableConstraint,
        new_constraint: SchemaConstraintTableConstraint,
    ):
        if old_constraint.delegated and not new_constraint.delegated:
            # No longer delegated, create db structures
            self.create_constraint(new_constraint)

        elif not old_constraint.delegated and new_constraint.delegated:
            # Now delegated, drop db structures
            self.drop_constraint(old_constraint)

        elif not new_constraint.delegated:
            # Some other modification, drop/create
            self.drop_constraint(old_constraint)
            self.create_constraint(new_constraint)

    def update_constraint_enabled(
        self, constraint: SchemaConstraintTableConstraint
    ):
        if constraint.requires_triggers():
            if constraint.can_disable_triggers():
                self.add_commands(
                    self.disable_constr_trigger(self.name, constraint))
            else:
                self.add_commands(
                    self.enable_constr_trigger(self.name, constraint))

    def drop_constraint(self, constraint: SchemaConstraintTableConstraint):
        if constraint.requires_triggers():
            self.add_commands(self.drop_constr_trigger(
                constraint._subject_name, constraint))
            proc_name = constraint.get_trigger_procname()
            self.add_commands(self.drop_constr_trigger_function(proc_name))

        # Drop the constraint normally from our table
        #
        my_alter = dbops.AlterTable(constraint._subject_name)

        drop_constr = AlterTableDropMultiConstraint(constraint=constraint)
        my_alter.add_command(drop_constr)

        self.add_command(my_alter)


class AlterTableAddConstraint(AlterTableConstraintBase):
    def __repr__(self):
        return '<{}.{} {!r}>'.format(
            self.__class__.__module__, self.__class__.__name__,
            self._constraint)

    def generate(self, block):
        if not self._constraint.delegated:
            self.create_constraint(self._constraint)
        super().generate(block)


class AlterTableAlterConstraint(AlterTableConstraintBase):
    def __init__(
        self, name, *, constraint, new_constraint, only_modify_enabled, **kwargs
    ):
        super().__init__(name, constraint=constraint, **kwargs)
        self._new_constraint = new_constraint
        self._only_modify_enabled = only_modify_enabled

    def __repr__(self):
        return '<{}.{} {!r}>'.format(
            self.__class__.__module__, self.__class__.__name__,
            self._constraint)

    def generate(self, block):
        if self._only_modify_enabled:
            self.update_constraint_enabled(self._new_constraint)
        else:
            self.alter_constraint(self._constraint, self._new_constraint)
        super().generate(block)


class AlterTableDropConstraint(AlterTableConstraintBase):
    def __repr__(self):
        return '<{}.{} {!r}>'.format(
            self.__class__.__module__, self.__class__.__name__,
            self._constraint)

    def generate(self, block):
        if not self._constraint.delegated:
            self.drop_constraint(self._constraint)
        super().generate(block)
