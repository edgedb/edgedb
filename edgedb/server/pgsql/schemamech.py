##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import re

from semantix import caos
from semantix.caos import proto
from semantix.utils import datastructures
from semantix.utils import markup
from semantix.utils.lang import yaml
from semantix.utils.lang.import_ import get_object

from .datasources import introspection
from . import astexpr
from . import dbops
from . import deltadbops
from . import common
from . import types
from . import parser


class ConstraintMech:

    atom_constraint_name_re = re.compile(r"""
        ^(?P<concept_name>[.\w]+):(?P<link_name>[.\w]+)::(?P<constraint_class>[.\w]+)::atom_constr$
    """, re.X)

    ptr_constraint_name_re = re.compile(r"""
        ^(?P<concept_name>[.\w]+):(?P<link_name>[.\w]+)::(?P<constraint_class>[.\w]+)::ptr_constr$
    """, re.X)


    def __init__(self):
        self.parser = parser.PgSQLParser()
        self.atom_constr_exprs = {}
        self._table_atom_constraints_cache = None
        self._table_ptr_constraints_cache = None

    def init_cache(self, connection):
        self.read_table_ptr_constraints(connection)
        self.read_table_atom_constraints(connection)

    def invalidate_meta_cache(self):
        self._table_atom_constraints_cache = None
        self._table_ptr_constraints_cache = None

    def read_table_constraints(self, connection, suffix, interpreter):
        constraints = {}
        index_by_pg_name = {}
        constraints_ds = introspection.tables.TableConstraints(connection)

        for row in constraints_ds.fetch(schema_pattern='caos%',
                                        constraint_pattern='%%::%s' % suffix):
            concept_constr = constraints[tuple(row['table_name'])] = {}

            for pg_name, (link_name, constraint) in interpreter(row):
                idx = datastructures.OrderedIndex(key=lambda i: i.get_canonical_class())
                ptr_constraints = concept_constr.setdefault(link_name, idx)
                cls = constraint.get_canonical_class()
                try:
                    existing_constraint = ptr_constraints[cls]
                    existing_constraint.merge(constraint)
                except KeyError:
                    ptr_constraints.add(constraint)
                index_by_pg_name[pg_name] = constraint, link_name, tuple(row['table_name'])

        return constraints, index_by_pg_name

    def interpret_atom_constraint(self, constraint_class, expr, name):

        try:
            expr_tree = self.parser.parse(expr)
        except parser.PgSQLParserError as e:
            msg = 'could not interpret constraint %s' % name
            details = 'Syntax error when parsing expression: %s' % e.args[0]
            raise caos.MetaError(msg, details=details) from e

        pattern = self.atom_constr_exprs.get(constraint_class)
        if not pattern:
            adapter = astexpr.AtomConstraintAdapterMeta.get_adapter(constraint_class)

            if not adapter:
                msg = 'could not interpret constraint %s' % name
                details = 'No matching pattern defined for constraint class "%s"' % constraint_class
                hint = 'Implement matching pattern for "%s"' % constraint_class
                hint += '\nExpression:\n{}'.format(markup.dumps(expr_tree))
                raise caos.MetaError(msg, details=details, hint=hint)

            pattern = adapter()
            self.atom_constr_exprs[constraint_class] = pattern

        constraint_data = pattern.match(expr_tree)

        if constraint_data is None:
            msg = 'could not interpret constraint {!r}'.format(str(name))
            details = 'Pattern "{!r}" could not match expression:\n{}'. \
                                        format(pattern.__class__, markup.dumps(expr_tree))
            hint = 'Take a look at the matching pattern and adjust'
            raise caos.MetaError(msg, details=details, hint=hint)

        return constraint_data

    def interpret_table_atom_constraint(self, name, expr):
        m = self.atom_constraint_name_re.match(name)
        if not m:
            raise caos.MetaError('could not interpret table constraint %s' % name)

        link_name = m.group('link_name')
        constraint_class = get_object(m.group('constraint_class'))
        constraint_data = self.interpret_atom_constraint(constraint_class, expr, name)

        return link_name, constraint_class(constraint_data)

    def interpret_table_atom_constraints(self, constr):
        cs = zip(constr['constraint_names'], constr['constraint_expressions'],
                 constr['constraint_descriptions'])

        for name, expr, description in cs:
            yield name, self.interpret_table_atom_constraint(description, expr)

    def read_table_atom_constraints(self, connection):
        if self._table_atom_constraints_cache is None:
            constraints, index = self.read_table_constraints(connection, 'atom_constr',
                                                             self.interpret_table_atom_constraints)
            self._table_atom_constraints_cache = (constraints, index)

        return self._table_atom_constraints_cache

    def get_table_atom_constraints(self, connection):
        return self.read_table_atom_constraints(connection)[0]

    def interpret_table_ptr_constraint(self, name, expr, columns):
        name_parts = name.split('::')

        try:
            if len(name_parts) < 3 or name_parts[2] != 'ptr_constr':
                raise ValueError

            constraint_class_name = name_parts[1]
            subject_parts = name_parts[0].rsplit(':', 1)
            if len(subject_parts) < 2:
                raise ValueError
            link_name = subject_parts[1]
        except ValueError:
            raise caos.MetaError('could not interpret table constraint %s' % name)

        constraint_class = get_object(constraint_class_name)

        if issubclass(constraint_class, proto.PointerConstraintUnique):
            col_name = common.caos_name_to_pg_name(link_name)
            if len(columns) != 1 or not col_name in columns:
                msg = 'internal metadata inconsistency'
                details = ('Link constraint "%s" expected to have exactly one column "%s" '
                           'in the expression, got: %s') % (name, col_name,
                                                            ','.join('"%s"' % c for c in columns))
                raise caos.MetaError(msg, details=details)

            constraint_data = {True}
        else:
            msg = 'internal metadata inconsistency'
            details = 'Link constraint "%s" has an unexpected class "%s"' % \
                      (name, constraint_class_name)
            raise caos.MetaError(msg, details=details)

        return link_name, constraint_class(constraint_data)

    def interpret_table_ptr_constraints(self, constr):
        cs = zip(constr['constraint_names'], constr['constraint_expressions'],
                 constr['constraint_descriptions'], constr['constraint_columns'])

        for name, expr, description, cols in cs:
            cols = cols.split('~~~~')
            yield name, self.interpret_table_ptr_constraint(description, expr, cols)

    def read_table_ptr_constraints(self, connection):
        if self._table_ptr_constraints_cache is None:
            constraints, index = self.read_table_constraints(connection, 'ptr_constr',
                                                             self.interpret_table_ptr_constraints)
            self._table_ptr_constraints_cache = (constraints, index)

        return self._table_ptr_constraints_cache

    def get_table_ptr_constraints(self, connection):
        return self.read_table_ptr_constraints(connection)[0]

    def constraint_from_pg_name(self, connection, pg_name):
        return self.read_table_ptr_constraints(connection)[1].get(pg_name)

    def unpack_constraints(self, meta, constraints):
        result = []
        if constraints:
            for cls, val in constraints.items():
                constraint = get_object(cls)(next(iter(yaml.Language.load(val))))
                result.append(constraint)
        return result

    def create_unique_constraint_trigger(self, source, pointer_name, constraint,
                                               constraint_origins, meta, context, priority=3):

        colname = common.quote_ident(common.caos_name_to_pg_name(pointer_name))
        if len(constraint_origins) == 1:
            origin = common.get_table_name(next(iter(constraint_origins)))
        else:
            origin = []
            for o in constraint_origins:
                origin.append('(SELECT * FROM %s)' % common.get_table_name(o))
            origin = ' UNION ALL '.join(origin)

        text = '''
                  BEGIN
                  PERFORM
                        TRUE
                      FROM %(origin)s
                      WHERE %(colname)s = NEW.%(colname)s;
                  IF FOUND THEN
                      RAISE unique_violation
                          USING
                              MESSAGE = 'duplicate key value violates unique constraint %(constr)s',
                              DETAIL = 'Key (%(colname)s)=(' || NEW.%(colname)s || ') already exists.';
                  END IF;
                  RETURN NEW;
                  END;
               ''' % {'colname': colname,
                      'origin': origin,
                      'constr': constraint.constraint_name()}

        schema = common.caos_module_name_to_schema_name(source.name.module)
        proc_name = constraint.raw_constraint_name() + '_trigproc'
        proc_name = schema, common.caos_name_to_pg_name(proc_name)
        table_name = common.get_table_name(source, catenate=False)
        proc = dbops.CreateTriggerFunction(name=proc_name, text=text, volatility='stable')

        trigger_name = common.caos_name_to_pg_name(constraint.raw_constraint_name() + '_instrigger')
        instrigger = dbops.CreateConstraintTrigger(trigger_name=trigger_name,
                                                   table_name=table_name,
                                                   events=('insert',), procedure=proc_name)

        trigger_name = common.caos_name_to_pg_name(constraint.raw_constraint_name() + '_updtrigger')
        condition = 'OLD.%(colname)s IS DISTINCT FROM NEW.%(colname)s' % {'colname': colname}
        updtrigger = dbops.CreateConstraintTrigger(trigger_name=trigger_name,
                                                   table_name=table_name,
                                                   events=('update',),
                                                   condition=condition, procedure=proc_name)

        result = dbops.CommandGroup(priority=priority,
                                    neg_conditions=[dbops.FunctionExists(name=proc_name)])
        result.add_command(proc)
        result.add_command(instrigger)
        result.add_command(updtrigger)

        return result

    def unique_constraint_trigger_exists(self, source, pointer_name, constraint):
        tabname = common.get_table_name(source, catenate=False)
        return self._unique_constraint_trigger_exists(source.name, tabname, pointer_name, constraint)

    def _unique_constraint_trigger_exists(self, source_name, source_table_name, pointer_name,
                                                                                constraint):
        schema = common.caos_module_name_to_schema_name(source_name.module)
        proc_name = constraint.raw_constraint_name() + '_trigproc'
        proc_name = schema, common.caos_name_to_pg_name(proc_name)
        func_exists = dbops.FunctionExists(proc_name)

        trigger_name = common.caos_name_to_pg_name(constraint.raw_constraint_name() + '_instrigger')
        ins_trig_exists = dbops.TriggerExists(trigger_name=trigger_name,
                                              table_name=source_table_name)

        trigger_name = common.caos_name_to_pg_name(constraint.raw_constraint_name() + '_updtrigger')
        upd_trig_exists = dbops.TriggerExists(trigger_name=trigger_name,
                                              table_name=source_table_name)

        return [func_exists, ins_trig_exists, upd_trig_exists]

    def drop_unique_constraint_trigger(self, source, pointer_name, constraint, meta, context):
        schema = common.caos_module_name_to_schema_name(source.name.module)
        table_name = common.get_table_name(source, catenate=False)

        result = dbops.CommandGroup()

        trigger_name = common.caos_name_to_pg_name(constraint.raw_constraint_name() + '_instrigger')
        result.add_command(dbops.DropTrigger(trigger_name=trigger_name, table_name=table_name))
        trigger_name = common.caos_name_to_pg_name(constraint.raw_constraint_name() + '_updtrigger')
        result.add_command(dbops.DropTrigger(trigger_name=trigger_name, table_name=table_name))

        proc_name = constraint.raw_constraint_name() + '_trigproc'
        proc_name = schema, common.caos_name_to_pg_name(proc_name)
        result.add_command(dbops.DropFunction(name=proc_name, args=()))

        return result

    def rename_unique_constraint_trigger(self, orig_source, source, pointer_name,
                                               old_constraint, new_constraint, meta, context):

        result = dbops.CommandGroup()

        table_name = common.get_table_name(source, catenate=False)
        orig_table_name = common.get_table_name(orig_source, catenate=False)

        old_trigger_name = common.caos_name_to_pg_name('%s_instrigger' % \
                                                       old_constraint.raw_constraint_name())
        new_trigger_name = common.caos_name_to_pg_name('%s_instrigger' % \
                                                       new_constraint.raw_constraint_name())

        result.add_command(dbops.AlterTriggerRenameTo(trigger_name=old_trigger_name,
                                                      new_trigger_name=new_trigger_name,
                                                      table_name=table_name))

        old_trigger_name = common.caos_name_to_pg_name('%s_updtrigger' % \
                                                       old_constraint.raw_constraint_name())
        new_trigger_name = common.caos_name_to_pg_name('%s_updtrigger' % \
                                                       new_constraint.raw_constraint_name())

        result.add_command(dbops.AlterTriggerRenameTo(trigger_name=old_trigger_name,
                                                      new_trigger_name=new_trigger_name,
                                                      table_name=table_name))

        old_proc_name = common.caos_name_to_pg_name('%s_trigproc' % \
                                                    old_constraint.raw_constraint_name())
        old_proc_name = orig_table_name[0], old_proc_name


        new_proc_name = common.caos_name_to_pg_name('%s_trigproc' % \
                                                    new_constraint.raw_constraint_name())
        new_proc_name = table_name[0], new_proc_name

        result.add_command(dbops.RenameFunction(name=old_proc_name, args=(), new_name=new_proc_name))
        old = common.get_table_name(orig_source)
        new = common.get_table_name(source)
        result.add_command(dbops.AlterFunctionReplaceText(name=new_proc_name, args=(), old_text=old,
                                                          new_text=new))

        return result

    @classmethod
    def schema_constraint_to_backend_constraint(cls, constraint, source, pointer_name):
        source_table_name = common.get_table_name(source, catenate=False)
        return cls._schema_constraint_to_backend_constraint(constraint, source.name,
                                                            source_table_name, pointer_name)

    @classmethod
    def _schema_constraint_to_backend_constraint(cls, constraint, source_name, source_table_name,
                                                                               pointer_name):
        column_name = common.caos_name_to_pg_name(pointer_name)
        prefix = (source_name, pointer_name)

        if isinstance(constraint, proto.AtomConstraint):
            constraint = deltadbops.AtomConstraintTableConstraint(table_name=source_table_name,
                                                                  column_name=column_name,
                                                                  prefix=prefix,
                                                                  constraint=constraint)
        else:
            constraint = deltadbops.PointerConstraintTableConstraint(table_name=source_table_name,
                                                                     column_name=column_name,
                                                                     prefix=prefix,
                                                                     constraint=constraint)

        return constraint


class TypeMech:
    def __init__(self):
        self._column_cache = None
        self._table_cache = None

    def invalidate_meta_cache(self):
        self._column_cache = None
        self._column_cache = None

    def init_cache(self, connection):
        self._load_table_columns(('caos_%', None), connection)

    def _load_table_columns(self, table_name, connection):
        cols = introspection.tables.TableColumns(connection)
        cols = cols.fetch(table_name=table_name[1], schema_name=table_name[0])

        if self._column_cache is None:
            self._column_cache = {}

        for col in cols:
            try:
                table_cols = self._column_cache[(col['table_schema'], col['table_name'])]
            except KeyError:
                table_cols = collections.OrderedDict()
                self._column_cache[(col['table_schema'], col['table_name'])] = table_cols

            table_cols[col['column_name']] = col

    def get_table_columns(self, table_name, connection, cache='auto'):
        if cache is not None and self._column_cache is not None:
            cols = self._column_cache.get(table_name)
        else:
            cols = None

        if cols is None and cache != 'always':
            cols = self._load_table_columns(table_name, connection)

        return self._column_cache.get(table_name)

    def _load_type_attributes(self, type_name, connection):
        cols = introspection.types.CompositeTypeAttributes(connection)
        cols = cols.fetch(type_name=type_name[1], schema_name=type_name[0])

        if self._column_cache is None:
            self._column_cache = {}

        for col in cols:
            try:
                type_attrs = self._column_cache[(col['type_schema'], col['type_name'])]
            except KeyError:
                type_attrs = collections.OrderedDict()
                self._column_cache[(col['type_schema'], col['type_name'])] = type_attrs

            type_attrs[col['attribute_name']] = col

    def get_type_attributes(self, type_name, connection, cache='auto'):
        if cache is not None and self._column_cache is not None:
            cols = self._column_cache.get(type_name)
        else:
            cols = None

        if cols is None and cache != 'always':
            self._load_type_attributes(type_name, connection)

        return self._column_cache.get(type_name)

    def get_table(self, prototype, proto_schema):
        if self._table_cache is None:
            self._table_cache = {}

        table = self._table_cache.get(prototype)

        if table is None:
            table_name = common.get_table_name(prototype, catenate=False)
            table = dbops.Table(table_name)

            cols = []

            if isinstance(prototype, caos.types.ProtoLink):
                cols.extend([
                    dbops.Column(name='link_type_id', type='int'),
                    dbops.Column(name='semantix.caos.builtins.source', type='uuid'),
                    dbops.Column(name='semantix.caos.builtins.target', type='uuid')
                ])

            elif isinstance(prototype, caos.types.ProtoConcept):
                cols.extend([
                    dbops.Column(name='concept_id', type='int')
                ])

            else:
                assert False

            for pointer_name, pointer in prototype.pointers.items():
                if isinstance(pointer, caos.types.ProtoComputable) or not pointer.singular():
                    continue

                if pointer_name == 'semantix.caos.builtins.source':
                    continue

                ptr_stor_info = types.get_pointer_storage_info(proto_schema, pointer)

                if ptr_stor_info.column_name == 'semantix.caos.builtins.target':
                    continue

                if ptr_stor_info.table_type[0] == 'source':
                    cols.append(dbops.Column(name=ptr_stor_info.column_name,
                                             type=ptr_stor_info.column_type))
            table.add_columns(cols)

            self._table_cache[prototype] = table

        return table
