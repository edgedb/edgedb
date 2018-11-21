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


import collections.abc
import itertools
import json
import pickle
import textwrap
import typing

from edb.lang.edgeql import ast as ql_ast
from edb.lang.edgeql import compiler as ql_compiler
from edb.lang.edgeql import errors as ql_errors
from edb.lang.edgeql import functypes as ql_ft

from edb.lang.schema import attributes as s_attrs
from edb.lang.schema import scalars as s_scalars
from edb.lang.schema import objtypes as s_objtypes
from edb.lang.schema import constraints as s_constr
from edb.lang.schema import database as s_db
from edb.lang.schema import delta as sd
from edb.lang.schema import expr as s_expr
from edb.lang.schema import functions as s_funcs
from edb.lang.schema import indexes as s_indexes
from edb.lang.schema import links as s_links
from edb.lang.schema import lproperties as s_props
from edb.lang.schema import modules as s_mod
from edb.lang.schema import name as sn
from edb.lang.schema import named as s_named
from edb.lang.schema import objects as s_obj
from edb.lang.schema import operators as s_opers
from edb.lang.schema import referencing as s_referencing
from edb.lang.schema import sources as s_sources
from edb.lang.schema import types as s_types

from edb.lang.common import ordered
from edb.lang.common import markup

from edb.lang.ir import utils as irutils

from edb.server.pgsql import common

from edb.server.pgsql import dbops, deltadbops, metaschema

from . import ast as pg_ast
from .common import quote_literal as ql
from . import compiler
from . import codegen
from . import datasources
from . import schemamech
from . import types


BACKEND_FORMAT_VERSION = 30


class CommandMeta(sd.CommandMeta):
    pass


class ObjectCommandMeta(sd.ObjectCommandMeta, CommandMeta):
    _transparent_adapter_subclass = True


class ReferencedObjectCommandMeta(
        s_referencing.ReferencedObjectCommandMeta, ObjectCommandMeta):
    _transparent_adapter_subclass = True


class MetaCommand(sd.Command, metaclass=CommandMeta):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pgops = ordered.OrderedSet()

    def apply(self, schema, context=None):
        for op in self.ops:
            self.pgops.add(op)
        return schema, None

    def generate(self, block: dbops.PLBlock) -> None:
        for op in sorted(
                self.pgops, key=lambda i: getattr(i, 'priority', 0),
                reverse=True):
            op.generate(block)

    @classmethod
    def as_markup(cls, self, *, ctx):
        node = markup.elements.lang.TreeNode(name=repr(self))

        for op in self.pgops:
            node.add_child(node=markup.serialize(op, ctx=ctx))

        return node


class CommandGroupAdapted(MetaCommand, adapts=sd.CommandGroup):
    def apply(self, schema, context):
        schema, _ = sd.CommandGroup.apply(self, schema, context)
        schema, _ = MetaCommand.apply(self, schema, context)
        return schema, None


class ObjectMetaCommand(MetaCommand, sd.ObjectCommand,
                        metaclass=ObjectCommandMeta):
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


class NamedObjectMetaCommand(
        ObjectMetaCommand, s_named.NamedObjectCommand):
    op_priority = 0

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._type_mech = schemamech.TypeMech()

    def get_table(self, schema):
        raise NotImplementedError

    def _get_name(self, schema, value):
        if isinstance(value, s_obj.ObjectRef):
            name = value.get_name(schema)
        elif isinstance(value, s_named.NamedObject):
            name = value.get_name(schema)
        else:
            raise ValueError(
                'expecting a ObjectRef or a '
                'NamedObject, got {!r}'.format(value))

        return name

    def _serialize_field(self, schema, value, col, *, use_defaults=False):
        recvalue = None
        result = value

        if isinstance(value, (s_obj.ObjectSet, s_obj.ObjectList)):
            result = tuple(self._get_name(schema, v)
                           for v in value.objects(schema))
            name_array = ', '.join(ql(n) for n in result)
            recvalue = dbops.Query(
                f'edgedb._resolve_type_id(ARRAY[{name_array}]::text[])')

        elif isinstance(value, s_obj.ObjectIndexBase):
            result = s_types.Tuple.from_subtypes(
                schema,
                dict(value.items(schema)),
                {'named': True})
            recvalue = types.TypeDesc.from_type(schema, result)

        elif isinstance(value, s_obj.ObjectCollection):
            result = s_types.Tuple.from_subtypes(schema, value)
            recvalue = types.TypeDesc.from_type(schema, result)

        elif isinstance(value, s_obj.Object):
            recvalue = types.TypeDesc.from_type(schema, value)

        elif isinstance(value, sn.SchemaName):
            recvalue = str(value)

        elif isinstance(value, collections.abc.Mapping):
            # Other dicts are JSON'ed by default
            recvalue = json.dumps(dict(value))

        if recvalue is None:
            if result is None and use_defaults:
                recvalue = dbops.Default
            else:
                recvalue = result
        elif isinstance(recvalue, types.TypeDesc):
            recvalue = dbops.Query(
                'edgedb._encode_type({type_desc})'.format(
                    type_desc=recvalue.to_sql_expr())
            )

        return result, recvalue

    def get_fields(self, schema):
        if isinstance(self, sd.CreateObject):
            fields = dict(self.scls.get_fields_values(schema))
        else:
            fields = self.get_struct_properties(schema)

        return fields

    def fill_record(self, schema, *, use_defaults=False):
        updates = {}

        rec = None
        table = self.get_table(schema)

        fields = self.get_fields(schema)

        for name, value in fields.items():
            col = table.get_column(name)

            v1, refqry = self._serialize_field(
                schema, value, col, use_defaults=use_defaults)

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
        rec, updates = self.fill_record(schema, use_defaults=True)
        self.pgops.add(
            dbops.Insert(
                table=self.get_table(schema),
                records=[rec],
                priority=self.op_priority))
        return updates

    def update(self, schema, context):
        updaterec, updates = self.fill_record(schema)

        if updaterec:
            condition = [('name', str(self.scls.get_name(schema)))]
            self.pgops.add(
                dbops.Update(
                    table=self.get_table(schema),
                    record=updaterec,
                    condition=condition,
                    priority=self.op_priority))

        return updates

    def rename(self, schema, context, old_name, new_name):
        table = self.get_table(schema)
        updaterec = table.record(name=str(new_name))
        condition = [('name', str(old_name))]
        self.pgops.add(
            dbops.Update(
                table=table, record=updaterec, condition=condition))

    def delete(self, schema, context, scls):
        table = self.get_table(schema)
        self.pgops.add(
            dbops.Delete(
                table=table, condition=[('name', str(scls.get_name(schema)))]))


class CreateNamedObject(NamedObjectMetaCommand):
    def apply(self, schema, context):
        schema, obj = self.__class__.get_adaptee().apply(self, schema, context)
        schema, _ = NamedObjectMetaCommand.apply(self, schema, context)
        updates = self.create_object(schema, obj)
        self.updates = updates
        return schema, obj


class CreateOrAlterNamedObject(NamedObjectMetaCommand):
    def apply(self, schema, context):
        existing = schema.get(self.classname, None)

        schema, self.scls = self.__class__.get_adaptee().apply(
            self, schema, context)
        schema, _ = NamedObjectMetaCommand.apply(self, schema, context)

        if existing is None:
            updates = self.create_object(schema, self.scls)
            self.updates = updates
        else:
            self.updates = self.update(schema, context)
        return schema, self.scls


class RenameNamedObject(NamedObjectMetaCommand):
    def apply(self, schema, context):
        schema, obj = self.__class__.get_adaptee().apply(self, schema, context)
        schema, _ = NamedObjectMetaCommand.apply(self, schema, context)
        self.rename(schema, context, self.classname, self.new_name)
        return schema, obj


class RebaseNamedObject(NamedObjectMetaCommand):
    def apply(self, schema, context):
        schema, obj = self.__class__.get_adaptee().apply(self, schema, context)
        schema, _ = NamedObjectMetaCommand.apply(self, schema, context)
        return schema, obj


class AlterNamedObject(NamedObjectMetaCommand):
    def apply(self, schema, context):
        schema, _ = NamedObjectMetaCommand.apply(self, schema, context)
        schema, self.scls = self.__class__.get_adaptee().apply(
            self, schema, context)
        self.updates = self.update(schema, context)
        return schema, self.scls


class DeleteNamedObject(NamedObjectMetaCommand):
    def apply(self, schema, context):
        schema, obj = self.__class__.get_adaptee().apply(self, schema, context)
        schema, _ = NamedObjectMetaCommand.apply(self, schema, context)
        self.delete(schema, context, obj)
        return schema, obj


class AlterObjectProperty(MetaCommand, adapts=sd.AlterObjectProperty):
    pass


class ParameterCommand:
    _table = metaschema.get_metaclass_table(s_funcs.Parameter)

    def get_table(self, schema):
        return self._table


class CreateParameter(ParameterCommand, CreateNamedObject,
                      adapts=s_funcs.CreateParameter):

    pass


class DeleteParameter(ParameterCommand, DeleteNamedObject,
                      adapts=s_funcs.DeleteParameter):

    pass


class FunctionCommand:
    _table = metaschema.get_metaclass_table(s_funcs.Function)

    def get_table(self, schema):
        return self._table

    def get_pgname(self, func: s_funcs.Function, schema):
        func_sn = func.get_shortname(schema)
        return (
            common.edgedb_module_name_to_schema_name(func_sn.module),
            common.edgedb_name_to_pg_name(func_sn.name)
        )

    def get_pgtype(self, func: s_funcs.Function, obj, schema):
        if obj.is_any():
            return ('anyelement',)

        try:
            return types.pg_type_from_object(schema, obj)
        except ValueError:
            raise ql_errors.EdgeQLError(
                f'could not compile parameter type {obj!r} '
                f'of function {func.get_shortname(schema)}',
                context=self.source_context) from None

    def compile_default(self, func: s_funcs.Function, default: str, schema):
        try:
            ir = ql_compiler.compile_fragment_to_ir(
                default, schema, location='parameter-default')

            if not irutils.is_const(ir):
                raise ValueError('expression not constant')

            sql_tree = compiler.compile_ir_to_sql_tree(
                ir, schema=schema, singleton_mode=True)
            return codegen.SQLSourceGenerator.to_source(sql_tree)

        except Exception as ex:
            raise ql_errors.EdgeQLError(
                f'could not compile default expression {default!r} '
                f'of function {func.get_shortname(schema)}: {ex}',
                context=self.source_context) from ex

    def compile_args(self, func: s_funcs.Function, schema):
        func_params = func.get_params(schema)
        pg_params = s_funcs.PgParams.from_params(schema, func_params)
        has_inlined_defaults = func.has_inlined_defaults(schema)

        args = []
        if has_inlined_defaults:
            args.append(('__defaults_mask__', ('bytea',), None))

        compile_defaults = not (
            has_inlined_defaults or func_params.find_named_only(schema)
        )

        for param in pg_params.params:
            param_type = param.get_type(schema)
            param_default = param.get_default(schema)

            pg_at = self.get_pgtype(func, param_type, schema)

            default = None
            if compile_defaults and param_default is not None:
                default = self.compile_default(func, param_default, schema)

            args.append((param.get_shortname(schema), pg_at, default))

        return args


class CreateFunction(FunctionCommand, CreateNamedObject,
                     adapts=s_funcs.CreateFunction):

    def make_function(self, func: s_funcs.Function, code, schema):
        func_return_typemod = func.get_return_typemod(schema)
        func_params = func.get_params(schema)
        return dbops.Function(
            name=self.get_pgname(func, schema),
            args=self.compile_args(func, schema),
            has_variadic=func_params.find_variadic(schema) is not None,
            set_returning=func_return_typemod is ql_ft.TypeModifier.SET_OF,
            returns=self.get_pgtype(
                func, func.get_return_type(schema), schema),
            text=code)

    def compile_sql_function(self, func: s_funcs.Function, schema):
        return self.make_function(func, func.get_code(schema), schema)

    def compile_edgeql_function(self, func: s_funcs.Function, schema):
        body_ir = ql_compiler.compile_func_to_ir(func, schema)

        sql_text, _ = compiler.compile_ir_to_sql(
            body_ir,
            schema=schema,
            ignore_shapes=True,
            use_named_params=True)

        return self.make_function(func, sql_text, schema)

    def apply(self, schema, context):
        schema, func = super().apply(schema, context)

        if func.get_code(schema) is None:
            return schema, func

        func_language = func.get_language(schema)

        if func_language is ql_ast.Language.SQL:
            dbf = self.compile_sql_function(func, schema)
        elif func_language is ql_ast.Language.EdgeQL:
            dbf = self.compile_edgeql_function(func, schema)
        else:
            raise ql_errors.EdgeQLError(
                f'cannot compile function {func.get_shortname(schema)}: '
                f'unsupported language {func_language}',
                context=self.source_context)

        op = dbops.CreateFunction(dbf)
        self.pgops.add(op)
        return schema, func


class RenameFunction(
        FunctionCommand, RenameNamedObject, adapts=s_funcs.RenameFunction):
    pass


class AlterFunction(
        FunctionCommand, AlterNamedObject, adapts=s_funcs.AlterFunction):
    pass


class DeleteFunction(
        FunctionCommand, DeleteNamedObject, adapts=s_funcs.DeleteFunction):

    def apply(self, schema, context):
        schema, func = super().apply(schema, context)

        if func.get_code(schema):
            # An EdgeQL or a SQL function
            # (not just an alias to a SQL function).

            variadic = func.get_params(schema).find_variadic(schema)
            self.pgops.add(
                dbops.DropFunction(
                    name=self.get_pgname(func, schema),
                    args=self.compile_args(func, schema),
                    has_variadic=variadic is not None,
                )
            )

        return schema, func


class OperatorCommand:
    _table = metaschema.get_metaclass_table(s_opers.Operator)

    def get_table(self, schema):
        return self._table

    def get_pg_name(self, schema,
                    oper: s_opers.Operator) -> typing.Tuple[str, str]:
        return common.get_backend_operator_name(oper.get_shortname(schema))

    def get_pg_operands(self, schema, oper: s_opers.Operator):
        left_type = None
        right_type = None
        oper_params = list(oper.get_params(schema).objects(schema))
        oper_kind = oper.get_operator_kind(schema)

        if oper_kind is ql_ft.OperatorKind.INFIX:
            left_type = types.pg_type_from_object(
                schema, oper_params[0].get_type(schema))

            right_type = types.pg_type_from_object(
                schema, oper_params[1].get_type(schema))

        elif oper_kind is ql_ft.OperatorKind.PREFIX:
            right_type = types.pg_type_from_object(
                schema, oper_params[0].get_type(schema))

        elif oper_kind is ql_ft.OperatorKind.POSTFIX:
            left_type = types.pg_type_from_object(
                schema, oper_params[0].get_type(schema))

        else:
            raise RuntimeError(
                f'unexpected operator type: {oper.get_type(schema)!r}')

        return left_type, right_type


class CreateOperator(OperatorCommand, CreateNamedObject,
                     adapts=s_opers.CreateOperator):

    def apply(self, schema, context):
        schema, oper = super().apply(schema, context)
        oper_language = oper.get_language(schema)
        oper_fromop = oper.get_from_operator(schema)

        if oper_language is ql_ast.Language.SQL and oper_fromop:
            args = self.get_pg_operands(schema, oper)
            self.pgops.add(dbops.CreateOperatorAlias(
                name=self.get_pg_name(schema, oper),
                args=args,
                operator=('pg_catalog', oper_fromop)
            ))
        else:
            raise ql_errors.EdgeQLError(
                f'cannot create operator {oper.get_shortname(schema)}: '
                f'only "FROM SQL OPERATOR" operators are currently supported',
                context=self.source_context)

        return schema, oper


class RenameOperator(
        OperatorCommand, RenameNamedObject, adapts=s_opers.RenameOperator):
    pass


class AlterOperator(
        OperatorCommand, AlterNamedObject, adapts=s_opers.AlterOperator):
    pass


class DeleteOperator(
        OperatorCommand, DeleteNamedObject, adapts=s_opers.DeleteOperator):

    def apply(self, schema, context):
        schema, oper = super().apply(schema, context)
        name = self.get_pg_name(schema, oper)
        args = self.get_pg_operands(schema, oper)
        self.pgops.add(dbops.DropOperator(name=name, args=args))
        return schema, oper


class AttributeCommand:
    _table = metaschema.get_metaclass_table(s_attrs.Attribute)

    def get_table(self, schema):
        return self._table


class CreateAttribute(
        AttributeCommand, CreateNamedObject,
        adapts=s_attrs.CreateAttribute):
    op_priority = 1


class AlterAttribute(
        AttributeCommand, AlterNamedObject, adapts=s_attrs.AlterAttribute):
    pass


class DeleteAttribute(
        AttributeCommand, DeleteNamedObject,
        adapts=s_attrs.DeleteAttribute):
    pass


class AttributeValueCommand(sd.ObjectCommand,
                            metaclass=ReferencedObjectCommandMeta):
    _table = metaschema.get_metaclass_table(s_attrs.AttributeValue)
    op_priority = 1

    def get_table(self, schema):
        return self._table

    def fill_record(self, schema, *, use_defaults=False):
        rec, updates = super().fill_record(schema, use_defaults=use_defaults)

        if rec:
            value = updates.get('value')
            if value:
                rec.value = pickle.dumps(value)

        return rec, updates


class CreateAttributeValue(
        AttributeValueCommand, CreateOrAlterNamedObject,
        adapts=s_attrs.CreateAttributeValue):
    pass


class AlterAttributeValue(
        AttributeValueCommand, AlterNamedObject,
        adapts=s_attrs.AlterAttributeValue):
    pass


class DeleteAttributeValue(
        AttributeValueCommand, DeleteNamedObject,
        adapts=s_attrs.DeleteAttributeValue):
    pass


class ConstraintCommand(sd.ObjectCommand,
                        metaclass=ReferencedObjectCommandMeta):
    _table = metaschema.get_metaclass_table(s_constr.Constraint)
    op_priority = 3

    def get_table(self, schema):
        return self._table


class CreateConstraint(
        ConstraintCommand, CreateNamedObject,
        adapts=s_constr.CreateConstraint):
    def apply(self, schema, context):
        schema, constraint = super().apply(schema, context)

        subject = constraint.get_subject(schema)

        if subject is not None:
            schemac_to_backendc = \
                schemamech.ConstraintMech.\
                schema_constraint_to_backend_constraint
            bconstr = schemac_to_backendc(subject, constraint, schema)

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.create_ops())
            self.pgops.add(op)

        return schema, constraint


class RenameConstraint(
        ConstraintCommand, RenameNamedObject,
        adapts=s_constr.RenameConstraint):
    def apply(self, schema, context):
        constr_ctx = context.get(s_constr.ConstraintCommandContext)
        assert constr_ctx
        orig_constraint = constr_ctx.original_class
        schemac_to_backendc = \
            schemamech.ConstraintMech.schema_constraint_to_backend_constraint
        orig_bconstr = schemac_to_backendc(
            orig_constraint.get_subject(schema), orig_constraint, schema)

        schema, constraint = super().apply(schema, context)

        subject = constraint.get_subject(schema)

        if subject is not None:
            bconstr = schemac_to_backendc(subject, constraint, schema)

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.rename_ops(orig_bconstr))
            self.pgops.add(op)

        return schema, constraint


class AlterConstraint(
        ConstraintCommand, AlterNamedObject,
        adapts=s_constr.AlterConstraint):
    def _alter_finalize(self, schema, context, constraint):
        schema = super()._alter_finalize(schema, context, constraint)

        subject = constraint.get_subject(schema)
        ctx = context.get(s_constr.ConstraintCommandContext)

        if subject is not None:
            schemac_to_backendc = \
                schemamech.ConstraintMech.\
                schema_constraint_to_backend_constraint

            bconstr = schemac_to_backendc(subject, constraint, schema)

            orig_constraint = ctx.original_class
            orig_bconstr = schemac_to_backendc(
                orig_constraint.get_subject(schema), orig_constraint, schema)

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.alter_ops(orig_bconstr))
            self.pgops.add(op)

        return schema


class DeleteConstraint(
        ConstraintCommand, DeleteNamedObject,
        adapts=s_constr.DeleteConstraint):
    def apply(self, schema, context):
        schema, constraint = super().apply(schema, context)

        subject = constraint.get_subject(schema)

        if subject is not None:
            schemac_to_backendc = \
                schemamech.ConstraintMech.\
                schema_constraint_to_backend_constraint
            bconstr = schemac_to_backendc(subject, constraint, schema)

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.delete_ops())
            self.pgops.add(op)

        return schema, constraint


class ViewCapableObjectMetaCommand(NamedObjectMetaCommand):
    def fill_record(self, schema, *, use_defaults=False):
        rec, updates = super().fill_record(schema, use_defaults=use_defaults)
        if rec and False:
            expr = updates.get('expr')
            if expr:
                self.pgops.add(
                    deltadbops.MangleExprObjectRefs(
                        scls=self.scls, field='expr', expr=expr, priority=3,
                        schema=schema))

        return rec, updates


class ScalarTypeMetaCommand(ViewCapableObjectMetaCommand):
    _table = metaschema.get_metaclass_table(s_scalars.ScalarType)

    def get_table(self, schema):
        return self._table

    def is_sequence(self, schema, scalar):
        seq = schema.get('std::sequence', default=None)
        return seq is not None and scalar.issubclass(schema, seq)

    def fill_record(self, schema, *, use_defaults=False):
        table = self.get_table(schema)
        rec, updates = super().fill_record(schema, use_defaults=use_defaults)
        default = updates.get('default')
        if default:
            if not rec:
                rec = table.record()
            rec.default = self.pack_default(default)

        return rec, updates

    def alter_scalar_type(self, scalar, schema, new_type, intent):

        users = []

        for link in schema.get_objects(type='link'):
            if (link.get_target(schema) and
                    link.get_target(schema).get_name(schema) ==
                    scalar.get_name(schema)):
                users.append((link.get_source(schema), link))

        domain_name = common.scalar_name_to_domain_name(
            scalar.get_name(schema), catenate=False)

        new_constraints = scalar.get_own_constraints(schema)
        base = types.get_scalar_base(schema, scalar)

        target_type = new_type

        schemac_to_backendc = \
            schemamech.ConstraintMech.\
            schema_constraint_to_backend_constraint

        if intent == 'alter':
            new_name = domain_name[0], domain_name[1] + '_tmp'
            self.pgops.add(dbops.RenameDomain(domain_name, new_name))
            target_type = domain_name

            self.pgops.add(dbops.CreateDomain(
                name=domain_name, base=new_type))

            for constraint in new_constraints.objects(schema):
                bconstr = schemac_to_backendc(scalar, constraint, schema)
                op = dbops.CommandGroup(priority=1)
                op.add_command(bconstr.create_ops())
                self.pgops.add(op)

            domain_name = new_name

        elif intent == 'create':
            self.pgops.add(dbops.CreateDomain(name=domain_name, base=base))

        for host_class, item_class in users:
            if isinstance(item_class, s_links.Link):
                name = item_class.get_shortname(schema)
            else:
                name = item_class.get_name(schema)

            table_name = common.get_table_name(
                schema, host_class, catenate=False)
            column_name = common.edgedb_name_to_pg_name(name)

            alter_type = dbops.AlterTableAlterColumnType(
                column_name, target_type)
            alter_table = dbops.AlterTable(table_name)
            alter_table.add_operation(alter_type)
            self.pgops.add(alter_table)

        for child_scalar in schema.get_objects(type='ScalarType'):
            bases = child_scalar.get_bases(schema).objects(schema)
            scalar_name = scalar.get_name(schema)
            if [b.get_name(schema) for b in bases] == [scalar_name]:
                self.alter_scalar_type(
                    child_scalar, schema, target_type, 'alter')

        if intent == 'drop':
            self.pgops.add(dbops.DropDomain(domain_name))


class CreateScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.CreateScalarType):
    def apply(self, schema, context=None):
        schema, scalar = s_scalars.CreateScalarType.apply(
            self, schema, context)

        schema, _ = ScalarTypeMetaCommand.apply(self, schema, context)

        updates = self.create_object(schema, scalar)

        if scalar.get_is_abstract(schema):
            return schema, scalar

        new_domain_name = common.scalar_name_to_domain_name(
            scalar.get_name(schema), catenate=False)
        base = types.get_scalar_base(schema, scalar)

        self.pgops.add(dbops.CreateDomain(name=new_domain_name, base=base))

        if self.is_sequence(schema, scalar):
            seq_name = common.scalar_name_to_sequence_name(
                scalar.get_name(schema), catenate=False)
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

        return schema, scalar


class RenameScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.RenameScalarType):
    def apply(self, schema, context=None):
        schema, scls = s_scalars.RenameScalarType.apply(self, schema, context)
        schema, _ = ScalarTypeMetaCommand.apply(self, schema, context)

        domain_name = common.scalar_name_to_domain_name(
            self.classname, catenate=False)
        new_domain_name = common.scalar_name_to_domain_name(
            self.new_name, catenate=False)

        self.pgops.add(
            dbops.RenameDomain(name=domain_name, new_name=new_domain_name))
        self.rename(schema, context, self.classname, self.new_name)

        if self.is_sequence(schema, scls):
            seq_name = common.scalar_name_to_sequence_name(
                self.classname, catenate=False)
            new_seq_name = common.scalar_name_to_sequence_name(
                self.new_name, catenate=False)

            self.pgops.add(
                dbops.RenameSequence(name=seq_name, new_name=new_seq_name))

        return schema, scls


class RebaseScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.RebaseScalarType):
    # Rebase is taken care of in AlterScalarType
    pass


class AlterScalarType(ScalarTypeMetaCommand, adapts=s_scalars.AlterScalarType):
    def apply(self, schema, context=None):
        table = self.get_table(schema)
        schema, old_scalar = schema.get(self.classname).temp_copy(schema)
        schema, new_scalar = s_scalars.AlterScalarType.apply(
            self, schema, context)
        schema, _ = ScalarTypeMetaCommand.apply(self, schema, context)

        updaterec, updates = self.fill_record(schema)

        if updaterec:
            condition = [('name', str(new_scalar.get_name(schema)))]
            self.pgops.add(
                dbops.Update(
                    table=table, record=updaterec, condition=condition))

        self.alter_scalar(
            self, schema, context, old_scalar, new_scalar, updates=updates)

        return schema, new_scalar

    @classmethod
    def alter_scalar(
            cls, op, schema, context, old_scalar, new_scalar, in_place=True,
            updates=None):

        old_base = types.get_scalar_base(schema, old_scalar)
        base = types.get_scalar_base(schema, new_scalar)

        domain_name = common.scalar_name_to_domain_name(
            new_scalar.get_name(schema), catenate=False)

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
                op.alter_scalar_type(
                    new_scalar, schema, new_type, intent=type_intent)

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


class DeleteScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.DeleteScalarType):
    def apply(self, schema, context=None):
        schema, scalar = s_scalars.DeleteScalarType.apply(
            self, schema, context)
        schema, _ = ScalarTypeMetaCommand.apply(self, schema, context)

        link = None
        if context:
            link = context.get(s_links.LinkCommandContext)

        ops = link.op.pgops if link else self.pgops

        old_domain_name = common.scalar_name_to_domain_name(
            self.classname, catenate=False)

        # Domain dropping gets low priority since other things may
        # depend on it.
        table = self.get_table(schema)
        cond = dbops.DomainExists(old_domain_name)
        ops.add(
            dbops.DropDomain(
                name=old_domain_name, conditions=[cond], priority=3))
        ops.add(
            dbops.Delete(
                table=table, condition=[(
                    'name', str(self.classname))]))

        if self.is_sequence(schema, scalar):
            seq_name = common.scalar_name_to_sequence_name(
                self.classname, catenate=False)
            self.pgops.add(dbops.DropSequence(name=seq_name))

        return schema, scalar


class CompositeObjectMetaCommand(NamedObjectMetaCommand):
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
            self, schema, context, priority=0, force_new=False,
            contained=False, manual=False, table_name=None):

        tabname = table_name if table_name else self.table_name

        if not tabname:
            ctx = context.get(self.__class__)
            assert ctx
            tabname = common.get_table_name(schema, ctx.scls, catenate=False)
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
            old_table_name = common.objtype_name_to_table_name(
                old_name, catenate=False)
            new_table_name = common.objtype_name_to_table_name(
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

    @classmethod
    def get_source_and_pointer_ctx(cls, schema, context):
        if context:
            objtype = context.get(s_objtypes.ObjectTypeCommandContext)
            link = context.get(s_links.LinkCommandContext)
        else:
            objtype = link = None

        if objtype:
            source, pointer = objtype, link
        elif link:
            property = context.get(s_props.PropertyCommandContext)
            source, pointer = link, property
        else:
            source = pointer = None

        return source, pointer

    def adjust_pointer_storage(self, orig_pointer, pointer, schema, context):
        old_ptr_stor_info = types.get_pointer_storage_info(
            orig_pointer, schema=schema)
        new_ptr_stor_info = types.get_pointer_storage_info(
            pointer, schema=schema)

        old_target = orig_pointer.get_target(schema)
        new_target = pointer.get_target(schema)

        source_ctx = context.get(s_objtypes.ObjectTypeCommandContext)
        source_op = source_ctx.op

        type_change_ok = False

        if (old_target.get_name(schema) != new_target.get_name(schema) or
                old_ptr_stor_info.table_type != new_ptr_stor_info.table_type):

            for op in self.get_subcommands(type=s_scalars.ScalarTypeCommand):
                for rename in op(s_scalars.RenameScalarType):
                    if (old_target.get_name(schema) == rename.classname and
                            new_target.get_name(schema) == rename.new_name):
                        # Our target alter is a mere rename
                        type_change_ok = True

                if isinstance(op, s_scalars.CreateScalarType):
                    if op.classname == new_target.get_name(schema):
                        # CreateScalarType will take care of everything for us
                        type_change_ok = True

            if old_ptr_stor_info.table_type != new_ptr_stor_info.table_type:
                # The attribute is being moved from one table to another
                opg = dbops.CommandGroup(priority=1)
                at = source_op.get_alter_table(schema, context, manual=True)

                if old_ptr_stor_info.table_type == 'ObjectType':
                    pat = self.get_alter_table(schema, context, manual=True)

                    # Moved from object table to link table
                    col = dbops.Column(
                        name=old_ptr_stor_info.column_name,
                        type=common.qname(*old_ptr_stor_info.column_type))
                    at.add_command(dbops.AlterTableDropColumn(col))

                    newcol = dbops.Column(
                        name=new_ptr_stor_info.column_name,
                        type=common.qname(*new_ptr_stor_info.column_type))

                    cond = dbops.ColumnExists(
                        new_ptr_stor_info.table_name, column_name=newcol.name)

                    pat.add_command(
                        (dbops.AlterTableAddColumn(newcol), None, (cond, )))
                else:
                    otabname = common.get_table_name(
                        schema, orig_pointer, catenate=False)
                    pat = self.get_alter_table(
                        schema, context, manual=True, table_name=otabname)

                    oldcol = dbops.Column(
                        name=old_ptr_stor_info.column_name,
                        type=common.qname(*old_ptr_stor_info.column_type))

                    if oldcol.name != 'std::target':
                        pat.add_command(dbops.AlterTableDropColumn(oldcol))

                    # Moved from link to object
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
                    if isinstance(old_target, s_scalars.ScalarType):
                        AlterScalarType.alter_scalar(
                            self, schema, context, old_target, new_target,
                            in_place=False)

                        alter_table = source_op.get_alter_table(
                            schema, context, priority=1)

                        new_type = \
                            types.pg_type_from_object(schema, new_target)

                        alter_type = dbops.AlterTableAlterColumnType(
                            old_ptr_stor_info.column_name,
                            common.qname(*new_type))
                        alter_table.add_operation(alter_type)

    def apply_base_delta(self, orig_source, source, schema, context):
        db_ctx = context.get(s_db.DatabaseCommandContext)
        schema = orig_source.set_field_value(
            schema, 'bases',
            [db_ctx.op._renames.get(b, b)
             for b in orig_source.get_bases(schema).objects(schema)]
        )

        orig_bases = {b.get_name(schema)
                      for b in orig_source.get_bases(schema).objects(schema)}
        new_bases = {b.get_name(schema)
                     for b in source.get_bases(schema).objects(schema)}

        dropped_bases = orig_bases - new_bases

        if isinstance(source, s_objtypes.ObjectType):
            nameconv = common.objtype_name_to_table_name
            source_ctx = context.get(s_objtypes.ObjectTypeCommandContext)
            ptr_cmd = s_links.CreateLink
        else:
            nameconv = common.link_name_to_table_name
            source_ctx = context.get(s_links.LinkCommandContext)
            ptr_cmd = s_props.CreateProperty

        alter_table = source_ctx.op.get_alter_table(
            schema, context, force_new=True)

        if (isinstance(source, s_objtypes.ObjectType) or
                source_ctx.op.has_table(source, schema)):

            schema = source.acquire_ancestor_inheritance(schema)
            schema = orig_source.acquire_ancestor_inheritance(schema)

            created_ptrs = set()
            for ptr in source_ctx.op.get_subcommands(type=ptr_cmd):
                created_ptrs.add(ptr.classname)

            inherited_aptrs = set()

            for base in source.get_bases(schema).objects(schema):
                for ptr in base.get_pointers(schema).objects(schema):
                    if ptr.scalar():
                        inherited_aptrs.add(ptr.get_shortname(schema))

            added_inh_ptrs = inherited_aptrs - {
                p.get_shortname(schema)
                for p in orig_source.get_pointers(schema).objects(schema)
            }

            ptrs = source.get_pointers(schema)
            for added_ptr in added_inh_ptrs - created_ptrs:
                ptr = ptrs.get(schema, added_ptr)
                ptr_stor_info = types.get_pointer_storage_info(
                    ptr, schema=schema)

                is_a_column = ((
                    ptr_stor_info.table_type == 'ObjectType' and
                    isinstance(source, s_objtypes.ObjectType)) or (
                        ptr_stor_info.table_type == 'link' and
                        isinstance(source, s_links.Link)))

                if is_a_column:
                    col = dbops.Column(
                        name=ptr_stor_info.column_name,
                        type=common.qname(*ptr_stor_info.column_type),
                        required=ptr.get_required(schema))
                    cond = dbops.ColumnExists(
                        table_name=source_ctx.op.table_name,
                        column_name=ptr_stor_info.column_name)
                    alter_table.add_operation(
                        (dbops.AlterTableAddColumn(col), None, (cond, )))

            if dropped_bases:
                alter_table_drop_parent = source_ctx.op.get_alter_table(
                    schema, context, force_new=True)

                for dropped_base in dropped_bases:
                    parent_table_name = nameconv(
                        sn.Name(dropped_base), catenate=False)
                    op = dbops.AlterTableDropParent(
                        parent_name=parent_table_name)
                    alter_table_drop_parent.add_operation(op)

                orig_ptrs = orig_source.get_pointers(schema)
                dropped_ptrs = (
                    set(orig_ptrs.shortnames(schema)) -
                    set(ptrs.shortnames(schema))
                )

                if dropped_ptrs:
                    alter_table_drop_ptr = source_ctx.op.get_alter_table(
                        schema, context, force_new=True)

                    for dropped_ptr in dropped_ptrs:
                        ptr = orig_ptrs.get(schema, dropped_ptr)
                        ptr_stor_info = types.get_pointer_storage_info(
                            ptr, schema=schema)

                        is_a_column = ((
                            ptr_stor_info.table_type == 'ObjectType' and
                            isinstance(source, s_objtypes.ObjectType)) or (
                                ptr_stor_info.table_type == 'link' and
                                isinstance(source, s_links.Link)))

                        if is_a_column:
                            col = dbops.Column(
                                name=ptr_stor_info.column_name,
                                type=common.qname(*ptr_stor_info.column_type),
                                required=ptr.get_required(schema))

                            cond = dbops.ColumnExists(
                                table_name=ptr_stor_info.table_name,
                                column_name=ptr_stor_info.column_name)
                            op = dbops.AlterTableDropColumn(col)
                            alter_table_drop_ptr.add_command(
                                (op, (cond, ), ()))

            current_bases = [
                b.get_name(schema)
                for b in orig_source.get_bases(schema).objects(schema)
                if b.get_name(schema) not in dropped_bases
            ]

            new_bases = [b.get_name(schema)
                         for b in source.get_bases(schema).objects(schema)]

            unchanged_order = list(
                itertools.takewhile(
                    lambda x: x[0] == x[1], zip(current_bases, new_bases)))

            old_base_order = current_bases[len(unchanged_order):]
            new_base_order = new_bases[len(unchanged_order):]

            if new_base_order:
                table_name = nameconv(source.get_name(schema), catenate=False)
                alter_table_drop_parent = source_ctx.op.get_alter_table(
                    schema, context, force_new=True)
                alter_table_add_parent = source_ctx.op.get_alter_table(
                    schema, context, force_new=True)

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

        return schema


class SourceIndexCommand(sd.ObjectCommand,
                         metaclass=ReferencedObjectCommandMeta):
    _table = metaschema.get_metaclass_table(s_indexes.SourceIndex)

    def get_table(self, schema):
        return self._table


class CreateSourceIndex(SourceIndexCommand, CreateNamedObject,
                        adapts=s_indexes.CreateSourceIndex):

    def apply(self, schema, context):
        schema, index = CreateNamedObject.apply(self, schema, context)

        source = context.get(s_links.LinkCommandContext)
        if not source:
            source = context.get(s_objtypes.ObjectTypeCommandContext)
        table_name = common.get_table_name(schema, source.scls, catenate=False)
        ir = ql_compiler.compile_fragment_to_ir(
            index.get_field_value(schema, 'expr'),
            schema,
            location='selector')

        sql_tree = compiler.compile_ir_to_sql_tree(
            ir, schema=schema, singleton_mode=True)
        sql_expr = codegen.SQLSourceGenerator.to_source(sql_tree)

        if isinstance(sql_tree, pg_ast.ImplicitRowExpr):
            # Trim the parentheses to avoid PostgreSQL choking on double
            # parentheses. since it expects only a single set around the column
            # list.
            sql_expr = sql_expr[1:-1]
        index_name = '{}_reg_idx'.format(index.get_name(schema))
        pg_index = dbops.Index(
            name=index_name, table_name=table_name, expr=sql_expr,
            unique=False, inherit=True,
            metadata={'schemaname': index.get_name(schema)})
        self.pgops.add(dbops.CreateIndex(pg_index, priority=3))

        return schema, index


class RenameSourceIndex(SourceIndexCommand, RenameNamedObject,
                        adapts=s_indexes.RenameSourceIndex):

    def apply(self, schema, context):
        schema, index = s_indexes.RenameSourceIndex.apply(
            self, schema, context)
        schema, _ = RenameNamedObject.apply(self, schema, context)

        subject = context.get(s_links.LinkCommandContext)
        if not subject:
            subject = context.get(s_objtypes.ObjectTypeCommandContext)
        orig_table_name = common.get_table_name(
            schema, subject.original_class, catenate=False)

        index_ctx = context.get(s_indexes.SourceIndexCommandContext)
        new_index_name = '{}_reg_idx'.format(index.get_name(schema))

        orig_idx = index_ctx.original_class
        orig_idx_name = '{}_reg_idx'.format(orig_idx.get_name(schema))
        orig_pg_idx = dbops.Index(
            name=orig_idx_name, table_name=orig_table_name, inherit=True,
            metadata={'schemaname': index.get_name(schema)})

        rename = dbops.RenameIndex(orig_pg_idx, new_name=new_index_name)
        self.pgops.add(rename)

        return schema, index


class AlterSourceIndex(SourceIndexCommand, AlterNamedObject,
                       adapts=s_indexes.AlterSourceIndex):
    def apply(self, schema, context=None):
        schema, result = s_indexes.AlterSourceIndex.apply(
            self, schema, context)
        schema, _ = AlterNamedObject.apply(self, schema, context)
        return schema, result


class DeleteSourceIndex(SourceIndexCommand, DeleteNamedObject,
                        adapts=s_indexes.DeleteSourceIndex):

    def apply(self, schema, context=None):
        schema, index = s_indexes.DeleteSourceIndex.apply(
            self, schema, context)
        schema, _ = DeleteNamedObject.apply(self, schema, context)

        source = context.get(s_links.LinkCommandContext)
        if not source:
            source = context.get(s_objtypes.ObjectTypeCommandContext)

        if not isinstance(source.op, s_named.DeleteNamedObject):
            # We should not drop indexes when the host is being dropped since
            # the indexes are dropped automatically in this case.
            #
            table_name = common.get_table_name(
                schema, source.scls, catenate=False)
            index_name = '{}_reg_idx'.format(index.get_name(schema))
            index = dbops.Index(
                name=index_name, table_name=table_name, inherit=True)
            index_exists = dbops.IndexExists(
                (table_name[0], index.name_in_catalog))
            self.pgops.add(
                dbops.DropIndex(
                    index, priority=3, conditions=(index_exists, )))

        return schema, index


class ObjectTypeMetaCommand(ViewCapableObjectMetaCommand,
                            CompositeObjectMetaCommand):
    def get_table(self, schema):
        if self.scls.get_is_virtual(schema):
            mcls = s_objtypes.UnionObjectType
        elif self.scls.get_is_derived(schema):
            mcls = s_objtypes.DerivedObjectType
        else:
            mcls = s_objtypes.ObjectType

        return metaschema.get_metaclass_table(mcls)

    @classmethod
    def has_table(cls, objtype, schema):
        return not (
            objtype.get_is_virtual(schema) or
            objtype.get_is_derived(schema)
        )


class CreateObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.CreateObjectType):
    def apply(self, schema, context=None):
        objtype_props = self.get_struct_properties(schema)
        is_virtual = objtype_props.get('is_virtual')
        is_derived = objtype_props.get('is_derived')
        if is_virtual or is_derived:
            schema, objtype = s_objtypes.CreateObjectType.apply(
                self, schema, context)
            self.create_object(schema, objtype)
            return schema, objtype

        new_table_name = common.objtype_name_to_table_name(
            self.classname, catenate=False)
        self.table_name = new_table_name

        columns = []
        if objtype_props.get('name') == 'std::Object':
            token_col = dbops.Column(
                name='__edb_token', type='uuid', required=False)
            columns.append(token_col)

        objtype_table = dbops.Table(name=new_table_name, columns=columns)
        self.pgops.add(dbops.CreateTable(table=objtype_table))

        alter_table = self.get_alter_table(schema, context)

        schema, objtype = s_objtypes.CreateObjectType.apply(
            self, schema, context)
        schema, _ = ObjectTypeMetaCommand.apply(self, schema, context)

        fields = self.create_object(schema, objtype)

        if objtype.get_name(schema).module != 'schema':
            constr_name = common.edgedb_name_to_pg_name(
                self.classname + '.class_check')

            constr_expr = dbops.Query(textwrap.dedent(f"""\
                SELECT '"std::__type__" = ' || quote_literal(id)
                FROM edgedb.ObjectType WHERE name =
                    {ql(objtype.get_name(schema))}
            """), type='text')

            cid_constraint = dbops.CheckConstraint(
                self.table_name, constr_name, constr_expr, inherit=False)
            alter_table.add_operation(
                dbops.AlterTableAddConstraint(cid_constraint))

            cid_col = dbops.Column(
                name='std::__type__', type='uuid', required=True)

            if objtype.get_name(schema) == 'std::Object':
                alter_table.add_operation(dbops.AlterTableAddColumn(cid_col))

            constraint = dbops.PrimaryKey(
                table_name=alter_table.name, columns=['std::id'])
            alter_table.add_operation(
                dbops.AlterTableAddConstraint(constraint))

        cntn = common.objtype_name_to_table_name

        bases = (
            dbops.Table(name=cntn(sn.Name(p), catenate=False))
            for p in fields['bases']
        )
        objtype_table.add_bases(bases)

        self.attach_alter_table(context)

        if self.update_search_indexes:
            schema, _ = self.update_search_indexes.apply(schema, context)
            self.pgops.add(self.update_search_indexes)

        self.pgops.add(
            dbops.Comment(object=objtype_table, text=self.classname))

        return schema, objtype


class RenameObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.RenameObjectType):
    def apply(self, schema, context=None):
        schema, scls = s_objtypes.RenameObjectType.apply(self, schema, context)
        schema, _ = ObjectTypeMetaCommand.apply(self, schema, context)

        objtype = context.get(s_objtypes.ObjectTypeCommandContext)
        assert objtype

        db_ctx = context.get(s_db.DatabaseCommandContext)
        assert db_ctx

        db_ctx.op._renames[objtype.original_class] = scls

        objtype.op.attach_alter_table(context)

        self.rename(schema, context, self.classname, self.new_name)

        new_table_name = common.objtype_name_to_table_name(
            self.new_name, catenate=False)
        objtype_table = dbops.Table(name=new_table_name)
        self.pgops.add(dbops.Comment(object=objtype_table, text=self.new_name))

        objtype.op.table_name = common.objtype_name_to_table_name(
            self.new_name, catenate=False)

        # Need to update all bits that reference objtype name

        old_constr_name = common.edgedb_name_to_pg_name(
            self.classname + '.class_check')
        new_constr_name = common.edgedb_name_to_pg_name(
            self.new_name + '.class_check')

        alter_table = self.get_alter_table(schema, context, manual=True)
        rc = dbops.AlterTableRenameConstraintSimple(
            alter_table.name, old_name=old_constr_name,
            new_name=new_constr_name)
        self.pgops.add(rc)

        self.table_name = common.objtype_name_to_table_name(
            self.new_name, catenate=False)

        schema = objtype.original_class.set_field_value(
            schema, 'name', scls.get_name(schema))

        return schema, scls


class RebaseObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.RebaseObjectType):
    def apply(self, schema, context):
        schema, result = s_objtypes.RebaseObjectType.apply(
            self, schema, context)
        schema, _ = ObjectTypeMetaCommand.apply(self, schema, context)

        if self.has_table(result, schema):
            objtype_ctx = context.get(s_objtypes.ObjectTypeCommandContext)
            source = objtype_ctx.scls
            orig_source = objtype_ctx.original_class
            schema = self.apply_base_delta(
                orig_source, source, schema, context)

        return schema, result


class AlterObjectType(ObjectTypeMetaCommand,
                      adapts=s_objtypes.AlterObjectType):
    def apply(self, schema, context=None):
        self.table_name = common.objtype_name_to_table_name(
            self.classname, catenate=False)
        schema, objtype = s_objtypes.AlterObjectType.apply(
            self, schema, context=context)
        schema, _ = ObjectTypeMetaCommand.apply(self, schema, context)

        updaterec, updates = self.fill_record(schema)

        if updaterec:
            table = self.get_table(schema)
            condition = [('name', str(objtype.get_name(schema)))]
            self.pgops.add(
                dbops.Update(
                    table=table, record=updaterec, condition=condition))

        if self.has_table(objtype, schema):
            self.attach_alter_table(context)

            if self.update_search_indexes:
                schema, _ = self.update_search_indexes.apply(schema, context)
                self.pgops.add(self.update_search_indexes)

        return schema, objtype


class DeleteObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.DeleteObjectType):
    def apply(self, schema, context=None):
        old_table_name = common.objtype_name_to_table_name(
            self.classname, catenate=False)

        schema, objtype = s_objtypes.DeleteObjectType.apply(
            self, schema, context)
        schema, _ = ObjectTypeMetaCommand.apply(self, schema, context)

        self.delete(schema, context, objtype)

        if self.has_table(objtype, schema):
            self.pgops.add(dbops.DropTable(name=old_table_name, priority=3))

        return schema, objtype


class SchedulePointerCardinalityUpdate(MetaCommand):
    pass


class CancelPointerCardinalityUpdate(MetaCommand):
    pass


class PointerMetaCommand(MetaCommand, sd.ObjectCommand,
                         metaclass=ReferencedObjectCommandMeta):
    def get_host(self, schema, context):
        if context:
            link = context.get(s_links.LinkCommandContext)
            if link and isinstance(self, s_props.PropertyCommand):
                return link
            objtype = context.get(s_objtypes.ObjectTypeCommandContext)
            if objtype:
                return objtype

    def record_metadata(self, pointer, old_pointer, schema, context):
        rec, updates = self.fill_record(
            schema, use_defaults=old_pointer is None)

        table = self.get_table(schema)

        if updates:
            if not rec:
                rec = table.record()

        default = updates.get('default')
        if default:
            if not rec:
                rec = table.record()
            rec.default = self.pack_default(default)

        return rec, updates

    def alter_host_table_column(
            self, old_ptr, ptr, schema, context, old_type, new_type):

        dropped_scalar = None

        for op in self.get_subcommands(type=s_scalars.ScalarTypeCommand):
            for rename in op(s_scalars.RenameScalarType):
                if (old_type == rename.classname and
                        new_type == rename.new_name):
                    # Our target alter is a mere rename
                    return
            if isinstance(op, s_scalars.CreateScalarType):
                if op.classname == new_type:
                    # CreateScalarType will take care of everything for us
                    return
            elif isinstance(op, s_scalars.DeleteScalarType):
                if op.classname == old_type:
                    # The former target scalar might as well have been dropped
                    dropped_scalar = op.old_class

        old_target = schema.get(old_type, dropped_scalar)
        assert old_target
        new_target = schema.get(new_type)

        alter_table = context.get(
            s_objtypes.ObjectTypeCommandContext).op.get_alter_table(
                schema, context, priority=1)
        column_name = common.edgedb_name_to_pg_name(ptr.get_shortname(schema))

        if isinstance(new_target, s_scalars.ScalarType):
            target_type = types.pg_type_from_object(schema, new_target)

            if isinstance(old_target, s_scalars.ScalarType):
                AlterScalarType.alter_scalar(
                    self, schema, context, old_target, new_target,
                    in_place=False)
                alter_type = dbops.AlterTableAlterColumnType(
                    column_name, common.qname(*target_type))
                alter_table.add_operation(alter_type)
            else:
                cols = self.get_columns(ptr, schema)
                ops = [dbops.AlterTableAddColumn(col) for col in cols]
                for op in ops:
                    alter_table.add_operation(op)
        else:
            col = dbops.Column(name=column_name, type='text')
            alter_table.add_operation(dbops.AlterTableDropColumn(col))

    def get_pointer_default(self, ptr, schema, context):
        if ptr.is_pure_computable(schema):
            return None

        default = self.updates.get('default')
        default_value = None

        if default is not None:
            if isinstance(default, s_expr.ExpressionText):
                default_value = schemamech.ptr_default_to_col_default(
                    schema, ptr, default)
            else:
                default_value = common.quote_literal(
                    str(default))
        elif ptr.get_target(schema).issubclass(
                schema, schema.get('std::sequence')):
            # TODO: replace this with a generic scalar type default
            #       using std::nextval().
            seq_name = common.quote_literal(
                common.scalar_name_to_sequence_name(
                    ptr.get_target(schema).get_name(schema)))
            default_value = f'nextval({seq_name}::regclass)'

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
                    CompositeObjectMetaCommand.get_source_and_pointer_ctx(
                        schema, context)
                alter_table = source_ctx.op.get_alter_table(
                    schema, context, contained=True, priority=3)
                column_name = common.edgedb_name_to_pg_name(
                    pointer.get_shortname(schema))
                alter_table.add_operation(
                    dbops.AlterTableAlterColumnDefault(
                        column_name=column_name, default=new_default))

    @classmethod
    def get_columns(cls, pointer, schema, default=None):
        ptr_stor_info = types.get_pointer_storage_info(pointer, schema=schema)
        col_type = list(ptr_stor_info.column_type)
        if col_type[-1].endswith('[]'):
            # Array
            col_type[-1] = col_type[-1][:-2]
            col_type = common.qname(*col_type) + '[]'
        else:
            col_type = common.qname(*col_type)

        return [
            dbops.Column(
                name=ptr_stor_info.column_name,
                type=col_type,
                required=pointer.get_required(schema),
                default=default,
                comment=pointer.get_shortname(schema))
        ]

    def rename_pointer(self, pointer, schema, context, old_name, new_name):
        if context:
            old_name = pointer.shortname_from_fullname(old_name)
            new_name = pointer.shortname_from_fullname(new_name)

            host = self.get_host(schema, context)

            if host and old_name != new_name:
                if (new_name.endswith('std::source') and
                        not host.scls.generic(schema)):
                    pass
                else:
                    old_col_name = common.edgedb_name_to_pg_name(old_name)
                    new_col_name = common.edgedb_name_to_pg_name(new_name)

                    ptr_stor_info = types.get_pointer_storage_info(
                        pointer, schema=schema)

                    is_a_column = ((
                        ptr_stor_info.table_type == 'ObjectType' and
                        isinstance(host.scls, s_objtypes.ObjectType)) or (
                            ptr_stor_info.table_type == 'link' and
                            isinstance(host.scls, s_links.Link)))

                    if is_a_column:
                        table_name = common.get_table_name(
                            schema, host.scls, catenate=False)
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

        table = self.get_table(schema)
        rec = table.record()
        rec.name = str(self.new_name)
        self.pgops.add(
            dbops.Update(
                table=table, record=rec, condition=[(
                    'name', str(self.classname))], priority=1))

    @classmethod
    def has_table(cls, src, schema):
        if isinstance(src, s_objtypes.ObjectType):
            return True
        elif src.is_pure_computable(schema) or src.get_is_derived(schema):
            return False
        elif src.generic(schema):
            if src.get_name(schema) == 'std::link':
                return True
            elif src.has_user_defined_properties(schema):
                return True
            else:
                for l in src.children(schema):
                    if not l.generic(schema):
                        ptr_stor_info = types.get_pointer_storage_info(
                            l, resolve_type=False)
                        if ptr_stor_info.table_type == 'link':
                            return True

                return False
        else:
            return (not src.scalar() or not src.singular(schema) or
                    src.has_user_defined_properties(schema))

    def create_table(self, ptr, schema, context, conditional=False):
        c = self._create_table(ptr, schema, context, conditional=conditional)
        self.pgops.add(c)

    def provide_table(self, ptr, schema, context):
        if not ptr.generic(schema):
            gen_ptr = ptr.get_bases(schema).first(schema)

            if self.has_table(gen_ptr, schema):
                self.create_table(gen_ptr, schema, context, conditional=True)

        if self.has_table(ptr, schema):
            self.create_table(ptr, schema, context, conditional=True)


class LinkMetaCommand(CompositeObjectMetaCommand, PointerMetaCommand):
    _table = metaschema.get_metaclass_table(s_links.Link)

    def get_table(self, schema):
        return self._table

    @classmethod
    def _create_table(
            cls, link, schema, context, conditional=False, create_bases=True,
            create_children=True):
        new_table_name = common.get_table_name(schema, link, catenate=False)

        create_c = dbops.CommandGroup()

        constraints = []
        columns = []

        src_col = common.edgedb_name_to_pg_name('std::source')
        tgt_col = common.edgedb_name_to_pg_name('std::target')

        if link.get_name(schema) == 'std::link':
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
                    name='ptr_item_id', type='uuid', required=True))

        constraints.append(
            dbops.UniqueConstraint(
                table_name=new_table_name,
                columns=[src_col, tgt_col, 'ptr_item_id']))

        if not link.generic(schema) and link.scalar():
            try:
                tgt_prop = link.getptr(schema, 'std::target')
            except KeyError:
                pass
            else:
                tgt_ptr = types.get_pointer_storage_info(
                    tgt_prop, schema=schema)
                columns.append(
                    dbops.Column(
                        name=tgt_ptr.column_name,
                        type=common.qname(*tgt_ptr.column_type)))

        table = dbops.Table(name=new_table_name)
        table.add_columns(columns)
        table.constraints = constraints

        link_bases = link.get_bases(schema)

        if link_bases:
            bases = []

            for parent in link_bases.objects(schema):
                if isinstance(parent, s_obj.Object):
                    if create_bases:
                        bc = cls._create_table(
                            parent, schema, context, conditional=True,
                            create_children=False)
                        create_c.add_command(bc)

                    tabname = common.get_table_name(
                        schema, parent, catenate=False)
                    bases.append(dbops.Table(name=tabname))

            table.add_bases(bases)

        ct = dbops.CreateTable(table=table)

        index_name = common.edgedb_name_to_pg_name(
            str(link.get_name(schema)) + 'target_id_default_idx')
        index = dbops.Index(index_name, new_table_name, unique=False)
        index.add_columns([tgt_col])
        ci = dbops.CreateIndex(index)

        if conditional:
            c = dbops.CommandGroup(
                neg_conditions=[dbops.TableExists(new_table_name)])
        else:
            c = dbops.CommandGroup()

        c.add_command(ct)
        c.add_command(ci)

        c.add_command(dbops.Comment(table, link.get_name(schema)))

        create_c.add_command(c)

        if create_children:
            for l_descendant in link.descendants(schema):
                if cls.has_table(l_descendant, schema):
                    lc = LinkMetaCommand._create_table(
                        l_descendant, schema, context, conditional=True,
                        create_bases=False, create_children=False)
                    create_c.add_command(lc)

        return create_c

    def schedule_endpoint_delete_action_update(self, link, schema, context):
        endpoint_delete_actions = context.get(
            s_db.DatabaseCommandContext).op.update_endpoint_delete_actions
        endpoint_delete_actions.link_ops.append((self, link))


class CreateLink(LinkMetaCommand, adapts=s_links.CreateLink):
    def apply(self, schema, context=None):
        # Need to do this early, since potential table alters triggered by
        # sub-commands need this.
        schema, link = s_links.CreateLink.apply(self, schema, context)
        self.table_name = common.get_table_name(schema, link, catenate=False)
        schema, _ = LinkMetaCommand.apply(self, schema, context)

        # We do not want to create a separate table for scalar links, unless
        # they have properties, or are non-singular, since those are stored
        # directly in the source table.
        #
        # Implicit derivative links also do not get their own table since
        # they're just a special case of the parent.
        #
        # On the other hand, much like with objects we want all other links
        # to be in separate tables even if they do not define additional
        # properties. This is to allow for further schema evolution.
        #
        self.provide_table(link, schema, context)

        objtype = context.get(s_objtypes.ObjectTypeCommandContext)
        rec, updates = self.record_metadata(link, None, schema, context)
        self.updates = updates

        if not link.generic(schema):
            ptr_stor_info = types.get_pointer_storage_info(
                link, resolve_type=False)

            if ptr_stor_info.table_type == 'ObjectType':
                default_value = self.get_pointer_default(link, schema, context)

                cols = self.get_columns(link, schema, default_value)
                table_name = common.get_table_name(
                    schema, objtype.scls, catenate=False)
                objtype_alter_table = objtype.op.get_alter_table(
                    schema, context)

                for col in cols:
                    # The column may already exist as inherited from parent
                    # table.
                    cond = dbops.ColumnExists(
                        table_name=table_name, column_name=col.name)
                    cmd = dbops.AlterTableAddColumn(col)
                    objtype_alter_table.add_operation((cmd, None, (cond, )))

                if default_value is not None:
                    self.alter_pointer_default(link, schema, context)

        objtype = context.get(s_objtypes.ObjectTypeCommandContext)
        table = self.get_table(schema)
        self.pgops.add(
            dbops.Insert(table=table, records=[rec], priority=1))

        self.attach_alter_table(context)

        if not link.generic(schema):
            self.schedule_endpoint_delete_action_update(link, schema, context)

        return schema, link


class RenameLink(LinkMetaCommand, adapts=s_links.RenameLink):
    def apply(self, schema, context=None):
        schema, result = s_links.RenameLink.apply(self, schema, context)
        schema, _ = LinkMetaCommand.apply(self, schema, context)

        self.rename_pointer(
            result, schema, context, self.classname, self.new_name)

        self.attach_alter_table(context)

        if result.generic(schema):
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

        return schema, result


class RebaseLink(LinkMetaCommand, adapts=s_links.RebaseLink):
    def apply(self, schema, context):
        schema, result = s_links.RebaseLink.apply(self, schema, context)
        schema, _ = LinkMetaCommand.apply(self, schema, context)

        schema = result.acquire_ancestor_inheritance(schema)

        link_ctx = context.get(s_links.LinkCommandContext)
        source = link_ctx.scls

        orig_source = link_ctx.original_class

        if self.has_table(source, schema):
            schema = self.apply_base_delta(
                orig_source, source, schema, context)

        return schema, result


class AlterLink(LinkMetaCommand, adapts=s_links.AlterLink):
    def apply(self, schema, context=None):
        schema, old_link = schema.get(self.classname).temp_copy(schema)
        self.old_link = old_link
        schema, link = s_links.AlterLink.apply(self, schema, context)
        schema, _ = LinkMetaCommand.apply(self, schema, context)

        with context(s_links.LinkCommandContext(schema, self, link)):
            rec, updates = self.record_metadata(
                link, old_link, schema, context)
            self.updates = updates

            self.provide_table(link, schema, context)

            if rec:
                table = self.get_table(schema)
                self.pgops.add(
                    dbops.Update(
                        table=table, record=rec, condition=[(
                            'name', str(link.get_name(schema)))], priority=1))

            new_type = None
            for op in self.get_subcommands(type=sd.AlterObjectProperty):
                if op.property == 'target':
                    new_type = op.new_value.name \
                        if op.new_value is not None else None
                    break

            if new_type:
                if not isinstance(link.get_target(schema), s_obj.Object):
                    schema = link.set_field_value(
                        schema, 'target', schema.get(link.get_target(schema)))

            self.attach_alter_table(context)

            if not link.generic(schema):
                self.adjust_pointer_storage(old_link, link, schema, context)

                old_ptr_stor_info = types.get_pointer_storage_info(
                    old_link, schema=schema)
                ptr_stor_info = types.get_pointer_storage_info(
                    link, schema=schema)

                link_required = link.get_required(schema)
                old_link_required = self.old_link.get_required(schema)

                if (old_ptr_stor_info.table_type == 'ObjectType' and
                        ptr_stor_info.table_type == 'ObjectType' and
                        link_required != old_link_required):

                    ot_ctx = context.get(s_objtypes.ObjectTypeCommandContext)
                    alter_table = ot_ctx.op.get_alter_table(
                        schema, context)
                    column_name = common.edgedb_name_to_pg_name(
                        link.get_shortname(schema))
                    alter_table.add_operation(
                        dbops.AlterTableAlterColumnNull(
                            column_name=column_name,
                            null=not link.get_required(schema)))

            if isinstance(link.get_target(schema), s_scalars.ScalarType):
                self.alter_pointer_default(link, schema, context)

        return schema, link


class DeleteLink(LinkMetaCommand, adapts=s_links.DeleteLink):
    def apply(self, schema, context=None):
        schema, result = s_links.DeleteLink.apply(self, schema, context)
        schema, _ = LinkMetaCommand.apply(self, schema, context)

        if not result.generic(schema):
            ptr_stor_info = types.get_pointer_storage_info(
                result, schema=schema)
            objtype = context.get(s_objtypes.ObjectTypeCommandContext)

            name = result.get_shortname(schema)

            if ptr_stor_info.table_type == 'ObjectType':
                # Only drop the column if the link was not reinherited in the
                # same delta.
                if objtype.scls.getptr(schema, name) is None:
                    # This must be a separate so that objects depending
                    # on this column can be dropped correctly.
                    #
                    alter_table = objtype.op.get_alter_table(
                        schema, context, manual=True, priority=2)
                    col = dbops.Column(
                        name=ptr_stor_info.column_name,
                        type=common.qname(*ptr_stor_info.column_type))
                    cond = dbops.ColumnExists(
                        table_name=objtype.op.table_name, column_name=col.name)
                    col = dbops.AlterTableDropColumn(col)
                    alter_table.add_operation((col, [cond], []))
                    self.pgops.add(alter_table)

        old_table_name = common.get_table_name(schema, result, catenate=False)
        condition = dbops.TableExists(name=old_table_name)
        self.pgops.add(
            dbops.DropTable(name=old_table_name, conditions=[condition]))

        table = self.get_table(schema)
        self.pgops.add(
            dbops.Delete(
                table=table,
                condition=[('name', str(result.get_name(schema)))]))

        return schema, result


class PropertyMetaCommand(NamedObjectMetaCommand, PointerMetaCommand):
    _table = metaschema.get_metaclass_table(s_props.Property)

    def get_table(self, schema):
        return self._table

    @classmethod
    def _create_table(
            cls, prop, schema, context, conditional=False, create_bases=True,
            create_children=True):
        new_table_name = common.get_table_name(schema, prop, catenate=False)

        create_c = dbops.CommandGroup()

        constraints = []
        columns = []

        src_col = common.edgedb_name_to_pg_name('std::source')

        if prop.get_name(schema) == 'std::property':
            columns.append(
                dbops.Column(
                    name=src_col, type='uuid', required=True,
                    comment='std::source'))
            columns.append(
                dbops.Column(
                    name='ptr_item_id', type='uuid', required=True))

        index_name = common.convert_name(
            prop.get_name(schema), 'idx0', catenate=True)

        pg_index = dbops.Index(
            name=index_name, table_name=new_table_name,
            unique=False, columns=[src_col])

        ci = dbops.CreateIndex(pg_index)

        if not prop.generic(schema):
            tgt_cols = cls.get_columns(prop, schema, None)
            columns.extend(tgt_cols)

            constraints.append(
                dbops.UniqueConstraint(
                    table_name=new_table_name,
                    columns=[src_col, 'ptr_item_id'] +
                            [tgt_col.name for tgt_col in tgt_cols]
                )
            )

        table = dbops.Table(name=new_table_name)
        table.add_columns(columns)
        table.constraints = constraints

        prop_bases = prop.get_bases(schema)
        if prop_bases:
            bases = []

            for parent in prop_bases.objects(schema):
                if isinstance(parent, s_obj.Object):
                    if create_bases:
                        bc = cls._create_table(
                            parent, schema, context, conditional=True,
                            create_children=False)
                        create_c.add_command(bc)

                    tabname = common.get_table_name(
                        schema, parent, catenate=False)
                    bases.append(dbops.Table(name=tabname))

            table.add_bases(bases)

        ct = dbops.CreateTable(table=table)

        if conditional:
            c = dbops.CommandGroup(
                neg_conditions=[dbops.TableExists(new_table_name)])
        else:
            c = dbops.CommandGroup()

        c.add_command(ct)
        c.add_command(ci)

        c.add_command(dbops.Comment(table, prop.get_name(schema)))

        create_c.add_command(c)

        if create_children:
            for p_descendant in prop.descendants(schema):
                if cls.has_table(p_descendant, schema):
                    pc = PropertyMetaCommand._create_table(
                        p_descendant, schema, context, conditional=True,
                        create_bases=False, create_children=False)
                    create_c.add_command(pc)

        return create_c


class CreateProperty(PropertyMetaCommand, adapts=s_props.CreateProperty):
    def apply(self, schema, context):
        schema, prop = s_props.CreateProperty.apply(self, schema, context)
        schema, _ = PropertyMetaCommand.apply(self, schema, context)

        src = context.get(s_sources.SourceCommandContext)

        self.provide_table(prop, schema, context)

        with context(s_props.PropertyCommandContext(schema, self, prop)):
            rec, updates = self.record_metadata(prop, None, schema, context)
            self.updates = updates

        if src and self.has_table(src.scls, schema):
            if isinstance(src, s_links.Link):
                src.op.provide_table(src.scls, schema, context)

            ptr_stor_info = types.get_pointer_storage_info(
                prop, resolve_type=False)

            if (not isinstance(src.scls, s_objtypes.ObjectType) or
                    ptr_stor_info.table_type == 'ObjectType'):
                alter_table = src.op.get_alter_table(schema, context)

                default_value = self.get_pointer_default(prop, schema, context)

                cols = self.get_columns(prop, schema, default_value)

                for col in cols:
                    # The column may already exist as inherited from
                    # parent table
                    cond = dbops.ColumnExists(
                        table_name=alter_table.name, column_name=col.name)

                    if prop.get_required(schema):
                        # For some reason, Postgres allows dropping NOT NULL
                        # constraints from inherited columns, but we really
                        # should only always increase constraints down the
                        # inheritance chain.
                        cmd = dbops.AlterTableAlterColumnNull(
                            column_name=col.name,
                            null=not prop.get_required(schema))
                        alter_table.add_operation((cmd, (cond, ), None))

                    cmd = dbops.AlterTableAddColumn(col)
                    alter_table.add_operation((cmd, None, (cond, )))

        # Priority is set to 2 to make sure that INSERT is run after the host
        # link is INSERTed into edgedb.link.
        table = self.get_table(schema)
        self.pgops.add(
            dbops.Insert(table=table, records=[rec], priority=2))

        return schema, prop


class RenameProperty(
        PropertyMetaCommand, adapts=s_props.RenameProperty):
    def apply(self, schema, context=None):
        schema, result = s_props.RenameProperty.apply(self, schema, context)
        schema, _ = PropertyMetaCommand.apply(self, schema, context)

        self.rename_pointer(
            result, schema, context, self.classname, self.new_name)

        return schema, result


class AlterProperty(
        PropertyMetaCommand, adapts=s_props.AlterProperty):
    def apply(self, schema, context=None):
        metaclass = self.get_schema_metaclass()
        schema, old_prop = \
            schema.get(self.classname, type=metaclass).temp_copy(schema)
        self.old_prop = old_prop
        schema, prop = s_props.AlterProperty.apply(self, schema, context)
        schema, _ = PropertyMetaCommand.apply(self, schema, context)

        with context(s_props.PropertyCommandContext(schema, self, prop)):
            rec, updates = self.record_metadata(
                prop, old_prop, schema, context)
            self.updates = updates

            self.provide_table(prop, schema, context)

            if rec:
                table = self.get_table(schema)
                self.pgops.add(
                    dbops.Update(
                        table=table, record=rec, condition=[(
                            'name', str(prop.get_name(schema)))], priority=1))

            prop_target = prop.get_target(schema)
            old_prop_target = self.old_prop.get_target(schema)

            prop_required = prop.get_required(schema)
            old_prop_required = self.old_prop.get_required(schema)

            if (isinstance(prop_target, s_scalars.ScalarType) and
                    isinstance(old_prop_target, s_scalars.ScalarType) and
                    prop_required != old_prop_required):

                src_ctx = context.get(s_sources.SourceCommandContext)
                src_op = src_ctx.op
                alter_table = src_op.get_alter_table(
                    schema, context, priority=5)
                column_name = common.edgedb_name_to_pg_name(
                    prop.get_shortname(schema))
                if prop.get_required(schema):
                    table = src_op._type_mech.get_table(src_ctx.scls, schema)
                    rec = table.record(**{column_name: dbops.Default()})
                    cond = [(column_name, None)]
                    update = dbops.Update(table, rec, cond, priority=4)
                    self.pgops.add(update)
                alter_table.add_operation(
                    dbops.AlterTableAlterColumnNull(
                        column_name=column_name,
                        null=not prop.get_required(schema)))

            new_type = None
            for op in self.get_subcommands(type=sd.AlterObjectProperty):
                if (op.property == 'target' and
                        prop.get_shortname(schema) not in
                        {'std::source', 'std::target'}):
                    new_type = op.new_value.name \
                        if op.new_value is not None else None
                    old_type = op.old_value.name \
                        if op.old_value is not None else None
                    break

            if new_type:
                self.alter_host_table_column(
                    old_prop, prop, schema, context, old_type, new_type)

            self.alter_pointer_default(prop, schema, context)

        return schema, prop


class DeleteProperty(
        PropertyMetaCommand, adapts=s_props.DeleteProperty):
    def apply(self, schema, context=None):
        schema, prop = s_props.DeleteProperty.apply(self, schema, context)
        schema, _ = PropertyMetaCommand.apply(self, schema, context)

        link = context.get(s_links.LinkCommandContext)

        if link:
            alter_table = link.op.get_alter_table(schema, context)

            column_name = common.edgedb_name_to_pg_name(
                prop.get_shortname(schema))
            # We don't really care about the type -- we're dropping the thing
            column_type = 'text'

            col = dbops.AlterTableDropColumn(
                dbops.Column(name=column_name, type=column_type))
            alter_table.add_operation(col)

        table = self.get_table(schema)
        self.pgops.add(
            dbops.Delete(
                table=table, condition=[('name', str(prop.get_name(schema)))]))

        return schema, prop


class UpdateEndpointDeleteActions(MetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.link_ops = []

    def _get_link_table_union(self, schema, links) -> str:
        selects = []
        for link in links:
            selects.append(textwrap.dedent('''\
                (SELECT ptr_item_id, {src} as source, {tgt} as target
                FROM {table})
            ''').format(
                src=common.quote_ident('std::source'),
                tgt=common.quote_ident('std::target'),
                table=common.link_name_to_table_name(
                    link.get_name(schema)),
            ))

        return '(' + '\nUNION ALL\n    '.join(selects) + ') as q'

    def get_trigger_name(self, schema, target,
                         disposition, deferred=False):
        if disposition == 'target':
            name = 'deltde' if deferred else 'deltim'
        else:
            name = 'delsde' if deferred else 'delsim'

        return common.edgedb_name_to_pg_name(
            f'{target.get_name(schema)}_{name}')

    def get_trigger_proc_name(self, schema, target,
                              disposition, deferred=False):
        if disposition == 'target':
            name = 'deltdef' if deferred else 'deltimf'
        else:
            name = 'delsdef' if deferred else 'delsimf'

        return common.convert_name(
            target.get_name(schema), name, catenate=False)

    def get_trigger_proc_text(self, target, links, disposition, schema):
        chunks = []

        DA = s_links.LinkTargetDeleteAction

        if disposition == 'target':
            groups = itertools.groupby(
                links, lambda l: l.get_on_target_delete(schema))
            near_endpoint, far_endpoint = 'target', 'source'
        else:
            groups = [(DA.SET_EMPTY, links)]
            near_endpoint, far_endpoint = 'source', 'target'

        for action, links in groups:
            if action is DA.RESTRICT or action is DA.DEFERRED_RESTRICT:
                tables = self._get_link_table_union(schema, links)

                text = textwrap.dedent('''\
                    SELECT
                        q.ptr_item_id, q.source, q.target
                        INTO link_type_id, srcid, tgtid
                    FROM
                        {tables}
                    WHERE
                        q.{near_endpoint} = OLD.{id}
                    LIMIT 1;

                    IF FOUND THEN
                        SELECT
                            edgedb.shortname_from_fullname(link.name),
                            edgedb._resolve_type_name(link.{far_endpoint})
                            INTO linkname, endname
                        FROM
                            edgedb.Link AS link
                        WHERE
                            link.id = link_type_id;
                        RAISE foreign_key_violation
                            USING
                                TABLE = TG_TABLE_NAME,
                                SCHEMA = TG_TABLE_SCHEMA,
                                MESSAGE = 'deletion of {tgtname} (' || tgtid
                                    || ') is prohibited by link target policy',
                                DETAIL = 'Object is still referenced in link '
                                    || linkname || ' of ' || endname || ' ('
                                    || srcid || ').';
                    END IF;
                ''').format(
                    tables=tables,
                    id=common.quote_ident('std::id'),
                    tgtname=target.get_displayname(schema),
                    near_endpoint=near_endpoint,
                    far_endpoint=far_endpoint,
                )

                chunks.append(text)

            elif action == s_links.LinkTargetDeleteAction.SET_EMPTY:
                for link in links:
                    link_table = common.link_name_to_table_name(
                        link.get_name(schema))

                    text = textwrap.dedent('''\
                        DELETE FROM
                            {link_table}
                        WHERE
                            {endpoint} = OLD.{id};
                    ''').format(
                        link_table=link_table,
                        endpoint=common.quote_ident(f'std::{near_endpoint}'),
                        id=common.quote_ident('std::id')
                    )

                    chunks.append(text)

            elif action == s_links.LinkTargetDeleteAction.DELETE_SOURCE:
                sources = collections.defaultdict(list)
                for link in links:
                    sources[link.get_source(schema)].append(link)

                for source, source_links in sources.items():
                    tables = self._get_link_table_union(schema, source_links)

                    text = textwrap.dedent('''\
                        DELETE FROM
                            {source_table}
                        WHERE
                            {source_table}.{id} IN (
                                SELECT source
                                FROM {tables}
                                WHERE target = OLD.{id}
                            );
                    ''').format(
                        source_table=common.get_table_name(schema, source),
                        id=common.quote_ident('std::id'),
                        tables=tables,
                    )

                    chunks.append(text)

        text = textwrap.dedent('''\
            DECLARE
                link_type_id uuid;
                srcid uuid;
                tgtid uuid;
                linkname text;
                endname text;
            BEGIN
                {chunks}
                RETURN OLD;
            END;
        ''').format(chunks='\n\n'.join(chunks))

        return text

    def apply(self, schema, context):
        if not self.link_ops:
            return schema, None

        DA = s_links.LinkTargetDeleteAction

        source_map = collections.defaultdict(list)
        target_map = collections.defaultdict(list)
        deferred_target_map = collections.defaultdict(list)

        for link_op, link in self.link_ops:
            if link.generic(schema) or link.get_source(schema).is_view(schema):
                continue

            ptr_stor_info = types.get_pointer_storage_info(link, schema=schema)
            if ptr_stor_info.table_type != 'link':
                continue

            source_map[link.get_source(schema).get_name(schema)].append(link)

            if link.get_on_target_delete(schema) is DA.DEFERRED_RESTRICT:
                deferred_target_map[
                    link.get_target(schema).get_name(schema)].append(link)
            else:
                target_map[
                    link.get_target(schema).get_name(schema)].append(link)

        for source_name, links in source_map.items():
            self._create_action_triggers(
                schema, source_name, links, disposition='source')

        for links in target_map.values():
            links.sort(
                key=lambda l: (l.get_on_target_delete(schema),
                               l.get_name(schema)))

        for target_name, links in target_map.items():
            self._create_action_triggers(
                schema, target_name, links, disposition='target')

        for target_name, links in deferred_target_map.items():
            self._create_action_triggers(
                schema, target_name, links,
                disposition='target', deferred=True)

        return schema, None

    def _create_action_triggers(
            self,
            schema,
            objtype_name: str,
            links: typing.List[s_links.Link], *,
            disposition: str,
            deferred: bool=False) -> None:
        objtype = schema.get(objtype_name)

        if objtype.get_is_virtual(schema):
            objtypes = tuple(objtype.children(schema))
        else:
            objtypes = (objtype,)

        for objtype in objtypes:
            table_name = common.objtype_name_to_table_name(
                objtype.get_name(schema), catenate=False)

            trigger_name = self.get_trigger_name(
                schema, objtype, disposition=disposition, deferred=deferred)
            proc_name = self.get_trigger_proc_name(
                schema, objtype, disposition=disposition, deferred=deferred)
            proc_text = self.get_trigger_proc_text(
                objtype, links, disposition=disposition, schema=schema)

            trig_func = dbops.Function(
                name=proc_name, text=proc_text, volatility='volatile',
                returns='trigger', language='plpgsql')

            self.pgops.add(dbops.CreateOrReplaceFunction(trig_func))

            trigger = dbops.Trigger(
                name=trigger_name, table_name=table_name,
                events=('delete',), procedure=proc_name,
                is_constraint=True, inherit=True, deferred=deferred)

            self.pgops.add(dbops.CreateTrigger(
                trigger, neg_conditions=[dbops.TriggerExists(
                    trigger_name=trigger_name, table_name=table_name
                )]
            ))


class CommandContext(sd.CommandContext):
    def __init__(self, db):
        super().__init__()
        self.db = db
        self.class_name_to_id_map = None

    async def _get_class_map(self, reverse=False):
        classes = await datasources.schema.objects.fetch(self.db)
        grouped = itertools.groupby(classes, key=lambda i: i['id'])
        if reverse:
            class_map = {k: next(i)['name'] for k, i in grouped}
        else:
            class_map = {next(i)['name']: k for k, i in grouped}
        return class_map

    async def get_class_map(self):
        class_map = self.class_name_to_id_map
        if not class_map:
            class_map = await self._get_class_map()
            self.class_name_to_id_map = class_map
        return class_map


class ModuleMetaCommand(NamedObjectMetaCommand):
    _table = metaschema.get_metaclass_table(s_mod.Module)

    def get_table(self, schema):
        return self._table


class CreateModule(ModuleMetaCommand, adapts=s_mod.CreateModule):
    def apply(self, schema, context):
        schema, _ = CompositeObjectMetaCommand.apply(self, schema, context)
        schema, module = s_mod.CreateModule.apply(self, schema, context)
        self.scls = module

        module_name = module.get_name(schema)
        schema_name = common.edgedb_module_name_to_schema_name(module_name)
        condition = dbops.SchemaExists(name=schema_name)

        cmd = dbops.CommandGroup(neg_conditions={condition})
        cmd.add_command(dbops.CreateSchema(name=schema_name))
        self.pgops.add(cmd)

        self.create_object(schema, module)

        return schema, module


class AlterModule(ModuleMetaCommand, adapts=s_mod.AlterModule):
    def apply(self, schema, context):
        schema, module = s_mod.AlterModule.apply(self, schema, context=context)
        schema, _ = CompositeObjectMetaCommand.apply(self, schema, context)

        updaterec, updates = self.fill_record(schema)

        if updaterec:
            table = self.get_table(schema)
            condition = [('name', str(module.get_name(schema)))]
            self.pgops.add(
                dbops.Update(
                    table=table, record=updaterec, condition=condition))

        self.attach_alter_table(context)

        return schema, module


class DeleteModule(ModuleMetaCommand, adapts=s_mod.DeleteModule):
    def apply(self, schema, context):
        schema, _ = CompositeObjectMetaCommand.apply(self, schema, context)
        schema, module = s_mod.DeleteModule.apply(self, schema, context)

        module_name = module.get_name(schema)
        schema_name = common.edgedb_module_name_to_schema_name(module_name)
        condition = dbops.SchemaExists(name=schema_name)

        table = self.get_table(schema)
        cmd = dbops.CommandGroup()
        cmd.add_command(
            dbops.DropSchema(
                name=schema_name, conditions={condition}, priority=4))
        cmd.add_command(
            dbops.Delete(
                table=table, condition=[(
                    'name', str(module.get_name(schema)))]))

        self.pgops.add(cmd)

        return schema, module


class CreateDatabase(ObjectMetaCommand, adapts=s_db.CreateDatabase):
    def apply(self, schema, context):
        schema, _ = s_db.CreateDatabase.apply(self, schema, context)
        self.pgops.add(dbops.CreateDatabase(dbops.Database(self.name)))
        return schema, None


class AlterDatabase(ObjectMetaCommand, adapts=s_db.AlterDatabase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._renames = {}

    def apply(self, schema, context):
        self.update_endpoint_delete_actions = UpdateEndpointDeleteActions()

        schema, _ = s_db.AlterDatabase.apply(self, schema, context)
        schema, _ = MetaCommand.apply(self, schema)

        self.update_endpoint_delete_actions.apply(schema, context)

        self.pgops.add(self.update_endpoint_delete_actions)

        return schema, None

    def is_material(self):
        return True

    def generate(self, block: dbops.PLBlock) -> None:
        for op in self.serialize_ops():
            op.generate(block)

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


class DropDatabase(ObjectMetaCommand, adapts=s_db.DropDatabase):
    def apply(self, schema, context):
        schema, _ = s_db.CreateDatabase.apply(self, schema, context)
        self.pgops.add(dbops.DropDatabase(self.name))
        return schema, None
