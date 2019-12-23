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


from __future__ import annotations

import collections.abc
import itertools
import json
import textwrap
from typing import *  # NoQA

from edb import errors

from edb.edgeql import ast as ql_ast
from edb.edgeql import compiler as ql_compiler
from edb.edgeql import qltypes as ql_ft

from edb.schema import annos as s_anno
from edb.schema import casts as s_casts
from edb.schema import scalars as s_scalars
from edb.schema import objtypes as s_objtypes
from edb.schema import constraints as s_constr
from edb.schema import database as s_db
from edb.schema import delta as sd
from edb.schema import expr as s_expr
from edb.schema import functions as s_funcs
from edb.schema import indexes as s_indexes
from edb.schema import links as s_links
from edb.schema import lproperties as s_props
from edb.schema import modules as s_mod
from edb.schema import name as sn
from edb.schema import objects as s_obj
from edb.schema import operators as s_opers
from edb.schema import pointers as s_pointers
from edb.schema import referencing as s_referencing
from edb.schema import roles as s_roles
from edb.schema import sources as s_sources
from edb.schema import types as s_types

from edb.common import ordered
from edb.common import markup

from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from edb.pgsql import common

from edb.pgsql import dbops, metaschema

from . import ast as pg_ast
from .common import qname as q
from .common import quote_literal as ql
from .common import quote_ident as qi
from .common import quote_type as qt
from . import compiler
from . import codegen
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
            if not isinstance(op, sd.AlterObjectProperty):
                self.pgops.add(op)
        return schema, None

    def generate(self, block: dbops.PLBlock) -> None:
        for op in sorted(
                self.pgops, key=lambda i: getattr(i, 'priority', 0),
                reverse=True):
            op.generate(block)

    @classmethod
    def as_markup(cls, self, *, ctx):
        node = super().as_markup(self, ctx=ctx)

        for op in self.pgops:
            node.add_child(node=markup.serialize(op, ctx=ctx))

        return node


class CommandGroupAdapted(MetaCommand, adapts=sd.CommandGroup):
    def apply(self, schema, context):
        schema, _ = sd.CommandGroup.apply(self, schema, context)
        schema, _ = MetaCommand.apply(self, schema, context)
        return schema, None


class Record:
    def __init__(self, items):
        self._items = items

    def __iter__(self):
        return iter(self._items)

    def __len__(self):
        return len(self._items)

    def __repr__(self):
        return '<_Record {!r}>'.format(self._items)


class ObjectMetaCommand(MetaCommand, sd.ObjectCommand,
                        metaclass=ObjectCommandMeta):
    op_priority = 0

    def get_table(self, schema):
        raise NotImplementedError

    def _get_id(self, schema, value):
        if isinstance(value, s_obj.ObjectRef):
            obj_id = value._resolve_ref(schema).id
        elif isinstance(value, s_obj.Object):
            obj_id = value.id
        else:
            raise ValueError(
                f'expecting a ObjectRef or an Object, got {value!r}')

        return obj_id

    def _serialize_field(self, schema, value, col, *, use_defaults=False):
        recvalue = None
        result = value

        if isinstance(value, (s_obj.ObjectSet, s_obj.ObjectList)):
            result = tuple(self._get_id(schema, v)
                           for v in value.objects(schema))
            id_array = ', '.join(ql(str(v)) for v in result)
            recvalue = dbops.Query(f'ARRAY[{id_array}]::uuid[]')

        elif isinstance(value, (s_obj.ObjectIndexBase, s_obj.ObjectDict)):
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

        elif isinstance(value, s_expr.Expression):
            ref_ids = value.refs.ids(schema)
            ref_ids_expr = ', '.join(ql(str(i)) for i in ref_ids)
            recvalue = (value.text, value.origtext,
                        dbops.Query(f'ARRAY[{ref_ids_expr}]::uuid[]'))

        if recvalue is None:
            if result is None and use_defaults:
                recvalue = dbops.Default
            else:
                recvalue = result
        elif isinstance(recvalue, types.TypeDesc):
            recvalue = dbops.Query(recvalue.to_sql_expr())

        return result, recvalue

    def get_fields(self, schema, context):
        if isinstance(self, sd.CreateObject):
            schema, fields = self._get_create_fields(schema, context)
        else:
            schema, fields = self._get_field_updates(schema, context)

        return schema, fields

    def fill_record(self, schema, context, *, use_defaults=False):
        updates = {}

        rec = None
        table = self.get_table(schema)

        schema, fields = self.get_fields(schema, context)

        for name, value in fields.items():
            col = table.get_column(name)

            v1, refqry = self._serialize_field(
                schema, value, col, use_defaults=use_defaults)

            updates[name] = v1
            if col is not None:
                if rec is None:
                    rec = table.record()
                setattr(rec, name, refqry)

        return schema, rec, updates

    def create_object(self, schema, context, scls):
        schema, rec, updates = self.fill_record(
            schema, context, use_defaults=True)
        op = dbops.Insert(
            table=self.get_table(schema),
            records=[rec],
            priority=self.op_priority)
        return schema, updates, op

    def update(self, schema, context):
        schema, updaterec, updates = self.fill_record(schema, context)

        if updaterec:
            condition = [('id', self.scls.id)]
            self.pgops.add(
                dbops.Update(
                    table=self.get_table(schema),
                    record=updaterec,
                    condition=condition,
                    priority=self.op_priority))

        return schema, updates

    def update_fields(self, schema, context, **kwargs):
        table = self.get_table(schema)
        rec = table.record()
        for k, v in kwargs.items():
            setattr(rec, k, v)

        condition = [('id', self.scls.id)]
        self.pgops.add(
            dbops.Update(
                table=self.get_table(schema),
                record=rec,
                condition=condition,
                priority=self.op_priority))

    def rename(self, schema, orig_schema, context, obj):
        table = self.get_table(schema)
        new_name = obj.get_name(schema)
        old_name = obj.get_name(orig_schema)
        updaterec = table.record(name=str(new_name))
        condition = [('name', str(old_name))]
        self.pgops.add(
            dbops.Update(
                table=table, record=updaterec, condition=condition))

    def delete(self, schema, context, scls):
        table = self.get_table(schema)
        self.pgops.add(
            dbops.Delete(
                table=table, condition=[('id', scls.id)]))


class CreateObject(ObjectMetaCommand):
    def apply(self, schema, context):
        schema, obj = self.__class__.get_adaptee().apply(self, schema, context)
        schema, _ = ObjectMetaCommand.apply(self, schema, context)
        schema, updates, op = self.create_object(schema, context, obj)
        self.pgops.add(op)
        self.updates = updates
        return schema, obj


class RenameObject(ObjectMetaCommand):
    def apply(self, schema, context):
        ctx = context.get(sd.ObjectCommandContext)
        schema, obj = self.__class__.get_adaptee().apply(self, schema, context)
        schema, _ = ObjectMetaCommand.apply(self, schema, context)
        self.rename(schema, ctx.original_schema, context, obj)
        return schema, obj


class RebaseObject(ObjectMetaCommand):
    def apply(self, schema, context):
        schema, obj = self.__class__.get_adaptee().apply(self, schema, context)
        schema, _ = ObjectMetaCommand.apply(self, schema, context)
        schema, self.updates = self.update(schema, context)
        return schema, obj


class AlterObject(ObjectMetaCommand):
    def apply(self, schema, context):
        schema, _ = ObjectMetaCommand.apply(self, schema, context)
        schema, self.scls = self.__class__.get_adaptee().apply(
            self, schema, context)
        schema, self.updates = self.update(schema, context)
        return schema, self.scls


class DeleteObject(ObjectMetaCommand):
    def apply(self, schema, context):
        schema, obj = self.__class__.get_adaptee().apply(self, schema, context)
        schema, _ = ObjectMetaCommand.apply(self, schema, context)
        self.delete(schema, context, obj)
        return schema, obj


class AlterObjectProperty(MetaCommand, adapts=sd.AlterObjectProperty):
    pass


class TupleCommand(ObjectMetaCommand):

    pass


class CreateTuple(TupleCommand, adapts=s_types.CreateTuple):

    def apply(self, schema, context):
        schema, self.scls = self.__class__.get_adaptee().apply(
            self, schema, context)
        schema, _ = TupleCommand.apply(self, schema, context)

        elements = self.scls.get_element_types(schema).items(schema)

        ctype = dbops.CompositeType(
            name=common.get_backend_name(schema, self.scls, catenate=False),
            columns=[
                dbops.Column(
                    name=n,
                    type=qt(types.pg_type_from_object(
                        schema, t, persistent_tuples=True)),
                )
                for n, t in elements
            ]
        )

        self.pgops.add(dbops.CreateCompositeType(type=ctype))

        return schema, self.scls


class DeleteTuple(TupleCommand, adapts=s_types.DeleteTuple):

    def apply(self, schema, context):
        tup = schema.get_global(s_types.SchemaTuple, self.classname)

        self.pgops.add(dbops.DropCompositeType(
            name=common.get_backend_name(schema, tup, catenate=False),
            priority=2,
        ))

        schema, self.scls = self.__class__.get_adaptee().apply(
            self, schema, context)
        schema, _ = TupleCommand.apply(self, schema, context)

        return schema, self.scls


class TupleExprAliasCommand(ObjectMetaCommand):

    _table = metaschema.get_metaclass_table(s_types.TupleExprAlias)

    def get_table(self, schema):
        return self._table


class CreateTupleExprAlias(
        TupleExprAliasCommand, CreateObject,
        adapts=s_types.CreateTupleExprAlias):

    pass


class DeleteTupleExprAlias(
        TupleExprAliasCommand, DeleteObject,
        adapts=s_types.DeleteTupleExprAlias):

    pass


class ArrayCommand(ObjectMetaCommand):

    pass


class CreateArray(ArrayCommand, adapts=s_types.CreateArray):

    def apply(self, schema, context):
        schema, self.scls = self.__class__.get_adaptee().apply(
            self, schema, context)
        schema, _ = ArrayCommand.apply(self, schema, context)
        return schema, self.scls


class DeleteArray(ArrayCommand, adapts=s_types.DeleteArray):

    def apply(self, schema, context):
        schema, self.scls = self.__class__.get_adaptee().apply(
            self, schema, context)
        schema, _ = ArrayCommand.apply(self, schema, context)
        return schema, self.scls


class ArrayExprAliasCommand(ObjectMetaCommand):

    _table = metaschema.get_metaclass_table(s_types.ArrayExprAlias)

    def get_table(self, schema):
        return self._table


class CreateArrayExprAlias(
        ArrayExprAliasCommand, CreateObject,
        adapts=s_types.CreateArrayExprAlias):

    pass


class DeleteArrayExprAlias(
        ArrayExprAliasCommand, DeleteObject,
        adapts=s_types.DeleteArrayExprAlias):

    pass


class ParameterCommand(sd.ObjectCommand,
                       metaclass=ReferencedObjectCommandMeta):

    _table = metaschema.get_metaclass_table(s_funcs.Parameter)

    def get_table(self, schema):
        return self._table


class CreateParameter(ParameterCommand, CreateObject,
                      adapts=s_funcs.CreateParameter):

    pass


class DeleteParameter(ParameterCommand, DeleteObject,
                      adapts=s_funcs.DeleteParameter):

    pass


class FunctionCommand:
    _table = metaschema.get_metaclass_table(s_funcs.Function)

    def get_table(self, schema):
        return self._table

    def get_pgname(self, func: s_funcs.Function, schema):
        return common.get_backend_name(schema, func, catenate=False)

    def get_pgtype(self, func: s_funcs.Function, obj, schema):
        if obj.is_any():
            return ('anyelement',)

        try:
            return types.pg_type_from_object(
                schema, obj, persistent_tuples=True)
        except ValueError:
            raise errors.QueryError(
                f'could not compile parameter type {obj!r} '
                f'of function {func.get_shortname(schema)}',
                context=self.source_context) from None

    def compile_default(self, func: s_funcs.Function,
                        default: s_expr.Expression, schema):
        try:
            comp = s_expr.Expression.compiled(
                default,
                schema=schema,
                as_fragment=True,
            )

            ir = comp.irast
            if not irutils.is_const(ir.expr):
                raise ValueError('expression not constant')

            sql_tree = compiler.compile_ir_to_sql_tree(
                ir.expr, singleton_mode=True)
            return codegen.SQLSourceGenerator.to_source(sql_tree)

        except Exception as ex:
            raise errors.QueryError(
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


class CreateFunction(FunctionCommand, CreateObject,
                     adapts=s_funcs.CreateFunction):

    def make_function(self, func: s_funcs.Function, code, schema):
        func_return_typemod = func.get_return_typemod(schema)
        func_params = func.get_params(schema)
        return dbops.Function(
            name=self.get_pgname(func, schema),
            args=self.compile_args(func, schema),
            has_variadic=func_params.find_variadic(schema) is not None,
            set_returning=func_return_typemod is ql_ft.TypeModifier.SET_OF,
            volatility=func.get_volatility(schema),
            returns=self.get_pgtype(
                func, func.get_return_type(schema), schema),
            text=code)

    def compile_sql_function(self, func: s_funcs.Function, schema):
        return self.make_function(func, func.get_code(schema), schema)

    def compile_edgeql_function(self, func: s_funcs.Function, schema):
        body_ir = ql_compiler.compile_func_to_ir(func, schema)

        sql_text, _ = compiler.compile_ir_to_sql(
            body_ir,
            ignore_shapes=True,
            explicit_top_cast=irtyputils.type_to_typeref(
                schema, func.get_return_type(schema)),
            output_format=compiler.OutputFormat.NATIVE,
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
            raise errors.QueryError(
                f'cannot compile function {func.get_shortname(schema)}: '
                f'unsupported language {func_language}',
                context=self.source_context)

        op = dbops.CreateFunction(dbf)
        self.pgops.add(op)
        return schema, func


class RenameFunction(
        FunctionCommand, RenameObject, adapts=s_funcs.RenameFunction):
    pass


class AlterFunction(
        FunctionCommand, AlterObject, adapts=s_funcs.AlterFunction):
    pass


class DeleteFunction(
        FunctionCommand, DeleteObject, adapts=s_funcs.DeleteFunction):

    def apply(self, schema, context):
        orig_schema = schema
        schema, func = super().apply(schema, context)

        if func.get_code(orig_schema):
            # An EdgeQL or a SQL function
            # (not just an alias to a SQL function).

            variadic = func.get_params(orig_schema).find_variadic(orig_schema)
            self.pgops.add(
                dbops.DropFunction(
                    name=self.get_pgname(func, orig_schema),
                    args=self.compile_args(func, orig_schema),
                    has_variadic=variadic is not None,
                )
            )

        return schema, func


class OperatorCommand(FunctionCommand):
    _table = metaschema.get_metaclass_table(s_opers.Operator)

    def get_table(self, schema):
        return self._table

    def get_pg_name(self, schema,
                    oper: s_opers.Operator) -> Tuple[str, str]:
        return common.get_backend_name(schema, oper, catenate=False)

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

    def compile_args(self, oper: s_opers.Operator, schema):
        args = []
        oper_params = oper.get_params(schema)
        pg_params = s_funcs.PgParams.from_params(schema, oper_params)

        for param in pg_params.params:
            pg_at = self.get_pgtype(oper, param.get_type(schema), schema)
            args.append((param.get_shortname(schema), pg_at))

        return args

    def make_operator_function(self, oper: s_opers.Operator, schema):
        return dbops.Function(
            name=common.get_backend_name(
                schema, oper, catenate=False, aspect='function'),
            args=self.compile_args(oper, schema),
            volatility=oper.get_volatility(schema),
            returns=self.get_pgtype(
                oper, oper.get_return_type(schema), schema),
            text=oper.get_code(schema))


class CreateOperator(OperatorCommand, CreateObject,
                     adapts=s_opers.CreateOperator):

    def apply(self, schema, context):
        schema, oper = super().apply(schema, context)
        if oper.get_is_abstract(schema):
            return schema, oper

        oper_language = oper.get_language(schema)
        oper_fromop = oper.get_from_operator(schema)
        oper_fromfunc = oper.get_from_function(schema)
        oper_code = oper.get_code(schema)

        if oper_language is ql_ast.Language.SQL and oper_fromop:
            pg_oper_name = oper_fromop[0]
            args = self.get_pg_operands(schema, oper)
            if len(oper_fromop) > 1:
                # Explicit operand types given in FROM SQL OPERATOR.
                from_args = oper_fromop[1:]
            else:
                from_args = args

            if oper_code:
                oper_func = self.make_operator_function(oper, schema)
                self.pgops.add(dbops.CreateFunction(oper_func))
                oper_func_name = common.qname(*oper_func.name)

            elif oper_fromfunc:
                oper_func_name = oper_fromfunc

            elif from_args != args:
                # Need a proxy function with casts
                oper_kind = oper.get_operator_kind(schema)

                if oper_kind is ql_ft.OperatorKind.INFIX:
                    op = (f'$1::{from_args[0]} {pg_oper_name} '
                          f'$2::{from_args[1]}')
                elif oper_kind is ql_ft.OperatorKind.POSTFIX:
                    op = f'$1::{from_args[0]} {pg_oper_name}'
                elif oper_kind is ql_ft.OperatorKind.PREFIX:
                    op = f'{pg_oper_name} $1::{from_args[1]}'
                else:
                    raise RuntimeError(
                        f'unexpected operator kind: {oper_kind!r}')

                rtype = self.get_pgtype(
                    oper, oper.get_return_type(schema), schema)

                oper_func = dbops.Function(
                    name=common.get_backend_name(
                        schema, oper, catenate=False, aspect='function'),
                    args=[(None, a) for a in args if a],
                    volatility=oper.get_volatility(schema),
                    returns=rtype,
                    text=f'SELECT ({op})::{qt(rtype)}',
                )

                self.pgops.add(dbops.CreateFunction(oper_func))
                oper_func_name = common.qname(*oper_func.name)

            else:
                oper_func_name = None

            params = oper.get_params(schema)

            if (not params.has_polymorphic(schema) or
                    all(p.get_type(schema).is_array()
                        for p in params.objects(schema))):
                self.pgops.add(dbops.CreateOperatorAlias(
                    name=self.get_pg_name(schema, oper),
                    args=args,
                    procedure=oper_func_name,
                    operator=('pg_catalog', pg_oper_name),
                    operator_args=from_args,
                ))

        elif oper_language is ql_ast.Language.SQL and oper_code:
            args = self.get_pg_operands(schema, oper)
            oper_func = self.make_operator_function(oper, schema)
            self.pgops.add(dbops.CreateFunction(oper_func))
            oper_func_name = common.qname(*oper_func.name)

            self.pgops.add(dbops.CreateOperator(
                name=self.get_pg_name(schema, oper),
                args=args,
                procedure=oper_func_name,
            ))

        elif oper.get_from_expr(schema):
            # This operator is handled by the compiler and does not
            # need explicit representation in the backend.
            pass

        else:
            raise errors.QueryError(
                f'cannot create operator {oper.get_shortname(schema)}: '
                f'only "FROM SQL" and "FROM SQL OPERATOR" operators '
                f'are currently supported',
                context=self.source_context)

        return schema, oper


class RenameOperator(
        OperatorCommand, RenameObject, adapts=s_opers.RenameOperator):
    pass


class AlterOperator(
        OperatorCommand, AlterObject, adapts=s_opers.AlterOperator):
    pass


class DeleteOperator(
        OperatorCommand, DeleteObject, adapts=s_opers.DeleteOperator):

    def apply(self, schema, context):
        orig_schema = schema
        oper = schema.get(self.classname)

        if oper.get_is_abstract(schema):
            return super().apply(schema, context)

        name = self.get_pg_name(schema, oper)
        args = self.get_pg_operands(schema, oper)

        schema, oper = super().apply(schema, context)
        if not oper.get_from_expr(orig_schema):
            self.pgops.add(dbops.DropOperator(name=name, args=args))
        return schema, oper


class CastCommand:
    _table = metaschema.get_metaclass_table(s_casts.Cast)

    def get_table(self, schema):
        return self._table

    def make_cast_function(self, cast: s_casts.Cast, schema):
        name = common.get_backend_name(
            schema, cast, catenate=False, aspect='function')

        args = [(
            'val',
            types.pg_type_from_object(schema, cast.get_from_type(schema))
        )]

        returns = types.pg_type_from_object(schema, cast.get_to_type(schema))

        return dbops.Function(
            name=name,
            args=args,
            returns=returns,
            text=cast.get_code(schema),
        )


class CreateCast(CastCommand, CreateObject,
                 adapts=s_casts.CreateCast):

    def apply(self, schema, context):
        schema, cast = super().apply(schema, context)
        cast_language = cast.get_language(schema)
        cast_code = cast.get_code(schema)
        from_cast = cast.get_from_cast(schema)
        from_expr = cast.get_from_expr(schema)

        if cast_language is ql_ast.Language.SQL and cast_code:
            cast_func = self.make_cast_function(cast, schema)
            self.pgops.add(dbops.CreateFunction(cast_func))

        elif from_cast is not None or from_expr is not None:
            # This operator is handled by the compiler and does not
            # need explicit representation in the backend.
            pass

        else:
            raise errors.QueryError(
                f'cannot create cast: '
                f'only "FROM SQL" and "FROM SQL FUNCTION" casts '
                f'are currently supported',
                context=self.source_context)

        return schema, cast


class RenameCast(
        CastCommand, RenameObject, adapts=s_casts.RenameCast):
    pass


class AlterCast(
        CastCommand, AlterObject, adapts=s_casts.AlterCast):
    pass


class DeleteCast(
        CastCommand, DeleteObject, adapts=s_casts.DeleteCast):

    def apply(self, schema, context):
        cast = schema.get(self.classname)
        cast_language = cast.get_language(schema)
        cast_code = cast.get_code(schema)

        schema, cast = super().apply(schema, context)

        if cast_language is ql_ast.Language.SQL and cast_code:
            cast_func = self.make_cast_function(cast, schema)
            self.pgops.add(dbops.DropFunction(
                cast_func.name, cast_func.args))

        return schema, cast


class AnnotationCommand:
    _table = metaschema.get_metaclass_table(s_anno.Annotation)

    def get_table(self, schema):
        return self._table


class CreateAnnotation(
        AnnotationCommand, CreateObject,
        adapts=s_anno.CreateAnnotation):
    op_priority = 1


class AlterAnnotation(
        AnnotationCommand, AlterObject, adapts=s_anno.AlterAnnotation):
    pass


class DeleteAnnotation(
        AnnotationCommand, DeleteObject,
        adapts=s_anno.DeleteAnnotation):
    pass


class AnnotationValueCommand(sd.ObjectCommand,
                             metaclass=ReferencedObjectCommandMeta):
    _table = metaschema.get_metaclass_table(s_anno.AnnotationValue)
    op_priority = 4

    def get_table(self, schema):
        return self._table


class CreateAnnotationValue(
        AnnotationValueCommand, CreateObject,
        adapts=s_anno.CreateAnnotationValue):
    pass


class AlterAnnotationValue(
        AnnotationValueCommand, AlterObject,
        adapts=s_anno.AlterAnnotationValue):
    pass


class DeleteAnnotationValue(
        AnnotationValueCommand, DeleteObject,
        adapts=s_anno.DeleteAnnotationValue):
    pass


class ConstraintCommand(sd.ObjectCommand,
                        metaclass=ReferencedObjectCommandMeta):
    _table = metaschema.get_metaclass_table(s_constr.Constraint)
    op_priority = 3

    def get_table(self, schema):
        return self._table

    def constraint_is_effective(self, schema, constraint):
        is_local = constraint.get_is_local(schema)
        delegated_from_parent = any(
            b.get_delegated(schema)
            for b in constraint.get_bases(schema).objects(schema)
        )
        return is_local or delegated_from_parent


class CreateConstraint(
        ConstraintCommand, CreateObject,
        adapts=s_constr.CreateConstraint):
    def apply(self, schema, context):
        schema, constraint = super().apply(schema, context)
        if not self.constraint_is_effective(schema, constraint):
            return schema, constraint

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
        ConstraintCommand, RenameObject,
        adapts=s_constr.RenameConstraint):
    # Constraints are created using the ID in the backend,
    # so there is nothing special we need to do here.
    pass


class AlterConstraint(
        ConstraintCommand, AlterObject,
        adapts=s_constr.AlterConstraint):
    def _alter_finalize(self, schema, context, constraint):
        schema = super()._alter_finalize(schema, context, constraint)
        if not self.constraint_is_effective(schema, constraint):
            return schema

        subject = constraint.get_subject(schema)

        subcommands = list(self.get_subcommands())
        if (not subcommands or
                isinstance(subcommands[0], s_constr.RenameConstraint)):
            # This is a pure rename, so everything had been handled by
            # RenameConstraint above.
            return schema

        if subject is not None:
            schemac_to_backendc = \
                schemamech.ConstraintMech.\
                schema_constraint_to_backend_constraint

            bconstr = schemac_to_backendc(subject, constraint, schema)

            delta_root_ctx = context.top()
            orig_schema = delta_root_ctx.original_schema
            orig_bconstr = schemac_to_backendc(
                constraint.get_subject(orig_schema),
                constraint, orig_schema)

            op = dbops.CommandGroup(priority=1)
            if not self.constraint_is_effective(orig_schema, constraint):
                op.add_command(bconstr.create_ops())
            else:
                op.add_command(bconstr.alter_ops(orig_bconstr))
            self.pgops.add(op)

        return schema


class DeleteConstraint(
        ConstraintCommand, DeleteObject,
        adapts=s_constr.DeleteConstraint):
    def apply(self, schema, context):
        constraint = schema.get(self.classname)
        if self.constraint_is_effective(schema, constraint):
            subject = constraint.get_subject(schema)

            if subject is not None:
                schemac_to_backendc = \
                    schemamech.ConstraintMech.\
                    schema_constraint_to_backend_constraint
                bconstr = schemac_to_backendc(subject, constraint, schema)

                op = dbops.CommandGroup()
                op.add_command(bconstr.delete_ops())
                self.pgops.add(op)

        schema, _ = super().apply(schema, context)
        return schema, constraint


class RebaseConstraint(
        ConstraintCommand, RebaseObject,
        adapts=s_constr.RebaseConstraint):
    pass


class AliasCapableObjectMetaCommand(ObjectMetaCommand):
    pass


class ScalarTypeMetaCommand(AliasCapableObjectMetaCommand):
    _table = metaschema.get_metaclass_table(s_scalars.ScalarType)

    def get_table(self, schema):
        return self._table

    def is_sequence(self, schema, scalar):
        seq = schema.get('std::sequence', default=None)
        return seq is not None and scalar.issubclass(schema, seq)

    def alter_scalar_type(self, scalar, schema, new_type, intent):

        users = []

        for link in schema.get_objects(type=s_links.Link):
            if (link.get_target(schema) and
                    link.get_target(schema).get_name(schema) ==
                    scalar.get_name(schema)):
                users.append((link.get_source(schema), link))

        domain_name = common.get_backend_name(
            schema, scalar, catenate=False)

        new_constraints = scalar.get_constraints(schema)
        base = types.get_scalar_base(schema, scalar)

        target_type = new_type

        schemac_to_backendc = \
            schemamech.ConstraintMech.\
            schema_constraint_to_backend_constraint

        if intent == 'alter':
            new_name = domain_name[0], domain_name[1] + '_tmp'
            self.pgops.add(dbops.RenameDomain(domain_name, new_name))
            target_type = domain_name

            domain = dbops.Domain(name=domain_name, base=new_type)
            self.pgops.add(dbops.CreateDomain(domain=domain))

            for constraint in new_constraints.objects(schema):
                bconstr = schemac_to_backendc(scalar, constraint, schema)
                op = dbops.CommandGroup(priority=1)
                op.add_command(bconstr.create_ops())
                self.pgops.add(op)

            domain_name = new_name

        elif intent == 'create':
            domain = dbops.Domain(name=domain_name, base=base)
            self.pgops.add(dbops.CreateDomain(domain=domain))

        for _host_class, item_class in users:
            ptr_stor_info = types.get_pointer_storage_info(
                item_class, schema=schema)

            alter_type = dbops.AlterTableAlterColumnType(
                ptr_stor_info.column_name, target_type)
            alter_table = dbops.AlterTable(ptr_stor_info.table_name)
            alter_table.add_operation(alter_type)
            self.pgops.add(alter_table)

        for child_scalar in schema.get_objects(type=s_scalars.ScalarType):
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

        schema, updates, op = self.create_object(schema, context, scalar)
        self.pgops.add(op)

        if scalar.get_is_abstract(schema):
            return schema, scalar

        new_domain_name = types.pg_type_from_scalar(schema, scalar)

        if types.is_builtin_scalar(schema, scalar):
            self.update_fields(
                schema, context, backend_id=dbops.type_oid(new_domain_name))
            return schema, scalar

        enum_values = scalar.get_enum_values(schema)
        if enum_values:
            new_enum_name = common.get_backend_name(
                schema, scalar, catenate=False)
            self.pgops.add(dbops.CreateEnum(
                name=new_enum_name, values=enum_values))
            base = q(*new_enum_name)
            self.update_fields(
                schema, context, backend_id=dbops.type_oid(new_enum_name))

        else:
            base = types.get_scalar_base(schema, scalar)

            if self.is_sequence(schema, scalar):
                seq_name = common.get_backend_name(
                    schema, scalar, catenate=False, aspect='sequence')
                self.pgops.add(dbops.CreateSequence(name=seq_name))

            domain = dbops.Domain(name=new_domain_name, base=base)
            self.pgops.add(dbops.CreateDomain(domain=domain))

            default = updates.get('default')
            if default:
                if (default is not None
                        and not isinstance(default, s_expr.Expression)):
                    # We only care to support literal defaults here. Supporting
                    # defaults based on queries has no sense on the database
                    # level since the database forbids queries for DEFAULT and
                    # pre- calculating the value does not make sense either
                    # since the whole point of query defaults is for them to be
                    # dynamic.
                    self.pgops.add(
                        dbops.AlterDomainAlterDefault(
                            name=new_domain_name, default=default))

            self.update_fields(
                schema, context, backend_id=dbops.type_oid(new_domain_name))

        return schema, scalar


class RenameScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.RenameScalarType):
    def apply(self, schema, context=None):
        schema, scls = s_scalars.RenameScalarType.apply(self, schema, context)
        schema, _ = ScalarTypeMetaCommand.apply(self, schema, context)

        ctx = context.get(s_scalars.ScalarTypeCommandContext)
        orig_schema = ctx.original_schema

        domain_name = common.get_backend_name(
            orig_schema, scls, catenate=False)

        new_domain_name = common.get_backend_name(
            schema, scls, catenate=False)

        if scls.is_enum(schema):
            enum_name = common.get_backend_name(
                orig_schema, scls, catenate=False, aspect='enum')
            new_enum_name = common.get_backend_name(
                schema, scls, catenate=False, aspect='enum')
            self.pgops.add(
                dbops.RenameEnum(name=enum_name, new_name=new_enum_name))

        else:
            self.pgops.add(
                dbops.RenameDomain(name=domain_name, new_name=new_domain_name))

        self.rename(schema, orig_schema, context, scls)

        if self.is_sequence(schema, scls):
            seq_name = common.get_backend_name(
                orig_schema, scls, catenate=False, aspect='sequence')
            new_seq_name = common.get_backend_name(
                schema, scls, catenate=False, aspect='sequence')

            if seq_name != new_seq_name:
                self.pgops.add(
                    dbops.RenameSequence(name=seq_name, new_name=new_seq_name))

        return schema, scls


class RebaseScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.RebaseScalarType):
    def apply(self, schema, context):
        # Actual rebase is taken care of in AlterScalarType
        schema, _ = ScalarTypeMetaCommand.apply(self, schema, context)
        return s_scalars.RebaseScalarType.apply(self, schema, context)


class AlterScalarType(ScalarTypeMetaCommand, adapts=s_scalars.AlterScalarType):
    def apply(self, schema, context=None):
        orig_schema = schema
        table = self.get_table(schema)
        schema, new_scalar = s_scalars.AlterScalarType.apply(
            self, schema, context)
        schema, _ = ScalarTypeMetaCommand.apply(self, schema, context)

        schema, updaterec, updates = self.fill_record(schema, context)

        if updaterec:
            condition = [('id', new_scalar.id)]
            self.pgops.add(
                dbops.Update(
                    table=table, record=updaterec, condition=condition))

        old_enum_values = new_scalar.get_enum_values(orig_schema)
        new_enum_values = new_scalar.get_enum_values(schema)

        if new_enum_values:
            type_name = common.get_backend_name(
                schema, new_scalar, catenate=False)

            if old_enum_values != new_enum_values:
                old_idx = 0
                old_enum_values = list(old_enum_values)
                for v in new_enum_values:
                    if old_idx >= len(old_enum_values):
                        self.pgops.add(
                            dbops.AlterEnumAddValue(
                                type_name, v,
                            )
                        )
                    elif v != old_enum_values[old_idx]:
                        self.pgops.add(
                            dbops.AlterEnumAddValue(
                                type_name, v, before=old_enum_values[old_idx],
                            )
                        )
                        old_enum_values.insert(old_idx, v)
                    else:
                        old_idx += 1

        if updates:
            default_delta = updates.get('default')
            if default_delta:
                if (default_delta is None or
                        isinstance(default_delta, s_expr.Expression)):
                    new_default = None
                else:
                    new_default = default_delta

                domain_name = common.get_backend_name(
                    schema, new_scalar, catenate=False)
                adad = dbops.AlterDomainAlterDefault(
                    name=domain_name, default=new_default)
                self.pgops.add(adad)

        return schema, new_scalar


class DeleteScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.DeleteScalarType):
    def apply(self, schema, context=None):
        orig_schema = schema
        schema, scalar = s_scalars.DeleteScalarType.apply(
            self, schema, context)
        schema, _ = ScalarTypeMetaCommand.apply(self, schema, context)

        link = None
        if context:
            link = context.get(s_links.LinkCommandContext)

        ops = link.op.pgops if link else self.pgops

        old_domain_name = common.get_backend_name(
            orig_schema, scalar, catenate=False)

        # Domain dropping gets low priority since other things may
        # depend on it.
        table = self.get_table(schema)

        if scalar.is_enum(orig_schema):
            old_enum_name = common.get_backend_name(
                orig_schema, scalar, catenate=False)
            cond = dbops.EnumExists(old_enum_name)
            ops.add(
                dbops.DropEnum(
                    name=old_enum_name, conditions=[cond], priority=3))
        else:
            cond = dbops.DomainExists(old_domain_name)
            ops.add(
                dbops.DropDomain(
                    name=old_domain_name, conditions=[cond], priority=3))

        ops.add(
            dbops.Delete(
                table=table, condition=[(
                    'name', str(self.classname))]))

        if self.is_sequence(orig_schema, scalar):
            seq_name = common.get_backend_name(
                orig_schema, scalar, catenate=False, aspect='sequence')
            self.pgops.add(dbops.DropSequence(name=seq_name))

        return schema, scalar


class CompositeObjectMetaCommand(ObjectMetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table_name = None
        self._multicommands = {}
        self.update_search_indexes = None

    def _get_multicommand(
            self, context, cmdtype, object_name, *, priority=0,
            force_new=False, manual=False, cmdkwargs=None):
        if cmdkwargs is None:
            cmdkwargs = {}
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
            tabname = common.get_backend_name(schema, ctx.scls, catenate=False)
            if table_name is None:
                self.table_name = tabname

        return self._get_multicommand(
            context, dbops.AlterTable, tabname, priority=priority,
            force_new=force_new, manual=manual,
            cmdkwargs={'contained': contained})

    def attach_alter_table(self, context):
        self._attach_multicommand(context, dbops.AlterTable)

    def rename(self, schema, orig_schema, context, obj):
        super().rename(schema, orig_schema, context, obj)

        old_table_name = common.get_backend_name(
            orig_schema, obj, catenate=False)

        new_table_name = common.get_backend_name(
            schema, obj, catenate=False)

        cond = dbops.TableExists(name=old_table_name)

        if old_table_name[0] != new_table_name[0]:
            self.pgops.add(
                dbops.AlterTableSetSchema(
                    old_table_name, new_table_name[0], conditions=(cond, )))
            old_table_name = (new_table_name[0], old_table_name[1])

            cond = dbops.TableExists(name=old_table_name)

        if old_table_name[1] != new_table_name[1]:
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

    def apply_base_delta(
            self, source, orig_schema, schema, context):
        delta_ctx = context.get(sd.DeltaRootContext)
        renames = delta_ctx.op._renames

        orig_bases = [
            b.get_name(orig_schema)
            for b in source.get_bases(orig_schema).objects(orig_schema)
        ]

        orig_bases = [renames.get(b, b) for b in orig_bases]

        new_bases = [
            b.get_name(schema)
            for b in source.get_bases(schema).objects(schema)
        ]

        dropped_bases = set(orig_bases) - set(new_bases)

        if isinstance(source, s_objtypes.ObjectType):
            source_ctx = context.get(s_objtypes.ObjectTypeCommandContext)
            ptr_cmd = s_links.CreateLink
        else:
            source_ctx = context.get(s_links.LinkCommandContext)
            ptr_cmd = s_props.CreateProperty

        alter_table = source_ctx.op.get_alter_table(
            schema, context, force_new=True)

        if (isinstance(source, s_objtypes.ObjectType) or
                source_ctx.op.has_table(source, schema)):

            created_ptrs = set()
            for ptr in source_ctx.op.get_subcommands(type=ptr_cmd):
                created_ptrs.add(ptr.classname)

            inherited_aptrs = set()

            for base in source.get_bases(schema).objects(schema):
                for ptr in base.get_pointers(schema).objects(schema):
                    if ptr.scalar():
                        inherited_aptrs.add(ptr.get_shortname(schema))

            added_inh_ptrs = inherited_aptrs - {
                p.get_shortname(orig_schema)
                for p in source.get_pointers(orig_schema).objects(orig_schema)
            }

            ptrs = source.get_pointers(schema)
            for added_ptr in added_inh_ptrs - created_ptrs:
                ptr = ptrs.get(schema, added_ptr.name)
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
                    parent_table_name = common.get_backend_name(
                        orig_schema, orig_schema.get(dropped_base),
                        catenate=False)
                    op = dbops.AlterTableDropParent(
                        parent_name=parent_table_name)
                    alter_table_drop_parent.add_operation(op)

                orig_ptrs = source.get_pointers(orig_schema)
                dropped_ptrs = (
                    set(orig_ptrs.keys(orig_schema)) -
                    set(ptrs.keys(schema))
                )

                if dropped_ptrs:
                    alter_table_drop_ptr = source_ctx.op.get_alter_table(
                        schema, context, force_new=True)

                    for dropped_ptr in dropped_ptrs:
                        ptr = orig_ptrs.get(orig_schema, dropped_ptr)
                        ptr_stor_info = types.get_pointer_storage_info(
                            ptr, schema=orig_schema)

                        is_a_column = ((
                            ptr_stor_info.table_type == 'ObjectType' and
                            isinstance(source, s_objtypes.ObjectType)) or (
                                ptr_stor_info.table_type == 'link' and
                                isinstance(source, s_links.Link)))

                        if is_a_column:
                            col = dbops.Column(
                                name=ptr_stor_info.column_name,
                                type=common.qname(*ptr_stor_info.column_type),
                                required=ptr.get_required(orig_schema))

                            cond = dbops.ColumnExists(
                                table_name=ptr_stor_info.table_name,
                                column_name=ptr_stor_info.column_name)
                            op = dbops.AlterTableDropColumn(col)
                            alter_table_drop_ptr.add_command(
                                (op, (cond, ), ()))

            current_bases = [b for b in orig_bases if b not in dropped_bases]

            unchanged_order = list(
                itertools.takewhile(
                    lambda x: x[0] == x[1], zip(current_bases, new_bases)))

            old_base_order = current_bases[len(unchanged_order):]
            new_base_order = new_bases[len(unchanged_order):]

            if new_base_order:
                table_name = common.get_backend_name(
                    schema, source, catenate=False)
                alter_table_drop_parent = source_ctx.op.get_alter_table(
                    schema, context, force_new=True)
                alter_table_add_parent = source_ctx.op.get_alter_table(
                    schema, context, force_new=True)

                for base in old_base_order:
                    parent_table_name = common.get_backend_name(
                        schema, schema.get(base), catenate=False)
                    cond = dbops.TableInherits(table_name, parent_table_name)
                    op = dbops.AlterTableDropParent(
                        parent_name=parent_table_name)
                    alter_table_drop_parent.add_operation((op, [cond], None))

                for added_base in new_base_order:
                    parent_table_name = common.get_backend_name(
                        schema, schema.get(added_base), catenate=False)
                    cond = dbops.TableInherits(table_name, parent_table_name)
                    op = dbops.AlterTableAddParent(
                        parent_name=parent_table_name)
                    alter_table_add_parent.add_operation((op, None, [cond]))

        return schema


class IndexCommand(sd.ObjectCommand, metaclass=ReferencedObjectCommandMeta):
    _table = metaschema.get_metaclass_table(s_indexes.Index)

    def get_table(self, schema):
        return self._table


class CreateIndex(IndexCommand, CreateObject, adapts=s_indexes.CreateIndex):

    def apply(self, schema, context):
        schema, index = CreateObject.apply(self, schema, context)
        if not index.get_is_local(schema):
            return schema, index

        parent_ctx = context.get_ancestor(
            s_indexes.IndexSourceCommandContext, self)
        subject_name = parent_ctx.op.classname
        subject = schema.get(subject_name, default=None)
        if not isinstance(subject, s_pointers.Pointer):
            singletons = [subject]
            path_prefix_anchor = ql_ast.Subject
        else:
            singletons = []
            path_prefix_anchor = None

        index_expr = index.get_expr(schema)
        ir = index_expr.irast
        if ir is None:
            index_expr = type(index_expr).compiled(
                index_expr,
                schema=schema,
                modaliases=context.modaliases,
                parent_object_type=self.get_schema_metaclass(),
                anchors={ql_ast.Subject: subject},
                path_prefix_anchor=path_prefix_anchor,
                singletons=singletons,
            )
            ir = index_expr.irast

        table_name = common.get_backend_name(
            schema, subject, catenate=False)

        sql_tree = compiler.compile_ir_to_sql_tree(
            ir.expr, singleton_mode=True)
        sql_expr = codegen.SQLSourceGenerator.to_source(sql_tree)

        if isinstance(sql_tree, pg_ast.ImplicitRowExpr):
            # Trim the parentheses to avoid PostgreSQL choking on double
            # parentheses. since it expects only a single set around the column
            # list.
            sql_expr = sql_expr[1:-1]

        module = schema.get_global(s_mod.Module, index.get_name(schema).module)
        index_name = common.get_index_backend_name(
            index.id, module.id, catenate=False)
        pg_index = dbops.Index(
            name=index_name[1], table_name=table_name, expr=sql_expr,
            unique=False, inherit=True,
            metadata={'schemaname': index.get_name(schema)})
        self.pgops.add(dbops.CreateIndex(pg_index, priority=3))

        return schema, index


class RenameIndex(IndexCommand, RenameObject, adapts=s_indexes.RenameIndex):

    def apply(self, schema, context):
        schema, index = s_indexes.RenameIndex.apply(
            self, schema, context)
        schema, _ = RenameObject.apply(self, schema, context)

        subject = context.get(s_links.LinkCommandContext)
        if not subject:
            subject = context.get(s_objtypes.ObjectTypeCommandContext)
        orig_table_name = common.get_backend_name(
            subject.original_schema, index, catenate=False)

        index_ctx = context.get(s_indexes.IndexCommandContext)

        orig_schema = index_ctx.original_schema
        module = schema.get_global(
            s_mod.Module, index.get_name(orig_schema).module)
        orig_idx_name = common.get_index_backend_name(
            index.id, module.id, catenate=False)

        new_index_name = orig_idx_name

        orig_pg_idx = dbops.Index(
            name=orig_idx_name[1], table_name=orig_table_name, inherit=True,
            metadata={'schemaname': index.get_name(schema)})

        rename = dbops.RenameIndex(orig_pg_idx, new_name=new_index_name[1])
        self.pgops.add(rename)

        return schema, index


class AlterIndex(IndexCommand, AlterObject, adapts=s_indexes.AlterIndex):
    pass


class DeleteIndex(IndexCommand, DeleteObject, adapts=s_indexes.DeleteIndex):

    def apply(self, schema, context=None):
        orig_schema = schema
        schema, index = DeleteObject.apply(self, schema, context)

        source = context.get(s_links.LinkCommandContext)
        if not source:
            source = context.get(s_objtypes.ObjectTypeCommandContext)

        if not isinstance(source.op, sd.DeleteObject):
            # We should not drop indexes when the host is being dropped since
            # the indexes are dropped automatically in this case.
            #
            table_name = common.get_backend_name(
                schema, source.scls, catenate=False)
            module = schema.get_global(
                s_mod.Module, index.get_name(orig_schema).module)
            orig_idx_name = common.get_index_backend_name(
                index.id, module.id, catenate=False)
            index = dbops.Index(
                name=orig_idx_name[1], table_name=table_name, inherit=True)
            index_exists = dbops.IndexExists(
                (table_name[0], index.name_in_catalog))
            self.pgops.add(
                dbops.DropIndex(
                    index, priority=3, conditions=(index_exists, )))

        return schema, index


class RebaseIndex(
        IndexCommand, RebaseObject,
        adapts=s_indexes.RebaseIndex):
    pass


class ObjectTypeMetaCommand(AliasCapableObjectMetaCommand,
                            CompositeObjectMetaCommand):
    def get_table(self, schema):
        if self.scls.get_union_of(schema):
            mcls = s_objtypes.DerivedObjectType
        else:
            mcls = s_objtypes.ObjectType

        return metaschema.get_metaclass_table(mcls)

    @classmethod
    def has_table(cls, objtype, schema):
        return not (
            objtype.get_union_of(schema) or
            objtype.get_is_derived(schema) or
            objtype.is_view(schema)
        )


class CreateObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.CreateObjectType):
    def apply(self, schema, context=None):
        schema, objtype = s_objtypes.CreateObjectType.apply(
            self, schema, context)
        if objtype.get_union_of(schema) or objtype.get_is_derived(schema):
            schema, _ = ObjectTypeMetaCommand.apply(self, schema, context)
            schema, _, op = self.create_object(schema, context, objtype)
            self.pgops.add(op)
            return schema, objtype

        new_table_name = common.get_backend_name(
            schema, objtype, catenate=False)
        self.table_name = new_table_name

        columns = []
        if objtype.get_name(schema) == 'std::Object':
            token_col = dbops.Column(
                name='__edb_token', type='uuid', required=False)
            columns.append(token_col)

        objtype_table = dbops.Table(name=new_table_name, columns=columns)
        self.pgops.add(dbops.CreateTable(table=objtype_table))

        alter_table = self.get_alter_table(schema, context)

        schema, _ = ObjectTypeMetaCommand.apply(self, schema, context)

        schema, _, op = self.create_object(schema, context, objtype)
        self.pgops.add(op)

        if objtype.get_name(schema).module != 'schema':
            constr_name = common.edgedb_name_to_pg_name(
                self.classname + '.class_check')

            constr_expr = dbops.Query(textwrap.dedent(f"""\
                SELECT '"__type__" = ' || quote_literal(id)
                FROM edgedb.ObjectType WHERE name =
                    {ql(objtype.get_name(schema))}
            """), type='text')

            cid_constraint = dbops.CheckConstraint(
                self.table_name, constr_name, constr_expr, inherit=False)
            alter_table.add_operation(
                dbops.AlterTableAddConstraint(cid_constraint))

            cid_col = dbops.Column(
                name='__type__', type='uuid', required=True)

            if objtype.get_name(schema) == 'std::Object':
                alter_table.add_operation(dbops.AlterTableAddColumn(cid_col))

            constraint = dbops.PrimaryKey(
                table_name=alter_table.name, columns=['id'])
            alter_table.add_operation(
                dbops.AlterTableAddConstraint(constraint))

        bases = (
            dbops.Table(
                name=common.get_backend_name(schema, b, catenate=False))
            for b in objtype.get_bases(schema).objects(schema)
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

        delta_ctx = context.get(sd.DeltaRootContext)
        assert delta_ctx

        orig_name = scls.get_name(objtype.original_schema)
        delta_ctx.op._renames[orig_name] = scls.get_name(schema)

        has_table = self.has_table(scls, schema)

        if has_table:
            objtype.op.attach_alter_table(context)

        self.rename(schema, objtype.original_schema, context, scls)

        if has_table:
            new_table_name = common.get_backend_name(
                schema, scls, catenate=False)
            objtype_table = dbops.Table(name=new_table_name)
            self.pgops.add(dbops.Comment(
                object=objtype_table, text=self.new_name))

            objtype.op.table_name = new_table_name

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

            self.table_name = new_table_name

        return schema, scls


class RebaseObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.RebaseObjectType):
    def apply(self, schema, context):
        schema, result = s_objtypes.RebaseObjectType.apply(
            self, schema, context)
        schema, _ = ObjectTypeMetaCommand.apply(self, schema, context)
        schema, _ = self.update(schema, context)

        if self.has_table(result, schema):
            objtype_ctx = context.get(s_objtypes.ObjectTypeCommandContext)
            source = objtype_ctx.scls
            orig_schema = objtype_ctx.original_schema
            schema = self.apply_base_delta(
                source, orig_schema, schema, context)

        return schema, result


class AlterObjectType(ObjectTypeMetaCommand,
                      adapts=s_objtypes.AlterObjectType):
    def apply(self, schema, context=None):
        schema, objtype = s_objtypes.AlterObjectType.apply(
            self, schema, context=context)

        self.table_name = common.get_backend_name(
            schema, objtype, catenate=False)

        schema, _ = ObjectTypeMetaCommand.apply(self, schema, context)

        schema, updaterec, updates = self.fill_record(schema, context)

        if updaterec:
            table = self.get_table(schema)
            condition = [('id', objtype.id)]
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
        self.scls = objtype = schema.get(self.classname)

        old_table_name = common.get_backend_name(
            schema, objtype, catenate=False)

        schema, _ = ObjectTypeMetaCommand.apply(self, schema, context)

        self.delete(schema, context, objtype)

        if self.has_table(objtype, schema):
            self.pgops.add(dbops.DropTable(name=old_table_name, priority=3))

        schema, _ = s_objtypes.DeleteObjectType.apply(
            self, schema, context)

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

    def record_metadata(self, pointer, schema, orig_schema, context):
        old_pointer = orig_schema.get_by_id(pointer.id, None)

        schema, rec, updates = self.fill_record(
            schema, use_defaults=old_pointer is None,
            context=context)

        table = self.get_table(schema)

        if updates:
            if not rec:
                rec = table.record()

        return rec, updates

    def alter_host_table_column(self, ptr, schema, orig_schema, context):

        old_target = ptr.get_target(orig_schema)
        new_target = ptr.get_target(schema)

        alter_table = context.get(
            s_objtypes.ObjectTypeCommandContext).op.get_alter_table(
                schema, context, priority=1)

        ptr_stor_info = types.get_pointer_storage_info(ptr, schema=schema)

        if isinstance(new_target, s_scalars.ScalarType):
            target_type = types.pg_type_from_object(schema, new_target)

            if isinstance(old_target, s_scalars.ScalarType):
                alter_type = dbops.AlterTableAlterColumnType(
                    ptr_stor_info.column_name, common.qname(*target_type))
                alter_table.add_operation(alter_type)
            else:
                cols = self.get_columns(ptr, schema)
                ops = [dbops.AlterTableAddColumn(col) for col in cols]
                for op in ops:
                    alter_table.add_operation(op)
        else:
            col = dbops.Column(
                name=ptr_stor_info.column_name,
                type=ptr_stor_info.column_type)
            alter_table.add_operation(dbops.AlterTableDropColumn(col))

    def get_pointer_default(self, ptr, schema, context):
        if ptr.is_pure_computable(schema):
            return None

        default = self.updates.get('default')
        default_value = None

        if default is not None:
            if isinstance(default, s_expr.Expression):
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
                common.get_backend_name(
                    schema, ptr.get_target(schema), aspect='sequence'))
            default_value = f'nextval({seq_name}::regclass)'

        return default_value

    def alter_pointer_default(self, pointer, schema, context):
        default = self.updates.get('default')
        if default:
            default_value = self.get_pointer_default(pointer, schema, context)
            source_ctx = context.get_ancestor(
                s_sources.SourceCommandContext, self)
            alter_table = source_ctx.op.get_alter_table(
                schema, context, contained=True, priority=3)

            ptr_stor_info = types.get_pointer_storage_info(
                pointer, schema=schema)
            alter_table.add_operation(
                dbops.AlterTableAlterColumnDefault(
                    column_name=ptr_stor_info.column_name,
                    default=default_value))

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
            # before proceeding with renaming, make sure that this is
            # not a computable
            pointer = self.get_object(schema, context)
            source = pointer.get_source(schema)
            if source is not None:
                is_computable = source.get_is_derived(schema)

                # potentially this is a link property, so source may
                # be a link
                if isinstance(source, s_links.Link):
                    source = source.get_source(schema)

                if source is not None:
                    is_computable = (
                        is_computable
                        or source.is_view(schema)
                        or source.get_union_of(schema)
                    )
            else:
                is_computable = None

            old_name = sn.shortname_from_fullname(old_name)
            new_name = sn.shortname_from_fullname(new_name)

            if not is_computable:
                host = self.get_host(schema, context)

                if host and old_name != new_name:
                    if (new_name == 'std::source' and
                            not host.scls.generic(schema)):
                        pass
                    else:
                        old_col_name = common.edgedb_name_to_pg_name(
                            old_name.name)
                        new_col_name = common.edgedb_name_to_pg_name(
                            new_name.name)

                        ptr_stor_info = types.get_pointer_storage_info(
                            pointer, schema=schema)

                        is_a_column = ((
                            ptr_stor_info.table_type == 'ObjectType' and
                            isinstance(host.scls, s_objtypes.ObjectType)) or (
                                ptr_stor_info.table_type == 'link' and
                                isinstance(host.scls, s_links.Link)))

                        if is_a_column:
                            table_name = common.get_backend_name(
                                schema, host.scls, catenate=False)
                            cond = [
                                dbops.ColumnExists(
                                    table_name=table_name,
                                    column_name=old_col_name)
                            ]
                            neg_cond = [
                                dbops.ColumnIsInherited(
                                    table_name=table_name,
                                    column_name=old_col_name)
                            ]
                            rename = dbops.AlterTableRenameColumn(
                                table_name, old_col_name, new_col_name,
                                conditions=cond, neg_conditions=neg_cond)
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
            if src.has_user_defined_properties(schema):
                return True
            elif src.get_name(schema) == 'std::link':
                return True
            else:
                for l in src.children(schema):
                    if not l.generic(schema):
                        ptr_stor_info = types.get_pointer_storage_info(
                            l, resolve_type=False, schema=schema)
                        if ptr_stor_info.table_type == 'link':
                            return True
                return False
        elif src.get_is_local(schema):
            if src.is_link_property(schema):
                return not src.singular(schema)
            else:
                ptr_stor_info = types.get_pointer_storage_info(
                    src, resolve_type=False, schema=schema, link_bias=True)

                return (
                    ptr_stor_info is not None
                    and ptr_stor_info.table_type == 'link'
                )
        else:
            return False

    def create_table(self, ptr, schema, context, conditional=False):
        c = self._create_table(ptr, schema, context, conditional=conditional)
        self.pgops.add(c)

    def provide_table(self, ptr, schema, context):
        if self.has_table(ptr, schema):
            self.create_table(ptr, schema, context, conditional=True)
            return True
        else:
            return False

    def adjust_pointer_storage(self, pointer, schema, orig_schema, context):
        old_ptr_stor_info = types.get_pointer_storage_info(
            pointer, schema=orig_schema)
        new_ptr_stor_info = types.get_pointer_storage_info(
            pointer, schema=schema)

        old_target = pointer.get_target(orig_schema)
        new_target = pointer.get_target(schema)

        source_ctx = context.get_ancestor(
            s_sources.SourceCommandContext, self)
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
                    move_data = dbops.Query(textwrap.dedent(f'''\
                        INSERT INTO {q(*new_ptr_stor_info.table_name)}
                        (source, target, ptr_item_id)
                        (SELECT
                            s.id AS source,
                            s.{qi(old_ptr_stor_info.column_name)} AS target,
                            {ql(str(pointer.id))}::uuid AS ptr_item_id
                         FROM
                            {q(*old_ptr_stor_info.table_name)} AS s
                        );
                    '''))

                    opg.add_command(move_data)

                    # Moved from source table to pointer table.
                    # The pointer table has already been created by now.
                    col = dbops.Column(
                        name=old_ptr_stor_info.column_name,
                        type=common.qname(*old_ptr_stor_info.column_type))
                    at.add_command(dbops.AlterTableDropColumn(col))

                    opg.add_command(at)
                else:
                    otabname = common.get_backend_name(
                        orig_schema, pointer, catenate=False)

                    # Moved from link to object
                    cols = self.get_columns(pointer, schema)

                    for col in cols:
                        cond = dbops.ColumnExists(
                            new_ptr_stor_info.table_name, column_name=col.name)
                        op = (dbops.AlterTableAddColumn(col), None, (cond, ))
                        at.add_operation(op)

                    opg.add_command(at)

                    move_data = dbops.Query(textwrap.dedent(f'''\
                        UPDATE {q(*new_ptr_stor_info.table_name)}
                        SET {qi(new_ptr_stor_info.column_name)} = l.target
                        FROM {q(*old_ptr_stor_info.table_name)} AS l
                        WHERE id = l.source
                    '''))

                    opg.add_command(move_data)

                    if not self.has_table(pointer, schema):
                        condition = dbops.TableExists(name=otabname)
                        dt = dbops.DropTable(
                            name=otabname, conditions=[condition])

                        opg.add_command(dt)

                self.pgops.add(opg)

            else:
                if old_target != new_target and not type_change_ok:
                    if not isinstance(old_target, s_objtypes.ObjectType):
                        alter_table = source_op.get_alter_table(
                            schema, context, priority=1)

                        new_type = types.pg_type_from_object(
                            schema, new_target, persistent_tuples=True)

                        alter_type = dbops.AlterTableAlterColumnType(
                            old_ptr_stor_info.column_name,
                            common.quote_type(new_type))

                        inherited_cond = dbops.ColumnIsInherited(
                            table_name=old_ptr_stor_info.table_name,
                            column_name=old_ptr_stor_info.column_name,
                        )

                        alter_table.add_operation(
                            (alter_type, [], [inherited_cond]))


class LinkMetaCommand(CompositeObjectMetaCommand, PointerMetaCommand):
    _table = metaschema.get_metaclass_table(s_links.Link)

    def get_table(self, schema):
        return self._table

    @classmethod
    def _create_table(
            cls, link, schema, context, conditional=False, create_bases=True,
            create_children=True):
        new_table_name = common.get_backend_name(schema, link, catenate=False)

        create_c = dbops.CommandGroup()

        constraints = []
        columns = []

        src_col = 'source'
        tgt_col = 'target'

        if link.get_name(schema) == 'std::link':
            columns.append(
                dbops.Column(
                    name=src_col, type='uuid', required=True))
            columns.append(
                dbops.Column(
                    name=tgt_col, type='uuid', required=False))
            columns.append(
                dbops.Column(
                    name='ptr_item_id', type='uuid', required=True))

        constraints.append(
            dbops.UniqueConstraint(
                table_name=new_table_name,
                columns=[src_col, tgt_col, 'ptr_item_id']))

        if not link.generic(schema) and link.scalar():
            try:
                tgt_prop = link.getptr(schema, 'target')
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

                    tabname = common.get_backend_name(
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

    def schedule_endpoint_delete_action_update(
            self, link, orig_schema, schema, context):
        endpoint_delete_actions = context.get(
            sd.DeltaRootContext).op.update_endpoint_delete_actions
        link_ops = endpoint_delete_actions.link_ops

        if isinstance(self, sd.DeleteObject):
            for i, (_, ex_link, _) in enumerate(link_ops):
                if ex_link == link:
                    link_ops.pop(i)
                    break

        link_ops.append((self, link, orig_schema))


class CreateLink(LinkMetaCommand, adapts=s_links.CreateLink):
    def apply(self, schema, context=None):
        # Need to do this early, since potential table alters triggered by
        # sub-commands need this.
        orig_schema = schema
        schema, link = s_links.CreateLink.apply(self, schema, context)
        self.table_name = common.get_backend_name(schema, link, catenate=False)
        schema, _ = LinkMetaCommand.apply(self, schema, context)

        self.provide_table(link, schema, context)

        objtype = context.get(s_objtypes.ObjectTypeCommandContext)
        rec, updates = self.record_metadata(link, schema, orig_schema, context)
        self.updates = updates
        extra_ops = []

        source = link.get_source(schema)
        if source is not None:
            source_is_view = (
                source.is_view(schema)
                or source.get_union_of(schema)
                or source.get_is_derived(schema)
            )
        else:
            source_is_view = None

        if source is not None and not source_is_view:
            ptr_stor_info = types.get_pointer_storage_info(
                link, resolve_type=False, schema=schema)

            if ptr_stor_info.table_type == 'ObjectType':
                default_value = self.get_pointer_default(link, schema, context)

                cols = self.get_columns(link, schema, default_value)
                table_name = common.get_backend_name(
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

                index_name = common.get_backend_name(
                    schema, link, catenate=False, aspect='index'
                )[1]

                pg_index = dbops.Index(
                    name=index_name, table_name=table_name,
                    unique=False, columns=[c.name for c in cols],
                    inherit=True)

                ci = dbops.CreateIndex(pg_index, priority=3)
                extra_ops.append(ci)

        objtype = context.get(s_objtypes.ObjectTypeCommandContext)
        table = self.get_table(schema)
        self.pgops.add(
            dbops.Insert(table=table, records=[rec], priority=1))

        self.attach_alter_table(context)

        self.pgops.update(extra_ops)

        if source is not None and not source_is_view:
            self.schedule_endpoint_delete_action_update(
                link, orig_schema, schema, context)

        return schema, link


class RenameLink(LinkMetaCommand, adapts=s_links.RenameLink):
    def apply(self, schema, context=None):
        schema, result = s_links.RenameLink.apply(self, schema, context)
        schema, _ = LinkMetaCommand.apply(self, schema, context)
        return schema, result

    def _rename_begin(self, schema, context, scls):
        schema = super()._rename_begin(schema, context, scls)

        self.rename_pointer(
            scls, schema, context, self.classname, self.new_name)

        self.attach_alter_table(context)

        if scls.generic(schema):
            link_cmd = context.get(s_links.LinkCommandContext)
            assert link_cmd

            self.rename(
                schema, link_cmd.original_schema, context, scls)
            link_cmd.op.table_name = common.get_backend_name(
                schema, scls, catenate=False)
        else:
            link_cmd = context.get(s_links.LinkCommandContext)

            if self.has_table(scls, schema):
                self.rename(
                    schema, link_cmd.original_schema, context, scls)

        return schema


class RebaseLink(LinkMetaCommand, adapts=s_links.RebaseLink):
    def apply(self, schema, context):
        schema, result = s_links.RebaseLink.apply(self, schema, context)
        schema, _ = LinkMetaCommand.apply(self, schema, context)
        schema, _ = self.update(schema, context)

        link_ctx = context.get(s_links.LinkCommandContext)
        source = link_ctx.scls

        if self.has_table(source, schema):
            orig_schema = link_ctx.original_schema
            schema = self.apply_base_delta(
                source, orig_schema, schema, context)

        return schema, result


class SetLinkType(
        LinkMetaCommand, adapts=s_links.SetLinkType):

    def apply(self, schema, context):
        schema, ptr = s_links.SetLinkType.apply(self, schema, context)
        schema, _ = LinkMetaCommand.apply(self, schema, context)
        schema, _ = self.update(schema, context)
        return schema, ptr


class AlterLink(LinkMetaCommand, adapts=s_links.AlterLink):
    def apply(self, schema, context=None):
        orig_schema = schema
        schema, link = s_links.AlterLink.apply(self, schema, context)
        schema, _ = LinkMetaCommand.apply(self, schema, context)

        with context(s_links.LinkCommandContext(schema, self, link)) as ctx:
            ctx.original_schema = orig_schema
            rec, updates = self.record_metadata(
                link, schema, orig_schema, context)
            self.updates = updates

            self.provide_table(link, schema, context)

            if rec:
                table = self.get_table(schema)
                self.pgops.add(
                    dbops.Update(
                        table=table, record=rec,
                        condition=[('id', link.id)], priority=1))

            new_type = None
            for op in self.get_subcommands(type=sd.AlterObjectProperty):
                if op.property == 'target':
                    new_type = op.new_value.get_name(schema) \
                        if op.new_value is not None else None
                    break

            if new_type:
                if not isinstance(link.get_target(schema), s_obj.Object):
                    schema = link.set_field_value(
                        schema, 'target', schema.get(link.get_target(schema)))

            self.attach_alter_table(context)

            if not link.generic(schema):
                self.adjust_pointer_storage(link, schema, orig_schema, context)

                old_ptr_stor_info = types.get_pointer_storage_info(
                    link, schema=orig_schema)
                ptr_stor_info = types.get_pointer_storage_info(
                    link, schema=schema)

                link_required = link.get_required(schema)
                old_link_required = link.get_required(orig_schema)

                if (old_ptr_stor_info.table_type == 'ObjectType' and
                        ptr_stor_info.table_type == 'ObjectType' and
                        link_required != old_link_required):

                    ot_ctx = context.get(s_objtypes.ObjectTypeCommandContext)
                    alter_table = ot_ctx.op.get_alter_table(
                        schema, context)

                    alter_table.add_operation(
                        dbops.AlterTableAlterColumnNull(
                            column_name=ptr_stor_info.column_name,
                            null=not link.get_required(schema)))

            if isinstance(link.get_target(schema), s_scalars.ScalarType):
                self.alter_pointer_default(link, schema, context)

            if 'on_target_delete' in updates:
                self.schedule_endpoint_delete_action_update(
                    link, orig_schema, schema, context)

        return schema, link


class DeleteLink(LinkMetaCommand, adapts=s_links.DeleteLink):
    def apply(self, schema, context=None):
        orig_schema = schema
        link = schema.get(self.classname)

        old_table_name = common.get_backend_name(
            schema, link, catenate=False)

        schema, _ = LinkMetaCommand.apply(self, schema, context)
        schema, _ = s_links.DeleteLink.apply(self, schema, context)

        if not link.generic(orig_schema):
            link_name = link.get_shortname(orig_schema).name
            ptr_stor_info = types.get_pointer_storage_info(
                link, schema=orig_schema)

            objtype = context.get(s_objtypes.ObjectTypeCommandContext)

            if (not isinstance(objtype.op, s_objtypes.DeleteObjectType)
                    and ptr_stor_info.table_type == 'ObjectType'
                    and objtype.scls.getptr(schema, link_name) is None):
                # Only drop the column if the parent is not being dropped
                # and the link was not reinherited in the same delta.
                if objtype.scls.getptr(schema, link_name) is None:
                    # This must be a separate so that objects depending
                    # on this column can be dropped correctly.
                    #
                    alter_table = objtype.op.get_alter_table(
                        schema, context, manual=True, priority=2)
                    col = dbops.Column(
                        name=ptr_stor_info.column_name,
                        type=common.qname(*ptr_stor_info.column_type))
                    exists_cond = dbops.ColumnExists(
                        table_name=objtype.op.table_name, column_name=col.name)
                    inherited_cond = dbops.ColumnIsInherited(
                        table_name=objtype.op.table_name, column_name=col.name)
                    col = dbops.AlterTableDropColumn(col)
                    alter_table.add_operation(
                        (col, [exists_cond], [inherited_cond]))
                    self.pgops.add(alter_table)

            if link.get_is_local(orig_schema):
                self.schedule_endpoint_delete_action_update(
                    link, orig_schema, schema, context)

        condition = dbops.TableExists(name=old_table_name)
        self.pgops.add(
            dbops.DropTable(name=old_table_name, conditions=[condition]))

        table = self.get_table(schema)
        self.pgops.add(
            dbops.Delete(
                table=table,
                condition=[('id', link.id)]))

        return schema, link


class PropertyMetaCommand(CompositeObjectMetaCommand, PointerMetaCommand):
    _table = metaschema.get_metaclass_table(s_props.Property)

    def get_table(self, schema):
        return self._table

    @classmethod
    def _create_table(
            cls, prop, schema, context, conditional=False, create_bases=True,
            create_children=True):
        new_table_name = common.get_backend_name(schema, prop, catenate=False)

        create_c = dbops.CommandGroup()

        constraints = []
        columns = []

        src_col = common.edgedb_name_to_pg_name('source')

        if prop.get_name(schema) == 'std::property':
            columns.append(
                dbops.Column(
                    name=src_col, type='uuid', required=True))
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

                    tabname = common.get_backend_name(
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
        orig_schema = schema
        schema, prop = s_props.CreateProperty.apply(self, schema, context)
        schema, _ = PropertyMetaCommand.apply(self, schema, context)

        src = context.get(s_sources.SourceCommandContext)

        self.provide_table(prop, schema, context)

        with context(s_props.PropertyCommandContext(schema, self, prop)):
            rec, updates = self.record_metadata(
                prop, schema, orig_schema, context)
            self.updates = updates

        if src and self.has_table(src.scls, schema):
            if isinstance(src, s_links.Link):
                src.op.provide_table(src.scls, schema, context)

            ptr_stor_info = types.get_pointer_storage_info(
                prop, resolve_type=False, schema=schema)

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
        return schema, result

    def _rename_begin(self, schema, context, scls):
        schema = super()._rename_begin(schema, context, scls)

        self.rename_pointer(
            scls, schema, context, self.classname, self.new_name)

        return schema


class RebaseProperty(
        PropertyMetaCommand, adapts=s_props.RebaseProperty):
    def apply(self, schema, context):
        schema, result = s_props.RebaseProperty.apply(self, schema, context)
        schema, _ = PropertyMetaCommand.apply(self, schema, context)
        schema, _ = self.update(schema, context)

        prop_ctx = context.get(s_props.PropertyCommandContext)
        source = prop_ctx.scls

        if self.has_table(source, schema):
            orig_schema = prop_ctx.original_schema
            schema = self.apply_base_delta(
                source, orig_schema, schema, context)

        return schema, result


class SetPropertyType(
        PropertyMetaCommand, adapts=s_props.SetPropertyType):

    def apply(self, schema, context):
        schema, ptr = s_props.SetPropertyType.apply(self, schema, context)
        schema, _ = PropertyMetaCommand.apply(self, schema, context)
        schema, _ = self.update(schema, context)
        return schema, ptr


class AlterProperty(
        PropertyMetaCommand, adapts=s_props.AlterProperty):
    def apply(self, schema, context=None):
        orig_schema = schema
        schema, prop = s_props.AlterProperty.apply(self, schema, context)
        schema, _ = PropertyMetaCommand.apply(self, schema, context)

        with context(
                s_props.PropertyCommandContext(schema, self, prop)) as ctx:
            ctx.original_schema = orig_schema

            rec, updates = self.record_metadata(
                prop, schema, orig_schema, context)
            self.updates = updates

            self.provide_table(prop, schema, context)

            if rec:
                table = self.get_table(schema)
                self.pgops.add(
                    dbops.Update(
                        table=table, record=rec,
                        condition=[('id', prop.id)], priority=2))

            prop_target = prop.get_target(schema)
            old_prop_target = prop.get_target(orig_schema)

            prop_required = prop.get_required(schema)
            old_prop_required = prop.get_required(orig_schema)

            if (isinstance(prop_target, s_scalars.ScalarType) and
                    isinstance(old_prop_target, s_scalars.ScalarType) and
                    prop_required != old_prop_required):

                src_ctx = context.get(s_sources.SourceCommandContext)
                src_op = src_ctx.op
                alter_table = src_op.get_alter_table(
                    schema, context, priority=5)
                ptr_stor_info = types.get_pointer_storage_info(
                    prop, schema=schema)
                alter_table.add_operation(
                    dbops.AlterTableAlterColumnNull(
                        column_name=ptr_stor_info.column_name,
                        null=not prop.get_required(schema)))

            self.alter_pointer_default(prop, schema, context)

            if not prop.generic(schema):
                self.adjust_pointer_storage(prop, schema, orig_schema, context)

        return schema, prop


class DeleteProperty(
        PropertyMetaCommand, adapts=s_props.DeleteProperty):
    def apply(self, schema, context=None):
        orig_schema = schema
        prop = schema.get(self.classname)

        schema, prop = s_props.DeleteProperty.apply(self, schema, context)
        schema, _ = PropertyMetaCommand.apply(self, schema, context)

        source_ctx = context.get(s_sources.SourceCommandContext)
        if source_ctx is not None:
            source = source_ctx.scls
            source_op = source_ctx.op
        else:
            source = source_op = None

        if (source
                and not source.getptr(
                    schema, prop.get_shortname(orig_schema).name)
                and source_ctx.op.has_table(source, schema)):
            alter_table = source_op.get_alter_table(schema, context)

            ptr_stor_info = types.get_pointer_storage_info(
                prop, schema=orig_schema)

            exists_cond = dbops.ColumnExists(
                table_name=ptr_stor_info.table_name,
                column_name=ptr_stor_info.column_name)

            inherited_cond = dbops.ColumnIsInherited(
                table_name=ptr_stor_info.table_name,
                column_name=ptr_stor_info.column_name)

            col = dbops.AlterTableDropColumn(
                dbops.Column(name=ptr_stor_info.column_name,
                             type=ptr_stor_info.column_type))

            alter_table.add_operation((col, (exists_cond,), (inherited_cond,)))

        table = self.get_table(schema)
        self.pgops.add(
            dbops.Delete(
                table=table, condition=[('id', prop.id)]))

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
                src=common.quote_ident('source'),
                tgt=common.quote_ident('target'),
                table=common.get_backend_name(
                    schema, link),
            ))

        return '(' + '\nUNION ALL\n    '.join(selects) + ') as q'

    def _get_inline_link_table_union(self, schema, links) -> str:
        selects = []
        for link in links:
            selects.append(textwrap.dedent('''\
                (SELECT
                    {id}::uuid AS ptr_item_id,
                    {src} as source,
                    {tgt} as target
                FROM {table})
            ''').format(
                id=ql(str(link.id)),
                src=common.quote_ident('id'),
                tgt=common.quote_ident(link.get_shortname(schema).name),
                table=common.get_backend_name(
                    schema, link.get_source(schema)),
            ))

        return '(' + '\nUNION ALL\n    '.join(selects) + ') as q'

    def get_trigger_name(self, schema, target,
                         disposition, deferred=False, inline=False):
        if disposition == 'target':
            aspect = 'target-del'
        else:
            aspect = 'source-del'

        if deferred:
            aspect += '-def'
        else:
            aspect += '-imm'

        if inline:
            aspect += '-inl'
        else:
            aspect += '-otl'

        aspect += '-t'

        return common.get_backend_name(
            schema, target, catenate=False, aspect=aspect)[1]

    def get_trigger_proc_name(self, schema, target,
                              disposition, deferred=False, inline=False):
        if disposition == 'target':
            aspect = 'target-del'
        else:
            aspect = 'source-del'

        if deferred:
            aspect += '-def'
        else:
            aspect += '-imm'

        if inline:
            aspect += '-inl'
        else:
            aspect += '-otl'

        aspect += '-f'

        return common.get_backend_name(
            schema, target, catenate=False, aspect=aspect)

    def get_trigger_proc_text(self, target, links, *,
                              disposition, inline, schema):
        if inline:
            return self._get_inline_link_trigger_proc_text(
                target, links, disposition=disposition, schema=schema)
        else:
            return self._get_outline_link_trigger_proc_text(
                target, links, disposition=disposition, schema=schema)

    def _get_outline_link_trigger_proc_text(
            self, target, links, *, disposition, schema):

        chunks = []

        DA = s_links.LinkTargetDeleteAction

        if disposition == 'target':
            groups = itertools.groupby(
                links, lambda l: l.get_on_target_delete(schema))
            near_endpoint, far_endpoint = 'target', 'source'
        else:
            groups = [(DA.ALLOW, links)]
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
                    id='id',
                    tgtname=target.get_displayname(schema),
                    near_endpoint=near_endpoint,
                    far_endpoint=far_endpoint,
                )

                chunks.append(text)

            elif action == s_links.LinkTargetDeleteAction.ALLOW:
                for link in links:
                    link_table = common.get_backend_name(
                        schema, link)

                    text = textwrap.dedent('''\
                        DELETE FROM
                            {link_table}
                        WHERE
                            {endpoint} = OLD.{id};
                    ''').format(
                        link_table=link_table,
                        endpoint=common.quote_ident(near_endpoint),
                        id='id'
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
                        source_table=common.get_backend_name(schema, source),
                        id='id',
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

    def _get_inline_link_trigger_proc_text(
            self, target, links, *, disposition, schema):

        if disposition == 'source':
            raise RuntimeError(
                'source disposition link target delete action trigger does '
                'not make sense for inline links')

        chunks = []

        DA = s_links.LinkTargetDeleteAction

        groups = itertools.groupby(
            links, lambda l: l.get_on_target_delete(schema))

        near_endpoint, far_endpoint = 'target', 'source'

        for action, links in groups:
            if action is DA.RESTRICT or action is DA.DEFERRED_RESTRICT:
                tables = self._get_inline_link_table_union(schema, links)

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
                    id='id',
                    tgtname=target.get_displayname(schema),
                    near_endpoint=near_endpoint,
                    far_endpoint=far_endpoint,
                )

                chunks.append(text)

            elif action == s_links.LinkTargetDeleteAction.ALLOW:
                for link in links:
                    source_table = common.get_backend_name(
                        schema, link.get_source(schema))

                    text = textwrap.dedent('''\
                        UPDATE
                            {source_table}
                        SET
                            {endpoint} = NULL
                        WHERE
                            {endpoint} = OLD.{id};
                    ''').format(
                        source_table=source_table,
                        endpoint=qi(link.get_shortname(schema).name),
                        id='id'
                    )

                    chunks.append(text)

            elif action == s_links.LinkTargetDeleteAction.DELETE_SOURCE:
                sources = collections.defaultdict(list)
                for link in links:
                    sources[link.get_source(schema)].append(link)

                for source, source_links in sources.items():
                    tables = self._get_inline_link_table_union(
                        schema, source_links)

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
                        source_table=common.get_backend_name(schema, source),
                        id='id',
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
                links text[];
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

        affected_sources = set()
        affected_targets = set()
        deletions = False

        for link_op, link, orig_schema in self.link_ops:
            if isinstance(link_op, DeleteLink):
                if (link.generic(orig_schema)
                        or not link.get_is_local(orig_schema)):
                    continue
                source = link.get_source(orig_schema)
                current_source = orig_schema.get_by_id(source.id, None)
                if (current_source is not None
                        and not current_source.is_view(orig_schema)):
                    affected_sources.add((current_source, orig_schema))
                target = link.get_target(orig_schema)
                current_target = schema.get_by_id(target.id, None)
                if current_target is not None:
                    affected_targets.add(current_target)
                deletions = True
            else:
                if link.generic(schema) or not link.get_is_local(schema):
                    continue
                source = link.get_source(schema)
                if source.is_view(schema):
                    continue

                affected_sources.add((source, schema))

                target = link.get_target(schema)
                affected_targets.add(target)

                if isinstance(link_op, AlterLink):
                    orig_target = link.get_target(orig_schema)
                    if target != orig_target:
                        current_orig_target = schema.get_by_id(
                            orig_target.id, None)
                        if current_orig_target is not None:
                            affected_targets.add(current_orig_target)

        for source, src_schema in affected_sources:
            links = []

            for l in source.get_pointers(src_schema).objects(src_schema):
                if (not isinstance(l, s_links.Link)
                        or not l.get_is_local(src_schema)):
                    continue
                ptr_stor_info = types.get_pointer_storage_info(
                    l, schema=src_schema)
                if ptr_stor_info.table_type != 'link':
                    continue

                links.append(l)

            links.sort(
                key=lambda l: (l.get_on_target_delete(src_schema),
                               l.get_name(src_schema)))

            if links or deletions:
                self._update_action_triggers(
                    src_schema, source, links, disposition='source')

        for target in affected_targets:
            deferred_links = []
            deferred_inline_links = []
            links = []
            inline_links = []

            for l in schema.get_referrers(target, scls_type=s_links.Link,
                                          field_name='target'):
                if not l.get_is_local(schema):
                    continue
                ptr_stor_info = types.get_pointer_storage_info(
                    l, schema=schema)
                if ptr_stor_info.table_type != 'link':
                    if l.get_on_target_delete(schema) is DA.DEFERRED_RESTRICT:
                        deferred_inline_links.append(l)
                    else:
                        inline_links.append(l)
                else:
                    if l.get_on_target_delete(schema) is DA.DEFERRED_RESTRICT:
                        deferred_links.append(l)
                    else:
                        links.append(l)

            links.sort(
                key=lambda l: (l.get_on_target_delete(schema),
                               l.get_name(schema)))

            inline_links.sort(
                key=lambda l: (l.get_on_target_delete(schema),
                               l.get_name(schema)))

            deferred_links.sort(
                key=lambda l: l.get_name(schema))

            deferred_inline_links.sort(
                key=lambda l: l.get_name(schema))

            if links or deletions:
                self._update_action_triggers(
                    schema, target, links, disposition='target')

            if inline_links or deletions:
                self._update_action_triggers(
                    schema, target, inline_links,
                    disposition='target', inline=True)

            if deferred_links or deletions:
                self._update_action_triggers(
                    schema, target, deferred_links,
                    disposition='target', deferred=True)

            if deferred_inline_links or deletions:
                self._update_action_triggers(
                    schema, target, deferred_inline_links,
                    disposition='target', deferred=True,
                    inline=True)

        return schema, None

    def _update_action_triggers(
            self,
            schema,
            objtype: s_objtypes.ObjectType,
            links: List[s_links.Link], *,
            disposition: str,
            deferred: bool=False,
            inline: bool=False) -> None:

        union_of = objtype.get_union_of(schema)
        if union_of:
            objtypes = tuple(union_of.objects(schema))
        else:
            objtypes = (objtype,)

        for objtype in objtypes:
            table_name = common.get_backend_name(
                schema, objtype, catenate=False)

            trigger_name = self.get_trigger_name(
                schema, objtype, disposition=disposition,
                deferred=deferred, inline=inline)

            proc_name = self.get_trigger_proc_name(
                schema, objtype, disposition=disposition,
                deferred=deferred, inline=inline)

            trigger = dbops.Trigger(
                name=trigger_name, table_name=table_name,
                events=('delete',), procedure=proc_name,
                is_constraint=True, inherit=True, deferred=deferred)

            if links:
                proc_text = self.get_trigger_proc_text(
                    objtype, links, disposition=disposition,
                    inline=inline, schema=schema)

                trig_func = dbops.Function(
                    name=proc_name, text=proc_text, volatility='volatile',
                    returns='trigger', language='plpgsql')

                self.pgops.add(dbops.CreateOrReplaceFunction(trig_func))

                self.pgops.add(dbops.CreateTrigger(
                    trigger, neg_conditions=[dbops.TriggerExists(
                        trigger_name=trigger_name, table_name=table_name
                    )]
                ))
            else:
                self.pgops.add(
                    dbops.DropTrigger(
                        trigger,
                        conditions=[dbops.TriggerExists(
                            trigger_name=trigger_name,
                            table_name=table_name,
                        )]
                    )
                )

                self.pgops.add(
                    dbops.DropFunction(
                        name=proc_name,
                        args=[],
                        conditions=[dbops.FunctionExists(
                            name=proc_name,
                            args=[],
                        )]
                    )
                )


class ModuleMetaCommand(ObjectMetaCommand):
    _table = metaschema.get_metaclass_table(s_mod.Module)

    def get_table(self, schema):
        return self._table


class CreateModule(ModuleMetaCommand, adapts=s_mod.CreateModule):
    def apply(self, schema, context):
        schema, _ = CompositeObjectMetaCommand.apply(self, schema, context)
        schema, module = s_mod.CreateModule.apply(self, schema, context)
        self.scls = module

        schema_name = common.get_backend_name(schema, module)
        condition = dbops.SchemaExists(name=schema_name)

        if self.if_not_exists:
            cmd = dbops.CommandGroup(neg_conditions={condition})
        else:
            cmd = dbops.CommandGroup()

        cmd.add_command(dbops.CreateSchema(name=schema_name))

        schema, _, op = self.create_object(schema, context, module)
        cmd.add_command(op)

        self.pgops.add(cmd)

        return schema, module


class AlterModule(ModuleMetaCommand, adapts=s_mod.AlterModule):
    def apply(self, schema, context):
        schema, module = s_mod.AlterModule.apply(self, schema, context=context)
        schema, _ = CompositeObjectMetaCommand.apply(self, schema, context)

        schema, updaterec, updates = self.fill_record(schema, context)

        if updaterec:
            table = self.get_table(schema)
            condition = [('id', module.id)]
            self.pgops.add(
                dbops.Update(
                    table=table, record=updaterec, condition=condition))

        # self.attach_alter_table(context)

        return schema, module


class DeleteModule(ModuleMetaCommand, adapts=s_mod.DeleteModule):
    def apply(self, schema, context):
        module = self.get_object(schema, context)
        schema_name = common.get_backend_name(schema, module)

        schema, _ = CompositeObjectMetaCommand.apply(self, schema, context)
        schema, module = s_mod.DeleteModule.apply(self, schema, context)

        condition = dbops.SchemaExists(name=schema_name)

        table = self.get_table(schema)
        cmd = dbops.CommandGroup()
        cmd.add_command(
            dbops.DropSchema(
                name=schema_name, conditions={condition}, priority=4))
        cmd.add_command(
            dbops.Delete(
                table=table,
                condition=[('id', module.id)]))

        self.pgops.add(cmd)

        return schema, module


class CreateDatabase(ObjectMetaCommand, adapts=s_db.CreateDatabase):
    def apply(self, schema, context):
        schema, _ = s_db.CreateDatabase.apply(self, schema, context)
        self.pgops.add(dbops.CreateDatabase(dbops.Database(self.classname)))
        return schema, None


class DropDatabase(ObjectMetaCommand, adapts=s_db.DropDatabase):
    def apply(self, schema, context):
        schema, _ = s_db.CreateDatabase.apply(self, schema, context)
        self.pgops.add(dbops.DropDatabase(self.classname))
        return schema, None


class CreateRole(ObjectMetaCommand, adapts=s_roles.CreateRole):
    def apply(self, schema, context):
        schema, role = s_roles.CreateRole.apply(self, schema, context)
        schema, _ = ObjectMetaCommand.apply(self, schema, context)

        role = dbops.Role(
            name=role.get_name(schema),
            allow_login=role.get_allow_login(schema),
            is_superuser=role.get_is_superuser(schema),
            password=role.get_password(schema),
            metadata=dict(id=str(role.id), __edgedb__='1'),
            membership=list(role.get_bases(schema).names(schema)),
        )
        self.pgops.add(dbops.CreateRole(role))
        return schema, role


class AlterRole(ObjectMetaCommand, adapts=s_roles.AlterRole):
    def apply(self, schema, context):
        schema, role = s_roles.AlterRole.apply(self, schema, context)
        schema, _ = ObjectMetaCommand.apply(self, schema, context)
        dbrole = dbops.Role(
            name=role.get_name(schema),
            allow_login=role.get_allow_login(schema),
            is_superuser=role.get_is_superuser(schema),
            password=role.get_password(schema),
        )
        self.pgops.add(dbops.AlterRole(dbrole))

        return schema, role


class RebaseRole(ObjectMetaCommand, adapts=s_roles.RebaseRole):
    def apply(self, schema, context):
        orig_schema = schema
        schema, role = s_roles.RebaseRole.apply(self, schema, context)
        schema, _ = ObjectMetaCommand.apply(self, schema, context)

        for dropped in self.removed_bases:
            self.pgops.add(dbops.AlterRoleDropMember(
                name=dropped.get_name(orig_schema),
                member=role.get_name(schema),
            ))

        for bases, _pos in self.added_bases:
            for added in bases:
                self.pgops.add(dbops.AlterRoleAddMember(
                    name=added.get_name(schema),
                    member=role.get_name(schema),
                ))

        return schema, role


class DeleteRole(ObjectMetaCommand, adapts=s_roles.DeleteRole):
    def apply(self, schema, context):
        schema, role = s_roles.DeleteRole.apply(self, schema, context)
        schema, _ = ObjectMetaCommand.apply(self, schema, context)
        self.pgops.add(dbops.DropRole(self.classname))
        return schema, role


class DeltaRoot(MetaCommand, adapts=sd.DeltaRoot):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._renames = {}

    def apply(self, schema, context):
        self.update_endpoint_delete_actions = UpdateEndpointDeleteActions()

        schema, _ = sd.DeltaRoot.apply(self, schema, context)
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
