# mypy: ignore-errors

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
import dataclasses
import itertools
import textwrap
from typing import *

from edb import errors

from edb.edgeql import ast as ql_ast
from edb.edgeql import qltypes as ql_ft
from edb.edgeql import compiler as qlcompiler

from edb.schema import annos as s_anno
from edb.schema import casts as s_casts
from edb.schema import scalars as s_scalars
from edb.schema import objtypes as s_objtypes
from edb.schema import constraints as s_constr
from edb.schema import database as s_db
from edb.schema import delta as sd
from edb.schema import expr as s_expr
from edb.schema import expraliases as s_aliases
from edb.schema import functions as s_funcs
from edb.schema import indexes as s_indexes
from edb.schema import links as s_links
from edb.schema import lproperties as s_props
from edb.schema import migrations as s_migrations
from edb.schema import modules as s_mod
from edb.schema import name as sn
from edb.schema import operators as s_opers
from edb.schema import pointers as s_pointers
from edb.schema import pseudo as s_pseudo
from edb.schema import roles as s_roles
from edb.schema import sources as s_sources
from edb.schema import types as s_types

from edb.common import markup
from edb.common import ordered
from edb.common import topological

from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from edb.pgsql import common
from edb.pgsql import dbops

from edb.server import pgcluster

from . import ast as pg_ast
from .common import qname as q
from .common import quote_literal as ql
from .common import quote_ident as qi
from .common import quote_type as qt
from . import compiler
from . import codegen
from . import schemamech
from . import types

if TYPE_CHECKING:
    from edb.schema import schema as s_schema


def has_table(obj, schema):
    if isinstance(obj, s_objtypes.ObjectType):
        return not (
            obj.is_compound_type(schema) or
            obj.get_is_derived(schema) or
            obj.is_view(schema)
        )
    elif obj.is_pure_computable(schema) or obj.get_is_derived(schema):
        return False
    elif obj.generic(schema):
        return (
            not isinstance(obj, s_props.Property)
            and str(obj.get_name(schema)) != 'std::link'
        )
    elif obj.is_link_property(schema):
        return not obj.singular(schema)
    elif not has_table(obj.get_source(schema), schema):
        return False
    else:
        ptr_stor_info = types.get_pointer_storage_info(
            obj, resolve_type=False, schema=schema, link_bias=True)

        return (
            ptr_stor_info is not None
            and ptr_stor_info.table_type == 'link'
        )


class CommandMeta(sd.CommandMeta):
    pass


class MetaCommand(sd.Command, metaclass=CommandMeta):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pgops = ordered.OrderedSet()

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        for op in self.before_ops:
            if not isinstance(op, sd.AlterObjectProperty):
                self.pgops.add(op)

        for op in self.ops:
            if not isinstance(op, sd.AlterObjectProperty):
                self.pgops.add(op)
        return schema

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
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = sd.CommandGroup.apply(self, schema, context)
        schema = MetaCommand.apply(self, schema, context)
        return schema


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
                        metaclass=CommandMeta):
    op_priority = 0


class CreateObject(ObjectMetaCommand):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = self.__class__.get_adaptee().apply(self, schema, context)
        return ObjectMetaCommand.apply(self, schema, context)


class RenameObject(ObjectMetaCommand):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = self.__class__.get_adaptee().apply(self, schema, context)
        return ObjectMetaCommand.apply(self, schema, context)


class RebaseObject(ObjectMetaCommand):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = self.__class__.get_adaptee().apply(self, schema, context)
        return ObjectMetaCommand.apply(self, schema, context)


class AlterObject(ObjectMetaCommand):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = ObjectMetaCommand.apply(self, schema, context)
        return self.__class__.get_adaptee().apply(self, schema, context)


class DeleteObject(ObjectMetaCommand):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = self.__class__.get_adaptee().apply(self, schema, context)
        return ObjectMetaCommand.apply(self, schema, context)


class AlterObjectProperty(MetaCommand, adapts=sd.AlterObjectProperty):
    pass


class PseudoTypeCommand(ObjectMetaCommand):
    pass


class CreatePseudoType(
    PseudoTypeCommand,
    CreateObject,
    adapts=s_pseudo.CreatePseudoType,
):
    pass


class TupleCommand(ObjectMetaCommand):

    pass


class CreateTuple(TupleCommand, adapts=s_types.CreateTuple):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = self.__class__.get_adaptee().apply(self, schema, context)
        schema = TupleCommand.apply(self, schema, context)

        if self.scls.is_polymorphic(schema):
            return schema

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

        return schema


class DeleteTuple(TupleCommand, adapts=s_types.DeleteTuple):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        tup = schema.get_global(s_types.Tuple, self.classname)

        if not tup.is_polymorphic(schema):
            self.pgops.add(dbops.DropCompositeType(
                name=common.get_backend_name(schema, tup, catenate=False),
                priority=2,
            ))

        schema = self.__class__.get_adaptee().apply(self, schema, context)
        schema = TupleCommand.apply(self, schema, context)

        return schema


class ExprAliasCommand(ObjectMetaCommand):
    pass


class CreateAlias(
    ExprAliasCommand,
    CreateObject,
    adapts=s_aliases.CreateAlias,
):
    pass


class RenameAlias(
    ExprAliasCommand,
    RenameObject,
    adapts=s_aliases.RenameAlias,
):
    pass


class AlterAlias(
    ExprAliasCommand,
    AlterObject,
    adapts=s_aliases.AlterAlias,
):
    pass


class DeleteAlias(
    ExprAliasCommand,
    DeleteObject,
    adapts=s_aliases.DeleteAlias,
):
    pass


class TupleExprAliasCommand(ObjectMetaCommand):
    pass


class CreateTupleExprAlias(
        TupleExprAliasCommand, CreateObject,
        adapts=s_types.CreateTupleExprAlias):

    pass


class RenameTupleExprAlias(
        TupleExprAliasCommand, RenameObject,
        adapts=s_types.RenameTupleExprAlias):

    pass


class AlterTupleExprAlias(
        TupleExprAliasCommand, AlterObject,
        adapts=s_types.AlterTupleExprAlias):

    pass


class DeleteTupleExprAlias(
        TupleExprAliasCommand, DeleteObject,
        adapts=s_types.DeleteTupleExprAlias):

    pass


class ArrayCommand(ObjectMetaCommand):

    pass


class CreateArray(ArrayCommand, adapts=s_types.CreateArray):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = self.__class__.get_adaptee().apply(self, schema, context)
        schema = ArrayCommand.apply(self, schema, context)
        return schema


class DeleteArray(ArrayCommand, adapts=s_types.DeleteArray):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = self.__class__.get_adaptee().apply(self, schema, context)
        schema = ArrayCommand.apply(self, schema, context)
        return schema


class ArrayExprAliasCommand(ObjectMetaCommand):
    pass


class CreateArrayExprAlias(
        ArrayExprAliasCommand, CreateObject,
        adapts=s_types.CreateArrayExprAlias):

    pass


class RenameArrayExprAlias(
        ArrayExprAliasCommand, RenameObject,
        adapts=s_types.RenameArrayExprAlias):

    pass


class AlterArrayExprAlias(
        ArrayExprAliasCommand, AlterObject,
        adapts=s_types.AlterArrayExprAlias):

    pass


class DeleteArrayExprAlias(
        ArrayExprAliasCommand, DeleteObject,
        adapts=s_types.DeleteArrayExprAlias):

    pass


class ParameterCommand(sd.ObjectCommand,
                       metaclass=CommandMeta):
    pass


class CreateParameter(ParameterCommand, CreateObject,
                      adapts=s_funcs.CreateParameter):

    pass


class DeleteParameter(ParameterCommand, DeleteObject,
                      adapts=s_funcs.DeleteParameter):

    pass


class RenameParameter(ParameterCommand, RenameObject,
                      adapts=s_funcs.RenameParameter):

    pass


class AlterParameter(ParameterCommand, AlterObject,
                     adapts=s_funcs.AlterParameter):

    pass


class FunctionCommand:

    def get_pgname(self, func: s_funcs.Function, schema):
        return common.get_backend_name(schema, func, catenate=False)

    def get_pgtype(self, func: s_funcs.Function, obj, schema):
        if obj.is_any(schema):
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
        has_inlined_defaults = func.has_inlined_defaults(schema)

        args = []
        if has_inlined_defaults:
            args.append(('__defaults_mask__', ('bytea',), None))

        compile_defaults = not (
            has_inlined_defaults or func_params.find_named_only(schema)
        )

        for param in func_params.get_in_canonical_order(schema):
            param_type = param.get_type(schema)
            param_default = param.get_default(schema)

            pg_at = self.get_pgtype(func, param_type, schema)

            default = None
            if compile_defaults and param_default is not None:
                default = self.compile_default(func, param_default, schema)

            args.append((param.get_parameter_name(schema), pg_at, default))

        return args

    def make_function(self, func: s_funcs.Function, code, schema):
        func_return_typemod = func.get_return_typemod(schema)
        func_params = func.get_params(schema)
        return dbops.Function(
            name=self.get_pgname(func, schema),
            args=self.compile_args(func, schema),
            has_variadic=func_params.find_variadic(schema) is not None,
            set_returning=func_return_typemod is ql_ft.TypeModifier.SetOfType,
            volatility=func.get_volatility(schema),
            returns=self.get_pgtype(
                func, func.get_return_type(schema), schema),
            text=code)

    def compile_sql_function(self, func: s_funcs.Function, schema):
        return self.make_function(func, func.get_code(schema), schema)

    def compile_edgeql_function(self, func: s_funcs.Function, schema, context):
        nativecode = func.get_nativecode(schema)
        if nativecode.irast is None:
            nativecode = self.compile_function(schema, context, nativecode)

        sql_text, _ = compiler.compile_ir_to_sql(
            nativecode.irast,
            ignore_shapes=True,
            explicit_top_cast=irtyputils.type_to_typeref(  # note: no cache
                schema, func.get_return_type(schema)),
            output_format=compiler.OutputFormat.NATIVE,
            use_named_params=True)

        return self.make_function(func, sql_text, schema)

    def make_op(
        self,
        func: s_funcs.Function,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        or_replace: bool=False,
    ) -> Tuple[s_schema.Schema, dbops.CreateFunction]:
        if (
            func.get_code(schema) is None
            and func.get_nativecode(schema) is None
        ):
            return schema, None

        func_language = func.get_language(schema)

        if func_language is ql_ast.Language.SQL:
            dbf = self.compile_sql_function(func, schema)
        elif func_language is ql_ast.Language.EdgeQL:
            dbf = self.compile_edgeql_function(func, schema, context)
        else:
            raise errors.QueryError(
                f'cannot compile function {func.get_shortname(schema)}: '
                f'unsupported language {func_language}',
                context=self.source_context)

        op = dbops.CreateFunction(dbf, or_replace=or_replace)
        return schema, op


class CreateFunction(FunctionCommand, CreateObject,
                     adapts=s_funcs.CreateFunction):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        schema, op = self.make_op(self.scls, schema, context)
        if op is not None:
            self.pgops.add(op)
        return schema


class RenameFunction(
        FunctionCommand, RenameObject, adapts=s_funcs.RenameFunction):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        func = self.get_object(schema, context)
        orig_schema = schema
        schema = super().apply(schema, context)

        variadic = func.get_params(orig_schema).find_variadic(orig_schema)
        self.pgops.add(
            dbops.RenameFunction(
                name=self.get_pgname(func, orig_schema),
                new_name=self.get_pgname(func, schema),
                args=self.compile_args(func, schema),
                has_variadic=variadic is not None,
            )
        )

        return schema


class AlterFunction(
        FunctionCommand, AlterObject, adapts=s_funcs.AlterFunction):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)

        if self.metadata_only:
            return schema

        if (
            self.get_attribute_value('volatility') is not None or
            self.get_attribute_value('nativecode') is not None
        ):
            schema, op = self.make_op(
                self.scls, schema, context, or_replace=True)
            if op is not None:
                self.pgops.add(op)

        return schema


class DeleteFunction(
        FunctionCommand, DeleteObject, adapts=s_funcs.DeleteFunction):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super().apply(schema, context)
        func = self.scls

        if func.get_code(orig_schema) or func.get_nativecode(orig_schema):
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

        return schema


class OperatorCommand(FunctionCommand):

    def oper_name_to_pg_name(
        self,
        schema,
        name: sn.QualName,
    ) -> Tuple[str, str]:
        return common.get_operator_backend_name(
            name, catenate=False)

    def get_pg_operands(self, schema, oper: s_opers.Operator):
        left_type = None
        right_type = None
        oper_params = list(oper.get_params(schema).objects(schema))
        oper_kind = oper.get_operator_kind(schema)

        if oper_kind is ql_ft.OperatorKind.Infix:
            left_type = types.pg_type_from_object(
                schema, oper_params[0].get_type(schema))

            right_type = types.pg_type_from_object(
                schema, oper_params[1].get_type(schema))

        elif oper_kind is ql_ft.OperatorKind.Prefix:
            right_type = types.pg_type_from_object(
                schema, oper_params[0].get_type(schema))

        elif oper_kind is ql_ft.OperatorKind.Postfix:
            left_type = types.pg_type_from_object(
                schema, oper_params[0].get_type(schema))

        else:
            raise RuntimeError(
                f'unexpected operator type: {oper.get_type(schema)!r}')

        return left_type, right_type

    def compile_args(self, oper: s_opers.Operator, schema):
        args = []
        oper_params = oper.get_params(schema)

        for param in oper_params.get_in_canonical_order(schema):
            pg_at = self.get_pgtype(oper, param.get_type(schema), schema)
            args.append((param.get_parameter_name(schema), pg_at))

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

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        oper = self.scls
        if oper.get_is_abstract(schema):
            return schema

        oper_language = oper.get_language(schema)
        oper_fromop = oper.get_from_operator(schema)
        oper_fromfunc = oper.get_from_function(schema)
        oper_code = oper.get_code(schema)
        oper_comm = oper.get_commutator(schema)
        if oper_comm:
            commutator = self.oper_name_to_pg_name(schema, oper_comm)
        else:
            commutator = None
        oper_neg = oper.get_negator(schema)
        if oper_neg:
            negator = self.oper_name_to_pg_name(schema, oper_neg)
        else:
            negator = None

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

                if oper_kind is ql_ft.OperatorKind.Infix:
                    op = (f'$1::{from_args[0]} {pg_oper_name} '
                          f'$2::{from_args[1]}')
                elif oper_kind is ql_ft.OperatorKind.Postfix:
                    op = f'$1::{from_args[0]} {pg_oper_name}'
                elif oper_kind is ql_ft.OperatorKind.Prefix:
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
                    name=common.get_backend_name(schema, oper, catenate=False),
                    args=args,
                    procedure=oper_func_name,
                    base_operator=('pg_catalog', pg_oper_name),
                    operator_args=from_args,
                    commutator=commutator,
                    negator=negator,
                ))

        elif oper_language is ql_ast.Language.SQL and oper_code:
            args = self.get_pg_operands(schema, oper)
            oper_func = self.make_operator_function(oper, schema)
            self.pgops.add(dbops.CreateFunction(oper_func))
            oper_func_name = common.qname(*oper_func.name)

            self.pgops.add(dbops.CreateOperator(
                name=common.get_backend_name(schema, oper, catenate=False),
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

        return schema


class RenameOperator(
        OperatorCommand, RenameObject, adapts=s_opers.RenameOperator):
    pass


class AlterOperator(
        OperatorCommand, AlterObject, adapts=s_opers.AlterOperator):
    pass


class DeleteOperator(
        OperatorCommand, DeleteObject, adapts=s_opers.DeleteOperator):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        oper = schema.get(self.classname)

        if oper.get_is_abstract(schema):
            return super().apply(schema, context)

        name = common.get_backend_name(schema, oper, catenate=False)
        args = self.get_pg_operands(schema, oper)

        schema = super().apply(schema, context)
        if not oper.get_from_expr(orig_schema):
            self.pgops.add(dbops.DropOperator(name=name, args=args))
        return schema


class CastCommand:

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

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        cast = self.scls
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

        return schema


class RenameCast(
        CastCommand, RenameObject, adapts=s_casts.RenameCast):
    pass


class AlterCast(
        CastCommand, AlterObject, adapts=s_casts.AlterCast):
    pass


class DeleteCast(
        CastCommand, DeleteObject, adapts=s_casts.DeleteCast):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        cast = schema.get(self.classname)
        cast_language = cast.get_language(schema)
        cast_code = cast.get_code(schema)

        schema = super().apply(schema, context)

        if cast_language is ql_ast.Language.SQL and cast_code:
            cast_func = self.make_cast_function(cast, schema)
            self.pgops.add(dbops.DropFunction(
                cast_func.name, cast_func.args))

        return schema


class AnnotationCommand:
    pass


class CreateAnnotation(
        AnnotationCommand, CreateObject,
        adapts=s_anno.CreateAnnotation):
    op_priority = 1


class RenameAnnotation(
        AnnotationCommand, RenameObject,
        adapts=s_anno.RenameAnnotation):
    pass


class AlterAnnotation(
        AnnotationCommand, AlterObject, adapts=s_anno.AlterAnnotation):
    pass


class DeleteAnnotation(
        AnnotationCommand, DeleteObject,
        adapts=s_anno.DeleteAnnotation):
    pass


class AnnotationValueCommand(sd.ObjectCommand,
                             metaclass=CommandMeta):
    op_priority = 4


class CreateAnnotationValue(
        AnnotationValueCommand, CreateObject,
        adapts=s_anno.CreateAnnotationValue):
    pass


class AlterAnnotationValue(
        AnnotationValueCommand, AlterObject,
        adapts=s_anno.AlterAnnotationValue):
    pass


class RenameAnnotationValue(
        AnnotationValueCommand, RenameObject,
        adapts=s_anno.RenameAnnotationValue):
    pass


class RebaseAnnotationValue(
    AnnotationValueCommand,
    RebaseObject,
    adapts=s_anno.RebaseAnnotationValue,
):
    pass


class DeleteAnnotationValue(
        AnnotationValueCommand, DeleteObject,
        adapts=s_anno.DeleteAnnotationValue):
    pass


class ConstraintCommand(sd.ObjectCommand,
                        metaclass=CommandMeta):
    op_priority = 3

    def constraint_is_effective(self, schema, constraint):
        subject = constraint.get_subject(schema)
        if subject is None:
            return False

        ancestors = [
            a for a in constraint.get_ancestors(schema).objects(schema)
            if not a.generic(schema)
        ]

        if (
            constraint.get_delegated(schema)
            and all(ancestor.get_delegated(schema) for ancestor in ancestors)
        ):
            return False

        elif isinstance(subject, s_pointers.Pointer):
            if subject.generic(schema):
                return True
            else:
                return has_table(subject.get_source(schema), schema)
        elif isinstance(subject, s_objtypes.ObjectType):
            return has_table(subject, schema)
        else:
            return True


class CreateConstraint(
        ConstraintCommand, CreateObject,
        adapts=s_constr.CreateConstraint):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        constraint = self.scls
        if not self.constraint_is_effective(schema, constraint):
            return schema

        subject = constraint.get_subject(schema)

        if subject is not None:
            schemac_to_backendc = \
                schemamech.ConstraintMech.\
                schema_constraint_to_backend_constraint
            bconstr = schemac_to_backendc(subject, constraint, schema, context)

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.create_ops())
            self.pgops.add(op)

        return schema


class RenameConstraint(
        ConstraintCommand, RenameObject,
        adapts=s_constr.RenameConstraint):
    def apply(self, schema, context):
        orig_schema = schema
        schema = super().apply(schema, context)
        constraint = self.scls
        if not self.constraint_is_effective(orig_schema, constraint):
            return schema

        subject = constraint.get_subject(schema)

        if subject is not None:
            schemac_to_backendc = \
                schemamech.ConstraintMech.\
                schema_constraint_to_backend_constraint

            bconstr = schemac_to_backendc(subject, constraint, schema, context)

            orig_subject = constraint.get_subject(orig_schema)
            orig_bconstr = schemac_to_backendc(
                orig_subject, constraint, orig_schema, context
            )

            op = dbops.CommandGroup(priority=1)
            op.add_command(bconstr.rename_ops(orig_bconstr))
            self.pgops.add(op)

        return schema


class AlterConstraintOwned(
    ConstraintCommand,
    AlterObject,
    adapts=s_constr.AlterConstraintOwned,
):
    pass


class AlterConstraint(
        ConstraintCommand, AlterObject,
        adapts=s_constr.AlterConstraint):
    def apply(self, schema, context):
        orig_schema = schema
        schema = super().apply(schema, context)
        constraint = self.scls
        if self.metadata_only:
            return schema
        if (
            not self.constraint_is_effective(schema, constraint)
            and not self.constraint_is_effective(orig_schema, constraint)
        ):
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

            bconstr = schemac_to_backendc(subject, constraint, schema, context)

            orig_bconstr = schemac_to_backendc(
                constraint.get_subject(orig_schema),
                constraint,
                orig_schema,
                context,
            )

            op = dbops.CommandGroup(priority=1)
            if not self.constraint_is_effective(orig_schema, constraint):
                op.add_command(bconstr.create_ops())

                for child in constraint.children(schema):
                    orig_cbconstr = schemac_to_backendc(
                        child.get_subject(orig_schema),
                        child,
                        orig_schema,
                        context,
                    )
                    cbconstr = schemac_to_backendc(
                        child.get_subject(schema),
                        child,
                        schema,
                        context,
                    )
                    op.add_command(cbconstr.alter_ops(orig_cbconstr))
            elif not self.constraint_is_effective(schema, constraint):
                op.add_command(bconstr.alter_ops(orig_bconstr))

                for child in constraint.children(schema):
                    orig_cbconstr = schemac_to_backendc(
                        child.get_subject(orig_schema),
                        child,
                        orig_schema,
                        context,
                    )
                    cbconstr = schemac_to_backendc(
                        child.get_subject(schema),
                        child,
                        schema,
                        context,
                    )
                    op.add_command(cbconstr.alter_ops(orig_cbconstr))
            else:
                op.add_command(bconstr.alter_ops(orig_bconstr))
            self.pgops.add(op)

        return schema


class DeleteConstraint(
        ConstraintCommand, DeleteObject,
        adapts=s_constr.DeleteConstraint):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        delta_root_ctx = context.top()
        orig_schema = delta_root_ctx.original_schema
        constraint = schema.get(self.classname)
        if self.constraint_is_effective(orig_schema, constraint):
            subject = constraint.get_subject(orig_schema)

            if subject is not None:
                schemac_to_backendc = \
                    schemamech.ConstraintMech.\
                    schema_constraint_to_backend_constraint
                bconstr = schemac_to_backendc(
                    subject, constraint, orig_schema, context)

                op = dbops.CommandGroup()
                op.add_command(bconstr.delete_ops())
                self.pgops.add(op)

        schema = super().apply(schema, context)
        return schema


class RebaseConstraint(
        ConstraintCommand, RebaseObject,
        adapts=s_constr.RebaseConstraint):
    pass


class AliasCapableObjectMetaCommand(ObjectMetaCommand):
    pass


class ScalarTypeMetaCommand(AliasCapableObjectMetaCommand):

    def is_sequence(self, schema, scalar):
        seq = schema.get('std::sequence', default=None)
        return seq is not None and scalar.issubclass(schema, seq)


class CreateScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.CreateScalarType):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_scalars.CreateScalarType.apply(self, schema, context)
        scalar = self.scls

        schema = ScalarTypeMetaCommand.apply(self, schema, context)

        if scalar.get_is_abstract(schema):
            return schema

        new_domain_name = types.pg_type_from_scalar(schema, scalar)

        if types.is_builtin_scalar(schema, scalar):
            return schema

        enum_values = scalar.get_enum_values(schema)
        if enum_values:
            new_enum_name = common.get_backend_name(
                schema, scalar, catenate=False)
            self.pgops.add(dbops.CreateEnum(
                dbops.Enum(name=new_enum_name, values=enum_values)))
            base = q(*new_enum_name)

        else:
            base = types.get_scalar_base(schema, scalar)

            if self.is_sequence(schema, scalar):
                seq_name = common.get_backend_name(
                    schema, scalar, catenate=False, aspect='sequence')
                self.pgops.add(dbops.CreateSequence(name=seq_name))

            domain = dbops.Domain(name=new_domain_name, base=base)
            self.pgops.add(dbops.CreateDomain(domain=domain))

            default = self.get_resolved_attribute_value(
                'default',
                schema=schema,
                context=context,
            )
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

        return schema


class RenameScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.RenameScalarType):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_scalars.RenameScalarType.apply(self, schema, context)
        schema = ScalarTypeMetaCommand.apply(self, schema, context)
        scls = self.scls

        ctx = context.get(s_scalars.ScalarTypeCommandContext)
        orig_schema = ctx.original_schema

        name = common.get_backend_name(orig_schema, scls, catenate=False)
        new_name = common.get_backend_name(schema, scls, catenate=False)

        if scls.is_enum(schema):
            self.pgops.add(dbops.RenameEnum(name=name, new_name=new_name))

        else:
            self.pgops.add(dbops.RenameDomain(name=name, new_name=new_name))

        if self.is_sequence(schema, scls):
            seq_name = common.get_backend_name(
                orig_schema, scls, catenate=False, aspect='sequence')
            new_seq_name = common.get_backend_name(
                schema, scls, catenate=False, aspect='sequence')

            if seq_name != new_seq_name:
                self.pgops.add(
                    dbops.RenameSequence(name=seq_name, new_name=new_seq_name))

        return schema


class RebaseScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.RebaseScalarType):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        # Actual rebase is taken care of in AlterScalarType
        schema = ScalarTypeMetaCommand.apply(self, schema, context)
        return s_scalars.RebaseScalarType.apply(self, schema, context)


class AlterScalarType(ScalarTypeMetaCommand, adapts=s_scalars.AlterScalarType):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = s_scalars.AlterScalarType.apply(self, schema, context)
        new_scalar = self.scls
        schema = ScalarTypeMetaCommand.apply(self, schema, context)

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

        default_delta = self.get_resolved_attribute_value(
            'default',
            schema=schema,
            context=context,
        )
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

        return schema


class DeleteScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.DeleteScalarType):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = s_scalars.DeleteScalarType.apply(self, schema, context)
        scalar = self.scls
        schema = ScalarTypeMetaCommand.apply(self, schema, context)

        link = None
        if context:
            link = context.get(s_links.LinkCommandContext)

        ops = link.op.pgops if link else self.pgops

        old_domain_name = common.get_backend_name(
            orig_schema, scalar, catenate=False)

        # Domain dropping gets low priority since other things may
        # depend on it.
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

        if self.is_sequence(orig_schema, scalar):
            seq_name = common.get_backend_name(
                orig_schema, scalar, catenate=False, aspect='sequence')
            self.pgops.add(dbops.DropSequence(name=seq_name))

        return schema


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

            self.pgops.add(self.drop_inhview(
                orig_schema, context, obj,
            ))

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

    def schedule_inhviews_update(
        self,
        schema,
        context,
        obj,
        *,
        update_ancestors: Optional[bool]=None,
        update_descendants: Optional[bool]=None,
    ):
        self.pgops.add(
            self.drop_inhview(
                schema, context, obj, drop_ancestors=update_ancestors)
        )

        root = context.get(sd.DeltaRootContext).op
        updates = root.update_inhviews.view_updates
        update = updates.get(obj)
        if update is None:
            update = updates[obj] = InheritanceViewUpdate()

        if update_ancestors is not None:
            update.update_ancestors = update_ancestors
        if update_descendants is not None:
            update.update_descendants = update_descendants

    def schedule_inhview_deletion(
        self,
        schema,
        context,
        obj,
    ):
        root = context.get(sd.DeltaRootContext).op
        updates = root.update_inhviews.view_updates
        updates.pop(obj, None)
        deletions = root.update_inhviews.view_deletions
        deletions[obj] = schema

    def update_base_inhviews(self, schema, context, obj):
        for base in obj.get_bases(schema).objects(schema):
            if not context.is_deleting(base):
                self.schedule_inhviews_update(
                    schema, context, base, update_ancestors=True)

    def update_lineage_inhviews(self, schema, context, obj):
        self.schedule_inhviews_update(
            schema, context, obj, update_ancestors=True)

    def update_base_inhviews_on_rebase(
        self,
        schema,
        orig_schema,
        context,
        obj,
    ):
        bases = set(obj.get_bases(schema).objects(schema))
        orig_bases = set(obj.get_bases(orig_schema).objects(orig_schema))

        for new_base in bases - orig_bases:
            self.schedule_inhviews_update(
                schema, context, new_base, update_ancestors=True)

        for old_base in orig_bases - bases:
            self.schedule_inhviews_update(
                schema, context, old_base, update_ancestors=True)

    def drop_inhview(
        self,
        schema,
        context,
        obj,
        *,
        drop_ancestors=False,
    ) -> dbops.CommandGroup:
        cmd = dbops.CommandGroup()
        objs = [obj]
        if drop_ancestors:
            objs.extend(obj.get_ancestors(schema).objects(schema))

        for obj in objs:
            if not has_table(obj, schema):
                continue
            inhview_name = common.get_backend_name(
                schema, obj, catenate=False, aspect='inhview')
            cmd.add_command(
                dbops.DropView(
                    inhview_name,
                    conditions=[dbops.ViewExists(inhview_name)],
                ),
            )

        return cmd


class IndexCommand(sd.ObjectCommand, metaclass=CommandMeta):
    pass


class CreateIndex(IndexCommand, CreateObject, adapts=s_indexes.CreateIndex):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = CreateObject.apply(self, schema, context)
        index = self.scls

        parent_ctx = context.get_ancestor(
            s_indexes.IndexSourceCommandContext, self)
        subject_name = parent_ctx.op.classname
        subject = schema.get(subject_name, default=None)
        if not isinstance(subject, s_pointers.Pointer):
            singletons = [subject]
            path_prefix_anchor = ql_ast.Subject().name
        else:
            singletons = []
            path_prefix_anchor = None

        index_expr = index.get_expr(schema)
        ir = index_expr.irast
        if ir is None:
            index_expr = type(index_expr).compiled(
                index_expr,
                schema=schema,
                options=qlcompiler.CompilerOptions(
                    modaliases=context.modaliases,
                    schema_object_context=self.get_schema_metaclass(),
                    anchors={ql_ast.Subject().name: subject},
                    path_prefix_anchor=path_prefix_anchor,
                    singletons=singletons,
                    apply_query_rewrites=not context.stdmode,
                ),
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

        module_name = index.get_name(schema).module
        index_name = common.get_index_backend_name(
            index.id, module_name, catenate=False)
        pg_index = dbops.Index(
            name=index_name[1], table_name=table_name, expr=sql_expr,
            unique=False, inherit=True,
            metadata={'schemaname': str(index.get_name(schema))})
        self.pgops.add(dbops.CreateIndex(pg_index, priority=3))

        return schema


class RenameIndex(IndexCommand, RenameObject, adapts=s_indexes.RenameIndex):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_indexes.RenameIndex.apply(self, schema, context)
        schema = RenameObject.apply(self, schema, context)

        return schema


class AlterIndexOwned(
    IndexCommand,
    AlterObject,
    adapts=s_indexes.AlterIndexOwned,
):
    pass


class AlterIndex(IndexCommand, AlterObject, adapts=s_indexes.AlterIndex):
    pass


class DeleteIndex(IndexCommand, DeleteObject, adapts=s_indexes.DeleteIndex):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = DeleteObject.apply(self, schema, context)
        index = self.scls

        source = context.get(s_links.LinkCommandContext)
        if not source:
            source = context.get(s_objtypes.ObjectTypeCommandContext)

        if not isinstance(source.op, sd.DeleteObject):
            # We should not drop indexes when the host is being dropped since
            # the indexes are dropped automatically in this case.
            #
            table_name = common.get_backend_name(
                schema, source.scls, catenate=False)
            module_name = index.get_name(orig_schema).module
            orig_idx_name = common.get_index_backend_name(
                index.id, module_name, catenate=False)
            index = dbops.Index(
                name=orig_idx_name[1], table_name=table_name, inherit=True)
            index_exists = dbops.IndexExists(
                (table_name[0], index.name_in_catalog))
            self.pgops.add(
                dbops.DropIndex(
                    index, priority=3, conditions=(index_exists, )))

        return schema


class RebaseIndex(
        IndexCommand, RebaseObject,
        adapts=s_indexes.RebaseIndex):
    pass


class CreateUnionType(
    MetaCommand,
    adapts=s_types.CreateUnionType,
    metaclass=CommandMeta,
):

    def apply(self, schema, context):
        schema = self.__class__.get_adaptee().apply(self, schema, context)
        schema = ObjectMetaCommand.apply(self, schema, context)
        return schema


class ObjectTypeMetaCommand(AliasCapableObjectMetaCommand,
                            CompositeObjectMetaCommand):
    pass


class CreateObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.CreateObjectType):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_objtypes.CreateObjectType.apply(self, schema, context)
        objtype = self.scls
        if objtype.is_compound_type(schema) or objtype.get_is_derived(schema):
            return ObjectTypeMetaCommand.apply(self, schema, context)

        new_table_name = common.get_backend_name(
            schema, objtype, catenate=False)
        self.table_name = new_table_name

        columns = []
        token_col = dbops.Column(
            name='__edb_token', type='uuid', required=False)
        columns.append(token_col)

        objtype_table = dbops.Table(name=new_table_name, columns=columns)
        self.pgops.add(dbops.CreateTable(table=objtype_table))
        self.update_lineage_inhviews(schema, context, objtype)

        schema = ObjectTypeMetaCommand.apply(self, schema, context)

        self.attach_alter_table(context)

        if self.update_search_indexes:
            schema = self.update_search_indexes.apply(schema, context)
            self.pgops.add(self.update_search_indexes)

        self.pgops.add(
            dbops.Comment(object=objtype_table, text=str(self.classname)))

        return schema


class RenameObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.RenameObjectType):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_objtypes.RenameObjectType.apply(self, schema, context)
        scls = self.scls
        schema = ObjectTypeMetaCommand.apply(self, schema, context)

        objtype = context.get(s_objtypes.ObjectTypeCommandContext)
        assert objtype

        delta_ctx = context.get(sd.DeltaRootContext)
        assert delta_ctx

        orig_name = scls.get_name(objtype.original_schema)
        delta_ctx.op._renames[orig_name] = scls.get_name(schema)

        obj_has_table = has_table(scls, schema)

        self.rename(schema, objtype.original_schema, context, scls)

        if obj_has_table:
            new_table_name = common.get_backend_name(
                schema, scls, catenate=False)
            objtype_table = dbops.Table(name=new_table_name)
            self.pgops.add(dbops.Comment(
                object=objtype_table, text=str(self.new_name)))

            objtype.op.table_name = new_table_name

            # Need to update all bits that reference objtype name

            old_constr_name = common.edgedb_name_to_pg_name(
                str(self.classname) + '.class_check')
            new_constr_name = common.edgedb_name_to_pg_name(
                str(self.new_name) + '.class_check')

            alter_table = self.get_alter_table(schema, context, manual=True)
            rc = dbops.AlterTableRenameConstraintSimple(
                alter_table.name, old_name=old_constr_name,
                new_name=new_constr_name)
            self.pgops.add(rc)

            self.table_name = new_table_name

        return schema


class RebaseObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.RebaseObjectType):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = s_objtypes.RebaseObjectType.apply(self, schema, context)
        result = self.scls
        schema = ObjectTypeMetaCommand.apply(self, schema, context)

        if has_table(result, schema):
            self.update_base_inhviews_on_rebase(
                schema, orig_schema, context, self.scls)

        return schema


class AlterObjectType(ObjectTypeMetaCommand,
                      adapts=s_objtypes.AlterObjectType):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_objtypes.AlterObjectType.apply(
            self, schema, context=context)
        objtype = self.scls

        self.table_name = common.get_backend_name(
            schema, objtype, catenate=False)

        schema = ObjectTypeMetaCommand.apply(self, schema, context)

        if has_table(objtype, schema):
            self.attach_alter_table(context)

            if self.update_search_indexes:
                schema = self.update_search_indexes.apply(schema, context)
                self.pgops.add(self.update_search_indexes)

        return schema


class DeleteObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.DeleteObjectType):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        self.scls = objtype = schema.get(self.classname)

        old_table_name = common.get_backend_name(
            schema, objtype, catenate=False)

        orig_schema = schema
        schema = ObjectTypeMetaCommand.apply(self, schema, context)
        schema = s_objtypes.DeleteObjectType.apply(self, schema, context)

        if has_table(objtype, orig_schema):
            self.attach_alter_table(context)
            self.pgops.add(dbops.DropTable(name=old_table_name, priority=3))
            self.update_base_inhviews(orig_schema, context, objtype)

        self.schedule_inhview_deletion(orig_schema, context, objtype)

        return schema


class SchedulePointerCardinalityUpdate(MetaCommand):
    pass


class CancelPointerCardinalityUpdate(MetaCommand):
    pass


class PointerMetaCommand(MetaCommand, sd.ObjectCommand,
                         metaclass=CommandMeta):
    def get_host(self, schema, context):
        if context:
            link = context.get(s_links.LinkCommandContext)
            if link and isinstance(self, s_props.PropertyCommand):
                return link
            objtype = context.get(s_objtypes.ObjectTypeCommandContext)
            if objtype:
                return objtype

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

        default = self.get_resolved_attribute_value(
            'default',
            schema=schema,
            context=context,
        )
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
        default = self.get_resolved_attribute_value(
            'default',
            schema=schema,
            context=context,
        )
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
                required=(
                    pointer.get_required(schema)
                    and not pointer.is_pure_computable(schema)
                ),
                default=default,
                comment=str(pointer.get_shortname(schema)),
            ),
        ]

    def create_table(self, ptr, schema, context, conditional=False):
        c = self._create_table(ptr, schema, context, conditional=conditional)
        self.pgops.add(c)

    def provide_table(self, ptr, schema, context):
        if has_table(ptr, schema):
            self.create_table(ptr, schema, context, conditional=True)
            self.update_lineage_inhviews(schema, context, ptr)
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

        if (old_target.get_name(orig_schema) != new_target.get_name(schema) or
                old_ptr_stor_info.table_type != new_ptr_stor_info.table_type):

            for op in self.get_subcommands(type=s_scalars.ScalarTypeCommand):
                for rename in op(s_scalars.RenameScalarType):
                    if (
                        old_target.get_name(orig_schema) == rename.classname
                        and new_target.get_name(schema) == rename.new_name
                    ):
                        # Our target alter is a mere rename
                        type_change_ok = True

                if isinstance(op, s_scalars.CreateScalarType):
                    if op.classname == new_target.get_name(schema):
                        # CreateScalarType will take care of everything for us
                        type_change_ok = True

            if old_ptr_stor_info.table_type != new_ptr_stor_info.table_type:
                # The attribute is being moved from one table to another
                opg = dbops.CommandGroup(priority=0)
                at = source_op.get_alter_table(schema, context, manual=True)

                if old_ptr_stor_info.table_type == 'ObjectType':
                    move_data = dbops.Query(textwrap.dedent(f'''\
                        INSERT INTO {q(*new_ptr_stor_info.table_name)}
                        (source, target)
                        (SELECT
                            s.id AS source,
                            s.{qi(old_ptr_stor_info.column_name)} AS target
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
                    opg.add_command(self.drop_inhview(
                        orig_schema, context, source_op.scls,
                        drop_ancestors=True))
                    at.add_command(dbops.AlterTableDropColumn(col))

                    opg.add_command(at)

                    self.schedule_inhviews_update(
                        schema,
                        context,
                        source_op.scls,
                        update_descendants=True,
                        update_ancestors=True,
                    )
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

                    if not has_table(pointer, schema):
                        opg.add_command(self.drop_inhview(
                            orig_schema, context, source_op.scls,
                            drop_ancestors=True))

                        opg.add_command(self.drop_inhview(
                            orig_schema, context, pointer,
                            drop_ancestors=True
                        ))

                        condition = dbops.TableExists(name=otabname)
                        dt = dbops.DropTable(
                            name=otabname, conditions=[condition])

                        opg.add_command(dt)

                    self.schedule_inhviews_update(
                        schema,
                        context,
                        source_op.scls,
                        update_descendants=True,
                    )

                self.pgops.add(opg)

            else:
                if old_target != new_target and not type_change_ok:
                    if not isinstance(old_target, s_objtypes.ObjectType):
                        source = source_ctx.scls

                        self.pgops.add(self.drop_inhview(
                            schema,
                            context,
                            source,
                            drop_ancestors=True,
                        ))

                        alter_table = source_op.get_alter_table(
                            schema, context, priority=0, force_new=True)

                        new_type = types.pg_type_from_object(
                            schema, new_target, persistent_tuples=True)

                        alter_type = dbops.AlterTableAlterColumnType(
                            old_ptr_stor_info.column_name,
                            common.quote_type(new_type))

                        alter_table.add_operation(alter_type)

                        self.schedule_inhviews_update(
                            schema,
                            context,
                            source,
                            update_descendants=True,
                            update_ancestors=True,
                        )


class LinkMetaCommand(CompositeObjectMetaCommand, PointerMetaCommand):

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

        columns.append(
            dbops.Column(
                name=src_col, type='uuid', required=True))
        columns.append(
            dbops.Column(
                name=tgt_col, type='uuid', required=False))

        constraints.append(
            dbops.UniqueConstraint(
                table_name=new_table_name,
                columns=[src_col, tgt_col]))

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

        c.add_command(dbops.Comment(table, str(link.get_name(schema))))

        create_c.add_command(c)

        if create_children:
            for l_descendant in link.descendants(schema):
                if has_table(l_descendant, schema):
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
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        # Need to do this early, since potential table alters triggered by
        # sub-commands need this.
        orig_schema = schema
        schema = s_links.CreateLink.apply(self, schema, context)
        link = self.scls
        self.table_name = common.get_backend_name(schema, link, catenate=False)
        schema = LinkMetaCommand.apply(self, schema, context)

        self.provide_table(link, schema, context)

        objtype = context.get(s_objtypes.ObjectTypeCommandContext)
        extra_ops = []

        source = link.get_source(schema)
        if source is not None:
            source_is_view = (
                source.is_view(schema)
                or source.is_compound_type(schema)
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
                    cmd = dbops.AlterTableAddColumn(col)
                    objtype_alter_table.add_operation(cmd)

                    if col.name == '__type__':
                        constr_name = common.edgedb_name_to_pg_name(
                            str(objtype.op.classname) + '.class_check')

                        constr_expr = dbops.Query(textwrap.dedent(f"""\
                            SELECT
                                '"__type__" = ' ||
                                quote_literal({ql(str(objtype.scls.id))})
                        """), type='text')

                        cid_constraint = dbops.CheckConstraint(
                            self.table_name,
                            constr_name,
                            constr_expr,
                            inherit=False,
                        )

                        objtype_alter_table.add_operation(
                            dbops.AlterTableAddConstraint(cid_constraint),
                        )

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

                self.update_lineage_inhviews(schema, context, link)

                self.schedule_inhviews_update(
                    schema,
                    context,
                    source,
                    update_descendants=True,
                )

        objtype = context.get(s_objtypes.ObjectTypeCommandContext)

        self.attach_alter_table(context)

        self.pgops.update(extra_ops)

        if (source is not None and not source_is_view
                and not link.is_pure_computable(schema)):
            self.schedule_endpoint_delete_action_update(
                link, orig_schema, schema, context)

        return schema


class RenameLink(LinkMetaCommand, adapts=s_links.RenameLink):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_links.RenameLink.apply(self, schema, context)
        schema = LinkMetaCommand.apply(self, schema, context)
        return schema

    def _rename_begin(self, schema, context):
        schema = super()._rename_begin(schema, context)
        scls = self.scls

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

            if has_table(scls, schema):
                self.rename(
                    schema, link_cmd.original_schema, context, scls)

        return schema


class RebaseLink(LinkMetaCommand, adapts=s_links.RebaseLink):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = s_links.RebaseLink.apply(self, schema, context)
        schema = LinkMetaCommand.apply(self, schema, context)

        link_ctx = context.get(s_links.LinkCommandContext)
        source = link_ctx.scls

        if has_table(source, schema):
            self.update_base_inhviews_on_rebase(
                schema, orig_schema, context, source)

        return schema


class SetLinkType(LinkMetaCommand, adapts=s_links.SetLinkType):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_links.SetLinkType.apply(self, schema, context)
        return LinkMetaCommand.apply(self, schema, context)


class AlterLinkUpperCardinality(
    LinkMetaCommand,
    adapts=s_links.AlterLinkUpperCardinality,
):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_links.AlterLinkUpperCardinality.apply(self, schema, context)
        return LinkMetaCommand.apply(self, schema, context)


class AlterLinkOwned(
    LinkMetaCommand,
    AlterObject,
    adapts=s_links.AlterLinkOwned,
):
    pass


class AlterLink(LinkMetaCommand, adapts=s_links.AlterLink):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = s_links.AlterLink.apply(self, schema, context)
        link = self.scls
        schema = LinkMetaCommand.apply(self, schema, context)

        with context(s_links.LinkCommandContext(schema, self, link)) as ctx:
            ctx.original_schema = orig_schema
            self.provide_table(link, schema, context)
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

            otd = self.get_resolved_attribute_value(
                'on_target_delete',
                schema=schema,
                context=context,
            )
            if otd and not link.is_pure_computable(schema):
                self.schedule_endpoint_delete_action_update(
                    link, orig_schema, schema, context)

        return schema


class DeleteLink(LinkMetaCommand, adapts=s_links.DeleteLink):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        delta_root_ctx = context.top()
        orig_schema = delta_root_ctx.original_schema
        link = schema.get(self.classname)

        old_table_name = common.get_backend_name(
            schema, link, catenate=False)

        schema = LinkMetaCommand.apply(self, schema, context)
        schema = s_links.DeleteLink.apply(self, schema, context)

        if (
            not link.generic(orig_schema)
            and has_table(link.get_source(orig_schema), orig_schema)
        ):
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
                    col = dbops.AlterTableDropColumn(col)
                    alter_table.add_operation(col)
                    self.pgops.add(alter_table)

                    self.schedule_inhviews_update(
                        schema,
                        context,
                        objtype.scls,
                        update_descendants=True,
                    )

            if link.get_is_owned(orig_schema):
                self.schedule_endpoint_delete_action_update(
                    link, orig_schema, schema, context)

            self.attach_alter_table(context)

        self.pgops.add(
            self.drop_inhview(orig_schema, context, link, drop_ancestors=True)
        )

        self.pgops.add(
            dbops.DropTable(
                name=old_table_name,
                priority=1,
                conditions=[dbops.TableExists(old_table_name)],
            )
        )

        self.update_base_inhviews(orig_schema, context, link)
        self.schedule_inhview_deletion(orig_schema, context, link)

        return schema


class PropertyMetaCommand(CompositeObjectMetaCommand, PointerMetaCommand):

    @classmethod
    def _create_table(
            cls, prop, schema, context, conditional=False, create_bases=True,
            create_children=True):
        new_table_name = common.get_backend_name(schema, prop, catenate=False)

        create_c = dbops.CommandGroup()

        constraints = []
        columns = []

        src_col = common.edgedb_name_to_pg_name('source')

        columns.append(
            dbops.Column(
                name=src_col, type='uuid', required=True))

        id = sn.QualName(
            module=prop.get_name(schema).module, name=str(prop.id))
        index_name = common.convert_name(id, 'idx0', catenate=True)

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
                    columns=[src_col] + [tgt_col.name for tgt_col in tgt_cols]
                )
            )

        table = dbops.Table(name=new_table_name)
        table.add_columns(columns)
        table.constraints = constraints

        ct = dbops.CreateTable(table=table)

        if conditional:
            c = dbops.CommandGroup(
                neg_conditions=[dbops.TableExists(new_table_name)])
        else:
            c = dbops.CommandGroup()

        c.add_command(ct)
        c.add_command(ci)

        c.add_command(dbops.Comment(table, str(prop.get_name(schema))))

        create_c.add_command(c)

        if create_children:
            for p_descendant in prop.descendants(schema):
                if has_table(p_descendant, schema):
                    pc = PropertyMetaCommand._create_table(
                        p_descendant, schema, context, conditional=True,
                        create_bases=False, create_children=False)
                    create_c.add_command(pc)

        return create_c


class CreateProperty(PropertyMetaCommand, adapts=s_props.CreateProperty):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_props.CreateProperty.apply(self, schema, context)
        prop = self.scls
        propname = prop.get_shortname(schema).name
        schema = PropertyMetaCommand.apply(self, schema, context)

        src = context.get(s_sources.SourceCommandContext)

        self.provide_table(prop, schema, context)

        if src and has_table(src.scls, schema):
            if isinstance(src.scls, s_links.Link):
                src.op.provide_table(src.scls, schema, context)

            ptr_stor_info = types.get_pointer_storage_info(
                prop, resolve_type=False, schema=schema)

            if (
                (
                    not isinstance(src.scls, s_objtypes.ObjectType)
                    or ptr_stor_info.table_type == 'ObjectType'
                )
                and (
                    not isinstance(src.scls, s_links.Link)
                    or propname not in {'source', 'target'}
                )
            ):
                alter_table = src.op.get_alter_table(schema, context)

                default_value = self.get_pointer_default(prop, schema, context)

                cols = self.get_columns(prop, schema, default_value)

                for col in cols:
                    cmd = dbops.AlterTableAddColumn(col)
                    alter_table.add_operation(cmd)

                    if col.name == 'id':
                        constraint = dbops.PrimaryKey(
                            table_name=alter_table.name,
                            columns=[col.name],
                        )
                        alter_table.add_operation(
                            dbops.AlterTableAddConstraint(constraint),
                        )

                self.update_lineage_inhviews(schema, context, prop)

                if has_table(src.op.scls, schema):
                    self.schedule_inhviews_update(
                        schema,
                        context,
                        src.op.scls,
                        update_descendants=True,
                    )

        return schema


class RenameProperty(
        PropertyMetaCommand, adapts=s_props.RenameProperty):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_props.RenameProperty.apply(self, schema, context)
        schema = PropertyMetaCommand.apply(self, schema, context)

        source_ctx = context.get(s_sources.SourceCommandContext)
        if source_ctx is not None:
            source = source_ctx.scls
        else:
            source = None

        if (
            source is not None
            and not context.is_deleting(source)
        ):
            self.schedule_inhviews_update(
                schema,
                context,
                source,
                update_descendants=True,
            )

        return schema


class RebaseProperty(
        PropertyMetaCommand, adapts=s_props.RebaseProperty):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = s_props.RebaseProperty.apply(self, schema, context)
        schema = PropertyMetaCommand.apply(self, schema, context)

        prop_ctx = context.get(s_props.PropertyCommandContext)
        source = prop_ctx.scls

        if has_table(source, schema):
            self.update_base_inhviews_on_rebase(
                schema, orig_schema, context, source)

        return schema


class SetPropertyType(
        PropertyMetaCommand, adapts=s_props.SetPropertyType):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_props.SetPropertyType.apply(self, schema, context)
        return PropertyMetaCommand.apply(self, schema, context)


class AlterPropertyUpperCardinality(
    PropertyMetaCommand,
    adapts=s_props.AlterPropertyUpperCardinality,
):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_props.AlterPropertyUpperCardinality.apply(
            self, schema, context)
        return PropertyMetaCommand.apply(self, schema, context)


class AlterPropertyOwned(
    PropertyMetaCommand,
    AlterObject,
    adapts=s_props.AlterPropertyOwned,
):
    pass


class AlterProperty(
        PropertyMetaCommand, adapts=s_props.AlterProperty):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = s_props.AlterProperty.apply(self, schema, context)
        prop = self.scls
        schema = PropertyMetaCommand.apply(self, schema, context)

        if self.metadata_only:
            return schema

        if prop.is_pure_computable(orig_schema):
            return schema

        with context(
                s_props.PropertyCommandContext(schema, self, prop)) as ctx:
            ctx.original_schema = orig_schema

            self.provide_table(prop, schema, context)
            prop_target = prop.get_target(schema)
            old_prop_target = prop.get_target(orig_schema)

            prop_required = prop.get_required(schema)
            old_prop_required = prop.get_required(orig_schema)

            if (isinstance(prop_target, s_scalars.ScalarType) and
                    isinstance(old_prop_target, s_scalars.ScalarType) and
                    prop_required != old_prop_required):

                src_ctx = context.get(s_sources.SourceCommandContext)
                src_op = src_ctx.op
                alter_table = src_op.get_alter_table(schema, context)
                ptr_stor_info = types.get_pointer_storage_info(
                    prop, schema=schema)
                alter_table.add_operation(
                    dbops.AlterTableAlterColumnNull(
                        column_name=ptr_stor_info.column_name,
                        null=not prop.get_required(schema)))

            self.alter_pointer_default(prop, schema, context)

            if not prop.generic(schema):
                self.adjust_pointer_storage(prop, schema, orig_schema, context)

        return schema


class DeleteProperty(
        PropertyMetaCommand, adapts=s_props.DeleteProperty):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        prop = schema.get(self.classname)

        schema = s_props.DeleteProperty.apply(self, schema, context)
        schema = PropertyMetaCommand.apply(self, schema, context)

        source_ctx = context.get(s_sources.SourceCommandContext)
        if source_ctx is not None:
            source = source_ctx.scls
            source_op = source_ctx.op
        else:
            source = source_op = None

        if (source
                and not source.getptr(
                    schema, prop.get_shortname(orig_schema).name)
                and has_table(source, schema)):

            self.pgops.add(
                self.drop_inhview(schema, context, source, drop_ancestors=True)
            )
            alter_table = source_op.get_alter_table(
                schema, context, force_new=True)
            ptr_stor_info = types.get_pointer_storage_info(
                prop,
                schema=orig_schema,
                link_bias=prop.is_link_property(orig_schema),
            )

            if ptr_stor_info.table_type == 'ObjectType':
                col = dbops.AlterTableDropColumn(
                    dbops.Column(name=ptr_stor_info.column_name,
                                 type=ptr_stor_info.column_type))

                alter_table.add_operation(col)

        if has_table(prop, orig_schema):
            self.pgops.add(
                self.drop_inhview(
                    orig_schema, context, prop, drop_ancestors=True)
            )
            old_table_name = common.get_backend_name(
                orig_schema, prop, catenate=False)
            self.pgops.add(dbops.DropTable(name=old_table_name, priority=1))
            self.update_base_inhviews(orig_schema, context, prop)
            self.schedule_inhview_deletion(orig_schema, context, prop)

        if (
            source is not None
            and not context.is_deleting(source)
        ):
            self.schedule_inhviews_update(
                schema,
                context,
                source,
                update_descendants=True,
            )

        return schema


class UpdateEndpointDeleteActions(MetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.link_ops = []

    def _get_link_table_union(self, schema, links) -> str:
        selects = []
        for link in links:
            selects.append(textwrap.dedent('''\
                (SELECT
                    {id}::uuid AS __sobj_id__,
                    {src} as source,
                    {tgt} as target
                FROM {table})
            ''').format(
                id=ql(str(link.id)),
                src=common.quote_ident('source'),
                tgt=common.quote_ident('target'),
                table=common.get_backend_name(schema, link),
            ))

        return '(' + '\nUNION ALL\n    '.join(selects) + ') as q'

    def _get_inline_link_table_union(self, schema, links) -> str:
        selects = []
        for link in links:
            link_psi = types.get_pointer_storage_info(link, schema=schema)
            link_col = link_psi.column_name
            selects.append(textwrap.dedent('''\
                (SELECT
                    {id}::uuid AS __sobj_id__,
                    {src} as source,
                    {tgt} as target
                FROM {table})
            ''').format(
                id=ql(str(link.id)),
                src=common.quote_ident('id'),
                tgt=common.quote_ident(link_col),
                table=common.get_backend_name(
                    schema,
                    link.get_source(schema),
                    aspect='inhview',
                ),
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
            groups = [(DA.Allow, links)]
            near_endpoint, far_endpoint = 'source', 'target'

        for action, links in groups:
            if action is DA.Restrict or action is DA.DeferredRestrict:
                tables = self._get_link_table_union(schema, links)

                text = textwrap.dedent('''\
                    SELECT
                        q.__sobj_id__, q.source, q.target
                        INTO link_type_id, srcid, tgtid
                    FROM
                        {tables}
                    WHERE
                        q.{near_endpoint} = OLD.{id}
                    LIMIT 1;

                    IF FOUND THEN
                        SELECT
                            edgedb.shortname_from_fullname(link.name),
                            edgedb._get_schema_object_name(link.{far_endpoint})
                            INTO linkname, endname
                        FROM
                            edgedb."_SchemaLink" AS link
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

            elif action == s_links.LinkTargetDeleteAction.Allow:
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

            elif action == s_links.LinkTargetDeleteAction.DeleteSource:
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
            if action is DA.Restrict or action is DA.DeferredRestrict:
                tables = self._get_inline_link_table_union(schema, links)

                text = textwrap.dedent('''\
                    SELECT
                        q.__sobj_id__, q.source, q.target
                        INTO link_type_id, srcid, tgtid
                    FROM
                        {tables}
                    WHERE
                        q.{near_endpoint} = OLD.{id}
                    LIMIT 1;

                    IF FOUND THEN
                        SELECT
                            edgedb.shortname_from_fullname(link.name),
                            edgedb._get_schema_object_name(link.{far_endpoint})
                            INTO linkname, endname
                        FROM
                            edgedb."_SchemaLink" AS link
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

            elif action == s_links.LinkTargetDeleteAction.Allow:
                for link in links:
                    link_psi = types.get_pointer_storage_info(
                        link, schema=schema)
                    link_col = link_psi.column_name
                    source_table = common.get_backend_name(
                        schema, link.get_source(schema))

                    text = textwrap.dedent(f'''\
                        UPDATE
                            {source_table}
                        SET
                            {qi(link_col)} = NULL
                        WHERE
                            {qi(link_col)} = OLD.id;
                    ''')

                    chunks.append(text)

            elif action == s_links.LinkTargetDeleteAction.DeleteSource:
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

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if not self.link_ops:
            return schema

        DA = s_links.LinkTargetDeleteAction

        affected_sources = set()
        affected_targets = set()
        deletions = False

        for link_op, link, orig_schema in self.link_ops:
            if isinstance(link_op, DeleteLink):
                if (link.generic(orig_schema)
                        or not link.get_is_owned(orig_schema)
                        or link.is_pure_computable(orig_schema)):
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
                if (
                    link.generic(schema)
                    or not link.get_is_owned(schema)
                    or link.is_pure_computable(schema)
                ):
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

            for link in source.get_pointers(src_schema).objects(src_schema):
                if (not isinstance(link, s_links.Link)
                        or not link.get_is_owned(src_schema)
                        or link.is_pure_computable(src_schema)):
                    continue
                ptr_stor_info = types.get_pointer_storage_info(
                    link, schema=src_schema)
                if ptr_stor_info.table_type != 'link':
                    continue

                links.append(link)

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

            for link in schema.get_referrers(target, scls_type=s_links.Link,
                                             field_name='target'):
                if (not link.get_is_owned(schema)
                        or link.is_pure_computable(schema)):
                    continue
                source = link.get_source(schema)
                if source.is_view(schema):
                    continue
                ptr_stor_info = types.get_pointer_storage_info(
                    link, schema=schema)
                if ptr_stor_info.table_type != 'link':
                    if (link.get_on_target_delete(schema)
                            is DA.DeferredRestrict):
                        deferred_inline_links.append(link)
                    else:
                        inline_links.append(link)
                else:
                    if (link.get_on_target_delete(schema)
                            is DA.DeferredRestrict):
                        deferred_links.append(link)
                    else:
                        links.append(link)

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

        return schema

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

        all_objtypes = set()
        for objtype in objtypes:
            all_objtypes.add(objtype)
            for descendant in objtype.descendants(schema):
                if has_table(descendant, schema):
                    all_objtypes.add(descendant)

        for objtype in all_objtypes:
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


@dataclasses.dataclass
class InheritanceViewUpdate:

    update_ancestors: bool = True
    update_descendants: bool = False


class UpdateInheritanceViews(MetaCommand):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.view_updates = {}
        self.view_deletions = {}

    def apply(self, schema, context):
        all_updates = set()

        for obj, update_info in self.view_updates.items():
            if not schema.has_object(obj.id):
                continue

            all_updates.add(obj)
            if update_info.update_ancestors:
                all_updates.update(obj.get_ancestors(schema).objects(schema))
            if update_info.update_descendants:
                all_updates.update(obj.descendants(schema))

        graph = {}
        for obj in all_updates:
            objname = obj.get_name(schema)
            graph[objname] = topological.DepGraphEntry(
                item=obj,
                deps=obj.get_bases(schema).names(schema),
                extra=False,
            )

        ordered = topological.sort(graph, allow_unresolved=True)
        for obj in reversed(list(ordered)):
            if has_table(obj, schema):
                self.update_inhview(schema, obj)

        for obj, obj_schema in self.view_deletions.items():
            self.delete_inhview(obj_schema, obj)

    def _get_select_from(self, schema, obj, ptrnames):
        if isinstance(obj, s_sources.Source):
            ptrs = dict(obj.get_pointers(schema).items(schema))

            cols = []

            for ptrname, alias in ptrnames.items():
                ptr = ptrs[ptrname]
                ptr_stor_info = types.get_pointer_storage_info(
                    ptr,
                    link_bias=isinstance(obj, s_links.Link),
                    schema=schema,
                )
                cols.append((ptr_stor_info.column_name, alias))
        else:
            cols = list(ptrnames.items())

        coltext = ',\n'.join(
            f'{qi(col)} AS {qi(alias)}' for col, alias in cols)

        tabname = common.get_backend_name(
            schema,
            obj,
            catenate=False,
            aspect='table',
        )

        return textwrap.dedent(f'''\
            (SELECT
               {coltext}
             FROM
               {q(*tabname)}
            )
        ''')

    def update_inhview(self, schema, obj):
        inhview_name = common.get_backend_name(
            schema, obj, catenate=False, aspect='inhview')

        ptrs = {}

        if isinstance(obj, s_sources.Source):
            pointers = list(obj.get_pointers(schema).items(schema))
            pointers.sort(key=lambda p: p[1].id)
            for ptrname, ptr in pointers:
                ptr_stor_info = types.get_pointer_storage_info(
                    ptr,
                    link_bias=isinstance(obj, s_links.Link),
                    schema=schema,
                )
                if (
                    isinstance(obj, s_links.Link)
                    or ptr_stor_info.table_type == 'ObjectType'
                ):
                    ptrs[ptrname] = ptr_stor_info.column_name

        else:
            # MULTI PROPERTY
            ptrs['source'] = 'source'
            ptrs['target'] = 'target'

        components = [self._get_select_from(schema, obj, ptrs)]

        components.extend(
            self._get_select_from(schema, descendant, ptrs)
            for descendant in obj.descendants(schema)
            if has_table(descendant, schema)
        )

        query = '\nUNION ALL\n'.join(components)

        view = dbops.View(
            name=inhview_name,
            query=query,
        )

        self.pgops.add(
            dbops.DropView(
                inhview_name,
                priority=1,
                conditions=[dbops.ViewExists(inhview_name)],
            ),
        )

        self.pgops.add(
            dbops.CreateView(
                view=view,
                priority=1,
            ),
        )

    def delete_inhview(self, schema, obj):
        inhview_name = common.get_backend_name(
            schema, obj, catenate=False, aspect='inhview')
        self.pgops.add(
            dbops.DropView(
                inhview_name,
                conditions=[dbops.ViewExists(inhview_name)],
                priority=1,
            ),
        )


class ModuleMetaCommand(ObjectMetaCommand):
    pass


class CreateModule(ModuleMetaCommand, adapts=s_mod.CreateModule):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = CompositeObjectMetaCommand.apply(self, schema, context)
        return s_mod.CreateModule.apply(self, schema, context)


class AlterModule(ModuleMetaCommand, adapts=s_mod.AlterModule):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_mod.AlterModule.apply(self, schema, context=context)
        return CompositeObjectMetaCommand.apply(self, schema, context)


class DeleteModule(ModuleMetaCommand, adapts=s_mod.DeleteModule):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = CompositeObjectMetaCommand.apply(self, schema, context)
        return s_mod.DeleteModule.apply(self, schema, context)


class CreateDatabase(ObjectMetaCommand, adapts=s_db.CreateDatabase):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_db.CreateDatabase.apply(self, schema, context)
        db = self.scls
        self.pgops.add(
            dbops.CreateDatabase(
                dbops.Database(
                    str(self.classname),
                    metadata=dict(
                        id=str(db.id),
                        builtin=db.get_builtin(schema),
                    ),
                    template=self.template,
                )
            )
        )
        return schema


class DropDatabase(ObjectMetaCommand, adapts=s_db.DropDatabase):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_db.DropDatabase.apply(self, schema, context)
        self.pgops.add(dbops.DropDatabase(str(self.classname)))
        return schema


class CreateRole(ObjectMetaCommand, adapts=s_roles.CreateRole):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_roles.CreateRole.apply(self, schema, context)
        role = self.scls
        schema = ObjectMetaCommand.apply(self, schema, context)

        membership = list(role.get_bases(schema).names(schema))
        passwd = role.get_password(schema)
        superuser_flag = False

        members = set()

        ctx_backend_params = context.backend_runtime_params
        if ctx_backend_params is not None:
            backend_params = cast(
                pgcluster.BackendRuntimeParams, ctx_backend_params)
        else:
            backend_params = pgcluster.get_default_runtime_params()

        instance_params = backend_params.instance_params
        capabilities = instance_params.capabilities

        if role.get_is_superuser(schema):
            if instance_params.base_superuser:
                # If the cluster is exposing an explicit superuser role,
                # become a member of that instead of creating a superuser
                # role directly.
                membership.append(instance_params.base_superuser)
            else:
                superuser_flag = (
                    capabilities
                    & pgcluster.BackendCapabilities.SUPERUSER_ACCESS
                )

        if backend_params.session_authorization_role is not None:
            # When we connect to the backend via a proxy role, we
            # must ensure that role is a member of _every_ EdgeDB
            # role so that `SET ROLE` can work properly.
            members.add(backend_params.session_authorization_role)

        role = dbops.Role(
            name=str(role.get_name(schema)),
            allow_login=True,
            is_superuser=superuser_flag,
            password=passwd,
            membership=membership,
            metadata=dict(
                id=str(role.id),
                password_hash=passwd,
                builtin=role.get_builtin(schema),
            ),
        )
        self.pgops.add(dbops.CreateRole(role))
        return schema


class AlterRole(ObjectMetaCommand, adapts=s_roles.AlterRole):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_roles.AlterRole.apply(self, schema, context)
        role = self.scls
        schema = ObjectMetaCommand.apply(self, schema, context)
        rolname = str(role.get_name(schema))
        kwargs = {}
        if self.has_attribute_value('password'):
            passwd = self.get_attribute_value('password')
            kwargs['password'] = passwd
            kwargs['metadata'] = dict(
                id=str(role.id),
                password_hash=passwd,
                builtin=role.get_builtin(schema),
            )
        if self.has_attribute_value('is_superuser'):
            ctx_backend_params = context.backend_runtime_params
            if ctx_backend_params is not None:
                backend_params = cast(
                    pgcluster.BackendRuntimeParams, ctx_backend_params)
            else:
                backend_params = pgcluster.get_default_runtime_params()

            instance_params = backend_params.instance_params
            capabilities = instance_params.capabilities

            superuser_flag = False
            if instance_params.base_superuser:
                # If the cluster is exposing an explicit superuser role,
                # become a member of that instead of creating a superuser
                # role directly.
                membership = list(role.get_bases(schema).names(schema))
                membership.append(instance_params.base_superuser)

                self.pgops.add(
                    dbops.AlterRoleAddMembership(
                        name=rolname,
                        membership=membership,
                    )
                )
            else:
                superuser_flag = (
                    capabilities
                    & pgcluster.BackendCapabilities.SUPERUSER_ACCESS
                )

            kwargs['is_superuser'] = superuser_flag

        dbrole = dbops.Role(name=rolname, **kwargs)
        self.pgops.add(dbops.AlterRole(dbrole))

        return schema


class RebaseRole(ObjectMetaCommand, adapts=s_roles.RebaseRole):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_roles.RebaseRole.apply(self, schema, context)
        role = self.scls
        schema = ObjectMetaCommand.apply(self, schema, context)

        for dropped in self.removed_bases:
            self.pgops.add(dbops.AlterRoleDropMember(
                name=str(dropped.name),
                member=str(role.get_name(schema)),
            ))

        for bases, _pos in self.added_bases:
            for added in bases:
                self.pgops.add(dbops.AlterRoleAddMember(
                    name=str(added.name),
                    member=str(role.get_name(schema)),
                ))

        return schema


class DeleteRole(ObjectMetaCommand, adapts=s_roles.DeleteRole):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_roles.DeleteRole.apply(self, schema, context)
        schema = ObjectMetaCommand.apply(self, schema, context)
        self.pgops.add(dbops.DropRole(str(self.classname)))
        return schema


class DeltaRoot(MetaCommand, adapts=sd.DeltaRoot):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._renames = {}

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        self.update_endpoint_delete_actions = UpdateEndpointDeleteActions()
        self.update_inhviews = UpdateInheritanceViews()

        schema = sd.DeltaRoot.apply(self, schema, context)
        schema = MetaCommand.apply(self, schema, context)

        self.update_endpoint_delete_actions.apply(schema, context)
        self.pgops.add(self.update_endpoint_delete_actions)

        self.update_inhviews.apply(schema, context)
        self.pgops.add(self.update_inhviews)

        return schema

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


class MigrationCommand(ObjectMetaCommand):
    pass


class CreateMigration(
    MigrationCommand,
    CreateObject,
    adapts=s_migrations.CreateMigration,
):
    pass


class AlterMigration(
    MigrationCommand,
    AlterObject,
    adapts=s_migrations.AlterMigration,
):
    pass


class DeleteMigration(
    MigrationCommand,
    DeleteObject,
    adapts=s_migrations.DeleteMigration,
):
    pass
