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
from edb.schema import extensions as s_exts
from edb.schema import functions as s_funcs
from edb.schema import indexes as s_indexes
from edb.schema import links as s_links
from edb.schema import properties as s_props
from edb.schema import migrations as s_migrations
from edb.schema import modules as s_mod
from edb.schema import name as sn
from edb.schema import objects as so
from edb.schema import operators as s_opers
from edb.schema import pointers as s_pointers
from edb.schema import pseudo as s_pseudo
from edb.schema import roles as s_roles
from edb.schema import sources as s_sources
from edb.schema import types as s_types
from edb.schema import version as s_ver
from edb.schema import utils as s_utils

from edb.common import markup
from edb.common import ordered
from edb.common import topological
from edb.common import uuidgen

from edb.ir import pathid as irpathid
from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from edb.pgsql import common
from edb.pgsql import dbops

from edb.server import defines as edbdef
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
        node = markup.elements.lang.TreeNode(name=str(self))

        for dd in self.pgops:
            if isinstance(dd, AlterObjectProperty):
                diff = markup.elements.doc.ValueDiff(
                    before=repr(dd.old_value), after=repr(dd.new_value))

                if dd.new_inherited:
                    diff.comment = 'inherited'
                elif dd.new_computed:
                    diff.comment = 'computed'

                node.add_child(label=dd.property, node=diff)
            else:
                node.add_child(node=markup.serialize(dd, ctx=ctx))

        return node

    def _get_backend_params(
        self,
        context: sd.CommandContext,
    ) -> pgcluster.BackendRuntimeParams:

        ctx_backend_params = context.backend_runtime_params
        if ctx_backend_params is not None:
            backend_params = cast(
                pgcluster.BackendRuntimeParams, ctx_backend_params)
        else:
            backend_params = pgcluster.get_default_runtime_params()

        return backend_params

    def _get_instance_params(
        self,
        context: sd.CommandContext,
    ) -> pgcluster.BackendInstanceParams:
        return self._get_backend_params(context).instance_params

    def _get_tenant_id(self, context: sd.CommandContext) -> str:
        return self._get_instance_params(context).tenant_id


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


class Nop(MetaCommand, adapts=sd.Nop):
    pass


class Query(MetaCommand, adapts=sd.Query):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)

        sql_tree = compiler.compile_ir_to_sql_tree(
            self.expr.irast,
            output_format=compiler.OutputFormat.NATIVE_INTERNAL,
            explicit_top_cast=irtyputils.type_to_typeref(
                schema,
                schema.get('std::str'),
            ),
        )

        sql_text = codegen.generate_source(sql_tree)

        # The INTO _dummy_text bit is needed because PL/pgSQL _really_
        # wants the result of a returning query to be stored in a variable,
        # and the PERFORM hack does not work if the query has DML CTEs.
        self.pgops.add(dbops.Query(
            text=f'{sql_text} INTO _dummy_text',
        ))

        return schema


class AlterObjectProperty(MetaCommand, adapts=sd.AlterObjectProperty):
    pass


class SchemaVersionCommand(ObjectMetaCommand):
    pass


class CreateSchemaVersion(
    SchemaVersionCommand,
    CreateObject,
    adapts=s_ver.CreateSchemaVersion,
):
    pass


class AlterSchemaVersion(
    SchemaVersionCommand,
    AlterObject,
    adapts=s_ver.AlterSchemaVersion,
):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        expected_ver = self.get_orig_attribute_value('version')
        check = dbops.Query(
            f'''
                SELECT
                    edgedb.raise_on_not_null(
                        (SELECT NULLIF(
                            (SELECT
                                version::text
                            FROM
                                edgedb."_SchemaSchemaVersion"
                            FOR UPDATE),
                            {ql(str(expected_ver))}
                        )),
                        'serialization_failure',
                        msg => (
                            'Cannot serialize DDL: '
                            || (SELECT version::text FROM
                                edgedb."_SchemaSchemaVersion")
                        )
                    )
                INTO _dummy_text
            '''
        )
        self.pgops.add(check)
        return schema


class GlobalSchemaVersionCommand(ObjectMetaCommand):
    pass


class CreateGlobalSchemaVersion(
    ObjectMetaCommand,
    adapts=s_ver.CreateGlobalSchemaVersion,
):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_ver.CreateGlobalSchemaVersion.apply(self, schema, context)
        schema = ObjectMetaCommand.apply(self, schema, context)
        ver_id = str(self.scls.id)
        ver_name = str(self.scls.get_name(schema))
        tenant_id = self._get_tenant_id(context)
        self.pgops.add(
            dbops.UpdateMetadataSection(
                dbops.Database(name=common.get_database_backend_name(
                    edbdef.EDGEDB_TEMPLATE_DB, tenant_id=tenant_id)),
                section='GlobalSchemaVersion',
                metadata={
                    ver_id: {
                        'id': ver_id,
                        'name': ver_name,
                        'version': str(self.scls.get_version(schema)),
                        'builtin': self.scls.get_builtin(schema),
                        'internal': self.scls.get_internal(schema),
                    }
                }
            )
        )

        return schema


class AlterGlobalSchemaVersion(
    ObjectMetaCommand,
    adapts=s_ver.AlterGlobalSchemaVersion,
):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_ver.AlterGlobalSchemaVersion.apply(self, schema, context)
        schema = ObjectMetaCommand.apply(self, schema, context)
        ver_id = str(self.scls.id)
        ver_name = str(self.scls.get_name(schema))

        ctx_backend_params = context.backend_runtime_params
        if ctx_backend_params is not None:
            backend_params = cast(
                pgcluster.BackendRuntimeParams, ctx_backend_params)
        else:
            backend_params = pgcluster.get_default_runtime_params()

        instance_params = backend_params.instance_params
        capabilities = instance_params.capabilities
        tenant_id = instance_params.tenant_id

        tpl_db_name = common.get_database_backend_name(
            edbdef.EDGEDB_TEMPLATE_DB, tenant_id=tenant_id)

        if capabilities & pgcluster.BackendCapabilities.SUPERUSER_ACCESS:
            # Only superusers are generally allowed to make an UPDATE
            # lock on shared catalogs.
            lock = dbops.Query(
                f'''
                    SELECT
                        description
                    FROM
                        pg_catalog.pg_shdescription
                    WHERE
                        objoid = (
                            SELECT oid
                            FROM pg_database
                            WHERE datname = {ql(tpl_db_name)}
                        )
                        AND classoid = 'pg_database'::regclass::oid
                    FOR UPDATE
                    INTO _dummy_text
                '''
            )
        else:
            # Without superuser access we have to resort to lock polling.
            # This is racy, but is unfortunately the best we can do.
            lock = dbops.Query(f'''
                SELECT
                    edgedb.raise_on_not_null(
                        (
                            SELECT 'locked'
                            FROM pg_catalog.pg_locks
                            WHERE
                                locktype = 'object'
                                AND classid = 'pg_database'::regclass::oid
                                AND objid = (
                                    SELECT oid
                                    FROM pg_database
                                    WHERE
                                        datname = {ql(tpl_db_name)}
                                )
                                AND mode = 'ShareUpdateExclusiveLock'
                                AND granted
                                AND pid != pg_backend_pid()
                        ),
                        'serialization_failure',
                        msg => (
                            'Cannot serialize global DDL: '
                            || (SELECT version::text FROM
                                edgedb."_SysGlobalSchemaVersion")
                        )
                    )
                INTO _dummy_text
            ''')

        self.pgops.add(lock)

        expected_ver = self.get_orig_attribute_value('version')
        check = dbops.Query(
            f'''
                SELECT
                    edgedb.raise_on_not_null(
                        (SELECT NULLIF(
                            (SELECT
                                version::text
                            FROM
                                edgedb."_SysGlobalSchemaVersion"
                            ),
                            {ql(str(expected_ver))}
                        )),
                        'serialization_failure',
                        msg => (
                            'Cannot serialize global DDL: '
                            || (SELECT version::text FROM
                                edgedb."_SysGlobalSchemaVersion")
                        )
                    )
                INTO _dummy_text
            '''
        )
        self.pgops.add(check)

        self.pgops.add(
            dbops.UpdateMetadataSection(
                dbops.Database(name=tpl_db_name),
                section='GlobalSchemaVersion',
                metadata={
                    ver_id: {
                        'id': ver_id,
                        'name': ver_name,
                        'version': str(self.scls.get_version(schema)),
                        'builtin': self.scls.get_builtin(schema),
                        'internal': self.scls.get_internal(schema),
                    }
                }
            )
        )

        return schema


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


class AlterTuple(TupleCommand, adapts=s_types.AlterTuple):
    pass


class RenameTuple(TupleCommand, adapts=s_types.RenameTuple):
    pass


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


class AlterArray(ArrayCommand, adapts=s_types.AlterArray):
    pass


class RenameArray(ArrayCommand, adapts=s_types.RenameArray):
    pass


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

    def fix_return_type(
            self, func: s_funcs.Function, nativecode, schema, context):

        return_type = self._get_attribute_value(schema, context, 'return_type')
        ir = nativecode.irast

        if not (
            return_type.is_object_type()
            or s_types.is_type_compatible(return_type, ir.stype,
                                          schema=nativecode.schema)
        ):
            # Add a cast and recompile it
            qlexpr = qlcompiler.astutils.ensure_qlstmt(ql_ast.TypeCast(
                type=s_utils.typeref_to_ast(schema, return_type),
                expr=nativecode.qlast,
            ))
            nativecode = self.compile_function(
                schema, context, type(nativecode).from_ast(qlexpr, schema))

        return nativecode

    def compile_edgeql_function(self, func: s_funcs.Function, schema, context):
        nativecode = func.get_nativecode(schema)
        if nativecode.irast is None:
            nativecode = self.compile_function(schema, context, nativecode)

        nativecode = self.fix_return_type(func, nativecode, schema, context)

        sql_text, _ = compiler.compile_ir_to_sql(
            nativecode.irast,
            ignore_shapes=True,
            explicit_top_cast=irtyputils.type_to_typeref(  # note: no cache
                schema, func.get_return_type(schema)),
            output_format=compiler.OutputFormat.NATIVE,
            use_named_params=True)

        return self.make_function(func, sql_text, schema)

    def sql_rval_consistency_check(
        self,
        cobj: s_funcs.CallableObject,
        expr: str,
        schema: s_schema.Schema,
    ) -> dbops.Command:
        fname = cobj.get_verbosename(schema)
        rtype = types.pg_type_from_object(
            schema,
            cobj.get_return_type(schema),
            persistent_tuples=True,
        )
        rtype_desc = '.'.join(rtype)

        # Determine the actual returned type of the SQL function.
        # We can't easily do this by looking in system catalogs because
        # of polymorphic dispatch, but, fortunately, there's pg_typeof().
        # We only need to be sure to actually NOT call the target function,
        # as we can't assume how it'll behave with dummy inputs. Hence, the
        # weird looking query below, where we rely in Postgres executor to
        # skip the call, because no rows satisfy the WHERE condition, but
        # we then still generate a NULL row via a LEFT JOIN.
        f_test = textwrap.dedent(f'''\
            (SELECT
                pg_typeof(f.i)
            FROM
                (SELECT NULL::text) AS spreader
                LEFT JOIN (SELECT {expr} WHERE False) AS f(i) ON (true))''')

        check = dbops.Query(text=f'''
            PERFORM
                edgedb.raise_on_not_null(
                    NULLIF(
                        pg_typeof(NULL::{qt(rtype)}),
                        {f_test}
                    ),
                    'invalid_function_definition',
                    msg => format(
                        '%s is declared to return SQL type "%s", but '
                        || 'the underlying SQL function returns "%s"',
                        {ql(fname)},
                        {ql(rtype_desc)},
                        {f_test}::text
                    ),
                    hint => (
                        'Declare the function with '
                        || '`force_return_cast := true`, '
                        || 'or add an explicit cast to its body.'
                    )
                );
        ''')

        return check

    def get_dummy_func_call(
        self,
        cobj: s_funcs.CallableObject,
        sql_func: str,
        schema: s_schema.Schema,
    ) -> str:
        args = []
        func_params = cobj.get_params(schema)
        for param in func_params.get_in_canonical_order(schema):
            param_type = param.get_type(schema)
            pg_at = self.get_pgtype(cobj, param_type, schema)
            args.append(f'NULL::{qt(pg_at)}')

        return f'{sql_func}({", ".join(args)})'

    def make_op(
        self,
        func: s_funcs.Function,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        or_replace: bool=False,
    ) -> Iterable[dbops.Command]:
        if func.get_from_expr(schema):
            # Intrinsic function, handled directly by the compiler.
            return ()
        elif sql_func := func.get_from_function(schema):
            func_params = func.get_params(schema)

            if (
                func.get_force_return_cast(schema)
                or func_params.has_polymorphic(schema)
                or func.get_sql_func_has_out_params(schema)
            ):
                return ()
            else:
                # Function backed directly by an SQL function.
                # Check the consistency of the return type.
                dexpr = self.get_dummy_func_call(func, sql_func, schema)
                check = self.sql_rval_consistency_check(func, dexpr, schema)
                return (check,)
        else:
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
            return (op,)


class CreateFunction(FunctionCommand, CreateObject,
                     adapts=s_funcs.CreateFunction):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        self.pgops.update(self.make_op(self.scls, schema, context))
        return schema


class RenameFunction(
        FunctionCommand, RenameObject, adapts=s_funcs.RenameFunction):
    pass


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
            self.pgops.update(
                self.make_op(self.scls, schema, context, or_replace=True))

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

    def get_dummy_operator_call(
        self,
        oper: s_opers.Operator,
        pgop: str,
        from_args: Tuple[Tuple[str, ...], ...],
        schema: s_schema.Schema,
    ) -> str:
        # Need a proxy function with casts
        oper_kind = oper.get_operator_kind(schema)

        if oper_kind is ql_ft.OperatorKind.Infix:
            op = f'NULL::{qt(from_args[0])} {pgop} NULL::{qt(from_args[1])}'
        elif oper_kind is ql_ft.OperatorKind.Postfix:
            op = f'NULL::{qt(from_args[0])} {pgop}'
        elif oper_kind is ql_ft.OperatorKind.Prefix:
            op = f'{pgop} NULL::{qt(from_args[1])}'
        else:
            raise RuntimeError(f'unexpected operator kind: {oper_kind!r}')

        return op


class CreateOperator(OperatorCommand, CreateObject,
                     adapts=s_opers.CreateOperator):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        oper = self.scls
        if oper.get_abstract(schema):
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

                if oper_func_name is not None:
                    cexpr = self.get_dummy_func_call(
                        oper, oper_func_name, schema)
                else:
                    cexpr = self.get_dummy_operator_call(
                        oper, pg_oper_name, from_args, schema)

                check = self.sql_rval_consistency_check(oper, cexpr, schema)
                self.pgops.add(check)

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

            cexpr = self.get_dummy_func_call(
                oper, q(*oper_func.name), schema)
            check = self.sql_rval_consistency_check(oper, cexpr, schema)
            self.pgops.add(check)

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

        if oper.get_abstract(schema):
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


class AlterAnnotationValueOwned(
        AnnotationValueCommand, AlterObject,
        adapts=s_anno.AlterAnnotationValueOwned):
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

    @classmethod
    def constraint_is_effective(cls, schema, constraint):
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

    @classmethod
    def create_constraint(
            cls, constraint, schema, context, source_context=None):
        if cls.constraint_is_effective(schema, constraint):
            subject = constraint.get_subject(schema)

            if subject is not None:
                schemac_to_backendc = \
                    schemamech.ConstraintMech.\
                    schema_constraint_to_backend_constraint
                bconstr = schemac_to_backendc(
                    subject, constraint, schema, context,
                    source_context)

                return bconstr.create_ops()
        else:
            return dbops.CommandGroup()

    @classmethod
    def delete_constraint(
            cls, constraint, schema, context, source_context=None):
        op = dbops.CommandGroup()
        if cls.constraint_is_effective(schema, constraint):
            subject = constraint.get_subject(schema)

            if subject is not None:
                schemac_to_backendc = \
                    schemamech.ConstraintMech.\
                    schema_constraint_to_backend_constraint
                bconstr = schemac_to_backendc(
                    subject, constraint, schema, context,
                    source_context)

                op.add_command(bconstr.delete_ops())

        return op


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

        op = self.create_constraint(
            constraint, schema, context, self.source_context)
        self.pgops.add(op)

        return schema


class RenameConstraint(
        ConstraintCommand, RenameObject,
        adapts=s_constr.RenameConstraint):
    pass


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

            bconstr = schemac_to_backendc(
                subject, constraint, schema, context, self.source_context)

            orig_bconstr = schemac_to_backendc(
                constraint.get_subject(orig_schema),
                constraint,
                orig_schema,
                context,
                self.source_context,
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
                        self.source_context,
                    )
                    cbconstr = schemac_to_backendc(
                        child.get_subject(schema),
                        child,
                        schema,
                        context,
                        self.source_context,
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
                        self.source_context,
                    )
                    cbconstr = schemac_to_backendc(
                        child.get_subject(schema),
                        child,
                        schema,
                        context,
                        self.source_context,
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
        op = self.delete_constraint(
            constraint, orig_schema, context, self.source_context)
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
        schema = ScalarTypeMetaCommand.apply(self, schema, context)

        return schema

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._create_begin(schema, context)
        scalar = self.scls

        if scalar.get_abstract(schema):
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


class RenameScalarType(ScalarTypeMetaCommand, RenameObject,
                       adapts=s_scalars.RenameScalarType):
    pass


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

    def _get_problematic_refs(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        composite_only: bool,
    ) -> Optional[Tuple[
        Tuple[so.Object, ...],
        List[Tuple[s_props.Property, s_types.TypeShell]],
    ]]:
        """Find problematic references to this scalar type that need handled.

        This is used to work around two irritating limitations of Postgres:
          1. That elements of enum types may not be removed or reordered
          2. That a constraint may not be added to a domain type if that
             domain type appears in a *composite* type that is used in a
             column somewhere.

        We don't want to have these limitations, and we need to do a decent
        amount of work to work around them.

        1. Find all of the affected properties. For case 2, this is any
           property whose type is a container type that contains this
           scalar. (Possibly transitively.) For case 1, the container type
           restriction is dropped.
        2. Change the type of all offending properties to an equivalent type
           that does not reference this scalar. This may require creating
           new types. (See _undo_everything.)
        3. Add the constraint.
        4. Restore the type of all offending properties. If existing data
           violates the new constraint, we will fail here. Delete any
           temporarily created types. (See _redo_everything.)

        Somewhat hackily, _undo_everything and _redo_everything
        operate by creating new schema delta command objects, and
        adapting and applying them. This is the most straightforward
        way to perform the high-level operations needed here.

        I've kept this code in pgsql/delta instead of trying to put in
        schema/delta because it is pretty aggressively an irritating
        pgsql implementation detail and because I didn't want it to
        have to interact with ordering ever.

        This function finds all of the relevant properties and returns
        a list of them along with the appropriate replacement type.

        In case 1, it also finds other referencing objects which need
        to be deleted and then recreated.
        """

        seen_props = set()
        seen_other = set()

        typ = self.scls
        # Do a worklist driven search for properties that refer to this scalar
        # through a collection type. We search backwards starting from
        # referring collection types or from all refs, depending on
        # composite_only.
        scls_type = s_types.Collection if composite_only else None
        wl = list(schema.get_referrers(typ, scls_type=scls_type))
        while wl:
            obj = wl.pop()
            if isinstance(obj, s_props.Property):
                seen_props.add(obj)
            elif isinstance(obj, s_scalars.ScalarType):
                pass
            elif isinstance(obj, s_types.Collection):
                wl.extend(schema.get_referrers(obj))
            elif isinstance(obj, s_funcs.Parameter) and not composite_only:
                wl.extend(schema.get_referrers(obj))
            elif isinstance(obj, s_funcs.Function) and not composite_only:
                wl.extend(schema.get_referrers(obj))
                seen_other.add(obj)
            elif isinstance(obj, s_constr.Constraint) and not composite_only:
                seen_other.add(obj)
            elif isinstance(obj, s_indexes.Index) and not composite_only:
                seen_other.add(obj)

        if not seen_props and not seen_other:
            return None

        props = []
        if seen_props:
            # Find a concrete ancestor to substitute in.
            if typ.is_enum(schema):
                ancestor = schema.get(sn.QualName('std', 'str'))
            else:
                for ancestor in typ.get_ancestors(schema).objects(schema):
                    if not ancestor.get_abstract(schema):
                        break
                else:
                    raise AssertionError("can't find concrete base for scalar")
            replacement_shell = ancestor.as_shell(schema)

            props = [
                (
                    prop,
                    s_utils.type_shell_substitute(
                        typ.get_name(schema),
                        replacement_shell,
                        prop.get_target(schema).as_shell(schema))
                )
                for prop in seen_props
            ]

        other = sd.sort_by_cross_refs(schema, seen_other)
        return other, props

    def _undo_everything(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        other: Tuple[so.Object, ...],
        props: List[Tuple[s_props.Property, s_types.TypeShell]],
    ) -> s_schema.Schema:
        """Rewrite the type of everything that uses this scalar dangerously.

        See _get_problematic_refs above for details.
        """

        # First we need to strip out any default value that might reference
        # one of the functions we are going to delete.
        cmd = sd.CommandGroup()
        for prop, _ in props:
            if prop.get_default(schema):
                delta_alter, cmd_alter, alter_context = prop.init_delta_branch(
                    schema, context, cmdtype=sd.AlterObject)
                cmd_alter.set_attribute_value('default', None)
                cmd.add(delta_alter)

        acmd = CommandMeta.adapt(cmd)
        schema = acmd.apply(schema, context)
        self.pgops.update(acmd.get_subcommands())

        for obj in other:
            if isinstance(obj, s_funcs.Function):
                # Force function deletions at the SQL level without ever
                # bothering to remove them from our schema.
                fc = FunctionCommand()
                variadic = obj.get_params(schema).find_variadic(schema)
                self.pgops.add(
                    dbops.DropFunction(
                        name=fc.get_pgname(obj, schema),
                        args=fc.compile_args(obj, schema),
                        has_variadic=variadic is not None,
                    )
                )
            elif isinstance(obj, s_constr.Constraint):
                self.pgops.add(
                    ConstraintCommand.delete_constraint(obj, schema, context))
            elif isinstance(obj, s_indexes.Index):
                self.pgops.add(
                    DeleteIndex.delete_index(
                        obj, schema, context, priority=0))

        cmd = sd.DeltaRoot()
        for prop, new_typ in props:
            try:
                cmd.add(new_typ.as_create_delta(schema))
            except NotImplementedError:
                pass

            delta_alter, cmd_alter, alter_context = prop.init_delta_branch(
                schema, context, cmdtype=sd.AlterObject)
            cmd_alter.set_attribute_value('target', new_typ)
            cmd_alter.set_attribute_value('default', None)
            cmd.add(delta_alter)

        cmd.apply(schema, context)

        for sub in cmd.get_subcommands():
            acmd = CommandMeta.adapt(sub)
            schema = acmd.apply(schema, context)
            self.pgops.add(acmd)

        return schema

    def _redo_everything(
        self,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
        other: Tuple[so.Object, ...],
        props: List[Tuple[s_props.Property, s_types.TypeShell]],
    ) -> s_schema.Schema:
        """Restore the type of everything that uses this scalar dangerously.

        See _get_problematic_refs above for details.
        """

        for obj in reversed(other):
            if isinstance(obj, s_funcs.Function):
                # Super hackily recreate the functions
                fc = CreateFunction(classname=obj.get_name(schema))
                for f in ('language', 'params', 'return_type'):
                    fc.set_attribute_value(f, obj.get_field_value(schema, f))
                self.pgops.update(fc.make_op(obj, schema, context))
            elif isinstance(obj, s_constr.Constraint):
                self.pgops.add(
                    ConstraintCommand.create_constraint(obj, schema, context))
            elif isinstance(obj, s_indexes.Index):
                self.pgops.add(
                    CreateIndex.create_index(obj, orig_schema, context))

        cmd = sd.DeltaRoot()

        for prop, new_typ in props:
            delta_alter, cmd_alter, alter_context = prop.init_delta_branch(
                schema, context, cmdtype=sd.AlterObject)
            cmd_alter.set_attribute_value(
                'target', prop.get_target(orig_schema))
            cmd_alter.set_attribute_value(
                'default', prop.get_default(orig_schema))
            cmd.add_prerequisite(delta_alter)

            rnew_typ = new_typ.resolve(schema)
            if delete := rnew_typ.as_type_delete_if_dead(schema):
                cmd.add(delete)

        # do an apply of the schema-level command to force it to canonicalize,
        # which prunes out duplicate deletions
        cmd.apply(schema, context)

        for sub in cmd.get_subcommands():
            acmd = CommandMeta.adapt(sub)
            schema = acmd.apply(schema, context)
            self.pgops.add(acmd)

        return schema

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = s_scalars.AlterScalarType.apply(self, schema, context)
        new_scalar = self.scls

        old_enum_values = new_scalar.get_enum_values(orig_schema)
        new_enum_values = new_scalar.get_enum_values(schema)

        # If values were deleted or reordered, we need to drop the enum
        # and recreate it.
        needs_recreate = (
            old_enum_values != new_enum_values
            and old_enum_values != new_enum_values[:len(old_enum_values)])
        has_create_constraint = bool(
            list(self.get_subcommands(type=s_constr.CreateConstraint)))

        problematic_refs = None
        if needs_recreate or has_create_constraint:
            problematic_refs = self._get_problematic_refs(
                schema, context, composite_only=not needs_recreate)
            if problematic_refs:
                other, props = problematic_refs
                schema = self._undo_everything(schema, context, other, props)

        schema = ScalarTypeMetaCommand.apply(self, schema, context)

        if new_enum_values:
            type_name = common.get_backend_name(
                schema, new_scalar, catenate=False)

            if needs_recreate:
                self.pgops.add(
                    dbops.DropEnum(name=type_name))
                self.pgops.add(dbops.CreateEnum(
                    dbops.Enum(name=type_name, values=new_enum_values)))

            elif old_enum_values != new_enum_values:
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

        if problematic_refs:
            other, props = problematic_refs
            schema = self._redo_everything(
                schema, orig_schema, context, other, props)

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

    def create_inhview(self, schema, context, obj):
        inhview = UpdateInheritanceViews.get_inhview(schema, obj)
        self.pgops.add(
            dbops.CreateView(
                view=inhview,
            ),
        )

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

    @classmethod
    def create_index(cls, index, schema, context):
        subject = index.get_subject(schema)

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
                    schema_object_context=cls.get_schema_metaclass(),
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
        return dbops.CreateIndex(pg_index, priority=3)

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = CreateObject.apply(self, schema, context)
        index = self.scls

        self.pgops.add(self.create_index(index, schema, context))

        return schema


class RenameIndex(IndexCommand, RenameObject, adapts=s_indexes.RenameIndex):
    pass


class AlterIndexOwned(
    IndexCommand,
    AlterObject,
    adapts=s_indexes.AlterIndexOwned,
):
    pass


class AlterIndex(IndexCommand, AlterObject, adapts=s_indexes.AlterIndex):
    pass


class DeleteIndex(IndexCommand, DeleteObject, adapts=s_indexes.DeleteIndex):
    @classmethod
    def delete_index(cls, index, schema, context, priority=3):
        subject = index.get_subject(schema)
        table_name = common.get_backend_name(
            schema, subject, catenate=False)
        module_name = index.get_name(schema).module
        orig_idx_name = common.get_index_backend_name(
            index.id, module_name, catenate=False)
        index = dbops.Index(
            name=orig_idx_name[1], table_name=table_name, inherit=True)
        index_exists = dbops.IndexExists(
            (table_name[0], index.name_in_catalog))
        conditions = (index_exists, ) if priority else ()
        return dbops.DropIndex(
            index, priority=priority, conditions=conditions)

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
            self.pgops.add(self.delete_index(index, orig_schema, context))

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
    def schedule_endpoint_delete_action_update(self, obj, schema, context):
        endpoint_delete_actions = context.get(
            sd.DeltaRootContext).op.update_endpoint_delete_actions
        changed_targets = endpoint_delete_actions.changed_targets
        changed_targets.add((self, obj))


class CreateObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.CreateObjectType):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_objtypes.CreateObjectType.apply(self, schema, context)
        schema = ObjectTypeMetaCommand.apply(self, schema, context)

        objtype = self.scls
        if objtype.is_compound_type(schema) or objtype.get_is_derived(schema):
            return schema

        self.update_lineage_inhviews(schema, context, objtype)

        self.attach_alter_table(context)
        self.create_inhview(schema, context, objtype)

        if self.update_search_indexes:
            schema = self.update_search_indexes.apply(schema, context)
            self.pgops.add(self.update_search_indexes)

        self.schedule_endpoint_delete_action_update(self.scls, schema, context)

        return schema

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._create_begin(schema, context)
        objtype = self.scls
        if objtype.is_compound_type(schema) or objtype.get_is_derived(schema):
            return schema
        new_table_name = common.get_backend_name(
            schema, self.scls, catenate=False)
        self.table_name = new_table_name
        columns = []
        token_col = dbops.Column(
            name='__edb_token', type='uuid', required=False)
        columns.append(token_col)

        objtype_table = dbops.Table(name=new_table_name, columns=columns)
        self.pgops.add(dbops.CreateTable(table=objtype_table))
        self.pgops.add(dbops.Comment(
            object=objtype_table,
            text=str(objtype.get_verbosename(schema)),
        ))
        return schema


class RenameObjectType(ObjectTypeMetaCommand, RenameObject,
                       adapts=s_objtypes.RenameObjectType):
    pass


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

        self.schedule_endpoint_delete_action_update(self.scls, schema, context)

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
        elif (tgt := ptr.get_target(schema)) and tgt.issubclass(
                schema, schema.get('std::sequence')):
            # TODO: replace this with a generic scalar type default
            #       using std::nextval().
            seq_name = common.quote_literal(
                common.get_backend_name(
                    schema, ptr.get_target(schema), aspect='sequence'))
            default_value = f'nextval({seq_name}::regclass)'

        return default_value

    def alter_pointer_default(self, pointer, orig_schema, schema, context):
        default_value = self.get_pointer_default(pointer, schema, context)
        if default_value is None and not (
            not orig_schema
            or pointer.get_default(orig_schema)
            or (tgt := pointer.get_target(orig_schema)) and tgt.issubclass(
                orig_schema, schema.get('std::sequence'))
        ):
            return

        source_ctx = context.get_ancestor(
            s_sources.SourceCommandContext, self)
        alter_table = source_ctx.op.get_alter_table(
            schema, context, contained=True, priority=0)

        ptr_stor_info = types.get_pointer_storage_info(
            pointer, schema=schema)
        alter_table.add_operation(
            dbops.AlterTableAlterColumnDefault(
                column_name=ptr_stor_info.column_name,
                default=default_value))

    @classmethod
    def get_columns(cls, pointer, schema, default=None, sets_required=False):
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
                    (
                        pointer.get_required(schema)
                        and not pointer.is_pure_computable(schema)
                        and not sets_required
                    ) or (
                        ptr_stor_info.table_type == 'link'
                        and not pointer.is_link_property(schema)
                    )
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

    def _alter_pointer_cardinality(
        self,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        ptr = self.scls
        ptr_stor_info = types.get_pointer_storage_info(ptr, schema=schema)
        old_ptr_stor_info = types.get_pointer_storage_info(
            ptr, schema=orig_schema)
        ptr_table = ptr_stor_info.table_type == 'link'
        is_lprop = ptr.is_link_property(schema)
        is_multi = ptr_table and not is_lprop
        is_required = ptr.get_required(schema)
        is_scalar = ptr.is_property(schema)

        ref_op = self.get_referrer_context_or_die(context).op

        if is_multi:
            if isinstance(self, sd.AlterObjectFragment):
                source_op = self.get_parent_op(context)
            else:
                source_op = self
        else:
            source_op = ref_op

        # Ignore cardinality changes resulting from the creation of
        # an overloaded pointer as there is no data yet.
        if isinstance(source_op, sd.CreateObject):
            return

        if self.conv_expr is not None:
            _, conv_sql_expr, orig_rel_alias, _ = (
                self._compile_conversion_expr(
                    pointer=ptr,
                    conv_expr=self.conv_expr,
                    schema=schema,
                    orig_schema=orig_schema,
                    context=context,
                    orig_rel_is_always_source=True,
                    target_as_singleton=False,
                )
            )

            if is_lprop:
                obj_id_ref = f'{qi(orig_rel_alias)}.source'
            else:
                obj_id_ref = f'{qi(orig_rel_alias)}.id'

            if is_required and not is_multi:
                conv_sql_expr = textwrap.dedent(f'''\
                    edgedb.raise_on_null(
                        ({conv_sql_expr}),
                        'not_null_violation',
                        msg => 'missing value for required property',
                        detail => '{{"object_id": "' || {obj_id_ref} || '"}}',
                        "column" => {ql(str(ptr.id))}
                    )
                ''')
        else:
            orig_rel_alias = f'alias_{uuidgen.uuid1mc()}'

            if not is_multi:
                raise AssertionError(
                    'explicit conversion expression was expected'
                    ' for multi->single transition'
                )
            else:
                # single -> multi
                conv_sql_expr = (
                    f'SELECT '
                    f'{qi(orig_rel_alias)}.{qi(old_ptr_stor_info.column_name)}'
                )

        tab = q(*ptr_stor_info.table_name)
        target_col = ptr_stor_info.column_name

        if not is_multi:
            # Moving from pointer table to source table.
            cols = self.get_columns(ptr, schema)
            alter_table = source_op.get_alter_table(
                schema, context, manual=True)

            for col in cols:
                cond = dbops.ColumnExists(
                    ptr_stor_info.table_name,
                    column_name=col.name,
                )
                op = (dbops.AlterTableAddColumn(col), None, (cond, ))
                alter_table.add_operation(op)

            self.pgops.add(alter_table)

            update_qry = textwrap.dedent(f'''\
                UPDATE {tab} AS {qi(orig_rel_alias)}
                SET {qi(target_col)} = ({conv_sql_expr})
            ''')
            self.pgops.add(dbops.Query(update_qry))

            if not has_table(ptr, schema):
                self.pgops.add(
                    self.drop_inhview(
                        orig_schema,
                        context,
                        source_op.scls,
                        drop_ancestors=True,
                    ),
                )

                self.pgops.add(
                    self.drop_inhview(
                        orig_schema,
                        context,
                        ptr,
                        drop_ancestors=True
                    ),
                )
                otabname = common.get_backend_name(
                    orig_schema, ptr, catenate=False)
                condition = dbops.TableExists(name=otabname)
                dt = dbops.DropTable(name=otabname, conditions=[condition])
                self.pgops.add(dt)

            self.schedule_inhviews_update(
                schema,
                context,
                source_op.scls,
                update_descendants=True,
            )
        else:
            # Moving from source table to pointer table.
            self.provide_table(ptr, schema, context)
            source = ptr.get_source(orig_schema)
            src_tab = q(*common.get_backend_name(
                orig_schema,
                source,
                catenate=False,
            ))

            update_qry = textwrap.dedent(f'''\
                INSERT INTO {tab} (source, target)
                (
                    SELECT
                        {qi(orig_rel_alias)}.id,
                        q.val
                    FROM
                        {src_tab} AS {qi(orig_rel_alias)},
                        LATERAL (
                            {conv_sql_expr}
                        ) AS q(val)
                    WHERE
                        q.val IS NOT NULL
                )
            ''')

            if not is_scalar:
                update_qry += 'ON CONFLICT (source, target) DO NOTHING'

            self.pgops.add(dbops.Query(update_qry))

            check_qry = textwrap.dedent(f'''\
                SELECT
                    edgedb.raise(
                        NULL::text,
                        'not_null_violation',
                        msg => 'missing value for required property',
                        detail => '{{"object_id": "' || id || '"}}',
                        "column" => {ql(str(ptr.id))}
                    )
                FROM {src_tab}
                WHERE id != ALL (SELECT source FROM {tab})
                LIMIT 1
                INTO _dummy_text;
            ''')

            self.pgops.add(dbops.Query(check_qry))

            self.pgops.add(
                self.drop_inhview(
                    orig_schema,
                    context,
                    ref_op.scls,
                    drop_ancestors=True,
                ),
            )

            ref_op = self.get_referrer_context_or_die(context).op
            alter_table = ref_op.get_alter_table(
                schema, context, manual=True)
            col = dbops.Column(
                name=old_ptr_stor_info.column_name,
                type=common.qname(*old_ptr_stor_info.column_type),
            )
            alter_table.add_operation(dbops.AlterTableDropColumn(col))
            self.pgops.add(alter_table)

            self.schedule_inhviews_update(
                schema,
                context,
                ref_op.scls,
                update_descendants=True,
                update_ancestors=True,
            )

    def _alter_pointer_optionality(
        self,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        fill_expr: Optional[s_expr.Expression],
    ) -> None:
        new_required = self.scls.get_required(schema)

        ptr = self.scls
        ptr_stor_info = types.get_pointer_storage_info(ptr, schema=schema)
        ptr_table = ptr_stor_info.table_type == 'link'
        is_lprop = ptr.is_link_property(schema)
        is_multi = ptr_table and not is_lprop
        is_required = ptr.get_required(schema)

        source_ctx = self.get_referrer_context_or_die(context)
        source_op = source_ctx.op

        # Ignore optionality changes resulting from the creation of
        # an overloaded pointer as there is no data yet.
        if isinstance(source_op, sd.CreateObject):
            return

        ops = dbops.CommandGroup(priority=1)

        # For multi pointers, if there is no fill expression, we
        # synthesize a bogus one so that an error will trip if there
        # are any objects with empty values.
        if fill_expr is None and is_multi:
            if (
                ptr.get_cardinality(schema).is_multi()
                and fill_expr is None
                and (target := ptr.get_target(schema))
            ):
                fill_ast = ql_ast.TypeCast(
                    expr=ql_ast.Set(elements=[]),
                    type=s_utils.typeref_to_ast(schema, target),
                )
                fill_expr = s_expr.Expression.from_ast(
                    qltree=fill_ast, schema=schema
                )

        if fill_expr is not None:
            _, fill_sql_expr, orig_rel_alias, _ = (
                self._compile_conversion_expr(
                    pointer=ptr,
                    conv_expr=fill_expr,
                    schema=schema,
                    orig_schema=orig_schema,
                    context=context,
                    orig_rel_is_always_source=True,
                )
            )

            if is_lprop:
                obj_id_ref = f'{qi(orig_rel_alias)}.source'
            else:
                obj_id_ref = f'{qi(orig_rel_alias)}.id'

            if is_required and not is_multi:
                fill_sql_expr = textwrap.dedent(f'''\
                    edgedb.raise_on_null(
                        ({fill_sql_expr}),
                        'not_null_violation',
                        msg => 'missing value for required property',
                        detail => '{{"object_id": "' || {obj_id_ref} || '"}}',
                        "column" => {ql(str(ptr.id))}
                    )
                ''')

            tab = q(*ptr_stor_info.table_name)
            target_col = ptr_stor_info.column_name

            if not is_multi:
                # For singleton pointers we simply update the
                # requisite column of the host source in every
                # row where it is NULL.
                update_qry = textwrap.dedent(f'''\
                    UPDATE {tab} AS {qi(orig_rel_alias)}
                    SET {qi(target_col)} = ({fill_sql_expr})
                    WHERE {qi(target_col)} IS NULL
                ''')
                ops.add_command(dbops.Query(update_qry))
            else:
                # For multi pointers we have to INSERT the
                # result of USING into the link table for
                # every source object that has _no entries_
                # in said link table.
                source = ptr.get_source(orig_schema)
                src_tab = q(*common.get_backend_name(
                    orig_schema,
                    source,
                    catenate=False,
                ))

                update_qry = textwrap.dedent(f'''\
                    INSERT INTO {tab} (source, target)
                    (
                        SELECT
                            {qi(orig_rel_alias)}.id,
                            q.val
                        FROM
                            (
                                SELECT *
                                FROM {src_tab}
                                WHERE id != ALL (
                                    SELECT source FROM {tab}
                                )
                            ) AS {qi(orig_rel_alias)},
                            LATERAL (
                                {fill_sql_expr}
                            ) AS q(val)
                        WHERE
                            q.val IS NOT NULL
                    )
                ''')

                ops.add_command(dbops.Query(update_qry))

                check_qry = textwrap.dedent(f'''\
                    SELECT
                        edgedb.raise(
                            NULL::text,
                            'not_null_violation',
                            msg => 'missing value for required property',
                            detail => '{{"object_id": "' || id || '"}}',
                            "column" => {ql(str(ptr.id))}
                        )
                    FROM {src_tab}
                    WHERE id != ALL (SELECT source FROM {tab})
                    LIMIT 1
                    INTO _dummy_text;
                ''')

                ops.add_command(dbops.Query(check_qry))

        if not ptr_table or is_lprop:
            alter_table = source_op.get_alter_table(
                schema,
                context,
                manual=True,
            )
            alter_table.add_operation(
                dbops.AlterTableAlterColumnNull(
                    column_name=ptr_stor_info.column_name,
                    null=not new_required,
                )
            )
            ops.add_command(alter_table)

        self.pgops.add(ops)

    def _drop_constraints(self, pointer, schema, context):
        # We need to be able to drop all the constraints referencing a
        # pointer before modifying its type, and then recreate them
        # once the change is done.
        # We look at all referrers to the pointer (and not just the
        # constraints directly on the pointer) because we want to
        # pick up object constraints that reference it as well.
        for cnstr in schema.get_referrers(
                pointer, scls_type=s_constr.Constraint):
            self.pgops.add(
                ConstraintCommand.delete_constraint(cnstr, schema, context))

    def _recreate_constraints(self, pointer, schema, context):
        for cnstr in schema.get_referrers(
                pointer, scls_type=s_constr.Constraint):
            self.pgops.add(
                ConstraintCommand.create_constraint(cnstr, schema, context))

    def _alter_pointer_type(self, pointer, schema, orig_schema, context):
        old_ptr_stor_info = types.get_pointer_storage_info(
            pointer, schema=orig_schema)
        new_target = pointer.get_target(schema)

        ptr_table = old_ptr_stor_info.table_type == 'link'
        is_link = isinstance(pointer, s_links.Link)
        is_lprop = pointer.is_link_property(schema)
        is_multi = ptr_table and not is_lprop
        is_required = pointer.get_required(schema)
        changing_col_type = not is_link

        source_ctx = self.get_referrer_context_or_die(context)
        if is_multi:
            if isinstance(self, sd.AlterObjectFragment):
                source_op = self.get_parent_op(context)
            else:
                source_op = self
        else:
            source_op = source_ctx.op

        # Ignore type narrowing resulting from a creation of a subtype
        # as there isn't any data in the link yet.
        if is_link and isinstance(source_ctx.op, sd.CreateObject):
            return

        new_target = pointer.get_target(schema)
        orig_target = pointer.get_target(orig_schema)
        new_type = types.pg_type_from_object(
            schema, new_target, persistent_tuples=True)

        source = source_op.scls
        using_eql_expr = self.cast_expr

        # For links, when the new type is a supertype of the old, no
        # SQL-level changes are necessary, unless an explicit conversion
        # expression was specified.
        if (
            is_link
            and using_eql_expr is None
            and orig_target.issubclass(orig_schema, new_target)
        ):
            return

        # We actually have work to do, so drop any constraints we have
        self._drop_constraints(pointer, schema, context)

        if using_eql_expr is None and not is_link:
            # A lack of an explicit EdgeQL conversion expression means
            # that the new type is assignment-castable from the old type
            # in the EdgeDB schema.  BUT, it would not necessarily be
            # assignment-castable in Postgres, especially if the types are
            # compound.  Thus, generate an explicit cast expression.
            pname = pointer.get_shortname(schema).name
            using_eql_expr = s_expr.Expression.from_ast(
                ql_ast.TypeCast(
                    expr=ql_ast.Path(
                        partial=True,
                        steps=[
                            ql_ast.Ptr(
                                ptr=ql_ast.ObjectRef(name=pname),
                                type='property' if is_lprop else None,
                            ),
                        ],
                    ),
                    type=s_utils.typeref_to_ast(schema, new_target),
                ),
                schema=orig_schema,
            )

        # There are two major possibilities about the USING claus:
        # 1) trivial case, where the USING clause refers only to the
        # columns of the source table, in which case we simply compile that
        # into an equivalent SQL USING clause, and 2) complex case, which
        # supports arbitrary queries, but requires a temporary column,
        # which is populated with the transition query and then used as the
        # source for the SQL USING clause.
        using_eql_expr, using_sql_expr, orig_rel_alias, sql_expr_is_trivial = (
            self._compile_conversion_expr(
                pointer=pointer,
                conv_expr=using_eql_expr,
                schema=schema,
                orig_schema=orig_schema,
                context=context,
            )
        )

        expr_is_nullable = using_eql_expr.cardinality.can_be_zero()

        need_temp_col = (
            (is_multi and expr_is_nullable)
            or (changing_col_type and not sql_expr_is_trivial)
        )

        if changing_col_type:
            self.pgops.add(source_op.drop_inhview(
                schema,
                context,
                source,
                drop_ancestors=True,
            ))

        tab = q(*old_ptr_stor_info.table_name)
        target_col = old_ptr_stor_info.column_name
        aux_ptr_table = None
        aux_ptr_col = None

        if is_link:
            old_lb_ptr_stor_info = types.get_pointer_storage_info(
                pointer, link_bias=True, schema=orig_schema)

            if (
                old_lb_ptr_stor_info is not None
                and old_lb_ptr_stor_info.table_type == 'link'
            ):
                aux_ptr_table = old_lb_ptr_stor_info.table_name
                aux_ptr_col = old_lb_ptr_stor_info.column_name

        if not sql_expr_is_trivial:
            if need_temp_col:
                alter_table = source_op.get_alter_table(
                    schema, context, priority=0, force_new=True, manual=True)
                temp_column = dbops.Column(
                    name=f'??{pointer.id}_{common.get_unique_random_name()}',
                    type=qt(new_type),
                )
                alter_table.add_operation(
                    dbops.AlterTableAddColumn(temp_column))
                self.pgops.add(alter_table)
                target_col = temp_column.name

            if is_multi:
                obj_id_ref = f'{qi(orig_rel_alias)}.source'
            else:
                obj_id_ref = f'{qi(orig_rel_alias)}.id'

            if is_required and not is_multi:
                using_sql_expr = textwrap.dedent(f'''\
                    edgedb.raise_on_null(
                        ({using_sql_expr}),
                        'not_null_violation',
                        msg => 'missing value for required property',
                        detail => '{{"object_id": "' || {obj_id_ref} || '"}}',
                        "column" => {ql(str(pointer.id))}
                    )
                ''')

            update_qry = textwrap.dedent(f'''\
                UPDATE {tab} AS {qi(orig_rel_alias)}
                SET {qi(target_col)} = ({using_sql_expr})
            ''')

            self.pgops.add(dbops.Query(update_qry))
            actual_using_expr = qi(target_col)
        else:
            actual_using_expr = using_sql_expr

        if changing_col_type or need_temp_col:
            alter_table = source_op.get_alter_table(
                schema, context, priority=0, force_new=True, manual=True)

        if is_multi:
            # Remove all rows where the conversion expression produced NULLs.
            col = qi(target_col)
            if pointer.get_required(schema):
                clean_nulls = dbops.Query(textwrap.dedent(f'''\
                    WITH d AS (
                        DELETE FROM {tab} WHERE {col} IS NULL RETURNING source
                    )
                    SELECT
                        edgedb.raise(
                            NULL::text,
                            'not_null_violation',
                            msg => 'missing value for required property',
                            detail => '{{"object_id": "' || l.source || '"}}',
                            "column" => {ql(str(pointer.id))}
                        )
                    FROM
                        {tab} AS l
                    WHERE
                        l.source IN (SELECT source FROM d)
                        AND True = ALL (
                            SELECT {col} IS NULL
                            FROM {tab} AS l2
                            WHERE l2.source = l.source
                        )
                    LIMIT
                        1
                    INTO _dummy_text;
                '''))
            else:
                clean_nulls = dbops.Query(textwrap.dedent(f'''\
                    DELETE FROM {tab} WHERE {col} IS NULL
                '''))

            self.pgops.add(clean_nulls)

        elif aux_ptr_table is not None:
            # SINGLE links with link properties are represented in
            # _two_ tables (the host type table and a link table with
            # properties), and we must update both.
            actual_col = qi(old_ptr_stor_info.column_name)

            if expr_is_nullable and not is_required:
                cleanup_qry = textwrap.dedent(f'''\
                    DELETE FROM {q(*aux_ptr_table)} AS aux
                    USING {tab} AS main
                    WHERE
                        main.id = aux.source
                        AND {actual_col} IS NULL
                ''')
                self.pgops.add(dbops.Query(cleanup_qry))

            update_qry = textwrap.dedent(f'''\
                UPDATE {q(*aux_ptr_table)} AS aux
                SET {qi(aux_ptr_col)} = main.{actual_col}
                FROM {tab} AS main
                WHERE
                    main.id = aux.source
            ''')
            self.pgops.add(dbops.Query(update_qry))

        if changing_col_type:
            alter_type = dbops.AlterTableAlterColumnType(
                old_ptr_stor_info.column_name,
                common.quote_type(new_type),
                using_expr=actual_using_expr,
            )

            alter_table.add_operation(alter_type)
        elif need_temp_col:
            move_data = dbops.Query(textwrap.dedent(f'''\
                UPDATE
                    {q(*old_ptr_stor_info.table_name)} AS {qi(orig_rel_alias)}
                SET
                    {qi(old_ptr_stor_info.column_name)} = ({qi(target_col)})
            '''))
            self.pgops.add(move_data)

        if need_temp_col:
            alter_table.add_operation(dbops.AlterTableDropColumn(temp_column))

        if changing_col_type or need_temp_col:
            self.pgops.add(alter_table)

        self._recreate_constraints(pointer, schema, context)

        if changing_col_type:
            self.schedule_inhviews_update(
                schema,
                context,
                source,
                update_descendants=True,
                update_ancestors=True,
            )

    def _compile_conversion_expr(
        self,
        *,
        pointer: s_pointers.Pointer,
        conv_expr: s_expr.Expression,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
        orig_rel_is_always_source: bool = False,
        target_as_singleton: bool = True,
    ) -> Tuple[
        s_expr.Expression,  # Possibly-amended EdgeQL conversion expression
        str,                # SQL text
        str,                # original relation alias
        bool,               # whether SQL expression is trivial
    ]:
        old_ptr_stor_info = types.get_pointer_storage_info(
            pointer, schema=orig_schema)

        ptr_table = old_ptr_stor_info.table_type == 'link'
        is_link = isinstance(pointer, s_links.Link)
        is_lprop = pointer.is_link_property(schema)
        is_multi = ptr_table and not is_lprop
        is_required = pointer.get_required(schema)

        new_target = pointer.get_target(schema)
        expr_is_trivial = False

        if conv_expr.irast is not None:
            ir = conv_expr.irast
        else:
            conv_expr = self._compile_expr(
                orig_schema,
                context,
                conv_expr,
                target_as_singleton=target_as_singleton,
            )
            ir = conv_expr.irast

        assert ir is not None

        if ir.stype != new_target and not is_link:
            # The result of an EdgeQL USING clause does not match
            # the target type exactly, but is castable.  Like in the
            # case of an empty USING clause, we still have to make
            # ane explicit EdgeQL cast rather than rely on Postgres
            # casting.
            conv_expr = self._compile_expr(
                orig_schema,
                context,
                s_expr.Expression.from_ast(
                    ql_ast.TypeCast(
                        expr=conv_expr.qlast,
                        type=s_utils.typeref_to_ast(schema, new_target),
                    ),
                    schema=orig_schema,
                ),
                target_as_singleton=target_as_singleton,
            )

            ir = conv_expr.irast

        expr_is_nullable = conv_expr.cardinality.can_be_zero()

        refs = irutils.get_longest_paths(ir.expr)
        ref_tables = schemamech.get_ref_storage_info(ir.schema, refs)

        local_table_only = all(
            t == old_ptr_stor_info.table_name
            for t in ref_tables
        )

        # TODO: implement IR complexity inference
        can_translate_to_sql_value_expr = False

        expr_is_trivial = (
            # Only allow trivial USING if we can compile the
            # EdgeQL expression into a trivial SQL value expression.
            can_translate_to_sql_value_expr
            # No link expr is trivially translatable into
            # a USING SQL clause.
            and not is_link
            # SQL SET TYPE cannot contain references
            # outside of the local table.
            and local_table_only
            # Changes to a multi-pointer might involve contraction of
            # the overall cardinality, i.e. the deletion some rows.
            and not is_multi
            # If the property is required, and the USING expression
            # was not proven by the compiler to not return ZERO, we
            # must inject an explicit NULL guard, as the SQL null
            # violation error is very nondescript in the context of
            # a table rewrite, making it hard to pinpoint the failing
            # object.
            and (not is_required or not expr_is_nullable)
        )

        alias = f'alias_{uuidgen.uuid1mc()}'

        if not expr_is_trivial:
            # Non-trivial conversion expression means that we
            # are compiling a full-blown EdgeQL statement as
            # opposed to compiling a scalar fragment in trivial
            # expression mode.
            external_rvars = {}

            if is_lprop:
                tgt_path_id = irpathid.PathId.from_pointer(
                    orig_schema,
                    pointer,
                ).src_path()
            else:
                tgt_path_id = irpathid.PathId.from_pointer(
                    orig_schema,
                    pointer,
                )

            ptr_path_id = tgt_path_id.ptr_path()
            src_path_id = ptr_path_id.src_path()

            if ptr_table and not orig_rel_is_always_source:
                rvar = compiler.new_external_rvar(
                    rel_name=(alias,),
                    path_id=ptr_path_id,
                    outputs={
                        (src_path_id, ('identity',)): 'source',
                    },
                )
                external_rvars[ptr_path_id, 'source'] = rvar
                external_rvars[ptr_path_id, 'value'] = rvar
                external_rvars[src_path_id, 'identity'] = rvar
                if local_table_only and not is_lprop:
                    external_rvars[src_path_id, 'source'] = rvar
                    external_rvars[src_path_id, 'value'] = rvar
                elif is_lprop:
                    external_rvars[tgt_path_id, 'identity'] = rvar
                    external_rvars[tgt_path_id, 'value'] = rvar
            else:
                src_rvar = compiler.new_external_rvar(
                    rel_name=(alias,),
                    path_id=src_path_id,
                    outputs={},
                )
                external_rvars[src_path_id, 'identity'] = src_rvar
                external_rvars[src_path_id, 'value'] = src_rvar
                external_rvars[src_path_id, 'source'] = src_rvar
        else:
            external_rvars = None

        sql_tree = compiler.compile_ir_to_sql_tree(
            ir,
            output_format=compiler.OutputFormat.NATIVE_INTERNAL,
            singleton_mode=expr_is_trivial,
            external_rvars=external_rvars,
        )

        sql_text = codegen.generate_source(sql_tree)

        return (conv_expr, sql_text, alias, expr_is_trivial)

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
                name=tgt_col, type='uuid', required=True))

        constraints.append(
            dbops.UniqueConstraint(
                table_name=new_table_name,
                columns=[src_col, tgt_col]))

        if not link.generic(schema) and link.scalar():
            tgt_prop = link.getptr(schema, 'target')
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

            sets_required = bool(
                self.get_subcommands(
                    type=s_pointers.AlterPointerLowerCardinality))

            if ptr_stor_info.table_type == 'ObjectType':
                default_value = self.get_pointer_default(link, schema, context)
                cols = self.get_columns(
                    link, schema, default_value, sets_required)
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
                    self.alter_pointer_default(link, None, schema, context)

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

            # If we're creating a required multi pointer without a SET
            # REQUIRED USING inside, run the alter_pointer_optionality
            # path to produce an error if there is existing data.
            if (
                link.get_cardinality(schema).is_multi()
                and link.get_required(schema)
                and not sets_required
            ):
                self._alter_pointer_optionality(
                    schema, schema, context, fill_expr=None)

        objtype = context.get(s_objtypes.ObjectTypeCommandContext)

        self.attach_alter_table(context)

        self.pgops.update(extra_ops)

        if (source is not None and not source_is_view
                and not link.is_pure_computable(schema)):
            self.schedule_endpoint_delete_action_update(
                link, orig_schema, schema, context)

        return schema


class RenameLink(LinkMetaCommand, RenameObject, adapts=s_links.RenameLink):
    pass


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

        if not source.is_pure_computable(schema):
            self.schedule_endpoint_delete_action_update(
                source, orig_schema, schema, context)

        return schema


class SetLinkType(LinkMetaCommand, adapts=s_links.SetLinkType):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = s_links.SetLinkType.apply(self, schema, context)
        schema = LinkMetaCommand.apply(self, schema, context)
        pop = self.get_parent_op(context)
        orig_type = self.scls.get_target(orig_schema)
        new_type = self.scls.get_target(schema)
        if (
            not pop.maybe_get_object_aux_data('from_alias')
            and (orig_type != new_type or self.cast_expr is not None)
        ):
            self._alter_pointer_type(self.scls, schema, orig_schema, context)
        return schema


class AlterLinkUpperCardinality(
    LinkMetaCommand,
    adapts=s_links.AlterLinkUpperCardinality,
):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        pop = self.get_parent_op(context)
        orig_schema = schema
        schema = s_links.AlterLinkUpperCardinality.apply(self, schema, context)
        schema = LinkMetaCommand.apply(self, schema, context)

        if (
            not self.scls.generic(schema)
            and not self.scls.is_pure_computable(schema)
            and not pop.maybe_get_object_aux_data('from_alias')
        ):
            orig_card = self.scls.get_cardinality(orig_schema)
            new_card = self.scls.get_cardinality(schema)
            if orig_card != new_card:
                self._alter_pointer_cardinality(schema, orig_schema, context)

        return schema


class AlterLinkLowerCardinality(
    LinkMetaCommand,
    adapts=s_links.AlterLinkLowerCardinality,
):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        pop = self.get_parent_op(context)
        orig_schema = schema
        schema = s_links.AlterLinkLowerCardinality.apply(
            self, schema, context)
        schema = LinkMetaCommand.apply(self, schema, context)

        if not self.scls.generic(schema):
            orig_required = self.scls.get_required(orig_schema)
            new_required = self.scls.get_required(schema)
            if (
                not pop.maybe_get_object_aux_data('from_alias')
                and not self.scls.is_endpoint_pointer(schema)
                and orig_required != new_required
            ):
                self._alter_pointer_optionality(
                    schema, orig_schema, context, fill_expr=self.fill_expr)

        return schema


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

            otd = self.get_resolved_attribute_value(
                'on_target_delete',
                schema=schema,
                context=context,
            )
            card = self.get_resolved_attribute_value(
                'cardinality',
                schema=schema,
                context=context,
            )
            if (otd or card) and not link.is_pure_computable(schema):
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
                    and objtype.scls.maybe_get_ptr(schema, link_name) is None):
                # Only drop the column if the parent is not being dropped
                # and the link was not reinherited in the same delta.
                if objtype.scls.maybe_get_ptr(schema, link_name) is None:
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
        orig_schema = schema
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

            sets_required = bool(
                self.get_subcommands(
                    type=s_pointers.AlterPointerLowerCardinality))
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
                alter_table = src.op.get_alter_table(
                    schema,
                    context,
                    force_new=True,
                    manual=True,
                )

                default_value = self.get_pointer_default(prop, schema, context)

                cols = self.get_columns(
                    prop, schema, default_value, sets_required)

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

                self.pgops.add(alter_table)

                self.update_lineage_inhviews(schema, context, prop)

                if has_table(src.op.scls, schema):
                    self.schedule_inhviews_update(
                        schema,
                        context,
                        src.op.scls,
                        update_descendants=True,
                    )

            # If we're creating a required multi pointer without a SET
            # REQUIRED USING inside, run the alter_pointer_optionality
            # path to produce an error if there is existing data.
            if (
                prop.get_cardinality(schema).is_multi()
                and prop.get_required(schema)
                and not sets_required
            ):
                self._alter_pointer_optionality(
                    schema, schema, context, fill_expr=None)

            if not prop.is_pure_computable(schema):
                self.schedule_endpoint_delete_action_update(
                    prop, orig_schema, schema, context)

        return schema


class RenameProperty(
        PropertyMetaCommand, RenameObject, adapts=s_props.RenameProperty):
    pass


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

        if not source.is_pure_computable(schema):
            self.schedule_endpoint_delete_action_update(
                source, orig_schema, schema, context)

        return schema


class SetPropertyType(
        PropertyMetaCommand, adapts=s_props.SetPropertyType):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        pop = self.get_parent_op(context)
        orig_schema = schema
        schema = s_props.SetPropertyType.apply(self, schema, context)
        schema = PropertyMetaCommand.apply(self, schema, context)
        orig_type = self.scls.get_target(orig_schema)
        new_type = self.scls.get_target(schema)
        if (
            not pop.maybe_get_object_aux_data('from_alias')
            and not self.scls.is_endpoint_pointer(schema)
            and (orig_type != new_type or self.cast_expr is not None)
        ):
            self._alter_pointer_type(self.scls, schema, orig_schema, context)
        return schema


class AlterPropertyUpperCardinality(
    PropertyMetaCommand,
    adapts=s_props.AlterPropertyUpperCardinality,
):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        pop = self.get_parent_op(context)
        orig_schema = schema
        schema = s_props.AlterPropertyUpperCardinality.apply(
            self, schema, context)
        schema = PropertyMetaCommand.apply(self, schema, context)

        if (
            not self.scls.generic(schema)
            and not self.scls.is_pure_computable(schema)
            and not self.scls.is_endpoint_pointer(schema)
            and not pop.maybe_get_object_aux_data('from_alias')
        ):
            orig_card = self.scls.get_cardinality(orig_schema)
            new_card = self.scls.get_cardinality(schema)
            if orig_card != new_card:
                self._alter_pointer_cardinality(schema, orig_schema, context)

        return schema


class AlterPropertyLowerCardinality(
    PropertyMetaCommand,
    adapts=s_props.AlterPropertyLowerCardinality,
):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        pop = self.get_parent_op(context)
        orig_schema = schema
        schema = s_props.AlterPropertyLowerCardinality.apply(
            self, schema, context)
        schema = PropertyMetaCommand.apply(self, schema, context)

        if not self.scls.generic(schema):
            orig_required = self.scls.get_required(orig_schema)
            new_required = self.scls.get_required(schema)
            if (
                not pop.maybe_get_object_aux_data('from_alias')
                and not self.scls.is_endpoint_pointer(schema)
                and orig_required != new_required
            ):
                self._alter_pointer_optionality(
                    schema, orig_schema, context, fill_expr=self.fill_expr)

        return schema


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
            self.alter_pointer_default(prop, orig_schema, schema, context)
            card = self.get_resolved_attribute_value(
                'cardinality',
                schema=schema,
                context=context,
            )
            if card and not prop.is_pure_computable(schema):
                self.schedule_endpoint_delete_action_update(
                    prop, orig_schema, schema, context)

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
                and not source.maybe_get_ptr(
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
            self.schedule_endpoint_delete_action_update(
                prop, orig_schema, schema, context)

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
        self.changed_targets = set()

    def _get_link_table_union(self, schema, links, include_children) -> str:
        selects = []
        aspect = 'inhview' if include_children else None
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
                table=common.get_backend_name(
                    schema,
                    link,
                    aspect=aspect,
                ),
            ))

        return '(' + '\nUNION ALL\n    '.join(selects) + ') as q'

    def _get_inline_link_table_union(
            self, schema, links, include_children) -> str:
        selects = []
        aspect = 'inhview' if include_children else None
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
                    aspect=aspect,
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
                # Inherited link targets with restrict actions are
                # elided by apply() to enable us to use inhviews here
                # when looking for live references.
                tables = self._get_link_table_union(
                    schema, links, include_children=True)

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

                    # Since enforcement of 'required' on multi links
                    # is enforced manually on the query side and (not
                    # through constraints/triggers of its own), we
                    # also need to do manual enforcement of it when
                    # deleting a required multi link.
                    if link.get_required(schema) and disposition == 'target':
                        required_text = textwrap.dedent('''\
                            SELECT q.source INTO srcid
                            FROM {link_table} as q
                                WHERE q.target = OLD.{id}
                                AND NOT EXISTS (
                                    SELECT FROM {link_table} as q2
                                    WHERE q.source = q2.source
                                          AND q2.target != OLD.{id}
                                );

                            IF FOUND THEN
                                RAISE not_null_violation
                                    USING
                                        TABLE = TG_TABLE_NAME,
                                        SCHEMA = TG_TABLE_SCHEMA,
                                        MESSAGE = 'missing value',
                                        COLUMN = '{link_id}';
                            END IF;
                        ''').format(
                            link_table=link_table,
                            link_id=str(link.id),
                            id='id'
                        )

                        chunks.append(required_text)

                    # Otherwise just delete it from the link table.
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
                    tables = self._get_link_table_union(
                        schema, source_links, include_children=False)

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
                # Inherited link targets with restrict actions are
                # elided by apply() to enable us to use inhviews here
                # when looking for live references.
                tables = self._get_inline_link_table_union(
                    schema, links, include_children=True)

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
                        schema, source_links, include_children=False)

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
        if not self.link_ops and not self.changed_targets:
            return schema

        DA = s_links.LinkTargetDeleteAction

        affected_sources = set()
        affected_targets = {t for _, t in self.changed_targets}
        modifications = any(
            isinstance(op, RebaseObjectType) and op.removed_bases
            for op, _ in self.changed_targets
        )

        for link_op, link, orig_schema in self.link_ops:
            # If our link has a restrict policy, we don't need to update
            # the target on changes to inherited links.
            # Most importantly, this optimization lets us avoid updating
            # the triggers for every schema::Type subtype every time a
            # new object type is created containing a __type__ link.
            eff_schema = (
                orig_schema
                if isinstance(link_op, (DeleteProperty, DeleteLink))
                else schema)
            if not eff_schema.has_object(link.id):
                continue
            action = (
                link.get_on_target_delete(eff_schema)
                if isinstance(link, s_links.Link) else None)
            target_is_affected = not (
                (action is DA.Restrict or action is DA.DeferredRestrict)
                and link.get_implicit_bases(eff_schema)
            ) and isinstance(link, s_links.Link)

            if (
                link.generic(eff_schema)
                or link.is_pure_computable(eff_schema)
            ):
                continue

            source = link.get_source(eff_schema)
            target = link.get_target(eff_schema)

            if not isinstance(source, s_objtypes.ObjectType):
                continue

            if not isinstance(link_op, (CreateProperty, CreateLink)):
                modifications = True

            if isinstance(link_op, (DeleteProperty, DeleteLink)):
                current_source = orig_schema.get_by_id(source.id, None)
                if (current_source is not None
                        and not current_source.is_view(orig_schema)):
                    affected_sources.add((current_source, orig_schema))
                current_target = schema.get_by_id(target.id, None)
                if target_is_affected and current_target is not None:
                    affected_targets.add(current_target)
            else:
                if source.is_view(schema):
                    continue

                affected_sources.add((source, schema))

                if target_is_affected:
                    affected_targets.add(target)

                if isinstance(link_op, (AlterProperty, AlterLink)):
                    orig_target = link.get_target(orig_schema)
                    if target != orig_target:
                        current_orig_target = schema.get_by_id(
                            orig_target.id, None)
                        if current_orig_target is not None:
                            affected_targets.add(current_orig_target)

        for source, src_schema in affected_sources:
            links = []

            for link in source.get_pointers(src_schema).objects(src_schema):
                if link.is_pure_computable(src_schema):
                    continue
                ptr_stor_info = types.get_pointer_storage_info(
                    link, schema=src_schema)
                if ptr_stor_info.table_type != 'link':
                    continue

                links.append(link)

            links.sort(
                key=lambda l: (
                    (l.get_on_target_delete(src_schema),)
                    if isinstance(l, s_links.Link) else (),
                    l.get_name(src_schema)))

            if links or modifications:
                self._update_action_triggers(
                    src_schema, source, links, disposition='source')

        # All descendants of affected targets also need to have their
        # triggers updated, so track them down.
        all_affected_targets = set()
        for target in affected_targets:
            union_of = target.get_union_of(schema)
            if union_of:
                objtypes = tuple(union_of.objects(schema))
            else:
                objtypes = (target,)

            for objtype in objtypes:
                all_affected_targets.add(objtype)
                for descendant in objtype.descendants(schema):
                    if has_table(descendant, schema):
                        all_affected_targets.add(descendant)

        for target in all_affected_targets:
            deferred_links = []
            deferred_inline_links = []
            links = []
            inline_links = []

            inbound_links = schema.get_referrers(
                target, scls_type=s_links.Link, field_name='target')

            # We need to look at all inbound links to all ancestors
            for ancestor in target.get_ancestors(schema).objects(schema):
                inbound_links |= schema.get_referrers(
                    ancestor, scls_type=s_links.Link, field_name='target')

            for link in inbound_links:
                if link.is_pure_computable(schema):
                    continue
                action = link.get_on_target_delete(schema)

                # Enforcing link deletion policies on targets are
                # handled by looking at the inheritance views, when
                # restrict is the policy.
                # If the policy is allow or delete source, we need to
                # actually process this for each link.
                if (
                    (action is DA.Restrict or action is DA.DeferredRestrict)
                    and link.get_implicit_bases(schema)
                ):
                    continue

                source = link.get_source(schema)
                if source.is_view(schema):
                    continue
                ptr_stor_info = types.get_pointer_storage_info(
                    link, schema=schema)
                if ptr_stor_info.table_type != 'link':
                    if action is DA.DeferredRestrict:
                        deferred_inline_links.append(link)
                    else:
                        inline_links.append(link)
                else:
                    if action is DA.DeferredRestrict:
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

            if links or modifications:
                self._update_action_triggers(
                    schema, target, links, disposition='target')

            if inline_links or modifications:
                self._update_action_triggers(
                    schema, target, inline_links,
                    disposition='target', inline=True)

            if deferred_links or modifications:
                self._update_action_triggers(
                    schema, target, deferred_links,
                    disposition='target', deferred=True)

            if deferred_inline_links or modifications:
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

    @classmethod
    def _get_select_from(cls, schema, obj, ptrnames):
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

    @classmethod
    def get_inhview(cls, schema, obj):
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

        components = [cls._get_select_from(schema, obj, ptrs)]

        components.extend(
            cls._get_select_from(schema, descendant, ptrs)
            for descendant in obj.descendants(schema)
            if has_table(descendant, schema)
        )

        query = '\nUNION ALL\n'.join(components)

        return dbops.View(
            name=inhview_name,
            query=query,
        )

    def update_inhview(self, schema, obj):
        inhview = self.get_inhview(schema, obj)

        self.pgops.add(
            dbops.DropView(
                inhview.name,
                priority=1,
                conditions=[dbops.ViewExists(inhview.name)],
            ),
        )

        self.pgops.add(
            dbops.CreateView(
                view=inhview,
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
        tenant_id = self._get_tenant_id(context)
        db_name = common.get_database_backend_name(
            str(self.classname), tenant_id=tenant_id)
        tpl_name = common.get_database_backend_name(
            self.template or edbdef.EDGEDB_TEMPLATE_DB, tenant_id=tenant_id)
        self.pgops.add(
            dbops.CreateDatabase(
                dbops.Database(
                    db_name,
                    metadata=dict(
                        id=str(db.id),
                        tenant_id=tenant_id,
                        builtin=self.get_attribute_value('builtin'),
                        name=str(self.classname),
                    ),
                ),
                template=tpl_name,
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
        tenant_id = self._get_tenant_id(context)
        db_name = common.get_database_backend_name(
            str(self.classname), tenant_id=tenant_id)
        self.pgops.add(dbops.DropDatabase(db_name))
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

        role_name = str(role.get_name(schema))

        backend_params = self._get_backend_params(context)
        capabilities = backend_params.instance_params.capabilities
        tenant_id = backend_params.instance_params.tenant_id

        if role.get_superuser(schema):
            membership.append(edbdef.EDGEDB_SUPERGROUP)

            # If the cluster is not exposing an explicit superuser role,
            # we will make the created Postgres role superuser if we can
            if not backend_params.instance_params.base_superuser:
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
            name=common.get_role_backend_name(role_name, tenant_id=tenant_id),
            allow_login=True,
            superuser=superuser_flag,
            password=passwd,
            membership=[
                common.get_role_backend_name(parent_role, tenant_id=tenant_id)
                for parent_role in membership
            ],
            metadata=dict(
                id=str(role.id),
                name=role_name,
                tenant_id=tenant_id,
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

        backend_params = self._get_backend_params(context)
        capabilities = backend_params.instance_params.capabilities
        tenant_id = backend_params.instance_params.tenant_id
        instance_params = backend_params.instance_params
        role_name = str(role.get_name(schema))

        kwargs = {}
        if self.has_attribute_value('password'):
            passwd = self.get_attribute_value('password')
            kwargs['password'] = passwd
            kwargs['metadata'] = dict(
                id=str(role.id),
                name=role_name,
                tenant_id=tenant_id,
                password_hash=passwd,
                builtin=role.get_builtin(schema),
            )

        pg_role_name = common.get_role_backend_name(
            role_name, tenant_id=tenant_id)
        if self.has_attribute_value('superuser'):
            membership = list(role.get_bases(schema).names(schema))
            membership.append(edbdef.EDGEDB_SUPERGROUP)
            self.pgops.add(
                dbops.AlterRoleAddMembership(
                    name=pg_role_name,
                    membership=[
                        common.get_role_backend_name(
                            parent_role, tenant_id=tenant_id)
                        for parent_role in membership
                    ],
                )
            )

            superuser_flag = False

            # If the cluster is not exposing an explicit superuser role,
            # we will make the modified Postgres role superuser if we can
            if not instance_params.base_superuser:
                superuser_flag = (
                    capabilities
                    & pgcluster.BackendCapabilities.SUPERUSER_ACCESS
                )

            kwargs['superuser'] = superuser_flag

        dbrole = dbops.Role(name=pg_role_name, **kwargs)
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

        tenant_id = self._get_tenant_id(context)

        for dropped in self.removed_bases:
            self.pgops.add(dbops.AlterRoleDropMember(
                name=common.get_role_backend_name(
                    str(dropped.name), tenant_id=tenant_id),
                member=common.get_role_backend_name(
                    str(role.get_name(schema)), tenant_id=tenant_id),
            ))

        for bases, _pos in self.added_bases:
            for added in bases:
                self.pgops.add(dbops.AlterRoleAddMember(
                    name=common.get_role_backend_name(
                        str(added.name), tenant_id=tenant_id),
                    member=common.get_role_backend_name(
                        str(role.get_name(schema)), tenant_id=tenant_id),
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
        tenant_id = self._get_tenant_id(context)
        self.pgops.add(dbops.DropRole(
            common.get_role_backend_name(
                str(self.classname), tenant_id=tenant_id)))
        return schema


class CreateExtensionPackage(
    ObjectMetaCommand,
    adapts=s_exts.CreateExtensionPackage,
):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_exts.CreateExtensionPackage.apply(self, schema, context)
        schema = ObjectMetaCommand.apply(self, schema, context)
        ext_id = str(self.scls.id)
        name__internal = str(self.scls.get_name(schema))
        name = self.scls.get_displayname(schema)
        version = self.scls.get_version(schema)._asdict()
        version['stage'] = version['stage'].name.lower()

        tenant_id = self._get_tenant_id(context)
        tpl_db_name = common.get_database_backend_name(
            edbdef.EDGEDB_TEMPLATE_DB, tenant_id=tenant_id)

        self.pgops.add(
            dbops.UpdateMetadataSection(
                dbops.Database(name=tpl_db_name),
                section='ExtensionPackage',
                metadata={
                    ext_id: {
                        'id': ext_id,
                        'name': name,
                        'name__internal': name__internal,
                        'script': self.scls.get_script(schema),
                        'version': version,
                        'builtin': self.scls.get_builtin(schema),
                        'internal': self.scls.get_internal(schema),
                    }
                }
            )
        )

        return schema


class DeleteExtensionPackage(
    ObjectMetaCommand,
    adapts=s_exts.DeleteExtensionPackage,
):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = s_exts.DeleteExtensionPackage.apply(self, schema, context)
        schema = ObjectMetaCommand.apply(self, schema, context)

        tenant_id = self._get_tenant_id(context)
        tpl_db_name = common.get_database_backend_name(
            edbdef.EDGEDB_TEMPLATE_DB, tenant_id=tenant_id)
        ext_id = str(self.scls.id)
        self.pgops.add(
            dbops.UpdateMetadataSection(
                dbops.Database(name=tpl_db_name),
                section='ExtensionPackage',
                metadata={
                    ext_id: None
                }
            )
        )

        return schema


class CreateExtension(
    CreateObject,
    adapts=s_exts.CreateExtension,
):
    pass


class DeleteExtension(
    DeleteObject,
    adapts=s_exts.DeleteExtension,
):
    pass


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
