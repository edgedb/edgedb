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
from typing import (
    Callable,
    Optional,
    Tuple,
    Type,
    Iterable,
    Mapping,
    Sequence,
    Dict,
    List,
    cast,
    TYPE_CHECKING,
)
from copy import copy

import collections.abc
import itertools
import textwrap
import uuid

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
from edb.schema import futures as s_futures
from edb.schema import functions as s_funcs
from edb.schema import globals as s_globals
from edb.schema import indexes as s_indexes
from edb.schema import links as s_links
from edb.schema import policies as s_policies
from edb.schema import properties as s_props
from edb.schema import migrations as s_migrations
from edb.schema import modules as s_mod
from edb.schema import name as sn
from edb.schema import objects as so
from edb.schema import operators as s_opers
from edb.schema import pointers as s_pointers
from edb.schema import pseudo as s_pseudo
from edb.schema import roles as s_roles
from edb.schema import rewrites as s_rewrites
from edb.schema import sources as s_sources
from edb.schema import triggers as s_triggers
from edb.schema import types as s_types
from edb.schema import version as s_ver
from edb.schema import utils as s_utils

from edb.common import markup
from edb.common import ordered
from edb.common import uuidgen
from edb.common import parsing
from edb.common.typeutils import not_none

from edb.ir import ast as irast
from edb.ir import pathid as irpathid
from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from edb.pgsql import common
from edb.pgsql import dbops
from edb.pgsql import params
from edb.pgsql import deltafts
from edb.pgsql import delta_ext_ai

from edb.server import defines as edbdef
from edb.server import config
from edb.server.config import ops as config_ops
from edb.server.compiler import sertypes

from . import ast as pgast
from .common import qname as q
from .common import quote_literal as ql
from .common import quote_ident as qi
from .common import quote_type as qt
from .common import versioned_schema as V
from .compiler import enums as pgce
from . import compiler
from . import codegen
from . import schemamech
from . import trampoline
from . import types

if TYPE_CHECKING:
    from edb.schema import schema as s_schema


DEFAULT_INDEX_CODE = ' ((__col__) NULLS FIRST)'


class CommandMeta(sd.CommandMeta):
    pass


class MetaCommand(sd.Command, metaclass=CommandMeta):
    pgops: ordered.OrderedSet[dbops.Command | sd.Command]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pgops = ordered.OrderedSet()

    def apply_prerequisites(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply_prerequisites(schema, context)
        for op in self.get_prerequisites():
            if not isinstance(op, sd.AlterObjectProperty):
                self.pgops.add(op)
        return schema

    def apply_subcommands(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply_subcommands(schema, context)
        for op in self.get_subcommands(
            include_prerequisites=False,
            include_caused=False,
        ):
            if not isinstance(op, sd.AlterObjectProperty):
                self.pgops.add(op)
        return schema

    def apply_caused(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply_caused(schema, context)
        for op in self.get_caused():
            if not isinstance(op, sd.AlterObjectProperty):
                self.pgops.add(op)
        return schema

    def generate(self, block: dbops.PLBlock) -> None:
        for op in self.pgops:
            assert isinstance(op, (dbops.Command, MetaCommand))
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
    ) -> params.BackendRuntimeParams:

        ctx_backend_params = context.backend_runtime_params
        if ctx_backend_params is not None:
            backend_params = cast(
                params.BackendRuntimeParams, ctx_backend_params)
        else:
            backend_params = params.get_default_runtime_params()

        return backend_params

    def _get_instance_params(
        self,
        context: sd.CommandContext,
    ) -> params.BackendInstanceParams:
        return self._get_backend_params(context).instance_params

    def _get_tenant_id(self, context: sd.CommandContext) -> str:
        return self._get_instance_params(context).tenant_id

    def _get_topmost_command_op(
        self,
        context: sd.CommandContext,
        ctxcls: Type[sd.CommandContextToken[sd.Command]],
    ) -> CompositeMetaCommand:
        ctx = context.get_topmost_ancestor(ctxcls)
        if ctx is None:
            raise AssertionError(f"there is no {ctxcls} in context stack")
        assert isinstance(ctx.op, CompositeMetaCommand)
        return ctx.op

    def schedule_constraint_trigger_update(
        self,
        constraint: s_constr.Constraint,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        ctxcls: Type[sd.CommandContextToken[sd.Command]],
    ) -> None:

        if (
            not isinstance(
                constraint.get_subject(schema),
                (s_objtypes.ObjectType, s_pointers.Pointer)
            )
            or not schemamech.table_constraint_requires_triggers(
                constraint, schema, 'unique'
            )
        ):
            return

        op = self._get_topmost_command_op(context, ctxcls)
        op.constraint_trigger_updates.add(constraint.id)

    @staticmethod
    def get_function_type(
        name: tuple[str, str]
    ) -> Type[dbops.Function] | Type[trampoline.VersionedFunction]:
        return (
            trampoline.VersionedFunction if name[0] == 'edgedbstd'
            else dbops.Function
        )

    @classmethod
    def maybe_trampoline(
        cls,
        f: Optional[dbops.Function],
        context: sd.CommandContext,
    ) -> None:
        if isinstance(f, trampoline.VersionedFunction):
            create = trampoline.make_trampoline(f)

            ctx = not_none(context.get(sd.DeltaRootContext))
            assert isinstance(ctx.op, DeltaRoot)
            create_trampolines = ctx.op.create_trampolines
            create_trampolines.trampolines.append(create)


class CommandGroupAdapted(MetaCommand, adapts=sd.CommandGroup):
    pass


class Nop(MetaCommand, adapts=sd.Nop):
    pass


class Query(MetaCommand, adapts=sd.Query):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)

        assert self.expr.irast
        sql_res = compiler.compile_ir_to_sql_tree(
            self.expr.irast,
            output_format=compiler.OutputFormat.NATIVE_INTERNAL,
            explicit_top_cast=irtyputils.type_to_typeref(
                schema,
                schema.get('std::str', type=s_types.Type),
                cache=None,
            ),
            backend_runtime_params=context.backend_runtime_params,
        )
        sql_text = codegen.generate_source(sql_res.ast)

        # The INTO _dummy_text bit is needed because PL/pgSQL _really_
        # wants the result of a returning query to be stored in a variable,
        # and the PERFORM hack does not work if the query has DML CTEs.
        self.pgops.add(dbops.Query(
            text=f'{sql_text} INTO _dummy_text',
        ))

        return schema


class AlterObjectProperty(MetaCommand, adapts=sd.AlterObjectProperty):
    pass


class SchemaVersionCommand(MetaCommand):
    pass


class CreateSchemaVersion(
    SchemaVersionCommand,
    adapts=s_ver.CreateSchemaVersion,
):
    pass


class AlterSchemaVersion(
    SchemaVersionCommand,
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
                    edgedb_VER.raise_on_not_null(
                        (SELECT NULLIF(
                            (SELECT
                                version::text
                            FROM
                                {V('edgedb')}."_SchemaSchemaVersion"
                            FOR UPDATE),
                            {ql(str(expected_ver))}
                        )),
                        'serialization_failure',
                        msg => (
                            'Cannot serialize DDL: '
                            || (SELECT version::text FROM
                                {V('edgedb')}."_SchemaSchemaVersion")
                        )
                    )
                INTO _dummy_text
            '''
        )
        self.pgops.add(check)
        return schema


class GlobalSchemaVersionCommand(MetaCommand):
    pass


class CreateGlobalSchemaVersion(
    MetaCommand,
    adapts=s_ver.CreateGlobalSchemaVersion,
):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        ver_id = str(self.scls.id)
        ver_name = str(self.scls.get_name(schema))

        ctx_backend_params = context.backend_runtime_params
        if ctx_backend_params is not None:
            backend_params = cast(
                params.BackendRuntimeParams, ctx_backend_params)
        else:
            backend_params = params.get_default_runtime_params()

        metadata = {
            ver_id: {
                'id': ver_id,
                'name': ver_name,
                'version': str(self.scls.get_version(schema)),
                'builtin': self.scls.get_builtin(schema),
                'internal': self.scls.get_internal(schema),
            }
        }
        if backend_params.has_create_database:
            self.pgops.add(
                dbops.UpdateMetadataSection(
                    dbops.DatabaseWithTenant(name=edbdef.EDGEDB_TEMPLATE_DB),
                    section='GlobalSchemaVersion',
                    metadata=metadata
                )
            )
        else:
            self.pgops.add(
                dbops.UpdateSingleDBMetadataSection(
                    edbdef.EDGEDB_TEMPLATE_DB,
                    section='GlobalSchemaVersion',
                    metadata=metadata
                )
            )

        return schema


class AlterGlobalSchemaVersion(
    MetaCommand,
    adapts=s_ver.AlterGlobalSchemaVersion,
):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        ver_id = str(self.scls.id)
        ver_name = str(self.scls.get_name(schema))

        ctx_backend_params = context.backend_runtime_params
        if ctx_backend_params is not None:
            backend_params = cast(
                params.BackendRuntimeParams, ctx_backend_params)
        else:
            backend_params = params.get_default_runtime_params()

        if not backend_params.has_create_database:
            key = f'{edbdef.EDGEDB_TEMPLATE_DB}metadata'
            lock = dbops.Query(
                trampoline.fixup_query(f'''
                SELECT
                    json
                FROM
                    edgedbinstdata_VER.instdata
                WHERE
                    key = {ql(key)}
                FOR UPDATE
                INTO _dummy_text
            '''
            ))
        elif backend_params.has_superuser_access:
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
                            WHERE datname =
                              {V('edgedb')}.get_database_backend_name(
                                {ql(edbdef.EDGEDB_TEMPLATE_DB)})
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
                    edgedb_VER.raise_on_not_null(
                        (
                            SELECT 'locked'
                            FROM pg_catalog.pg_locks
                            WHERE
                                locktype = 'object'
                                AND classid = 'pg_database'::regclass::oid
                                AND objid = (
                                    SELECT oid
                                    FROM pg_database
                                    WHERE datname =
                                      {V('edgedb')}.get_database_backend_name(
                                        {ql(edbdef.EDGEDB_TEMPLATE_DB)})
                                )
                                AND mode = 'ShareUpdateExclusiveLock'
                                AND granted
                                AND pid != pg_backend_pid()
                        ),
                        'serialization_failure',
                        msg => (
                            'Cannot serialize global DDL: '
                            || (SELECT version::text FROM
                                {V('edgedb')}."_SysGlobalSchemaVersion")
                        )
                    )
                INTO _dummy_text
            ''')

        self.pgops.add(lock)

        expected_ver = self.get_orig_attribute_value('version')
        check = dbops.Query(
            f'''
                SELECT
                    edgedb_VER.raise_on_not_null(
                        (SELECT NULLIF(
                            (SELECT
                                version::text
                            FROM
                                {V('edgedb')}."_SysGlobalSchemaVersion"
                            ),
                            {ql(str(expected_ver))}
                        )),
                        'serialization_failure',
                        msg => (
                            'Cannot serialize global DDL: '
                            || (SELECT version::text FROM
                                {V('edgedb')}."_SysGlobalSchemaVersion")
                        )
                    )
                INTO _dummy_text
            '''
        )
        self.pgops.add(check)

        metadata = {
            ver_id: {
                'id': ver_id,
                'name': ver_name,
                'version': str(self.scls.get_version(schema)),
                'builtin': self.scls.get_builtin(schema),
                'internal': self.scls.get_internal(schema),
            }
        }
        if backend_params.has_create_database:
            self.pgops.add(
                dbops.UpdateMetadataSection(
                    dbops.DatabaseWithTenant(name=edbdef.EDGEDB_TEMPLATE_DB),
                    section='GlobalSchemaVersion',
                    metadata=metadata
                )
            )
        else:
            self.pgops.add(
                dbops.UpdateSingleDBMetadataSection(
                    edbdef.EDGEDB_TEMPLATE_DB,
                    section='GlobalSchemaVersion',
                    metadata=metadata
                )
            )

        return schema


class PseudoTypeCommand(MetaCommand):
    pass


class CreatePseudoType(
    PseudoTypeCommand,
    adapts=s_pseudo.CreatePseudoType,
):
    pass


class TupleCommand(MetaCommand):

    pass


class CreateTuple(TupleCommand, adapts=s_types.CreateTuple):

    @classmethod
    def create_tuple(
        cls,
        tup: s_types.Tuple,
        schema: s_schema.Schema,
        conditional: bool=False,
    ) -> dbops.Command:
        elements = tup.get_element_types(schema).items(schema)

        name = common.get_backend_name(schema, tup, catenate=False)
        ctype = dbops.CompositeType(
            name=name,
            columns=[
                dbops.Column(
                    name=n,
                    type=qt(types.pg_type_from_object(
                        schema, t, persistent_tuples=True)),
                )
                for n, t in elements
            ]
        )

        neg_conditions = []
        if conditional:
            neg_conditions.append(dbops.TypeExists(name=name))

        return dbops.CreateCompositeType(
            type=ctype, neg_conditions=neg_conditions)

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)

        if self.scls.is_polymorphic(schema):
            return schema

        self.pgops.add(self.create_tuple(
            self.scls,
            schema,
            # XXX: WHY
            conditional=context.stdmode,
        ))

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
            domain_name = common.get_backend_name(schema, tup, catenate=False)
            assert isinstance(domain_name, tuple)
            self.pgops.add(drop_dependant_func_cache(domain_name))
            self.pgops.add(dbops.DropCompositeType(name=domain_name))

        schema = super().apply(schema, context)

        return schema


class ExprAliasCommand(MetaCommand):
    pass


class CreateAlias(
    ExprAliasCommand,
    adapts=s_aliases.CreateAlias,
):
    pass


class RenameAlias(
    ExprAliasCommand,
    adapts=s_aliases.RenameAlias,
):
    pass


class AlterAlias(
    ExprAliasCommand,
    adapts=s_aliases.AlterAlias,
):
    pass


class DeleteAlias(
    ExprAliasCommand,
    adapts=s_aliases.DeleteAlias,
):
    pass


class GlobalCommand(MetaCommand):
    pass


class CreateGlobal(
    GlobalCommand,
    adapts=s_globals.CreateGlobal,
):
    pass


class RenameGlobal(
    GlobalCommand,
    adapts=s_globals.RenameGlobal,
):
    pass


class AlterGlobal(
    GlobalCommand,
    adapts=s_globals.AlterGlobal,
):
    pass


class SetGlobalType(
    GlobalCommand,  # ???
    adapts=s_globals.SetGlobalType,
):
    def register_config_op(self, op, context):
        ops = context.get(sd.DeltaRootContext).op.config_ops
        ops.append(op)

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:

        schema = super().apply(schema, context)
        if self.reset_value:
            op = config_ops.Operation(
                opcode=config_ops.OpCode.CONFIG_RESET,
                scope=ql_ft.ConfigScope.GLOBAL,
                setting_name=str(self.scls.get_name(schema)),
                value=None,
            )
            self.register_config_op(op, context)

        return schema


class DeleteGlobal(
    GlobalCommand,
    adapts=s_globals.DeleteGlobal,
):
    pass


class AccessPolicyCommand(MetaCommand):
    pass


class CreateAccessPolicy(
    AccessPolicyCommand,
    adapts=s_policies.CreateAccessPolicy,
):
    pass


class RenameAccessPolicy(
    AccessPolicyCommand,
    adapts=s_policies.RenameAccessPolicy,
):
    pass


class RebaseAccessPolicy(
    AccessPolicyCommand,
    adapts=s_policies.RebaseAccessPolicy,
):
    pass


class AlterAccessPolicy(
    AccessPolicyCommand,
    adapts=s_policies.AlterAccessPolicy,
):
    pass


class DeleteAccessPolicy(
    AccessPolicyCommand,
    adapts=s_policies.DeleteAccessPolicy,
):
    pass


class TriggerCommand(MetaCommand):
    pass


class CreateTrigger(
    TriggerCommand,
    adapts=s_triggers.CreateTrigger,
):
    pass


class RenameTrigger(
    TriggerCommand,
    adapts=s_triggers.RenameTrigger,
):
    pass


class RebaseTrigger(
    TriggerCommand,
    adapts=s_triggers.RebaseTrigger,
):
    pass


class AlterTrigger(
    TriggerCommand,
    adapts=s_triggers.AlterTrigger,
):
    pass


class DeleteTrigger(
    TriggerCommand,
    adapts=s_triggers.DeleteTrigger,
):
    pass


class RewriteCommand(MetaCommand):
    pass


class CreateRewrite(
    RewriteCommand,
    adapts=s_rewrites.CreateRewrite,
):
    pass


class RebaseRewrite(
    RewriteCommand,
    adapts=s_rewrites.RebaseRewrite,
):
    pass


class RenameRewrite(
    RewriteCommand,
    adapts=s_rewrites.RenameRewrite,
):
    pass


class AlterRewrite(
    RewriteCommand,
    adapts=s_rewrites.AlterRewrite,
):
    pass


class DeleteRewrite(
    RewriteCommand,
    adapts=s_rewrites.DeleteRewrite,
):
    pass


class TupleExprAliasCommand(MetaCommand):
    pass


class CreateTupleExprAlias(
    TupleExprAliasCommand,
    adapts=s_types.CreateTupleExprAlias,
):
    pass


class RenameTupleExprAlias(
    TupleExprAliasCommand,
    adapts=s_types.RenameTupleExprAlias,
):
    pass


class AlterTupleExprAlias(
    TupleExprAliasCommand,
    adapts=s_types.AlterTupleExprAlias,
):
    pass


class DeleteTupleExprAlias(
    TupleExprAliasCommand,
    adapts=s_types.DeleteTupleExprAlias,
):
    pass


class ArrayCommand(MetaCommand):
    pass


class CreateArray(ArrayCommand, adapts=s_types.CreateArray):
    pass


class AlterArray(ArrayCommand, adapts=s_types.AlterArray):
    pass


class RenameArray(ArrayCommand, adapts=s_types.RenameArray):
    pass


class DeleteArray(ArrayCommand, adapts=s_types.DeleteArray):
    pass


class ArrayExprAliasCommand(MetaCommand):
    pass


class CreateArrayExprAlias(
    ArrayExprAliasCommand,
    adapts=s_types.CreateArrayExprAlias,
):
    pass


class RenameArrayExprAlias(
    ArrayExprAliasCommand,
    adapts=s_types.RenameArrayExprAlias,
):
    pass


class AlterArrayExprAlias(
    ArrayExprAliasCommand,
    adapts=s_types.AlterArrayExprAlias,
):
    pass


class DeleteArrayExprAlias(
    ArrayExprAliasCommand,
    adapts=s_types.DeleteArrayExprAlias,
):
    pass


class RangeCommand(MetaCommand):
    pass


class CreateRange(RangeCommand, adapts=s_types.CreateRange):
    pass


class AlterRange(RangeCommand, adapts=s_types.AlterRange):
    pass


class RenameRange(RangeCommand, adapts=s_types.RenameRange):
    pass


class DeleteRange(RangeCommand, adapts=s_types.DeleteRange):
    pass


class RangeExprAliasCommand(MetaCommand):
    pass


class CreateRangeExprAlias(
    RangeExprAliasCommand,
    adapts=s_types.CreateRangeExprAlias,
):
    pass


class RenameRangeExprAlias(
    RangeExprAliasCommand,
    adapts=s_types.RenameRangeExprAlias,
):
    pass


class AlterRangeExprAlias(
    RangeExprAliasCommand,
    adapts=s_types.AlterRangeExprAlias,
):
    pass


class DeleteRangeExprAlias(
    RangeExprAliasCommand,
    adapts=s_types.DeleteRangeExprAlias,
):
    pass


class MultiRangeCommand(MetaCommand):
    pass


class CreateMultiRange(MultiRangeCommand, adapts=s_types.CreateMultiRange):
    pass


class AlterMultiRange(MultiRangeCommand, adapts=s_types.AlterMultiRange):
    pass


class RenameMultiRange(MultiRangeCommand, adapts=s_types.RenameMultiRange):
    pass


class DeleteMultiRange(MultiRangeCommand, adapts=s_types.DeleteMultiRange):
    pass


class ParameterCommand(MetaCommand):
    pass


class CreateParameter(
    ParameterCommand,
    adapts=s_funcs.CreateParameter,
):
    pass


class DeleteParameter(
    ParameterCommand,
    adapts=s_funcs.DeleteParameter,
):
    pass


class RenameParameter(
    ParameterCommand,
    adapts=s_funcs.RenameParameter,
):
    pass


class AlterParameter(
    ParameterCommand,
    adapts=s_funcs.AlterParameter,
):
    pass


class FunctionCommand(MetaCommand):
    def get_pgname(self, func: s_funcs.Function, schema, versioned: bool=False):
        return common.get_backend_name(
            schema, func, catenate=False, versioned=versioned)

    def get_pgtype(self, func: s_funcs.CallableObject, obj, schema):
        if obj.is_any(schema):
            return ('anyelement',)

        try:
            return types.pg_type_from_object(
                schema, obj, persistent_tuples=True)
        except ValueError:
            raise errors.QueryError(
                f'could not compile parameter type {obj!r} '
                f'of function {func.get_shortname(schema)}',
                span=self.span) from None

    def compile_default(
        self, func: s_funcs.Function, default: s_expr.Expression, schema
    ):
        try:
            comp = default.compiled(
                schema=schema,
                as_fragment=True,
                context=None,
            )

            ir = comp.irast
            if not irutils.is_const(ir.expr):
                raise ValueError('expression not constant')

            sql_res = compiler.compile_ir_to_sql_tree(
                ir.expr, singleton_mode=True)
            return codegen.generate_source(sql_res.ast)

        except Exception as ex:
            raise errors.QueryError(
                f'could not compile default expression {default!r} '
                f'of function {func.get_shortname(schema)}: {ex}',
                span=self.span) from ex

    def compile_args(self, func: s_funcs.Function, schema):
        func_params = func.get_params(schema)
        has_inlined_defaults = func.has_inlined_defaults(schema)

        args = []

        func_language = func.get_language(schema)
        if func_language is ql_ast.Language.EdgeQL:
            args.append(('__edb_json_globals__', ('jsonb',), None))

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

            pn = param.get_parameter_name(schema)
            args.append((pn, pg_at, default))

            if param_type.is_object_type():
                args.append((f'__{pn}__type', ('uuid',), None))

        return args

    def make_function(self, func: s_funcs.Function, code, schema):
        func_return_typemod = func.get_return_typemod(schema)
        func_params = func.get_params(schema)

        name = self.get_pgname(func, schema, versioned=False)
        return self.get_function_type(name)(
            name=name,
            args=self.compile_args(func, schema),
            has_variadic=func_params.find_variadic(schema) is not None,
            set_returning=func_return_typemod is ql_ft.TypeModifier.SetOfType,
            volatility=func.get_volatility(schema),
            strict=func.get_impl_is_strict(schema),
            returns=self.get_pgtype(
                func, func.get_return_type(schema), schema),
            text=code,
        )

    def compile_sql_function(self, func: s_funcs.Function, schema):
        return self.make_function(func, func.get_code(schema), schema)

    def _compile_edgeql_function(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        func: s_funcs.Function,
        body: s_expr.Expression,
    ) -> s_expr.CompiledExpression:
        if isinstance(body, s_expr.CompiledExpression):
            return body

        # HACK: When an object type selected by a function (via
        # inheritance) is dropped, the function gets
        # recompiled. Unfortunately, 'caused' subcommands run *before*
        # the object is actually deleted, and so we would ordinarily
        # still try to select from the deleted object. To avoid
        # needing to add *another* type of subcommand, we work around
        # this by temporarily stripping all objects that are about to
        # be deleted from the schema.
        for ctx in context.stack:
            if isinstance(ctx.op, s_objtypes.DeleteObjectType):
                schema = schema.delete(ctx.op.scls)

        return s_funcs.compile_function(
            schema,
            context,
            body=body,
            func_name=func.get_name(schema),
            params=func.get_params(schema),
            language=ql_ast.Language.EdgeQL,
            return_type=func.get_return_type(schema),
            return_typemod=func.get_return_typemod(schema),
        )

    def fix_return_type(
        self,
        func: s_funcs.Function,
        nativecode: s_expr.CompiledExpression,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_expr.CompiledExpression:

        return_type = func.get_return_type(schema)
        ir = nativecode.irast

        if not (
            return_type.is_object_type()
            or s_types.is_type_compatible(return_type, ir.stype,
                                          schema=nativecode.schema)
        ):
            # Add a cast and recompile it
            qlexpr = qlcompiler.astutils.ensure_ql_query(
                ql_ast.TypeCast(
                    type=s_utils.typeref_to_ast(schema, return_type),
                    expr=nativecode.parse(),
                )
            )
            nativecode = self._compile_edgeql_function(
                schema,
                context,
                func,
                type(nativecode).from_ast(qlexpr, schema),
            )

        return nativecode

    def compile_edgeql_function_body(
        self,
        func: s_funcs.Function,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> str:
        nativecode = func.get_nativecode(schema)
        assert nativecode
        nativecode = self._compile_edgeql_function(
            schema,
            context,
            func,
            nativecode,
        )

        nativecode = self.fix_return_type(func, nativecode, schema, context)

        sql_res = compiler.compile_ir_to_sql_tree(
            nativecode.irast,
            ignore_shapes=True,
            explicit_top_cast=irtyputils.type_to_typeref(  # note: no cache
                schema, func.get_return_type(schema), cache=None),
            output_format=compiler.OutputFormat.NATIVE,
            named_param_prefix=self.get_pgname(func, schema)[-1:],
            versioned_stdlib=context.stdmode,
        )

        return codegen.generate_source(sql_res.ast)

    def compile_edgeql_overloaded_function_body(
        self,
        func: s_funcs.Function,
        overloads: List[s_funcs.Function],
        ov_param_idx: int,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> str:
        func_return_typemod = func.get_return_typemod(schema)
        set_returning = func_return_typemod is ql_ft.TypeModifier.SetOfType
        my_params = func.get_params(schema).objects(schema)
        param_name = my_params[ov_param_idx].get_parameter_name(schema)
        type_param_name = f'__{param_name}__type'
        cases = {}
        all_overloads = list(overloads)
        if not isinstance(self, DeleteFunction):
            all_overloads.append(func)
        for overload in all_overloads:
            ov_p = tuple(overload.get_params(schema).objects(schema))
            ov_p_t = ov_p[ov_param_idx].get_type(schema)
            ov_body = self.compile_edgeql_function_body(
                overload, schema, context)

            if set_returning:
                case = (
                    f"(SELECT * FROM ({ov_body}) AS q "
                    f"WHERE ancestor = {ql(str(ov_p_t.id))})"
                )
            else:
                case = (
                    f"WHEN ancestor = {ql(str(ov_p_t.id))} "
                    f"THEN \n({ov_body})"
                )

            cases[ov_p_t] = case

        impl_ids = ', '.join(f'{ql(str(t.id))}::uuid' for t in cases)
        branches = list(cases.values())

        # N.B: edgedb_VER.raise and coalesce are used below instead of
        #      raise_on_null, because the latter somehow results in a
        #      significantly more complex query plan.
        matching_impl = f"""
            coalesce(
                (
                    SELECT
                        ancestor
                    FROM
                        (SELECT
                            {qi(type_param_name)} AS ancestor,
                            -1 AS index
                        UNION ALL
                        SELECT
                            target AS ancestor,
                            index
                        FROM
                            edgedb._object_ancestors
                            WHERE source = {qi(type_param_name)}
                        ) a
                    WHERE ancestor IN ({impl_ids})
                    ORDER BY index
                    LIMIT 1
                ),

                edgedb.raise(
                    NULL::uuid,
                    'assert_failure',
                    msg => format(
                        'unhandled object type %s in overloaded function',
                        {qi(type_param_name)}
                    )
                )
            ) AS impl(ancestor)
        """

        if set_returning:
            arms = "\nUNION ALL\n".join(branches)
            return f"""
                SELECT
                    q.*
                FROM
                    {matching_impl},
                    LATERAL (
                        {arms}
                    ) AS q
            """
        else:
            arms = "\n".join(branches)
            return f"""
                SELECT
                    (CASE {arms} END)
                FROM
                    {matching_impl}
            """

    def compile_edgeql_function(
        self,
        func: s_funcs.Function,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> tuple[Optional[dbops.Function], bool]:
        if func.get_volatility(schema) == ql_ft.Volatility.Modifying:
            # Modifying functions cannot be compiled correctly and should be
            # inlined at the call point.

            if func.find_object_param_overloads(schema) is not None:
                raise errors.SchemaDefinitionError(
                    f"cannot overload an existing function "
                    f"with a modifying function: "
                    f"'{func.get_shortname(schema)}'",
                    span=self.span,
                )

            return None, False

        nativecode: s_expr.Expression = not_none(func.get_nativecode(schema))
        compiled_expr = self._compile_edgeql_function(
            schema, context, func, nativecode
        )
        compiled_expr = self.fix_return_type(
            func, compiled_expr, schema, context
        )

        replace = False

        obj_overload = func.find_object_param_overloads(schema)
        if obj_overload is not None:
            overloads, ov_param_idx = obj_overload
            if any(
                overload.get_volatility(schema) == ql_ft.Volatility.Modifying
                for overload in overloads
            ):
                raise errors.SchemaDefinitionError(
                    f"cannot overload an existing modifying function: "
                    f"'{func.get_shortname(schema)}'",
                    span=self.span,
                )

            body = self.compile_edgeql_overloaded_function_body(
                func, overloads, ov_param_idx, schema, context
            )
            replace = True
        else:
            sql_res = compiler.compile_ir_to_sql_tree(
                compiled_expr.irast,
                ignore_shapes=True,
                explicit_top_cast=irtyputils.type_to_typeref(  # note: no cache
                    schema, func.get_return_type(schema), cache=None),
                output_format=compiler.OutputFormat.NATIVE,
                named_param_prefix=self.get_pgname(func, schema)[-1:],
                backend_runtime_params=context.backend_runtime_params,
                versioned_stdlib=context.stdmode,
            )
            body = codegen.generate_source(sql_res.ast)

        return self.make_function(func, body, schema), replace

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
                edgedb_VER.raise_on_not_null(
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

    def sql_strict_consistency_check(
        self,
        cobj: s_funcs.CallableObject,
        func: str,
        schema: s_schema.Schema,
    ) -> dbops.Command:
        fname = cobj.get_verbosename(schema)

        # impl_is_strict means that the function is strict in all
        # singleton arguments, so we don't need to do the check if
        # no such arguments exist.
        if (
            not cobj.get_impl_is_strict(schema)
            or not cobj.get_params(schema).has_type_mod(
                schema, ql_ft.TypeModifier.SingletonType
            )
        ):
            return dbops.CommandGroup()

        if '.' in func:
            ns, func = func.split('.')
        else:
            ns = 'pg_catalog'

        f_test = textwrap.dedent(f'''\
            COALESCE((
                SELECT bool_and(proisstrict) FROM pg_proc
                INNER JOIN pg_namespace ON pg_namespace.oid = pronamespace
                WHERE proname = {ql(func)} AND nspname = {ql(ns)}
            ), false)
        ''')

        check = dbops.Query(text=f'''
            PERFORM
                edgedb_VER.raise_on_null(
                    NULLIF(
                        false,
                        {f_test}
                    ),
                    'invalid_function_definition',
                    msg => format(
                        '%s is declared to have a strict impl but does not',
                        {ql(fname)}
                    ),
                    hint => (
                        'Add `impl_is_strict := false` to the declaration.'
                    )
                );
        ''')

        return check

    def get_dummy_func_call(
        self,
        cobj: s_funcs.CallableObject,
        sql_func: Sequence[str],
        schema: s_schema.Schema,
    ) -> str:
        name = common.maybe_versioned_name(
            tuple(sql_func),
            versioned=(
                cobj.get_name(schema).get_root_module_name().name != 'ext'
            ),
        )

        args = []
        func_params = cobj.get_params(schema)
        for param in func_params.get_in_canonical_order(schema):
            param_type = param.get_type(schema)
            pg_at = self.get_pgtype(cobj, param_type, schema)
            args.append(f'NULL::{qt(pg_at)}')

        return f'{q(*name)}({", ".join(args)})'

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
                dexpr = self.get_dummy_func_call(
                    func, sql_func.split('.'), schema)
                return (
                    self.sql_rval_consistency_check(func, dexpr, schema),
                    self.sql_strict_consistency_check(func, sql_func, schema),
                )
        else:
            func_language = func.get_language(schema)

            dbf: Optional[dbops.Function]
            if func_language is ql_ast.Language.SQL:
                dbf = self.compile_sql_function(func, schema)
            elif func_language is ql_ast.Language.EdgeQL:
                dbf, overload_replace = self.compile_edgeql_function(
                    func, schema, context
                )
                if overload_replace:
                    or_replace = True
            else:
                raise errors.QueryError(
                    f'cannot compile function {func.get_shortname(schema)}: '
                    f'unsupported language {func_language}',
                    span=self.span)

            ops: list[dbops.Command] = []

            if dbf is not None:
                ops.append(dbops.CreateFunction(dbf, or_replace=or_replace))
                self.maybe_trampoline(dbf, context)
            return ops


class CreateFunction(
    FunctionCommand,
    adapts=s_funcs.CreateFunction,
):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        ops = self.make_op(self.scls, schema, context)
        self.pgops.update(ops)
        return schema


class RenameFunction(FunctionCommand, adapts=s_funcs.RenameFunction):
    pass


class AlterFunction(FunctionCommand, adapts=s_funcs.AlterFunction):
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
            self.get_attribute_value('nativecode') is not None or
            self.get_attribute_value('code') is not None
        ):
            self.pgops.update(
                self.make_op(self.scls, schema, context, or_replace=True))

        return schema


class DeleteFunction(FunctionCommand, adapts=s_funcs.DeleteFunction):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        func = self.get_object(schema, context)
        nativecode = func.get_nativecode(schema)

        if func.get_code(schema) or nativecode:
            # An EdgeQL or a SQL function
            # (not just an alias to a SQL function).

            overload = False
            if nativecode and func.find_object_param_overloads(schema):
                dbf, overload_replace = self.compile_edgeql_function(
                    func, schema, context
                )
                if dbf is not None and overload_replace:
                    self.pgops.add(dbops.CreateFunction(dbf, or_replace=True))
                    overload = True

            if not overload:
                variadic = func.get_params(schema).find_variadic(schema)
                if func.get_volatility(schema) != ql_ft.Volatility.Modifying:
                    # Modifying functions are not compiled.
                    # See: compile_edgeql_function
                    self.pgops.add(
                        dbops.DropFunction(
                            name=self.get_pgname(func, schema),
                            args=self.compile_args(func, schema),
                            has_variadic=variadic is not None,
                        )
                    )

        return super().apply(schema, context)


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
                f'unexpected operator type: {oper_kind!r}')

        return left_type, right_type

    # FIXME: We should make split FunctionCommand into CallableCommand
    # and FunctionCommand and only inherit from CallableCommand
    def compile_args(self, oper: s_opers.Operator, schema):  # type: ignore
        args = []
        oper_params = oper.get_params(schema)

        for param in oper_params.get_in_canonical_order(schema):
            pg_at = self.get_pgtype(oper, param.get_type(schema), schema)
            args.append((param.get_parameter_name(schema), pg_at))

        return args

    def make_operator_function(self, oper: s_opers.Operator, schema):
        name = common.get_backend_name(
            schema, oper, catenate=False, versioned=False, aspect='function')
        return self.get_function_type(name)(
            name=name,
            args=self.compile_args(oper, schema),
            volatility=oper.get_volatility(schema),
            returns=self.get_pgtype(
                oper, oper.get_return_type(schema), schema),
            text=not_none(oper.get_code(schema)),
        )

    def get_dummy_operator_call(
        self,
        oper: s_opers.Operator,
        pgop: str,
        from_args: Sequence[Tuple[str, ...] | str],
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


class CreateOperator(OperatorCommand, adapts=s_opers.CreateOperator):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        oper = self.scls
        if oper.get_abstract(schema):
            return schema

        params = oper.get_params(schema)
        oper_language = oper.get_language(schema)
        oper_fromop = oper.get_from_operator(schema)
        oper_fromfunc = oper.get_from_function(schema)
        oper_code = oper.get_code(schema)

        # We support having both fromop and one of the others for
        # "legacy" purposes, but ignore it.
        if oper_code or oper_fromfunc:
            oper_fromop = None

        if oper_language is ql_ast.Language.SQL and oper_fromop:
            pg_oper_name = oper_fromop[0]
            args = self.get_pg_operands(schema, oper)
            if len(oper_fromop) > 1:
                # Explicit operand types given in FROM SQL OPERATOR.
                from_args = oper_fromop[1:]
            else:
                from_args = args

            if (
                pg_oper_name is not None
                and not params.has_polymorphic(schema)
                and not oper.get_force_return_cast(schema)
            ):
                cexpr = self.get_dummy_operator_call(
                    oper, pg_oper_name, from_args, schema)

                # We don't do a strictness consistency check for
                # USING SQL OPERATOR because they are heavily
                # overloaded, and so we'd need to take the types
                # into account; this is doable, but doesn't seem
                # worth doing since the only non-strict operator
                # is || on arrays, and we use array_cat for that
                # anyway!
                check = self.sql_rval_consistency_check(
                    oper, cexpr, schema)
                self.pgops.add(check)

        elif oper_language is ql_ast.Language.SQL and oper_code:
            args = self.get_pg_operands(schema, oper)
            oper_func = self.make_operator_function(oper, schema)
            self.pgops.add(dbops.CreateFunction(oper_func))

            self.maybe_trampoline(oper_func, context)

            if not params.has_polymorphic(schema):
                cexpr = self.get_dummy_func_call(
                    oper, oper_func.name, schema)
                check = self.sql_rval_consistency_check(oper, cexpr, schema)
                self.pgops.add(check)

        elif oper_language is ql_ast.Language.SQL and oper_fromfunc:
            args = self.get_pg_operands(schema, oper)
            oper_func_name = oper_fromfunc[0]
            if len(oper_fromfunc) > 1:
                args = oper_fromfunc[1:]

            cargs = []
            for t in args:
                if t is not None:
                    cargs.append(f'NULL::{qt(t)}')

            if not params.has_polymorphic(schema):
                cexpr = f"{qi(oper_func_name)}({', '.join(cargs)})"
                check = self.sql_rval_consistency_check(oper, cexpr, schema)
                self.pgops.add(check)
            check2 = self.sql_strict_consistency_check(
                oper, oper_func_name, schema)
            self.pgops.add(check2)

        elif oper.get_from_expr(schema):
            # This operator is handled by the compiler and does not
            # need explicit representation in the backend.
            pass

        else:
            raise errors.QueryError(
                f'cannot create operator {oper.get_shortname(schema)}: '
                f'only "FROM SQL" and "FROM SQL OPERATOR" operators '
                f'are currently supported',
                span=self.span)

        return schema


class RenameOperator(OperatorCommand, adapts=s_opers.RenameOperator):
    pass


class AlterOperator(OperatorCommand, adapts=s_opers.AlterOperator):
    pass


class DeleteOperator(OperatorCommand, adapts=s_opers.DeleteOperator):
    pass


class CastCommand(MetaCommand):
    def make_cast_function(self, cast: s_casts.Cast, schema):
        name = common.get_backend_name(
            schema, cast, catenate=False, versioned=False, aspect='function')

        args: Sequence[dbops.FunctionArg] = [
            (
                'val',
                types.pg_type_from_object(schema, cast.get_from_type(schema))
            ),
            ('detail', ('text',), "''"),
        ]

        returns = types.pg_type_from_object(schema, cast.get_to_type(schema))

        # N.B: Semantically, strict *ought* to be true, since we want
        # all of our casts to have strict behavior. Unfortunately,
        # actually marking them as strict causes a huge performance
        # regression when bootstrapping (and probably anything else that
        # is heavy on json casts), so instead we just need to make sure
        # to write cast code that is naturally strict (this is enforced
        # by test_edgeql_casts_all_null).
        return self.get_function_type(name)(
            name=name,
            args=args,
            returns=returns,
            strict=False,
            text=not_none(cast.get_code(schema)),
        )


class CreateCast(CastCommand, adapts=s_casts.CreateCast):
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
            self.maybe_trampoline(cast_func, context)

        elif from_cast is not None or from_expr is not None:
            # This operator is handled by the compiler and does not
            # need explicit representation in the backend.
            pass

        else:
            raise errors.QueryError(
                f'cannot create cast: '
                f'only "FROM SQL" and "FROM SQL FUNCTION" casts '
                f'are currently supported',
                span=self.span)

        return schema


class RenameCast(CastCommand, adapts=s_casts.RenameCast):
    pass


class AlterCast(CastCommand, adapts=s_casts.AlterCast):
    pass


class DeleteCast(CastCommand, adapts=s_casts.DeleteCast):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        cast = schema.get(self.classname, type=s_casts.Cast)
        cast_language = cast.get_language(schema)
        cast_code = cast.get_code(schema)

        schema = super().apply(schema, context)

        if cast_language is ql_ast.Language.SQL and cast_code:
            cast_func = self.make_cast_function(cast, orig_schema)
            self.pgops.add(dbops.DropFunction(
                cast_func.name, cast_func.args))

        return schema


class AnnotationCommand(MetaCommand):
    pass


class CreateAnnotation(AnnotationCommand, adapts=s_anno.CreateAnnotation):
    pass


class RenameAnnotation(AnnotationCommand, adapts=s_anno.RenameAnnotation):
    pass


class AlterAnnotation(AnnotationCommand, adapts=s_anno.AlterAnnotation):
    pass


class DeleteAnnotation(AnnotationCommand, adapts=s_anno.DeleteAnnotation):
    pass


class AnnotationValueCommand(MetaCommand):
    pass


class CreateAnnotationValue(
    AnnotationValueCommand,
    adapts=s_anno.CreateAnnotationValue,
):
    pass


class AlterAnnotationValue(
    AnnotationValueCommand,
    adapts=s_anno.AlterAnnotationValue,
):
    pass


class AlterAnnotationValueOwned(
    AnnotationValueCommand,
    adapts=s_anno.AlterAnnotationValueOwned,
):
    pass


class RenameAnnotationValue(
    AnnotationValueCommand,
    adapts=s_anno.RenameAnnotationValue,
):
    pass


class RebaseAnnotationValue(
    AnnotationValueCommand,
    adapts=s_anno.RebaseAnnotationValue,
):
    pass


class DeleteAnnotationValue(
    AnnotationValueCommand,
    adapts=s_anno.DeleteAnnotationValue,
):
    pass


class ConstraintCommand(MetaCommand):
    @classmethod
    def constraint_is_effective(
        cls, schema: s_schema.Schema, constraint: s_constr.Constraint
    ) -> bool:
        subject = constraint.get_subject(schema)
        if subject is None:
            return False

        ancestors = [
            a for a in constraint.get_ancestors(schema).objects(schema)
            if not a.is_non_concrete(schema)
        ]

        if (
            constraint.get_delegated(schema)
            and all(ancestor.get_delegated(schema) for ancestor in ancestors)
        ):
            return False

        if irtyputils.is_cfg_view(subject, schema):
            return False

        match subject:
            case s_pointers.Pointer():
                if subject.is_non_concrete(schema):
                    return True
                else:
                    return types.has_table(subject.get_source(schema), schema)
            case s_objtypes.ObjectType():
                return types.has_table(subject, schema)
            case s_scalars.ScalarType():
                return not subject.get_abstract(schema)
        raise NotImplementedError(subject)

    def schedule_relatives_constraint_trigger_update(
        self,
        constraint: s_constr.Constraint,
        orig_schema: s_schema.Schema,
        curr_schema: s_schema.Schema,
        context: sd.CommandContext,
    ):
        # Find all origins whose relationship with the constraint has changed.
        orig_origins: dict[uuid.UUID, s_constr.Constraint] = {}
        if orig_schema.has_object(constraint.id):
            for origin in constraint.get_constraint_origins(orig_schema):
                orig_origins[origin.id] = origin
        curr_origins: dict[uuid.UUID, s_constr.Constraint] = {}
        if curr_schema.has_object(constraint.id):
            for origin in constraint.get_constraint_origins(curr_schema):
                curr_origins[origin.id] = origin

        # Find all constraints whose inheritance relationship with the
        # constraint has changed.
        relative_ids: set[uuid.UUID] = set()
        for origin_id in (orig_origins.keys() - curr_origins.keys()):
            origin = orig_origins[origin_id]
            for relative in (
                [origin] + list(origin.descendants(orig_schema))
            ):
                if not curr_schema.has_object(relative.id):
                    # The constraint was deleted, updating the triggers is
                    # not needed.
                    continue
                relative_ids.add(relative.id)

        for origin_id in (curr_origins.keys() - orig_origins.keys()):
            origin = curr_origins[origin_id]
            for relative in (
                [origin] + list(origin.descendants(curr_schema))
            ):
                relative_ids.add(relative.id)

        relatives: list[s_constr.Constraint] = [
            curr_schema.get_by_id(relative_id, type=s_constr.Constraint)
            for relative_id in relative_ids
        ]

        op = dbops.CommandGroup()

        # Schedule constraint trigger updates for relatives.
        for relative in relatives:
            self.schedule_constraint_trigger_update(
                relative,
                curr_schema,
                context,
                s_sources.SourceCommandContext,
            )

        return op

    @staticmethod
    def create_constraint(
        current_command: MetaCommand,
        constraint: s_constr.Constraint,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        span: Optional[parsing.Span] = None,
        *,
        create_triggers_if_needed: bool = True,
    ) -> dbops.Command:
        op = dbops.CommandGroup()
        if ConstraintCommand.constraint_is_effective(schema, constraint):
            subject = constraint.get_subject(schema)

            if subject is not None:
                op.add_command(ConstraintCommand._get_create_ops(
                    current_command,
                    constraint,
                    schema,
                    context,
                    span,
                    create_triggers_if_needed=create_triggers_if_needed,
                ))

        return op

    @staticmethod
    def _get_create_ops(
        current_command: MetaCommand,
        constraint: s_constr.Constraint,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        span: Optional[parsing.Span] = None,
        *,
        create_triggers_if_needed: bool = True,
    ) -> dbops.CommandGroup:
        subject = constraint.get_subject(schema)
        assert subject is not None
        compiled_constraint = schemamech.compile_constraint(
            subject,
            constraint,
            schema,
            span,
        )

        op = compiled_constraint.create_ops()

        if create_triggers_if_needed:
            # Constraint triggers are created last to avoid repeated
            # recompilation.
            current_command.schedule_constraint_trigger_update(
                constraint,
                schema,
                context,
                s_sources.SourceCommandContext,
            )

        return op

    @staticmethod
    def _get_alter_ops(
        current_command: MetaCommand,
        constraint: s_constr.Constraint,
        orig_schema: s_schema.Schema,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        span: Optional[parsing.Span] = None,
    ) -> dbops.CommandGroup:
        orig_subject = constraint.get_subject(orig_schema)
        assert orig_subject is not None
        orig_compiled_constraint = schemamech.compile_constraint(
            orig_subject,
            constraint,
            orig_schema,
            span,
        )

        subject = constraint.get_subject(schema)
        assert subject is not None
        compiled_constraint = schemamech.compile_constraint(
            subject,
            constraint,
            schema,
            span,
        )

        op = compiled_constraint.alter_ops(orig_compiled_constraint)

        # Constraint triggers are created last to avoid repeated recompilation.
        current_command.schedule_constraint_trigger_update(
            constraint,
            schema,
            context,
            s_sources.SourceCommandContext,
        )

        return op

    @classmethod
    def delete_constraint(
        cls,
        constraint: s_constr.Constraint,
        schema: s_schema.Schema,
        span: Optional[parsing.Span] = None,
    ) -> dbops.Command:
        op = dbops.CommandGroup()
        if cls.constraint_is_effective(schema, constraint):
            subject = constraint.get_subject(schema)

            if subject is not None:
                bconstr = schemamech.compile_constraint(
                    subject, constraint, schema, span
                )

                op.add_command(bconstr.delete_ops())

        return op

    @classmethod
    def enforce_constraint(
        cls,
        constraint: s_constr.Constraint,
        schema: s_schema.Schema,
        span: Optional[parsing.Span] = None,
    ) -> dbops.Command:

        if cls.constraint_is_effective(schema, constraint):
            subject = constraint.get_subject(schema)

            if subject is not None:
                bconstr = schemamech.compile_constraint(
                    subject, constraint, schema, span
                )

                return bconstr.enforce_ops()
        else:
            return dbops.CommandGroup()


class CreateConstraint(ConstraintCommand, adapts=s_constr.CreateConstraint):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super().apply(schema, context)
        constraint: s_constr.Constraint = self.scls

        self.pgops.add(ConstraintCommand.create_constraint(
            self, constraint, schema, context, self.span
        ))

        self.pgops.add(self.schedule_relatives_constraint_trigger_update(
            constraint, orig_schema, schema, context,
        ))

        # If the constraint is being added to existing data,
        # we need to enforce it on the existing data. (This only
        # matters when inheritance is in play and we use triggers
        # to enforce exclusivity across tables.)
        if (
            (subject := constraint.get_subject(schema))
            and isinstance(
                subject, (s_objtypes.ObjectType, s_pointers.Pointer))
            and not context.is_creating(subject)
        ):
            self.pgops.add(self.enforce_constraint(
                constraint, schema, self.span
            ))

        return schema


class RenameConstraint(ConstraintCommand, adapts=s_constr.RenameConstraint):
    pass


class AlterConstraintOwned(
    ConstraintCommand,
    adapts=s_constr.AlterConstraintOwned,
):
    pass


class AlterConstraint(
    ConstraintCommand,
    adapts=s_constr.AlterConstraint,
):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super().apply(schema, context)
        constraint: s_constr.Constraint = self.scls
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
            if pcontext := context.get(s_pointers.PointerCommandContext):
                orig_schema = pcontext.original_schema

            op = dbops.CommandGroup()
            if not self.constraint_is_effective(orig_schema, constraint):
                op.add_command(ConstraintCommand._get_create_ops(
                    self, constraint, schema, context, self.span
                ))

                # XXX: I don't think any of this logic is needed??
                for child in constraint.children(schema):
                    op.add_command(ConstraintCommand._get_alter_ops(
                        self, child, orig_schema, schema, context, self.span
                    ))
            elif not self.constraint_is_effective(schema, constraint):
                op.add_command(ConstraintCommand._get_alter_ops(
                    self, constraint, orig_schema, schema, context, self.span
                ))

                for child in constraint.children(schema):
                    op.add_command(ConstraintCommand._get_alter_ops(
                        self, child, orig_schema, schema, context, self.span
                    ))
            else:
                op.add_command(ConstraintCommand._get_alter_ops(
                    self, constraint, orig_schema, schema, context, self.span
                ))
            self.pgops.add(op)

            if (
                (subject := constraint.get_subject(schema))
                and isinstance(
                    subject, (s_objtypes.ObjectType, s_pointers.Pointer))
                and not context.is_creating(subject)
                and not context.is_deleting(subject)
            ):
                self.pgops.add(self.enforce_constraint(
                    constraint, schema, self.span
                ))

            self.pgops.add(self.schedule_relatives_constraint_trigger_update(
                constraint, orig_schema, schema, context,
            ))

        return schema


class DeleteConstraint(ConstraintCommand, adapts=s_constr.DeleteConstraint):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        delta_root_ctx = context.top()
        orig_schema = delta_root_ctx.original_schema
        constraint: s_constr.Constraint = (
            schema.get(self.classname, type=s_constr.Constraint)
        )

        schema = super().apply(schema, context)
        op = self.delete_constraint(
            constraint, orig_schema, self.span
        )
        self.pgops.add(op)

        self.pgops.add(self.schedule_relatives_constraint_trigger_update(
            constraint, orig_schema, schema, context,
        ))

        return schema


class RebaseConstraint(ConstraintCommand, adapts=s_constr.RebaseConstraint):
    pass


class AliasCapableMetaCommand(MetaCommand):
    pass


class ScalarTypeMetaCommand(AliasCapableMetaCommand):

    @classmethod
    def is_sequence(cls, schema, scalar):
        seq = schema.get('std::sequence', default=None)
        return seq is not None and scalar.issubclass(schema, seq)


class CreateScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.CreateScalarType):

    @classmethod
    def create_scalar(
        cls,
        scalar: s_scalars.ScalarType,
        default: Optional[s_expr.Expression],
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> dbops.Command:

        if scalar.is_concrete_enum(schema):
            enum_values = scalar.get_enum_values(schema)
            assert enum_values

            return CreateScalarType.create_enum(
                scalar, enum_values, schema, context)
        else:
            ops = dbops.CommandGroup()

            if scalar.get_transient(schema):
                return ops

            base = types.get_scalar_base(schema, scalar)

            new_domain_name = types.pg_type_from_scalar(schema, scalar)

            if cls.is_sequence(schema, scalar):
                seq_name = common.get_backend_name(
                    schema, scalar, catenate=False, aspect='sequence')
                ops.add_command(dbops.CreateSequence(name=seq_name))

            domain = dbops.Domain(name=new_domain_name, base=base)
            ops.add_command(dbops.CreateDomain(domain=domain))

            if (default is not None
                    and not isinstance(default, s_expr.Expression)):
                # We only care to support literal defaults here. Supporting
                # defaults based on queries has no sense on the database
                # level since the database forbids queries for DEFAULT and
                # pre- calculating the value does not make sense either
                # since the whole point of query defaults is for them to be
                # dynamic.
                ops.add_command(
                    dbops.AlterDomainAlterDefault(
                        name=new_domain_name, default=default))

            return ops

    @classmethod
    def create_enum(
        cls,
        scalar: s_scalars.ScalarType,
        values: Sequence[str],
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> dbops.Command:
        ops = dbops.CommandGroup()

        new_enum_name = common.get_backend_name(schema, scalar, catenate=False)

        neg_conditions = []
        if context.stdmode:
            neg_conditions.append(dbops.EnumExists(name=new_enum_name))

        ops.add_command(
            dbops.CreateEnum(
                dbops.Enum(name=new_enum_name, values=values),
                neg_conditions=neg_conditions,
            )
        )

        fcls = cls.get_function_type(new_enum_name)

        # Cast wrapper function is needed for immutable casts, which are
        # needed for casting within indexes/constraints.
        # (Postgres casts are only stable)
        cast_func_name = common.get_backend_name(
            schema, scalar, catenate=False, aspect="enum-cast-from-str"
        )
        cast_func = fcls(
            name=cast_func_name,
            args=[("value", ("anyelement",))],
            volatility="immutable",
            returns=new_enum_name,
            text=f"SELECT value::{qt(new_enum_name)}",
        )
        ops.add_command(dbops.CreateFunction(cast_func))
        cls.maybe_trampoline(cast_func, context)

        # Simialry, uncast from enum to str
        uncast_func_name = common.get_backend_name(
            schema, scalar, catenate=False, aspect="enum-cast-into-str"
        )
        uncast_func = fcls(
            name=uncast_func_name,
            args=[("value", ("anyelement",))],
            volatility="immutable",
            returns="text",
            text=f"SELECT value::text",
        )
        ops.add_command(dbops.CreateFunction(uncast_func))
        cls.maybe_trampoline(uncast_func, context)
        return ops

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._create_begin(schema, context)
        scalar = self.scls

        if scalar.get_abstract(schema):
            return schema

        if types.is_builtin_scalar(schema, scalar):
            return schema
        # If this type exposes a SQL type or is a parameterized
        # subtype of a SQL type, we don't create a real type here.
        if scalar.resolve_sql_type_scheme(schema)[0]:
            return schema

        default = self.get_resolved_attribute_value(
            'default',
            schema=schema,
            context=context,
        )
        self.pgops.add(self.create_scalar(scalar, default, schema, context))

        return schema


class RenameScalarType(
    ScalarTypeMetaCommand,
    adapts=s_scalars.RenameScalarType,
):
    pass


class RebaseScalarType(
    ScalarTypeMetaCommand,
    adapts=s_scalars.RebaseScalarType,
):
    # Actual rebase is taken care of in AlterScalarType
    pass


class AlterScalarType(ScalarTypeMetaCommand, adapts=s_scalars.AlterScalarType):

    problematic_refs: Optional[Tuple[
        Tuple[so.Object, ...],
        Dict[s_props.Property, s_types.TypeShell],
    ]]

    def _get_problematic_refs(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        composite_only: bool,
    ) -> Optional[Tuple[
        Tuple[so.Object, ...],
        Dict[s_props.Property, s_types.TypeShell],
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
        seen_other: set[so.Object] = set()

        typ = self.scls
        typs = [typ]
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
            elif isinstance(obj, s_scalars.ScalarType) and not composite_only:
                wl.extend(schema.get_referrers(obj))
                seen_other.add(obj)
                typs.append(obj)
            elif isinstance(obj, s_types.Collection):
                wl.extend(schema.get_referrers(obj))
                seen_other.add(obj)
            elif isinstance(obj, s_funcs.Parameter) and not composite_only:
                wl.extend(schema.get_referrers(obj))
                seen_other.add(obj)
            elif isinstance(obj, s_funcs.Function) and not composite_only:
                wl.extend(schema.get_referrers(obj))
                seen_other.add(obj)
            elif isinstance(obj, s_constr.Constraint) and not composite_only:
                seen_other.add(obj)
            elif isinstance(obj, s_indexes.Index) and not composite_only:
                seen_other.add(obj)

        if not seen_props and not seen_other:
            return None

        props = {}
        if seen_props:
            type_substs: dict[sn.Name, s_types.TypeShell[s_types.Type]] = {}
            for typ in typs:
                # Find a concrete ancestor to substitute in.
                if typ.is_enum(schema):
                    ancestor = schema.get(
                        sn.QualName('std', 'str'), type=s_types.Type)
                else:
                    for ancestor in typ.get_ancestors(schema).objects(schema):
                        if not ancestor.get_abstract(schema):
                            break
                    else:
                        raise AssertionError(
                            "can't find concrete base for scalar")
                type_substs[typ.get_name(schema)] = ancestor.as_shell(schema)

            props = {
                prop:
                s_utils.type_shell_multi_substitute(
                    type_substs,
                    not_none(prop.get_target(schema)).as_shell(schema),
                    schema,
                )
                for prop in seen_props
            }

        objs = sd.sort_by_cross_refs(schema, seen_props | seen_other)

        return objs, props

    def _undo_everything(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        objs: Tuple[so.Object, ...],
        props: Dict[s_props.Property, s_types.TypeShell],
    ) -> s_schema.Schema:
        """Rewrite the type of everything that uses this scalar dangerously.

        See _get_problematic_refs above for details.
        """

        # First we need to strip out any default value that might reference
        # one of the functions we are going to delete.
        # We also create any new types, in this pass.
        cmd = sd.DeltaRoot()
        for prop, new_typ in props.items():
            try:
                cmd.add(new_typ.as_create_delta(schema))
            except errors.UnsupportedFeatureError:
                pass

            if prop.get_default(schema):
                delta_alter, cmd_alter, _alter_context = prop.init_delta_branch(
                    schema, context, cmdtype=sd.AlterObject)
                cmd_alter.set_attribute_value('default', None)
                cmd.add(delta_alter)

        cmd.apply(schema, context)
        acmd = CommandMeta.adapt(cmd)
        schema = acmd.apply(schema, context)
        self.pgops.update(acmd.get_subcommands())

        # Now process all the objects in the appropriate order
        for obj in objs:
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
                self.pgops.add(ConstraintCommand.delete_constraint(obj, schema))
            elif isinstance(obj, s_indexes.Index):
                self.pgops.add(DeleteIndex.delete_index(obj, schema, context))
            elif isinstance(obj, s_types.Tuple):
                self.pgops.add(dbops.DropCompositeType(
                    name=common.get_backend_name(schema, obj, catenate=False),
                ))
            elif isinstance(obj, s_scalars.ScalarType):
                self.pgops.add(DeleteScalarType.delete_scalar(obj, schema))
            elif isinstance(obj, s_props.Property):
                new_typ = props[obj]

                delta_alter, cmd_alter, _alter_context = obj.init_delta_branch(
                    schema, context, cmdtype=sd.AlterObject)
                cmd_alter.set_attribute_value('target', new_typ)
                cmd_alter.set_attribute_value('default', None)

                delta_alter.apply(schema, context)
                acmd2 = CommandMeta.adapt(delta_alter)
                schema = acmd2.apply(schema, context)
                self.pgops.add(acmd2)

        return schema

    def _redo_everything(
        self,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
        objs: Tuple[so.Object, ...],
        props: Dict[s_props.Property, s_types.TypeShell],
    ) -> s_schema.Schema:
        """Restore the type of everything that uses this scalar dangerously.

        See _get_problematic_refs above for details.
        """

        for obj in reversed(objs):
            if isinstance(obj, s_funcs.Function):
                # Super hackily recreate the functions
                fc = CreateFunction(
                    classname=obj.get_name(schema))  # type: ignore
                for f in ('language', 'params', 'return_type'):
                    fc.set_attribute_value(f, obj.get_field_value(schema, f))
                self.pgops.update(fc.make_op(obj, schema, context))
            elif isinstance(obj, s_constr.Constraint):
                self.pgops.add(ConstraintCommand.create_constraint(
                    self,
                    obj,
                    schema,
                    context,
                    create_triggers_if_needed=False,
                ))
            elif isinstance(obj, s_indexes.Index):
                self.pgops.add(
                    CreateIndex.create_index(obj, orig_schema, context))
            elif isinstance(obj, s_types.Tuple):
                self.pgops.add(CreateTuple.create_tuple(obj, orig_schema))
            elif isinstance(obj, s_scalars.ScalarType):
                self.pgops.add(
                    CreateScalarType.create_scalar(
                        obj, obj.get_default(schema), orig_schema, context
                    )
                )
            elif isinstance(obj, s_props.Property):
                new_typ = props[obj]

                delta_alter, cmd_alter, _ = obj.init_delta_branch(
                    schema, context, cmdtype=sd.AlterObject)
                cmd_alter.set_attribute_value(
                    'target', obj.get_target(orig_schema))

                delta_alter.apply(schema, context)
                acmd = CommandMeta.adapt(delta_alter)
                schema = acmd.apply(schema, context)
                self.pgops.add(acmd)

        # Restore defaults and prune newly created types
        cmd = sd.DeltaRoot()
        for prop, new_typ in props.items():
            rnew_typ = new_typ.resolve(schema)
            if delete := rnew_typ.as_type_delete_if_unused(schema):
                cmd.add_caused(delete)

            delta_alter, cmd_alter, _ = prop.init_delta_branch(
                schema, context, cmdtype=sd.AlterObject)
            cmd_alter.set_attribute_value(
                'default', prop.get_default(orig_schema))
            cmd.add(delta_alter)

        # do an apply of the schema-level command to force it to canonicalize,
        # which prunes out duplicate deletions
        #
        # HACK: Clear out the context's stack so that
        # context.canonical is false while doing this.
        stack, context.stack = context.stack, []
        cmd.apply(schema, context)
        context.stack = stack

        for sub in cmd.get_subcommands():
            acmd2 = CommandMeta.adapt(sub)
            schema = acmd2.apply(schema, context)
            self.pgops.add(acmd2)

        return schema

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super()._alter_begin(schema, context)
        new_scalar = self.scls

        has_create_constraint = bool(
            list(self.get_subcommands(type=s_constr.CreateConstraint)))
        has_rebase = bool(
            list(self.get_subcommands(type=s_scalars.RebaseScalarType)))

        old_enum_values: Sequence[str] = (
            new_scalar.get_enum_values(orig_schema) or [])
        new_enum_values: Sequence[str]

        if has_rebase and old_enum_values:
            # Ugly hack alert: we need to do this "lookahead" rebase
            # apply to get the list of new enum values to decide
            # whether special handling is needed _before_ the actual
            # _alter_innards() takes place, because we are also handling
            # domain constraints here.  TODO: a cleaner way to handle this
            # would be to move this logic into actual subcomands
            # (RebaseScalarType and CreateConstraint).
            rebased_schema = super()._alter_innards(schema, context)
            new_enum_values = new_scalar.get_enum_values(rebased_schema) or []
        else:
            new_enum_values = old_enum_values

        # If values were deleted or reordered, we need to drop the enum
        # and recreate it.
        needs_recreate = (
            old_enum_values != new_enum_values
            and old_enum_values != new_enum_values[:len(old_enum_values)])

        self.problematic_refs = None
        if needs_recreate or has_create_constraint:
            self.problematic_refs = self._get_problematic_refs(
                schema, context, composite_only=not needs_recreate)
            if self.problematic_refs:
                objs, props = self.problematic_refs
                schema = self._undo_everything(schema, context, objs, props)

        if new_enum_values:
            type_name = common.get_backend_name(
                schema, new_scalar, catenate=False)

            if needs_recreate:
                self.pgops.add(
                    DeleteScalarType.delete_scalar(new_scalar, orig_schema)
                )
                self.pgops.add(
                    CreateScalarType.create_enum(
                        new_scalar, new_enum_values, schema, context
                    )
                )

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

    def _alter_finalize(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_finalize(schema, context)
        if self.problematic_refs:
            objs, props = self.problematic_refs
            schema = self._redo_everything(
                schema,
                context.current().original_schema,
                context,
                objs,
                props,
            )

        return schema


def drop_dependant_func_cache(pg_type: Tuple[str, ...]) -> dbops.PLQuery:
    if len(pg_type) == 1:
        types_cte = f'''
                    SELECT
                        pt.oid AS oid
                    FROM
                        pg_type pt
                    WHERE
                        pt.typname = {ql(pg_type[0])}
                        OR pt.typname = {ql('_' + pg_type[0])}\
        '''
    else:
        types_cte = f'''
                    SELECT
                        pt.oid AS oid
                    FROM
                        pg_type pt
                        JOIN pg_namespace pn
                            ON pt.typnamespace = pn.oid
                    WHERE
                        pn.nspname = {ql(pg_type[0])}
                        AND (
                            pt.typname = {ql(pg_type[1])}
                            OR pt.typname = {ql('_' + pg_type[1])}
                        )\
        '''
    drop_func_cache_sql = textwrap.dedent(f'''
        DECLARE
            qc RECORD;
        BEGIN
            FOR qc IN
                WITH
                types AS ({types_cte}
                ),
                class AS (
                    SELECT
                        pc.oid AS oid
                    FROM
                        pg_class pc
                        JOIN pg_namespace pn
                            ON pc.relnamespace = pn.oid
                    WHERE
                        pn.nspname = 'pg_catalog'
                        AND pc.relname = 'pg_type'
                )
                SELECT
                    substring(p.proname FROM 6)::uuid AS key
                FROM
                    pg_proc p
                    JOIN pg_depend d
                        ON d.objid = p.oid
                    JOIN types t
                        ON d.refobjid = t.oid
                    JOIN class c
                        ON d.refclassid = c.oid
                WHERE
                    p.proname LIKE '__qh_%'
            LOOP
                PERFORM edgedb_VER."_evict_query_cache"(qc.key);
            END LOOP;
        END;
    ''')
    return dbops.PLQuery(drop_func_cache_sql)


class DeleteScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.DeleteScalarType):
    @classmethod
    def delete_scalar(
        cls, scalar: s_scalars.ScalarType, orig_schema: s_schema.Schema
    ) -> dbops.Command:
        ops = dbops.CommandGroup()

        # The custom scalar types are sometimes included in the function
        # signatures of query cache functions under QueryCacheMode.PgFunc.
        # We need to find such functions through pg_depend and evict the cache
        # before dropping the custom scalar type.
        pg_type = types.pg_type_from_scalar(orig_schema, scalar)
        ops.add_command(drop_dependant_func_cache(pg_type))

        old_domain_name = common.get_backend_name(
            orig_schema, scalar, catenate=False)
        cond: dbops.Condition
        if scalar.is_concrete_enum(orig_schema):
            old_enum_name = old_domain_name
            cond = dbops.EnumExists(old_enum_name)

            cast_func_name = common.get_backend_name(
                orig_schema, scalar, False, aspect="enum-cast-from-str"
            )
            cast_func = dbops.DropFunction(
                name=cast_func_name,
                args=[("value", ("anyelement",))],
                conditions=[cond],
            )
            ops.add_command(cast_func)

            uncast_func_name = common.get_backend_name(
                orig_schema, scalar, False, aspect="enum-cast-into-str"
            )
            uncast_func = dbops.DropFunction(
                name=uncast_func_name,
                args=[("value", ("anyelement",))],
                conditions=[cond],
            )
            ops.add_command(uncast_func)

            enum = dbops.DropEnum(name=old_enum_name, conditions=[cond])
            ops.add_command(enum)

        else:
            cond = dbops.DomainExists(old_domain_name)
            domain = dbops.DropDomain(name=old_domain_name, conditions=[cond])
            ops.add_command(domain)

        return ops

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super().apply(schema, context)
        scalar = self.scls

        link = None
        if context:
            link = context.get(s_links.LinkCommandContext)

        if link:
            assert isinstance(link.op, MetaCommand)
            ops = link.op.pgops
        else:
            ops = self.pgops

        ops.add(self.delete_scalar(scalar, orig_schema))

        if self.is_sequence(orig_schema, scalar):
            seq_name = common.get_backend_name(
                orig_schema, scalar, catenate=False, aspect='sequence')
            self.pgops.add(dbops.DropSequence(name=seq_name))

        return schema


if TYPE_CHECKING:
    # In pgsql/delta, a "composite object" is anything that can have a table.
    # That is, an object type, a link, or a property.
    # We represent it as Source | Pointer, since many call sites are generic
    # over one of those things.
    CompositeObject = s_sources.Source | s_pointers.Pointer

    PostCommand = (
        dbops.Command
        | Callable[
            [s_schema.Schema, sd.CommandContext],
            Optional[dbops.Command]
        ]
    )


class CompositeMetaCommand(MetaCommand):

    constraint_trigger_updates: set[uuid.UUID]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table_name = None
        self._multicommands = {}
        self.update_search_indexes = None
        self.constraint_trigger_updates = set()

    def schedule_trampoline(self, obj, schema, context):
        delta = context.get(sd.DeltaRootContext).op
        create_trampolines = delta.create_trampolines
        create_trampolines.table_targets.append(obj)

    def _get_multicommand(
        self,
        context,
        cmdtype,
        object_name,
        *,
        force_new=False,
        manual=False,
        cmdkwargs=None,
    ):
        if cmdkwargs is None:
            cmdkwargs = {}
        key = (object_name, frozenset(cmdkwargs.items()))

        try:
            typecommands = self._multicommands[cmdtype]
        except KeyError:
            typecommands = self._multicommands[cmdtype] = {}

        commands = typecommands.get(key)

        if commands is None or force_new or manual:
            command = cmdtype(object_name, **cmdkwargs)

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
                self.pgops.update(commands)

    def get_alter_table(
        self,
        schema,
        context,
        force_new=False,
        contained=False,
        manual=False,
        table_name=None,
    ):

        tabname = table_name if table_name else self.table_name

        # XXX: should this be arranged to always have been done?
        if not tabname:
            ctx = context.get(self.__class__)
            assert ctx
            tabname = self._get_table_name(ctx.scls, schema)
            if table_name is None:
                self.table_name = tabname

        return self._get_multicommand(
            context, dbops.AlterTable, tabname,
            force_new=force_new, manual=manual,
            cmdkwargs={'contained': contained})

    def attach_alter_table(self, context):
        self._attach_multicommand(context, dbops.AlterTable)

    @staticmethod
    def _get_table_name(obj, schema) -> tuple[str, str]:
        is_internal_view = irtyputils.is_cfg_view(obj, schema)
        aspect = 'dummy' if is_internal_view else None
        return common.get_backend_name(
            schema, obj, catenate=False, aspect=aspect)

    @classmethod
    def _refresh_fake_cfg_view_cmd(
        cls,
        obj: CompositeObject,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> dbops.Command:
        if not types.has_table(obj, schema):
            return dbops.CommandGroup()
        # Objects in sys and cfg are actually implemented by views
        # that are defined in metaschema. The metaschema scripts run
        # *after* the schema is instantiated, though, and we need to
        # populate something *now* that can go into inhviews.
        #
        # The way we do this is by creating an actual concrete table
        # with the suffix "_dummy" and then creating a view with the
        # expected table name that simply `select *`s from the dummy
        # table. Pointer creation on the type gets routed to the dummy
        # table, so it has the right columns. Since the view `select
        # *`s from the table, it also has the right columns, and can
        # go into all of the inheritance views without any trouble.
        #
        # We refresh the fake config view before creating/updating
        # inhviews associated with the object, since that corresponds
        # with when it actually needs to happen by.
        #
        # Then, when we run the metaschema script, it simply swaps out
        # this hacky view for the real one and everything works out fine.
        orig_name = common.get_backend_name(
            schema, obj, catenate=False,
        )
        dummy_name = cls._get_table_name(obj, schema)
        query = f'''
            SELECT * FROM {q(*dummy_name)}
        '''
        view = dbops.View(name=orig_name, query=query)
        return dbops.CreateView(view, or_replace=True)

    def update_if_cfg_view(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        obj: CompositeObject,
    ):
        if irtyputils.is_cfg_view(obj, schema) and not context.in_deletion():
            self.pgops.add(
                self._refresh_fake_cfg_view_cmd(obj, schema, context))

    def update_source_if_cfg_view(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        ptr: s_pointers.Pointer,
    ) -> None:
        if src := ptr.get_source(schema):
            assert isinstance(src, s_sources.Source)
            self.update_if_cfg_view(schema, context, src)

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

    @classmethod
    def create_type_trampoline(
        cls,
        schema: s_schema.Schema,
        obj: CompositeObject,
        aspect: str='table',
    ) -> Optional[trampoline.TrampolineView]:
        versioned_name = common.get_backend_name(
            schema, obj, aspect=aspect, catenate=False
        )
        trampolined_name = common.get_backend_name(
            schema, obj, aspect=aspect, catenate=False, versioned=False
        )
        if versioned_name != trampolined_name:
            return trampoline.make_table_trampoline(versioned_name)
        else:
            return None

    def apply_constraint_trigger_updates(
        self,
        schema: s_schema.Schema,
    ) -> None:
        for constraint_id in self.constraint_trigger_updates:
            constraint = (
                schema.get_by_id(constraint_id, type=s_constr.Constraint)
                if schema.has_object(constraint_id) else
                None
            )
            if not constraint:
                continue

            if not ConstraintCommand.constraint_is_effective(
                schema, constraint
            ):
                continue

            subject = constraint.get_subject(schema)
            bconstr = schemamech.compile_constraint(
                subject, constraint, schema, None
            )

            self.pgops.add(bconstr.update_trigger_ops())


class IndexCommand(MetaCommand):
    pass


def get_index_compile_options(
    index: s_indexes.Index,
    schema: s_schema.Schema,
    modaliases: Mapping[Optional[str], str],
    schema_object_context: Optional[Type[so.Object_T]],
) -> qlcompiler.CompilerOptions:
    subject = index.get_subject(schema)
    assert isinstance(subject, (s_types.Type, s_pointers.Pointer))

    return qlcompiler.CompilerOptions(
        modaliases=modaliases,
        schema_object_context=schema_object_context,
        anchors={'__subject__': subject},
        path_prefix_anchor='__subject__',
        singletons=[subject],
        apply_query_rewrites=False,
    )


def get_reindex_sql(
    obj: s_objtypes.ObjectType,
    restore_desc: sertypes.ShapeDesc,
    schema: s_schema.Schema,
) -> Optional[str]:
    """Generate SQL statement that repopulates the index after a restore.

    Currently this only applies to FTS indexes, and it only fires if
    __fts_document__ is not in the dump (which it wasn't prior to 5.0).

    AI index columns might also be missing if they were made with a
    5.0rc1 dump, but the indexer will pick them up without our
    intervention.
    """

    (fts_index, _) = s_indexes.get_effective_object_index(
        schema, obj, sn.QualName("std::fts", "index")
    )
    if fts_index and '__fts_document__' not in restore_desc.fields:
        options = get_index_compile_options(fts_index, schema, {}, None)
        cmd = deltafts.update_fts_document(fts_index, options, schema)
        return cmd.code()

    return None


class CreateIndex(IndexCommand, adapts=s_indexes.CreateIndex):
    @classmethod
    def create_index(
        cls,
        index: s_indexes.Index,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ):
        from .compiler import astutils

        options = get_index_compile_options(
            index, schema, context.modaliases, cls.get_schema_metaclass()
        )

        index_sexpr: Optional[s_expr.Expression] = index.get_expr(schema)
        assert index_sexpr
        index_expr = index_sexpr.ensure_compiled(
            schema=schema,
            options=options,
            context=None,
        )
        ir = index_expr.irast

        except_expr = index.get_except_expr(schema)
        if except_expr:
            except_expr = except_expr.ensure_compiled(
                schema=schema,
                options=options,
                context=None,
            )
            assert except_expr.irast
            except_res = compiler.compile_ir_to_sql_tree(
                except_expr.irast.expr, singleton_mode=True)
            except_src = codegen.generate_source(except_res.ast)
            predicate_src = f'({except_src}) is not true'
        else:
            predicate_src = None

        sql_kwarg_exprs = dict()
        # Get the name of the root index that this index implements
        orig_name: sn.Name = sn.shortname_from_fullname(index.get_name(schema))
        root_name: sn.Name
        root_code: str | None
        if orig_name == s_indexes.DEFAULT_INDEX:
            root_name = orig_name
            root_code = DEFAULT_INDEX_CODE
        else:
            root = index.get_root(schema)
            root_name = root.get_name(schema)
            root_code = root.get_code(schema)

            kwargs = index.get_concrete_kwargs(schema)
            for name, expr in kwargs.items():
                kw_ir = expr.assert_compiled().irast
                kw_sql_res = compiler.compile_ir_to_sql_tree(
                    kw_ir.expr, singleton_mode=True)
                kw_sql_tree = kw_sql_res.ast
                # HACK: the compiled SQL is expected to have some unnecessary
                # casts, strip them as they mess with the requirement that
                # index expressions are IMMUTABLE (also indexes expect the
                # usage of literals and will do their own implicit casts).
                if isinstance(kw_sql_tree, pgast.TypeCast):
                    kw_sql_tree = kw_sql_tree.arg
                sql = codegen.generate_source(kw_sql_tree)
                sql_kwarg_exprs[name] = sql

        # FTS
        if root_name == sn.QualName('std::fts', 'index'):
            return deltafts.create_fts_index(
                index,
                ir.expr,
                predicate_src,
                sql_kwarg_exprs,
                options,
                schema,
                context,
            )
        elif root_name == sn.QualName('ext::ai', 'index'):
            return delta_ext_ai.create_ext_ai_index(
                index,
                predicate_src,
                sql_kwarg_exprs,
                options,
                schema,
                context,
            )

        if root_code is None:
            raise AssertionError(f'index {root_name} is missing the code')

        sql_res = compiler.compile_ir_to_sql_tree(ir.expr, singleton_mode=True)
        exprs = astutils.maybe_unpack_row(sql_res.ast)

        if len(exprs) == 0:
            raise errors.SchemaDefinitionError(
                f'cannot index empty tuples using {root_name}'
            )

        subject = index.get_subject(schema)
        assert subject
        table_name = common.get_backend_name(schema, subject, catenate=False)

        module_name = index.get_name(schema).module
        index_name = common.get_index_backend_name(
            index.id, module_name, catenate=False)

        sql_exprs = [codegen.generate_source(e) for e in exprs]
        pg_index = dbops.Index(
            name=index_name[1],
            table_name=table_name,  # type: ignore
            exprs=sql_exprs,
            unique=False,
            inherit=True,
            predicate=predicate_src,
            metadata={
                'schemaname': str(index.get_name(schema)),
                'code': root_code,
                'kwargs': sql_kwarg_exprs,
            }
        )
        return dbops.CreateIndex(pg_index)

    def _create_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._create_innards(schema, context)
        index = self.scls

        if index.get_abstract(schema):
            # Don't do anything for abstract indexes
            return schema

        with errors.ensure_span(self.span):
            self.pgops.add(self.create_index(index, schema, context))

        return schema


# mypy claims that _cmd_from_ast in IndexCommand is incompatible with
# that in RenameObject.
class RenameIndex(IndexCommand, adapts=s_indexes.RenameIndex):  # type: ignore
    pass


class AlterIndexOwned(IndexCommand, adapts=s_indexes.AlterIndexOwned):
    pass


class AlterIndex(IndexCommand, adapts=s_indexes.AlterIndex):
    pass


class DeleteIndex(IndexCommand, adapts=s_indexes.DeleteIndex):
    @classmethod
    def delete_index(
        cls,
        index: s_indexes.Index,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ):
        subject = index.get_subject(schema)
        assert subject
        table_name = common.get_backend_name(
            schema, subject, catenate=False)
        module_name = index.get_name(schema).module
        orig_idx_name = common.get_index_backend_name(
            index.id, module_name, catenate=False)
        pg_index = dbops.Index(
            name=orig_idx_name[1], table_name=table_name, inherit=True)
        index_exists = dbops.IndexExists(
            (table_name[0], pg_index.name_in_catalog)
        )
        return dbops.DropIndex(pg_index, conditions=(index_exists,))

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super().apply(schema, context)
        index = self.scls

        if index.get_abstract(orig_schema):
            # Don't do anything for abstract indexes
            return schema

        source: Optional[
            sd.CommandContextToken[s_sources.SourceCommand[s_sources.Source]]]
        # XXX: I think to make these work, the type vars in the Commands
        # would need to be covariant.
        source = context.get(s_links.LinkCommandContext)  # type: ignore
        if not source:
            source = context.get(
                s_objtypes.ObjectTypeCommandContext)   # type: ignore
            assert source

        if not isinstance(source.op, sd.DeleteObject):
            # We should not drop indexes when the host is being dropped since
            # the indexes are dropped automatically in this case.
            drop_index = self.delete_index(index, orig_schema, context)
        else:
            drop_index = dbops.NoOpCommand()

        # FTS
        if s_indexes.is_fts_index(orig_schema, index):
            # compile commands for index drop
            options = get_index_compile_options(
                index,
                orig_schema,
                context.modaliases,
                self.get_schema_metaclass()
            )
            self.pgops.add(deltafts.delete_fts_index(
                index, drop_index, options, schema, orig_schema, context
            ))

        # ext::ai::index
        elif s_indexes.is_ext_ai_index(orig_schema, index):
            # compile commands for index drop
            options = get_index_compile_options(
                index,
                orig_schema,
                context.modaliases,
                self.get_schema_metaclass()
            )
            drop_support_ops, drop_col_ops = delta_ext_ai.delete_ext_ai_index(
                index, drop_index, options, schema, orig_schema, context
            )

            # Even though the object type table is getting dropped, we have
            # to drop the trigger and its function
            self.pgops.add(drop_support_ops)
            self.pgops.add(drop_col_ops)
        else:
            self.pgops.add(drop_index)

        return schema


class RebaseIndex(IndexCommand, adapts=s_indexes.RebaseIndex):
    pass


class IndexMatchCommand(MetaCommand):
    pass


class CreateIndexMatch(IndexMatchCommand, adapts=s_indexes.CreateIndexMatch):
    # Index match is handled by the compiler and does not need explicit
    # representation in the backend.
    pass


class DeleteIndexMatch(IndexMatchCommand, adapts=s_indexes.DeleteIndexMatch):
    pass


class CreateUnionType(
    MetaCommand,
    adapts=s_types.CreateUnionType,
    metaclass=CommandMeta,
):
    pass


class ObjectTypeMetaCommand(AliasCapableMetaCommand, CompositeMetaCommand):
    def schedule_endpoint_delete_action_update(self, obj, schema, context):
        endpoint_delete_actions = context.get(
            sd.DeltaRootContext).op.update_endpoint_delete_actions
        changed_targets = endpoint_delete_actions.changed_targets
        changed_targets.add((self, obj))

    def _fixup_configs(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        orig_schema = context.current().original_schema
        eff_schema = (
            orig_schema if isinstance(self, sd.DeleteObject) else schema)
        scls: s_objtypes.ObjectType = self.scls  # type: ignore

        # If we are updating a config object that is *not* in cfg::
        # (that is, an extension config), we need to update the config
        # views and specs. We *don't* do that for standard library
        # configs, since those need to be created after the standard
        # schema is in place.
        if not (
            irtyputils.is_cfg_view(scls, eff_schema)
            and scls.get_name(eff_schema).module not in irtyputils.VIEW_MODULES
        ):
            return

        from edb.pgsql import metaschema

        new_local_spec = config.load_spec_from_schema(
            schema,
            only_exts=True,
            # suppress validation because we might be in an intermediate state
            validate=False,
        )
        spec_json = config.spec_to_json(new_local_spec)
        self.pgops.add(dbops.Query(textwrap.dedent(trampoline.fixup_query(f'''\
            UPDATE
                edgedbinstdata_VER.instdata
            SET
                json = {ql(spec_json)}
            WHERE
                key = 'configspec_ext';
        '''))))

        for sub in self.get_subcommands(type=s_pointers.DeletePointer):
            if types.has_table(sub.scls, orig_schema):
                self.pgops.add(dbops.DropView(common.get_backend_name(
                    orig_schema, sub.scls, catenate=False)))

        if isinstance(self, sd.DeleteObject):
            self.pgops.add(dbops.DropView(common.get_backend_name(
                eff_schema, scls, catenate=False)))
        elif isinstance(self, sd.CreateObject):
            views = metaschema.get_config_type_views(
                eff_schema, scls, scope=None)
            self.pgops.update(views)
        # FIXME: ALTER doesn't work in meaningful ways. We'll maybe
        # need to fix that when we have patching configs.


class CreateObjectType(
    ObjectTypeMetaCommand, adapts=s_objtypes.CreateObjectType
):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)

        objtype = self.scls
        if objtype.is_compound_type(schema) or objtype.get_is_derived(schema):
            return schema

        self.attach_alter_table(context)

        if self.update_search_indexes:
            schema = self.update_search_indexes.apply(schema, context)
            self.pgops.add(self.update_search_indexes)

        self.schedule_endpoint_delete_action_update(self.scls, schema, context)
        self.schedule_trampoline(self.scls, schema, context)

        self._fixup_configs(schema, context)

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

        new_table_name = self._get_table_name(self.scls, schema)

        self.table_name = new_table_name
        columns: list[dbops.Column] = []

        objtype_table = dbops.Table(name=new_table_name, columns=columns)
        self.pgops.add(dbops.CreateTable(table=objtype_table))
        self.pgops.add(dbops.Comment(
            object=objtype_table,
            text=str(objtype.get_verbosename(schema)),
        ))
        # Don't update ancestors yet: no pointers have been added to
        # the type yet, so this type won't actually be added to any
        # ancestor views. We'll fix up the ancestors in
        # _create_finalize.
        self.update_if_cfg_view(schema, context, objtype)
        return schema

    def _create_finalize(self, schema, context):
        schema = super()._create_finalize(schema, context)
        self.apply_constraint_trigger_updates(schema)
        return schema


class RenameObjectType(
    ObjectTypeMetaCommand,
    adapts=s_objtypes.RenameObjectType,
):
    pass


class RebaseObjectType(
    ObjectTypeMetaCommand, adapts=s_objtypes.RebaseObjectType
):
    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if types.has_table(self.scls, schema):
            self.update_if_cfg_view(schema, context, self.scls)

        schema = super()._alter_innards(schema, context)
        self.schedule_endpoint_delete_action_update(self.scls, schema, context)

        return schema


class AlterObjectType(ObjectTypeMetaCommand, adapts=s_objtypes.AlterObjectType):
    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_begin(schema, context)
        # We want to set this name up early, so children operations see it
        self.table_name = self._get_table_name(self.scls, schema)
        return schema

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super().apply(schema, context=context)
        objtype = self.scls

        self.apply_constraint_trigger_updates(schema)

        self._maybe_do_abstract_test(orig_schema, schema, context)

        if types.has_table(objtype, schema):
            self.attach_alter_table(context)

            if self.update_search_indexes:
                schema = self.update_search_indexes.apply(schema, context)
                self.pgops.add(self.update_search_indexes)

        self._fixup_configs(schema, context)

        return schema

    def _maybe_do_abstract_test(
        self,
        orig_schema: s_schema.Schema,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        orig_abstract = self.scls.get_abstract(orig_schema)
        new_abstract = self.scls.get_abstract(schema)
        if orig_abstract or not new_abstract:
            return

        table = q(*common.get_backend_name(
            schema,
            self.scls,
            catenate=False,
        ))

        vn = self.scls.get_verbosename(schema)
        check_qry = textwrap.dedent(f'''\
            SELECT
                edgedb_VER.raise(
                    NULL::text,
                    'cardinality_violation',
                    msg => {common.quote_literal(
                            f"may not make non-empty {vn} abstract")},
                    "constraint" => 'set abstract'
                )
            FROM {table}
            INTO _dummy_text;
        ''')

        self.pgops.add(dbops.Query(check_qry))


class DeleteObjectType(
    ObjectTypeMetaCommand, adapts=s_objtypes.DeleteObjectType
):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        self.scls = objtype = schema.get(
            self.classname, type=s_objtypes.ObjectType)

        old_table_name = self._get_table_name(self.scls, schema)

        orig_schema = schema
        schema = super().apply(schema, context)

        self.apply_constraint_trigger_updates(schema)

        if types.has_table(objtype, orig_schema):
            self.attach_alter_table(context)
            self.pgops.add(dbops.DropTable(name=old_table_name))

        self._fixup_configs(schema, context)

        return schema


class SchedulePointerCardinalityUpdate(MetaCommand):
    pass


class CancelPointerCardinalityUpdate(MetaCommand):
    pass


class PointerMetaCommand(
    CompositeMetaCommand,
    s_pointers.PointerCommand[s_pointers.Pointer_T],
):
    def get_host(self, schema, context):
        if context:
            link = context.get(s_links.LinkCommandContext)
            if link and isinstance(self, s_props.PropertyCommand):
                return link
            objtype = context.get(s_objtypes.ObjectTypeCommandContext)
            if objtype:
                return objtype

    def is_sequence_ptr(self, ptr, schema):
        return bool(
            (tgt := ptr.get_target(schema))
            and tgt.issubclass(schema, schema.get('std::sequence'))
        )

    def get_pointer_default(self, ptr, schema, context):
        if ptr.is_pure_computable(schema):
            return None

        # Skip id, because it shouldn't ever matter for performance
        # and because it wants to use the trampoline function, which
        # might not exist yet.
        if ptr.is_id_pointer(schema):
            return None
        if context.stdmode:
            return None

        # We only *need* to use postgres defaults for link properties
        # and sequence values (since we always explicitly inject it in
        # INSERTs anyway), but we *want* to use it whenever we can,
        # since it is much faster than explicitly populating the
        # column.
        default = ptr.get_default(schema)
        default_value = None

        if default is not None:
            default_value = schemamech.ptr_default_to_col_default(
                schema, ptr, default)
        elif self.is_sequence_ptr(ptr, schema):
            # TODO: replace this with a generic scalar type default
            #       using std::nextval().
            seq_name = common.quote_literal(
                common.get_backend_name(
                    schema, ptr.get_target(schema), aspect='sequence'))
            default_value = f'nextval({seq_name}::regclass)'

        return default_value

    @classmethod
    def get_columns(
        cls, pointer, schema, default=None, sets_required=False
    ) -> List[dbops.Column]:
        ptr_stor_info = types.get_pointer_storage_info(pointer, schema=schema)
        col_type = common.quote_type(tuple(ptr_stor_info.column_type))

        return [
            dbops.Column(
                name=ptr_stor_info.column_name,
                type=col_type,
                required=(
                    (
                        pointer.get_required(schema)
                        and not pointer.is_pure_computable(schema)
                        and not sets_required
                        and not (pointer.get_default(schema) and not default)
                    ) or (
                        ptr_stor_info.table_type == 'link'
                        and not pointer.is_link_property(schema)
                    )
                ),
                default=default,
                comment=str(pointer.get_shortname(schema)),
            ),
        ]

    def create_table(self, ptr, schema, context):
        if types.has_table(ptr, schema):
            c = self._create_table(ptr, schema, context, conditional=True)
            self.pgops.add(c)
            self.update_if_cfg_view(schema, context, ptr)
            return True
        else:
            return False

    def _alter_pointer_cardinality(
        self,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        assert isinstance(self, s_pointers.AlterPointerUpperCardinality)

        ptr = self.scls
        ptr_stor_info = types.get_pointer_storage_info(ptr, schema=schema)
        old_ptr_stor_info = types.get_pointer_storage_info(
            ptr, schema=orig_schema)
        ptr_table = ptr_stor_info.table_type == 'link'
        is_lprop = ptr.is_link_property(schema)
        is_multi = ptr_table and not is_lprop
        is_required = ptr.get_required(schema)
        is_scalar = ptr.is_property(schema)

        ref_ctx = self.get_referrer_context_or_die(context)
        ref_op = ref_ctx.op

        source_op: sd.ObjectCommand
        if is_multi:
            if isinstance(self, sd.AlterObjectFragment):
                source_op = self.get_parent_op(context)
            else:
                source_op = self
        else:
            source_op = ref_op
        assert isinstance(source_op, CompositeMetaCommand)

        # Ignore cardinality changes resulting from the creation of
        # an overloaded pointer as there is no data yet.
        if isinstance(source_op, sd.CreateObject):
            return

        # If the pointer has any constraints, drop them now. We'll
        # create them again at the end.
        # N.B: Since the pointer is either starting or ending as multi,
        # it can't have any object constraints referencing it.
        # TODO?: Maybe we should handle the constraint by generating
        # an alter in the front-end and running _alter_innards in the
        # middle of this function. (After creations, before deletions.)
        for constr in ptr.get_constraints(schema).objects(schema):
            self.pgops.add(ConstraintCommand.delete_constraint(
                constr, orig_schema
            ))

        assert ptr_stor_info.table_name
        tab = q(*ptr_stor_info.table_name)
        target_col = ptr_stor_info.column_name
        source = ptr.get_source(orig_schema)
        assert source
        src_tab = q(*common.get_backend_name(
            orig_schema,
            source,
            catenate=False,
        ))

        # initial extern relvar (see docs of _compile_conversion_expr)
        source_rel = textwrap.dedent(f'''\
            SELECT * FROM {src_tab}
        ''')
        source_rel_alias = f'source_{uuidgen.uuid1mc()}'

        if self.conv_expr is not None:
            (conv_expr_ctes, _, _) = self._compile_conversion_expr(
                ptr,
                self.conv_expr,
                source_rel_alias,
                schema=schema,
                orig_schema=orig_schema,
                context=context,
                target_as_singleton=False,
                check_non_null=is_required and not is_multi
            )
        else:
            if not is_multi:
                raise AssertionError(
                    'explicit conversion expression was expected'
                    ' for multi->single transition'
                )

            # single -> multi
            conv_expr_ctes = textwrap.dedent(f'''\
                _conv_rel(val, id) AS (
                    SELECT {qi(old_ptr_stor_info.column_name)}, id
                    FROM {qi(source_rel_alias)}
                )
            ''')

        if not is_multi:
            # Moving from pointer table to source table.
            cols = self.get_columns(ptr, schema)

            # create columns
            alter_table = source_op.get_alter_table(
                schema, context, manual=True)
            cols_required: List[dbops.Column] = []
            for col in cols:
                cond = dbops.ColumnExists(
                    ptr_stor_info.table_name,
                    column_name=col.name,
                )
                if col.required:
                    cols_required.append(copy(col))

                col.required = False
                op = (dbops.AlterTableAddColumn(col), None, (cond, ))
                alter_table.add_operation(op)

            self.pgops.add(alter_table)

            update_qry = textwrap.dedent(f'''\
                WITH
                "{source_rel_alias}" AS ({source_rel}),
                {conv_expr_ctes}
                UPDATE {tab} AS _update
                SET {qi(target_col)} = _conv_rel.val
                FROM _conv_rel WHERE _update.id = _conv_rel.id
            ''')
            self.pgops.add(dbops.Query(update_qry))

            # set NOT NULL
            if cols_required:
                alter_table = source_op.get_alter_table(
                    schema, context, manual=True
                )
                for col in cols_required:
                    op2 = dbops.AlterTableAlterColumnNull(col.name, False)
                    alter_table.add_operation(op2)
                self.pgops.add(alter_table)

            # A link might still own a table if it has properties.
            if not types.has_table(ptr, schema):
                otabname = common.get_backend_name(
                    orig_schema, ptr, catenate=False)
                condition = dbops.TableExists(name=otabname)
                dt = dbops.DropTable(name=otabname, conditions=[condition])
                self.pgops.add(dt)

            self.update_source_if_cfg_view(
                schema, context, ptr,
            )
        else:
            # Moving from source table to pointer table.
            self.create_table(ptr, schema, context)
            source = ptr.get_source(orig_schema)
            assert source
            src_tab = q(*common.get_backend_name(
                orig_schema,
                source,
                catenate=False,
            ))

            update_qry = textwrap.dedent(f'''\
                WITH
                "{source_rel_alias}" AS ({source_rel}),
                {conv_expr_ctes}
                INSERT INTO {tab} (source, target)
                (SELECT id, val FROM _conv_rel WHERE _conv_rel.val IS NOT NULL)
            ''')

            if not is_scalar:
                update_qry += 'ON CONFLICT (source, target) DO NOTHING'

            self.pgops.add(dbops.Query(update_qry))

            assert isinstance(ref_op.scls, s_sources.Source)
            self.update_if_cfg_view(schema, context, ref_op.scls)

            ref_op = self.get_referrer_context_or_die(context).op
            assert isinstance(ref_op, CompositeMetaCommand)
            alter_table = ref_op.get_alter_table(
                schema, context, manual=True)
            col = dbops.Column(
                name=old_ptr_stor_info.column_name,
                type=common.qname(*old_ptr_stor_info.column_type),
            )
            alter_table.add_operation(dbops.AlterTableDropColumn(col))
            self.pgops.add(alter_table)

        for constr in ptr.get_constraints(schema).objects(schema):
            self.pgops.add(ConstraintCommand.create_constraint(
                self, constr, schema, context
            ))

    def _alter_pointer_optionality(
        self,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        fill_expr: Optional[s_expr.Expression],
        is_default: bool=False,
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
        assert isinstance(source_op, CompositeMetaCommand)

        alter_table = None
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

        # Ignore optionality changes resulting from the creation of
        # an overloaded pointer as there is no data yet.
        if isinstance(source_op, sd.CreateObject):
            if alter_table:
                self.pgops.add(alter_table)
            return

        ops = dbops.CommandGroup()

        # For multi pointers, if there is no fill expression, we
        # synthesize a bogus one so that an error will trip if there
        # are any objects with empty values.
        if fill_expr is None and is_multi and is_required:
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

            assert ptr_stor_info.table_name
            tab = q(*ptr_stor_info.table_name)
            target_col = ptr_stor_info.column_name
            source = ptr.get_source(orig_schema)
            assert source
            src_tab = q(*common.get_backend_name(
                orig_schema,
                source,
                catenate=False,
            ))

            if not is_multi:
                # For singleton pointers we simply update the
                # requisite column of the host source in every
                # row where it is NULL.
                source_rel = textwrap.dedent(f'''\
                    SELECT * FROM {tab}
                    WHERE {qi(target_col)} IS NULL
                ''')
            else:
                # For multi pointers we have to INSERT the
                # result of USING into the link table for
                # every source object that has _no entries_
                # in said link table.
                source_rel = textwrap.dedent(f'''\
                    SELECT * FROM {src_tab}
                    WHERE id NOT IN (SELECT source FROM {tab})
                ''')

            source_rel_alias = f'source_{uuidgen.uuid1mc()}'

            (conv_expr_ctes, _, _) = self._compile_conversion_expr(
                ptr,
                fill_expr,
                source_rel_alias,
                schema=schema,
                orig_schema=orig_schema,
                context=context,
                check_non_null=is_required and not is_multi,
                allow_globals=is_default,
            )

            if not is_multi:
                update_qry = textwrap.dedent(f'''\
                    WITH
                    "{source_rel_alias}" AS ({source_rel}),
                    {conv_expr_ctes}
                    UPDATE {tab} AS _update
                    SET {qi(target_col)} = _conv_rel.val
                    FROM _conv_rel WHERE _update.id = _conv_rel.id
                ''')
                ops.add_command(dbops.Query(update_qry))
            else:
                update_qry = textwrap.dedent(f'''\
                    WITH
                    "{source_rel_alias}" AS ({source_rel}),
                    {conv_expr_ctes}
                    INSERT INTO {tab} (source, target)
                    (SELECT id, val FROM _conv_rel WHERE val IS NOT NULL)
                ''')

                ops.add_command(dbops.Query(update_qry))

                if is_required:
                    check_qry = textwrap.dedent(f'''\
                        SELECT
                            edgedb_VER.raise(
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

        if alter_table:
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
            pointer, scls_type=s_constr.Constraint
        ):
            self.pgops.add(ConstraintCommand.delete_constraint(cnstr, schema))

    def _recreate_constraints(self, pointer, schema, context):
        for cnstr in schema.get_referrers(
            pointer, scls_type=s_constr.Constraint
        ):
            self.pgops.add(ConstraintCommand.create_constraint(
                self, cnstr, schema, context,
            ))

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
        ptr_op = self.get_parent_op(context)
        if is_multi:
            source_op = ptr_op
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
        cast_expr = self.cast_expr

        # For links, when the new type is a supertype of the old, no
        # SQL-level changes are necessary, unless an explicit conversion
        # expression was specified.
        if (
            is_link
            and cast_expr is None
            and orig_target.issubclass(orig_schema, new_target)
        ):
            return

        # We actually have work to do, so drop any constraints we have
        self._drop_constraints(pointer, schema, context)

        if cast_expr is None and not is_link:
            # A lack of an explicit EdgeQL conversion expression means
            # that the new type is assignment-castable from the old type
            # in the EdgeDB schema.  BUT, it would not necessarily be
            # assignment-castable in Postgres, especially if the types are
            # compound.  Thus, generate an explicit cast expression.
            pname = pointer.get_shortname(schema).name
            cast_expr = s_expr.Expression.from_ast(
                ql_ast.TypeCast(
                    expr=ql_ast.Path(
                        partial=True,
                        steps=[
                            ql_ast.Ptr(
                                name=pname,
                                type='property' if is_lprop else None,
                            ),
                        ],
                    ),
                    type=s_utils.typeref_to_ast(schema, new_target),
                ),
                schema=orig_schema,
            )

        tab = q(*old_ptr_stor_info.table_name)
        target_col = old_ptr_stor_info.column_name
        aux_ptr_table = None
        aux_ptr_col = None

        source_rel_alias = f'source_{uuidgen.uuid1mc()}'

        # There are two major possibilities about the USING claus:
        # 1) trivial case, where the USING clause refers only to the
        # columns of the source table, in which case we simply compile that
        # into an equivalent SQL USING clause, and 2) complex case, which
        # supports arbitrary queries, but requires a temporary column,
        # which is populated with the transition query and then used as the
        # source for the SQL USING clause.
        (cast_expr_ctes, cast_expr_sql, expr_is_nullable) = (
            self._compile_conversion_expr(
                pointer,
                cast_expr,
                source_rel_alias,
                schema=schema,
                orig_schema=orig_schema,
                context=context,
                check_non_null=is_required and not is_multi,
                produce_ctes=False,
            )
        )
        assert cast_expr_sql is not None
        need_temp_col = (
            (is_multi and expr_is_nullable) or changing_col_type
        )

        if is_link:
            old_lb_ptr_stor_info = types.get_pointer_storage_info(
                pointer, link_bias=True, schema=orig_schema)

            if (
                old_lb_ptr_stor_info is not None
                and old_lb_ptr_stor_info.table_type == 'link'
            ):
                aux_ptr_table = old_lb_ptr_stor_info.table_name
                aux_ptr_col = old_lb_ptr_stor_info.column_name

        if need_temp_col:
            alter_table = source_op.get_alter_table(
                schema, context, force_new=True, manual=True)
            temp_column = dbops.Column(
                name=f'??{pointer.id}_{common.get_unique_random_name()}',
                type=qt(new_type),
            )
            alter_table.add_operation(
                dbops.AlterTableAddColumn(temp_column))
            self.pgops.add(alter_table)
            target_col = temp_column.name

        update_with = f'WITH {cast_expr_ctes}' if cast_expr_ctes else ''
        update_qry = f'''
            {update_with}
            UPDATE {tab} AS {qi(source_rel_alias)}
            SET {qi(target_col)} = ({cast_expr_sql})
        '''
        self.pgops.add(dbops.Query(update_qry))
        trivial_cast_expr = qi(target_col)

        if changing_col_type or need_temp_col:
            alter_table = source_op.get_alter_table(
                schema, context, force_new=True, manual=True)

        if is_multi:
            # Remove all rows where the conversion expression produced NULLs.
            col = qi(target_col)
            if pointer.get_required(schema):
                clean_nulls = dbops.Query(textwrap.dedent(f'''\
                    WITH d AS (
                        DELETE FROM {tab} WHERE {col} IS NULL RETURNING source
                    )
                    SELECT
                        edgedb_VER.raise(
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
            # In case the column has a default, clear it out before
            # changing the type
            alter_table.add_operation(
                dbops.AlterTableAlterColumnDefault(
                    column_name=old_ptr_stor_info.column_name,
                    default=None))

            alter_type = dbops.AlterTableAlterColumnType(
                old_ptr_stor_info.column_name,
                common.quote_type(new_type),
                cast_expr=trivial_cast_expr,
            )

            alter_table.add_operation(alter_type)
        elif need_temp_col:
            move_data = dbops.Query(textwrap.dedent(f'''\
                UPDATE {q(*old_ptr_stor_info.table_name)}
                SET {qi(old_ptr_stor_info.column_name)} = ({qi(target_col)})
            '''))
            self.pgops.add(move_data)

        if need_temp_col:
            alter_table.add_operation(dbops.AlterTableDropColumn(temp_column))

        if changing_col_type or need_temp_col:
            self.pgops.add(alter_table)

        self._recreate_constraints(pointer, schema, context)

        if changing_col_type:
            self.update_if_cfg_view(schema, context, source)

    def _compile_conversion_expr(
        self,
        pointer: s_pointers.Pointer,
        conv_expr: s_expr.Expression,
        source_alias: str,
        *,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
        target_as_singleton: bool = True,
        check_non_null: bool = False,
        produce_ctes: bool = True,
        allow_globals: bool=False,
    ) -> Tuple[
        str,  # CTE SQL
        Optional[str],  # Query SQL
        bool,  # is_nullable
    ]:
        """
        Compile USING expression of an ALTER statement.

        producing_ctes contract:
        - Must be provided with alias of "source" rel - the relation that
          contains a row for each of the evaluations for the USING expression.
        - Source rel var must contain all columns of the __subject__
          ObjectType.
        - Result is SQL string that contains CTEs, last of which has following
          signature: _conv_rel (id, val)

        not producing_ctes contract:
        - Alias of the source must refer to a relation var, not a relation.
        - Result is SQL string that contain a single SELECT statement that
          has a single value column.
        """
        old_ptr_stor_info = types.get_pointer_storage_info(
            pointer, schema=orig_schema)
        ptr_table = old_ptr_stor_info.table_type == 'link'
        is_link = isinstance(pointer, s_links.Link)
        is_lprop = pointer.is_link_property(schema)

        new_target = not_none(pointer.get_target(schema))

        if conv_expr.irast is None:
            conv_expr = self._compile_expr(
                orig_schema,
                context,
                conv_expr,
                target_as_singleton=target_as_singleton,
                make_globals_empty=allow_globals,
                no_query_rewrites=True,
            )
        ir = conv_expr.irast
        assert ir

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
                        expr=conv_expr.parse(),
                        type=s_utils.typeref_to_ast(schema, new_target),
                    ),
                    schema=orig_schema,
                ),
                target_as_singleton=target_as_singleton,
                make_globals_empty=allow_globals,
                no_query_rewrites=True,
            )

            ir = conv_expr.irast

        if params := irutils.get_parameters(ir):
            param = list(params)[0]
            if param.is_global:
                if param.is_implicit_global:
                    problem = 'functions that reference globals'
                else:
                    problem = 'globals'
            else:
                problem = 'parameters'
            raise errors.UnsupportedFeatureError(
                f'{problem} may not be used when converting/populating '
                f'data in migrations',
                span=self.span,
            )

        # Non-trivial conversion expression means that we
        # are compiling a full-blown EdgeQL statement as
        # opposed to compiling a scalar fragment in trivial
        # expression mode.

        if is_lprop:
            # For linkprops we actually want the source path.
            # To make it work for abstract links, get the source
            # path out of the IR's output (to take advantage
            # of the types we made up for it).
            # FIXME: Maybe we shouldn't be compiling stuff
            # for abstract links!
            tgt_path_id = ir.singletons[0]
        else:
            tgt_path_id = irpathid.PathId.from_pointer(
                orig_schema, pointer, env=None
            )

        refs = irutils.get_longest_paths(ir.expr)
        ref_tables = schemamech.get_ref_storage_info(ir.schema, refs)
        local_table_only = all(
            t == old_ptr_stor_info.table_name
            for t in ref_tables
        )

        ptr_path_id = tgt_path_id.ptr_path()
        src_path_id = ptr_path_id.src_path()
        assert src_path_id

        external_rels = {}
        external_rvars = {}

        if produce_ctes:
            external_rels[src_path_id] = compiler.new_external_rel(
                rel_name=(source_alias,),
                path_id=src_path_id,
            ), (pgce.PathAspect.VALUE, pgce.PathAspect.SOURCE)
        else:
            if ptr_table:
                rvar = compiler.new_external_rvar(
                    rel_name=(source_alias,),
                    path_id=ptr_path_id,
                    outputs={
                        (src_path_id, (pgce.PathAspect.IDENTITY,)): 'source',
                    },
                )
                external_rvars[ptr_path_id, pgce.PathAspect.SOURCE] = rvar
                external_rvars[ptr_path_id, pgce.PathAspect.VALUE] = rvar
                external_rvars[src_path_id, pgce.PathAspect.IDENTITY] = rvar
                external_rvars[tgt_path_id, pgce.PathAspect.IDENTITY] = rvar
                if local_table_only and not is_lprop:
                    external_rvars[src_path_id, pgce.PathAspect.SOURCE] = rvar
                    external_rvars[src_path_id, pgce.PathAspect.VALUE] = rvar
                elif is_lprop:
                    external_rvars[tgt_path_id, pgce.PathAspect.VALUE] = rvar
            else:
                src_rvar = compiler.new_external_rvar(
                    rel_name=(source_alias,),
                    path_id=src_path_id,
                    outputs={},
                )
                external_rvars[src_path_id, pgce.PathAspect.IDENTITY] = src_rvar
                external_rvars[src_path_id, pgce.PathAspect.VALUE] = src_rvar
                external_rvars[src_path_id, pgce.PathAspect.SOURCE] = src_rvar

        # Wrap the expression into a select with iterator, so DML and
        # volatile expressions are executed once for each object.
        #
        # The result is roughly equivalent to:
        # for obj in Object union select <expr>

        # generate a unique path id for the outer scope
        typ = orig_schema.get(f'schema::ObjectType', type=s_types.Type)
        outer_path = irast.PathId.from_type(
            orig_schema, typ, typename=sn.QualName("std", "obj"), env=None,
        )

        root_uid = -1
        iter_uid = -2
        body_uid = -3
        # scope tree wrapping is roughly equivalent to:
        # "(std::obj) uid:-1": {
        #   "BRANCH uid:-2",
        #   "FENCE uid:-3": { ... compiled scope children ... }
        # }
        scope_iter = irast.ScopeTreeNode(
            unique_id=iter_uid,
        )
        scope_body = irast.ScopeTreeNode(
            unique_id=body_uid,
            fenced=True
        )
        # Need to make a copy of the children list because
        # attach_child removes the node from the parent list.
        for child in list(ir.scope_tree.children):
            scope_body.attach_child(child)

        scope_root = irast.ScopeTreeNode(
            unique_id=root_uid,
            path_id=outer_path,
        )
        scope_root.attach_child(scope_iter)
        scope_root.attach_child(scope_body)
        ir.scope_tree = scope_root

        # IR ast wrapping
        assert isinstance(ir.expr, irast.Set)
        for_body = ir.expr
        for_body.path_scope_id = body_uid
        ir.expr = irast.Set(
            path_id=outer_path,
            typeref=outer_path.target,
            path_scope_id=root_uid,
            expr=irast.SelectStmt(
                iterator_stmt=irast.Set(
                    path_id=src_path_id,
                    typeref=src_path_id.target,
                    path_scope_id=iter_uid,
                    expr=irast.SelectStmt(
                        result=irast.Set(
                            path_scope_id=iter_uid,
                            path_id=src_path_id,
                            typeref=src_path_id.target,
                            expr=irast.TypeRoot(typeref=src_path_id.target),
                        )
                    )
                ),

                result=for_body,
            )
        )

        # compile
        sql_res = compiler.compile_ir_to_sql_tree(
            ir,
            output_format=compiler.OutputFormat.NATIVE_INTERNAL,
            external_rels=external_rels,
            external_rvars=external_rvars,
            backend_runtime_params=context.backend_runtime_params,
        )
        sql_tree = sql_res.ast
        assert isinstance(sql_tree, pgast.SelectStmt)

        if produce_ctes:
            # ensure the result contains the object id in the second column

            from edb.pgsql.compiler import pathctx
            pathctx.get_path_output(
                sql_tree,
                src_path_id,
                aspect=pgce.PathAspect.IDENTITY,
                env=sql_res.env,
            )

        ctes = list(sql_tree.ctes or [])
        if sql_tree.ctes:
            sql_tree.ctes.clear()

        if check_non_null:
            # wrap into raise_on_null
            pointer_name = 'link' if is_link else 'property'
            msg = pgast.StringConstant(
                val=f"missing value for required {pointer_name}"
            )
            # Concat to string which is a JSON. Great. Equivalent to SQL:
            # '{"object_id": "' || {obj_id_ref} || '"}'
            detail = pgast.Expr(
                name='||',
                lexpr=pgast.StringConstant(val='{"object_id": "'),
                rexpr=pgast.Expr(
                    name='||',
                    lexpr=pgast.ColumnRef(name=('id',)),
                    rexpr=pgast.StringConstant(val='"}'),
                )
            )
            column = pgast.StringConstant(val=str(pointer.id))

            null_check = pgast.FuncCall(
                name=("edgedb", "raise_on_null"),
                args=[
                    pgast.ColumnRef(name=("val",)),
                    pgast.StringConstant(val="not_null_violation"),
                    pgast.NamedFuncArg(name="msg", val=msg),
                    pgast.NamedFuncArg(name="detail", val=detail),
                    pgast.NamedFuncArg(name="column", val=column),
                ],
            )

            inner_colnames = ["val"]
            target_list = [pgast.ResTarget(val=null_check)]
            if produce_ctes:
                inner_colnames.append("id")
                target_list.append(
                    pgast.ResTarget(val=pgast.ColumnRef(name=("id",)))
                )

            sql_tree = pgast.SelectStmt(
                target_list=target_list,
                from_clause=[
                    pgast.RangeSubselect(
                        subquery=sql_tree,
                        alias=pgast.Alias(
                            aliasname="_inner", colnames=inner_colnames
                        )
                    )
                ]
            )

        nullable = conv_expr.cardinality.can_be_zero()

        if produce_ctes:
            # convert root query into last CTE
            ctes.append(
                pgast.CommonTableExpr(
                    name="_conv_rel",
                    aliascolnames=["val", "id"],
                    query=sql_tree,
                )
            )
            # compile to SQL
            ctes_sql = codegen.generate_ctes_source(ctes)

            return (ctes_sql, None, nullable)

        else:
            # keep CTEs and select separate
            ctes_sql = codegen.generate_ctes_source(ctes)
            select_sql = codegen.generate_source(sql_tree)

            return (ctes_sql, select_sql, nullable)

    def schedule_endpoint_delete_action_update(
        self, link, orig_schema, schema, context
    ):
        endpoint_delete_actions = context.get(
            sd.DeltaRootContext).op.update_endpoint_delete_actions
        link_ops = endpoint_delete_actions.link_ops

        if isinstance(self, sd.DeleteObject):
            for i, (_, ex_link, _, _) in enumerate(link_ops):
                if ex_link == link:
                    link_ops.pop(i)
                    break

        link_ops.append((self, link, orig_schema, schema))


class LinkMetaCommand(PointerMetaCommand[s_links.Link]):

    @classmethod
    def _create_table(
        cls,
        link: s_links.Link,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        conditional: bool = False,
        create_children: bool = True,
    ):
        new_table_name = cls._get_table_name(link, schema)

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

        if not link.is_non_concrete(schema) and link.scalar():
            tgt_prop = link.getptr(schema, sn.UnqualName('target'))
            tgt_ptr = types.get_pointer_storage_info(
                tgt_prop, schema=schema)
            columns.append(
                dbops.Column(
                    name=tgt_ptr.column_name,
                    type=common.qname(*tgt_ptr.column_type)))

        table = dbops.Table(name=new_table_name)
        table.add_columns(columns)
        table.constraints = ordered.OrderedSet(constraints)

        ct = dbops.CreateTable(table=table)

        index_name = common.edgedb_name_to_pg_name(
            str(link.id) + '_target_key')
        index = dbops.Index(
            index_name,
            new_table_name,
            unique=False,
            metadata={'code': DEFAULT_INDEX_CODE},
        )
        index.add_columns([tgt_col])
        ci = dbops.CreateIndex(index)

        if conditional:
            c = dbops.CommandGroup(
                neg_conditions=[dbops.TableExists(new_table_name)])
        else:
            c = dbops.CommandGroup()

        c.add_command(ct)
        c.add_command(ci)

        c.add_command(
            dbops.Comment(
                table,
                str(link.get_verbosename(schema, with_parent=True)),
            ),
        )

        create_c.add_command(c)

        if create_children:
            for l_descendant in link.descendants(schema):
                if types.has_table(l_descendant, schema):
                    lc = LinkMetaCommand._create_table(
                        l_descendant,
                        schema,
                        context,
                        conditional=True,
                        create_children=False,
                    )
                    create_c.add_command(lc)

        return create_c

    def _create_link(
        self,
        link: s_links.Link,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:

        objtype = context.get(s_objtypes.ObjectTypeCommandContext)
        source = link.get_source(schema)

        if source is not None:
            source_is_view = (
                source.is_view(schema)
                or source.is_compound_type(schema)
                or source.get_is_derived(schema)
            )
        else:
            source_is_view = None

        if types.has_table(self.scls, schema):
            self.create_table(self.scls, schema, context)

        if (
            source is not None
            and not source_is_view
            and not link.is_pure_computable(schema)
        ):
            # We optimize away __type__ and don't store it.
            # Nothing to do except make sure the inhviews get updated.
            if link.get_shortname(schema).name == '__type__':
                self.update_source_if_cfg_view(
                    schema, context, link
                )
                return

            assert objtype
            ptr_stor_info = types.get_pointer_storage_info(
                link, resolve_type=False, schema=schema)

            fills_required = any(
                x.fill_expr for x in
                self.get_subcommands(
                    type=s_pointers.AlterPointerLowerCardinality))
            sets_required = bool(
                self.get_subcommands(
                    type=s_pointers.AlterPointerLowerCardinality))

            if ptr_stor_info.table_type == 'ObjectType':
                cols = self.get_columns(
                    link, schema, None, sets_required)
                table_name = objtype.op.table_name  # type: ignore
                assert isinstance(objtype.op, CompositeMetaCommand)
                objtype_alter_table = objtype.op.get_alter_table(
                    schema, context, manual=True)

                for col in cols:
                    cmd = dbops.AlterTableAddColumn(col)
                    objtype_alter_table.add_operation(cmd)

                self.pgops.add(objtype_alter_table)

                index_name = common.get_backend_name(
                    schema, link, catenate=False, aspect='index'
                )[1]

                pg_index = dbops.Index(
                    name=index_name, table_name=table_name,
                    unique=False, columns=[c.name for c in cols],
                    inherit=True,
                    metadata={
                        'code': DEFAULT_INDEX_CODE,
                    },
                )

                ci = dbops.CreateIndex(pg_index)
                self.pgops.add(ci)

                self.update_source_if_cfg_view(
                    schema, context, link
                )

            if (
                (default := link.get_default(schema))
                and not link.is_pure_computable(schema)
                and not fills_required
            ):
                self._alter_pointer_optionality(
                    schema, schema, context,
                    fill_expr=default, is_default=True)
            # If we're creating a required multi pointer without a SET
            # REQUIRED USING inside, run the alter_pointer_optionality
            # path to produce an error if there is existing data.
            elif (
                link.get_cardinality(schema).is_multi()
                and link.get_required(schema)
                and not link.is_pure_computable(schema)
                and not sets_required
            ):
                self._alter_pointer_optionality(
                    schema, schema, context, fill_expr=None)

            if not link.is_pure_computable(schema):
                self.schedule_endpoint_delete_action_update(
                    link, orig_schema, schema, context)

    def _delete_link(
        self,
        link: s_links.Link,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:

        # We optimize away __type__ and don't store it. Nothing to do.
        if link.get_shortname(schema).name == '__type__':
            return

        old_table_name = self._get_table_name(link, schema)

        if (
            not link.is_non_concrete(orig_schema)
            and types.has_table(link.get_source(orig_schema), orig_schema)
            and not link.is_pure_computable(orig_schema)
        ):
            ptr_stor_info = types.get_pointer_storage_info(
                link, schema=orig_schema)

            objtype = context.get(s_objtypes.ObjectTypeCommandContext)
            assert objtype

            if (not isinstance(objtype.op, s_objtypes.DeleteObjectType)
                    and ptr_stor_info.table_type == 'ObjectType'):
                self.update_if_cfg_view(schema, context, objtype.scls)
                assert isinstance(objtype.op, CompositeMetaCommand)
                alter_table = objtype.op.get_alter_table(
                    schema, context, manual=True)
                col = dbops.Column(
                    name=ptr_stor_info.column_name,
                    type=common.qname(*ptr_stor_info.column_type))
                colop = dbops.AlterTableDropColumn(col)
                alter_table.add_operation(colop)
                self.pgops.add(alter_table)

            self.attach_alter_table(context)

        if types.has_table(link, orig_schema):
            condition = dbops.TableExists(name=old_table_name)
            self.pgops.add(
                dbops.DropTable(name=old_table_name, conditions=[condition]))

        self.schedule_endpoint_delete_action_update(
            link, orig_schema, schema, context)


class CreateLink(LinkMetaCommand, adapts=s_links.CreateLink):
    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super()._create_begin(schema, context)

        link = self.scls
        self.table_name = self._get_table_name(self.scls, schema)

        self._create_link(link, schema, orig_schema, context)

        return schema

    def _create_finalize(self, schema, context):
        schema = super()._create_finalize(schema, context)
        self.apply_constraint_trigger_updates(schema)
        self.schedule_trampoline(self.scls, schema, context)
        return schema


class RenameLink(LinkMetaCommand, adapts=s_links.RenameLink):
    pass


class RebaseLink(LinkMetaCommand, adapts=s_links.RebaseLink):
    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = context.current().original_schema
        if types.has_table(self.scls, schema):
            self.update_if_cfg_view(schema, context, self.scls)

        schema = super()._alter_innards(schema, context)

        self.schedule_endpoint_delete_action_update(
            self.scls, orig_schema, schema, context)

        return schema


class SetLinkType(LinkMetaCommand, adapts=s_links.SetLinkType):

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super()._alter_begin(schema, context)
        orig_type = self.scls.get_target(orig_schema)
        new_type = self.scls.get_target(schema)
        if (
            types.has_table(self.scls.get_source(schema), schema)
            and not self.scls.is_pure_computable(schema)
            and (orig_type != new_type or self.cast_expr is not None)
        ):
            self._alter_pointer_type(self.scls, schema, orig_schema, context)
            self.schedule_endpoint_delete_action_update(
                self.scls, orig_schema, schema, context)
        return schema


class AlterLinkUpperCardinality(
    LinkMetaCommand,
    adapts=s_links.AlterLinkUpperCardinality,
):
    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = context.current().original_schema

        # We need to run the parent change *before* the children,
        # or else the view update in the child might fail if a
        # link table isn't created in the parent yet.
        if (
            not self.scls.is_non_concrete(schema)
            and not self.scls.is_pure_computable(schema)
            and types.has_table(self.scls.get_source(schema), schema)
        ):
            orig_card = self.scls.get_cardinality(orig_schema)
            new_card = self.scls.get_cardinality(schema)
            if orig_card != new_card:
                self._alter_pointer_cardinality(schema, orig_schema, context)

        return super()._alter_innards(schema, context)


class AlterLinkLowerCardinality(
    LinkMetaCommand,
    adapts=s_links.AlterLinkLowerCardinality,
):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super().apply(schema, context)

        if not self.scls.is_non_concrete(schema):
            orig_required = self.scls.get_required(orig_schema)
            new_required = self.scls.get_required(schema)
            if (
                types.has_table(self.scls.get_source(schema), schema)
                and not self.scls.is_endpoint_pointer(schema)
                and not self.scls.is_pure_computable(schema)
                and orig_required != new_required
            ):
                self._alter_pointer_optionality(
                    schema, orig_schema, context, fill_expr=self.fill_expr)

        return schema


class AlterLinkOwned(LinkMetaCommand, adapts=s_links.AlterLinkOwned):
    pass


class AlterLink(LinkMetaCommand, adapts=s_links.AlterLink):
    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_begin(schema, context)
        # We want to set this name up early, so children operations see it
        self.table_name = self._get_table_name(self.scls, schema)
        return schema

    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = context.current().original_schema

        link = self.scls
        is_abs = link.is_non_concrete(schema)
        is_comp = link.is_pure_computable(schema)
        was_comp = link.is_pure_computable(orig_schema)

        if not is_abs and (was_comp and not is_comp):
            self._create_link(link, schema, orig_schema, context)

        schema = super()._alter_innards(schema, context)

        if not is_abs and (not was_comp and is_comp):
            self._delete_link(link, schema, orig_schema, context)

        # We check whether otd has changed, rather than whether
        # it is an attribute on this alter, because it might
        # live on a nested SetOwned, for example.
        otd_changed = (
            link.get_on_target_delete(orig_schema) !=
            link.get_on_target_delete(schema)
        )
        osd_changed = (
            link.get_on_source_delete(orig_schema) !=
            link.get_on_source_delete(schema)
        )
        card_changed = (
            link.get_cardinality(orig_schema) !=
            link.get_cardinality(schema)
        )
        if (
            (otd_changed or osd_changed or card_changed)
            and not link.is_pure_computable(schema)
        ):
            self.schedule_endpoint_delete_action_update(
                link, orig_schema, schema, context)

        return schema

    def _alter_finalize(self, schema, context):
        schema = super()._alter_finalize(schema, context)
        self.apply_constraint_trigger_updates(schema)
        return schema


class DeleteLink(LinkMetaCommand, adapts=s_links.DeleteLink):
    def _delete_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = context.current().original_schema
        link = schema.get(self.classname, type=s_links.Link)

        schema = super()._delete_innards(schema, context)
        self._delete_link(link, schema, orig_schema, context)

        return schema

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)

        self.apply_constraint_trigger_updates(schema)

        return schema


class PropertyMetaCommand(PointerMetaCommand[s_props.Property]):

    @classmethod
    def _create_table(
        cls,
        prop: s_props.Property,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        conditional: bool = False,
        create_children: bool = True,
    ):
        new_table_name = cls._get_table_name(prop, schema)

        create_c = dbops.CommandGroup()

        columns = []

        src_col = common.edgedb_name_to_pg_name('source')

        columns.append(
            dbops.Column(
                name=src_col, type='uuid', required=True))

        id = sn.QualName(
            module=prop.get_name(schema).module, name=str(prop.id))
        index_name = common.convert_name(id, 'idx0', catenate=False)

        pg_index = dbops.Index(
            name=index_name[1], table_name=new_table_name,
            unique=False, columns=[src_col],
            metadata={'code': DEFAULT_INDEX_CODE},
        )

        ci = dbops.CreateIndex(pg_index)

        if not prop.is_non_concrete(schema):
            tgt_cols = cls.get_columns(prop, schema, None)
            columns.extend(tgt_cols)

        table = dbops.Table(name=new_table_name)
        table.add_columns(columns)

        ct = dbops.CreateTable(table=table)

        if conditional:
            c = dbops.CommandGroup(
                neg_conditions=[dbops.TableExists(new_table_name)])
        else:
            c = dbops.CommandGroup()

        c.add_command(ct)
        c.add_command(ci)

        c.add_command(
            dbops.Comment(
                table,
                str(prop.get_verbosename(schema, with_parent=True)),
            ),
        )

        create_c.add_command(c)

        if create_children:
            for p_descendant in prop.descendants(schema):
                if types.has_table(p_descendant, schema):
                    pc = PropertyMetaCommand._create_table(
                        p_descendant,
                        schema,
                        context,
                        conditional=True,
                        create_children=False,
                    )
                    create_c.add_command(pc)

        return create_c

    def _create_property(
        self,
        prop: s_props.Property,
        src: Optional[sd.ObjectCommandContext[s_sources.Source]],
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        propname = prop.get_shortname(schema).name

        if types.has_table(prop, schema):
            self.create_table(prop, schema, context)

        if (
            src
            and types.has_table(src.scls, schema)
            and not prop.is_pure_computable(schema)
        ):
            if (
                isinstance(src.scls, s_links.Link)
                and not types.has_table(src.scls, orig_schema)
            ):
                ct = src.op._create_table(  # type: ignore
                    src.scls, schema, context)
                self.pgops.add(ct)

            ptr_stor_info = types.get_pointer_storage_info(
                prop, resolve_type=False, schema=schema)

            fills_required = any(
                x.fill_expr for x in
                self.get_subcommands(
                    type=s_pointers.AlterPointerLowerCardinality))
            sets_required = bool(
                self.get_subcommands(
                    type=s_pointers.AlterPointerLowerCardinality))

            if (
                not isinstance(src.scls, s_objtypes.ObjectType)
                or ptr_stor_info.table_type == 'ObjectType'
            ):
                if (
                    not isinstance(src.scls, s_links.Link)
                    or propname not in {'source', 'target'}
                ):
                    assert isinstance(src.op, CompositeMetaCommand)

                    alter_table = src.op.get_alter_table(
                        schema,
                        context,
                        force_new=True,
                        manual=True,
                    )

                    default_value = self.get_pointer_default(
                        prop, schema, context)

                    if (
                        isinstance(src.scls, s_links.Link)
                        and not default_value
                        and prop.get_default(schema)
                    ):
                        raise errors.UnsupportedFeatureError(
                            f'default value for '
                            f'{prop.get_verbosename(schema, with_parent=True)}'
                            f' is too complicated; link property defaults '
                            f'must not depend on database contents',
                            span=self.span)

                    cols = self.get_columns(
                        prop, schema, default_value, sets_required)

                    for col in cols:
                        cmd = dbops.AlterTableAddColumn(col)
                        alter_table.add_operation(cmd)

                    self.pgops.add(alter_table)

                self.update_source_if_cfg_view(
                    schema, context, prop
                )

            if (
                (default := prop.get_default(schema))
                and not prop.is_pure_computable(schema)
                and not fills_required
                and not irtyputils.is_cfg_view(src.scls, schema)  # sigh
                # link properties use SQL defaults and shouldn't need
                # us to do it explicitly (which is good, since
                # _alter_pointer_optionality doesn't currently work on
                # linkprops)
                and not prop.is_link_property(schema)
            ):
                self._alter_pointer_optionality(
                    schema, schema, context,
                    fill_expr=default, is_default=True)
            # If we're creating a required multi pointer without a SET
            # REQUIRED USING inside, run the alter_pointer_optionality
            # path to produce an error if there is existing data.
            elif (
                prop.get_cardinality(schema).is_multi()
                and prop.get_required(schema)
                and not prop.is_pure_computable(schema)
                and not sets_required
            ):
                self._alter_pointer_optionality(
                    schema, schema, context, fill_expr=None)

            if not prop.is_pure_computable(schema):
                self.schedule_endpoint_delete_action_update(
                    prop, orig_schema, schema, context)

    def _delete_property(
        self,
        prop: s_props.Property,
        source: s_sources.Source,
        source_op,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        if types.has_table(source, schema):
            ptr_stor_info = types.get_pointer_storage_info(
                prop,
                schema=schema,
                link_bias=prop.is_link_property(schema),
            )

            if (
                ptr_stor_info.table_type == 'ObjectType'
                or prop.is_link_property(schema)
            ):
                alter_table = source_op.get_alter_table(
                    schema, context, force_new=True, manual=True)

                # source and target don't have a proper inheritence
                # hierarchy, so we can't do the source trick for them
                self.update_if_cfg_view(schema, context, source)

                col = dbops.AlterTableDropColumn(
                    dbops.Column(name=ptr_stor_info.column_name,
                                 type=ptr_stor_info.column_type))

                alter_table.add_operation(col)

                self.pgops.add(alter_table)
        elif (
            prop.is_link_property(schema)
            and types.has_table(source, orig_schema)
        ):
            old_table_name = self._get_table_name(source, orig_schema)
            self.pgops.add(dbops.DropTable(name=old_table_name))

        if types.has_table(prop, orig_schema):
            old_table_name = self._get_table_name(prop, orig_schema)
            self.pgops.add(dbops.DropTable(name=old_table_name))
            self.schedule_endpoint_delete_action_update(
                prop, orig_schema, schema, context)


class CreateProperty(PropertyMetaCommand, adapts=s_props.CreateProperty):
    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super()._create_begin(schema, context)
        prop = self.scls

        src = context.get(s_sources.SourceCommandContext)

        self.table_name = self._get_table_name(prop, schema)

        self._create_property(prop, src, schema, orig_schema, context)
        self.schedule_trampoline(self.scls, schema, context)

        return schema


class RenameProperty(PropertyMetaCommand, adapts=s_props.RenameProperty):
    pass


class RebaseProperty(PropertyMetaCommand, adapts=s_props.RebaseProperty):
    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = context.current().original_schema
        if types.has_table(self.scls, schema):
            self.update_if_cfg_view(schema, context, self.scls)

        schema = super()._alter_innards(schema, context)

        if not self.scls.is_pure_computable(schema):
            self.schedule_endpoint_delete_action_update(
                self.scls, orig_schema, schema, context)

        return schema


class SetPropertyType(PropertyMetaCommand, adapts=s_props.SetPropertyType):
    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super()._alter_begin(schema, context)
        orig_type = self.scls.get_target(orig_schema)
        new_type = self.scls.get_target(schema)
        if (
            types.has_table(self.scls.get_source(schema), schema)
            and not self.scls.is_pure_computable(schema)
            and not self.scls.is_endpoint_pointer(schema)
            and (orig_type != new_type or self.cast_expr is not None)
        ):
            self._alter_pointer_type(self.scls, schema, orig_schema, context)
        return schema


class AlterPropertyUpperCardinality(
    PropertyMetaCommand,
    adapts=s_props.AlterPropertyUpperCardinality,
):
    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = context.current().original_schema

        # We need to run the parent change *before* the children,
        # or else the view update in the child might fail if a
        # link table isn't created in the parent yet.
        if (
            not self.scls.is_non_concrete(schema)
            and not self.scls.is_pure_computable(schema)
            and not self.scls.is_endpoint_pointer(schema)
            and types.has_table(self.scls.get_source(schema), schema)
        ):
            orig_card = self.scls.get_cardinality(orig_schema)
            new_card = self.scls.get_cardinality(schema)
            if orig_card != new_card:
                self._alter_pointer_cardinality(schema, orig_schema, context)

        return super()._alter_innards(schema, context)


class AlterPropertyLowerCardinality(
    PropertyMetaCommand,
    adapts=s_props.AlterPropertyLowerCardinality,
):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super().apply(schema, context)

        if not self.scls.is_non_concrete(schema):
            orig_required = self.scls.get_required(orig_schema)
            new_required = self.scls.get_required(schema)
            if (
                types.has_table(self.scls.get_source(schema), schema)
                and not self.scls.is_endpoint_pointer(schema)
                and not self.scls.is_pure_computable(schema)
                and orig_required != new_required
            ):
                self._alter_pointer_optionality(
                    schema, orig_schema, context, fill_expr=self.fill_expr)

        return schema


class AlterPropertyOwned(
    PropertyMetaCommand,
    adapts=s_props.AlterPropertyOwned,
):
    pass


class AlterProperty(PropertyMetaCommand, adapts=s_props.AlterProperty):
    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        prop = self.scls
        orig_schema = context.current().original_schema

        src = context.get(s_sources.SourceCommandContext)
        is_comp = prop.is_pure_computable(schema)
        was_comp = prop.is_pure_computable(orig_schema)

        if src and (was_comp and not is_comp):
            self._create_property(prop, src, schema, orig_schema, context)

        schema = super()._alter_innards(schema, context)

        if src and (not was_comp and is_comp):
            self._delete_property(
                prop, src.scls, src.op, schema, orig_schema, context)

        if self.metadata_only:
            return schema

        if (
            not is_comp
            and (src and types.has_table(src.scls, schema))
        ):
            orig_def_val = self.get_pointer_default(prop, orig_schema, context)
            def_val = self.get_pointer_default(prop, schema, context)

            if orig_def_val != def_val:
                if prop.get_cardinality(schema).is_multi():
                    source_op: sd.Command = self
                else:
                    source_op = not_none(context.get_ancestor(
                        s_sources.SourceCommandContext, self)).op

                assert isinstance(source_op, CompositeMetaCommand)
                alter_table = source_op.get_alter_table(
                    schema, context, manual=True)

                ptr_stor_info = types.get_pointer_storage_info(
                    prop, schema=schema)
                alter_table.add_operation(
                    dbops.AlterTableAlterColumnDefault(
                        column_name=ptr_stor_info.column_name,
                        default=def_val))

                self.pgops.add(alter_table)

            card = self.get_resolved_attribute_value(
                'cardinality',
                schema=schema,
                context=context,
            )
            if card:
                self.schedule_endpoint_delete_action_update(
                    prop, orig_schema, schema, context)

        return schema


class DeleteProperty(PropertyMetaCommand, adapts=s_props.DeleteProperty):

    def _delete_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._delete_innards(schema, context)
        prop = self.scls
        orig_schema = context.current().original_schema

        source_ctx = self.get_referrer_context(context)
        if source_ctx is not None:
            source = source_ctx.scls
            source_op = source_ctx.op
        else:
            source = None
            source_op = None

        if source and not prop.is_pure_computable(schema):
            assert isinstance(source, s_sources.Source)
            self._delete_property(
                prop, source, source_op, schema, orig_schema, context)

        return schema


class CreateTrampolines(MetaCommand):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.trampolines: list[trampoline.Trampoline] = []
        self.table_targets: list[s_objtypes.ObjectType | s_pointers.Pointer] = (
            []
        )

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        for obj in self.table_targets:
            if not (schema.has_object(obj.id) and types.has_table(obj, schema)):
                continue
            if tramp := CompositeMetaCommand.create_type_trampoline(
                schema, obj
            ):
                self.trampolines.append(tramp)

        for t in self.trampolines:
            self.pgops.add(t.make())

        return schema


class UpdateEndpointDeleteActions(MetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.link_ops = []
        self.changed_targets = set()

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
                table=common.get_backend_name(
                    schema,
                    link,
                ),
            ))

        return '(' + '\nUNION ALL\n    '.join(selects) + ') as q'

    def _get_inline_link_table_union(
        self, schema, links
    ) -> str:
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
                ),
            ))

        return '(' + '\nUNION ALL\n    '.join(selects) + ') as q'

    def get_target_objs(self, link, schema):
        tgt = link.get_target(schema)
        if union := tgt.get_union_of(schema).objects(schema):
            objs = set(union)
        else:
            objs = {tgt}
        objs |= {
            x for obj in objs for x in obj.descendants(schema)}
        return {
            obj for obj in objs
            if (
                not obj.is_view(schema)
                and not irtyputils.is_cfg_view(obj, schema)
            )
        }

    def get_orphan_link_ancestors(self, link, schema):
        val = s_links.LinkSourceDeleteAction.DeleteTargetIfOrphan
        if link.get_on_source_delete(schema) != val:
            return set()
        ancestors = {
            x
            for base in link.get_bases(schema).objects(schema)
            for x in self.get_orphan_link_ancestors(base, schema)
        }
        if ancestors:
            return ancestors
        else:
            return {link}

    def get_trigger_name(
        self, schema, target, disposition, deferred=False, inline=False
    ):
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

        # Postgres applies triggers in alphabetical order, and
        # the names are uuids, which are not useful here.
        #
        # All we want for now is for source triggers to apply first,
        # though, so that a loop of objects with
        # 'on source delete delete target' + 'on target delete restrict'
        # succeeds.
        #
        # Fortunately S comes before T.
        order_prefix = disposition[0]

        name = common.get_backend_name(schema, target, catenate=False)
        return f'{order_prefix}_{name[1]}_{aspect}'

    def get_trigger_proc_name(
        self, schema, target, disposition, deferred=False, inline=False
    ):
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

        name = common.get_backend_name(schema, target, catenate=False)
        return (name[0], f'{name[1]}_{aspect}')

    def get_trigger_proc_text(
        self, target, links, *, disposition, inline, schema, context,
    ):
        if inline:
            return self._get_inline_link_trigger_proc_text(
                target, links, disposition=disposition,
                schema=schema, context=context)
        else:
            return self._get_outline_link_trigger_proc_text(
                target, links, disposition=disposition,
                schema=schema, context=context)

    def _get_outline_link_trigger_proc_text(
        self, target, links, *, disposition, schema, context
    ):

        chunks = []

        DA = s_links.LinkTargetDeleteAction

        if disposition == 'target':
            groups = itertools.groupby(
                links, lambda l: l.get_on_target_delete(schema))
            near_endpoint, far_endpoint = 'target', 'source'
        else:
            groups = itertools.groupby(
                links, lambda l: (
                    l.get_on_source_delete(schema)
                    if isinstance(l, s_links.Link)
                    else s_links.LinkSourceDeleteAction.Allow))
            near_endpoint, far_endpoint = 'source', 'target'

        for action, links in groups:
            if action is DA.Restrict or action is DA.DeferredRestrict:
                tables = self._get_link_table_union(schema, links)

                # We want versioned for stdlib (since the trampolines
                # don't exist yet) but trampolined for user code
                prefix = 'edgedb_VER' if context.stdmode else 'edgedb'

                text = textwrap.dedent(trampoline.fixup_query('''\
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
                            {prefix}.shortname_from_fullname(link.name),
                            {prefix}._get_schema_object_name(
                                link.{far_endpoint})
                            INTO linkname, endname
                        FROM
                            {prefix}._schema_links AS link
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
                '''.format(
                    tables=tables,
                    id='id',
                    tgtname=target.get_displayname(schema),
                    near_endpoint=near_endpoint,
                    far_endpoint=far_endpoint,
                    prefix=prefix,
                )))

                chunks.append(text)

            elif (
                action == s_links.LinkTargetDeleteAction.Allow
                or action == s_links.LinkSourceDeleteAction.Allow
            ):
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

            elif (
                action == s_links.LinkSourceDeleteAction.DeleteTarget
                or action ==
                    s_links.LinkSourceDeleteAction.DeleteTargetIfOrphan
            ):
                for link in links:
                    link_table = common.get_backend_name(schema, link)
                    objs = self.get_target_objs(link, schema)

                    # If the link is DELETE TARGET IF ORPHAN, build
                    # filters to ignore any objects that aren't
                    # orphans (wrt to this link).
                    roots = {
                        x
                        for root in self.get_orphan_link_ancestors(link, schema)
                        for x in [root, *root.descendants(schema)]
                    }

                    orphan_check = ''
                    for orphan_check_root in roots:
                        if not types.has_table(orphan_check_root, schema):
                            continue
                        check_table = common.get_backend_name(
                            schema, orphan_check_root
                        )
                        orphan_check += f'''\
                            AND NOT EXISTS (
                                SELECT FROM {check_table} as q2
                                WHERE q.target = q2.target
                                      AND q2.source != OLD.id
                            )
                        '''.strip()

                    # We find all the objects to delete in a CTE, then
                    # delete the link table entries, and then delete
                    # the targets. We apply the non-orphan filter when
                    # finding the objects.
                    prefix = textwrap.dedent(f'''\
                        WITH range AS (
                            SELECT target FROM {link_table} as q
                            WHERE q.source = OLD.id
                            {orphan_check}
                        ),
                        del AS (
                            DELETE FROM
                                {link_table}
                            WHERE
                                source = OLD.id
                        )
                    ''').strip()
                    parts = [prefix]

                    for i, obj in enumerate(objs):
                        tgt_table = common.get_backend_name(schema, obj)
                        text = textwrap.dedent(f'''\
                            d{i} AS (
                                DELETE FROM
                                    {tgt_table}
                                WHERE
                                    {tgt_table}.id IN (
                                        SELECT target
                                        FROM range
                                    )
                            )
                        ''').strip()
                        parts.append(text)

                    full = ',\n'.join(parts) + "\nSELECT '' INTO _dummy_text;"
                    chunks.append(full)

        text = textwrap.dedent('''\
            DECLARE
                link_type_id uuid;
                srcid uuid;
                tgtid uuid;
                linkname text;
                endname text;
                _dummy_text text;
            BEGIN
                {chunks}
                RETURN OLD;
            END;
        ''').format(chunks='\n\n'.join(chunks))

        return text

    def _get_inline_link_trigger_proc_text(
        self, target, links, *, disposition, schema, context
    ):

        chunks = []

        DA = s_links.LinkTargetDeleteAction

        if disposition == 'target':
            groups = itertools.groupby(
                links, lambda l: l.get_on_target_delete(schema))
        else:
            groups = itertools.groupby(
                links, lambda l: l.get_on_source_delete(schema))

        near_endpoint, far_endpoint = 'target', 'source'

        for action, links in groups:
            if action is DA.Restrict or action is DA.DeferredRestrict:
                tables = self._get_inline_link_table_union(schema, links)

                # We want versioned for stdlib (since the trampolines
                # don't exist yet) but trampolined for user code
                prefix = 'edgedb_VER' if context.stdmode else 'edgedb'

                text = textwrap.dedent(trampoline.fixup_query('''\
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
                            {prefix}.shortname_from_fullname(link.name),
                            {prefix}._get_schema_object_name(
                                link.{far_endpoint})
                            INTO linkname, endname
                        FROM
                            {prefix}._schema_links AS link
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
                '''.format(
                    tables=tables,
                    id='id',
                    tgtname=target.get_displayname(schema),
                    near_endpoint=near_endpoint,
                    far_endpoint=far_endpoint,
                    prefix=prefix,
                )))

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

            elif (
                action == s_links.LinkSourceDeleteAction.DeleteTarget
                or action ==
                    s_links.LinkSourceDeleteAction.DeleteTargetIfOrphan
            ):
                for link in links:
                    objs = self.get_target_objs(link, schema)

                    link_psi = types.get_pointer_storage_info(
                        link, schema=schema)
                    link_col = common.quote_ident(link_psi.column_name)

                    # If the link is DELETE TARGET IF ORPHAN, filter out
                    # any objects that aren't orphans (wrt to this link).
                    roots = {
                        x
                        for root in self.get_orphan_link_ancestors(link, schema)
                        for x in [root, *root.descendants(schema)]
                    }

                    orphan_check = ''
                    for orphan_check_root in roots:
                        check_source = orphan_check_root.get_source(schema)
                        if not types.has_table(check_source, schema):
                            continue
                        check_table = common.get_backend_name(
                            schema, check_source
                        )

                        check_link_psi = types.get_pointer_storage_info(
                            orphan_check_root, schema=schema)
                        check_link_col = common.quote_ident(
                            check_link_psi.column_name)

                        orphan_check += f'''\
                            AND NOT EXISTS (
                                SELECT FROM {check_table} as q2
                                WHERE q2.{check_link_col} = OLD.{link_col}
                                      AND q2.id != OLD.id
                            )
                        '''.strip()

                    # Do the orphan check (which trivially succeeds if
                    # the link isn't IF ORPHAN)
                    text = textwrap.dedent(f'''\
                        SELECT (
                            SELECT true
                            {orphan_check}
                        ) INTO ok;
                    ''').strip()

                    chunks.append(text)
                    for obj in objs:
                        tgt_table = common.get_backend_name(schema, obj)
                        text = textwrap.dedent(f'''\
                            IF ok THEN
                                DELETE FROM
                                    {tgt_table}
                                WHERE
                                    {tgt_table}.id = OLD.{link_col};
                            END IF;
                        ''')
                        chunks.append(text)

        text = textwrap.dedent('''\
            DECLARE
                link_type_id uuid;
                srcid uuid;
                tgtid uuid;
                linkname text;
                endname text;
                ok bool;
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
        DS = s_links.LinkSourceDeleteAction

        affected_sources: set[s_sources.Source] = set()
        affected_targets = {t for _, t in self.changed_targets}
        modifications = any(
            isinstance(op, RebaseObjectType) and op.removed_bases
            for op, _ in self.changed_targets
        )

        for link_op, link, orig_schema, eff_schema in self.link_ops:
            # Skip __type__ triggers, since __type__ isn't real and
            # also would be a huge pain to update each time if it was.
            if link.get_shortname(eff_schema).name == '__type__':
                continue

            if (
                isinstance(link_op, (DeleteProperty, DeleteLink))
                or (
                    link.is_pure_computable(eff_schema)
                    and not link.is_pure_computable(orig_schema)
                )
            ):
                source = link.get_source(orig_schema)
                if source:
                    current_source = schema.get_by_id(source.id, None)
                    if (current_source is not None
                            and not current_source.is_view(schema)):
                        modifications = True
                        affected_sources.add(current_source)

            if not eff_schema.has_object(link.id):
                continue

            target_is_affected = isinstance(link, s_links.Link)

            if link.is_non_concrete(eff_schema) or (
                link.is_pure_computable(eff_schema)
                and link.is_pure_computable(orig_schema)
            ):
                continue

            source = link.get_source(eff_schema)
            target = link.get_target(eff_schema)

            if (
                not isinstance(source, s_objtypes.ObjectType)
                or irtyputils.is_cfg_view(source, eff_schema)
            ):
                continue

            if not isinstance(link_op, (CreateProperty, CreateLink)):
                modifications = True

            if isinstance(link_op, (DeleteProperty, DeleteLink)):
                current_target = schema.get_by_id(target.id, None)
                if target_is_affected and current_target is not None:
                    affected_targets.add(current_target)
            else:
                if not source.is_material_object_type(eff_schema):
                    continue

                current_source = schema.get_by_id(source.id, None)
                if current_source:
                    affected_sources.add(current_source)

                if target_is_affected:
                    affected_targets.add(target)

                if isinstance(link_op, (SetLinkType, SetPropertyType)):
                    orig_target = link.get_target(orig_schema)
                    if target != orig_target:
                        current_orig_target = schema.get_by_id(
                            orig_target.id, None)
                        if current_orig_target is not None:
                            affected_targets.add(current_orig_target)

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
                    if types.has_table(descendant, schema):
                        all_affected_targets.add(descendant)

        delete_target_targets = set()

        for target in all_affected_targets:
            if irtyputils.is_cfg_view(target, schema):
                continue

            deferred_links = []
            deferred_inline_links = []
            links = []
            inline_links = []

            inbound_links = schema.get_referrers(
                target, scls_type=s_links.Link, field_name='target')

            # We need to look at all inbound links to all ancestors
            for ancestor in itertools.chain(
                target.get_ancestors(schema).objects(schema),
                schema.get_referrers(
                    target, scls_type=s_objtypes.ObjectType,
                    field_name='union_of'
                ),
            ):
                inbound_links |= schema.get_referrers(
                    ancestor, scls_type=s_links.Link, field_name='target')

            for link in inbound_links:
                if link.is_pure_computable(schema):
                    continue

                # Skip __type__ triggers, since __type__ isn't real and
                # also would be a huge pain to update each time if it was.
                if link.get_shortname(schema).name == '__type__':
                    continue

                source = link.get_source(schema)
                if (
                    not source.is_material_object_type(schema)
                    or irtyputils.is_cfg_view(source, schema)
                ):
                    continue

                # We need to track what objects are targets that can be
                # deleted on a source delete; it feeds into a decision we
                # need to make when handling source triggers below
                if link.get_on_source_delete(schema) != DS.Allow:
                    delete_target_targets.add(target)
                    affected_sources.add(target)

                action = link.get_on_target_delete(schema)

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

            # The ordering that we process links matters: Restrict
            # must be processed *after* Allow and DeleteSource,
            # because Restrict is applied (via views) to all
            # descendant links regardless of whether they have been
            # overridden, and so Allow and DeleteSource must be
            # handled first.
            ordering = (DA.Restrict, DA.Allow, DA.DeleteSource)

            links.sort(
                key=lambda l: (ordering.index(l.get_on_target_delete(schema)),
                               l.get_name(schema)))

            inline_links.sort(
                key=lambda l: (ordering.index(l.get_on_target_delete(schema)),
                               l.get_name(schema)))

            deferred_links.sort(
                key=lambda l: l.get_name(schema))

            deferred_inline_links.sort(
                key=lambda l: l.get_name(schema))

            if links or modifications:
                self._update_action_triggers(
                    schema, context, target, links, disposition='target')

            if inline_links or modifications:
                self._update_action_triggers(
                    schema, context, target, inline_links,
                    disposition='target', inline=True)

            if deferred_links or modifications:
                self._update_action_triggers(
                    schema, context, target, deferred_links,
                    disposition='target', deferred=True)

            if deferred_inline_links or modifications:
                self._update_action_triggers(
                    schema, context, target, deferred_inline_links,
                    disposition='target', deferred=True,
                    inline=True)

        # Now process source targets
        for source in affected_sources:
            links = []
            inline_links = []

            can_be_deleted_by_trigger = any(
                link.get_on_target_delete(schema) == DA.DeleteSource
                for link in source.get_pointers(schema).objects(schema)
                if isinstance(link, s_links.Link)
            ) or source in delete_target_targets

            for link in source.get_pointers(schema).objects(schema):
                if link.is_pure_computable(schema):
                    continue
                ptr_stor_info = types.get_pointer_storage_info(
                    link, schema=schema)

                delete_target = (
                    isinstance(link, s_links.Link)
                    and link.get_on_source_delete(schema) != DS.Allow
                )

                if ptr_stor_info.table_type == 'link' and (
                    # When a query does a delete, link tables get
                    # cleared out explicitly in our SQL, and so we
                    # don't need to run a source trigger unless there
                    # is an interesting source delete policy.
                    #
                    # However, if the object might be deleted by a
                    # link policy, then we still use a trigger to
                    # clean up the link table, since handling it
                    # in the original policy triggers would require
                    # lots of pretty nonlocal changes (adding a link
                    # to type Bar might require changing the triggers for
                    # type Foo that links to Bar).
                    can_be_deleted_by_trigger
                    or delete_target
                ):
                    links.append(link)
                # Inline links only need source actions if they might
                # delete the target
                elif delete_target:
                    inline_links.append(link)

            links.sort(
                key=lambda l: (
                    (l.get_on_target_delete(schema),)
                    if isinstance(l, s_links.Link) else (),
                    l.get_name(schema)))

            inline_links.sort(
                key=lambda l: (
                    (l.get_on_target_delete(schema),)
                    if isinstance(l, s_links.Link) else (),
                    l.get_name(schema)))

            if links or modifications:
                self._update_action_triggers(
                    schema, context, source, links, disposition='source')

            if inline_links or modifications:
                self._update_action_triggers(
                    schema, context, source, inline_links,
                    inline=True, disposition='source')

        return schema

    def _update_action_triggers(
        self,
        schema,
        context: sd.CommandContext,
        objtype: s_objtypes.ObjectType,
        links: List[s_links.Link],
        *,
        disposition: str,
        deferred: bool = False,
        inline: bool = False,
    ) -> None:

        table_name = common.get_backend_name(schema, objtype, catenate=False)

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
                inline=inline, schema=schema, context=context)

            trig_func = dbops.Function(
                name=proc_name, text=proc_text, volatility='volatile',
                returns='trigger', language='plpgsql')

            self.pgops.add(dbops.CreateFunction(trig_func, or_replace=True))

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


class ModuleMetaCommand(MetaCommand):
    pass


class CreateModule(ModuleMetaCommand, adapts=s_mod.CreateModule):
    pass


class AlterModule(ModuleMetaCommand, adapts=s_mod.AlterModule):
    pass


class DeleteModule(ModuleMetaCommand, adapts=s_mod.DeleteModule):
    pass


class DatabaseMixin:
    def ensure_has_create_database(self, backend_params):
        if not backend_params.has_create_database:
            self.pgops.add(
                dbops.Query(
                    f'''
                    SELECT
                        edgedb_VER.raise(
                            NULL::uuid,
                            msg => 'operation is not supported by the backend',
                            exc => 'feature_not_supported'
                        )
                    INTO _dummy_text
                    '''
                )
            )


class CreateDatabase(MetaCommand, DatabaseMixin, adapts=s_db.CreateBranch):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        backend_params = self._get_backend_params(context)
        self.ensure_has_create_database(backend_params)

        schema = super().apply(schema, context)
        db = self.scls
        tenant_id = self._get_tenant_id(context)
        db_name = common.get_database_backend_name(
            str(self.classname), tenant_id=tenant_id)
        # We use the base template for SCHEMA and DATA branches, since we
        # implement branches ourselves using pg_dump in order to avoid
        # connection restrictions.
        # For internal-only TEMPLATE branches, we use the source as
        # the template.
        template = (
            self.template
            if self.template and self.branch_type == ql_ast.BranchType.TEMPLATE
            else edbdef.EDGEDB_TEMPLATE_DB
        )
        tpl_name = common.get_database_backend_name(
            template, tenant_id=tenant_id)
        self.pgops.add(
            dbops.CreateDatabase(
                dbops.Database(
                    db_name,
                    metadata=dict(
                        id=str(db.id),
                        tenant_id=tenant_id,
                        builtin=self.get_attribute_value('builtin'),
                    ),
                ),
                template=tpl_name,
            )
        )
        return schema


class DropDatabase(MetaCommand, DatabaseMixin, adapts=s_db.DropBranch):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        backend_params = self._get_backend_params(context)
        self.ensure_has_create_database(backend_params)

        schema = super().apply(schema, context)
        tenant_id = self._get_tenant_id(context)
        db_name = common.get_database_backend_name(
            str(self.classname), tenant_id=tenant_id)
        self.pgops.add(dbops.DropDatabase(db_name))
        return schema


class AlterDatabase(MetaCommand, DatabaseMixin, adapts=s_db.AlterBranch):
    pass


class RenameDatabase(MetaCommand, DatabaseMixin, adapts=s_db.RenameBranch):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        backend_params = self._get_backend_params(context)
        self.ensure_has_create_database(backend_params)

        schema = super().apply(schema, context)
        tenant_id = self._get_tenant_id(context)
        db_name = common.get_database_backend_name(
            str(self.classname), tenant_id=tenant_id)
        new_name = common.get_database_backend_name(
            str(self.new_name), tenant_id=tenant_id)
        self.pgops.add(
            dbops.RenameDatabase(
                dbops.Database(
                    new_name,
                ),
                old_name=db_name,
            )
        )
        return schema


class RoleMixin:
    def ensure_has_create_role(self, backend_params):
        if not backend_params.has_create_role:
            self.pgops.add(
                dbops.Query(
                    f'''
                    SELECT
                        edgedb_VER.raise(
                            NULL::uuid,
                            msg => 'operation is not supported by the backend',
                            exc => 'feature_not_supported'
                        )
                    INTO _dummy_text
                    '''
                )
            )


class CreateRole(MetaCommand, RoleMixin, adapts=s_roles.CreateRole):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        backend_params = self._get_backend_params(context)
        self.ensure_has_create_role(backend_params)

        schema = super().apply(schema, context)
        role = self.scls

        membership = [str(x) for x in role.get_bases(schema).names(schema)]
        passwd = role.get_password(schema)
        superuser_flag = False

        members = set()

        role_name = str(role.get_name(schema))

        instance_params = backend_params.instance_params
        tenant_id = instance_params.tenant_id

        if role.get_superuser(schema):
            membership.append(edbdef.EDGEDB_SUPERGROUP)

            # If the cluster is not exposing an explicit superuser role,
            # we will make the created Postgres role superuser if we can
            if not instance_params.base_superuser:
                superuser_flag = backend_params.has_superuser_access

        if backend_params.session_authorization_role is not None:
            # When we connect to the backend via a proxy role, we
            # must ensure that role is a member of _every_ EdgeDB
            # role so that `SET ROLE` can work properly.
            members.add(backend_params.session_authorization_role)

        db_role = dbops.Role(
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
        self.pgops.add(dbops.CreateRole(db_role))
        return schema


class AlterRole(MetaCommand, RoleMixin, adapts=s_roles.AlterRole):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        role = self.scls

        backend_params = self._get_backend_params(context)
        instance_params = backend_params.instance_params
        tenant_id = instance_params.tenant_id
        role_name = str(role.get_name(schema))

        kwargs = {}
        if self.has_attribute_value('password'):
            passwd = self.get_attribute_value('password')
            if backend_params.has_create_role:
                # Only modify Postgres password of roles managed by EdgeDB
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
            self.ensure_has_create_role(backend_params)
            membership = [str(x) for x in role.get_bases(schema).names(schema)]
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
                superuser_flag = backend_params.has_superuser_access

            kwargs['superuser'] = superuser_flag

        if backend_params.has_create_role:
            dbrole = dbops.Role(name=pg_role_name, **kwargs)
        else:
            dbrole = dbops.SingleRole(**kwargs)
        self.pgops.add(dbops.AlterRole(dbrole))

        return schema


class RebaseRole(MetaCommand, RoleMixin, adapts=s_roles.RebaseRole):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        backend_params = self._get_backend_params(context)
        self.ensure_has_create_role(backend_params)

        schema = super().apply(schema, context)
        role = self.scls

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


class DeleteRole(MetaCommand, RoleMixin, adapts=s_roles.DeleteRole):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        backend_params = self._get_backend_params(context)
        self.ensure_has_create_role(backend_params)

        schema = super().apply(schema, context)
        tenant_id = self._get_tenant_id(context)
        self.pgops.add(dbops.DropRole(
            common.get_role_backend_name(
                str(self.classname), tenant_id=tenant_id)))
        return schema


class CreateExtensionPackage(
    MetaCommand,
    adapts=s_exts.CreateExtensionPackage,
):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)

        ext_id = str(self.scls.id)
        name__internal = str(self.scls.get_name(schema))
        name = self.scls.get_displayname(schema)
        version = self.scls.get_version(schema)._asdict()
        version['stage'] = version['stage'].name.lower()

        ext_module = self.scls.get_ext_module(schema)
        metadata = {
            ext_id: {
                'id': ext_id,
                'name': name,
                'name__internal': name__internal,
                'script': self.scls.get_script(schema),
                'version': version,
                'builtin': self.scls.get_builtin(schema),
                'internal': self.scls.get_internal(schema),
                'ext_module': ext_module and str(ext_module),
                'sql_extensions': list(self.scls.get_sql_extensions(schema)),
                'sql_setup_script': self.scls.get_sql_setup_script(schema),
                'sql_teardown_script': (
                    self.scls.get_sql_teardown_script(schema)
                ),
                'dependencies': list(self.scls.get_dependencies(schema)),
            }
        }

        ctx_backend_params = context.backend_runtime_params
        if ctx_backend_params is not None:
            backend_params = cast(
                params.BackendRuntimeParams, ctx_backend_params)
        else:
            backend_params = params.get_default_runtime_params()

        if backend_params.has_create_database:
            self.pgops.add(
                dbops.UpdateMetadataSection(
                    dbops.DatabaseWithTenant(name=edbdef.EDGEDB_TEMPLATE_DB),
                    section='ExtensionPackage',
                    metadata=metadata
                )
            )
        else:
            self.pgops.add(
                dbops.UpdateSingleDBMetadataSection(
                    edbdef.EDGEDB_TEMPLATE_DB,
                    section='ExtensionPackage',
                    metadata=metadata
                )
            )

        return schema


class DeleteExtensionPackage(
    MetaCommand,
    adapts=s_exts.DeleteExtensionPackage,
):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)

        ctx_backend_params = context.backend_runtime_params
        if ctx_backend_params is not None:
            backend_params = cast(
                params.BackendRuntimeParams, ctx_backend_params)
        else:
            backend_params = params.get_default_runtime_params()

        ext_id = str(self.scls.id)
        metadata = {
            ext_id: None
        }

        if backend_params.has_create_database:
            self.pgops.add(
                dbops.UpdateMetadataSection(
                    dbops.DatabaseWithTenant(name=edbdef.EDGEDB_TEMPLATE_DB),
                    section='ExtensionPackage',
                    metadata=metadata
                )
            )
        else:
            self.pgops.add(
                dbops.UpdateSingleDBMetadataSection(
                    edbdef.EDGEDB_TEMPLATE_DB,
                    section='ExtensionPackage',
                    metadata=metadata
                )
            )

        return schema


class CreateExtensionPackageMigration(
    MetaCommand,
    adapts=s_exts.CreateExtensionPackageMigration,
):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)

        ext_id = str(self.scls.id)
        name__internal = str(self.scls.get_name(schema))
        name = self.scls.get_displayname(schema)
        from_version = self.scls.get_from_version(schema)._asdict()
        from_version['stage'] = from_version['stage'].name.lower()
        to_version = self.scls.get_to_version(schema)._asdict()
        to_version['stage'] = to_version['stage'].name.lower()

        metadata = {
            ext_id: {
                'id': ext_id,
                'name': name,
                'name__internal': name__internal,
                'script': self.scls.get_script(schema),
                'from_version': from_version,
                'to_version': to_version,
                'builtin': self.scls.get_builtin(schema),
                'internal': self.scls.get_internal(schema),
                'sql_early_script': self.scls.get_sql_early_script(schema),
                'sql_late_script': self.scls.get_sql_late_script(schema),
            }
        }

        ctx_backend_params = context.backend_runtime_params
        if ctx_backend_params is not None:
            backend_params = cast(
                params.BackendRuntimeParams, ctx_backend_params)
        else:
            backend_params = params.get_default_runtime_params()

        if backend_params.has_create_database:
            self.pgops.add(
                dbops.UpdateMetadataSection(
                    dbops.DatabaseWithTenant(name=edbdef.EDGEDB_TEMPLATE_DB),
                    section='ExtensionPackageMigration',
                    metadata=metadata
                )
            )
        else:
            self.pgops.add(
                dbops.UpdateSingleDBMetadataSection(
                    edbdef.EDGEDB_TEMPLATE_DB,
                    section='ExtensionPackageMigration',
                    metadata=metadata
                )
            )

        return schema


class DeleteExtensionPackageMigration(
    MetaCommand,
    adapts=s_exts.DeleteExtensionPackageMigration,
):
    # XXX: 100% duplication with DeleteExtensionPackage
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)

        ctx_backend_params = context.backend_runtime_params
        if ctx_backend_params is not None:
            backend_params = cast(
                params.BackendRuntimeParams, ctx_backend_params)
        else:
            backend_params = params.get_default_runtime_params()

        ext_id = str(self.scls.id)
        metadata = {
            ext_id: None
        }

        if backend_params.has_create_database:
            self.pgops.add(
                dbops.UpdateMetadataSection(
                    dbops.DatabaseWithTenant(name=edbdef.EDGEDB_TEMPLATE_DB),
                    section='ExtensionPackageMigration',
                    metadata=metadata
                )
            )
        else:
            self.pgops.add(
                dbops.UpdateSingleDBMetadataSection(
                    edbdef.EDGEDB_TEMPLATE_DB,
                    section='ExtensionPackageMigration',
                    metadata=metadata
                )
            )

        return schema


class ExtensionCommand(MetaCommand):
    def _compute_version(self, ext_spec: str) -> None:
        '''Emits a Query to compute the version.

        Dumps it in _dummy_text.
        '''
        ext, vclauses = _parse_spec(ext_spec)

        # Dynamically select the highest version extension that matches
        # the provided version specification.
        lclauses = []
        for op, ver in vclauses:
            pver = f"string_to_array({ql(ver)}, '.')::int8[]"
            assert op in {'=', '>', '>=', '<', '<='}
            lclauses.append(f'v.split {op} {pver}')
        cond = ' and '.join(lclauses) if lclauses else 'true'

        ver_regexp = r'^\d+(\.\d+)+$'
        qry = textwrap.dedent(f'''\
            with v as (
               select name, version,
               string_to_array(version, '.')::int8[] as split
               from pg_available_extension_versions
               where
                 name = {ql(ext)}
                 and
                 version ~ '{ver_regexp}'
            )
            select edgedb_VER.raise_on_null(
              (
                 select v.version from v
                 where {cond}
                 order by split desc limit 1
              ),
              'feature_not_supported',
              msg => (
                'could not find extension satisfying ' || {ql(ext_spec)}
                || ': ' ||
                coalesce(
                  'only found versions ' ||
                    (select string_agg(v.version, ', ' order by v.split)
                     from v),
                  'extension not found'
                )
              )
            )
            into _dummy_text;
        ''')
        self.pgops.add(dbops.Query(qry))

    def _create_extension(self, ext_spec: str) -> None:
        ext = _get_ext_name(ext_spec)
        self._compute_version(ext_spec)

        # XXX: hardcode to put stuff into edgedb schema
        # so that operations can be easily accessed.
        # N.B: this won't work on heroku; is that fine?
        target_schema = 'edgedb'

        self.pgops.add(dbops.Query(textwrap.dedent(f"""\
            EXECUTE
              'CREATE EXTENSION {ext} WITH SCHEMA {target_schema} VERSION '''
              || _dummy_text || ''''
        """)))


def _get_ext_name(spec: str) -> str:
    return spec.split(' ')[0]


def _parse_spec(spec: str) -> tuple[str, list[tuple[str, str]]]:
    if ' ' not in spec:
        return (spec, [])

    ext, versions = spec.split(' ', 1)
    clauses = versions.split(',')
    pclauses = []
    for clause in clauses:
        for i in range(len(clause)):
            if clause[i].isnumeric():
                break
        pclauses.append((clause[:i], clause[i:]))

    return ext, pclauses


class CreateExtension(ExtensionCommand, adapts=s_exts.CreateExtension):

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._create_begin(schema, context)
        # backend_params = self._get_backend_params(context)
        # ext_schema = backend_params.instance_params.ext_schema

        package = self.scls.get_package(schema)

        for ext_spec in package.get_sql_extensions(schema):
            self._create_extension(ext_spec)

        if script := package.get_sql_setup_script(schema):
            self.pgops.add(dbops.Query(script))

        return schema

    def _create_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._create_innards(schema, context)

        if str(self.classname) == "ai":
            self.pgops.add(
                delta_ext_ai.pg_rebuild_all_pending_embeddings_views(
                    schema, context
                ),
            )

        return schema


class AlterExtension(ExtensionCommand, adapts=s_exts.AlterExtension):
    def _upgrade_extension(self, ext_spec: str) -> None:
        ext = _get_ext_name(ext_spec)
        self._compute_version(ext_spec)

        self.pgops.add(dbops.Query(textwrap.dedent(f"""\
            EXECUTE
              'ALTER EXTENSION {ext} UPDATE TO '''
              || _dummy_text || ''''
        """)))

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        old_package = self.scls.get_package(schema)
        schema = super()._alter_begin(schema, context)
        if not self.migration:
            return schema

        new_package = self.scls.get_package(schema)

        old_exts = {
            _get_ext_name(spec)
            for spec in old_package.get_sql_extensions(schema)
        }

        new_exts = set()
        # XXX: be smarter!
        for ext_spec in new_package.get_sql_extensions(schema):
            ext = _get_ext_name(ext_spec)
            new_exts.add(ext)
            if ext in old_exts:
                self._upgrade_extension(ext_spec)
            else:
                self._create_extension(ext_spec)

        # # XXX??? should do this after
        # for ext in old_exts:
        #     if ext not in new_exts:
        #         self.pgops.add(
        #             dbops.DropExtension(
        #                 dbops.Extension(
        #                     name=ext,
        #                     schema=ext,
        #                 ),
        #             )
        #         )

        # TODO: UPDATE the sql extension? Or should we do that in the
        # script?
        if script := self.migration.get_sql_early_script(schema):
            self.pgops.add(dbops.Query(script))

        return schema

    def _alter_finalize(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_finalize(schema, context)

        if (
            self.migration
            and (script := self.migration.get_sql_late_script(schema))
        ):
            self.pgops.add(dbops.Query(script))

        return schema


class DeleteExtension(ExtensionCommand, adapts=s_exts.DeleteExtension):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        extension = schema.get_global(s_exts.Extension, self.classname)
        package = extension.get_package(schema)

        if str(self.classname) == "ai":
            self.pgops.add(
                delta_ext_ai.pg_drop_all_pending_embeddings_views(schema),
            )

        schema = super().apply(schema, context)

        if script := package.get_sql_teardown_script(schema):
            self.pgops.add(dbops.Query(script))

        for ext_spec in package.get_sql_extensions(schema):
            ext = _get_ext_name(ext_spec)

            self.pgops.add(
                dbops.DropExtension(
                    dbops.Extension(
                        name=ext,
                        schema=ext,
                    ),
                )
            )

        return schema


class FutureBehaviorCommand(MetaCommand, s_futures.FutureBehaviorCommand):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        if self.future_cmd:
            self.pgops.add(self.future_cmd)
        return schema


class CreateFutureBehavior(
        FutureBehaviorCommand, adapts=s_futures.CreateFutureBehavior):
    pass


class DeleteFutureBehavior(
        FutureBehaviorCommand, adapts=s_futures.DeleteFutureBehavior):
    pass


class AlterFutureBehavior(
        FutureBehaviorCommand, adapts=s_futures.AlterFutureBehavior):
    pass


class DeltaRoot(MetaCommand, adapts=sd.DeltaRoot):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.config_ops = []

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        self.update_endpoint_delete_actions = UpdateEndpointDeleteActions()
        self.create_trampolines = CreateTrampolines()

        schema = super().apply(schema, context)

        self.update_endpoint_delete_actions.apply(schema, context)
        self.pgops.add(self.update_endpoint_delete_actions)

        self.create_trampolines.apply(schema, context)

        return schema


class MigrationCommand(MetaCommand):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        if last_mig := schema.get_last_migration():
            last_mig_name = last_mig.get_name(schema).name
        else:
            last_mig_name = None
        self.pgops.add(dbops.UpdateMetadata(
            dbops.CurrentDatabase(),
            {'last_migration': last_mig_name},
        ))
        return schema


class CreateMigration(
    MigrationCommand,
    adapts=s_migrations.CreateMigration,
):
    pass


class AlterMigration(
    MigrationCommand,
    adapts=s_migrations.AlterMigration,
):
    pass


class DeleteMigration(
    MigrationCommand,
    adapts=s_migrations.DeleteMigration,
):
    pass
