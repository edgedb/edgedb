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
from typing import Any, Optional, Tuple, Dict, List, TYPE_CHECKING, Set

from edb import errors
from edb.common import parsing

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes
from edb.edgeql.compiler import astutils as qlastutils

from . import annos as s_anno
from . import expr as s_expr
from . import delta as sd
from . import name as sn
from . import objects as so
from . import types as s_types


if TYPE_CHECKING:
    from edb.ir import ast as irast
    from . import schema as s_schema


class Alias(
    so.QualifiedObject,
    s_anno.AnnotationSubject,
    qlkind=qltypes.SchemaObjectClass.ALIAS,
    data_safe=True,
):

    expr = so.SchemaField(
        s_expr.Expression,
        default=None,
        coerce=True,
        compcoef=0.909,
    )

    type = so.SchemaField(
        s_types.Type,
        compcoef=0.909,
    )

    created_types = so.SchemaField(
        so.ObjectSet[s_types.Type],
        default=so.DEFAULT_CONSTRUCTOR,
    )


class AliasCommandContext(
    sd.ObjectCommandContext[so.Object],
    s_anno.AnnotationSubjectCommandContext
):
    pass


def _is_view_from_alias(
    alias_name: sn.QualName,
    obj: s_types.Type,
    schema: s_schema.Schema,
) -> bool:
    name = obj.get_name(schema)
    return alias_name == name or (
        isinstance(name, sn.QualName)
        and name.module == alias_name.module
        and name.name.startswith(f'__{alias_name.name}__')
    )


class AliasLikeCommand(
    sd.QualifiedObjectCommand[so.QualifiedObject_T],
):
    """Common code for "alias-likes": that is, aliases and globals

    Aliases and computed globals behave extremely similarly, except
    for a few annoying differences that need to be handled in the
    subclasses with appropriate overloads:
      * In aliases, the type field name is 'type', while for computed
        globals it is 'target'. This annoying discrepency is because
        computed globals also share a bunch of code paths with pointers,
        which use 'target', and so there needed to be a mismatch on one
        of the sides. Handled by overloading TYPE_FIELD_NAME.
      * For aliases, it is the generated view type that gets the real
        name and the alias that gets the mangled one. For globals,
        the real global needs to get the real name, so that the name
        does not depend on whether it is computed or not.
        This is handled by overloading _get_alias_name, which computes
        the name of the alias type (and _classname_from_ast).
      * Also aliases *always* are alias-like, while globals only are when
        computed. This is handled by overloading _is_alias.

    Computed globals also have explicit 'required' and 'cardinality' fields,
    which are managed explicitly in the globals code.
    """

    TYPE_FIELD_NAME = ''

    @classmethod
    def _get_alias_name(cls, type_name: sn.QualName) -> sn.QualName:
        raise NotImplementedError

    @classmethod
    def _is_alias(
            cls, obj: so.QualifiedObject_T, schema: s_schema.Schema) -> bool:
        raise NotImplementedError

    # Generic code

    def _delete_alias_type(
        self,
        scls: so.QualifiedObject_T,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> sd.CommandGroup:
        from . import globals as s_globals

        assert isinstance(scls, (Alias, s_globals.Global))
        types: so.ObjectSet[s_types.Type] = scls.get_created_types(schema)
        created = types.objects(schema)

        cmd = sd.CommandGroup()

        wipe_created_types = scls.init_delta_command(schema, sd.AlterObject)
        wipe_created_types.canonical = True
        wipe_created_types.set_attribute_value('created_types', set())
        cmd.add(wipe_created_types)

        for dep_type in created:
            drop_dep = dep_type.init_delta_command(
                schema, sd.DeleteObject, if_exists=True
            )
            subcmds = drop_dep._canonicalize(schema, context, dep_type)
            drop_dep.update(subcmds)

            cmd.add(drop_dep)
        return cmd

    @classmethod
    def get_type(
        cls, obj: so.QualifiedObject_T, schema: s_schema.Schema
    ) -> s_types.Type:
        obj = obj.get_field_value(schema, cls.TYPE_FIELD_NAME)
        assert isinstance(obj, s_types.Type)
        return obj

    @classmethod
    def _mangle_name(cls, type_name: sn.QualName) -> sn.QualName:
        base_name = type_name
        quals = (cls.get_schema_metaclass().get_schema_class_displayname(),)
        pnn = sn.get_specialized_name(base_name, str(type_name), *quals)
        name = sn.QualName(name=pnn, module=type_name.module)
        assert isinstance(name, sn.QualName)
        return name

    def get_dummy_expr_field_value(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: Any,
    ) -> Optional[s_expr.Expression]:
        if field.name == 'expr':
            # XXX: this is imprecise
            return s_expr.Expression(text='std::Object')
        else:
            raise NotImplementedError(f'unhandled field {field.name!r}')

    def _handle_alias_op(
        self,
        *,
        expr: s_expr.Expression,
        classname: sn.QualName,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        is_alter: bool = False,
        parser_context: Optional[parsing.ParserContext] = None,
    ) -> Tuple[
        sd.Command,
        s_types.TypeShell[s_types.Type],
        s_expr.Expression,
        Set[so.ObjectShell[s_types.Type]],
    ]:
        pschema = schema

        # For alters, drop the alias first, use the schema without the alias
        # for compilation of the new alias expr
        drop_old_types_cmd: Optional[sd.Command] = None
        if is_alter:
            drop_old_types_cmd = self._delete_alias_type(
                self.scls, schema, context)
            with context.suspend_dep_verification():
                pschema = drop_old_types_cmd.apply(pschema, context)

        ir = compile_alias_expr(
            expr.qlast,
            classname,
            pschema,
            context,
            parser_context=parser_context,
        )

        expr = s_expr.Expression.from_ir(expr, ir, schema=schema)            

        is_global = (self.get_schema_metaclass().
                     get_schema_class_displayname() == 'global')
        cmd, type_shell = define_alias(
            expr=expr,
            classname=classname,
            schema=schema,
            is_global=is_global,
            parser_context=parser_context,
        )
        if drop_old_types_cmd:
            cmd.prepend(drop_old_types_cmd)

        created_types: Set[so.ObjectShell[s_types.Type]] = {
            so.ObjectShell(
                name=ty.get_name(expr.ir_statement.schema),
                schemaclass=type(ty),
            )
            for ty in expr.ir_statement.created_schema_types
        }
        return cmd, type_shell, expr, created_types


class AliasCommand(
    AliasLikeCommand[Alias],
    context_class=AliasCommandContext,
):
    TYPE_FIELD_NAME = 'type'

    @classmethod
    def _get_alias_name(cls, type_name: sn.QualName) -> sn.QualName:
        alias_name = sn.shortname_from_fullname(type_name)
        assert isinstance(alias_name, sn.QualName), "expected qualified name"
        return alias_name

    @classmethod
    def _is_alias(cls, obj: Alias, schema: s_schema.Schema) -> bool:
        return True

    @classmethod
    def _classname_from_ast(cls,
                            schema: s_schema.Schema,
                            astnode: qlast.NamedDDL,
                            context: sd.CommandContext
                            ) -> sn.QualName:
        type_name = super()._classname_from_ast(schema, astnode, context)
        return cls._mangle_name(type_name)

    def compile_expr_field(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        field: so.Field[Any],
        value: s_expr.Expression,
        track_schema_ref_exprs: bool=False,
    ) -> s_expr.CompiledExpression:
        assert field.name == 'expr'
        classname = sn.shortname_from_fullname(self.classname)
        assert isinstance(classname, sn.QualName), \
            "expected qualified name"
        return value.compiled(
            schema=schema,
            options=qlcompiler.CompilerOptions(
                derived_target_module=classname.module,
                modaliases=context.modaliases,
                in_ddl_context_name='alias definition',
                track_schema_ref_exprs=track_schema_ref_exprs,
            ),
        )


class CreateAliasLike(
    AliasLikeCommand[so.QualifiedObject_T],
    sd.CreateObject[so.QualifiedObject_T],
):
    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if not context.canonical and self.get_attribute_value('expr'):
            alias_name = self._get_alias_name(self.classname)
            type_cmd, type_shell, expr, created_types = self._handle_alias_op(
                expr=self.get_attribute_value('expr'),
                classname=alias_name,
                schema=schema,
                context=context,
                parser_context=self.get_attribute_source_context('expr'),
            )
            self.add_prerequisite(type_cmd)
            self.set_attribute_value('expr', expr)
            self.set_attribute_value(
                self.TYPE_FIELD_NAME, type_shell, computed=True)
            self.set_attribute_value('created_types', created_types)

        return super()._create_begin(schema, context)


class CreateAlias(
    CreateAliasLike[Alias],
    AliasCommand,
):
    astnode = qlast.CreateAlias


class RenameAliasLike(
    AliasLikeCommand[so.QualifiedObject_T],
    sd.RenameObject[so.QualifiedObject_T],
):

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if not context.canonical and self._is_alias(self.scls, schema):
            assert isinstance(self.new_name, sn.QualName)
            new_alias_name = self._get_alias_name(self.new_name)
            alias_type = self.get_type(self.scls, schema)
            alter_cmd = alias_type.init_delta_command(schema, sd.AlterObject)
            rename_cmd = alias_type.init_delta_command(
                schema,
                sd.RenameObject,
                new_name=new_alias_name,
            )
            alter_cmd.add(rename_cmd)
            self.add_prerequisite(alter_cmd)

        return super()._alter_begin(schema, context)


class RenameAlias(RenameAliasLike[Alias], AliasCommand):
    pass


class AlterAliasLike(
    AliasLikeCommand[so.QualifiedObject_T],
    sd.AlterObject[so.QualifiedObject_T],
):
    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if (
            not context.canonical
            # FIXME: This is not really correct, but alias altering is
            # currently too broken to accept expr propagations.
            and not self.from_expr_propagation
        ):
            expr = self.get_attribute_value('expr')
            is_alias = self._is_alias(self.scls, schema)
            if expr:
                alias_name = self._get_alias_name(self.classname)
                type_cmd, type_shell, expr, created_tys = self._handle_alias_op(
                    expr=expr,
                    classname=alias_name,
                    schema=schema,
                    context=context,
                    is_alter=is_alias,
                    parser_context=self.get_attribute_source_context('expr'),
                )

                self.add_prerequisite(type_cmd)

                self.set_attribute_value('expr', expr)
                self.set_attribute_value(
                    self.TYPE_FIELD_NAME, type_shell, computed=True)

                self.set_attribute_value('created_types', created_tys)

                # Clear out the type field in the schema *now*,
                # before we call the parent _alter_begin, which will
                # run prerequisites. This prevents the type reference
                # from interferring with deletion. (And the deletion of
                # the type has to be done as a prereq, since it needs
                # to precede the creation of the replacement type
                # with the same name.)
                schema = schema.unset_obj_field(
                    self.scls, self.TYPE_FIELD_NAME)
            elif not expr and is_alias and self.has_attribute_value('expr'):
                self.add(self._delete_alias_type(self.scls, schema, context))

            schema = self._propagate_if_expr_refs(
                schema,
                context,
                action=self.get_friendly_description(schema=schema),
            )

        return super()._alter_begin(schema, context)


class AlterAlias(
    AlterAliasLike[Alias],
    AliasCommand,
):
    astnode = qlast.AlterAlias


class DeleteAliasLike(
    AliasLikeCommand[so.QualifiedObject_T],
    sd.DeleteObject[so.QualifiedObject_T],
):
    def _canonicalize(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        scls: so.QualifiedObject_T,
    ) -> List[sd.Command]:
        ops = super()._canonicalize(schema, context, scls)
        if self._is_alias(scls, schema):
            ops.append(self._delete_alias_type(scls, schema, context))
        return ops


class DeleteAlias(
    DeleteAliasLike[Alias],
    AliasCommand,
):
    astnode = qlast.DropAlias


def compile_alias_expr(
    expr: qlast.Expr,
    classname: sn.QualName,
    schema: s_schema.Schema,
    context: sd.CommandContext,
    parser_context: Optional[parsing.ParserContext] = None,
) -> irast.Statement:
    cached: Optional[irast.Statement] = (
        context.get_cached((expr, classname)))
    if cached is not None:
        return cached

    expr = qlastutils.ensure_ql_query(expr)

    ir = qlcompiler.compile_ast_to_ir(
        expr,
        schema,
        options=qlcompiler.CompilerOptions(
            derived_target_module=classname.module,
            result_view_name=classname,
            modaliases=context.modaliases,
            schema_view_mode=True,
            in_ddl_context_name='alias definition',
        ),
    )

    if ir.volatility == qltypes.Volatility.Volatile:
        raise errors.SchemaDefinitionError(
            f'volatile functions are not permitted in schema-defined '
            f'computed expressions',
            context=parser_context,
        )

    context.cache_value((expr, classname), ir)

    return ir


def define_alias(
    *,
    expr: s_expr.CompiledExpression,
    classname: sn.QualName,
    schema: s_schema.Schema,
    is_global: bool,
    parser_context: Optional[parsing.ParserContext] = None,
) -> Tuple[sd.Command, s_types.TypeShell[s_types.Type]]:
    from . import ordering as s_ordering

    ir = expr.irast
    new_schema = ir.schema

    derived_delta = sd.DeltaRoot()

    for ty in ir.created_schema_types:            
        new_schema = ty.set_field_value(
            new_schema, 'alias_is_persistent', True
        )
        new_schema = ty.set_field_value(
            new_schema, 'expr_type', s_types.ExprType.Select
        )
        new_schema = ty.set_field_value(
            new_schema, 'from_alias', True
        )
        new_schema = ty.set_field_value(
            new_schema, 'from_global', is_global
        )
        new_schema = ty.set_field_value(
            new_schema, 'internal', False
        )
        new_schema = ty.set_field_value(
            new_schema, 'builtin', False
        )
        
        derived_delta.add(
            ty.as_create_delta(
                schema=new_schema,
                context=so.ComparisonContext()
            )
        )

    derived_delta = s_ordering.linearize_delta(
        derived_delta, old_schema=schema, new_schema=new_schema
    )

    type_cmd = None
    for op in derived_delta.get_subcommands():
        assert isinstance(op, sd.ObjectCommand)
        if op.classname == classname:
            type_cmd = op
            break
    assert type_cmd

    type_cmd.set_attribute_value('expr', expr)

    result = sd.CommandGroup()
    result.update(derived_delta.get_subcommands())

    type_shell = s_types.TypeShell(
        name=classname,
        origname=classname,
        schemaclass=type_cmd.get_schema_metaclass(),
        sourcectx=parser_context,
    )
    return result, type_shell
