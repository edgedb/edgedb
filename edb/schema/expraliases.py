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
from typing import Any, Optional, Tuple, List, TYPE_CHECKING, Set

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
        computed. This is handled by overloading _is_computable.

    Computed globals also have explicit 'required' and 'cardinality' fields,
    which are managed explicitly in the globals code.
    """

    TYPE_FIELD_NAME = ''
    ALIAS_LIKE_EXPR_FIELDS: tuple[str, ...] = ()

    @classmethod
    def _get_alias_name(cls, type_name: sn.QualName) -> sn.QualName:
        raise NotImplementedError

    @classmethod
    def _is_computable(
        cls, obj: so.QualifiedObject_T, schema: s_schema.Schema
    ) -> bool:
        raise NotImplementedError

    # Generic code

    def _delete_alias_types(
        self,
        scls: so.QualifiedObject_T,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        unset_type: bool = True,
    ) -> sd.CommandGroup:
        from . import globals as s_globals
        from . import ordering as s_ordering

        assert isinstance(scls, (Alias, s_globals.Global))
        types: so.ObjectSet[s_types.Type] = scls.get_created_types(schema)
        created = types.objects(schema)

        delta = sd.DeltaRoot()

        # Unset created_types and type/target, so the types can be dropped
        alter_alias = scls.init_delta_command(schema, sd.AlterObject)
        alter_alias.canonical = True
        # This would usually not be needed, as both Alias.type and
        # Global.target have ON TARGET DELETE DEFERRED RESTRICT.
        # But because we are using "if_unused", we want to delete references
        # to these types so they get dropped if had been the only ref.
        if unset_type:
            # (there are cases when we don't need to unset the type, such as
            # when a computed global has been converted to a non-computed one)
            alter_alias.add(
                sd.AlterObjectProperty(
                    property=self.TYPE_FIELD_NAME, new_value=None
                )
            )
        alter_alias.set_attribute_value('created_types', set())

        for dep_type in created:
            if_unused = isinstance(dep_type, s_types.Collection)

            drop_dep = dep_type.init_delta_command(
                schema, sd.DeleteObject, if_exists=True, if_unused=if_unused
            )
            subcmds = drop_dep._canonicalize(schema, context, dep_type)
            drop_dep.update(subcmds)

            delta.add(drop_dep)

        delta = s_ordering.linearize_delta(
            delta, old_schema=schema, new_schema=schema
        )
        delta.prepend(alter_alias)
        return delta

    @classmethod
    def get_type(
        cls, obj: so.QualifiedObject_T, schema: s_schema.Schema
    ) -> s_types.Type:
        obj = obj.get_field_value(schema, cls.TYPE_FIELD_NAME)
        assert isinstance(obj, s_types.Type)
        return obj

    @classmethod
    def _mangle_name(
        cls,
        type_name: sn.QualName,
        *,
        include_module_in_name: bool,
    ) -> sn.QualName:
        base_name = (
            type_name
            if include_module_in_name else
            type_name.get_local_name()
        )
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
        if field.name in self.ALIAS_LIKE_EXPR_FIELDS:
            rt = self.get_type(self.scls, schema)
            return s_types.type_dummy_expr(rt, schema)
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
        span: Optional[parsing.Span] = None,
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
            drop_old_types_cmd = self._delete_alias_types(
                self.scls, schema, context)
            with context.suspend_dep_verification():
                pschema = drop_old_types_cmd.apply(pschema, context)

        ir = compile_alias_expr(
            expr.parse(),
            classname,
            pschema,
            context,
            span=span,
        )

        expr = s_expr.Expression.from_ir(expr, ir, schema=schema)

        is_global = (self.get_schema_metaclass().
                     get_schema_class_displayname() == 'global')
        cmd, type_shell, created_types = _create_alias_types(
            expr=expr,
            classname=classname,
            schema=schema,
            is_global=is_global,
            span=span,
        )
        if drop_old_types_cmd:
            cmd.prepend(drop_old_types_cmd)

        return cmd, type_shell, expr, created_types


class AliasCommand(
    AliasLikeCommand[Alias],
    context_class=AliasCommandContext,
):
    TYPE_FIELD_NAME = 'type'
    ALIAS_LIKE_EXPR_FIELDS = ('expr',)

    @classmethod
    def _get_alias_name(cls, type_name: sn.QualName) -> sn.QualName:
        alias_name = sn.shortname_from_fullname(type_name)
        assert isinstance(alias_name, sn.QualName), "expected qualified name"
        return alias_name

    @classmethod
    def _is_computable(cls, obj: Alias, schema: s_schema.Schema) -> bool:
        return True

    @classmethod
    def _classname_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.ObjectDDL,
        context: sd.CommandContext,
    ) -> sn.QualName:
        type_name = super()._classname_from_ast(schema, astnode, context)
        return cls._mangle_name(type_name, include_module_in_name=True)

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
            context=context,
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

            # generated types might conflict with existing types
            if other_obj := schema.get(alias_name, default=None):
                vn = other_obj.get_verbosename(schema, with_parent=True)
                raise errors.SchemaError(f'{vn} already exists')

            type_cmd, type_shell, expr, created_types = self._handle_alias_op(
                expr=self.get_attribute_value('expr'),
                classname=alias_name,
                schema=schema,
                context=context,
                span=self.get_attribute_span('expr'),
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
        if not context.canonical and self._is_computable(self.scls, schema):
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
        if not context.canonical:
            schema = self._propagate_if_expr_refs(
                schema,
                context,
                action=self.get_friendly_description(schema=schema),
            )

            expr = self.get_attribute_value('expr')
            is_computable = self._is_computable(self.scls, schema)
            if expr:
                alias_name = self._get_alias_name(self.classname)
                type_cmd, type_shell, expr, created_tys = self._handle_alias_op(
                    expr=expr,
                    classname=alias_name,
                    schema=schema,
                    context=context,
                    is_alter=is_computable,
                    span=self.get_attribute_span('expr'),
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
            else:
                # there is no expr

                if is_computable and self.has_attribute_value('expr'):
                    # this is a global that just had its expr unset
                    self.add(
                        self._delete_alias_types(
                            self.scls, schema, context, unset_type=False
                        )
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
        if self._is_computable(scls, schema):
            ops.append(self._delete_alias_types(scls, schema, context))
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
    span: Optional[parsing.Span] = None,
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
            bootstrap_mode=context.stdmode,
        ),
    )

    if ir.volatility.is_volatile():
        raise errors.SchemaDefinitionError(
            f'volatile functions are not permitted in schema-defined '
            f'computed expressions',
            span=span,
        )

    context.cache_value((expr, classname), ir)

    return ir


def _create_alias_types(
    *,
    expr: s_expr.CompiledExpression,
    classname: sn.QualName,
    schema: s_schema.Schema,
    is_global: bool,
    span: Optional[parsing.Span] = None,
) -> Tuple[
    sd.Command,
    s_types.TypeShell[s_types.Type],
    Set[so.ObjectShell[s_types.Type]],
]:
    from . import ordering as s_ordering
    from edb.ir import utils as irutils

    ir = expr.irast
    new_schema = ir.schema

    derived_delta = sd.DeltaRoot()

    created_type_shells: Set[so.ObjectShell[s_types.Type]] = set()

    for ty_id in irutils.collect_schema_types(ir.expr):
        if schema.has_object(ty_id):
            # this is not a new type, skip
            continue
        ty = new_schema.get_by_id(ty_id, type=s_types.Type)

        name = ty.get_name(new_schema)
        if (
            not isinstance(ty, s_types.Collection)
            and not _has_alias_name_prefix(classname, name)
        ):
            # not all created types are visible from the root, so they don't
            # need to be created in the schema
            continue

        new_schema = ty.update(
            new_schema,
            dict(
                alias_is_persistent=True,
                expr_type=s_types.ExprType.Select,
                from_alias=True,
                from_global=is_global,
                internal=False,
                builtin=False,
            ),
        )
        if isinstance(ty, s_types.Collection):
            new_schema = ty.set_field_value(new_schema, 'is_persistent', True)

        derived_delta.add(
            ty.as_create_delta(
                schema=new_schema, context=so.ComparisonContext()
            )
        )
        created_type_shells.add(so.ObjectShell(name=name, schemaclass=type(ty)))

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
        sourcectx=span,
    )
    return result, type_shell, created_type_shells


def _has_alias_name_prefix(
    alias_name: sn.QualName,
    name: sn.Name,
) -> bool:
    return alias_name == name or (
        isinstance(name, sn.QualName)
        and name.module == alias_name.module
        and name.name.startswith(f'__{alias_name.name}__')
    )
