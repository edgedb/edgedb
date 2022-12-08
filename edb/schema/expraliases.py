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
from typing import *

from edb import errors
from edb.common import parsing

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes

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

    @classmethod
    def _get_created_types(
        cls,
        scls: so.QualifiedObject_T,
        schema: s_schema.Schema,
    ) -> set[s_types.Type]:
        objs = set()

        typ = cls.get_type(scls, schema)
        our_name = typ.get_name(schema)
        assert isinstance(our_name, sn.QualName)
        name_prefix = f'__{our_name.name}__'

        # XXX: This is pretty unfortunate from a performance
        # perspective, and not technically correct either.
        # For 3.x we should track this information in the objects
        # (or possibly instead ensure we do not put any types in the schema
        # that are not directly part of the output type.)
        for obj in schema.get_objects(exclude_stdlib=True, type=s_types.Type):
            name = obj.get_name(schema)
            if (
                obj.get_alias_is_persistent(schema)
                and isinstance(name, sn.QualName)
                and name.module == our_name.module
                and name.name.startswith(name_prefix)
            ):
                objs.add(obj)

        return objs

    def _delete_alias_type(
        self,
        scls: so.QualifiedObject_T,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> sd.DeleteObject[s_types.Type]:
        created = self._get_created_types(scls, schema)

        alias_type = self.get_type(scls, schema)
        drop_type = alias_type.init_delta_command(
            schema, sd.DeleteObject)
        subcmds = drop_type._canonicalize(schema, context, alias_type)
        drop_type.update(subcmds)

        for dep_type in created:
            drop_dep = dep_type.init_delta_command(
                schema, sd.DeleteObject, if_exists=True)
            subcmds = drop_dep._canonicalize(schema, context, dep_type)
            drop_dep.update(subcmds)

            drop_type.add(drop_dep)

        return drop_type

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
    ) -> Tuple[sd.Command, s_types.TypeShell[s_types.Type], s_expr.Expression]:
        pschema = schema

        # On alters, remove all the existing objects from the schema before
        # trying to compile again.
        if is_alter:
            drop_cmd = self._delete_alias_type(
                self.scls, schema, context)
            with context.suspend_dep_verification():
                pschema = drop_cmd.apply(pschema, context)

        ir = compile_alias_expr(
            expr.qlast,
            classname,
            pschema,
            context,
            parser_context=parser_context,
        )

        expr = s_expr.Expression.from_ir(expr, ir, schema=schema)

        if not is_alter:
            prev_expr = None
        else:
            prev = schema.get(classname, type=s_types.Type)
            prev_expr_ = prev.get_expr(schema)
            assert prev_expr_ is not None
            prev_ir = compile_alias_expr(
                prev_expr_.qlast,
                classname,
                pschema,
                context,
                parser_context=parser_context,
            )
            prev_expr = s_expr.Expression.from_ir(
                prev_expr_, prev_ir, schema=schema)

        is_global = (self.get_schema_metaclass().
                     get_schema_class_displayname() == 'global')
        cmd, type_shell = define_alias(
            expr=expr,
            prev_expr=prev_expr,
            classname=classname,
            schema=schema,
            is_global=is_global,
            parser_context=parser_context,
        )

        return cmd, type_shell, expr


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
            type_cmd, type_shell, expr = self._handle_alias_op(
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
        if not context.canonical and not self.metadata_only:
            expr = self.get_attribute_value('expr')
            is_alias = self._is_alias(self.scls, schema)
            if expr:
                alias_name = self._get_alias_name(self.classname)
                type_cmd, type_shell, expr = self._handle_alias_op(
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

    if not isinstance(expr, qlast.Statement):
        expr = qlast.SelectQuery(result=expr)

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

    return ir  # type: ignore


def define_alias(
    *,
    expr: s_expr.CompiledExpression,
    prev_expr: Optional[s_expr.CompiledExpression] = None,
    classname: sn.QualName,
    schema: s_schema.Schema,
    is_global: bool,
    parser_context: Optional[parsing.ParserContext] = None,
) -> Tuple[sd.Command, s_types.TypeShell[s_types.Type]]:
    from edb.ir import ast as irast
    from . import ordering as s_ordering

    ir = expr.irast
    new_schema = ir.schema

    coll_expr_aliases: List[s_types.Collection] = []
    prev_coll_expr_aliases: List[s_types.Collection] = []
    expr_aliases: List[s_types.Type] = []
    prev_expr_aliases: List[s_types.Type] = []
    prev_ir: Optional[irast.Statement] = None
    old_schema: Optional[s_schema.Schema] = None

    for vt in ir.views.values():
        if isinstance(vt, s_types.Collection):
            coll_expr_aliases.append(vt)
        elif prev_expr is not None or not schema.has_object(vt.id):
            new_schema = vt.set_field_value(
                new_schema, 'alias_is_persistent', True)
            new_schema = vt.set_field_value(
                new_schema, 'from_global', is_global)

            expr_aliases.append(vt)

    if prev_expr is not None:
        prev_ir = prev_expr.irast
        old_schema = prev_ir.schema
        for vt in prev_ir.views.values():
            if isinstance(vt, s_types.Collection):
                prev_coll_expr_aliases.append(vt)
            else:
                prev_expr_aliases.append(vt)

    derived_delta = sd.DeltaRoot()

    for ref in ir.new_coll_types:
        colltype_shell = ref.as_shell(new_schema)
        # not "new_schema", because that already contains this
        # collection type.
        derived_delta.add(colltype_shell.as_create_delta(new_schema))

    if prev_expr is not None:
        assert old_schema is not None
        derived_delta.add(
            sd.delta_objects(
                prev_expr_aliases,
                expr_aliases,
                sclass=s_types.Type,
                old_schema=old_schema,
                new_schema=new_schema,
                context=so.ComparisonContext(),
            )
        )
    else:
        for expr_alias in expr_aliases:
            derived_delta.add(
                expr_alias.as_create_delta(
                    schema=new_schema,
                    context=so.ComparisonContext(),
                )
            )

    if prev_ir is not None:
        assert old_schema
        for vt in prev_coll_expr_aliases:
            dt = vt.as_type_delete_if_dead(old_schema)
            derived_delta.prepend(dt)
        for vt in prev_ir.new_coll_types:
            dt = vt.as_type_delete_if_dead(old_schema)
            derived_delta.prepend(dt)

    for vt in coll_expr_aliases:
        new_schema = vt.set_field_value(new_schema, 'expr', expr)
        new_schema = vt.set_field_value(
            new_schema, 'alias_is_persistent', True)
        ct = vt.as_shell(new_schema).as_create_delta(
            # not "new_schema", to ensure the nested collection types
            # are picked up properly.
            new_schema,
            view_name=classname,
            attrs={
                'expr': expr,
                'alias_is_persistent': True,
                'expr_type': s_types.ExprType.Select,
                'from_alias': True,
            },
        )
        derived_delta.add(ct)

    derived_delta = s_ordering.linearize_delta(
        derived_delta, old_schema=schema, new_schema=new_schema)

    existing_type_cmd = None
    for op in derived_delta.get_subcommands():
        assert isinstance(op, sd.ObjectCommand)
        if (
            op.classname == classname
            and not isinstance(op, sd.DeleteObject)
        ):
            existing_type_cmd = op
            break

    if existing_type_cmd is not None:
        type_cmd = existing_type_cmd
    else:
        assert prev_expr is not None
        for expr_alias in expr_aliases:
            if expr_alias.get_name(new_schema) == classname:
                type_cmd = expr_alias.init_delta_command(
                    new_schema,
                    sd.AlterObject,
                )
                derived_delta.add(type_cmd)
                break
        else:
            raise RuntimeError(
                'view delta does not contain the expected '
                'view Create/Alter command')

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
