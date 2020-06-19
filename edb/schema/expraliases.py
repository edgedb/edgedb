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

from edb.edgeql import ast as qlast
from edb.edgeql import compiler as qlcompiler
from edb.edgeql import qltypes

from . import annos as s_anno
from . import expr as s_expr
from . import delta as sd
from . import name as sn
from . import objects as so
from . import types as s_types
from . import utils as s_utils


if TYPE_CHECKING:
    from edb.ir import ast as irast
    from . import schema as s_schema


class Alias(
    so.QualifiedObject,
    s_anno.AnnotationSubject,
    qlkind=qltypes.SchemaObjectClass.ALIAS,
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


class AliasCommand(
    sd.QualifiedObjectCommand[Alias],
    context_class=AliasCommandContext,
    schema_metaclass=Alias,
):

    @classmethod
    def _classname_from_ast(cls,
                            schema: s_schema.Schema,
                            astnode: qlast.NamedDDL,
                            context: sd.CommandContext
                            ) -> sn.Name:
        type_name = super()._classname_from_ast(schema, astnode, context)
        base_name = type_name
        quals = ('alias',)
        pnn = sn.get_specialized_name(base_name, type_name, *quals)
        name = sn.Name(name=pnn, module=type_name.module)
        assert isinstance(name, sn.Name)
        return name

    def _compile_alias_expr(
        self,
        expr: qlast.Base,
        classname: sn.SchemaName,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> irast.Statement:
        cached: Optional[irast.Statement] = (
            context.get_cached((expr, classname)))
        if cached is not None:
            return cached

        if not isinstance(expr, qlast.Statement):
            expr = qlast.SelectQuery(result=expr)

        existing = schema.get(classname, type=s_types.Type, default=None)
        if existing is not None:
            drop_cmd = existing.init_delta_command(schema, sd.DeleteObject)
            with context.suspend_dep_verification():
                schema = drop_cmd.apply(schema, context)

        ir = qlcompiler.compile_ast_to_ir(
            expr,
            schema,
            options=qlcompiler.CompilerOptions(
                derived_target_module=classname.module,
                result_view_name=classname,
                modaliases=context.modaliases,
                schema_view_mode=True,
            ),
        )

        context.cache_value((expr, classname), ir)

        return ir  # type: ignore

    def _handle_alias_op(
        self,
        expr: s_expr.Expression,
        classname: sn.SchemaName,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        is_alter: bool = False,
    ) -> sd.Command:
        from . import ordering as s_ordering

        ir = self._compile_alias_expr(expr.qlast, classname, schema, context)
        new_schema = ir.schema
        expr = s_expr.Expression.from_ir(expr, ir, schema=schema)

        coll_expr_aliases: List[s_types.Collection] = []
        prev_coll_expr_aliases: List[s_types.Collection] = []
        expr_aliases: List[s_types.Type] = []
        prev_expr_aliases: List[s_types.Type] = []
        prev_ir: Optional[irast.Statement] = None
        old_schema: Optional[s_schema.Schema] = None

        for vt in ir.views.values():
            if isinstance(vt, s_types.Collection):
                coll_expr_aliases.append(vt)
            else:
                new_schema = vt.set_field_value(
                    new_schema, 'alias_is_persistent', True)

                expr_aliases.append(vt)

        if is_alter:
            prev = cast(s_types.Type, schema.get(classname))
            prev_expr = prev.get_expr(schema)
            assert prev_expr is not None
            prev_ir = self._compile_alias_expr(
                prev_expr.qlast, classname, schema, context)
            old_schema = prev_ir.schema
            for vt in prev_ir.views.values():
                if isinstance(vt, s_types.Collection):
                    prev_coll_expr_aliases.append(vt)
                else:
                    prev_expr_aliases.append(vt)

        derived_delta = sd.DeltaRoot()

        for ref in ir.new_coll_types:
            s_types.ensure_schema_collection(
                # not "new_schema", because that already contains this
                # collection type.
                schema,
                ref.as_shell(new_schema),
                derived_delta,
                context=context,
            )

        if is_alter:
            assert old_schema is not None
            derived_delta.add(
                sd.delta_objects(
                    prev_expr_aliases,
                    expr_aliases,
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
            for vt in prev_coll_expr_aliases:
                dt = vt.as_colltype_delete_delta(
                    prev_ir.schema,
                    view_name=classname,
                )
                derived_delta.prepend(dt)

        for vt in coll_expr_aliases:
            new_schema = vt.set_field_value(new_schema, 'expr', expr)
            ct = vt.as_shell(new_schema).as_create_delta(
                # not "new_schema", to ensure the nested collection types
                # are picked up properly.
                schema,
                view_name=classname,
                attrs={
                    'expr': expr,
                    'alias_is_persistent': True,
                    'expr_type': s_types.ExprType.Select,
                },
            )
            new_schema = ct.apply(new_schema, context)
            derived_delta.add(ct)

        derived_delta = s_ordering.linearize_delta(
            derived_delta, old_schema=old_schema, new_schema=new_schema)

        real_cmd = None
        for op in derived_delta.get_subcommands():
            assert isinstance(op, sd.ObjectCommand)
            if op.classname == classname:
                real_cmd = op
                break

        if real_cmd is None:
            raise RuntimeError(
                'view delta does not contain the expected '
                'view Create/Alter command')

        real_cmd.set_attribute_value('expr', expr)

        result = sd.CommandGroup()
        result.update(derived_delta.get_subcommands())
        result.canonical = True
        return result


class CreateAlias(
    AliasCommand,
    sd.CreateObject[Alias],
):
    astnode = qlast.CreateAlias

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if not context.canonical:
            alias_name = sn.shortname_from_fullname(self.classname)
            type_cmd = self._handle_alias_op(
                self.get_attribute_value('expr'),
                alias_name,
                schema,
                context,
            )
            self.add_prerequisite(type_cmd)
            for cmd in type_cmd.get_subcommands(type=sd.ObjectCommand):
                if cmd.classname == alias_name:
                    break
            else:
                raise AssertionError(
                    '_handle_alias_op() did not return a command'
                    ' for derived type'
                )
            assert isinstance(cmd, sd.ObjectCommand)
            self.set_attribute_value(
                'expr',
                cmd.get_attribute_value('expr'),
            )
            self.set_attribute_value(
                'type',
                s_utils.ast_objref_to_object_shell(
                    s_utils.name_to_ast_ref(cmd.classname),
                    metaclass=cmd.get_schema_metaclass(),
                    modaliases={},
                    schema=schema,
                )
            )

        return super()._create_begin(schema, context)


class RenameAlias(AliasCommand, sd.RenameObject[Alias]):

    def _rename_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if not context.canonical:
            new_alias_name = sn.shortname_from_fullname(self.new_name)
            alias_type = self.scls.get_type(schema)
            alter_cmd = alias_type.init_delta_command(schema, sd.AlterObject)
            rename_cmd = alias_type.init_delta_command(
                schema,
                sd.RenameObject,
                new_name=new_alias_name,
            )
            alter_cmd.add(rename_cmd)
            self.add_prerequisite(alter_cmd)

        return super()._rename_begin(schema, context)


class AlterAlias(
    AliasCommand,
    sd.AlterObject[Alias],
):
    astnode = qlast.AlterAlias

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if not context.canonical:
            expr = self.get_attribute_value('expr')
            if expr:
                alias_name = sn.shortname_from_fullname(self.classname)
                type_cmd = self._handle_alias_op(
                    expr,
                    alias_name,
                    schema,
                    context,
                    is_alter=True,
                )
                self.add_prerequisite(type_cmd)
                for cmd in type_cmd.get_subcommands():
                    if (
                        isinstance(cmd, sd.CreateObject)
                        and cmd.classname == alias_name
                    ):
                        break
                else:
                    for cmd in type_cmd.get_subcommands():
                        if (
                            isinstance(cmd, sd.AlterObject)
                            and cmd.classname == alias_name
                        ):
                            break
                    else:
                        raise AssertionError(
                            '_handle_alias_op() did not return a command'
                            ' for derived type'
                        )

                self.set_attribute_value(
                    'expr',
                    cmd.get_attribute_value('expr'),
                )

                self.set_attribute_value(
                    'type',
                    s_utils.ast_objref_to_object_shell(
                        s_utils.name_to_ast_ref(cmd.classname),
                        metaclass=cmd.get_schema_metaclass(),
                        modaliases={},
                        schema=schema,
                    )
                )

        return super()._alter_begin(schema, context)


class DeleteAlias(
    AliasCommand,
    sd.DeleteObject[Alias],
):
    astnode = qlast.DropAlias

    def _delete_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if not context.canonical:
            alias_type = self.scls.get_type(schema)
            drop_type = alias_type.init_delta_command(schema, sd.DeleteObject)
            self.add_prerequisite(drop_type)

        return super()._delete_begin(schema, context)
