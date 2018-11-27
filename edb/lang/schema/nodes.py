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


import typing

from edb import errors

from edb.lang.edgeql import ast as qlast

from . import delta as sd
from . import inheriting
from . import objects as so
from . import schema as s_schema
from . import types as s_types
from . import utils


class Node(inheriting.InheritingObject, s_types.Type):
    def material_type(self, schema):
        t = self
        while t.is_view(schema):
            t = t.get_bases(schema).first(schema)
        return t

    def derive_subtype(
            self, schema, *,
            name: str,
            attrs: typing.Optional[typing.Mapping]=None
    ) -> typing.Tuple[s_schema.Schema, s_types.Type]:

        return type(self).create_in_schema_with_inheritance(
            schema, name=name, bases=[self], **attrs)

    def peel_view(self, schema):
        if self.is_view(schema):
            return self.get_bases(schema).first(schema)
        else:
            return self

    def get_common_parent_type_distance(
            self, other: s_types.Type, schema) -> int:
        if other.is_any():
            return s_types.MAX_TYPE_DISTANCE

        if not isinstance(other, type(self)):
            return -1

        if self == other:
            return 0

        ancestor = utils.get_class_nearest_common_ancestor(
            schema, [self, other])

        if ancestor == self:
            return 0
        else:
            mro = list(self.get_mro(schema).objects(schema))
            return mro.index(ancestor) + 1


class NodeCommandContext:
    # context mixin
    pass


class NodeCommand(sd.ObjectCommand):
    @classmethod
    def _maybe_get_view_expr(cls, astnode):
        for subcmd in astnode.commands:
            if (isinstance(subcmd, qlast.SetField) and
                    subcmd.name.name == 'expr'):
                return subcmd.value

    @classmethod
    def _get_view_expr(cls, astnode):
        expr = cls._maybe_get_view_expr(astnode)
        if expr is None:
            raise errors.InvalidViewDefinitionError(
                f'missing required view expression', context=astnode.context)
        return expr

    @classmethod
    def _compile_view_expr(cls, expr, classname, schema, context):
        from edb.lang.edgeql import compiler as qlcompiler

        ir = context.get_cached((expr, classname))
        if ir is None:
            if not isinstance(expr, qlast.Statement):
                expr = qlast.SelectQuery(result=expr)
            ir = qlcompiler.compile_ast_to_ir(
                expr, schema, derived_target_module=classname.module,
                result_view_name=classname, modaliases=context.modaliases,
                schema_view_mode=True)
            context.cache_value((expr, classname), ir)

        return ir

    @classmethod
    def _handle_view_op(cls, schema, cmd, astnode, context):
        view_expr = cls._maybe_get_view_expr(astnode)
        if view_expr is not None:
            ir = cls._compile_view_expr(view_expr, cmd.classname,
                                        schema, context)

            view_types = ir.views.values()

            if isinstance(astnode, qlast.AlterObjectType):
                prev = schema.get(cmd.classname)
                prev_ir = cls._compile_view_expr(
                    prev.expr, cmd.classname, schema, context)
                prev_view_types = prev_ir.views.values()
            else:
                prev_ir = None
                prev_view_types = []

            derived_delta = sd.DeltaRoot()

            new_schema = ir.schema
            old_schema = prev_ir.schema if prev_ir is not None else None

            adds_mods, dels = so.Object._delta_sets(
                prev_view_types, view_types,
                old_schema=old_schema, new_schema=new_schema)

            derived_delta.update(adds_mods)
            derived_delta.update(dels)

            if ir.stype.is_view(ir.schema):
                for op in list(derived_delta.get_subcommands()):
                    if op.classname == cmd.classname:
                        for subop in op.get_subcommands():
                            if isinstance(subop, sd.AlterObjectProperty):
                                cmd.discard_attribute(subop.property)
                            cmd.add(subop)

                        derived_delta.discard(op)

            cmd.update(derived_delta.get_subcommands())
            cmd.discard_attribute('view_type')
            cmd.add(sd.AlterObjectProperty(
                property='view_type', new_value=s_types.ViewType.Select))

        return cmd
