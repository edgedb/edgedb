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

from edb.lang.edgeql import ast as qlast

from . import abc as s_abc
from . import attributes
from . import casts as s_casts
from . import constraints
from . import delta as sd
from . import expr
from . import inheriting
from . import name as sn
from . import named
from . import nodes
from . import objects as so
from . import types as s_types


class ScalarType(nodes.Node, constraints.ConsistencySubject,
                 attributes.AttributeSubject, s_abc.ScalarType):

    default = so.SchemaField(
        expr.ExpressionText, default=None,
        coerce=True, compcoef=0.909)

    def _get_deps(self, schema):
        deps = super()._get_deps(schema)

        consts = self.get_constraints(schema)
        if consts:
            N = sn.Name

            # Add dependency on all built-in scalars unconditionally
            deps.add(N(module='std', name='str'))
            deps.add(N(module='std', name='bytes'))
            deps.add(N(module='std', name='int16'))
            deps.add(N(module='std', name='int32'))
            deps.add(N(module='std', name='int64'))
            deps.add(N(module='std', name='float32'))
            deps.add(N(module='std', name='float64'))
            deps.add(N(module='std', name='decimal'))
            deps.add(N(module='std', name='bool'))
            deps.add(N(module='std', name='uuid'))

            for constraint in consts.objects(schema):
                c_params = constraint.get_params(schema).objects(schema)
                ptypes = [p.get_type(schema) for p in c_params]
                if ptypes:
                    for ptype in ptypes:
                        if isinstance(ptype, s_abc.Collection):
                            subtypes = ptype.get_subtypes()
                        else:
                            subtypes = [ptype]

                        for subtype in subtypes:
                            if subtype is not self:
                                deps.add(subtype.get_name(schema))

        return deps

    def is_scalar(self):
        return True

    def is_polymorphic(self, schema):
        return self.get_is_abstract(schema)

    def _resolve_polymorphic(self, schema, concrete_type: s_types.Type):
        if (self.is_polymorphic(schema) and
                concrete_type.is_scalar() and
                not concrete_type.is_polymorphic(schema)):
            return concrete_type

    def _to_nonpolymorphic(self, schema, concrete_type: s_types.Type):
        if (not concrete_type.is_polymorphic(schema) and
                concrete_type.issubclass(schema, self)):
            return concrete_type
        raise TypeError(
            f'cannot interpret {concrete_type.get_name(schema)} '
            f'as {self.get_name(schema)}')

    def _test_polymorphic(self, schema, other: s_types.Type):
        if other.is_any():
            return True
        else:
            return self.issubclass(schema, other)

    def assignment_castable_to(self, target: s_types.Type, schema) -> bool:
        if self.implicitly_castable_to(target, schema):
            return True

        source = self.get_topmost_concrete_base(schema)
        target = target.get_topmost_concrete_base(schema)

        return s_casts.is_assignment_castable(schema, source, target)

    def implicitly_castable_to(self, other: s_types.Type, schema) -> bool:
        if not isinstance(other, ScalarType):
            return False
        left = self.get_topmost_concrete_base(schema)
        right = other.get_topmost_concrete_base(schema)
        return s_casts.is_implicitly_castable(schema, left, right)

    def get_implicit_cast_distance(self, other: s_types.Type, schema) -> int:
        if not isinstance(other, ScalarType):
            return -1
        left = self.get_topmost_concrete_base(schema)
        right = other.get_topmost_concrete_base(schema)
        return s_casts.get_implicit_cast_distance(schema, left, right)

    def find_common_implicitly_castable_type(
            self, other: s_types.Type,
            schema) -> typing.Optional[s_types.Type]:

        if not isinstance(other, ScalarType):
            return

        if self.is_polymorphic(schema) and other.is_polymorphic(schema):
            return self

        left = self.get_topmost_concrete_base(schema)
        right = other.get_topmost_concrete_base(schema)

        if left == right:
            return left
        else:
            return s_casts.find_common_castable_type(schema, left, right)


class ScalarTypeCommandContext(sd.ObjectCommandContext,
                               attributes.AttributeSubjectCommandContext,
                               constraints.ConsistencySubjectCommandContext,
                               nodes.NodeCommandContext):
    pass


class ScalarTypeCommand(constraints.ConsistencySubjectCommand,
                        attributes.AttributeSubjectCommand,
                        nodes.NodeCommand,
                        schema_metaclass=ScalarType,
                        context_class=ScalarTypeCommandContext):
    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        cmd = cls._handle_view_op(schema, cmd, astnode, context)
        return cmd


class CreateScalarType(ScalarTypeCommand, inheriting.CreateInheritingObject):
    astnode = qlast.CreateScalarType

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)

        for sub in cmd.get_subcommands(type=sd.AlterObjectProperty):
            if sub.property == 'default':
                sub.new_value = [sub.new_value]

        return cmd

    def _apply_field_ast(self, schema, context, node, op):
        if op.property == 'default':
            if op.new_value:
                op.new_value = op.new_value[0]
                super()._apply_field_ast(schema, context, node, op)
        else:
            super()._apply_field_ast(schema, context, node, op)


class RenameScalarType(ScalarTypeCommand, named.RenameNamedObject):
    pass


class RebaseScalarType(ScalarTypeCommand, inheriting.RebaseNamedObject):
    pass


class AlterScalarType(ScalarTypeCommand, inheriting.AlterInheritingObject):
    astnode = qlast.AlterScalarType


class DeleteScalarType(ScalarTypeCommand, inheriting.DeleteInheritingObject):
    astnode = qlast.DropScalarType
